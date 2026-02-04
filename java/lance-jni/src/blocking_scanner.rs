// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::ffi::JNIEnvExt;
use crate::traits::{import_vec_from_method, import_vec_to_rust};
use arrow::array::Float32Array;
use arrow::{ffi::FFI_ArrowSchema, ffi_stream::FFI_ArrowArrayStream};
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use jni::objects::{JByteArray, JLongArray, JObject, JString};
use jni::sys::{jboolean, jint, JNI_TRUE};
use jni::{sys::jlong, JNIEnv};
use lance::dataset::scanner::{
    ColumnOrdering, DatasetRecordBatchStream, Scanner, SplitOptions, Splits,
};
use lance::deps::datafusion::prelude::Expr;
use lance::io::exec::filtered_read::FilteredReadPlan;
use lance_datafusion::exec::{get_session_context, LanceExecutionOptions};
use lance_datafusion::substrait::{encode_substrait, parse_substrait};
use lance_index::scalar::inverted::query::{
    BooleanQuery as FtsBooleanQuery, BoostQuery as FtsBoostQuery, FtsQuery,
    MatchQuery as FtsMatchQuery, MultiMatchQuery as FtsMultiMatchQuery, Occur as FtsOccur,
    PhraseQuery as FtsPhraseQuery,
};
use lance_index::scalar::FullTextSearchQuery;
use lance_io::ffi::to_ffi_arrow_array_stream;
use lance_linalg::distance::DistanceType;

use crate::{
    blocking_dataset::{BlockingDataset, NATIVE_DATASET},
    traits::IntoJava,
    RT,
};

pub const NATIVE_SCANNER: &str = "nativeScannerHandle";

#[derive(Clone)]
pub struct BlockingScanner {
    pub(crate) inner: Arc<Scanner>,
}

impl BlockingScanner {
    pub fn create(scanner: Scanner) -> Self {
        Self {
            inner: Arc::new(scanner),
        }
    }

    pub fn open_stream(&self) -> Result<DatasetRecordBatchStream> {
        let res = RT.block_on(self.inner.try_into_stream())?;
        Ok(res)
    }

    pub fn schema(&self) -> Result<SchemaRef> {
        let res = RT.block_on(self.inner.schema())?;
        Ok(res)
    }

    pub fn count_rows(&self) -> Result<u64> {
        let res = RT.block_on(self.inner.count_rows())?;
        Ok(res)
    }

    pub fn plan_splits(&self, options: Option<SplitOptions>) -> Result<Splits> {
        let res = RT.block_on(self.inner.plan_splits(options))?;
        Ok(res)
    }

    pub fn execute_filtered_read_plan(
        &self,
        plan: FilteredReadPlan,
    ) -> Result<DatasetRecordBatchStream> {
        let res = RT.block_on(self.inner.execute_filtered_read_plan(plan))?;
        Ok(res)
    }
}

fn build_full_text_search_query<'a>(env: &mut JNIEnv<'a>, java_obj: JObject) -> Result<FtsQuery> {
    let type_obj = env
        .call_method(
            &java_obj,
            "getType",
            "()Lorg/lance/ipc/FullTextQuery$Type;",
            &[],
        )?
        .l()?;
    let type_name = env.get_string_from_method(&type_obj, "name")?;

    match type_name.as_str() {
        "MATCH" => {
            let query_text = env.get_string_from_method(&java_obj, "getQueryText")?;
            let column = env.get_string_from_method(&java_obj, "getColumn")?;
            let boost = env.get_f32_from_method(&java_obj, "getBoost")?;
            let fuzziness = env.get_optional_u32_from_method(&java_obj, "getFuzziness")?;
            let max_expansions = env.get_int_as_usize_from_method(&java_obj, "getMaxExpansions")?;
            let operator = env.get_fts_operator_from_method(&java_obj)?;
            let prefix_length = env.get_u32_from_method(&java_obj, "getPrefixLength")?;

            let mut query = FtsMatchQuery::new(query_text);
            query = query.with_column(Some(column));
            query = query
                .with_boost(boost)
                .with_fuzziness(fuzziness)
                .with_max_expansions(max_expansions)
                .with_operator(operator)
                .with_prefix_length(prefix_length);

            Ok(FtsQuery::Match(query))
        }
        "MATCH_PHRASE" => {
            let query_text = env.get_string_from_method(&java_obj, "getQueryText")?;
            let column = env.get_string_from_method(&java_obj, "getColumn")?;
            let slop = env.get_u32_from_method(&java_obj, "getSlop")?;

            let mut query = FtsPhraseQuery::new(query_text);
            query = query.with_column(Some(column));
            query = query.with_slop(slop);

            Ok(FtsQuery::Phrase(query))
        }
        "MULTI_MATCH" => {
            let query_text = env.get_string_from_method(&java_obj, "getQueryText")?;
            let columns: Vec<String> =
                import_vec_from_method(env, &java_obj, "getColumns", |env, elem| {
                    let jstr = JString::from(elem);
                    let value: String = env.get_string(&jstr)?.into();
                    Ok(value)
                })?;

            let boosts: Option<Vec<f32>> =
                env.get_optional_from_method(&java_obj, "getBoosts", |env, list_obj| {
                    import_vec_to_rust(env, &list_obj, |env, elem| {
                        env.get_f32_from_method(&elem, "floatValue")
                    })
                })?;
            let operator = env.get_fts_operator_from_method(&java_obj)?;

            let mut query = FtsMultiMatchQuery::try_new(query_text, columns)?;
            if let Some(boosts) = boosts {
                query = query.try_with_boosts(boosts)?;
            }
            query = query.with_operator(operator);

            Ok(FtsQuery::MultiMatch(query))
        }
        "BOOST" => {
            let positive_obj = env
                .call_method(
                    &java_obj,
                    "getPositive",
                    "()Lorg/lance/ipc/FullTextQuery;",
                    &[],
                )?
                .l()?;
            if positive_obj.is_null() {
                return Err(Error::input_error(
                    "positive query must not be null in BOOST FullTextQuery".to_string(),
                ));
            }
            let negative_obj = env
                .call_method(
                    &java_obj,
                    "getNegative",
                    "()Lorg/lance/ipc/FullTextQuery;",
                    &[],
                )?
                .l()?;
            if negative_obj.is_null() {
                return Err(Error::input_error(
                    "negative query must not be null in BOOST FullTextQuery".to_string(),
                ));
            }

            let positive = build_full_text_search_query(env, positive_obj)?;
            let negative = build_full_text_search_query(env, negative_obj)?;
            let negative_boost = env.get_f32_from_method(&java_obj, "getNegativeBoost")?;

            let query = FtsBoostQuery::new(positive, negative, Some(negative_boost));
            Ok(FtsQuery::Boost(query))
        }
        "BOOLEAN" => {
            let clauses: Vec<(FtsOccur, FtsQuery)> =
                import_vec_from_method(env, &java_obj, "getClauses", |env, clause_obj| {
                    let occur = env.get_occur_from_method(&clause_obj)?;

                    let query_obj = env
                        .call_method(
                            &clause_obj,
                            "getQuery",
                            "()Lorg/lance/ipc/FullTextQuery;",
                            &[],
                        )?
                        .l()?;
                    if query_obj.is_null() {
                        return Err(Error::input_error(
                            "BooleanClause query must not be null".to_string(),
                        ));
                    }
                    let query = build_full_text_search_query(env, query_obj)?;
                    Ok((occur, query))
                })?;

            let boolean_query = FtsBooleanQuery::new(clauses);
            Ok(FtsQuery::Boolean(boolean_query))
        }
        other => Err(Error::input_error(format!(
            "Unsupported FullTextQuery type: {}",
            other
        ))),
    }
}

///////////////////
// Write Methods //
///////////////////
#[no_mangle]
pub extern "system" fn Java_org_lance_ipc_LanceScanner_createScanner<'local>(
    mut env: JNIEnv<'local>,
    _reader: JObject,
    jdataset: JObject,
    fragment_ids_obj: JObject,     // Optional<List<Integer>>
    columns_obj: JObject,          // Optional<List<String>>
    substrait_filter_obj: JObject, // Optional<ByteBuffer>
    filter_obj: JObject,           // Optional<String>
    batch_size_obj: JObject,       // Optional<Long>
    limit_obj: JObject,            // Optional<Integer>
    offset_obj: JObject,           // Optional<Integer>
    query_obj: JObject,            // Optional<Query>
    fts_query_obj: JObject,        // Optional<FullTextQuery>
    with_row_id: jboolean,         // boolean
    with_row_address: jboolean,    // boolean
    batch_readahead: jint,         // int
    column_orderings: JObject,     // Optional<List<ColumnOrdering>>
) -> JObject<'local> {
    ok_or_throw!(
        env,
        inner_create_scanner(
            &mut env,
            jdataset,
            fragment_ids_obj,
            columns_obj,
            substrait_filter_obj,
            filter_obj,
            batch_size_obj,
            limit_obj,
            offset_obj,
            query_obj,
            fts_query_obj,
            with_row_id,
            with_row_address,
            batch_readahead,
            column_orderings
        )
    )
}

#[allow(clippy::too_many_arguments)]
fn inner_create_scanner<'local>(
    env: &mut JNIEnv<'local>,
    jdataset: JObject,
    fragment_ids_obj: JObject,
    columns_obj: JObject,
    substrait_filter_obj: JObject,
    filter_obj: JObject,
    batch_size_obj: JObject,
    limit_obj: JObject,
    offset_obj: JObject,
    query_obj: JObject,
    fts_query_obj: JObject,
    with_row_id: jboolean,
    with_row_address: jboolean,
    batch_readahead: jint,
    column_orderings: JObject,
) -> Result<JObject<'local>> {
    let fragment_ids_opt = env.get_ints_opt(&fragment_ids_obj)?;
    let dataset_guard =
        unsafe { env.get_rust_field::<_, _, BlockingDataset>(jdataset, NATIVE_DATASET) }?;

    let mut scanner = dataset_guard.inner.scan();

    // handle fragment_ids
    if let Some(fragment_ids) = fragment_ids_opt {
        let mut fragments = Vec::with_capacity(fragment_ids.len());
        for fragment_id in fragment_ids {
            let Some(fragment) = dataset_guard.inner.get_fragment(fragment_id as usize) else {
                return Err(Error::input_error(format!(
                    "Fragment {fragment_id} not found"
                )));
            };
            fragments.push(fragment.metadata().clone());
        }
        scanner.with_fragments(fragments);
    }
    drop(dataset_guard);

    let columns_opt = env.get_strings_opt(&columns_obj)?;
    if let Some(columns) = columns_opt {
        scanner.project(&columns)?;
    };

    let substrait_opt = env.get_bytes_opt(&substrait_filter_obj)?;
    if let Some(substrait) = substrait_opt {
        RT.block_on(async { scanner.filter_substrait(substrait) })?;
    }

    let filter_opt = env.get_string_opt(&filter_obj)?;
    if let Some(filter) = filter_opt {
        scanner.filter(filter.as_str())?;
    }

    let batch_size_opt = env.get_long_opt(&batch_size_obj)?;
    if let Some(batch_size) = batch_size_opt {
        scanner.batch_size(batch_size as usize);
    }

    let limit_opt = env.get_long_opt(&limit_obj)?;
    let offset_opt = env.get_long_opt(&offset_obj)?;
    scanner
        .limit(limit_opt, offset_opt)
        .map_err(|err| Error::input_error(err.to_string()))?;

    if with_row_id == JNI_TRUE {
        scanner.with_row_id();
    }

    if with_row_address == JNI_TRUE {
        scanner.with_row_address();
    }

    env.get_optional(&query_obj, |env, java_obj| {
        // Set column and key for nearest search
        let column = env.get_string_from_method(&java_obj, "getColumn")?;
        let key_array = env.get_vec_f32_from_method(&java_obj, "getKey")?;
        let key = Float32Array::from(key_array);
        let k = env.get_int_as_usize_from_method(&java_obj, "getK")?;
        let _ = scanner.nearest(&column, &key, k);

        let minimum_nprobes = env.get_int_as_usize_from_method(&java_obj, "getMinimumNprobes")?;
        scanner.minimum_nprobes(minimum_nprobes);

        let maximum_nprobes = env.get_optional_usize_from_method(&java_obj, "getMaximumNprobes")?;
        if let Some(maximum_nprobes) = maximum_nprobes {
            scanner.maximum_nprobes(maximum_nprobes);
        }

        if let Some(ef) = env.get_optional_usize_from_method(&java_obj, "getEf")? {
            scanner.ef(ef);
        }

        if let Some(refine_factor) =
            env.get_optional_u32_from_method(&java_obj, "getRefineFactor")?
        {
            scanner.refine(refine_factor);
        }

        if let Some(distance_type_str) =
            env.get_optional_string_from_method(&java_obj, "getDistanceTypeString")?
        {
            let distance_type = DistanceType::try_from(distance_type_str.as_str())?;
            scanner.distance_metric(distance_type);
        }

        let use_index = env.get_boolean_from_method(&java_obj, "isUseIndex")?;
        scanner.use_index(use_index);
        Ok(())
    })?;

    env.get_optional(&fts_query_obj, |env, java_obj| {
        let fts_query = build_full_text_search_query(env, java_obj)?;
        let full_text_query = FullTextSearchQuery::new_query(fts_query);
        scanner.full_text_search(full_text_query)?;
        Ok(())
    })?;

    scanner.batch_readahead(batch_readahead as usize);

    env.get_optional(&column_orderings, |env, java_obj| {
        let list = env.get_list(&java_obj)?;
        let mut iter = list.iter(env)?;
        let mut results = Vec::with_capacity(list.size(env)? as usize);
        while let Some(elem) = iter.next(env)? {
            let column_name = env.get_string_from_method(&elem, "getColumnName")?;
            let nulls_first = env.get_boolean_from_method(&elem, "isNullFirst")?;
            let ascending = env.get_boolean_from_method(&elem, "isAscending")?;
            let col_order = ColumnOrdering {
                ascending,
                nulls_first,
                column_name,
            };
            results.push(col_order)
        }
        scanner.order_by(Some(results))?;
        Ok(())
    })?;

    let scanner = BlockingScanner::create(scanner);
    scanner.into_java(env)
}

#[no_mangle]
pub extern "system" fn Java_org_lance_ipc_LanceScanner_releaseNativeScanner(
    mut env: JNIEnv,
    j_scanner: JObject,
) {
    ok_or_throw_without_return!(env, inner_release_native_scanner(&mut env, j_scanner));
}

fn inner_release_native_scanner(env: &mut JNIEnv, j_scanner: JObject) -> Result<()> {
    let _: BlockingScanner = unsafe { env.take_rust_field(j_scanner, NATIVE_SCANNER) }?;
    Ok(())
}

impl IntoJava for BlockingScanner {
    fn into_java<'local>(self, env: &mut JNIEnv<'local>) -> Result<JObject<'local>> {
        attach_native_scanner(env, self)
    }
}

fn attach_native_scanner<'local>(
    env: &mut JNIEnv<'local>,
    scanner: BlockingScanner,
) -> Result<JObject<'local>> {
    let j_scanner = create_java_scanner_object(env)?;
    // This block sets a native Rust object (scanner) as a field in the Java object (j_scanner).
    // Caution: This creates a potential for memory leaks. The Rust object (scanner) is not
    // automatically garbage-collected by Java, and its memory will not be freed unless
    // explicitly handled.
    //
    // To prevent memory leaks, ensure the following:
    // 1. The Java object (`j_scanner`) should implement the `java.io.Closeable` interface.
    // 2. Users of this Java object should be instructed to always use it within a try-with-resources
    //    statement (or manually call the `close()` method) to ensure that `self.close()` is invoked.
    unsafe { env.set_rust_field(&j_scanner, NATIVE_SCANNER, scanner) }?;
    Ok(j_scanner)
}

fn create_java_scanner_object<'a>(env: &mut JNIEnv<'a>) -> Result<JObject<'a>> {
    let res = env.new_object("org/lance/ipc/LanceScanner", "()V", &[])?;
    Ok(res)
}

//////////////////
// Read Methods //
//////////////////
#[no_mangle]
pub extern "system" fn Java_org_lance_ipc_LanceScanner_openStream(
    mut env: JNIEnv,
    j_scanner: JObject,
    stream_addr: jlong,
) {
    ok_or_throw_without_return!(env, inner_open_stream(&mut env, j_scanner, stream_addr));
}

fn inner_open_stream(env: &mut JNIEnv, j_scanner: JObject, stream_addr: jlong) -> Result<()> {
    let record_batch_stream = {
        let scanner_guard =
            unsafe { env.get_rust_field::<_, _, BlockingScanner>(j_scanner, NATIVE_SCANNER) }?;
        scanner_guard.open_stream()?
    };
    let ffi_stream = to_ffi_arrow_array_stream(record_batch_stream, RT.handle().clone())?;
    unsafe { std::ptr::write_unaligned(stream_addr as *mut FFI_ArrowArrayStream, ffi_stream) }
    Ok(())
}

#[no_mangle]
pub extern "system" fn Java_org_lance_ipc_LanceScanner_importFfiSchema(
    mut env: JNIEnv,
    j_scanner: JObject,
    schema_addr: jlong,
) {
    ok_or_throw_without_return!(
        env,
        inner_import_ffi_schema(&mut env, j_scanner, schema_addr)
    );
}

fn inner_import_ffi_schema(env: &mut JNIEnv, j_scanner: JObject, schema_addr: jlong) -> Result<()> {
    let schema = {
        let scanner_guard =
            unsafe { env.get_rust_field::<_, _, BlockingScanner>(j_scanner, NATIVE_SCANNER) }?;
        scanner_guard.schema()?
    };
    let ffi_schema = FFI_ArrowSchema::try_from(&*schema)?;
    unsafe { std::ptr::write_unaligned(schema_addr as *mut FFI_ArrowSchema, ffi_schema) }
    Ok(())
}

#[no_mangle]
pub extern "system" fn Java_org_lance_ipc_LanceScanner_nativeCountRows(
    mut env: JNIEnv,
    j_scanner: JObject,
) -> jlong {
    ok_or_throw_with_return!(env, inner_count_rows(&mut env, j_scanner), -1) as jlong
}

fn inner_count_rows(env: &mut JNIEnv, j_scanner: JObject) -> Result<u64> {
    let scanner_guard =
        unsafe { env.get_rust_field::<_, _, BlockingScanner>(j_scanner, NATIVE_SCANNER) }?;
    scanner_guard.count_rows()
}

////////////////////////
// plan_splits        //
////////////////////////
#[no_mangle]
pub extern "system" fn Java_org_lance_ipc_LanceScanner_nativePlanSplits<'local>(
    mut env: JNIEnv<'local>,
    j_scanner: JObject,
    max_size_bytes_obj: JObject, // Optional<Long>
    max_row_count_obj: JObject,  // Optional<Long>
) -> JObject<'local> {
    ok_or_throw!(
        env,
        inner_plan_splits(&mut env, j_scanner, max_size_bytes_obj, max_row_count_obj)
    )
}

fn inner_plan_splits<'local>(
    env: &mut JNIEnv<'local>,
    j_scanner: JObject,
    max_size_bytes_obj: JObject,
    max_row_count_obj: JObject,
) -> Result<JObject<'local>> {
    let max_size_bytes = env.get_long_opt(&max_size_bytes_obj)?.map(|v| v as usize);
    let max_row_count = env.get_long_opt(&max_row_count_obj)?.map(|v| v as usize);

    let options = if max_size_bytes.is_some() || max_row_count.is_some() {
        Some(SplitOptions {
            max_size_bytes,
            max_row_count,
        })
    } else {
        None
    };

    let (splits, dataset_schema) = {
        let scanner_guard =
            unsafe { env.get_rust_field::<_, _, BlockingScanner>(j_scanner, NATIVE_SCANNER) }?;
        let splits = scanner_guard.plan_splits(options)?;
        let schema = scanner_guard.inner.dataset_schema().clone();
        (splits, schema)
    };

    create_java_splits(env, splits, &dataset_schema)
}

fn create_java_splits<'a>(
    env: &mut JNIEnv<'a>,
    splits: Splits,
    dataset_schema: &lance_core::datatypes::Schema,
) -> Result<JObject<'a>> {
    match splits {
        Splits::FilteredReadPlans(plans) => {
            // Create ArrayList<FilteredReadPlan>
            let j_list = env.new_object(
                "java/util/ArrayList",
                "(I)V",
                &[(plans.len() as jint).into()],
            )?;
            for plan in plans {
                let j_plan = create_java_filtered_read_plan(env, plan, dataset_schema)?;
                env.call_method(&j_list, "add", "(Ljava/lang/Object;)Z", &[(&j_plan).into()])?;
            }
            // new Splits(filteredReadPlans, null)
            let j_splits = env.new_object(
                "org/lance/ipc/Splits",
                "(Ljava/util/List;Ljava/util/List;)V",
                &[(&j_list).into(), (&JObject::null()).into()],
            )?;
            Ok(j_splits)
        }
        Splits::Fragments(fragment_ids) => {
            // Create ArrayList<Integer>
            let j_list = env.new_object(
                "java/util/ArrayList",
                "(I)V",
                &[(fragment_ids.len() as jint).into()],
            )?;
            for frag_id in fragment_ids {
                let j_int =
                    env.new_object("java/lang/Integer", "(I)V", &[(frag_id as jint).into()])?;
                env.call_method(&j_list, "add", "(Ljava/lang/Object;)Z", &[(&j_int).into()])?;
            }
            // new Splits(null, fragments)
            let j_splits = env.new_object(
                "org/lance/ipc/Splits",
                "(Ljava/util/List;Ljava/util/List;)V",
                &[(&JObject::null()).into(), (&j_list).into()],
            )?;
            Ok(j_splits)
        }
    }
}

fn create_java_filtered_read_plan<'a>(
    env: &mut JNIEnv<'a>,
    plan: FilteredReadPlan,
    dataset_schema: &lance_core::datatypes::Schema,
) -> Result<JObject<'a>> {
    let j_plan = env.new_object("org/lance/ipc/FilteredReadPlan", "()V", &[])?;

    // Create HashMap<Integer, List<long[]>> for fragmentRanges
    let j_map = env.new_object("java/util/HashMap", "()V", &[])?;
    for (frag_id, ranges) in &plan.rows {
        let j_key = env.new_object("java/lang/Integer", "(I)V", &[(*frag_id as jint).into()])?;
        let j_range_list = env.new_object(
            "java/util/ArrayList",
            "(I)V",
            &[(ranges.len() as jint).into()],
        )?;
        for range in ranges {
            let j_arr = env.new_long_array(2)?;
            env.set_long_array_region(&j_arr, 0, &[range.start as jlong, range.end as jlong])?;
            env.call_method(
                &j_range_list,
                "add",
                "(Ljava/lang/Object;)Z",
                &[(&j_arr).into()],
            )?;
        }
        env.call_method(
            &j_map,
            "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &[(&j_key).into(), (&j_range_list).into()],
        )?;
    }
    env.set_field(
        &j_plan,
        "fragmentRanges",
        "Ljava/util/Map;",
        (&j_map).into(),
    )?;

    // Set scanRangeAfterFilter if present
    if let Some(ref scan_range) = plan.scan_range_after_filter {
        let j_arr = env.new_long_array(2)?;
        env.set_long_array_region(
            &j_arr,
            0,
            &[scan_range.start as jlong, scan_range.end as jlong],
        )?;
        env.set_field(&j_plan, "scanRangeAfterFilter", "[J", (&j_arr).into())?;
    }

    // Encode filters as Substrait — deduplicate by Arc pointer
    let arrow_schema = Arc::new(ArrowSchema::from(dataset_schema));
    let ctx = get_session_context(&LanceExecutionOptions::default());
    let state = ctx.state();

    // Deduplicate filters by Arc pointer identity
    let mut unique_filters: Vec<(*const Expr, Vec<u8>)> = Vec::new();
    let mut filter_index_map: HashMap<u32, usize> = HashMap::new();

    for (frag_id, filter_arc) in &plan.filters {
        let ptr = Arc::as_ptr(filter_arc);
        if let Some(idx) = unique_filters.iter().position(|(p, _)| *p == ptr) {
            filter_index_map.insert(*frag_id, idx);
        } else {
            let encoded =
                encode_substrait(filter_arc.as_ref().clone(), arrow_schema.clone(), &state)?;
            let idx = unique_filters.len();
            unique_filters.push((ptr, encoded));
            filter_index_map.insert(*frag_id, idx);
        }
    }

    // Create List<byte[]> for filterExpressions
    let j_filter_list = env.new_object(
        "java/util/ArrayList",
        "(I)V",
        &[(unique_filters.len() as jint).into()],
    )?;
    for (_, encoded) in &unique_filters {
        let j_bytes = env.byte_array_from_slice(encoded)?;
        env.call_method(
            &j_filter_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[(&j_bytes).into()],
        )?;
    }
    env.set_field(
        &j_plan,
        "filterExpressions",
        "Ljava/util/List;",
        (&j_filter_list).into(),
    )?;

    // Create Map<Integer, Integer> for fragmentFilterIndex
    let j_filter_index_map = env.new_object("java/util/HashMap", "()V", &[])?;
    for (frag_id, idx) in &filter_index_map {
        let j_frag_key =
            env.new_object("java/lang/Integer", "(I)V", &[(*frag_id as jint).into()])?;
        let j_idx_val = env.new_object("java/lang/Integer", "(I)V", &[(*idx as jint).into()])?;
        env.call_method(
            &j_filter_index_map,
            "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &[(&j_frag_key).into(), (&j_idx_val).into()],
        )?;
    }
    env.set_field(
        &j_plan,
        "fragmentFilterIndex",
        "Ljava/util/Map;",
        (&j_filter_index_map).into(),
    )?;

    Ok(j_plan)
}

/////////////////////////////////////
// execute_filtered_read_plan      //
/////////////////////////////////////
#[no_mangle]
pub extern "system" fn Java_org_lance_ipc_LanceScanner_nativeExecuteFilteredReadPlan(
    mut env: JNIEnv,
    j_scanner: JObject,
    j_plan: JObject,
    stream_addr: jlong,
) {
    ok_or_throw_without_return!(
        env,
        inner_execute_filtered_read_plan(&mut env, j_scanner, j_plan, stream_addr)
    );
}

fn inner_execute_filtered_read_plan(
    env: &mut JNIEnv,
    j_scanner: JObject,
    j_plan: JObject,
    stream_addr: jlong,
) -> Result<()> {
    // Get dataset schema from scanner for Substrait decoding
    let dataset_schema = {
        let scanner_guard =
            unsafe { env.get_rust_field::<_, _, BlockingScanner>(&j_scanner, NATIVE_SCANNER) }?;
        scanner_guard.inner.dataset_schema().clone()
    };
    let arrow_schema = Arc::new(ArrowSchema::from(&dataset_schema));
    let ctx = get_session_context(&LanceExecutionOptions::default());
    let state = ctx.state();

    // Read filterExpressions (List<byte[]>)
    let j_filter_exprs = env
        .get_field(&j_plan, "filterExpressions", "Ljava/util/List;")
        .map_err(|e| Error::input_error(format!("Failed to get filterExpressions: {e}")))?
        .l()?;
    let mut decoded_filters: Vec<Expr> = Vec::new();
    if !j_filter_exprs.is_null() {
        let filter_list = env.get_list(&j_filter_exprs)?;
        let size = filter_list.size(env)? as usize;
        decoded_filters.reserve(size);
        let mut iter = filter_list.iter(env)?;
        while let Some(elem) = iter.next(env)? {
            let j_byte_array: JByteArray = elem.into();
            let len = env.get_array_length(&j_byte_array)? as usize;
            let mut buf = vec![0i8; len];
            env.get_byte_array_region(&j_byte_array, 0, &mut buf)?;
            let bytes: Vec<u8> = buf.into_iter().map(|b| b as u8).collect();
            let expr = RT.block_on(parse_substrait(&bytes, arrow_schema.clone(), &state))?;
            decoded_filters.push(expr);
        }
    }

    // Read fragmentFilterIndex (Map<Integer, Integer>)
    let j_filter_index = env
        .get_field(&j_plan, "fragmentFilterIndex", "Ljava/util/Map;")
        .map_err(|e| Error::input_error(format!("Failed to get fragmentFilterIndex: {e}")))?
        .l()?;
    let mut filters: HashMap<u32, Arc<Expr>> = HashMap::new();
    if !j_filter_index.is_null() {
        let j_map = env.get_map(&j_filter_index)?;
        let mut iter = j_map.iter(env)?;
        while let Some((key, value)) = iter.next(env)? {
            let frag_id = env.call_method(&key, "intValue", "()I", &[])?.i()? as u32;
            let idx = env.call_method(&value, "intValue", "()I", &[])?.i()? as usize;
            if idx >= decoded_filters.len() {
                return Err(Error::input_error(format!(
                    "Filter index {idx} out of range (have {} filters)",
                    decoded_filters.len()
                )));
            }
            filters.insert(frag_id, Arc::new(decoded_filters[idx].clone()));
        }
    }

    // Read fragmentRanges (Map<Integer, List<long[]>>)
    let j_frag_ranges = env
        .get_field(&j_plan, "fragmentRanges", "Ljava/util/Map;")
        .map_err(|e| Error::input_error(format!("Failed to get fragmentRanges: {e}")))?
        .l()?;
    let mut rows: BTreeMap<u32, Vec<Range<u64>>> = BTreeMap::new();
    if !j_frag_ranges.is_null() {
        let j_map = env.get_map(&j_frag_ranges)?;
        let mut iter = j_map.iter(env)?;
        while let Some((key, value)) = iter.next(env)? {
            let frag_id = env.call_method(&key, "intValue", "()I", &[])?.i()? as u32;
            let range_list = env.get_list(&value)?;
            let mut ranges = Vec::new();
            let mut range_iter = range_list.iter(env)?;
            while let Some(range_elem) = range_iter.next(env)? {
                let j_arr: JLongArray = range_elem.into();
                let mut buf = [0i64; 2];
                env.get_long_array_region(&j_arr, 0, &mut buf)?;
                ranges.push(buf[0] as u64..buf[1] as u64);
            }
            rows.insert(frag_id, ranges);
        }
    }

    // Read scanRangeAfterFilter (long[] or null)
    let j_scan_range = env
        .get_field(&j_plan, "scanRangeAfterFilter", "[J")
        .map_err(|e| Error::input_error(format!("Failed to get scanRangeAfterFilter: {e}")))?
        .l()?;
    let scan_range_after_filter = if j_scan_range.is_null() {
        None
    } else {
        let mut buf = [0i64; 2];
        let j_scan_arr: JLongArray = j_scan_range.into();
        env.get_long_array_region(&j_scan_arr, 0, &mut buf)?;
        Some(buf[0] as u64..buf[1] as u64)
    };

    // Build the FilteredReadPlan and execute
    let plan = FilteredReadPlan {
        rows,
        filters,
        scan_range_after_filter,
    };

    let record_batch_stream = {
        let scanner_guard =
            unsafe { env.get_rust_field::<_, _, BlockingScanner>(&j_scanner, NATIVE_SCANNER) }?;
        scanner_guard.execute_filtered_read_plan(plan)?
    };
    let ffi_stream = to_ffi_arrow_array_stream(record_batch_stream, RT.handle().clone())?;
    unsafe { std::ptr::write_unaligned(stream_addr as *mut FFI_ArrowArrayStream, ffi_stream) }
    Ok(())
}
