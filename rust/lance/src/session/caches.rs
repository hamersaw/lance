// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Caches for Lance datasets. They are organized in a hierarchical manner to
//! avoid collisions.
//!
//!  GlobalMetadataCache
//!     │
//!     ├─► DSMetadataCache (prefixed by dataset URI)
//!     │    │
//!     └────┴──► FileMetadataCache (prefixed by file path)

use std::{
    borrow::Cow,
    ops::Deref,
    sync::{Arc, RwLock},
};

use deepsize::{Context, DeepSizeOf};
use lance_core::{
    cache::{CacheKey, LanceCache},
    utils::{deletion::DeletionVector, mask::RowAddrMask},
};
use lance_table::{
    format::{DeletionFile, Manifest},
    rowids::{FragmentRowIdIndex, RowIdIndex, RowIdSequence},
};
use object_store::path::Path;
use roaring::RoaringBitmap;

use crate::dataset::transaction::Transaction;

/// A type-safe wrapper around a LanceCache that enforces namespaces for dataset metadata.
pub struct GlobalMetadataCache(pub(super) LanceCache);

impl GlobalMetadataCache {
    pub fn for_dataset(&self, uri: &str) -> DSMetadataCache {
        // Create a sub-cache for the dataset by adding the URI as a key prefix.
        // This prevents collisions between different datasets.
        DSMetadataCache(self.0.with_key_prefix(uri))
    }
}

impl Clone for GlobalMetadataCache {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl DeepSizeOf for GlobalMetadataCache {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.0.deep_size_of_children(context)
    }
}

/// A type-safe wrapper around a LanceCache that enforces namespaces and keys
/// for dataset metadata.
pub struct DSMetadataCache(pub(crate) LanceCache);

impl Deref for DSMetadataCache {
    type Target = LanceCache;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Cache key types for type-safe cache access
#[derive(Debug)]
pub struct ManifestKey<'a> {
    pub version: u64,
    pub e_tag: Option<&'a str>,
}

impl CacheKey for ManifestKey<'_> {
    type ValueType = Manifest;
    fn key(&self) -> Cow<'_, str> {
        if let Some(e_tag) = self.e_tag {
            Cow::Owned(format!("manifest/{}/{}", self.version, e_tag))
        } else {
            Cow::Owned(format!("manifest/{}", self.version))
        }
    }
    fn type_name() -> &'static str {
        "Manifest"
    }
}

#[derive(Debug)]
pub struct TransactionKey {
    pub version: u64,
}

impl CacheKey for TransactionKey {
    type ValueType = Transaction;
    fn key(&self) -> Cow<'_, str> {
        Cow::Owned(format!("txn/{}", self.version))
    }
    fn type_name() -> &'static str {
        "Transaction"
    }
}

#[derive(Debug)]
pub struct DeletionFileKey<'a> {
    pub fragment_id: u64,
    pub deletion_file: &'a DeletionFile,
}

impl CacheKey for DeletionFileKey<'_> {
    type ValueType = DeletionVector;
    fn key(&self) -> Cow<'_, str> {
        Cow::Owned(format!(
            "deletion/{}/{}/{}/{}",
            self.fragment_id,
            self.deletion_file.read_version,
            self.deletion_file.id,
            self.deletion_file.file_type.suffix()
        ))
    }
    fn type_name() -> &'static str {
        "DeletionVector"
    }
}

#[derive(Debug)]
pub struct RowAddrMaskKey {
    pub version: u64,
}

impl CacheKey for RowAddrMaskKey {
    type ValueType = RowAddrMask;
    fn key(&self) -> Cow<'_, str> {
        Cow::Owned(format!("row_addr_mask/{}", self.version))
    }
    fn type_name() -> &'static str {
        "RowAddrMask"
    }
}

#[derive(Debug)]
pub struct RowIdSequenceKey {
    pub fragment_id: u64,
}

impl CacheKey for RowIdSequenceKey {
    type ValueType = RowIdSequence;
    fn key(&self) -> Cow<'_, str> {
        Cow::Owned(format!("row_id_sequence/{}", self.fragment_id))
    }
    fn type_name() -> &'static str {
        "RowIdSequence"
    }
}

/// One growing assembled `RowIdIndex` per manifest version.
///
/// `index` spans exactly the fragments in `covered`. `fragment_indices`
/// retains the (Arc'd, already-parsed) inputs so the index can be re-merged
/// when more fragments are loaded, without re-parsing any sequence or DV.
/// The lazy take path extends this lazily; the scan path extends it to full
/// coverage — they share one slot and warm each other.
pub struct Assembled {
    pub fragment_indices: Vec<FragmentRowIdIndex>,
    pub index: Arc<RowIdIndex>,
    pub covered: RoaringBitmap,
}

impl Assembled {
    pub fn empty() -> Self {
        Self {
            fragment_indices: Vec::new(),
            // An empty index resolves every id to `None`, which the lazy
            // path correctly treats as "not yet covered" and falls through.
            index: Arc::new(RowIdIndex::empty()),
            covered: RoaringBitmap::new(),
        }
    }
}

impl DeepSizeOf for Assembled {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.fragment_indices.deep_size_of_children(context)
            + self.index.deep_size_of_children(context)
            // RoaringBitmap has no DeepSizeOf impl; mirror DeletionVector.
            + self.covered.serialized_size()
    }
}

/// Cache key for the per-version assembled `RowIdIndex` slot. Keyed by
/// manifest version only: a DV is mutable across versions but immutable
/// within one, so each version gets its own slot and a new deletion file
/// never aliases a stale baked-in DV.
#[derive(Debug)]
pub struct RowIdIndexKey {
    pub version: u64,
}

impl CacheKey for RowIdIndexKey {
    type ValueType = RwLock<Assembled>;
    fn key(&self) -> Cow<'_, str> {
        Cow::Owned(format!("row_id_index/{}", self.version))
    }
    fn type_name() -> &'static str {
        "RowIdIndex"
    }
}

impl DSMetadataCache {
    /// Create a file-specific metadata cache with the given prefix.
    /// This is used by file readers and other components that need file-level caching.
    pub(crate) fn file_metadata_cache(&self, prefix: &Path) -> LanceCache {
        self.0.with_key_prefix(prefix.as_ref())
    }
}
