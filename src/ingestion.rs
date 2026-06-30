// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{
    AnyTree, UserKey, UserValue, blob_tree::ingest::BlobIngestion, tree::ingest::Ingestion,
};

/// Unified ingestion builder over `AnyTree`
// Keep zero allocations and direct dispatch; boxing introduces heap indirection and `dyn` adds virtual dispatch.
// Ingestion calls use `&mut self` in tight loops; the active variant is stable and branch prediction makes the match cheap.
// Allowing this lint preserves hot-path performance at the cost of a larger enum size.
#[expect(clippy::large_enum_variant)]
pub enum AnyIngestion<'a> {
    /// Ingestion for a standard LSM-tree
    Standard(Ingestion<'a>),

    /// Ingestion for a [`BlobTree`](crate::BlobTree) with KV separation
    Blob(BlobIngestion<'a>),
}

impl AnyIngestion<'_> {
    /// Writes a key-value pair.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        key: K,
        value: V,
    ) -> crate::Result<()> {
        match self {
            Self::Standard(i) => i.write(key.into(), value.into()),
            Self::Blob(b) => b.write(key.into(), value.into()),
        }
    }

    /// Writes a tombstone for a key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write_tombstone<K: Into<UserKey>>(&mut self, key: K) -> crate::Result<()> {
        match self {
            Self::Standard(i) => i.write_tombstone(key.into()),
            Self::Blob(b) => b.write_tombstone(key.into()),
        }
    }

    /// Writes a weak tombstone for a key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Config;
    /// # let folder = tempfile::tempdir()?;
    /// # let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    /// #
    /// let mut ingestion = tree.ingestion()?;
    /// ingestion.write("a", "abc")?;
    /// ingestion.write_weak_tombstone("b")?;
    /// ingestion.finish()?;
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write_weak_tombstone<K: Into<UserKey>>(&mut self, key: K) -> crate::Result<()> {
        match self {
            Self::Standard(i) => i.write_weak_tombstone(key.into()),
            Self::Blob(b) => b.write_weak_tombstone(key.into()),
        }
    }

    /// Writes a consumer-provided columnar batch (its value sub-columns) as one
    /// columnar block, stored directly without re-transposing the value.
    ///
    /// The batch carries the three intrinsic columns (`[key, seqno, value-type]`)
    /// plus one or more value sub-columns. Its keys must be strictly increasing
    /// (by the tree comparator) within the batch and after any previously written
    /// data, and every per-row seqno must be `0`: [`finish`](Self::finish) assigns
    /// the atomic global sequence number. The columnar layout must be enabled
    /// (`columnar` in the runtime config) on a standard tree; a row-mode or blob
    /// tree rejects the batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch shape is invalid, the keys are not strictly
    /// increasing, any row carries a non-zero seqno, the layout is not columnar,
    /// a block write fails, or the tree is a blob tree (columnar ingest does not
    /// support KV separation).
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::table::columnar::{Column, TypeTag, entries_to_column_batch};
    /// use lsm_tree::{AnyTree, Config, InternalValue, ValueType};
    ///
    /// let folder = tempfile::tempdir()?;
    /// let any = Config::new(folder, Default::default(), Default::default()).open()?;
    /// if let AnyTree::Standard(tree) = &any {
    ///     tree.update_runtime_config(|cfg| cfg.columnar = true)?;
    /// }
    ///
    /// // One row whose value is a single fixed-4 sub-column (id 3).
    /// let mut batch = entries_to_column_batch(&[InternalValue::from_components(
    ///     b"k0".to_vec(),
    ///     b"x".to_vec(),
    ///     0,
    ///     ValueType::Value,
    /// )])?;
    /// batch.columns.pop();
    /// batch.columns.push(Column {
    ///     column_id: 3,
    ///     type_tag: TypeTag::Fixed(4),
    ///     validity: None,
    ///     data: vec![1, 0, 0, 0],
    /// });
    ///
    /// let mut ingestion = any.ingestion()?;
    /// ingestion.write_columnar_batch(&batch)?;
    /// ingestion.finish()?;
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    #[cfg(feature = "columnar")]
    pub fn write_columnar_batch(
        &mut self,
        batch: &crate::table::columnar::ColumnBatch,
    ) -> crate::Result<()> {
        match self {
            Self::Standard(i) => i.write_columnar_batch(batch),
            Self::Blob(_) => Err(crate::Error::FeatureUnsupported(
                "columnar batch ingest is not supported for blob trees",
            )),
        }
    }

    /// Finalizes ingestion and registers created tables (and blob files if present).
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn finish(self) -> crate::Result<()> {
        match self {
            Self::Standard(i) => i.finish(),
            Self::Blob(b) => b.finish(),
        }
    }
}

impl AnyTree {
    /// Starts an ingestion for any tree type (standard or blob).
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn ingestion(&self) -> crate::Result<AnyIngestion<'_>> {
        match self {
            Self::Standard(t) => Ok(AnyIngestion::Standard(Ingestion::new(t)?)),
            Self::Blob(b) => Ok(AnyIngestion::Blob(BlobIngestion::new(b)?)),
        }
    }

    /// Runs a projected columnar scan across the tree.
    ///
    /// Delegates to [`Tree::columnar_scan`](crate::Tree::columnar_scan) on a
    /// standard tree: iterates the columnar segments intersecting `range` and
    /// visible at `seqno`, applies each segment's positional delete-bitmap and the
    /// optional `predicate`, and yields projected
    /// [`ColumnBatch`](crate::table::columnar::ColumnBatch)es in key order,
    /// merging overlapping segments newest-seqno-wins. See
    /// [`Tree::columnar_scan`](crate::Tree::columnar_scan) for the full contract.
    ///
    /// # Errors
    ///
    /// Returns an error if the tree is a blob tree (columnar scan does not support
    /// KV separation), if a visible non-columnar segment overlaps `range`, or —
    /// lazily, while iterating — on a block read / decode failure.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::table::columnar::{Column, TypeTag, entries_to_column_batch};
    /// use lsm_tree::{AnyTree, Config, InternalValue, SeqNo, ValueType};
    ///
    /// let folder = tempfile::tempdir()?;
    /// let any = Config::new(folder, Default::default(), Default::default()).open()?;
    /// if let AnyTree::Standard(tree) = &any {
    ///     tree.update_runtime_config(|cfg| cfg.columnar = true)?;
    /// }
    ///
    /// // Ingest one row whose value is a single fixed-4 sub-column (id 3).
    /// let mut batch = entries_to_column_batch(&[InternalValue::from_components(
    ///     b"k0".to_vec(),
    ///     b"x".to_vec(),
    ///     0,
    ///     ValueType::Value,
    /// )])?;
    /// batch.columns.pop();
    /// batch.columns.push(Column {
    ///     column_id: 3,
    ///     type_tag: TypeTag::Fixed(4),
    ///     validity: None,
    ///     data: vec![1, 0, 0, 0],
    /// });
    /// let mut ingestion = any.ingestion()?;
    /// ingestion.write_columnar_batch(&batch)?;
    /// ingestion.finish()?;
    ///
    /// // Scan projecting only sub-column 3 across the whole tree.
    /// let mut rows = 0;
    /// for batch in any.columnar_scan(&[3], None, SeqNo::MAX, ..)? {
    ///     rows += batch?.row_count;
    /// }
    /// assert_eq!(rows, 1);
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    #[cfg(feature = "columnar")]
    pub fn columnar_scan<R: core::ops::RangeBounds<crate::UserKey>>(
        &self,
        projection: &[u16],
        predicate: Option<&crate::table::columnar_predicate::ColumnRangePredicate>,
        seqno: crate::SeqNo,
        range: R,
    ) -> crate::Result<crate::tree::columnar_scan::ColumnarScan> {
        match self {
            Self::Standard(t) => t.columnar_scan(projection, predicate, seqno, range),
            Self::Blob(_) => Err(crate::Error::FeatureUnsupported(
                "columnar scan is not supported for blob trees",
            )),
        }
    }
}
