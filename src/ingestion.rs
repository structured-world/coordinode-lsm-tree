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
    /// plus one or more value sub-columns; its keys must be sorted and order
    /// after any previously written data. The per-row seqnos are typically `0`:
    /// [`finish`](Self::finish) assigns the atomic global sequence number. The
    /// columnar layout must be enabled (`columnar` in the runtime config) on a
    /// standard tree; a row-mode or blob tree rejects the batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch shape is invalid, the layout is not
    /// columnar, a block write fails, or the tree is a blob tree (columnar
    /// ingest does not support KV separation).
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
}
