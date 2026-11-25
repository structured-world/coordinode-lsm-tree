// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::{
    SeqNo, UserKey, UserValue, file::BLOBS_FOLDER, tree::ingest::Ingestion as TableIngestion,
    vlog::BlobFileWriter,
};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::cmp::Ordering;

/// Bulk ingestion for [`BlobTree`](crate::BlobTree)
///
/// Items NEED to be added in ascending key order.
///
/// Uses table ingestion for the index and a blob file writer for large
/// values so both streams advance together.
pub struct BlobIngestion<'a> {
    tree: &'a crate::BlobTree,
    pub(crate) table: TableIngestion<'a>,
    pub(crate) blob: BlobFileWriter,
    seqno: SeqNo,
    separation_threshold: u32,
    /// Separation thresholds of a cell row's fields, by column; see
    /// [`KvSeparationOptions::cell_separation_threshold`](crate::KvSeparationOptions::cell_separation_threshold).
    cell_thresholds: Vec<(u16, u32)>,
    /// Picks the lifetime class of each separated value.
    lifetime: crate::config::blob_lifetime::LifetimeClassifier<'a>,
    last_key: Option<UserKey>,
}

impl<'a> BlobIngestion<'a> {
    /// Creates a new ingestion.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn new(tree: &'a crate::BlobTree) -> crate::Result<Self> {
        #[expect(
            clippy::expect_used,
            reason = "cannot define blob tree without kv separation options"
        )]
        let kv = tree
            .index
            .config
            .kv_separation_opts
            .as_ref()
            .expect("kv separation options should exist");

        let blob_file_size = kv.file_target_size;

        // One snapshot for the whole ingestion, blob files and index tables
        // alike: an ingestion is one batch of files, written under one policy.
        // The table ingestion holds it until `finish` has installed both, so a
        // collection cannot take a dictionary it names in between.
        let rc = tree.index.0.runtime_config.load_full();
        let blob_compression = rc.blob_compression;

        let table = TableIngestion::with_runtime_snapshot(&tree.index, &rc)?;
        let blob = BlobFileWriter::new_for(
            tree.index.0.blob_file_id_counter.clone(),
            tree.index.config.path.join(BLOBS_FOLDER),
            tree.index.id,
            tree.index.config.descriptor_table.clone(),
            tree.index.config.fs.clone(),
            crate::fs::WriteOrigin::IngestBlob,
        )?
        .use_target_size(blob_file_size)
        .use_compression(blob_compression)
        .use_sync_mode(tree.index.config.sync_mode)
        .use_writeback_bytes(tree.index.config.writeback_bytes)
        .use_lifetime_groups(kv.lifetime_groups)
        .use_frame_sizes(kv.frame_sizes())
        .use_dictionary_classes(kv.dictionary_classes)
        .use_clock(tree.index.config.clock.clone())
        .use_key_domains(
            tree.index.config.key_domains.clone(),
            tree.index.config.comparator.clone(),
        );

        // Ingestion writes blob files under the tree's own blob policy, so it
        // needs the dictionary to compress with and the set to pin on what it
        // produces. Without the first, a `ZstdDict` policy fails the write.
        #[cfg(zstd_any)]
        let blob = {
            let dicts = tree.index.config.current_zstd_dictionaries();
            blob.use_zstd_dictionary(dicts.for_compression(blob_compression)?)
                .use_zstd_two_pass_seed(rc.zstd_two_pass_seed)
                .use_zstd_dictionaries(dicts)
                // A zstd file builds a dictionary of its own, as a flush's does.
                .use_file_dictionary(kv.file_dictionary)
        };
        #[cfg(feature = "std")]
        let blob = blob.use_parallel_compression(tree.index.config.parallel_compression());

        let separation_threshold = kv.separation_threshold;
        let cell_thresholds = kv.cell_separation_thresholds.clone();

        Ok(Self {
            tree,
            table,
            blob,
            seqno: 0,
            separation_threshold,
            cell_thresholds,
            lifetime: kv.lifetime_classifier(),
            last_key: None,
        })
    }

    /// Writes a key-value pair.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write(&mut self, key: UserKey, value: UserValue) -> crate::Result<()> {
        // Check order before any blob I/O to avoid partial writes on failure
        if let Some(prev) = &self.last_key {
            assert!(
                self.tree.index.config.comparator.compare(prev, &key) == Ordering::Less,
                "next key in ingestion must be ordered after last key by configured comparator"
            );
        }

        #[expect(clippy::cast_possible_truncation)]
        let value_size = value.len() as u32;

        if value_size >= self.separation_threshold {
            // The table ingestion takes in the indirection only, so the value
            // is charged here.
            #[cfg(feature = "metrics")]
            self.table.accept(1, (key.len() + value.len()) as u64);
            // An ingested key has a single version: nothing overwrote it.
            self.blob
                .select_lifetime_class(self.lifetime.class_of(&key, false))?;
            let indirection = self.blob.write(&key, self.seqno, &value)?;

            let cloned_key = key.clone();
            let res = self.table.write_indirection(key, indirection);
            if res.is_ok() {
                self.last_key = Some(cloned_key);
            }
            res
        } else {
            let cloned_key = key.clone();
            let res = self.table.write(key, value);
            if res.is_ok() {
                self.last_key = Some(cloned_key);
            }
            res
        }
    }

    /// Writes `key` as a row of value fields, as
    /// [`BlobTree::insert_cells`](crate::BlobTree::insert_cells) writes one:
    /// each field at or above its column's separation threshold goes to a
    /// blob file on its own, and the row keeps a reference to it.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::BlobRef`] for a
    /// [`Cell::Ref`](crate::blob_tree::field_row::Cell::Ref) field (an
    /// ingested key has no earlier version to keep an object of),
    /// [`crate::Error::CellRow`] if two fields share a column or a
    /// fixed-width field is not its type's width, and an error if an IO error
    /// occurs, the row has more than `u16::MAX` fields or a field exceeds
    /// `u32::MAX` bytes.
    pub fn write_cells(
        &mut self,
        key: UserKey,
        fields: &[crate::blob_tree::field_row::Field<'_>],
    ) -> crate::Result<()> {
        use crate::blob_tree::field_row::{
            Cell, RowCell, RowField, encode_row, order_fields, separate_row,
        };

        // Check order before any blob I/O to avoid partial writes on failure
        if let Some(prev) = &self.last_key {
            assert!(
                self.tree.index.config.comparator.compare(prev, &key) == Ordering::Less,
                "next key in ingestion must be ordered after last key by configured comparator"
            );
        }

        let mut row = Vec::with_capacity(fields.len());
        #[cfg(feature = "metrics")]
        let mut payload = key.len();
        for field in fields {
            let Cell::Value(bytes) = field.cell else {
                return Err(crate::Error::BlobRef("a reference in an ingested row"));
            };
            #[cfg(feature = "metrics")]
            {
                payload += bytes.len();
            }
            row.push(RowField {
                column: field.column,
                tag: field.tag,
                cell: RowCell::Value(bytes),
            });
        }
        order_fields(&mut row)?;
        let row = encode_row(&row)?;
        #[cfg(feature = "metrics")]
        self.table.accept(1, payload as u64);
        let (thresholds, default) = (&self.cell_thresholds, self.separation_threshold);
        let (blob, seqno) = (&mut self.blob, self.seqno);
        let class = self.lifetime.class_of(&key, false);
        let row = separate_row(
            &row,
            |column| crate::config::cell_threshold(thresholds, default, column),
            |bytes| {
                // Selected per separated field, so a row that separates
                // nothing opens no file for its class.
                blob.select_lifetime_class(class)?;
                blob.write(&key, seqno, bytes)
            },
        )?
        .unwrap_or(row);

        let cloned_key = key.clone();
        let res = self.table.write_cell_row(key, &UserValue::from(row));
        if res.is_ok() {
            self.last_key = Some(cloned_key);
        }
        res
    }

    /// Writes a tombstone for a key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write_tombstone(&mut self, key: UserKey) -> crate::Result<()> {
        if let Some(prev) = &self.last_key {
            assert!(
                self.tree.index.config.comparator.compare(prev, &key) == Ordering::Less,
                "next key in ingestion must be ordered after last key by configured comparator"
            );
        }

        let cloned_key = key.clone();
        let res = self.table.write_tombstone(key);
        if res.is_ok() {
            self.last_key = Some(cloned_key);
        }
        res
    }

    /// Writes a weak tombstone for a key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write_weak_tombstone(&mut self, key: UserKey) -> crate::Result<()> {
        if let Some(prev) = &self.last_key {
            assert!(
                self.tree.index.config.comparator.compare(prev, &key) == Ordering::Less,
                "next key in ingestion must be ordered after last key by configured comparator"
            );
        }

        let cloned_key = key.clone();
        let res = self.table.write_weak_tombstone(key);
        if res.is_ok() {
            self.last_key = Some(cloned_key);
        }
        res
    }

    /// Finishes the ingestion.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[allow(clippy::significant_drop_tightening)]
    pub fn finish(mut self) -> crate::Result<()> {
        use crate::AbstractTree;

        let index = self.index().clone();

        // CRITICAL SECTION: Atomic flush + seqno allocation + registration
        //
        // For BlobTree, we must coordinate THREE components atomically:
        //   1. Index tree memtable flush
        //   2. Value log blob files
        //   3. Index tree tables (with blob indirections)
        //
        // The sequence ensures all components see the same global_seqno:
        //   1. Acquire flush lock on index tree
        //   2. Flush index tree active memtable
        //   3. Finalize blob writer (creates blob files)
        //   4. Finalize table writer (creates index tables)
        //   5. Allocate next global seqno
        //   6. Recover tables with that seqno
        //   7. Register version with same seqno + blob files
        //
        // This prevents race conditions where blob files and their index
        // entries could have mismatched sequence numbers.
        let flush_lock = index.get_flush_lock();

        // Flush any pending index memtable writes to ensure ingestion sees
        // a consistent snapshot of the index.
        // We call rotate + flush directly because we already hold the lock.
        index.rotate_memtable();
        index.flush(&flush_lock, 0)?;

        // The hold on the ingestion's dictionaries stays here until the files
        // are installed.
        let write_pin = core::mem::take(&mut self.table.write_pin);

        // Finalize the blob writer first, ensuring all large values are
        // written to blob files before we finalize the index tables that
        // reference them.
        let blob_files = self.blob.finish()?;

        // Finalize the table writer, creating index tables with blob
        // indirections pointing to the blob files we just created.
        let written = self.table.writer.finish()?;
        let entries_durable = written.entries_durable;

        #[cfg(all(test, feature = "std"))]
        index.config.fire_before_output_install();

        // Acquire locks for version registration on the index tree. We must
        // hold both the compaction state lock and version history lock to
        // safely modify the tree's version.
        let mut _compaction_state = index.compaction_state.lock();
        let mut version_lock = index.version_history.write();

        // Allocate the next global sequence number. This seqno will be shared
        // by all ingested tables, blob files, and the version that registers
        // them, ensuring consistent MVCC snapshots across the value log.
        let global_seqno = index.config.seqno.next();
        // L0 recency from the install, as for a table ingestion (see
        // `Ingestion::finish`): the ids the tables were written under predate
        // every table installed while this ingestion ran.
        let recency = index.table_id_counter.next();

        // Recover all created index tables, assigning them the global_seqno
        // we just allocated. These tables contain indirections to the blob
        // files created above, so they must share the same sequence number
        // for MVCC correctness.
        let created_tables = TableIngestion::recover_run(
            &index,
            &self.table.folder,
            &self.table.level_fs,
            written.tables,
            global_seqno,
            recency,
            entries_durable,
        )?;
        bind_blob_files(&index, &blob_files);

        // Upgrade the version with our ingested tables and blob files, using
        // the global_seqno we allocated earlier. This ensures the version,
        // tables, and blob files all share the same sequence number, which is
        // critical for GC correctness - we must not delete blob files that are
        // still referenced by visible snapshots.
        //
        // We use upgrade_version_with_seqno (instead of upgrade_version) because
        // we need precise control over the seqno: it must match the seqno we
        // already assigned to the recovered tables.
        let mut installed = crate::tree::InstalledCharge::default();
        installed.add(
            &created_tables,
            &blob_files,
            crate::fs::WriteOrigin::IngestTable,
            crate::fs::WriteOrigin::IngestBlob,
        )?;
        version_lock.upgrade_version_with_seqno(
            &index.config.path,
            |current| {
                let mut copy = current.clone();
                let ctx = crate::version::TransformContext::new(index.config.comparator.as_ref());
                copy.version =
                    copy.version
                        .with_new_l0_run(&created_tables, Some(&blob_files), None, &ctx);
                Ok(copy)
            },
            global_seqno,
            &self.tree.index.config.visible_seqno,
            &*self.tree.index.config.fs,
            self.tree.index.0.runtime_config.load_full(),
            self.tree.index.0.config.encryption.clone(),
            // Ingestion only adds tables and blob files: older snapshots keep
            // everything.
            crate::version::RetentionEffect::Keep,
        )?;
        installed.charge();
        // The installed files name their dictionaries from here on.
        drop(write_pin);

        Ok(())
    }

    /// Finishes the ingestion by replacing the tree's whole content, index
    /// tables and blob files alike, with what it wrote, in one version
    /// install: see [`AnyIngestion::finish_replacing`](crate::AnyIngestion::finish_replacing)
    /// for what a reader, a later write and a crash see. The earlier blob
    /// files go with the earlier tables, once nothing holds them.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs; the tree then keeps its
    /// earlier content.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::{AbstractTree, AnyTree, Config, KvSeparationOptions, SeqNo};
    ///
    /// let folder = tempfile::tempdir()?;
    /// let AnyTree::Blob(tree) = Config::new(folder, Default::default(), Default::default())
    ///     .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    ///     .open()?
    /// else {
    ///     panic!("a key-value separated config opens a blob tree");
    /// };
    /// tree.insert("old", "gone", 0);
    ///
    /// let mut ingestion = lsm_tree::blob_tree::ingest::BlobIngestion::new(&tree)?;
    /// ingestion.write("a".into(), "1".into())?;
    /// ingestion.finish_replacing()?;
    ///
    /// assert!(tree.get("old", SeqNo::MAX)?.is_none());
    /// assert_eq!(tree.get("a", SeqNo::MAX)?.as_deref(), Some(b"1".as_slice()));
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    pub fn finish_replacing(mut self) -> crate::Result<()> {
        use crate::AbstractTree;

        let index = self.index().clone();
        // The hold on the ingestion's dictionaries stays here until its files
        // are installed.
        let write_pin = core::mem::take(&mut self.table.write_pin);
        let written = if self.last_key.is_some() {
            // The blob files first, which the index tables point into.
            let blob_files = self.blob.finish()?;
            let written = self.table.writer.finish()?;
            Some((written.tables, written.entries_durable, blob_files))
        } else {
            None
        };

        #[cfg(all(test, feature = "std"))]
        index.config.fire_before_output_install();

        let prior = {
            // The locks of an ingestion's install, as for a table ingestion's
            // replacing finish: the memtables are not flushed, their content is
            // what this install drops.
            let _flush_lock = index.get_flush_lock();
            let _compaction_state = index.compaction_state.lock();
            let mut versions = index.version_history.write();

            let (tables, blob_files) = match written {
                Some((tables, entries_durable, blob_files)) => {
                    let recency = index.table_id_counter.next();
                    let tables = TableIngestion::recover_run(
                        &index,
                        &self.table.folder,
                        &self.table.level_fs,
                        tables,
                        0,
                        recency,
                        entries_durable,
                    )?;
                    bind_blob_files(&index, &blob_files);
                    (tables, blob_files)
                }
                None => (Vec::new(), Vec::new()),
            };
            let mut installed = crate::tree::InstalledCharge::default();
            installed.add(
                &tables,
                &blob_files,
                crate::fs::WriteOrigin::IngestTable,
                crate::fs::WriteOrigin::IngestBlob,
            )?;
            let prior = index.install_replacing(&mut versions, &tables, &blob_files)?;
            drop(versions);
            installed.charge();
            prior
        };
        // The installed files name their dictionaries from here on.
        drop(write_pin);
        crate::tree::reclaim_replaced(&prior);
        Ok(())
    }

    #[inline]
    fn index(&self) -> &crate::Tree {
        &self.tree.index
    }
}

/// Binds the blob files an ingestion wrote to the tree BEFORE the version edit
/// makes them reachable, as its tables are: an unbound file's `Drop` can
/// unlink it while a checkpoint is capturing, and a tight-space prefix punch
/// can zero bytes the checkpoint has already hard-linked.
fn bind_blob_files(index: &crate::Tree, blob_files: &[crate::BlobFile]) {
    for blob_file in blob_files {
        blob_file.bind_to_tree(&crate::table::TableSinks {
            deletion_pause: &index.deletion_pause,
            heal_hints: &index.heal_hints,
            read_budget: index.config.columnar_read_budget,
            #[cfg(feature = "std")]
            background_deleter: &index.background_deleter,
            // A blob file has no filter.
            track_filter_probes: false,
        });
    }
}

#[cfg(test)]
mod tests;
