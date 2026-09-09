// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::writer::Writer;
use crate::fs::FsFile;
use crate::path::{Path, PathBuf};
use crate::{
    BlobFile, CompressionType, DescriptorTable, SeqNo, SequenceNumberCounter, TreeId,
    file_accessor::FileAccessor,
    fs::{Fs, SyncMode},
    vlog::{
        ValueHandle,
        blob_file::{Inner as BlobFileInner, Metadata},
    },
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{string::ToString, vec::Vec};
use core::sync::atomic::AtomicBool;

#[cfg(test)]
mod tests;

/// Blob file writer, may write multiple blob files
pub struct MultiWriter {
    fs: Arc<dyn Fs>,

    folder: PathBuf,
    target_size: u64,

    active_writer: Writer,

    results: Vec<BlobFile>,

    id_generator: SequenceNumberCounter,

    compression: CompressionType,
    passthrough_compression: CompressionType,

    /// Durability level wired from `Config::sync_mode`, stamped on every
    /// rotated blob writer.
    sync_mode: SyncMode,

    /// Dictionary for `ZstdDict` compression, shared across all rotated writers.
    #[cfg(zstd_any)]
    zstd_dictionary: Option<alloc::sync::Arc<crate::compression::ZstdDictionary>>,

    /// The tree's dictionary set, used to PIN the dictionary each finished file
    /// records on its handle (see `blob_file::Inner::zstd_dictionary`).
    ///
    /// Not the same question as [`Self::zstd_dictionary`], which is what this
    /// writer COMPRESSES with. A relocation pass compresses with nothing (it
    /// copies frames verbatim) yet still records a codec, so the file it
    /// produces needs a dictionary its own descriptor names and the write slot
    /// cannot answer.
    #[cfg(zstd_any)]
    zstd_dictionaries: crate::compression::ZstdDictionaries,

    tree_id: TreeId,
    descriptor_table: Option<Arc<DescriptorTable>>,
}

impl MultiWriter {
    /// Initializes a new blob file writer.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn new<P: AsRef<Path>>(
        id_generator: SequenceNumberCounter,
        folder: P,
        tree_id: TreeId,
        descriptor_table: Option<Arc<DescriptorTable>>,
        fs: Arc<dyn Fs>,
    ) -> crate::Result<Self> {
        let folder = folder.as_ref();

        let blob_file_id = id_generator.next();
        let blob_file_path = folder.join(blob_file_id.to_string());

        Ok(Self {
            id_generator,
            folder: folder.into(),
            target_size: 64 * 1_024 * 1_024,

            active_writer: Writer::new(blob_file_path, blob_file_id, tree_id, &*fs)?,

            results: Vec::new(),

            compression: CompressionType::None,
            passthrough_compression: CompressionType::None,
            sync_mode: SyncMode::Normal,

            #[cfg(zstd_any)]
            zstd_dictionary: None,
            #[cfg(zstd_any)]
            zstd_dictionaries: crate::compression::ZstdDictionaries::new(),

            tree_id,
            descriptor_table,
            fs,
        })
    }

    /// Wires the tree's `Config::sync_mode` through to every blob file this
    /// writer finalizes (active + rotated).
    #[must_use]
    pub fn use_sync_mode(mut self, sync_mode: SyncMode) -> Self {
        self.sync_mode = sync_mode;
        self.active_writer.sync_mode = sync_mode;
        self
    }

    /// Sets the blob file target size.
    #[must_use]
    pub fn use_target_size(mut self, bytes: u64) -> Self {
        self.target_size = bytes;
        self
    }

    /// Sets the compression method in blob file writer metadata, but does not actually compress blobs.
    ///
    /// This is used in garbage collection to pass through already-compressed blobs, but correctly
    /// set the compression type in the metadata.
    pub(crate) fn use_passthrough_compression(mut self, compression: CompressionType) -> Self {
        assert_eq!(self.compression, CompressionType::None);
        self.passthrough_compression = compression;
        // The bytes handed to `write_raw` are already compressed, so the writer
        // keeps `compression = None` (no re-compress) but must RECORD the real
        // codec in the file metadata or a reopened reader cannot decode it.
        self.active_writer.metadata_compression_override = Some(compression);
        self
    }

    /// Records `compression` on the file being filled, rotating first when a
    /// frame already in it claims a different one.
    ///
    /// The relocation counterpart of [`Self::use_passthrough_compression`],
    /// which fixes one codec for the whole pass. A relocation copies frames
    /// VERBATIM out of sources that need not share a codec (the blob policy may
    /// have moved since they were written), while a blob file records exactly
    /// one. Rotating on that boundary is what keeps every output file's
    /// descriptor true of every frame in it.
    ///
    /// # Errors
    ///
    /// Propagates the rotation's finish of the file being closed.
    pub(crate) fn record_source_compression(
        &mut self,
        compression: CompressionType,
    ) -> crate::Result<()> {
        if self.passthrough_compression == compression {
            return Ok(());
        }
        // Only what is already written constrains the codec; an untouched
        // writer can simply be restamped.
        if self.active_writer.item_count > 0 {
            self.rotate()?;
        }
        self.passthrough_compression = compression;
        self.active_writer.metadata_compression_override = Some(compression);
        Ok(())
    }

    /// Sets the compression method.
    #[must_use]
    #[doc(hidden)]
    pub fn use_compression(mut self, compression: CompressionType) -> Self {
        self.compression.clone_from(&compression);
        self.active_writer.compression = compression;
        self
    }

    /// Provides the zstd dictionary for [`CompressionType::ZstdDict`] writes.
    ///
    /// The dictionary is propagated to every rotated writer so that all blob
    /// files produced by this `MultiWriter` use the same dictionary.
    #[cfg(zstd_any)]
    #[must_use]
    pub fn use_zstd_dictionary(
        mut self,
        dict: Option<alloc::sync::Arc<crate::compression::ZstdDictionary>>,
    ) -> Self {
        self.active_writer = self.active_writer.use_zstd_dictionary(dict.clone());
        self.zstd_dictionary = dict;
        self
    }

    /// Provides the tree's dictionary set, so each finished file can pin the
    /// dictionary its own recorded descriptor names.
    ///
    /// Required wherever the files this writer produces are `ZstdDict`, whether
    /// this writer compresses them itself or passes already-compressed frames
    /// through: the handle a reader gets must carry its own dictionary.
    #[cfg(zstd_any)]
    #[must_use]
    pub fn use_zstd_dictionaries(mut self, dicts: crate::compression::ZstdDictionaries) -> Self {
        self.zstd_dictionaries = dicts;
        self
    }

    /// Sets up a new writer for the next blob file.
    fn rotate(&mut self) -> crate::Result<()> {
        log::debug!("Rotating blob file writer");

        let new_blob_file_id = self.id_generator.next();
        let blob_file_path = self.folder.join(new_blob_file_id.to_string());

        let new_writer = {
            let mut w = Writer::new(blob_file_path, new_blob_file_id, self.tree_id, &*self.fs)?
                .use_compression(self.compression)
                .use_sync_mode(self.sync_mode);
            // Carry the passthrough metadata codec onto each rotated writer so
            // every file in a relocation records the real compression.
            if self.passthrough_compression != CompressionType::None {
                w.metadata_compression_override = Some(self.passthrough_compression);
            }
            #[cfg(zstd_any)]
            let w = w.use_zstd_dictionary(self.zstd_dictionary.clone());
            w
        };

        let old_writer = core::mem::replace(&mut self.active_writer, new_writer);
        let blob_file = Self::consume_writer(
            old_writer,
            self.passthrough_compression,
            self.descriptor_table.clone(),
            &self.fs,
            #[cfg(zstd_any)]
            &self.zstd_dictionaries,
        )?;
        self.results.extend(blob_file);

        Ok(())
    }

    fn consume_writer(
        writer: Writer,
        passthrough_compression: CompressionType,
        descriptor_table: Option<Arc<DescriptorTable>>,
        fs: &Arc<dyn Fs>,
        #[cfg(zstd_any)] zstd_dictionaries: &crate::compression::ZstdDictionaries,
    ) -> crate::Result<Option<BlobFile>> {
        if writer.item_count > 0 {
            let blob_file_id = writer.blob_file_id;
            let path = writer.path.clone();

            log::debug!(
                "Created blob file #{blob_file_id:?} ({} items, {} userdata bytes)",
                writer.item_count,
                writer.uncompressed_bytes,
            );

            let tree_id = writer.tree_id;
            // Taken before `finish` consumes the writer.
            #[cfg(zstd_any)]
            let writer_dictionary = writer.zstd_dictionary.clone();

            let (metadata, checksum) = writer.finish()?;

            // What the FILE will record, which is what its reader resolves
            // against: the passthrough codec when relocation is stamping the
            // source's own, else what this writer compressed with.
            let recorded_compression = if passthrough_compression == CompressionType::None {
                metadata.compression
            } else {
                passthrough_compression
            };

            // Resolved BEFORE the file is opened and its descriptor published,
            // because those are side effects only the finished handle's `Drop`
            // knows how to undo. Failing between them would strand both: an
            // orphan file no version names, and an open descriptor in the
            // shared table for the life of the process.
            //
            // The set answers first, then the dictionary this writer compressed
            // with when THAT is the one the file records: a writer holding the
            // matching dictionary needs no set to pin it, since it just used
            // those bytes. The set is what answers for a relocation, which
            // compresses nothing and records the source's codec.
            #[cfg(zstd_any)]
            let zstd_dictionary = match zstd_dictionaries.for_compression(recorded_compression) {
                Ok(dict) => dict,
                Err(e) => match (&recorded_compression, &writer_dictionary) {
                    (CompressionType::ZstdDict { dict_id, .. }, Some(d)) if d.id() == *dict_id => {
                        Some(alloc::sync::Arc::clone(d))
                    }
                    _ => {
                        // No handle exists yet, so no `Drop` will reclaim this;
                        // the unlink has to happen here.
                        if let Err(remove) = fs.remove_file(&path) {
                            log::warn!(
                                "Could not delete unusable blob file at {}: {remove:?}",
                                path.display(),
                            );
                        }
                        return Err(e);
                    }
                },
            };

            let file: Arc<dyn FsFile> =
                Arc::from(fs.open(&path, &crate::fs::FsOpenOptions::new().read(true))?);
            let file_accessor = if let Some(dt) = descriptor_table {
                FileAccessor::DescriptorTable {
                    table: dt,
                    fs: fs.clone(),
                }
            } else {
                FileAccessor::File(file.clone())
            };
            file_accessor.insert_for_blob_file((tree_id, blob_file_id).into(), file);

            let blob_file = BlobFile(Arc::new(BlobFileInner {
                checksum,
                tree_id,
                path,
                is_deleted: AtomicBool::new(false),
                punch_on_drop: portable_atomic::AtomicU64::new(u64::MAX),
                // A freshly written file is whole: nothing has been reclaimed.
                live_data_start: 0,
                id: blob_file_id,
                file_accessor,
                #[cfg(zstd_any)]
                zstd_dictionary,
                meta: Metadata {
                    id: blob_file_id,
                    version: metadata.version,
                    created_at: crate::time::unix_timestamp().as_nanos(),
                    item_count: metadata.item_count,
                    total_compressed_bytes: metadata.total_compressed_bytes,
                    total_uncompressed_bytes: metadata.total_uncompressed_bytes,
                    key_range: metadata.key_range,

                    compression: recorded_compression,
                },
                fs: fs.clone(),
                deletion_pause: once_cell::race::OnceBox::new(),

                #[cfg(feature = "std")]
                background_deleter: once_cell::race::OnceBox::new(),
            }));

            Ok(Some(blob_file))
        } else {
            log::debug!(
                "Blob file writer at {} has written no data, deleting empty blob file",
                writer.path.display(),
            );

            if let Err(e) = fs.remove_file(&writer.path) {
                log::warn!(
                    "Could not delete empty blob file at {}: {e:?}",
                    writer.path.display(),
                );
            }

            Ok(None)
        }
    }

    /// Writes an item.
    ///
    /// Returns the [`ValueHandle`] of the written blob.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write(&mut self, key: &[u8], seqno: SeqNo, value: &[u8]) -> crate::Result<ValueHandle> {
        let target_size = self.target_size;

        // Write actual value into blob file
        let writer = &mut self.active_writer;

        let offset = writer.offset();
        let on_disk_value_len = writer.write(key, seqno, value)?;

        let handle = ValueHandle {
            blob_file_id: writer.blob_file_id(),
            offset,
            on_disk_size: on_disk_value_len,
        };

        // Check for blob file size target, maybe rotate to next writer
        if writer.offset() >= target_size {
            self.rotate()?;
        }

        Ok(handle)
    }

    pub(crate) fn write_raw(
        &mut self,
        key: &[u8],
        seqno: SeqNo,
        value: &[u8],
        uncompressed_len: u32,
    ) -> crate::Result<ValueHandle> {
        let target_size = self.target_size;

        // Write actual value into blob file
        let writer = &mut self.active_writer;

        let offset = writer.offset();
        let on_disk_value_len = writer.write_raw(key, seqno, value, uncompressed_len)?;

        let handle = ValueHandle {
            blob_file_id: writer.blob_file_id(),
            offset,
            on_disk_size: on_disk_value_len,
        };

        // Check for blob file size target, maybe rotate to next writer
        if writer.offset() >= target_size {
            self.rotate()?;
        }

        Ok(handle)
    }

    pub(crate) fn finish(mut self) -> crate::Result<Vec<BlobFile>> {
        let blob_file = Self::consume_writer(
            self.active_writer,
            self.passthrough_compression,
            self.descriptor_table.clone(),
            &self.fs,
            #[cfg(zstd_any)]
            &self.zstd_dictionaries,
        )?;
        self.results.extend(blob_file);
        Ok(self.results)
    }
}
