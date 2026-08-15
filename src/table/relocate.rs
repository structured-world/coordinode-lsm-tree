// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Merge-on-read segment relocation: rewrite a columnar SST into a new file that
//! reuses every data block verbatim and carries a positional delete-bitmap,
//! deferring the expensive re-transpose that copy-on-write pays.
//!
//! Because merge-on-read keeps all rows (deleted ones are masked by the bitmap,
//! not dropped), the output segment is byte-identical to the source except for a
//! new table id and an added `delete_bitmap` section. So this copies the data /
//! index / filter / zone-map / seqno-bounds sections (and the torn-write
//! defenses) verbatim, re-encodes only the two `meta` blocks with the new id,
//! and appends the bitmap. The data section stays first, so the block-index /
//! zone-map / seqno-bounds absolute offsets stay valid; every other section is
//! addressed through the table of contents.

use super::Table;
use super::block::decoder::ParsedItem;
use super::block::{Block, BlockIdentity, BlockTransform, BlockType};
use super::data_block::DataBlock;
use super::delete_bitmap::DeleteBitmap;
use crate::checksum::ChecksummedWriter;
use crate::fs::{FsFile, FsOpenOptions, SyncMode};
use crate::io::BufWriter;
use crate::path::Path;
use crate::{Checksum, InternalValue, TableId, UserValue};
use alloc::vec::Vec;

/// Largest chunk read+written when copying a section verbatim, so a multi-MiB
/// data section never has to be buffered whole.
const COPY_CHUNK: usize = 256 * 1024;

impl Table {
    /// Writes a new SST at `new_path` that reuses this columnar segment's data
    /// blocks verbatim, re-points the on-disk table id to `new_table_id`, and
    /// adds `delete_bitmap` as a positional row mask. Returns the new file's
    /// checksum (for [`Table::recover`]); the caller installs and recovers it.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::FeatureUnsupported`] when the segment cannot be
    /// relocated by verbatim block reuse: an encrypted segment (the AEAD binds
    /// the table id, so a re-pointed copy fails verification), a segment carrying
    /// Page ECC (the re-encoded meta would need the parity layout), a row-major
    /// segment, or one without a zone map (the positional mask needs the
    /// per-block row counts the zone map carries). The caller falls back to a
    /// copy-on-write rewrite in those cases.
    pub(crate) fn relocate_columnar_with_deletes(
        &self,
        new_path: &Path,
        out_fs: &dyn crate::fs::Fs,
        new_table_id: TableId,
        delete_bitmap: &DeleteBitmap,
        sync_mode: SyncMode,
    ) -> crate::Result<Checksum> {
        // Verbatim block reuse is sound only for a non-encrypted, non-ECC
        // columnar segment that already carries a zone map (the bitmap's
        // positional mask resolves each block's start row from the zone-map row
        // counts, and the open-time invariant rejects a bitmap without one).
        if self.encryption.is_some()
            || self.metadata.ecc_params.is_some()
            || self.metadata.ecc_unrecognized
            || !self.metadata.columnar
            || self.zone_map.is_empty()
        {
            return Err(crate::Error::FeatureUnsupported(
                "merge-on-read block reuse needs a non-encrypted, non-ECC columnar segment with a zone map",
            ));
        }

        // Read the source through ITS filesystem; write the output through the
        // destination level's `out_fs` (the same one that recovers and installs
        // the relocated table), so level routing stays consistent.
        let mut src = self.fs.open(&self.path, &FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut src)?;

        // Re-encode the meta KV block with the new id AND the new delete-bitmap
        // descriptors. The block is loaded TAIL (`meta`) since MID and TAIL carry
        // identical content; the same patched payload is written to both sections
        // below.
        let meta_payload = self.repoint_meta_block(&*src, new_table_id, delete_bitmap)?;

        let out = out_fs.open(new_path, &FsOpenOptions::new().write(true).create_new(true))?;

        // Once the output file exists, any later failure (section copy, bitmap
        // write, sync, directory fsync) must not leave a partial, uninstalled SST
        // behind. Best-effort remove it before propagating the error.
        let result = (|| -> crate::Result<Checksum> {
            let out = BufWriter::with_capacity(u16::MAX.into(), out);
            let out = ChecksummedWriter::new(out);
            let mut writer = crate::sfa::Writer::from_writer(out);

            let meta_identity = BlockIdentity {
                table_id: new_table_id,
                block_type: BlockType::Meta,
                dict_id: 0,
                window_log: 0,
            };
            for entry in reader.toc().iter() {
                let name = entry.name();
                writer.start(name)?;
                if name == b"meta_mid" || name == b"meta" {
                    // Re-encoded copy (new id), not the source bytes. Non-encrypted
                    // segment, so the two copies are byte-identical (no nonce).
                    Block::write_into(
                        &mut writer,
                        &meta_payload,
                        meta_identity,
                        &BlockTransform::PLAIN,
                    )?;
                } else {
                    copy_section(&*src, &mut writer, entry.pos(), entry.len())?;
                }
            }

            // Inject the positional delete-bitmap. Its position is free (addressed
            // by name through the table of contents); appended after the copied
            // sections. Same uncompressed envelope as the other meta sections.
            writer.start(b"delete_bitmap")?;
            let encoded = delete_bitmap.encode();
            Block::write_into(
                &mut writer,
                &encoded,
                BlockIdentity {
                    table_id: new_table_id,
                    block_type: BlockType::DeleteBitmap,
                    dict_id: 0,
                    window_log: 0,
                },
                &BlockTransform::PLAIN,
            )?;

            let mut checksummed = writer.into_inner()?;
            let checksum = checksummed.checksum();
            let file = checksummed.inner_mut().get_mut();
            FsFile::sync_all_with(&**file, sync_mode)?;
            #[expect(
                clippy::expect_used,
                reason = "an SST path always has a parent directory (the table folder)"
            )]
            crate::file::fsync_directory(
                new_path.parent().expect("table file has a parent folder"),
                out_fs,
                sync_mode,
            )?;
            Ok(checksum)
        })();

        if result.is_err() {
            let _ = out_fs.remove_file(new_path);
        }
        result
    }

    /// Loads this segment's meta KV block, replaces the `table_id` entry's value
    /// with `new_table_id` AND the two `descriptor#delete_bitmap_*` entries with
    /// values describing `delete_bitmap` (the NEW positional bitmap this
    /// relocation appends), then re-encodes the payload (uncompressed, ready for a
    /// [`BlockType::Meta`] write). Every other meta field is preserved byte-exact.
    ///
    /// Without patching the delete-bitmap descriptors, the copied values would
    /// still describe the SOURCE's (absent / different) bitmap, so
    /// `verify_metadata_bounds` would flag the healthy relocated table during
    /// `repair_with_salvage`, refuse to re-emit it, and quarantine it.
    fn repoint_meta_block(
        &self,
        src: &dyn FsFile,
        new_table_id: TableId,
        delete_bitmap: &DeleteBitmap,
    ) -> crate::Result<Vec<u8>> {
        // Non-encrypted precondition (checked by the caller): PLAIN transform.
        let block = Block::from_file(
            src,
            self.regions.metadata,
            BlockIdentity {
                table_id: self.metadata.id,
                block_type: BlockType::Meta,
                dict_id: 0,
                window_log: 0,
            },
            &BlockTransform::PLAIN,
        )?;
        let block = DataBlock::new(block);
        // Meta keys are lexicographic, so the default comparator orders them.
        let cmp = crate::comparator::default_comparator();
        let mut entries: Vec<InternalValue> = block
            .iter(cmp.clone())
            .map(|item| item.materialize(block.as_slice()))
            .collect();

        // New delete-bitmap descriptor values (same key, same fixed width, so the
        // in-place value swap preserves the meta block's sorted key order).
        let db_len_bytes = delete_bitmap.len().to_le_bytes();
        let db_hash_bytes = crate::hash::hash128(&delete_bitmap.encode()).to_le_bytes();
        let (mut id_patched, mut len_patched, mut hash_patched) = (false, false, false);
        for entry in &mut entries {
            match entry.key.user_key.as_ref() {
                b"table_id" => {
                    entry.value = UserValue::from(&new_table_id.to_le_bytes()[..]);
                    id_patched = true;
                }
                b"descriptor#delete_bitmap_len" => {
                    entry.value = UserValue::from(&db_len_bytes[..]);
                    len_patched = true;
                }
                b"descriptor#delete_bitmap_hash" => {
                    entry.value = UserValue::from(&db_hash_bytes[..]);
                    hash_patched = true;
                }
                _ => {}
            }
        }
        if !id_patched {
            return Err(crate::Error::InvalidHeader(
                "relocate: meta block missing table_id",
            ));
        }
        // A LEGACY columnar source predates `descriptor#delete_bitmap_len` /
        // `descriptor#delete_bitmap_hash` (the current writer always records both,
        // as `0` when it holds no bitmap). Failing here would permanently block a
        // single-input merge-on-read compaction that materializes range-tombstone
        // deletes on such a table (the planner has already chosen the relocation
        // fast path, with no copy-on-write fallback), so INSTEAD insert the
        // descriptors describing the appended bitmap. The meta block requires
        // sorted keys, so re-sort after inserting.
        if !len_patched {
            entries.push(InternalValue::from_components(
                b"descriptor#delete_bitmap_len".to_vec(),
                &db_len_bytes[..],
                0,
                crate::ValueType::Value,
            ));
        }
        if !hash_patched {
            entries.push(InternalValue::from_components(
                b"descriptor#delete_bitmap_hash".to_vec(),
                &db_hash_bytes[..],
                0,
                crate::ValueType::Value,
            ));
        }
        if !(len_patched && hash_patched) {
            entries.sort_by(|a, b| cmp.compare(a.key.user_key.as_ref(), b.key.user_key.as_ref()));
        }

        // Same encode parameters the writer uses for the meta block
        // (restart interval 1, no hashing). The reader point-reads by key, so the
        // restart interval need not match the source; keeping it identical avoids
        // surprises.
        let mut payload = Vec::new();
        DataBlock::encode_into(&mut payload, &entries, 1, 0.0)?;
        Ok(payload)
    }
}

/// Copies `len` bytes from `src` at absolute offset `pos` into `writer`,
/// in bounded chunks so a large data section is never buffered whole.
fn copy_section<W: crate::io::Write>(
    src: &dyn FsFile,
    writer: &mut W,
    pos: u64,
    len: u64,
) -> crate::Result<()> {
    let mut offset = pos;
    let end = pos + len;
    while offset < end {
        // `end - offset` is bounded by the section length (a u64 file size);
        // the `min` caps each read at COPY_CHUNK, so the cast cannot truncate.
        #[expect(clippy::cast_possible_truncation, reason = "capped at COPY_CHUNK")]
        let want = (end - offset).min(COPY_CHUNK as u64) as usize;
        let bytes = crate::file::read_exact(src, offset, want)?;
        writer.write_all(&bytes)?;
        offset += want as u64;
    }
    Ok(())
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "tests intentionally unwrap setup failures to keep assertions focused"
)]
mod tests;
