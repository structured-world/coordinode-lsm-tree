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

        // Re-encode the meta KV block with the new id. The block is loaded TAIL
        // (`meta`) since MID and TAIL carry identical content; the same patched
        // payload is written to both sections below.
        let meta_payload = self.repoint_meta_block(&*src, new_table_id)?;

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
    /// with `new_table_id`, and re-encodes the payload (uncompressed, ready for a
    /// [`BlockType::Meta`] write). Every other meta field is preserved byte-exact,
    /// so fields the parsed view does not expose still round-trip.
    fn repoint_meta_block(
        &self,
        src: &dyn FsFile,
        new_table_id: TableId,
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
            .iter(cmp)
            .map(|item| item.materialize(block.as_slice()))
            .collect();

        let mut patched = false;
        for entry in &mut entries {
            if entry.key.user_key.as_ref() == b"table_id" {
                entry.value = UserValue::from(&new_table_id.to_le_bytes()[..]);
                patched = true;
            }
        }
        if !patched {
            return Err(crate::Error::InvalidHeader(
                "relocate: meta block missing table_id",
            ));
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
mod tests {
    use super::*;
    use crate::cache::Cache;
    use crate::descriptor_table::DescriptorTable;
    use crate::fs::StdFs;
    use crate::table::Writer;
    #[cfg(feature = "columnar")]
    use crate::{SeqNo, hash::hash64};
    use alloc::sync::Arc;
    use test_log::test;

    #[cfg(feature = "metrics")]
    use crate::metrics::Metrics;

    fn recover_at(file: &Path, checksum: Checksum, table_id: TableId) -> crate::Result<Table> {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());
        Table::recover(
            file.to_path_buf(),
            checksum,
            0,
            0,
            table_id,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Some(Arc::new(DescriptorTable::new(10))),
            Arc::new(StdFs),
            false,
            false,
            None,
            #[cfg(zstd_any)]
            None,
            crate::comparator::default_comparator(),
            #[cfg(feature = "metrics")]
            metrics,
        )
    }

    #[cfg(feature = "columnar")]
    #[test]
    fn relocate_reuses_blocks_and_masks_deleted_rows() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let src_path = dir.path().join("src");
        let out_path = dir.path().join("out");

        let n = 96u32;
        // Positions follow write (= key) order.
        let deleted = [4u32, 7, 40, 95];

        // Source: a columnar segment with a zone map and NO deletes.
        let mut writer = Writer::new(src_path.clone(), 0, 0, Arc::new(StdFs))?
            .use_columnar(true)
            .use_zone_map(true);
        for i in 0..n {
            let key = format!("k{i:04}").into_bytes();
            writer.write(InternalValue::from_components(
                key,
                b"val",
                1,
                crate::ValueType::Value,
            ))?;
        }
        let (_, src_checksum) = writer.finish()?.expect("source table written");
        let source = recover_at(&src_path, src_checksum, 0)?;

        // Relocate into a new segment (id 1) carrying the bitmap.
        let mut bitmap = DeleteBitmap::new();
        for &row in &deleted {
            bitmap.insert(row);
        }
        let out_checksum = source.relocate_columnar_with_deletes(
            &out_path,
            &StdFs,
            1,
            &bitmap,
            SyncMode::Normal,
        )?;

        let relocated = recover_at(&out_path, out_checksum, 1)?;

        // (i) format flags + id preserved; (ii) deleted rows masked, live found.
        assert_eq!(relocated.metadata.id, 1, "meta carries the new table id");
        assert!(relocated.metadata.columnar, "columnar flag preserved");
        for i in 0..n {
            let key = format!("k{i:04}").into_bytes();
            let got = relocated.get(&key, SeqNo::MAX, hash64(&key))?;
            if deleted.contains(&i) {
                assert!(
                    got.is_none(),
                    "deleted row {i} must read absent after relocate"
                );
            } else {
                let got = got.expect("live row must survive relocate");
                assert_eq!(&*got.value, b"val", "live value preserved verbatim");
            }
        }
        Ok(())
    }

    #[test]
    fn relocate_rejects_row_major_segment() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let src_path = dir.path().join("src");

        // Row-major (no columnar): block reuse must refuse and let the caller CoW.
        let mut writer = Writer::new(src_path.clone(), 0, 0, Arc::new(StdFs))?.use_zone_map(true);
        writer.write(InternalValue::from_components(
            b"a",
            b"v",
            1,
            crate::ValueType::Value,
        ))?;
        let (_, checksum) = writer.finish()?.expect("table written");
        let source = recover_at(&src_path, checksum, 0)?;

        let out_path = dir.path().join("out");
        let mut bitmap = DeleteBitmap::new();
        bitmap.insert(0);
        let err = source
            .relocate_columnar_with_deletes(&out_path, &StdFs, 1, &bitmap, SyncMode::Normal)
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::FeatureUnsupported(_)),
            "row-major segment must be rejected, got {err:?}",
        );
        Ok(())
    }
}
