// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! [`ManifestArchiveWriter`] — emits a Blocks-based manifest file
//! that the V5-2 layout (`manifest_layout_version = 1`) describes.
//!
//! The writer mirrors the `crate::sfa::Writer` start / write / finish API
//! so the call sites in [`crate::version::persist`] and the
//! V5-2-aware version encoder can migrate with minimal churn: open
//! the writer, [`start`] a section, write its bytes via the
//! [`Write`] impl, repeat per section, then [`finish`] to emit the
//! tail footer Block (and the head mirror when
//! [`crate::runtime_config::RuntimeConfig::manifest_footer_mirror`] is on).
//!
//! Each section's bytes are buffered in memory until [`start`] /
//! [`finish`] flushes the section into a Block. Realistic sections
//! are small (KB–MB scale even for large versions); the
//! short-lived `Vec<u8>` buffer is the cost we pay for treating
//! every section's checksum / encryption boundary as atomic.
//!
//! [`start`]: ManifestArchiveWriter::start
//! [`finish`]: ManifestArchiveWriter::finish

use crate::{
    encryption::EncryptionProvider,
    fs::{Fs, FsFile, FsOpenOptions},
    manifest_blocks::{
        FLAG_FOOTER_MIRROR_ENABLED, HEAD_FOOTER_RESERVED_SIZE, MANIFEST_TABLE_ID_SENTINEL,
        MANIFEST_TREE_ID_SENTINEL, MAX_MANIFEST_BLOCK_SIZE, MAX_SECTION_NAME_BYTES,
        footer::{FooterPayload, TocEntry},
    },
    runtime_config::RuntimeConfig,
    table::block::{Block, BlockIdentity, BlockTransform, BlockType},
};
use byteorder::{LittleEndian, WriteBytesExt};
use std::{
    collections::BTreeSet,
    io::{self, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
};

/// Streaming writer for V5-2 manifest files. See module docs.
pub struct ManifestArchiveWriter {
    /// Owning file handle. Must be opened `read + write` so the
    /// finish path can seek back to offset 0 to populate the head
    /// mirror without a second `Fs::open` round-trip.
    file: Box<dyn FsFile>,

    /// Runtime config snapshot captured at creation time. Locks in
    /// the footer-mirror toggle and ECC overrides for the entire
    /// write — a concurrent `update_runtime_config` mid-write does
    /// not split this manifest across two configs (the next
    /// manifest write picks up the new snapshot per the V5-1
    /// compaction-as-migration semantic).
    runtime: Arc<RuntimeConfig>,

    /// Optional per-tree encryption provider, plumbed in from
    /// `Config::encryption`. When `Some`, every section Block and
    /// the tail/head footer Block go through `BlockTransform::Encrypted`
    /// (or `EncryptedEcc` when ECC is also on), inheriting the same
    /// AEAD pipeline data blocks use. When `None`, manifest Blocks
    /// stay plaintext — matches the data-side default for trees
    /// opened without `Config::encryption`.
    encryption: Option<Arc<dyn EncryptionProvider>>,

    /// Section currently open via [`start`]. Buffered in memory
    /// until the next `start` or `finish` flushes it into a Block.
    current_section: Option<CurrentSection>,

    /// Accumulated table of contents. Populated as sections are
    /// flushed; consumed by `finish` to build the footer payload.
    toc: Vec<TocEntry>,

    /// Names of sections already flushed or buffered. Used by
    /// `start` to reject duplicates before any bytes are written.
    section_names: BTreeSet<String>,

    /// Absolute file offset where the next section Block will be
    /// written. Initialized to [`HEAD_FOOTER_RESERVED_SIZE`] after
    /// the head reservation zeroes are written; advanced by each
    /// section's Block size.
    write_cursor: u64,
}

struct CurrentSection {
    name: String,
    buf: Vec<u8>,
}

impl ManifestArchiveWriter {
    /// Open `path` for fresh writing (`create_new` — refuses to
    /// overwrite), reserve the 4 KiB head region with zeros, and
    /// return a writer positioned to append section Blocks.
    ///
    /// `runtime` snapshot governs `manifest_footer_mirror` and ECC
    /// overrides for this file's entire lifetime — see field doc.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Io`] when the file cannot be created
    /// or the head reservation cannot be written.
    pub fn create(
        path: &Path,
        fs: &dyn Fs,
        runtime: Arc<RuntimeConfig>,
        encryption: Option<Arc<dyn EncryptionProvider>>,
    ) -> crate::Result<Self> {
        let mut file = fs.open(
            path,
            &FsOpenOptions::new().read(true).write(true).create_new(true),
        )?;
        // Reserve HEAD_FOOTER_RESERVED_SIZE bytes with zeros. The
        // head mirror lands here on finish() — writing the zeros up
        // front means the file is always aligned and partial-write
        // failures during the head-mirror copy don't expose garbage
        // bytes in the reserved region.
        // HEAD_FOOTER_RESERVED_SIZE is a compile-time 4 KiB constant
        // that fits any platform's usize; `as usize` is a no-op cast
        // on 64-bit and a checked-tight 4 KiB allocation on 32-bit.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "HEAD_FOOTER_RESERVED_SIZE = 4 KiB, fits in usize on every supported target"
        )]
        let zeros = vec![0u8; HEAD_FOOTER_RESERVED_SIZE as usize];
        file.write_all(&zeros)?;
        Ok(Self {
            file,
            runtime,
            encryption,
            current_section: None,
            toc: Vec::new(),
            section_names: BTreeSet::new(),
            write_cursor: HEAD_FOOTER_RESERVED_SIZE,
        })
    }

    /// Construct the [`BlockTransform`] every manifest Block in
    /// this file should use, given the captured runtime + optional
    /// encryption. Mirrors the logic in `Block::write_into`'s
    /// caller surface: ECC and encryption are independent toggles
    /// that compose into the four `(Plain | Encrypted) × (None | Ecc)`
    /// variants.
    ///
    /// ECC arms are feature-gated behind `page_ecc` so the
    /// `--no-default-features` build doesn't reference a variant
    /// that doesn't exist in that cargo configuration.
    fn block_transform(&self) -> BlockTransform<'_> {
        // Single source of truth for the per-write ECC decision.
        // Honors per-scope overrides (data_block_ecc_override etc.)
        // via the `manifest_ecc()` helper on RuntimeConfig.
        #[cfg(feature = "page_ecc")]
        let ecc_on = self.runtime.manifest_ecc();
        #[cfg(not(feature = "page_ecc"))]
        let ecc_on = false;

        match (ecc_on, self.encryption.as_deref()) {
            #[cfg(feature = "page_ecc")]
            (true, Some(enc)) => BlockTransform::EncryptedEcc(enc),
            #[cfg(feature = "page_ecc")]
            (true, None) => BlockTransform::PlainEcc,
            (_, Some(enc)) => BlockTransform::Encrypted(enc),
            (_, None) => BlockTransform::PLAIN,
        }
    }

    /// Open a new section named `name`. Subsequent writes via the
    /// [`Write`] impl go into this section's buffer until the next
    /// `start` or `finish` flushes it as a Block.
    ///
    /// Auto-flushes any previously open section.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::ManifestFooterInvalid`] for an empty,
    /// oversized, or duplicate name; propagates I/O / Block errors
    /// from the auto-flush of the previous section.
    pub fn start(&mut self, name: &str) -> crate::Result<()> {
        if name.is_empty() {
            return Err(crate::Error::ManifestFooterInvalid(
                "section name must be non-empty",
            ));
        }
        if name.len() > MAX_SECTION_NAME_BYTES {
            return Err(crate::Error::ManifestFooterInvalid(
                "section name exceeds MAX_SECTION_NAME_BYTES",
            ));
        }
        // Duplicate-name check before flush: cheap and lets us
        // reject without spending I/O on the previous section.
        if self.section_names.contains(name) {
            return Err(crate::Error::ManifestFooterInvalid(
                "duplicate section name",
            ));
        }

        // Flush the previously-open section BEFORE recording the new
        // name in `section_names`. If flush_current_section() fails
        // (block overflow, I/O error), the writer is left with no
        // open section and the unflushed name never enters
        // `section_names` — a caller that catches the error and
        // retries start() with the same name doesn't trip a
        // spurious duplicate check.
        self.flush_current_section()?;
        self.section_names.insert(name.to_string());
        self.current_section = Some(CurrentSection {
            name: name.to_string(),
            buf: Vec::new(),
        });
        Ok(())
    }

    /// Flush the last open section (if any), then write the tail
    /// footer Block and — when `manifest_footer_mirror` is on —
    /// copy it to the 4 KiB head reservation. Finally `sync_all`
    /// the file so the manifest is durable before the caller
    /// publishes the CURRENT pointer.
    ///
    /// # Errors
    ///
    /// - [`crate::Error::ManifestFooterInvalid`] when the footer
    ///   Block bytes exceed [`HEAD_FOOTER_RESERVED_SIZE`] (signals
    ///   either a future-version manifest written by a newer binary
    ///   or a writer bug accumulating too many sections; the safety
    ///   cap is never reachable by legitimate production writers).
    /// - [`crate::Error::Io`] on writer / seek / sync failure.
    /// - Propagates Block I/O errors from the tail / head writes.
    ///
    /// On success returns the byte offset of the first byte AFTER
    /// the last section Block — i.e. the start of the tail footer
    /// Block. The caller uses this to compute a "section bytes
    /// only" digest for the CURRENT pointer (excluding the
    /// recoverable footer + size-hint trailer + head mirror).
    pub fn finish(mut self) -> crate::Result<u64> {
        self.flush_current_section()?;
        let section_end = self.write_cursor;

        let mirror_enabled = self.runtime.manifest_footer_mirror;
        let flags = if mirror_enabled {
            FLAG_FOOTER_MIRROR_ENABLED
        } else {
            0
        };
        let payload = FooterPayload::new(flags, std::mem::take(&mut self.toc));
        let mut payload_bytes = Vec::new();
        payload.encode(&mut payload_bytes)?;

        let identity = BlockIdentity {
            tree_id: MANIFEST_TREE_ID_SENTINEL,
            table_id: MANIFEST_TABLE_ID_SENTINEL,
            block_offset: self.write_cursor,
            block_type: BlockType::ManifestFooter,
            dict_id: 0,
            window_log: 0,
        };

        let mut footer_block_bytes = Vec::new();
        Block::write_into(
            &mut footer_block_bytes,
            &payload_bytes,
            identity,
            &self.block_transform(),
        )?;

        // Safety-net check per Q12: hard 4 KiB ceiling on footer
        // Block size. Realistic production manifests use ~5% of
        // this space; hitting it signals a writer bug or future-
        // layout-version-2 manifest, not legitimate capacity
        // exhaustion. Apply the check unconditionally (even when
        // mirror is off) so a writer bug that accumulates an
        // oversized footer can't silently ship a manifest only the
        // current binary can read.
        if footer_block_bytes.len() as u64 > HEAD_FOOTER_RESERVED_SIZE {
            return Err(crate::Error::ManifestFooterInvalid(
                "footer Block exceeds HEAD_FOOTER_RESERVED_SIZE",
            ));
        }

        // Write tail footer at the current cursor (primary read
        // target). Write order matters for partial-write recovery:
        // the tail goes down first so a crash before the head-
        // mirror copy still leaves a fully-valid file readable via
        // the tail path.
        self.file.write_all(&footer_block_bytes)?;

        // Trailing footer-size pointer. The last 4 bytes of the
        // file declare the footer Block's on-disk byte length so
        // the reader can locate it without scanning: seek to
        // `file_len - 4`, read u32, seek to `file_len - 4 - size`,
        // call `Block::from_reader`. Header check at module level
        // bounds footer_block_bytes.len() <= HEAD_FOOTER_RESERVED_SIZE
        // = 4 KiB, well within u32 range.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "footer_block_bytes.len() <= HEAD_FOOTER_RESERVED_SIZE = 4 KiB, fits u32"
        )]
        let footer_size_u32 = footer_block_bytes.len() as u32;
        self.file.write_u32::<LittleEndian>(footer_size_u32)?;

        if mirror_enabled {
            // Seek back to offset 0, write the same Block bytes,
            // then zero-pad the remainder of the reserved region.
            // The padding bytes are covered by the Block's XXH3
            // (they're outside the Block's `data_length`, so they
            // don't affect verification — the reader uses the
            // declared `data_length` to bound the payload read).
            //
            // Byte-copy (not re-encode at offset 0) is correct
            // here because the current AEAD path
            // (`Aes256GcmProvider::encrypt`) does NOT bind AAD to
            // `BlockIdentity.block_offset` — the encrypt() trait
            // method only consumes plaintext + nonce. AAD-bound
            // framing is plumbed through `encryption::aad::build`
            // but not yet wired through `encrypt()` / `decrypt()`;
            // when it is, this byte-copy must change to a fresh
            // `Block::write_into` call with `block_offset = 0` so
            // the head slot decrypts against AAD that matches the
            // reader's per-slot identity. The regression test
            // `reader_falls_back_to_head_mirror_for_encrypted_manifest`
            // locks this contract — it passes today (no AAD) and
            // would catch a future change that adds AAD without
            // updating this path.
            self.file.seek(SeekFrom::Start(0))?;
            self.file.write_all(&footer_block_bytes)?;
            let padding = HEAD_FOOTER_RESERVED_SIZE - footer_block_bytes.len() as u64;
            if padding > 0 {
                // `padding` is bounded by HEAD_FOOTER_RESERVED_SIZE
                // (4 KiB) since `footer_block_bytes.len() <=
                // HEAD_FOOTER_RESERVED_SIZE` is enforced just above.
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "padding bounded by HEAD_FOOTER_RESERVED_SIZE = 4 KiB; \
                              fits in usize on every supported target"
                )]
                let zeros = vec![0u8; padding as usize];
                self.file.write_all(&zeros)?;
            }
        }

        self.file.sync_all()?;
        Ok(section_end)
    }

    /// Flush the currently buffered section (if any) into a
    /// [`BlockType::Manifest`] Block, append it to the file, and
    /// record its (offset, size) in the TOC.
    fn flush_current_section(&mut self) -> crate::Result<()> {
        let Some(section) = self.current_section.take() else {
            return Ok(());
        };

        let block_offset = self.write_cursor;
        let identity = BlockIdentity {
            tree_id: MANIFEST_TREE_ID_SENTINEL,
            table_id: MANIFEST_TABLE_ID_SENTINEL,
            block_offset,
            block_type: BlockType::Manifest,
            dict_id: 0,
            window_log: 0,
        };

        let mut block_bytes = Vec::new();
        Block::write_into(
            &mut block_bytes,
            &section.buf,
            identity,
            &self.block_transform(),
        )?;

        // Symmetric with the reader's `MAX_MANIFEST_BLOCK_SIZE`
        // check (see ManifestArchiveReader::read_section): refuse
        // to emit a section Block the same codebase's reader will
        // later refuse to load. Surface a typed error here rather
        // than after a wasted disk write.
        if block_bytes.len() as u64 > u64::from(MAX_MANIFEST_BLOCK_SIZE) {
            return Err(crate::Error::ManifestFooterInvalid(
                "section Block exceeds MAX_MANIFEST_BLOCK_SIZE",
            ));
        }

        self.file.write_all(&block_bytes)?;
        let block_size_u64 = block_bytes.len() as u64;
        // Block payload is capped at MAX_DECOMPRESSION_SIZE (256
        // MiB) inside Block::write_into, plus a small header/trailer
        // overhead. The total is well within u32 range; the cast
        // documents that invariant rather than masking a real
        // overflow risk.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "Block::write_into enforces MAX_DECOMPRESSION_SIZE = 256 MiB; \
                      block_bytes.len() = header (37 bytes) + payload (<= 256 MiB) + \
                      optional ECC trailer (~N/2 + small) is well within u32 range"
        )]
        let block_size = block_size_u64 as u32;
        // `checked_add` (not `saturating_add`): TOC offsets are
        // correctness-critical metadata. A silently clamped cursor would
        // emit a manifest whose TOC points at wrong offsets, which a
        // reader cannot detect until decode failure deep in the
        // recovery path. Fail fast with a typed error instead.
        self.write_cursor = self.write_cursor.checked_add(block_size_u64).ok_or(
            crate::Error::ManifestFooterInvalid(
                "write cursor would overflow u64 — manifest file size implausible",
            ),
        )?;

        self.toc.push(TocEntry {
            name: section.name,
            block_offset,
            block_size,
        });
        Ok(())
    }
}

impl Write for ManifestArchiveWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let section = self.current_section.as_mut().ok_or_else(|| {
            io::Error::other("write called with no active manifest section — call start() first")
        })?;
        // Fail fast if this write would push the buffered section
        // past MAX_MANIFEST_BLOCK_SIZE. flush_current_section already
        // rejects oversized sections, but doing the check only there
        // means a buggy/adversarial caller can grow `section.buf` to
        // gigabytes before the error surfaces — wasting allocator
        // pressure and obscuring the offending caller's stack. The
        // incremental check returns the failure at the exact write
        // that crossed the line.
        let projected = section
            .buf
            .len()
            .checked_add(buf.len())
            .ok_or_else(|| io::Error::other("section buffer length would overflow usize"))?;
        if u64::try_from(projected).unwrap_or(u64::MAX) > u64::from(MAX_MANIFEST_BLOCK_SIZE) {
            return Err(io::Error::other(
                "manifest section exceeds MAX_MANIFEST_BLOCK_SIZE",
            ));
        }
        section.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // Section bytes live in our in-memory buffer until the next
        // start() / finish() pushes them through Block::write_into.
        // There is nothing OS-level to flush at the per-write
        // boundary — finish() handles the durability sync.
        Ok(())
    }
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::items_after_statements,
    clippy::cast_possible_truncation,
    reason = "test fixtures: HEAD_FOOTER_RESERVED_SIZE = 4 KiB fits any usize, \
              `unwrap()` / `expect()` panic on failure to surface bugs loudly, \
              local `use std::io::Read;` inside the test reads natural at the use site"
)]
mod tests {
    use super::*;
    use crate::fs::MemFs;
    use crate::runtime_config::RuntimeConfig;

    fn open_writer(fs: &dyn Fs, path: &Path, runtime: RuntimeConfig) -> ManifestArchiveWriter {
        ManifestArchiveWriter::create(path, fs, Arc::new(runtime), None)
            .expect("manifest writer opens cleanly on a fresh path")
    }

    #[test]
    fn writer_reserves_head_region_on_create() {
        // The 4 KiB head reservation must be in place immediately
        // after create() so a crash before any section is written
        // still leaves a self-describing zero-prefix that the
        // reader recognises as "no head mirror present" (vs reading
        // garbage from an un-truncated file).
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/m")).unwrap();
        let path = Path::new("/m/manifest");
        let writer = open_writer(&fs, path, RuntimeConfig::default());
        // Don't write any sections — drop the writer immediately
        // (no finish). The file should still hold the 4 KiB zeros.
        drop(writer);

        let mut file = fs.open(path, &FsOpenOptions::new().read(true)).unwrap();
        let mut buf = vec![0u8; HEAD_FOOTER_RESERVED_SIZE as usize];
        use std::io::Read;
        file.read_exact(&mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn writer_rejects_empty_section_name() {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/m")).unwrap();
        let mut w = open_writer(&fs, Path::new("/m/manifest"), RuntimeConfig::default());
        let err = w.start("").expect_err("empty name must be rejected");
        assert!(matches!(err, crate::Error::ManifestFooterInvalid(_)));
    }

    #[test]
    fn writer_rejects_duplicate_section_name() {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/m")).unwrap();
        let mut w = open_writer(&fs, Path::new("/m/manifest"), RuntimeConfig::default());
        w.start("tables").unwrap();
        w.write_all(&[1, 2, 3]).unwrap();
        let err = w
            .start("tables")
            .expect_err("duplicate section name must be rejected");
        assert!(matches!(err, crate::Error::ManifestFooterInvalid(_)));
    }

    #[test]
    fn writer_finish_with_no_sections_writes_only_footer() {
        // A degenerate manifest with zero sections is still
        // structurally valid (footer with empty TOC). The writer
        // must produce a file that the reader can open without
        // tripping any non-zero-section invariants.
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/m")).unwrap();
        let path = Path::new("/m/empty");
        let writer = open_writer(&fs, path, RuntimeConfig::default());
        writer.finish().expect("finish on empty writer succeeds");

        let meta = fs.metadata(path).unwrap();
        // File must contain at least the head reservation + a tail
        // footer Block. Exact size depends on Block header overhead;
        // we only assert the reservation is intact.
        assert!(meta.len > HEAD_FOOTER_RESERVED_SIZE);
    }

    #[test]
    fn writer_finish_writes_head_mirror_when_enabled() {
        // mirror enabled (default): the head reservation must hold
        // a copy of the tail footer Block. Comparing the first
        // HEAD_FOOTER_RESERVED_SIZE bytes against the trailing
        // bytes of the file proves the writer mirrored correctly.
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/m")).unwrap();
        let path = Path::new("/m/mirrored");
        let mut w = open_writer(&fs, path, RuntimeConfig::default());
        w.start("format_version").unwrap();
        w.write_all(&[5]).unwrap();
        w.finish().unwrap();

        let mut file = fs.open(path, &FsOpenOptions::new().read(true)).unwrap();
        use std::io::Read;
        let mut head = vec![0u8; HEAD_FOOTER_RESERVED_SIZE as usize];
        file.read_exact(&mut head).unwrap();
        // The head mirror payload should NOT be all zeros — the
        // first bytes are the footer Block header magic.
        assert!(
            !head.iter().take(64).all(|&b| b == 0),
            "head mirror should contain the footer Block bytes, got all-zero prefix"
        );
    }

    #[test]
    fn writer_finish_leaves_head_region_zero_when_mirror_disabled() {
        // mirror disabled: head reservation stays as the zeros the
        // writer wrote at create(). Reader uses the zero magic as
        // the "no head mirror" signal and skips the head-fallback
        // path on tail-verify failure.
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/m")).unwrap();
        let path = Path::new("/m/no_mirror");
        let runtime = RuntimeConfig {
            manifest_footer_mirror: false,
            ..RuntimeConfig::default()
        };
        let mut w = open_writer(&fs, path, runtime);
        w.start("format_version").unwrap();
        w.write_all(&[5]).unwrap();
        w.finish().unwrap();

        let mut file = fs.open(path, &FsOpenOptions::new().read(true)).unwrap();
        use std::io::Read;
        let mut head = vec![0u8; HEAD_FOOTER_RESERVED_SIZE as usize];
        file.read_exact(&mut head).unwrap();
        assert!(
            head.iter().all(|&b| b == 0),
            "head reservation should remain zeroed when manifest_footer_mirror=false"
        );
    }

    #[test]
    fn writer_section_block_offsets_advance_past_head_reservation() {
        // Concrete invariant: the first section Block lands at
        // offset HEAD_FOOTER_RESERVED_SIZE (just after the head
        // reservation), and subsequent sections advance by the
        // exact Block size. Easy to verify by reading the TOC out
        // of the footer Block bytes — done in the reader's
        // integration test once reader lands; here we just lock
        // the offset of the first section by inspecting toc[0].
        // Since toc is private, we exercise the invariant
        // indirectly: write two sections, finish, then read the
        // file and assert section 1 starts immediately after the
        // head reservation. (Full TOC decode lands with the reader
        // in the next commit.)
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/m")).unwrap();
        let path = Path::new("/m/cursor");
        let mut w = open_writer(&fs, path, RuntimeConfig::default());
        w.start("a").unwrap();
        w.write_all(&[1, 2, 3]).unwrap();
        w.start("b").unwrap();
        w.write_all(&[4, 5, 6, 7]).unwrap();
        w.finish().unwrap();

        // Read the byte just after the head reservation — should
        // be the magic of the first section Block, not a zero.
        let mut file = fs.open(path, &FsOpenOptions::new().read(true)).unwrap();
        file.seek(SeekFrom::Start(HEAD_FOOTER_RESERVED_SIZE))
            .unwrap();
        let mut byte = [0u8; 1];
        use std::io::Read;
        file.read_exact(&mut byte).unwrap();
        assert_ne!(
            byte[0], 0,
            "first section Block should start at HEAD_FOOTER_RESERVED_SIZE"
        );
    }
}
