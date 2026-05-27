// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! [`ManifestArchiveReader`] — locates and verifies the footer of a
//! Blocks-based manifest file, then exposes its sections via TOC
//! lookup + per-section Block read.
//!
//! ## Read protocol
//!
//! 1. **Tail (primary).** Read the last 4 bytes to learn the footer
//!    Block size. Seek to `file_len - 4 - size` and call
//!    [`Block::from_reader`] with `BlockType::ManifestFooter`. The
//!    Block layer verifies XXH3 (and ECC / AEAD when enabled).
//!    Decode [`FooterPayload`] from the verified payload.
//! 2. **Head mirror (fallback).** If the tail path fails for any
//!    reason (truncated file, corrupted size hint, XXH3 mismatch,
//!    payload parse error), seek to offset 0, read the
//!    [`HEAD_FOOTER_RESERVED_SIZE`]-byte reservation, and attempt
//!    the same Block decode + payload parse. The reservation is
//!    zero-padded by the writer; if the first bytes are all zero
//!    the mirror was never populated (writer ran with
//!    `manifest_footer_mirror = false`) and the fallback can't help.
//! 3. **Unrecoverable.** Both paths failed → return
//!    [`crate::Error::Unrecoverable`].
//!
//! Per-section reads use the TOC-recorded `(block_offset, block_size)`
//! pair to read exactly one Block per call without scanning.
//!
//! [`Block::from_reader`]: crate::table::block::Block::from_reader
//! [`FooterPayload`]: crate::manifest_blocks::footer::FooterPayload
//! [`HEAD_FOOTER_RESERVED_SIZE`]: crate::manifest_blocks::HEAD_FOOTER_RESERVED_SIZE

use crate::{
    encryption::EncryptionProvider,
    fs::{Fs, FsFile, FsOpenOptions},
    manifest_blocks::{
        HEAD_FOOTER_RESERVED_SIZE, MANIFEST_TABLE_ID_SENTINEL, MANIFEST_TREE_ID_SENTINEL,
        MAX_MANIFEST_BLOCK_SIZE, TAIL_FOOTER_SIZE_HINT_BYTES,
        footer::{FooterPayload, TocEntry},
    },
    runtime_config::RuntimeConfig,
    table::block::{Block, BlockIdentity, BlockTransform, BlockType},
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::{
    io::{Cursor, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

/// Reader over an already-validated manifest file: holds the parsed
/// footer payload and an open file handle for on-demand section
/// reads. Construct via [`ManifestArchiveReader::open`].
pub struct ManifestArchiveReader {
    /// Path the file was opened from, used to enrich error messages
    /// and to support reopening in tests.
    path: PathBuf,

    /// Open file handle, positioned arbitrarily; [`read_section`]
    /// seeks each time and reads exactly one Block per call so
    /// successive lookups do not depend on the cursor.
    file: Box<dyn FsFile>,

    /// Verified footer payload — the table of contents that every
    /// [`read_section`] / [`section`] lookup consults.
    footer: FooterPayload,

    /// Path that produced the footer: `Tail` (preferred) or `Head`
    /// (recovered from the 4 KiB mirror). Surfaced for diagnostics
    /// and corruption-matrix tests.
    source: FooterSource,

    /// Snapshot of the runtime config in effect when the reader
    /// opened the file. Used to pick which `BlockTransform`
    /// variant to hand to `Block::from_reader` (`Plain` vs
    /// `PlainEcc` vs `Encrypted` vs `EncryptedEcc`). The Block layer
    /// itself parses ECC presence from the per-Block header's
    /// `ecc_length` field — the runtime snapshot does NOT control
    /// whether the decoder accepts ECC bytes, only which decoder
    /// arm gets called. A manifest written with `page_ecc=true`
    /// still decodes correctly if the live tree has since toggled
    /// the flag off, because `BlockTransform::Plain` and
    /// `BlockTransform::PlainEcc` both go through the same
    /// header-driven verify path.
    runtime: Arc<RuntimeConfig>,

    /// Optional encryption provider — mirrors the writer's
    /// `Config::encryption` plumbing. When `Some`, per-section
    /// `Block::from_reader` runs through the AEAD pipeline; when
    /// `None`, plaintext.
    encryption: Option<Arc<dyn EncryptionProvider>>,
}

// Manual `Debug` impl skips the `file` field because
// `Box<dyn FsFile>` does not itself implement `Debug`. Path,
// source, and footer give a test failure or log entry enough
// context to reproduce.
impl core::fmt::Debug for ManifestArchiveReader {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ManifestArchiveReader")
            .field("path", &self.path)
            .field("source", &self.source)
            .field("footer", &self.footer)
            .finish_non_exhaustive()
    }
}

/// Which copy of the footer the reader actually loaded.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FooterSource {
    /// Loaded from the tail (primary path). Used in the happy case
    /// where the tail Block XXH3 verified and the payload parsed.
    Tail,

    /// Loaded from the head mirror at offset 0 because the tail
    /// path failed. Indicates either a partial-write recovery (the
    /// tail write was interrupted) or bit-rot in the tail region.
    /// Callers may want to log a warning, but recovery is automatic.
    Head,
}

impl ManifestArchiveReader {
    /// Open `path` and locate the footer using the tail-first
    /// protocol described in the module docs.
    ///
    /// # Errors
    ///
    /// - [`crate::Error::Io`] on read / seek / open failure
    /// - [`crate::Error::ManifestFooterInvalid`] when both the tail
    ///   and head paths fail to produce a valid footer payload. The
    ///   underlying per-path failures (Block I/O, XXH3 mismatch,
    ///   AEAD decryption failure, footer-payload structural error)
    ///   are logged at `error` level with the path that produced
    ///   them and then collapsed into this single variant; callers
    ///   that need to distinguish decrypt-vs-checksum-vs-structural
    ///   failure should consult the log. The collapse is
    ///   intentional: callers above this layer treat any
    ///   double-path failure as "manifest is unreadable, surface
    ///   recovery options" rather than branching on per-path cause.
    /// - [`crate::Error::Unrecoverable`] when the file is too small
    ///   to contain even the trailing footer-size hint
    pub fn open(
        path: &Path,
        fs: &dyn Fs,
        runtime: Arc<RuntimeConfig>,
        encryption: Option<Arc<dyn EncryptionProvider>>,
    ) -> crate::Result<Self> {
        let mut file = fs.open(path, &FsOpenOptions::new().read(true))?;
        let file_len = file_size(fs, path)?;

        if file_len < HEAD_FOOTER_RESERVED_SIZE + TAIL_FOOTER_SIZE_HINT_BYTES {
            return Err(crate::Error::Unrecoverable);
        }

        // Compute the per-Block transform once — both the
        // tail-footer and head-mirror reads use it, as do all
        // subsequent `read_section` calls. The transform borrows
        // the encryption provider through `as_deref()`, so it
        // can't outlive `self.encryption`; we construct a local
        // copy each time the borrow is needed.
        let footer_transform = build_transform(&runtime, encryption.as_deref());

        // ---- Tail path (primary) ----------------------------------
        let tail_err = match read_tail_footer(&mut file, file_len, &footer_transform) {
            Ok(footer) => {
                return Ok(Self {
                    path: path.to_path_buf(),
                    file,
                    footer,
                    source: FooterSource::Tail,
                    runtime,
                    encryption,
                });
            }
            Err(err) => {
                // We are about to retry from the head mirror; do not
                // return the tail error yet. Log it at debug so
                // operators can correlate corruption-matrix events
                // without crashing.
                log::debug!(
                    "manifest tail footer read failed for {}: {err:?}; trying head mirror",
                    path.display(),
                );
                err
            }
        };

        // ---- Head mirror (fallback) ------------------------------
        match read_head_footer(&mut file, &footer_transform) {
            Ok(footer) => Ok(Self {
                path: path.to_path_buf(),
                file,
                footer,
                source: FooterSource::Head,
                runtime,
                encryption,
            }),
            Err(head_err) => {
                log::error!(
                    "manifest unrecoverable for {}: tail err = {:?}, head err = {head_err:?}",
                    path.display(),
                    tail_err,
                );
                Err(crate::Error::ManifestFooterInvalid(
                    "both tail and head mirror failed to produce a valid footer payload",
                ))
            }
        }
    }

    /// Which footer copy the reader loaded. Useful for tests and
    /// observability hooks that want to count fallback events.
    #[must_use]
    pub fn source(&self) -> FooterSource {
        self.source
    }

    /// Borrow the verified footer payload — exposes
    /// [`FooterPayload::section`] for callers that want to walk
    /// the TOC directly.
    #[must_use]
    pub fn footer(&self) -> &FooterPayload {
        &self.footer
    }

    /// Path the reader was opened from.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Look up a section by name and return a borrowed TOC entry.
    /// Shortcut for `self.footer().section(name)`.
    #[must_use]
    pub fn section(&self, name: &str) -> Option<&TocEntry> {
        self.footer.section(name)
    }

    /// Read and verify the section Block for `name`, returning the
    /// decoded payload bytes. Each call performs one seek + one
    /// `Block::from_reader`; no internal caching, so callers that
    /// repeatedly request the same section should keep the bytes
    /// themselves.
    ///
    /// # Errors
    ///
    /// - [`crate::Error::ManifestFooterInvalid`] when `name` is not
    ///   in the TOC
    /// - propagates Block I/O / XXH3 / decryption errors when the
    ///   section Block fails verification
    pub fn read_section(&mut self, name: &str) -> crate::Result<Vec<u8>> {
        let entry = self
            .footer
            .section(name)
            .ok_or(crate::Error::ManifestFooterInvalid(
                "requested section name not in TOC",
            ))?;
        let block_offset = entry.block_offset;
        let block_size = entry.block_size;

        // Bound `block_size` against both an absolute cap and the
        // actual file length BEFORE allocating the buffer. The TOC
        // lives inside a XXH3-verified footer Block so legitimate
        // writers never put a forged size here, but a malicious
        // manifest could; without these checks `vec![0u8; block_size
        // as usize]` would allocate up to 4 GiB on a single Block
        // read and crash the process before any Block-level
        // verification fires. The absolute cap fires first because
        // the file-len check alone tolerates a 16-GiB v{N} file
        // pre-sized via sparse holes.
        if block_size > MAX_MANIFEST_BLOCK_SIZE {
            return Err(crate::Error::ManifestFooterInvalid(
                "TOC entry block_size exceeds MAX_MANIFEST_BLOCK_SIZE",
            ));
        }
        let file_len = self.file.metadata()?.len;
        let end = block_offset.checked_add(u64::from(block_size)).ok_or(
            crate::Error::ManifestFooterInvalid("TOC entry overflows u64 file offset"),
        )?;
        if end > file_len {
            return Err(crate::Error::ManifestFooterInvalid(
                "TOC entry extends past end of manifest file",
            ));
        }

        self.file.seek(SeekFrom::Start(block_offset))?;
        let mut block_bytes = vec![0u8; block_size as usize];
        self.file.read_exact(&mut block_bytes)?;

        let identity = BlockIdentity {
            tree_id: MANIFEST_TREE_ID_SENTINEL,
            table_id: MANIFEST_TABLE_ID_SENTINEL,
            block_offset,
            block_type: BlockType::Manifest,
            dict_id: 0,
            window_log: 0,
        };
        let block = Block::from_reader(
            &mut Cursor::new(&block_bytes),
            identity,
            &build_transform(&self.runtime, self.encryption.as_deref()),
        )?;
        // Defence in depth: `Block::from_reader` authenticates the
        // header's `block_type` against the `identity.block_type` we
        // passed (AAD-bound for encrypted, checksum-bound for plain),
        // so a forged TOC pointing at a non-Manifest block can't slip
        // past with matching crypto. The explicit assertion below
        // turns "decode succeeded but block_type drifted" into a
        // typed error rather than a confusing downstream parse
        // failure on data that happens to look section-shaped.
        if block.header.block_type != BlockType::Manifest {
            return Err(crate::Error::ManifestFooterInvalid(
                "TOC entry points at non-Manifest block",
            ));
        }
        Ok(block.data.to_vec())
    }
}

/// Construct the per-Block transform a reader / open path should
/// use, given the captured runtime + optional encryption. Mirrors
/// [`crate::manifest_blocks::writer::ManifestArchiveWriter::block_transform`]
/// so writer and reader agree on the encryption / ECC matrix.
fn build_transform<'a>(
    runtime: &RuntimeConfig,
    encryption: Option<&'a dyn EncryptionProvider>,
) -> BlockTransform<'a> {
    #[cfg(feature = "page_ecc")]
    let ecc_on = runtime.manifest_ecc();
    #[cfg(not(feature = "page_ecc"))]
    let _ = runtime;
    #[cfg(not(feature = "page_ecc"))]
    let ecc_on = false;

    match (ecc_on, encryption) {
        #[cfg(feature = "page_ecc")]
        (true, Some(enc)) => BlockTransform::EncryptedEcc(enc),
        #[cfg(feature = "page_ecc")]
        (true, None) => BlockTransform::PlainEcc,
        (_, Some(enc)) => BlockTransform::Encrypted(enc),
        (_, None) => BlockTransform::PLAIN,
    }
}

/// Try to load the footer from the tail of the file.
fn read_tail_footer(
    file: &mut Box<dyn FsFile>,
    file_len: u64,
    transform: &BlockTransform<'_>,
) -> crate::Result<FooterPayload> {
    // Read the trailing size hint (last 4 bytes).
    file.seek(SeekFrom::Start(file_len - TAIL_FOOTER_SIZE_HINT_BYTES))?;
    let footer_size = u64::from(file.read_u32::<LittleEndian>()?);

    // Validate the hint: footer Block must fit BETWEEN the head
    // reservation and the trailing size hint.
    let max_footer = file_len
        .checked_sub(HEAD_FOOTER_RESERVED_SIZE)
        .and_then(|len_after_head| len_after_head.checked_sub(TAIL_FOOTER_SIZE_HINT_BYTES))
        .ok_or(crate::Error::ManifestFooterInvalid(
            "file too small to hold footer between head reservation and size hint",
        ))?;
    if footer_size == 0 || footer_size > max_footer {
        return Err(crate::Error::ManifestFooterInvalid(
            "trailing footer-size hint out of bounds",
        ));
    }
    // Hard ceiling per design Q12: the footer Block is bounded at
    // HEAD_FOOTER_RESERVED_SIZE (4 KiB). Realistic production
    // manifests use ~5% of that space; any value above 4 KiB is a
    // writer bug or a forged manifest. Reject it before the
    // allocation below so a multi-MiB footer_size cannot bait the
    // reader into a giant `vec![0u8; footer_size]`.
    if footer_size > HEAD_FOOTER_RESERVED_SIZE {
        return Err(crate::Error::ManifestFooterInvalid(
            "footer-size hint exceeds HEAD_FOOTER_RESERVED_SIZE",
        ));
    }

    let footer_offset = file_len
        .checked_sub(TAIL_FOOTER_SIZE_HINT_BYTES)
        .and_then(|x| x.checked_sub(footer_size))
        .ok_or(crate::Error::ManifestFooterInvalid(
            "trailing footer-size hint underflows file length",
        ))?;
    file.seek(SeekFrom::Start(footer_offset))?;
    // Read the footer Block into a buffer first so we can pass an
    // owned `Cursor` to `Block::from_reader` (which requires
    // `Sized`). The footer is bounded at 4 KiB by the writer-side
    // check, so the buffer is small.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "footer_size <= HEAD_FOOTER_RESERVED_SIZE = 4 KiB, fits any platform's usize"
    )]
    let mut footer_buf = vec![0u8; footer_size as usize];
    file.read_exact(&mut footer_buf)?;
    let identity = BlockIdentity {
        tree_id: MANIFEST_TREE_ID_SENTINEL,
        table_id: MANIFEST_TABLE_ID_SENTINEL,
        block_offset: footer_offset,
        block_type: BlockType::ManifestFooter,
        dict_id: 0,
        window_log: 0,
    };
    let block = Block::from_reader(&mut Cursor::new(&footer_buf), identity, transform)?;
    if block.header.block_type != BlockType::ManifestFooter {
        return Err(crate::Error::ManifestFooterInvalid(
            "tail footer slot decoded as non-ManifestFooter block",
        ));
    }
    FooterPayload::decode(&block.data[..])
}

/// Try to load the footer from the head mirror at offset 0.
fn read_head_footer(
    file: &mut Box<dyn FsFile>,
    transform: &BlockTransform<'_>,
) -> crate::Result<FooterPayload> {
    file.seek(SeekFrom::Start(0))?;
    // Read the whole 4 KiB reservation. The Block decoder is
    // self-bounding via the header's declared length, so the
    // trailing zero pad is harmless — Block::from_reader will stop
    // reading once it has consumed the declared payload.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "HEAD_FOOTER_RESERVED_SIZE = 4 KiB, fits in usize on every supported target"
    )]
    let mut head_buf = vec![0u8; HEAD_FOOTER_RESERVED_SIZE as usize];
    file.read_exact(&mut head_buf)?;

    // Empty reservation: writer ran with manifest_footer_mirror=false.
    if head_buf.iter().all(|&b| b == 0) {
        return Err(crate::Error::ManifestFooterInvalid(
            "head mirror unpopulated (manifest_footer_mirror was off at write)",
        ));
    }

    let identity = BlockIdentity {
        tree_id: MANIFEST_TREE_ID_SENTINEL,
        table_id: MANIFEST_TABLE_ID_SENTINEL,
        block_offset: 0,
        block_type: BlockType::ManifestFooter,
        dict_id: 0,
        window_log: 0,
    };
    let block = Block::from_reader(&mut Cursor::new(&head_buf), identity, transform)?;
    if block.header.block_type != BlockType::ManifestFooter {
        return Err(crate::Error::ManifestFooterInvalid(
            "head mirror slot decoded as non-ManifestFooter block",
        ));
    }
    FooterPayload::decode(&block.data[..])
}

/// Query the file's on-disk size via the FS metadata API. Used at
/// open to bound the tail-footer lookup before any read.
fn file_size(fs: &dyn Fs, path: &Path) -> crate::Result<u64> {
    Ok(fs.metadata(path)?.len)
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::items_after_statements,
    reason = "tests panic on failure paths to surface bugs loudly; \
              localized `use std::io::Write;` reads natural at the call site"
)]
mod tests {
    use super::*;
    use crate::{
        fs::MemFs, manifest_blocks::writer::ManifestArchiveWriter, runtime_config::RuntimeConfig,
    };
    use std::sync::Arc;

    fn fresh_fs() -> MemFs {
        let fs = MemFs::new();
        fs.create_dir_all(Path::new("/m")).unwrap();
        fs
    }

    fn write_manifest(fs: &MemFs, path: &Path, runtime: RuntimeConfig, sections: &[(&str, &[u8])]) {
        let mut w = ManifestArchiveWriter::create(path, fs, Arc::new(runtime), None).unwrap();
        for (name, data) in sections {
            w.start(name).unwrap();
            use std::io::Write;
            w.write_all(data).unwrap();
        }
        w.finish().unwrap();
    }

    #[test]
    fn reader_opens_clean_manifest_via_tail() {
        // Happy path: writer emits a valid manifest, reader picks
        // it up via the tail (no head fallback needed). Locks the
        // primary read path and verifies source reporting.
        let fs = fresh_fs();
        let path = Path::new("/m/clean");
        write_manifest(
            &fs,
            path,
            RuntimeConfig::default(),
            &[("format_version", &[5]), ("tree_type", &[0])],
        );

        let reader = ManifestArchiveReader::open(
            path,
            &fs,
            std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
            None,
        )
        .unwrap();
        assert_eq!(reader.source(), FooterSource::Tail);
        assert!(reader.section("format_version").is_some());
        assert!(reader.section("tree_type").is_some());
        assert!(reader.section("nonexistent").is_none());
    }

    #[test]
    fn reader_reads_section_bytes_verbatim() {
        // Section roundtrip: bytes written via Write::write_all
        // come back byte-for-byte via read_section. Locks the
        // section payload contract — without this, manifest's
        // logical metadata (FormatVersion, TreeType, etc.) would
        // drift between write and read.
        let fs = fresh_fs();
        let path = Path::new("/m/roundtrip");
        write_manifest(
            &fs,
            path,
            RuntimeConfig::default(),
            &[
                ("format_version", &[5]),
                ("comparator_name", b"u64-big-endian"),
            ],
        );

        let mut reader = ManifestArchiveReader::open(
            path,
            &fs,
            std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
            None,
        )
        .unwrap();
        assert_eq!(reader.read_section("format_version").unwrap(), vec![5]);
        assert_eq!(
            reader.read_section("comparator_name").unwrap(),
            b"u64-big-endian".to_vec(),
        );
    }

    #[test]
    fn reader_falls_back_to_head_mirror_when_tail_corrupt() {
        // Simulate tail corruption by clobbering the last bytes of
        // the file (which includes the trailing size hint AND the
        // tail footer Block bytes). The reader's tail path must
        // fail, then succeed via the head mirror. Locks the core
        // partial-write recovery guarantee.
        let fs = fresh_fs();
        let path = Path::new("/m/tail_corrupt");
        write_manifest(
            &fs,
            path,
            RuntimeConfig::default(), // manifest_footer_mirror = true (default)
            &[("format_version", &[5])],
        );

        // Corrupt the trailing bytes: overwrite the size hint with
        // garbage that points outside the file.
        let mut file = fs
            .open(path, &FsOpenOptions::new().write(true).read(true))
            .unwrap();
        let size = file.metadata().unwrap().len;
        file.seek(SeekFrom::Start(size - 4)).unwrap();
        use std::io::Write;
        file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let reader = ManifestArchiveReader::open(
            path,
            &fs,
            std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
            None,
        )
        .unwrap();
        assert_eq!(
            reader.source(),
            FooterSource::Head,
            "reader should have fallen back to the head mirror"
        );
        assert!(reader.section("format_version").is_some());
    }

    #[test]
    fn reader_fails_when_tail_corrupt_and_no_mirror() {
        // With manifest_footer_mirror=false, the head reservation
        // is all zeros — corruption of the tail leaves no fallback,
        // so the reader must return ManifestFooterInvalid (not
        // crash, not return an inconsistent manifest).
        let fs = fresh_fs();
        let path = Path::new("/m/tail_corrupt_no_mirror");
        let runtime = RuntimeConfig {
            manifest_footer_mirror: false,
            ..RuntimeConfig::default()
        };
        write_manifest(&fs, path, runtime, &[("format_version", &[5])]);

        // Same tail corruption as above.
        let mut file = fs
            .open(path, &FsOpenOptions::new().write(true).read(true))
            .unwrap();
        let size = file.metadata().unwrap().len;
        file.seek(SeekFrom::Start(size - 4)).unwrap();
        use std::io::Write;
        file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let err = ManifestArchiveReader::open(
            path,
            &fs,
            std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
            None,
        )
        .expect_err("must reject");
        assert!(matches!(err, crate::Error::ManifestFooterInvalid(_)));
    }

    #[test]
    fn reader_rejects_request_for_missing_section() {
        // read_section on an unknown name returns
        // ManifestFooterInvalid rather than Io or a panic. Locks
        // the contract that callers can probe sections defensively.
        let fs = fresh_fs();
        let path = Path::new("/m/missing");
        write_manifest(&fs, path, RuntimeConfig::default(), &[("a", &[1])]);

        let mut reader = ManifestArchiveReader::open(
            path,
            &fs,
            std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
            None,
        )
        .unwrap();
        let err = reader
            .read_section("does_not_exist")
            .expect_err("missing section must error");
        assert!(matches!(err, crate::Error::ManifestFooterInvalid(_)));
    }

    #[test]
    fn reader_isolates_corruption_to_one_section_other_sections_readable() {
        // Acceptance criterion (#297 corruption matrix): a bit-flip
        // inside one section Block must fail verification for THAT
        // section while leaving the other sections + footer readable.
        // The TOC lives in the (separate) footer Block; per-section
        // XXH3 isolates the failure radius to a single Block.
        let fs = fresh_fs();
        let path = Path::new("/m/isolated");
        write_manifest(
            &fs,
            path,
            RuntimeConfig::default(),
            &[
                ("a", &[1, 2, 3, 4]),
                ("b", &[5, 6, 7, 8]),
                ("c", &[9, 10, 11, 12]),
            ],
        );

        // Reader before corruption: open + look up TOC entry for "b"
        // so we know the on-disk offset to flip a byte at.
        let b_offset = {
            let reader =
                ManifestArchiveReader::open(path, &fs, Arc::new(RuntimeConfig::default()), None)
                    .unwrap();
            let entry = reader.section("b").expect("b section is in TOC");
            entry.block_offset
        };

        // Flip one byte inside section b's payload region. Offset
        // well past the Block header so the bit is in the
        // checksummed payload.
        {
            let mut file = fs
                .open(path, &FsOpenOptions::new().write(true).read(true))
                .unwrap();
            file.seek(SeekFrom::Start(b_offset + 40)).unwrap();
            let mut byte = [0u8; 1];
            file.read_exact(&mut byte).unwrap();
            file.seek(SeekFrom::Start(b_offset + 40)).unwrap();
            file.write_all(&[byte[0] ^ 0xFF]).unwrap();
            file.sync_all().unwrap();
        }

        // Reopen — footer + TOC still load (different Block), sections
        // a and c still verify, only b fails.
        let mut reader =
            ManifestArchiveReader::open(path, &fs, Arc::new(RuntimeConfig::default()), None)
                .unwrap();
        let a_bytes = reader.read_section("a").unwrap();
        assert_eq!(a_bytes, vec![1, 2, 3, 4], "section a survives");
        let c_bytes = reader.read_section("c").unwrap();
        assert_eq!(c_bytes, vec![9, 10, 11, 12], "section c survives");
        let b_err = reader
            .read_section("b")
            .expect_err("section b decoded but should have failed XXH3");
        // Don't pin the exact error variant — different bit positions
        // surface as ChecksumMismatch, InvalidHeader, or Io depending
        // on what got flipped. The contract is: SOMETHING surfaces
        // and the other sections still work.
        log::debug!("section b corruption surfaced as: {b_err:?}");
    }

    #[test]
    fn reader_rejects_files_smaller_than_head_plus_hint() {
        // A file that is structurally too small to even hold the
        // head reservation + 4-byte trailer hint is unrecoverable
        // — Unrecoverable rather than ManifestFooterInvalid so
        // callers can route the "file is truncated / not a
        // manifest" case differently from "file is a manifest but
        // its structure is bad".
        let fs = fresh_fs();
        let path = Path::new("/m/too_small");
        let mut file = fs
            .open(path, &FsOpenOptions::new().write(true).create_new(true))
            .unwrap();
        use std::io::Write;
        file.write_all(&[0u8; 100]).unwrap(); // way under 4 KiB + 4 bytes
        file.sync_all().unwrap();
        drop(file);

        let err = ManifestArchiveReader::open(
            path,
            &fs,
            std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
            None,
        )
        .expect_err("must reject");
        assert!(matches!(err, crate::Error::Unrecoverable));
    }
}
