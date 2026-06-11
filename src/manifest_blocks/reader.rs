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
//! 3. **Double failure.** Both paths failed → return
//!    [`crate::Error::ManifestFooterInvalid`]. Per-path causes
//!    (Block I/O, XXH3 mismatch, AEAD decryption, structural parse
//!    error) are logged at `error` level with the path that
//!    produced them and collapsed into this single variant so
//!    callers above the manifest layer can route any
//!    double-path failure as "manifest is unreadable" without
//!    branching per cause. [`crate::Error::Unrecoverable`] is
//!    reserved for the narrower case of a file shorter than the
//!    head reservation itself (`< HEAD_FOOTER_RESERVED_SIZE`) —
//!    no head mirror exists in that state, so neither path can
//!    run. A file between `HEAD_FOOTER_RESERVED_SIZE` and
//!    `HEAD_FOOTER_RESERVED_SIZE + TAIL_FOOTER_SIZE_HINT_BYTES`
//!    is valid for the head fallback (tail simply skipped).
//!
//! Per-section reads use the TOC-recorded `(block_offset, block_size)`
//! pair to read exactly one Block per call without scanning.
//!
//! [`Block::from_reader`]: crate::table::block::Block::from_reader
//! [`FooterPayload`]: crate::manifest_blocks::footer::FooterPayload
//! [`HEAD_FOOTER_RESERVED_SIZE`]: crate::manifest_blocks::HEAD_FOOTER_RESERVED_SIZE

#[cfg(not(feature = "std"))]
use crate::io::{Cursor, Read, Seek, SeekFrom};
use crate::io::{LittleEndian, ReadBytesExt};
use crate::path::{Path, PathBuf};
use crate::{
    encryption::EncryptionProvider,
    fs::{Fs, FsFile, FsOpenOptions},
    manifest_blocks::{
        HEAD_FOOTER_RESERVED_SIZE, MANIFEST_TABLE_ID_SENTINEL, MAX_MANIFEST_BLOCK_SIZE,
        TAIL_FOOTER_SIZE_HINT_BYTES,
        footer::{FooterPayload, TocEntry},
    },
    runtime_config::RuntimeConfig,
    table::block::{Block, BlockIdentity, BlockTransform, BlockType, Header},
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};
#[cfg(feature = "std")]
use std::io::{Cursor, Read, Seek, SeekFrom};

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
    /// `ECC_PARITY` flag (the parity length is then derived from
    /// `data_length`) — the runtime snapshot does NOT control whether
    /// the decoder accepts ECC bytes, only which decoder arm gets
    /// called. A manifest written with `page_ecc=true` still decodes
    /// correctly if the live tree has since toggled the flag off,
    /// because the per-Block `ECC_PARITY` flag is self-describing and
    /// both `BlockTransform::Plain` and `BlockTransform::PlainEcc` go
    /// through the same header-driven verify path.
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
    /// - [`crate::Error::Unrecoverable`] when the file is shorter
    ///   than [`HEAD_FOOTER_RESERVED_SIZE`]: with no head mirror to
    ///   fall back to, neither recovery path can produce a footer.
    ///   Files at or above the head reservation but below
    ///   `HEAD + TAIL_FOOTER_SIZE_HINT_BYTES` are accepted and
    ///   routed straight to the head-mirror fallback (no tail to
    ///   try) — the partial-write recovery case where the tail was
    ///   truncated back to the head reservation.
    pub fn open(
        path: &Path,
        fs: &dyn Fs,
        runtime: Arc<RuntimeConfig>,
        encryption: Option<Arc<dyn EncryptionProvider>>,
    ) -> crate::Result<Self> {
        let mut file = fs.open(path, &FsOpenOptions::new().read(true))?;
        let file_len = file_size(fs, path)?;

        // Front gate: file must hold at least the head-mirror
        // reservation. Anything shorter cannot recover via either
        // path (no head mirror to fall back to). Files between
        // HEAD_FOOTER_RESERVED_SIZE and HEAD + HINT are valid for
        // the head-fallback path even though the tail size hint is
        // missing — that's the partial-write recovery case
        // (manifest truncated mid-tail-write but head mirror
        // already populated from a prior successful seal).
        if file_len < HEAD_FOOTER_RESERVED_SIZE {
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
        // Only attempt the tail when the file has room for both the
        // head reservation AND the trailing size hint. Without the
        // hint, `read_tail_footer` would underflow / read garbage —
        // skip straight to the head fallback in that case.
        let tail_err = if file_len < HEAD_FOOTER_RESERVED_SIZE + TAIL_FOOTER_SIZE_HINT_BYTES {
            log::debug!(
                "manifest {} is too short for a tail size hint ({} bytes); skipping tail path, trying head mirror",
                path.display(),
                file_len,
            );
            crate::Error::ManifestFooterInvalid(
                "tail size hint absent — file truncated to head reservation only",
            )
        } else {
            match read_tail_footer(&mut file, file_len, &footer_transform) {
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
    /// - [`crate::Error::ManifestSectionInvalid`] when `name` is
    ///   not in the TOC, when the section Block's inner header
    ///   declares a payload larger than the TOC slot, or when
    ///   the decoded Block carries a non-`Manifest` block type
    /// - [`crate::Error::ManifestFooterInvalid`] when the TOC
    ///   entry's `(block_offset, block_size)` is itself bogus
    ///   (overflows `u64`, extends past EOF, exceeds
    ///   `MAX_MANIFEST_BLOCK_SIZE`) — TOC bytes live in the
    ///   footer payload, so a malformed entry is a footer-layer
    ///   issue even though it surfaces in this method
    /// - propagates Block I/O / XXH3 / decryption errors when the
    ///   section Block fails verification
    pub fn read_section(&mut self, name: &str) -> crate::Result<Vec<u8>> {
        let entry = self
            .footer
            .section(name)
            .ok_or(crate::Error::ManifestSectionInvalid(
                "requested section name not in TOC",
            ))?;
        let block_offset = entry.block_offset;
        let block_size = entry.block_size;
        let expected_section_checksum = entry.section_checksum;

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

        validate_block_header_fits(&block_bytes, HeaderContext::SectionExact)?;
        let identity = BlockIdentity {
            table_id: MANIFEST_TABLE_ID_SENTINEL,
            block_type: BlockType::Manifest,
            dict_id: 0,
            window_log: 0,
        };
        let block = Block::from_reader(
            &mut Cursor::new(&block_bytes),
            identity,
            &build_transform(&self.runtime, self.encryption.as_deref()),
        )?;
        // Defence in depth: `Block::from_reader` today does NOT bind
        // `header.block_type` to the caller-supplied `identity.block_type`
        // (the `identity` arg is plumbed for the AAD-bound encrypt
        // path that's still landing — see `encryption::aad::build` and
        // the wiring TODO in `encryption::mod.rs`). So a forged TOC
        // entry pointing at a non-Manifest block whose XXH3 happens to
        // verify would currently slip past `from_reader`. The explicit
        // assertion below is what enforces the expected type; without
        // it a TOC-redirect attack or a writer-side mix-up would
        // surface as a confusing downstream parse failure on data that
        // happens to look section-shaped. When AAD-binding lands and
        // `from_reader` rejects type mismatch internally, this check
        // becomes redundant defence-in-depth (cheap; keep it).
        if block.header.block_type != BlockType::Manifest {
            return Err(crate::Error::ManifestSectionInvalid(
                "TOC entry points at non-Manifest block",
            ));
        }
        // Cross-check the decoded Block's own XXH3-128 against the
        // value the TOC entry committed at write time. Without this,
        // CURRENT-pointer validation only proves the footer payload
        // is self-consistent (TOC bytes intact) — a forged or
        // mis-targeted TOC entry could still point at a Block whose
        // content differs from what the writer recorded. The TOC's
        // `section_checksum` is what we hash into the CURRENT
        // pointer's canonical digest, so binding it here makes the
        // chain CURRENT → TOC → section bytes airtight.
        if block.header.checksum.into_u128() != expected_section_checksum {
            return Err(crate::Error::ManifestSectionInvalid(
                "section Block checksum does not match TOC entry section_checksum",
            ));
        }
        Ok(block.data.to_vec())
    }
}

/// Which manifest layer is asking [`validate_block_header_fits`]
/// to peek the inner Block header. Drives error classification:
/// footer/head-mirror failures stay in
/// [`crate::Error::ManifestFooterInvalid`] (whole-manifest
/// discovery is broken); section failures route through
/// [`crate::Error::ManifestSectionInvalid`] so callers can
/// distinguish "this one section is bad" from "the manifest
/// is unreadable".
#[derive(Copy, Clone)]
enum HeaderContext {
    /// Inner header lives in a section Block whose outer slot is sized
    /// EXACTLY by its TOC entry. The declared on-disk size must equal the
    /// slot length — an understated header would let `Block::from_reader`
    /// consume only the declared bytes and silently ignore the trailing
    /// remainder (e.g. a stripped `ECC_PARITY` trailer), which
    /// `Block::from_file` rejects. Failures classify as
    /// `ManifestSectionInvalid`.
    SectionExact,
    /// Inner header lives in the tail-footer Block, read into a buffer
    /// sized EXACTLY by the trailing size hint. Same exact-fit contract as
    /// [`Self::SectionExact`]. Failures classify as `ManifestFooterInvalid`.
    FooterExact,
    /// Inner header lives in the head-mirror footer, read into the fixed
    /// `HEAD_FOOTER_RESERVED_SIZE` reservation that is intentionally larger
    /// than the footer Block and zero-padded. Here the declared size may be
    /// smaller than the buffer (the trailing pad is expected), so only the
    /// over-read direction is rejected. Failures classify as
    /// `ManifestFooterInvalid`.
    FooterPadded,
}

/// Peek the Block header from `buf` and refuse to delegate to
/// [`Block::from_reader`] if the declared on-disk payload + parity
/// trailer would exceed the buffer length.
///
/// Why: `Block::from_reader` itself trusts `header.data_length` (and the
/// parity length derived from it) up to its own 256 MiB ceiling. A manifest
/// section / footer that was already capped by the caller (TOC
/// `block_size`, tail size hint, or `HEAD_FOOTER_RESERVED_SIZE`)
/// can still nest a forged header inside that smaller window
/// claiming a much larger payload, and the Block layer will
/// allocate a multi-MiB Vec before discovering the bounded buffer
/// runs out. Pre-validating the header here turns that allocation
/// surge into a typed manifest-layer error at the caller's
/// existing budget, without changing the Block decoder.
///
/// Uses [`crate::coding::Decode::decode_from`] on a borrowed slice
/// so the cost is one fixed-size header parse (cheap; well under
/// 50 bytes) before the main read path runs.
fn validate_block_header_fits(buf: &[u8], ctx: HeaderContext) -> crate::Result<()> {
    use crate::coding::Decode;
    let wrap = |msg: &'static str| -> crate::Error {
        match ctx {
            HeaderContext::SectionExact => crate::Error::ManifestSectionInvalid(msg),
            HeaderContext::FooterExact | HeaderContext::FooterPadded => {
                crate::Error::ManifestFooterInvalid(msg)
            }
        }
    };
    // Lower bound: every header is at least MIN_LEN; the exact length (with
    // or without the block_flags byte) is known only after the block_type is
    // decoded. Bound the decode cursor at MAX_LEN — `decode_from` reads only
    // the actual header for the parsed type and ignores any extra slot bytes.
    if buf.len() < Header::MIN_LEN {
        return Err(wrap("manifest Block buffer shorter than Block header"));
    }
    let cursor_end = Header::MAX_LEN.min(buf.len());
    let mut cursor = Cursor::new(
        buf.get(..cursor_end)
            .ok_or_else(|| wrap("manifest Block header slice unexpectedly short"))?,
    );
    // Map header-decode failures (InvalidTag for unknown
    // BlockType byte, Io for short reads, etc.) through the same
    // `wrap` closure so the surrounding context (section / footer)
    // owns the error variant. Without this remap, a malformed
    // inner header could leak as a generic `Io` / `InvalidTag`
    // and break the per-layer error classification CodeRabbit
    // asked us to enforce earlier.
    let header = Header::decode_from(&mut cursor).map_err(|_| {
        wrap("manifest Block header decode failed (truncated, unknown type, or invalid magic)")
    })?;
    // `on_disk_size` derives the parity-trailer length from `data_length`
    // plus the `ECC_PARITY` presence flag (there is no stored `ecc_length`);
    // it saturates in u32 rather than overflowing, so a forged oversized
    // `data_length` saturates to u32::MAX and is rejected by the bound check
    // below against the (much smaller) buffer length.
    let declared = u64::from(header.on_disk_size());
    let buf_len = buf.len() as u64;
    match ctx {
        // Exact-fit slots: the buffer IS the block (TOC `block_size` or the
        // tail-footer size hint). An understated header would let
        // `Block::from_reader` consume only `declared` bytes and silently
        // drop the remainder, accepting a block `Block::from_file` rejects.
        // Require an exact match in both directions.
        HeaderContext::SectionExact | HeaderContext::FooterExact => {
            if declared != buf_len {
                return Err(wrap(
                    "manifest Block header on-disk size does not match its exact slot",
                ));
            }
        }
        // Padded slot: the head-mirror reservation is intentionally larger
        // than the footer Block and zero-padded, so a smaller declared size
        // is expected. Reject an over-read past the buffer AND any non-zero
        // byte past the declared size: a forged smaller `declared` would let
        // `Block::from_reader` stop early and silently treat non-zero trailing
        // bytes (e.g. a stripped ECC trailer copied into the mirror) as
        // padding. Only genuine zero padding is acceptable here.
        HeaderContext::FooterPadded => {
            if declared > buf_len {
                return Err(wrap(
                    "manifest Block header declares on-disk size larger than buffer",
                ));
            }
            #[expect(
                clippy::cast_possible_truncation,
                reason = "declared <= buf_len = buf.len(), so it fits usize"
            )]
            let declared_usize = declared as usize;
            if buf
                .get(declared_usize..)
                .unwrap_or(&[])
                .iter()
                .any(|&b| b != 0)
            {
                return Err(wrap(
                    "manifest head-mirror footer has non-zero bytes past the declared block size",
                ));
            }
        }
    }
    Ok(())
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
        (true, Some(enc)) => {
            BlockTransform::EncryptedEcc(enc, crate::table::block::EccParams::RS_4_2)
        }
        #[cfg(feature = "page_ecc")]
        (true, None) => BlockTransform::PlainEcc(crate::table::block::EccParams::RS_4_2),
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
    validate_block_header_fits(&footer_buf, HeaderContext::FooterExact)?;
    let identity = BlockIdentity {
        table_id: MANIFEST_TABLE_ID_SENTINEL,
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

    validate_block_header_fits(&head_buf, HeaderContext::FooterPadded)?;
    let identity = BlockIdentity {
        table_id: MANIFEST_TABLE_ID_SENTINEL,
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

    #[test]
    fn validate_block_header_fits_rejects_understated_exact_slot() {
        use crate::coding::Encode;
        use crate::table::block::{BlockType, Header};

        // A valid (checksummed) header declaring data_length = 4 with no
        // parity, handed a slot LARGER than the block's on-disk size (extra
        // trailing bytes). For an exact-fit context the understated header
        // must be rejected: otherwise `Block::from_reader` consumes only the
        // declared bytes and silently ignores the trailing remainder (e.g. a
        // stripped ECC_PARITY trailer), which `Block::from_file` rejects.
        let header = Header {
            data_length: 4,
            uncompressed_length: 4,
            ..Header::test_dummy(BlockType::ManifestFooter)
        };
        let mut buf = header.encode_into_vec();
        buf.extend_from_slice(&[0u8; 4]); // declared payload
        buf.extend_from_slice(&[0xAB, 0xCD]); // extra trailing bytes beyond the block

        // on_disk_size = header (34) + data_length (4) = 38; buf.len() = 40.
        assert!(
            validate_block_header_fits(&buf, HeaderContext::SectionExact).is_err(),
            "an understated header in an exact-fit section slot must be rejected",
        );
        assert!(
            validate_block_header_fits(&buf, HeaderContext::FooterExact).is_err(),
            "an understated header in an exact-fit footer slot must be rejected",
        );
        // The padded head-mirror context tolerates a smaller declared size
        // ONLY when the trailing bytes are genuine zero padding. Here the
        // trailing bytes are non-zero (0xAB, 0xCD), so even the padded context
        // must reject them — otherwise a forged smaller declared size could
        // hide non-zero remainder (e.g. a stripped ECC trailer) as "padding".
        assert!(
            validate_block_header_fits(&buf, HeaderContext::FooterPadded).is_err(),
            "padded slot must reject non-zero bytes past the declared block size",
        );

        // Same understated header but with genuine zero padding past the
        // declared block: the padded head-mirror context accepts it.
        let mut zero_padded = header.encode_into_vec();
        zero_padded.extend_from_slice(&[0u8; 4]); // declared payload
        zero_padded.extend_from_slice(&[0u8; 2]); // zero pad beyond the block
        assert!(
            validate_block_header_fits(&zero_padded, HeaderContext::FooterPadded).is_ok(),
            "padded slot accepts a smaller declared size with genuine zero padding",
        );
    }

    fn write_manifest(fs: &MemFs, path: &Path, runtime: RuntimeConfig, sections: &[(&str, &[u8])]) {
        let mut w = ManifestArchiveWriter::create(
            path,
            fs,
            Arc::new(runtime),
            None,
            crate::fs::SyncMode::Normal,
        )
        .unwrap();
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
        // ManifestSectionInvalid rather than Io or a panic. Locks
        // the contract that callers can probe sections defensively
        // and distinguish per-section errors from footer-load
        // failures.
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
        assert!(matches!(err, crate::Error::ManifestSectionInvalid(_)));
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

        // Flip the first byte of section b's payload region (right after
        // the Block header) so the bit lands in the XXH3-checksummed
        // payload, not the header or an adjacent section. Manifest section
        // blocks are `BlockType::Manifest`, which carries the block_flags
        // byte, so the header is `header_len(Manifest)` bytes.
        let payload_off =
            b_offset + Header::header_len(crate::table::block::BlockType::Manifest) as u64;
        {
            let mut file = fs
                .open(path, &FsOpenOptions::new().write(true).read(true))
                .unwrap();
            file.seek(SeekFrom::Start(payload_off)).unwrap();
            let mut byte = [0u8; 1];
            file.read_exact(&mut byte).unwrap();
            file.seek(SeekFrom::Start(payload_off)).unwrap();
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
    fn reader_rejects_files_smaller_than_head_reservation() {
        // A file shorter than HEAD_FOOTER_RESERVED_SIZE (4 KiB)
        // has no head mirror to fall back to, so neither recovery
        // path can run — `Unrecoverable` rather than
        // `ManifestFooterInvalid` so callers can route "file is
        // truncated / not a manifest" differently from "file is a
        // manifest but its structure is bad". Files between
        // `HEAD_FOOTER_RESERVED_SIZE` and `HEAD + HINT` are NOT
        // rejected here — they reach the head-mirror fallback per
        // `reader_recovers_from_head_when_tail_hint_missing`.
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

    /// Future-regression guard: today the AEAD path in
    /// `EncryptionProvider::{encrypt, decrypt}` consumes only
    /// plaintext + nonce — AAD-binding to `BlockIdentity` (including
    /// `block_offset`) exists in `encryption::aad::build` /
    /// `encryption::block::{encrypt_block, decrypt_block}` but is
    /// not yet wired into the `Block::write_into` path the manifest
    /// writer uses. Because of that, byte-copying the tail footer
    /// Block into the head reservation today works fine: same nonce
    /// + same tag → head decrypts cleanly.
    ///
    /// This test asserts the head-mirror fallback round-trips with
    /// encryption on, locking that behaviour today. The day
    /// `Block::write_into` consumes the AAD-bound path (tracked
    /// separately) the head-mirror byte-copy must change to a fresh
    /// `Block::write_into` call at `block_offset = 0`, otherwise
    /// the head-slot decrypt will fail against AAD bound to the
    /// tail offset — and THIS TEST will be the one that catches it.
    /// See the parallel writer comment in `manifest_blocks::writer`
    /// for the exact code site to update.
    #[cfg(feature = "encryption")]
    #[test]
    fn reader_falls_back_to_head_mirror_for_encrypted_manifest() {
        use crate::encryption::{Aes256GcmProvider, EncryptionProvider};

        let fs = fresh_fs();
        let path = Path::new("/m/enc_tail_corrupt");
        let key = [42u8; 32];
        let enc: Arc<dyn EncryptionProvider> = Arc::new(Aes256GcmProvider::new(&key));

        let mut w = ManifestArchiveWriter::create(
            path,
            &fs,
            Arc::new(RuntimeConfig::default()),
            Some(Arc::clone(&enc)),
            crate::fs::SyncMode::Normal,
        )
        .unwrap();
        w.start("format_version").unwrap();
        use std::io::Write;
        w.write_all(&[5u8]).unwrap();
        w.finish().unwrap();

        // Same tail-corruption pattern as the plaintext head-mirror
        // test: clobber the trailing size hint so the tail path
        // fails and the reader has no choice but to fall back to
        // the head mirror.
        let mut file = fs
            .open(path, &FsOpenOptions::new().write(true).read(true))
            .unwrap();
        let size = file.metadata().unwrap().len;
        file.seek(SeekFrom::Start(size - 4)).unwrap();
        file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let reader =
            ManifestArchiveReader::open(path, &fs, Arc::new(RuntimeConfig::default()), Some(enc))
                .expect("encrypted head-mirror fallback must decrypt cleanly");
        assert_eq!(
            reader.source(),
            FooterSource::Head,
            "reader should have fallen back to the head mirror"
        );
        assert!(reader.section("format_version").is_some());
    }

    /// Regression: a manifest truncated to exactly
    /// `HEAD_FOOTER_RESERVED_SIZE` (4 KiB) has no tail footer Block
    /// and no trailing size hint, but the head mirror at offset 0
    /// is fully populated by the writer when
    /// `manifest_footer_mirror` is on. The reader should reach the
    /// head fallback in that state and succeed; rejecting with
    /// `Unrecoverable` at the front gate (because file is too short
    /// for a tail hint) defeats the advertised partial-write
    /// recovery contract. Only files shorter than the head
    /// reservation itself should bypass the head fallback.
    #[test]
    fn reader_recovers_from_head_when_tail_hint_missing() {
        let fs = fresh_fs();
        let path = Path::new("/m/head_only");
        write_manifest(
            &fs,
            path,
            RuntimeConfig::default(),
            &[("format_version", &[5])],
        );

        // Truncate the file back to exactly HEAD_FOOTER_RESERVED_SIZE.
        // After this, the head mirror is intact but the tail footer
        // Block + size hint are gone — the exact failure mode the
        // recovery contract has to handle.
        let file = fs
            .open(path, &FsOpenOptions::new().write(true).read(true))
            .unwrap();
        file.set_len(HEAD_FOOTER_RESERVED_SIZE).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let reader =
            ManifestArchiveReader::open(path, &fs, Arc::new(RuntimeConfig::default()), None)
                .expect("head-only manifest must recover via head fallback");
        assert_eq!(
            reader.source(),
            FooterSource::Head,
            "reader should have fallen back to the head mirror"
        );
        assert!(reader.section("format_version").is_some());
    }

    /// With `manifest_ecc` on (`RuntimeConfig` `page_ecc = true`), manifest
    /// blocks are written with a fixed RS(4,2) parity trailer and the reader
    /// must size + skip it to read the section bytes back verbatim. Exercises
    /// the `PlainEcc` arm of both the writer's and reader's transform matrix.
    #[cfg(feature = "page_ecc")]
    #[test]
    fn reader_reads_section_when_manifest_ecc_enabled_returns_verbatim_bytes() {
        let fs = fresh_fs();
        let path = Path::new("/m/ecc_plain");
        let runtime = RuntimeConfig {
            page_ecc: true,
            ..RuntimeConfig::default()
        };
        write_manifest(
            &fs,
            path,
            runtime.clone(),
            &[("format_version", &[5]), ("tree_type", &[0])],
        );

        let mut reader = ManifestArchiveReader::open(path, &fs, Arc::new(runtime), None).unwrap();
        assert_eq!(reader.read_section("format_version").unwrap(), vec![5]);
        assert_eq!(reader.read_section("tree_type").unwrap(), vec![0]);
    }

    /// `manifest_ecc` on AND encryption configured: the parity trailer sits
    /// over the ciphertext. Exercises the `EncryptedEcc` arm of both the
    /// writer's and reader's transform matrix.
    #[cfg(all(feature = "page_ecc", feature = "encryption"))]
    #[test]
    fn reader_reads_encrypted_section_when_manifest_ecc_enabled_returns_verbatim_bytes() {
        use crate::encryption::{Aes256GcmProvider, EncryptionProvider};

        let fs = fresh_fs();
        let path = Path::new("/m/ecc_enc");
        let enc: Arc<dyn EncryptionProvider> = Arc::new(Aes256GcmProvider::new(&[7u8; 32]));
        let runtime = RuntimeConfig {
            page_ecc: true,
            ..RuntimeConfig::default()
        };

        let mut w = ManifestArchiveWriter::create(
            path,
            &fs,
            Arc::new(runtime.clone()),
            Some(Arc::clone(&enc)),
            crate::fs::SyncMode::Normal,
        )
        .unwrap();
        w.start("format_version").unwrap();
        use std::io::Write;
        w.write_all(&[5u8]).unwrap();
        w.finish().unwrap();

        let mut reader =
            ManifestArchiveReader::open(path, &fs, Arc::new(runtime), Some(enc)).unwrap();
        assert_eq!(reader.read_section("format_version").unwrap(), vec![5]);
    }
}
