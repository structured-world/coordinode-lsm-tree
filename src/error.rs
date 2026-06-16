// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{Checksum, CompressionType};
#[cfg(not(feature = "std"))]
use alloc::string::String;

/// Represents errors that can occur in the LSM-tree
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// I/O error
    Io(crate::io::Error),

    /// Decompression failed
    Decompress(CompressionType),

    /// Invalid or unparsable data format version
    InvalidVersion(u8),

    /// Some required files could not be recovered from disk
    Unrecoverable,

    /// Checksum mismatch
    ChecksumMismatch {
        /// Checksum of loaded block
        got: Checksum,

        /// Checksum that was saved in block header
        expected: Checksum,
    },

    /// A memtable entry's per-KV digest, computed at insert under
    /// [`KvChecksumComputePoint::AtInsert`](crate::runtime_config::KvChecksumComputePoint::AtInsert),
    /// did not match a recompute over the entry's current bytes at flush.
    ///
    /// This is the memtable-residence RAM-corruption signal: the entry's
    /// logical content (`value_type`, `seqno`, key, or value) changed while it
    /// sat in the memtable, between insert and flush. Distinct from
    /// [`Self::ChecksumMismatch`] (on-disk block bytes) — this catches a flip
    /// that happens entirely in RAM, before any block is written.
    MemtableKvChecksumMismatch {
        /// Sequence number of the entry whose digest diverged (locates it).
        seqno: u64,

        /// Digest recomputed over the entry's current memtable bytes at flush.
        got: u64,

        /// Digest computed and stored when the entry was inserted.
        expected: u64,
    },

    /// A memtable entry carried an insert-time per-KV digest
    /// ([`KvChecksumComputePoint::AtInsert`](crate::runtime_config::KvChecksumComputePoint::AtInsert))
    /// tagged with an algorithm `AtInsert` never stores: a non-4-byte or
    /// unknown algorithm wire tag.
    ///
    /// `AtInsert` only ever writes a 4-byte algorithm tag (`Xxh3Low32` /
    /// `Crc32c`), so a digest-bearing node tagged otherwise means the node's
    /// algorithm metadata was corrupted in RAM during memtable residence.
    /// Distinct from [`Self::MemtableKvChecksumMismatch`] (the digest value
    /// diverged): here the algorithm itself is unusable, so the engine refuses
    /// to "verify" the entry under the wrong algorithm rather than risk a
    /// flipped tag passing the residence check.
    MemtableKvChecksumCorruptAlgorithm {
        /// Sequence number of the entry whose algorithm tag is invalid.
        seqno: u64,

        /// The invalid algorithm wire tag read from the node.
        tag: u8,
    },

    /// Blob frame header CRC mismatch (V4 format).
    /// Distinct from `ChecksumMismatch` which covers data payload checksums.
    HeaderCrcMismatch {
        /// CRC recomputed from header fields
        recomputed: u32,

        /// CRC stored in the blob frame header
        stored: u32,
    },

    /// Invalid enum tag
    InvalidTag((&'static str, u8)),

    /// Invalid block trailer
    InvalidTrailer,

    /// Invalid block header
    InvalidHeader(&'static str),

    /// Data size (decompressed, on-disk, or requested) is invalid or exceeds a safety limit
    DecompressedSizeTooLarge {
        /// Size associated with the data being processed. This may come from
        /// on-disk/in-memory metadata (e.g., header, block/value handle) or be
        /// derived from caller input (e.g., a requested key or value length),
        /// and may be zero, invalid, or over the configured limit.
        declared: u64,

        /// Maximum allowed size for the data or request being processed
        limit: u64,
    },

    /// UTF-8 error
    Utf8(core::str::Utf8Error),

    /// Merge operator failed.
    ///
    /// No context payload — consistent with other unit variants
    /// (`Unrecoverable`, `InvalidTrailer`). Operators should log
    /// details before returning this error.
    MergeOperator,

    /// Encryption failed
    Encrypt(&'static str),

    /// Decryption failed
    Decrypt(&'static str),

    /// Comparator mismatch on tree reopen.
    ///
    /// The tree was created with a comparator whose [`crate::UserComparator::name`]
    /// differs from the one supplied at reopen time.
    ComparatorMismatch {
        /// Comparator name persisted in the tree metadata.
        stored: String,

        /// Comparator name supplied by the caller.
        supplied: &'static str,
    },

    /// Zstd dictionary required but not provided, or `dict_id` mismatch
    ZstdDictMismatch {
        /// Dictionary ID stored in the block/table metadata
        expected: u32,

        /// Dictionary ID provided by the caller (`None` if no dictionary supplied)
        got: Option<u32>,
    },

    /// Per-record XXH3-64 mismatch inside a framed manifest section
    /// (`tables` / `blob_files`). Distinct from
    /// [`Error::ChecksumMismatch`] — same XXH3 family but a
    /// different output width (XXH3-64 here vs XXH3-128 for
    /// block-level payloads) on a different layer of the on-disk
    /// format, with different recovery semantics (manifest framing
    /// surfaces routed through `ManifestRecoveryMode`; block
    /// checksums surface via `Error::ChecksumMismatch` for the
    /// block I/O paths). Strict manifest recovery modes surface
    /// this so an operator can see the exact 64-bit digests that
    /// disagreed; `SkipAnyCorruptedRecords` and
    /// `PointInTimeRecovery` route around the corruption without
    /// raising it.
    ManifestFrameChecksumMismatch {
        /// SFA section the corrupt record was found in (e.g.
        /// `"tables"`, `"blob_files"`). Static so this can be
        /// compared without parsing message strings.
        section: &'static str,
        /// XXH3-64 digest the framing header claimed for the
        /// record's payload.
        expected: u64,
        /// XXH3-64 digest the reader recomputed over the bytes
        /// actually on disk.
        got: u64,
    },

    /// Range tombstone block decode failure.
    RangeTombstoneDecode {
        /// Which field or validation failed (e.g. `start_len`, `start`, `seqno`, `interval`)
        field: &'static str,

        /// Byte offset within the block to the start of the field whose decoding failed
        /// (captured before reading bytes for that field).
        offset: u64,
    },

    /// A [`WriteBatch`](crate::WriteBatch) contains mixed operation types
    /// (e.g. insert + remove) for the same user key.
    ///
    /// Mixed ops at the same logical version are rejected because the
    /// memtable/skiplist ordering ties on `(user_key, seqno)` and does not
    /// include `value_type` as a tie-breaker. That would otherwise make
    /// equal-key entries with different operation types ambiguous to later
    /// reads and merges, yielding tie-break-dependent "last write wins"
    /// semantics.
    MixedOperationBatch,

    /// Tree was opened with `Config::page_ecc(true)` but this build of
    /// the crate does not have the `page_ecc` cargo feature enabled.
    /// The reader has no way to verify or recover Reed-Solomon parity
    /// without the codec, so opening such a tree would silently
    /// downgrade integrity guarantees — return this error instead.
    PageEccUnsupported,

    /// Block payload failed the XXH3 integrity check and the
    /// attached Reed-Solomon parity trailer could not reconstruct
    /// it (more shards are corrupted than the (4, 2) RS scheme
    /// can recover). Surfaced ONLY by ECC-protected blocks
    /// (the `ECC_PARITY` header flag set); a block written without parity
    /// (`Config::page_ecc(false)`) on a checksum mismatch returns
    /// [`Self::ChecksumMismatch`] instead, because there's no
    /// parity to even attempt recovery from.
    PageEccUnrecoverable {
        /// XXH3 checksum recomputed from the on-disk bytes.
        got: Checksum,
        /// XXH3 checksum stored in the block header.
        expected: Checksum,
    },

    /// Route-compatibility mismatch on reopen.
    ///
    /// Recovery found fewer tables on disk than the manifest expects, and all
    /// missing tables are on levels not covered by any current
    /// [`level_routes`](crate::Config::level_routes).  This typically means a
    /// previously configured route was removed, leaving its directory
    /// unreachable.
    ///
    /// Re-adding the missing route(s) will usually resolve the error.  If
    /// missing tables are on levels that *are* covered by a current route,
    /// recovery returns [`Unrecoverable`](Self::Unrecoverable) instead
    /// (the SST files were genuinely lost).
    RouteMismatch {
        /// Number of tables listed in the manifest.
        expected: usize,

        /// Number of tables actually found across all configured routes.
        found: usize,
    },

    /// Valid configuration / on-disk layout that this build does not
    /// yet know how to process, or constructor input that violates a
    /// documented invariant (e.g. `CompressionType::None` passed to
    /// [`crate::table::block::CompressionContext::new`]). Distinct
    /// from [`Error::Unrecoverable`] (signals corruption) and from
    /// [`Error::Io`] with `ErrorKind::Unsupported` (which can also
    /// surface from platform / backend limits); the `&'static str`
    /// payload names the specific marker that triggered the rejection
    /// (e.g. `"filter_tli"` for a partitioned filter SFA section,
    /// `"compression-context-none"` for the constructor invariant) so
    /// the caller can route the diagnostic without parsing message
    /// strings.
    FeatureUnsupported(&'static str),

    /// The tree directory is already locked by another live instance.
    ///
    /// Returned by [`Config::open`](crate::Config::open) and
    /// [`Config::repair`](crate::Config::repair) when the cross-process
    /// directory lock (a `LOCK` file under the tree directory, held via an
    /// advisory OS file lock) could not be acquired because another process
    /// owns it. Holds the directory path as a display string for diagnostics.
    /// Two processes mutating the same manifest would corrupt it, so the second
    /// acquirer fails fast here. Disable the lock with
    /// [`Config::with_directory_lock`](crate::Config::with_directory_lock) only
    /// when exclusivity is enforced at a higher layer.
    Locked(String),

    /// Manifest footer / TOC / file-level discovery failure.
    ///
    /// Scoped to errors detected at or before the TOC is parsed —
    /// i.e. everything the reader needs *before* it can answer
    /// "where is section X". Section-content failures (a specific
    /// section's Block fails verification) go through
    /// [`ManifestSectionInvalid`](Error::ManifestSectionInvalid)
    /// instead so callers like `Tree::open` can distinguish a
    /// totally unreadable manifest from a per-section problem.
    ///
    /// Typical causes:
    ///
    /// - **Footer-payload structural failure:** unknown layout
    ///   version, oversized section count, empty/oversized section
    ///   name, invalid UTF-8, duplicate section name, footer
    ///   payload exceeds the 4 KiB reservation.
    /// - **Tail / head-mirror double failure:** both the
    ///   tail-footer Block read and the head-mirror fallback
    ///   failed verification (XXH3 mismatch, AEAD decryption,
    ///   parse error). Per-path causes are logged at `error`
    ///   level and collapsed here.
    /// - **TOC entry value corruption:** a TOC entry's
    ///   `block_offset + block_size` overflows `u64` or extends
    ///   past the end of the file. The TOC bytes are footer
    ///   payload, so a malformed TOC entry is a footer-level
    ///   issue even though it surfaces in
    ///   `ManifestArchiveReader::read_section`.
    /// - **Trailing size-hint corruption:** the tail's 4-byte
    ///   footer-size hint is zero or exceeds
    ///   `HEAD_FOOTER_RESERVED_SIZE` (4 KiB), or the implied
    ///   `section_end` lands inside the head reservation. Caught
    ///   in both the reader and `checkpoint::write_current_for_version`.
    /// - **Writer-side invariant breach:** `write_cursor` would
    ///   overflow `u64`, an in-memory section would exceed the
    ///   on-disk Block-size cap, etc.
    /// - **CURRENT pointer points at a missing manifest:** when
    ///   `version::get_current_version` opens the referenced
    ///   `v{N}` file and gets `NotFound`, the error is rewrapped
    ///   here so `Tree::open`'s outer `Io(NotFound) => create_new`
    ///   arm cannot mistake a half-applied recovery / corrupted
    ///   state for a clean first-open.
    ManifestFooterInvalid(&'static str),

    /// Manifest section content failed verification or matched no
    /// TOC entry.
    ///
    /// Surfaced by `ManifestArchiveReader::read_section` (and the
    /// helper that validates the inner Block header before
    /// delegating to `Block::from_reader`). Distinct from
    /// [`ManifestFooterInvalid`](Error::ManifestFooterInvalid)
    /// because the footer / TOC loaded fine — the bad bytes are
    /// localised to one section Block and a caller MAY route
    /// recovery differently (e.g. skip the section vs. refuse
    /// the whole manifest).
    ///
    /// Causes:
    ///
    /// - **Requested section name not in TOC:** the caller asked
    ///   for a section that the manifest doesn't declare.
    /// - **Section Block header doesn't fit its outer buffer:**
    ///   the inner block's derived on-disk size (header + payload +
    ///   parity-if-flagged) exceeds the TOC-declared `block_size`.
    ///   Defence-in-depth against a forged TOC pointing at a too-small
    ///   slot.
    /// - **Block decoded at the TOC offset has the wrong
    ///   `block_type`:** TOC says "section here" but the bytes
    ///   carry a non-`Manifest` Block. Defence-in-depth against
    ///   TOC-redirect attacks; once AAD-binding lands in
    ///   `encryption::block`, `Block::from_reader` will reject
    ///   this internally and the check here becomes belt-and-
    ///   braces.
    ManifestSectionInvalid(&'static str),

    /// The trailing record of the incremental manifest edit log is
    /// incomplete or corrupt, and the active
    /// [`ManifestRecoveryMode`](crate::config::ManifestRecoveryMode) does
    /// not tolerate that defect, so the open aborts rather than silently
    /// rolling the edit back.
    ///
    /// A clean end-of-log is never reported here: a crash exactly at a
    /// record boundary is byte-identical to a pristine close, so that
    /// case is always tolerated. This fires when bytes of a trailing
    /// record are present but the record fails framing — a
    /// power-loss-truncated append (only
    /// [`AbsoluteConsistency`](crate::config::ManifestRecoveryMode::AbsoluteConsistency)
    /// rejects it; other modes roll it back), or a fully-framed record
    /// whose checksum doesn't match (bit-rot) / whose header is forged
    /// (rejected by both `AbsoluteConsistency` and
    /// [`TolerateCorruptedTailRecords`](crate::config::ManifestRecoveryMode::TolerateCorruptedTailRecords),
    /// which salvages writer-incomplete tails only; rolled back under
    /// `PointInTimeRecovery` / `SkipAnyCorruptedRecords`).
    ///
    /// Recover by truncating the torn tail: run
    /// [`Config::repair`](crate::Config::repair), which rebuilds a clean
    /// standalone snapshot (dropping the edit log), or re-open under a
    /// [`ManifestRecoveryMode`](crate::config::ManifestRecoveryMode) that
    /// tolerates the defect to roll the trailing edit back.
    TornManifestEditLog {
        /// The trailing defect detected: `"truncated"` (partial record
        /// from a power-loss-interrupted append), `"checksum-mismatch"`
        /// (fully-framed record whose payload bit-rotted),
        /// `"bad-header"` (implausible framing length), or
        /// `"len-mismatch"` (record length disagrees with the expected
        /// fixed size). Static so callers can branch without parsing
        /// the message string.
        kind: &'static str,
    },
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "LsmTreeError: {self:?}")
    }
}

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<crate::sfa::Error> for Error {
    fn from(value: crate::sfa::Error) -> Self {
        match value {
            crate::sfa::Error::Io(e) => Self::from(e),
            crate::sfa::Error::ChecksumMismatch { got, expected } => {
                log::error!("Archive ToC checksum mismatch");
                Self::ChecksumMismatch {
                    got: got.into(),
                    expected: expected.into(),
                }
            }
            crate::sfa::Error::InvalidHeader => {
                log::error!("Invalid archive header");
                Self::Unrecoverable
            }
            crate::sfa::Error::InvalidVersion => {
                log::error!("Invalid archive version");
                Self::Unrecoverable
            }
            crate::sfa::Error::UnsupportedChecksumType => {
                log::error!("Invalid archive checksum type");
                Self::Unrecoverable
            }
        }
    }
}

// The `Io` variant carries `crate::io::Error` (the no_std-capable I/O error),
// so this bridge is a direct wrap. Std file-I/O paths surface `std::io::Error`;
// the std-gated bridge below folds those through `crate::io::Error`.
impl From<crate::io::Error> for Error {
    fn from(value: crate::io::Error) -> Self {
        Self::Io(value)
    }
}

#[cfg(feature = "std")]
impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.into())
    }
}

/// Tree result
pub type Result<T> = core::result::Result<T, Error>;
