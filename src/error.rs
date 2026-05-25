// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{Checksum, CompressionType};

/// Represents errors that can occur in the LSM-tree
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// I/O error
    Io(std::io::Error),

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
    Utf8(std::str::Utf8Error),

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
    /// [`Error::ChecksumMismatch`] (which covers the u128 XXH3 over
    /// block-level payloads — different surface, different
    /// algorithm, different invariants). Strict manifest recovery
    /// modes surface this so an operator can see the exact 64-bit
    /// digests that disagreed; `SkipAnyCorruptedRecords` and
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

    /// Valid on-disk layout that this build does not yet know how to
    /// process. Distinct from [`Error::Unrecoverable`] (which signals
    /// corruption) and from [`Error::Io`] with `ErrorKind::Unsupported`
    /// (which can also surface from platform / backend limits); the
    /// `&'static str` payload names the specific layout marker that
    /// triggered the rejection (e.g. `"filter_tli"` for a partitioned
    /// filter SFA section) so the caller can route the diagnostic
    /// without parsing message strings.
    FeatureUnsupported(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LsmTreeError: {self:?}")
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<sfa::Error> for Error {
    fn from(value: sfa::Error) -> Self {
        match value {
            sfa::Error::Io(e) => Self::from(e),
            sfa::Error::ChecksumMismatch { got, expected } => {
                log::error!("Archive ToC checksum mismatch");
                Self::ChecksumMismatch {
                    got: got.into(),
                    expected: expected.into(),
                }
            }
            sfa::Error::InvalidHeader => {
                log::error!("Invalid archive header");
                Self::Unrecoverable
            }
            sfa::Error::InvalidVersion => {
                log::error!("Invalid archive version");
                Self::Unrecoverable
            }
            sfa::Error::UnsupportedChecksumType => {
                log::error!("Invalid archive checksum type");
                Self::Unrecoverable
            }
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

/// Tree result
pub type Result<T> = std::result::Result<T, Error>;
