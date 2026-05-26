// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

// no-std foundation: when the `std` feature is OFF the crate root opts into
// `no_std`. Default builds keep `std` enabled (file I/O, threading, system
// clock all live in `std`), so existing consumers see no behaviour change.
// The migration to a fully no-std-clean build is incremental — modules with
// std-only dependencies stay gated behind `#[cfg(feature = "std")]` until
// they are ported. The CI job `no-std-check` exercises `cargo check
// --no-default-features --features alloc` and tracks remaining work.
#![cfg_attr(not(feature = "std"), no_std)]

//! Embedded LSM-tree storage engine.
//!
//! Provides keyed point reads, prefix and range scans, MVCC snapshots, block
//! and file-descriptor caching, and a configurable compaction subsystem. No
//! write-ahead log — durability is the caller's responsibility (`flush_active_memtable`
//! forces persistence when needed).
//!
//! ## Highlights
//!
//! - **AMQ filter**: `BuRR` (Bumped Ribbon Retrieval, Walzer & Dillinger 2022) for
//!   per-key and per-prefix membership checks. ~30% smaller filter blocks than a
//!   same-FPR Bloom filter, or ~10× tighter FPR at the same memory budget.
//! - **Compression**: pure-Rust zstd (incl. dictionary mode), LZ4, or none —
//!   per-table and per-level policy.
//! - **Encryption at rest**: AES-256-GCM block encryption with a caller-supplied
//!   key.
//! - **Range tombstones**: `delete_range` / `delete_prefix` (SST-encoded; the
//!   feature was added in disk format V4 and remains supported in the current
//!   V5 format — the V5 break extends the block header for per-block Reed-
//!   Solomon Page ECC, not the tombstone encoding).
//! - **Merge operators**: commutative-merge LSM operations with lazy resolution.
//! - **K/V separation (`BlobTree`)**: large-value workloads with automatic GC.
//! - **Pluggable `Fs`**: standard, in-memory, `io_uring`, or custom backends.
//! - **MVCC**: snapshot reads at a chosen `SeqNo`, custom `UserComparator`.
//! - **Concurrency**: thread-safe `BTreeMap`-like API.
//!
//! Keys: up to 65,535 bytes (`u16` length field). Values: up to 4,294,967,295
//! bytes (`u32` length field, `2³² − 1`). Larger keys and values
//! carry a proportional performance cost.
//!
//! ## Quick start
//!
//! ```no_run
//! use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
//!
//! let folder = tempfile::tempdir().unwrap();
//! let seqno = SequenceNumberCounter::default();
//! let tree = Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
//!     .open()
//!     .unwrap();
//!
//! tree.insert("key", "value", seqno.next());
//! let value = tree.get("key", lsm_tree::SeqNo::MAX).unwrap();
//! assert_eq!(value.map(|v| v.to_vec()), Some(b"value".to_vec()));
//! ```
//!
//! ## On-disk format
//!
//! Current version: **V5**. V5 introduces the `BuRR` filter wire format
//! and per-block Reed-Solomon Page ECC (collapsed into the same version
//! because V5 had not shipped when both landed): the block header gains
//! an `ecc_length` field and the block magic is bumped so a pre-V5
//! reader rejects V5 blocks immediately at header decode. V3-V4
//! databases are not readable by this version and vice versa. The
//! manifest version gate rejects pre-V5 databases at `Tree::open` time.
//! V4 introduced range tombstones (still supported).
#![deny(clippy::all, missing_docs, clippy::cargo)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::indexing_slicing)]
#![warn(clippy::pedantic, clippy::nursery)]
#![warn(clippy::expect_used)]
#![allow(clippy::missing_const_for_fn)]
#![warn(clippy::multiple_crate_versions)]
#![allow(clippy::option_if_let_else)]
#![warn(clippy::redundant_feature_names)]
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

// `alloc` is the minimal hard dependency — the crate uses `Arc`, `Vec`,
// `Box`, and other heap types throughout. `extern crate alloc` makes the
// `alloc` crate root visible to `no_std` builds; under `std` it is a
// no-op alias because the standard library re-exports the same types.
extern crate alloc;

#[doc(hidden)]
pub type HashMap<K, V> = std::collections::HashMap<K, V, rustc_hash::FxBuildHasher>;

pub(crate) type HashSet<K> = std::collections::HashSet<K, rustc_hash::FxBuildHasher>;

macro_rules! fail_iter {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return Some(Err(e.into())),
        }
    };
}

macro_rules! unwrap {
    ($x:expr) => {{ $x.expect("should read") }};
}

mod any_tree;

mod abstract_tree;

pub(crate) mod deletion_pause;

// `checkpoint` is `pub(crate)`: it contains internal helpers
// (`link_or_copy_cross_fs`, `prepare_target`, `run_checkpoint`) used by
// `Tree::create_checkpoint` and `BlobTree::create_checkpoint`. Exposing
// it via `pub` (even with `#[doc(hidden)]`) would lock the helpers into
// the stable surface; tests that need to exercise them live inline as
// unit tests inside `src/checkpoint.rs`.
pub(crate) mod checkpoint;

#[doc(hidden)]
pub mod blob_tree;

mod comparator;

#[doc(hidden)]
mod cache;

#[doc(hidden)]
pub mod checksum;

#[doc(hidden)]
pub mod coding;

pub mod compaction;
#[doc(hidden)]
pub mod compression;

/// Block-level encryption at rest.
pub mod encryption;

/// Configuration
pub mod config;

#[doc(hidden)]
pub mod descriptor_table;

/// Per-block Reed-Solomon Page ECC. Gated behind the `page_ecc`
/// cargo feature so the `reed-solomon-simd` dependency is only
/// pulled in when the feature is enabled.
#[cfg(feature = "page_ecc")]
pub mod ecc;

#[doc(hidden)]
pub mod file_accessor;

mod double_ended_peekable;
mod error;

#[doc(hidden)]
pub mod file;

/// Pluggable filesystem abstraction for I/O backends.
pub mod fs;

pub mod hash;

/// Local I/O trait surface mirroring `std::io::{Read, Write, Seek}`.
///
/// Provides `Error` / `ErrorKind` / `SeekFrom` plus the three trait
/// definitions so the bounds on the [`fs`] traits no longer carry
/// `std::io::*` directly. Under the `std` feature, supertrait
/// aliases + blanket impls forward to `std::io` types so existing
/// std-backed backends satisfy the trait surface automatically; the
/// alias form also propagates BACK to `std::io`, so a `dyn FsFile`
/// bounded on `crate::io::Read` still flows into `std::io::BufReader`,
/// `byteorder`, and friends.
///
/// Scope: this prerequisite slice (per #311) lifts only
/// `Read`/`Write`/`Seek` out of the [`fs`] trait bounds. The
/// `io::Result<T>` return types and `&Path` argument types in
/// `fs::Fs` / `fs::FsFile` still resolve to `std::io::Result<T>` and
/// `std::path::Path` and migrate in follow-up commits; the full
/// `--no-default-features --features alloc` build of the `fs::*`
/// surface arrives once those two follow-ups land.
pub mod io;

mod heap;
mod ingestion;
mod iter_guard;
mod key;
mod key_range;
mod loser_tree;
mod manifest;
mod memtable;
mod merge_operator;
mod run_reader;
mod run_scanner;

#[doc(hidden)]
pub mod merge;

#[doc(hidden)]
pub mod merge_source;
#[doc(hidden)]
pub mod seeking_merger;

#[cfg(feature = "metrics")]
pub(crate) mod metrics;

// mod multi_reader;

#[doc(hidden)]
pub mod mvcc_stream;

mod path;
mod pinnable_slice;
mod prefix;

#[doc(hidden)]
pub mod range;

pub(crate) mod active_tombstone_set;
pub(crate) mod range_tombstone;
pub(crate) mod range_tombstone_filter;

#[doc(hidden)]
pub mod table;

mod seqno;
mod slice;
mod slice_windows;

#[doc(hidden)]
pub mod stop_signal;

mod format_version;
mod time;
mod tree;

/// Utility functions
pub mod util;

mod value;
mod value_type;
mod write_batch;

/// Integrity verification for SST and blob files.
///
/// Gated behind `std` because the verify pipeline uses `std::fs`,
/// `std::path::Path`, `std::io::{Read, Seek}` everywhere — full-file
/// hashing and per-block walking both need real filesystem I/O. Once
/// the `Fs`/`FsFile` trait surface is no-std-friendly (tracked under
/// the no-std migration), the gate can be relaxed.
#[cfg(feature = "std")]
pub mod verify;

/// Out-of-band inspection of a single SST file.
///
/// Public read-only view of stored metadata (table id, key range,
/// counts, compression, timestamp) without spinning up a `Tree`.
/// Used by `sst-dump properties` and similar diagnostic tools. See
/// the module docs for the recovery semantics (mirrors
/// `Table::recover`'s TAIL-first / MID-fallback path from #295).
#[cfg(feature = "std")]
pub mod inspect;

mod version;
mod vlog;

/// User defined key (byte array)
pub type UserKey = Slice;

/// User defined data (byte array)
pub type UserValue = Slice;

/// KV-tuple (key + value)
pub type KvPair = (UserKey, UserValue);

// The `#[doc(hidden)]` block below re-exports crate internals that are reachable
// at the crate root for benchmarks and integration tests, but are NOT part of the
// public API contract — they carry no semver guarantee and may be renamed, moved,
// or removed without a major version bump. External callers that import these
// hidden items do so at their own risk. `cargo doc` excludes them from generated
// rustdoc output; only intra-crate test/bench code is expected to use them.
#[doc(hidden)]
pub use {
    blob_tree::{Guard as BlobGuard, handle::BlobIndirection},
    checksum::Checksum,
    iter_guard::IterGuardImpl,
    key_range::KeyRange,
    merge::BoxedIterator,
    slice::Builder,
    // Re-exported for `benches/lsp.rs` only — see hidden-block contract above.
    table::util::longest_shared_prefix_length,
    table::{GlobalTableId, Table, TableId},
    tree::Guard as StandardGuard,
    tree::inner::TreeId,
    value::InternalValue,
};

pub use encryption::EncryptionProvider;

#[cfg(feature = "encryption")]
pub use encryption::Aes256GcmProvider;

pub use pinnable_slice::PinnableSlice;
pub use write_batch::WriteBatch;

pub use {
    abstract_tree::{AbstractTree, CheckpointInfo},
    any_tree::AnyTree,
    blob_tree::BlobTree,
    cache::Cache,
    comparator::{DefaultUserComparator, SharedComparator, UserComparator},
    compression::CompressionType,
    config::{Config, KvSeparationOptions, TreeType},
    descriptor_table::DescriptorTable,
    error::{Error, Result},
    format_version::FormatVersion,
    ingestion::AnyIngestion,
    iter_guard::IterGuard as Guard,
    memtable::{Memtable, MemtableId},
    merge_operator::MergeOperator,
    prefix::PrefixExtractor,
    seqno::{
        MAX_SEQNO, SequenceNumberCounter, SequenceNumberGenerator, SharedSequenceNumberGenerator,
    },
    slice::Slice,
    tree::Tree,
    value::SeqNo,
    value_type::ValueType,
    vlog::BlobFile,
};

#[cfg(zstd_any)]
pub use compression::ZstdDictionary;

#[cfg(feature = "metrics")]
pub use metrics::Metrics;

#[doc(hidden)]
#[must_use]
#[allow(missing_docs, clippy::missing_errors_doc, clippy::unwrap_used)]
pub fn get_tmp_folder() -> tempfile::TempDir {
    if let Ok(p) = std::env::var("LSMT_TMP_FOLDER") {
        tempfile::tempdir_in(p)
    } else {
        tempfile::tempdir()
    }
    .unwrap()
}
