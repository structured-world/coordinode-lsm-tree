// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

// no-std foundation: when the `std` feature is OFF the crate root opts into
// `no_std`. Default builds keep `std` enabled (file I/O, threading, system
// clock all live in `std`), so existing consumers see no behaviour change.
// The migration to a fully no-std-clean build is incremental. Two patterns
// coexist while the port is in progress:
//   1. Modules with std-only dependencies that have no consumers above the
//      `fs` / `version` / `tree` layer stay gated behind `#[cfg(feature =
//      "std")]` and are ported in isolation.
//   2. Modules whose std-only dependencies cascade through every consumer
//      (`manifest_blocks`, vendored `sfa`, others noted at their `pub mod`
//      site) remain UNCONDITIONAL today — gating them would require
//      `#[cfg]` annotations on dozens of call sites without changing the
//      no-std error count, because those call sites are themselves
//      std-bound for unrelated reasons. They migrate in lockstep with the
//      consumer layer rather than ahead of it.
// The CI job `no-std-check` exercises `cargo check --no-default-features
// --features alloc` against a real no-std target (`thumbv7em-none-eabihf`)
// and tracks remaining work via the error count, which must monotonically
// decrease across PRs.
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
//! Current version: **V5**. V5 introduces the `BuRR` filter wire format,
//! per-block Reed-Solomon Page ECC, and per-entry (per-KV) checksum
//! footers (collapsed into the same version because V5 had not shipped
//! when they landed): the self-describing block types (`Meta` / `Manifest` /
//! `ManifestFooter`) gain a `block_flags` byte whose `ECC_PARITY` bit marks a
//! parity trailer and whose `KV_CHECKSUM_FOOTER` bit marks a per-entry
//! checksum footer. SST block types (`Data` / `Index` / `Filter` /
//! `RangeTombstone`) keep the compact header WITHOUT that byte and derive parity / footer
//! presence from the per-SST meta descriptors (`descriptor#page_ecc`,
//! `descriptor#kv_checksum`). The block magic is bumped so a pre-V5 reader
//! rejects V5 blocks immediately at header decode.
//! A V5 SST written with every optional transform off (no Page ECC, no per-KV
//! footers) is still NOT byte-identical to a pre-V5 table — the bumped block
//! magic and the per-SST meta descriptor keys always differ. Any
//! "byte-identical when off" guarantees in the feature docs are within-V5 and
//! payload-level (e.g. index entries when `seqno_in_index = false`), not a
//! cross-version equivalence.
//! V3-V4 databases are not readable by this version and vice versa. The
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
// The `#[test_log::test]` attribute macro expands to a body that places
// `use` items after statements, which trips `items_after_statements` in
// the test build (`clippy --all-targets`). The lint fires only on the
// macro's generated code, not on anything hand-written, so allow it
// crate-wide rather than annotating every instrumented test.
#![allow(clippy::items_after_statements)]
// Test fixtures routinely bind closely-named locals (`dict_a` / `dict_b`,
// `tree_1` / `tree_2`) where the similarity is the point; `similar_names`
// is pedantic noise on that style.
#![allow(clippy::similar_names)]
// Long scenario tests (fuzz harnesses, multi-phase integration cases)
// legitimately run past the `too_many_lines` ceiling; splitting them
// would obscure the scenario. The lint is only reached under
// `--all-targets`, which lints test bodies.
#![allow(clippy::too_many_lines)]
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

// `alloc` is the minimal hard dependency — the crate uses `Arc`, `Vec`,
// `Box`, and other heap types throughout. `extern crate alloc` makes the
// `alloc` crate root visible to `no_std` builds; under `std` it is a
// no-op alias because the standard library re-exports the same types.
#[macro_use]
extern crate alloc;

// f64 ceil / round: the native `f64` methods are std-only (they bind to
// platform float intrinsics), so the `no_std` build routes through `libm`.
// std keeps the native path (equal or faster); the results are identical.
#[cfg(feature = "std")]
#[inline]
pub(crate) fn f64_ceil(x: f64) -> f64 {
    x.ceil()
}
#[cfg(not(feature = "std"))]
#[inline]
pub(crate) fn f64_ceil(x: f64) -> f64 {
    libm::ceil(x)
}
#[cfg(feature = "std")]
#[inline]
pub(crate) fn f32_round(x: f32) -> f32 {
    x.round()
}
#[cfg(not(feature = "std"))]
#[inline]
pub(crate) fn f32_round(x: f32) -> f32 {
    libm::roundf(x)
}
#[cfg(feature = "std")]
#[inline]
pub(crate) fn f32_ceil(x: f32) -> f32 {
    x.ceil()
}
#[cfg(not(feature = "std"))]
#[inline]
pub(crate) fn f32_ceil(x: f32) -> f32 {
    libm::ceilf(x)
}
#[cfg(feature = "std")]
#[inline]
pub(crate) fn f64_log2(x: f64) -> f64 {
    x.log2()
}
#[cfg(not(feature = "std"))]
#[inline]
pub(crate) fn f64_log2(x: f64) -> f64 {
    libm::log2(x)
}
#[cfg(feature = "std")]
#[inline]
pub(crate) fn f32_log2(x: f32) -> f32 {
    x.log2()
}
#[cfg(not(feature = "std"))]
#[inline]
pub(crate) fn f32_log2(x: f32) -> f32 {
    libm::log2f(x)
}

// `hashbrown` (not `std::collections`) so the crate-wide map / set aliases
// compile on `no_std + alloc`. hashbrown IS the implementation std's HashMap
// wraps, so the API and performance match; using it directly just drops the
// std dependency. Hasher stays `FxHasher` (fast, non-DoS — internal keys are
// not attacker-controlled).
#[doc(hidden)]
pub type HashMap<K, V> = hashbrown::HashMap<K, V, rustc_hash::FxBuildHasher>;

pub(crate) type HashSet<K> = hashbrown::HashSet<K, rustc_hash::FxBuildHasher>;

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
pub mod heal_hints;

// `checkpoint` is `pub(crate)`: it contains internal helpers
// (`link_or_copy_cross_fs`, `prepare_target`, `run_checkpoint`) used by
// `Tree::create_checkpoint` and `BlobTree::create_checkpoint`. Exposing
// it via `pub` (even with `#[doc(hidden)]`) would lock the helpers into
// the stable surface; tests that need to exercise them live inline as
// unit tests inside `src/checkpoint.rs`.
#[cfg(feature = "std")]
pub(crate) mod checkpoint;

#[doc(hidden)]
pub mod blob_tree;

// Vendored, `no_std`-ported `byteview` (backs `Slice`); kept in-tree so the
// engine carries no external dependency that fails to compile on `no_std`.
mod byteview;

mod comparator;

#[doc(hidden)]
mod cache;

/// In-tree sharded S3-FIFO cache backing `cache` and `descriptor_table`
/// (replaces `quick_cache`; works on `std` and `no_std + alloc`).
mod sharded_cache;

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

/// Shard-based Page ECC (XOR single-parity and Reed-Solomon).
///
/// Gated behind the `page_ecc` cargo feature so the
/// `reed-solomon-simd` dependency is only pulled in when the feature
/// is enabled.
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
#[doc(hidden)]
pub mod manifest_blocks;
mod memtable;
mod merge_operator;
pub(crate) mod rate_limiter;
mod run_reader;
mod run_scanner;
// Vendored sfa is std-only internally (`std::io` / `std::fs` /
// `std::path`). Unconditional for the same cascading reason as
// `manifest_blocks` above: ~20 consumers across the table /
// blob-file / inspect / verify / checkpoint paths reference sfa
// types in unconditional code. Gating sfa alone explodes the
// `no-std-check` error count via unresolved-module failures on
// every consumer that hasn't been gated yet. Migration is the
// whole std-bound layer at once (tracked as issue #358), not sfa
// in isolation.
#[doc(hidden)]
pub mod sfa;

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

/// Runtime-toggleable configuration (`RuntimeConfig` + atomic-swap handle).
pub mod runtime_config;

/// Disaster-recovery: rebuild a missing/corrupt manifest from on-disk SSTs.
// std-only: scans table folders and rewrites the manifest via std::fs.
#[cfg(feature = "std")]
pub mod repair;

pub(crate) mod active_tombstone_set;
pub(crate) mod range_tombstone;
pub(crate) mod range_tombstone_filter;

#[doc(hidden)]
pub mod table;

mod background_deleter;
mod scan_since;

/// Single-error-correct / double-error-detect word codecs for the Page ECC
/// read path (pluggable SEC-DED shapes; default Hsiao `(72, 64)`).
///
/// Gated behind `page_ecc` and crate-internal: the codec only runs on the
/// Page ECC recovery path. Trailer sizing on the read path uses a plain
/// `ceil(len / 8)` so reading a SEC-DED SST does not require this module.
#[cfg(feature = "page_ecc")]
pub(crate) mod secded;

mod seqno;
mod slice;
mod slice_windows;

#[doc(hidden)]
pub mod stop_signal;

mod format_version;
mod time;
mod tree;

pub use time::Clock;
#[cfg(feature = "std")]
pub use time::SystemClock;
#[cfg(not(feature = "std"))]
pub use time::set_clock;

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

/// ECC patrol scrub: a proactive sweep over Page-ECC-protected SST blocks.
///
/// Reads blocks to detect and correct latent bit-rot before it accumulates
/// past the parity budget. Std-gated for the same reason as [`verify`]: the
/// sweep needs real filesystem I/O and thread-based parallelism.
#[cfg(feature = "std")]
pub mod scrub;

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
    checksum::Checksum,
    iter_guard::IterGuardImpl,
    key_range::KeyRange,
    merge::BoxedIterator,
    slice::Builder,
    // Re-exported for `benches/lsp.rs` only — see hidden-block contract above.
    table::util::longest_shared_prefix_length,
    table::{GlobalTableId, Table, TableId},
    value::InternalValue,
};

#[doc(hidden)]
pub use {
    blob_tree::{Guard as BlobGuard, handle::BlobIndirection},
    tree::Guard as StandardGuard,
    tree::inner::TreeId,
};

pub use encryption::EncryptionProvider;

#[cfg(feature = "encryption")]
pub use encryption::Aes256GcmProvider;

#[doc(hidden)]
#[cfg(feature = "std")]
pub use background_deleter::BackgroundDeleter;
pub use pinnable_slice::PinnableSlice;
#[cfg(feature = "std")]
pub use repair::RepairReport;
pub use write_batch::WriteBatch;

pub use {
    cache::Cache,
    comparator::{DefaultUserComparator, SharedComparator, UserComparator},
    compression::CompressionType,
    config::{Config, KvSeparationOptions, TreeType},
    error::{Error, Result},
    format_version::FormatVersion,
    iter_guard::IterGuard as Guard,
    memtable::{Memtable, MemtableId},
    merge_operator::MergeOperator,
    prefix::PrefixExtractor,
    seqno::{
        MAX_SEQNO, SequenceNumberCounter, SequenceNumberGenerator, SharedSequenceNumberGenerator,
    },
    slice::Slice,
    value::SeqNo,
    value_type::ValueType,
};

pub use {
    abstract_tree::{AbstractTree, CheckpointInfo},
    any_tree::AnyTree,
    blob_tree::BlobTree,
    descriptor_table::DescriptorTable,
    ingestion::AnyIngestion,
    scan_since::ScanSinceEvent,
    tree::Tree,
    vlog::BlobFile,
};

#[cfg(zstd_any)]
pub use compression::ZstdDictionary;

#[cfg(feature = "metrics")]
pub use metrics::Metrics;

#[cfg(feature = "std")]
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
