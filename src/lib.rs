// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

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
//!   V5 format — V5's breaking change is the filter wire format, not the
//!   tombstone encoding).
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
//! Current version: **V5**. V5 introduces a wire-format break for filter
//! blocks (`BuRR` replaces Bloom); V3 and V4 databases are not readable by
//! this version and vice versa. The manifest version gate rejects pre-V5
//! databases at `Tree::open` time.
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

#[doc(hidden)]
pub mod file_accessor;

mod double_ended_peekable;
mod error;

#[doc(hidden)]
pub mod file;

/// Pluggable filesystem abstraction for I/O backends.
pub mod fs;

pub mod hash;
mod heap;
mod ingestion;
mod iter_guard;
mod key;
mod key_range;
mod manifest;
mod memtable;
mod merge_operator;
mod run_reader;
mod run_scanner;

#[doc(hidden)]
pub mod merge;

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
pub mod verify;

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
    abstract_tree::AbstractTree,
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
