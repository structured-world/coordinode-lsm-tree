// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Runtime-toggleable configuration.
//!
//! `RuntimeConfig` carries settings that callers want to change
//! while a `Tree` is open — without restart, without breaking
//! existing on-disk data. Toggleable fields cover format-affecting
//! choices (block checksum algorithm, ECC, encryption, per-KV
//! protection, etc.) that other competing storage engines seal at
//! open time.
//!
//! ## Layered tier separation
//!
//! [`RuntimeConfig`](crate::runtime_config::RuntimeConfig) and its companion enums are POD data with no
//! I/O dependencies — they live in this module and compile under
//! `no_std + alloc`. The mutable handle (crate-internal type
//! `handle::RuntimeConfigHandle`) lives in a `pub(crate)` submodule
//! behind `#[cfg(feature = "std")]` because its `ArcSwap` backing
//! requires std-side primitives. The handle is deliberately NOT
//! re-exported: the stable public surface is
//! [`crate::Tree::runtime_config`] /
//! [`crate::Tree::update_runtime_config`], which return / accept
//! [`RuntimeConfig`](crate::runtime_config::RuntimeConfig) only, so `arc-swap` stays an implementation
//! detail and is not part of the crate's semver contract.
//!
//! Downstream no_std consumers (block decoders, format constants,
//! sst-dump-style tooling) can reach the type definitions without
//! pulling the mutation machinery.
//!
//! ## Compaction-as-migration semantic (designed, wired by V5 features)
//!
//! This module ships the snapshot + atomic-swap mechanism only. No
//! write/compaction/manifest code in the current crate consults the
//! handle yet — only the public Tree API
//! ([`crate::Tree::runtime_config`] /
//! [`crate::Tree::update_runtime_config`]) reads and updates it.
//! The wiring lands with the V5-batch format features (manifest
//! hardening, per-KV protection, scan-since-seqno), which extend
//! [`RuntimeConfig`](crate::runtime_config::RuntimeConfig) with their own fields and load the snapshot at
//! block write / manifest commit / compaction boundaries.
//!
//! Once wired, the intended semantics are:
//!
//! - Every write path loads the current snapshot via a lockless
//!   atomic load on the crate-internal handle. Reads are
//!   config-independent — each block / manifest is self-describing
//!   via its own header.
//! - A toggle on a running Tree affects subsequent writes; existing
//!   on-disk data stays in its original format and reads
//!   transparently.
//! - Compaction acts as the live-migration mechanism: rewriting
//!   source blocks per the current snapshot, so over time all data
//!   converges to the current settings without stop-the-world
//!   coordination.

pub mod types;

#[cfg(feature = "std")]
pub(crate) mod handle;

pub use types::{
    ChecksumAlgorithm, EccGranularity, EccScheme, KvChecksumComputePoint, KvChecksumPolicy,
    LevelMask, RuntimeConfig, TableIdRange,
};
