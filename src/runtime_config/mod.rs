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
//! [`RuntimeConfig`] and its companion enums are POD data with no
//! I/O dependencies — they live in this module and compile under
//! `no_std + alloc`. The mutable handle [`RuntimeConfigHandle`]
//! lives in the `handle` submodule behind `#[cfg(feature = "std")]`
//! because its `ArcSwap` backing requires std-side primitives.
//!
//! Downstream no_std consumers (block decoders, format constants,
//! sst-dump-style tooling) can reach the type definitions without
//! pulling the mutation machinery.
//!
//! ## Compaction-as-migration semantic
//!
//! Every write path (block write, manifest write, compaction
//! output) loads the current snapshot via `RuntimeConfigHandle::load`
//! at start. Reads are config-independent — each block / manifest
//! is self-describing via its own header. So a toggle on a running
//! Tree affects subsequent writes; existing on-disk data stays in
//! its original format and reads transparently. Compaction acts as
//! the live-migration mechanism: rewriting source blocks per the
//! current snapshot, so over time all data converges to the
//! current settings without stop-the-world coordination.

pub mod types;

#[cfg(feature = "std")]
pub mod handle;

pub use types::{ChecksumAlgorithm, RuntimeConfig};

#[cfg(feature = "std")]
pub use handle::RuntimeConfigHandle;
