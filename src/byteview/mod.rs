// SPDX-License-Identifier: Apache-2.0 OR MIT
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation
//
// Vendored from the `byteview` crate (https://github.com/fjall-rs/byteview),
// ported to `no_std` + `alloc` and kept in-tree as a self-contained module so
// the engine no longer carries an external dependency that does not yet
// compile on `no_std` targets. The full upstream surface is retained verbatim
// (only the `std::*` paths were rewritten to `core::*` / `alloc::*`, and the
// `std::io`-based `from_reader` is feature-gated) so it can later be lifted out
// as a standalone `structured-byteview` crate without an API diff.

//! An immutable byte slice that may be inlined, and can be partially cloned without heap allocation.
//!
//! The length is limited to 2^32 bytes (4 GiB).
//!
//! ```ignore
//! # use byteview::ByteView;
//! let slice = ByteView::from("helloworld_thisisaverylongstring");
//!
//! // No heap allocation - increases the ref count like an Arc<[u8]>
//! let full_copy = slice.clone();
//! drop(full_copy);
//!
//! // No heap allocation - increases the ref count like an Arc<[u8]>, but we only get a subslice
//! let copy = slice.slice(11..);
//! assert_eq!(b"thisisaverylongstring", &*copy);
//!
//! // No heap allocation - if the slice is small enough, it will be inlined into the struct...
//! let copycopy = copy.slice(0..4);
//! assert_eq!(b"this", &*copycopy);
//!
//! // ...so no ref count incrementing is done
//! assert_eq!(2, slice.ref_count());
//!
//! drop(copy);
//! assert_eq!(1, slice.ref_count());
//!
//! drop(copycopy);
//! assert_eq!(1, slice.ref_count());
//!
//! // Our original slice will be automatically freed if all slices vanish
//! drop(slice);
//! ```
//!
//! Backs the crate's [`Slice`](crate::Slice) type: a small slice is stored
//! inline; a larger one shares a single ref-counted heap allocation that even
//! subslices can clone without re-allocating.

// Vendored library: the full byteview API surface is kept even though the
// engine consumes only `ByteView` / `Builder` today, so the module stays a
// faithful, extraction-ready copy of upstream rather than a trimmed fork.
//
// - `dead_code` / `unused_imports`: the retained-but-unused surface (StrView,
//   Mutator re-export) is intentional for extraction parity.
// - `unexpected_cfgs`: the vendored `serde` integration is gated on byteview's
//   own `serde` feature, which this crate does not surface; the code is simply
//   never compiled here.
//
// The `clippy::*` allows hold this module to byteview's own (upstream) lint
// policy rather than the host crate's stricter house style: the SSO / refcount
// internals deliberately use raw-pointer casts, bounds-known indexing, and
// infallible `Layout` unwraps that this crate otherwise denies. Restyling
// upstream's correct, published code would diverge from it and churn the hot
// path of the core slice type for no behavioural gain (the same boundary the
// vendored Ribbon filter uses, and what a standalone `structured-byteview`
// crate would keep as its own lint config).
#![allow(dead_code, unused_imports, unexpected_cfgs)]
#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing
)]

mod builder;
mod byteview;
mod strview;

pub use byteview::ByteView;
pub use strview::StrView;

#[doc(hidden)]
pub use byteview::{Builder, Mutator};
