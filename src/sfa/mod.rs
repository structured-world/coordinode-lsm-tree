// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2025-present, fjall-rs
//
// Vendored from `sfa` v1.0.0 (https://github.com/fjall-rs/sfa) at
// upstream main 4d9a9aa2 (2026-01-14). Inlined into this crate so
// the public `sfa` crates.io dependency can be dropped — upstream
// does not accept PRs from this fork and we want local control
// over the format used by the blob-file (vlog) layer.
//
// LICENSE-MIT and LICENSE-APACHE preserved alongside this file.
// Future upstream sync, if any, is a manual diff across the tree.

//! Sectioned-archive format inherited from the upstream `sfa` crate.
//!
//! Used by the blob-file (vlog) layer as its on-disk wire format
//! for keys + values. The Blocks-based manifest layer (V5-2 #297)
//! does NOT use this module; manifest framing lives in
//! `crate::manifest_blocks`.
//!
//! Crate-internal: `pub(crate)` re-exports below give the rest of
//! the tree the same `Writer` / `Reader` / `TocEntry` / `Checksum`
//! / `Error` surface that the external `sfa::*` namespace used to
//! provide. No external API surface — consumers reach this through
//! callers in `vlog::blob_file::*` only.

// Vendored sfa: most lints triggered by upstream's coding style
// have been driven to zero by the per-site fixes (typed errors
// instead of expect, fully-documented exports, bounded
// allocations); the module-level suppression list that previously
// shielded those sites is now empty. Per-line `#[expect(...)]`
// attributes remain on the few sites that genuinely still need
// them — `#[expect]` will fire if any of those goes stale, which
// is the migration signal we want.

mod checksum;
mod checksum_writer;
mod error;
mod reader;
mod toc;
mod trailer;
mod writer;

pub(crate) type Result<T> = std::result::Result<T, Error>;

pub use checksum::Checksum;
pub use error::Error;
pub use reader::Reader;
pub use toc::{Toc, entry::TocEntry};
pub use writer::Writer;
