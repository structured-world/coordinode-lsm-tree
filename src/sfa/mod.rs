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

#![allow(
    dead_code,
    missing_docs,
    clippy::expect_used,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::redundant_pub_crate,
    reason = "vendored upstream code: keep verbatim where reasonable so the diff against \
              upstream stays mechanical; the crate's own lint defaults are deny-heavy and \
              would force a rewrite of upstream code rather than a faithful inline copy. \
              Some upstream methods (Writer::finish, Reader::new) are not exercised by our \
              in-tree consumers but stay verbatim so the upstream sync diff is a clean copy."
)]

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
