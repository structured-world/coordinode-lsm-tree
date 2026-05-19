// Vendored from https://github.com/WilliamRagstad/ribbon-filter v0.2.0.
// Original work copyright (c) William Rågstad, available upstream under
// MIT OR Apache-2.0. Preserved upstream license texts in `_vendored/`.
//
// This in-tree copy and any modifications (module layout, integration with
// the table::filter framework, removal of the standalone `lib.rs`
// crate-level attributes, and BuRR extensions) are distributed under the
// host crate's declared license: Apache-2.0. The dual-licensed upstream
// permits this — Apache-2.0 alone is one of the two licenses upstream
// offers. If the ribbon module is later extracted back into a standalone
// crate the dual MIT/Apache-2.0 posture can be restored at that time.
//
// This module is the **algorithmic foundation** for the LSM filter
// subsystem. Plan:
//   1. Vendor upstream ribbon-filter (Standard + Homogeneous Ribbon over
//      GF(2)) as the primitive layer — provides hashing, banded solver,
//      packed storage.
//   2. Build BuRR (Bumped Ribbon Retrieval, Walzer & Dillinger 2022) on top
//      of those primitives — multi-layer construction where rows that don't
//      fit the primary band are "bumped" to a smaller secondary BuRR
//      structure, recursively. Closes the construction-failure window
//      Standard Ribbon has and pushes memory to ~1% overhead vs the
//      information-theoretic minimum.
//   3. lsm-tree consumes BuRR directly (no Standard Ribbon intermediate
//      state — bloom is replaced with BuRR in one step).
//   4. (later) Extract `src/table/filter/ribbon/` into a standalone crate
//      (`coordinode-ribbon` or similar) bundling Standard + Homogeneous +
//      BuRR variants and publish to crates.io.

// Vendored upstream code follows its own lint conventions; the in-tree
// copy keeps them so a future extraction back into a standalone crate
// produces a clean diff against the upstream. We deliberately use a
// single crate-attribute `#![allow]` here rather than scattering
// `#[expect]` per item: minimising the diff vs upstream is the priority,
// and a future upstream refactor that removes one of the offending casts
// would otherwise yield an `unfulfilled_lint_expectations` error on the
// next sync.
//
// Lint-scope propagation: a crate-attribute `#![allow]` propagates into
// child modules, INCLUDING the first-party `burr/` submodule. That means
// the safety-critical lints (`expect_used`, `unwrap_used`,
// `indexing_slicing`) are currently relaxed inside `burr/` even though
// it's first-party code that would normally follow the host crate's
// stricter lint policy. Re-denying these inside `burr/` would require
// migrating ~30 existing internal sites in BuRR code to safe
// alternatives — that migration is tracked as a follow-up issue. In the
// meantime, new BuRR code uses `#[expect(..., reason)]` per use site
// for any new suppressions; the inherited blanket allow is for legacy
// sites only.
#![allow(
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::cast_lossless,
    clippy::doc_markdown,
    clippy::unreadable_literal,
    clippy::too_many_lines,
    clippy::redundant_pub_crate,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::missing_const_for_fn,
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::manual_range_contains,
    clippy::use_self,
    clippy::elidable_lifetime_names,
    clippy::missing_fields_in_debug,
    clippy::expect_used,
    clippy::unwrap_used,
    missing_docs
)]

//! Ribbon filter (static approximate-membership filter over GF(2)).
//!
//! Guarantees in the currently-vendored modes (`w <= 64`):
//! - no false negatives for inserted keys after successful build,
//! - probabilistic false positives controlled by `r` fingerprint bits,
//! - deterministic behavior for fixed params, key-set, and hasher.
//!
//! `Mode::Homogeneous` is also available and uses zero right-hand-side
//! equations (smaller storage at the cost of slightly higher false-positive
//! rate at small `r`).
//!
//! See [`Params::new`] for the entry point and [`RibbonBuilder::build`] for
//! the construction call.

pub mod builder;
pub mod burr;
pub mod error;
pub mod filter;
pub mod hashing;
pub mod params;

pub use builder::{RibbonBuilder, Scratch};
pub use error::{BuildError, ConstructionFailure, FilterReprError, ParamError};
pub use filter::RibbonFilter;
#[cfg(feature = "ribbon-serde")]
pub use filter::RibbonFilterRepr;
pub use params::{Mode, Params};

#[cfg(test)]
mod tests;
