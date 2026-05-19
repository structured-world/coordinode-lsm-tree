// Vendored from https://github.com/WilliamRagstad/ribbon-filter v0.2.0
// Original work copyright (c) William Rågstad, dual-licensed MIT OR
// Apache-2.0. Preserved license texts in `_vendored/`. This in-tree copy is
// continued under this crate's existing Apache-2.0 OR MIT terms (same dual
// license, so no incompatibility). Modifications made to the upstream source
// — module layout, integration with the table::filter framework, removal of
// the standalone `lib.rs` crate-level attributes, and any future BuRR
// extensions — are this crate's own contribution and licensed as above.
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
