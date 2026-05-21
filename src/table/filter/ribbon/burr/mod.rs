// BuRR (Bumped Ribbon Retrieval) — Walzer & Dillinger 2022, arXiv:2109.01892.
//
// Built on top of the vendored Ribbon primitives in `super::`. The Ribbon
// layer provides the GF(2) banded solver and packed `r`-bit fingerprint
// storage; BuRR adds:
//   * a per-block THRESHOLD scheme that deterministically decides which
//     keys are "bumped" out of a layer (rather than failing the whole
//     construction);
//   * MULTI-LAYER composition — bumped keys are passed to a smaller
//     secondary BuRR layer, recursively, until the residual fits;
//   * a BUMP-AWARE probe path — at each layer, check the block's
//     threshold against the key's offset-in-block; if bumped, walk to the
//     next layer; otherwise probe the Ribbon body and compare fingerprints.
//
// # Why BuRR over Standard Ribbon
//
// Standard Ribbon has a probabilistic construction failure mode: rare
// "inconsistent equation" or "out-of-bounds" terminations require seed
// retries. BuRR replaces retries with bumping — failure-prone keys go to
// the next layer instead of aborting the build. Memory overhead vs the
// information-theoretic lower bound:
//
//   bloom (current default):            ~45%
//   BinaryFuse8:                        ~36%
//   Standard Ribbon (vendored):         ~14%
//   BuRR (this module):                  ~1%
//
// # Architecture
//
//   BurrParams
//     ├─ key count `n`
//     ├─ fingerprint bits `r` (derived from FPR, FPR ≈ 2^-r)
//     ├─ band width `w` (= 64; single-word band)
//     ├─ block size `b` (rows per block, typically 64 = `w`)
//     └─ max layer count (typically 3; last layer always succeeds)
//
//   BurrBuilder
//     ├─ compute per-key (start, block_idx, offset_in_block, band, fp)
//     ├─ per-block threshold selection — bucket offsets, pick the largest
//     │  threshold τ such that {keys with offset < τ} fits the block's
//     │  ribbon capacity
//     ├─ partition: kept ↦ ribbon build for this layer, bumped ↦ next layer
//     └─ recurse until bumped set is small enough to fit at full capacity
//        (last layer trivially succeeds)
//
//   BurrFilter
//     ├─ per-layer: (thresholds: Vec<u8>, ribbon: RibbonFilter)
//     ├─ probe walks layers: re-hash with layer's seed, check threshold,
//     │  either descend or run RibbonFilter::contains_in
//     └─ wire format: serialised as `MAGIC | filter_type=Burr | header |
//        per-layer (thresholds bytes + ribbon z bits)`
//
// The wire format is intentionally NOT compatible with the upstream
// ribbon-filter crate's serde-based repr — that one is meant for in-memory
// snapshot/restore, while ours is the on-disk SST filter block format
// used by the LSM. Both can coexist if/when the ribbon module is extracted
// into a standalone crate.

// The parent `ribbon::` module-level `#![allow(...)]` covers vendored
// upstream code (clippy::indexing_slicing, clippy::expect_used,
// clippy::unwrap_used, etc.) and previously leaked into this BuRR
// submodule. We re-deny those three lints here so first-party BuRR
// code holds itself to the same bar as the rest of the crate. Each
// remaining `expect()` / `unwrap()` / direct index inside BuRR is
// either:
//
//   * justified by a `#[expect(..., reason = "<invariant>")]`
//     attribute documenting why the panic can't happen (per-call-site
//     for hot paths, function-scoped where multiple sites share the
//     same invariant), OR
//   * inside the `#[cfg(test)]` test module which has its own broader
//     suppression for test ergonomics.
#![deny(clippy::indexing_slicing, clippy::expect_used, clippy::unwrap_used)]

pub mod builder;
pub mod error;
pub mod filter;
pub mod params;
pub(crate) mod threshold;
pub(crate) mod wire;

pub use builder::BurrBuilder;
pub use error::{BurrBuildError, BurrConstructionFailure};
pub use filter::{BurrFilter, BurrFilterReader, contains_hash_from_bytes};
pub use params::BurrParams;

#[cfg(test)]
mod tests;
