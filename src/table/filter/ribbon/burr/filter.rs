#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use super::super::filter::RibbonFilter;
use super::super::hashing::standard_equation_from_hash;
use super::super::params::{Mode, Params};
use super::params::BurrParams;
use super::threshold::is_bumped;

/// One layer of a built BuRR filter.
pub(crate) struct BurrLayer {
    /// Slot count for this layer (== ribbon's m). Kept here so we don't
    /// have to reach into ribbon.params() on every probe.
    pub(crate) m: usize,
    /// Per-layer hash seed (derived from `BurrParams::seed` via the
    /// builder's layer-seed function). Stored so the probe path can
    /// recompute the equation under the same seed used at build time.
    pub(crate) seed: u64,
    /// Per-block thresholds for this layer: `thresholds[block_idx]` is
    /// the largest `offset_in_block` value that is KEPT at this layer.
    /// A key whose `offset_in_block >= thresholds[block_idx]` is BUMPED
    /// to the next layer at probe time (same decision the builder made).
    /// Length = `m.div_ceil(b)`.
    pub(crate) thresholds: Vec<u8>,
    /// The vendored Ribbon filter holding this layer's KEPT keys.
    pub(crate) ribbon: RibbonFilter,
}

impl core::fmt::Debug for BurrLayer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BurrLayer")
            .field("m", &self.m)
            .field("ribbon", &"<RibbonFilter>")
            .finish()
    }
}

/// A built, queryable BuRR filter.
///
/// Layers are tried in order on each probe: layer 0 first, then layer 1
/// (the bumped-from-layer-0 set), etc. A key is "present" if any layer's
/// Ribbon body reports a match. False positives carry the FPR ≈ 2⁻ʳ of
/// the underlying Ribbon layers.
pub struct BurrFilter {
    params: BurrParams,
    layers: Vec<BurrLayer>,
}

impl BurrFilter {
    pub(crate) fn from_layers(params: BurrParams, layers: Vec<BurrLayer>) -> Self {
        Self { params, layers }
    }

    /// Returns the layer count after construction. Useful for diagnostics
    /// and tests; a healthy BuRR build usually settles in 1-2 layers.
    #[must_use]
    pub fn layer_count(&self) -> usize {
        self.layers.len()
    }

    /// Borrowed access to the underlying layer descriptors. Used by the
    /// wire-format encoder; `pub(crate)` so it doesn't leak into the
    /// public API.
    #[must_use]
    pub(crate) fn layers_inner(&self) -> &[BurrLayer] {
        &self.layers
    }

    /// Serialize this filter into the BuRR wire format. The result can
    /// be later parsed by [`BurrFilterReader::new`].
    ///
    /// Returns an empty `Vec` for a filter with zero layers (e.g.
    /// `BurrBuilder::build_from_hashes(&[])`). The decoder rejects
    /// `num_layers == 0` as a malformed header (correctly — a zero-
    /// layer filter cannot answer any membership query), so emitting
    /// the header anyway would yield a wire payload that no reader
    /// can ingest. Empty wire bytes are the canonical "no filter for
    /// this block" signal, identical to what
    /// `build_burr_filter_bytes(_, &[])` returns up at the writer
    /// boundary.
    #[must_use]
    pub fn to_wire_bytes(&self) -> Vec<u8> {
        if self.layers.is_empty() {
            return Vec::new();
        }
        super::wire::encode(self)
    }

    /// Returns the parameters this filter was built with.
    #[must_use]
    pub fn params(&self) -> BurrParams {
        self.params
    }

    /// Probe with a pre-computed u64 hash (e.g. xxh3 output from
    /// `crate::hash::hash64`). The hash must be produced the same way
    /// the builder's input hashes were (the on-disk LSM filter uses
    /// `crate::hash::hash64` on both build and probe), so the two stay
    /// consistent.
    #[inline]
    #[expect(
        clippy::indexing_slicing,
        reason = "All indexing in this function is bounds-safe by construction. \
                  `fingerprint_buf[0]` is a fixed-size [u64; 1] array — index 0 \
                  is always in bounds. `z_words[equation.start + offset]` in the \
                  probe loop is gated by the algorithmic invariant `start ∈ [0, m-w]` \
                  and `offset ∈ [0, w-1]` (set-bit position in coeff_lo, which has \
                  at most w bits), so the sum is `< m = z_words.len()`. The inline \
                  GF(2) XOR-reduce block has a `// start ∈ [0, m-w] ...` comment \
                  restating this invariant near the access. Per-row `.get()` would \
                  add a branch on the probe hot path and dominate per-iter cost."
    )]
    pub fn contains_hash(&self, hash: u64) -> bool {
        // BurrParams::with_fp_rate / with_bpk both clamp r to 1..=64, so
        // stride is always 1. Single u64 buffer for fingerprint, scalar
        // u64 accumulator. The debug_assert pins the invariant — if the
        // format ever grows to r > 64 the probe path must be updated
        // at the same time.
        debug_assert!(self.params.r <= 64, "BuRR params pin r <= 64");
        let mut fingerprint_buf = [0_u64; 1];
        for layer in &self.layers {
            let layer_params = match Params::new(
                layer.m,
                usize::from(self.params.w),
                usize::from(self.params.r),
                Mode::Standard,
            ) {
                Ok(p) => p.with_seed(layer.seed),
                // In-memory filter: layer params were valid at build
                // time, so this is unreachable. Fail closed defensively.
                Err(_) => return true,
            };

            fingerprint_buf[0] = 0;
            let equation =
                standard_equation_from_hash(hash, layer.seed, &layer_params, &mut fingerprint_buf);
            let fingerprint = fingerprint_buf[0];

            if is_bumped(&equation, &layer.thresholds, self.params.b) {
                continue;
            }

            // GF(2) XOR-reduce. start ∈ [0, m-w] and every set bit offset
            // ∈ [0, w-1], so row_index ∈ [0, m-1] is always in-bounds
            // (proven; no per-row bounds check in the inner loop).
            let z_words = layer.ribbon.z_raw_words();
            let mut acc: u64 = 0;
            let mut lo = equation.coeff_lo;
            while lo != 0 {
                let offset = lo.trailing_zeros() as usize;
                acc ^= z_words[equation.start + offset];
                lo &= lo - 1;
            }
            debug_assert_eq!(
                equation.coeff_hi, 0,
                "BuRR builds with w <= 64; coeff_hi must be 0",
            );

            return acc == fingerprint;
        }
        false
    }

    /// Recover the r-bit value stored for `hash` in a *retrieval* BuRR
    /// (one built via [`BurrBuilder::build_from_hashes_with_values`]).
    ///
    /// For a key in the built set this returns its exact stored value
    /// (`Some(locator)`); for an absent key it returns an unspecified r-bit
    /// value, so the caller must verify the key at the located slot to reject
    /// absent keys — the locate step subsumes the membership probe. Returns
    /// `None` only if no layer can answer (every layer bumps the key), which
    /// a well-formed build never produces since the final layer accepts all.
    ///
    /// [`BurrBuilder::build_from_hashes_with_values`]: super::builder::BurrBuilder::build_from_hashes_with_values
    #[inline]
    #[must_use]
    #[expect(
        clippy::indexing_slicing,
        reason = "Same bounds invariant as `contains_hash`: start ∈ [0, m-w] and every set-bit \
                  offset ∈ [0, w-1], so `z_words[start + offset]` is `< m = z_words.len()`. A \
                  per-row `.get()` would add a branch on the locate hot path."
    )]
    pub fn recover_value(&self, hash: u64) -> Option<u64> {
        debug_assert!(self.params.r <= 64, "BuRR params pin r <= 64");
        let value_mask = if self.params.r == 64 {
            u64::MAX
        } else {
            (1u64 << self.params.r) - 1
        };
        let mut fingerprint_buf = [0_u64; 1];
        for layer in &self.layers {
            let layer_params = match Params::new(
                layer.m,
                usize::from(self.params.w),
                usize::from(self.params.r),
                Mode::Standard,
            ) {
                Ok(p) => p.with_seed(layer.seed),
                // Layer params were valid at build time → unreachable.
                Err(_) => return None,
            };

            fingerprint_buf[0] = 0;
            let equation =
                standard_equation_from_hash(hash, layer.seed, &layer_params, &mut fingerprint_buf);

            // The first layer that does NOT bump this key is the layer that
            // holds it (the builder kept it at exactly that layer). Same
            // routing as `contains_hash`.
            if is_bumped(&equation, &layer.thresholds, self.params.b) {
                continue;
            }

            // GF(2) XOR-reduce recovers the stored RHS = the locator.
            let z_words = layer.ribbon.z_raw_words();
            let mut acc: u64 = 0;
            let mut lo = equation.coeff_lo;
            while lo != 0 {
                let offset = lo.trailing_zeros() as usize;
                acc ^= z_words[equation.start + offset];
                lo &= lo - 1;
            }
            debug_assert_eq!(
                equation.coeff_hi, 0,
                "BuRR builds with w <= 64; coeff_hi must be 0",
            );

            return Some(acc & value_mask);
        }
        None
    }
}

impl core::fmt::Debug for BurrFilter {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BurrFilter")
            .field("params", &self.params)
            .field("layer_count", &self.layers.len())
            .finish()
    }
}

/// Wire-format reader for a BuRR filter loaded from a serialized buffer.
///
/// This is the type the LSM filter framework consumes: it owns a borrowed
/// slice of the on-disk filter block, parses the BuRR header, and answers
/// `contains_hash` lookups. Wire format documented in
/// `super::wire` — intentionally distinct from the vendored
/// `ribbon-serde` repr (that one is for in-memory snapshots).
#[derive(Debug)]
pub struct BurrFilterReader<'a> {
    decoded: super::wire::DecodedFilter<'a>,
}

/// Single-pass parse + probe over a wire-format BuRR filter buffer.
///
/// This is the preferred entry point for the LSM table read path: it
/// parses the header and walks per-layer payloads in place without
/// allocating an intermediate `BurrFilterReader` (the
/// `Vec<LayerView>` inside is the only heap allocation a fresh reader
/// would do). Use this when the wire buffer is already in the block
/// cache and you only need a one-shot membership check.
///
/// Behaviour matches `BurrFilterReader::new(bytes)?.contains_hash(hash)`
/// modulo allocation: on a structurally invalid header returns
/// `Err(InvalidHeader)`; on payload-level corruption (truncated z
/// slice past header-validated lengths) fails closed with `Ok(true)`
/// so the caller falls through to a real index lookup rather than
/// reporting a false negative.
#[inline]
pub fn contains_hash_from_bytes(bytes: &[u8], hash: u64) -> crate::Result<bool> {
    super::wire::contains_hash_from_bytes(bytes, hash)
}

impl<'a> BurrFilterReader<'a> {
    /// Parse a serialized BuRR filter slice. Returns an error if the
    /// magic bytes don't match, the version is unrecognised, or the
    /// buffer is truncated.
    pub fn new(bytes: &'a [u8]) -> crate::Result<Self> {
        let decoded = super::wire::decode(bytes)?;
        Ok(Self { decoded })
    }

    /// Number of layers in the decoded filter.
    #[must_use]
    pub fn layer_count(&self) -> usize {
        self.decoded.layers.len()
    }

    /// Probe with a pre-computed key hash. Used by the LSM filter
    /// framework's `block::FilterBlock` — the table read path already
    /// computes a u64 hash for block indexing, and the filter consumes
    /// that same hash directly (no re-hashing).
    #[inline]
    #[must_use]
    pub fn contains_hash(&self, hash: u64) -> bool {
        super::wire::contains_hash(&self.decoded, hash)
    }
}
