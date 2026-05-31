use std::hash::{BuildHasher, Hash};

use super::super::builder::Scratch;
use super::super::filter::RibbonFilter;
use super::super::hashing::{standard_equation_from_hash, standard_equation_w64};
use super::super::params::{Mode, Params};
use super::params::BurrParams;
use super::threshold::is_bumped;

/// One layer of a built BuRR filter.
pub(crate) struct BurrLayer<S> {
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
    pub(crate) ribbon: RibbonFilter<S>,
}

impl<S> core::fmt::Debug for BurrLayer<S> {
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
///
/// The probe path is allocation-free after the initial `new_scratch` call
/// (one `Scratch` is reused across layers — the largest layer's stride is
/// used).
pub struct BurrFilter<S> {
    params: BurrParams,
    /// Hasher used by the probe-time equation re-compute for the per-
    /// layer bump-check. All `BurrLayer::ribbon`s were given clones of
    /// this same hasher at build time, so hashes agree at the boundary
    /// (`BuildHasher::hash_one` is deterministic for a given hasher
    /// state).
    hasher: S,
    layers: Vec<BurrLayer<S>>,
}

impl<S> BurrFilter<S>
where
    S: BuildHasher + Clone,
{
    pub(crate) fn from_layers(params: BurrParams, hasher: S, layers: Vec<BurrLayer<S>>) -> Self {
        Self {
            params,
            hasher,
            layers,
        }
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
    pub(crate) fn layers_inner(&self) -> &[BurrLayer<S>] {
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

    /// Returns a fresh `Scratch` sized for the largest layer's stride.
    #[must_use]
    pub fn new_scratch(&self) -> Scratch {
        // All layers share the same r (fingerprint stride), so any
        // layer's scratch is interchangeable.
        match self.layers.first() {
            Some(layer) => layer.ribbon.new_scratch(),
            None => Scratch::new(0),
        }
    }

    /// Returns `true` if the key may be present.
    ///
    /// MUST be paired with [`BurrBuilder::build`] (the key-based build
    /// path): the probe hashes `key` via the filter's `BuildHasher` and
    /// looks the hash up under the same hashing the builder used.
    /// Calling `contains(&k)` on a filter built via
    /// [`BurrBuilder::build_from_hashes`] is NOT valid — those filters
    /// were built from caller-supplied u64 hashes that, in general, do
    /// not equal `BuildHasher::hash_one(&k)`, and the probe will report
    /// inserted keys as absent (false negative). Use
    /// [`Self::contains_hash`] with the same u64 hashing the builder
    /// used in that case.
    pub fn contains<Q: Hash + ?Sized>(&self, key: &Q) -> bool {
        let mut scratch = self.new_scratch();
        self.contains_in(key, &mut scratch)
    }

    /// Probe with a pre-computed u64 hash (e.g. xxh3 output from
    /// `crate::hash::hash64`). Equivalent to `contains` when the caller
    /// has already hashed the key — avoids re-running the
    /// `BuildHasher` on the hot path.
    ///
    /// MUST be paired with [`BurrBuilder::build_from_hashes`]: a filter
    /// built via `build(keys)` (which hashes with `BuildHasher`) is NOT
    /// queryable by `contains_hash(h)` unless `h` is the
    /// `BuildHasher::hash_one(key)` value. The on-disk LSM filter
    /// always uses the hash-based build + probe pair so the two stay
    /// consistent.
    ///
    /// Note on probe-mode tagging: a previous reviewer asked whether
    /// the construction mode (keyed vs hashed) should be encoded in
    /// the type so that mismatched pairs fail fast at runtime. The
    /// trade-off was considered and rejected: the in-tree LSM caller
    /// uses [`BurrBuilder::build_from_hashes`] + `contains_hash`
    /// exclusively (see the table filter pipeline), so the keyed API
    /// has a single, well-defined call site (this crate's own tests
    /// plus external callers who explicitly opt in). Adding a runtime
    /// mode tag would cost an extra branch on every probe; splitting
    /// into two filter types would bifurcate every consumer (writer,
    /// reader, builder, wire codec) for no in-tree benefit. The
    /// doc-comment contract above is the canonical guarantee.
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

    /// Allocation-free probe using a caller-provided scratch.
    ///
    /// Same pairing contract as [`Self::contains`]: only valid when
    /// the filter was built via [`BurrBuilder::build`] (key-based
    /// path). A filter built via [`BurrBuilder::build_from_hashes`]
    /// must be probed with [`Self::contains_hash`] instead, otherwise
    /// the probe reports inserted keys as absent.
    ///
    /// Walks layers descend-only: for each layer, recompute the equation
    /// under that layer's seed+m and check the per-block threshold. If
    /// the key would have been BUMPED at construction time
    /// (`offset >= thresholds[block]`), continue to the next layer. Else
    /// delegate to the layer's `RibbonFilter::contains_in` — which
    /// re-derives the same equation internally and runs the GF(2) XOR-
    /// reduce against the stored solution.
    ///
    /// The double equation work per kept-layer is the MVP cost
    /// (correctness first); a follow-up can expose a `contains_with_eq`
    /// path on `RibbonFilter` that reuses our pre-computed equation.
    pub fn contains_in<Q: Hash + ?Sized>(&self, key: &Q, scratch: &mut Scratch) -> bool {
        // Stack-sized throwaway fingerprint buffer reused across layers.
        // `BurrParams::with_*` clamp `r` to 1..=64 so `stride` is 1; the
        // assert pins the invariant.
        debug_assert!(self.params.r <= 64, "BuRR params pin r <= 64");
        let mut fp_throwaway = [0_u64; 1];
        for layer in &self.layers {
            // Build a Params reflecting this layer's m/w/r/seed so the
            // equation-computation matches what the builder did.
            let layer_params = match Params::new(
                layer.m,
                usize::from(self.params.w),
                usize::from(self.params.r),
                Mode::Standard,
            ) {
                Ok(p) => p.with_seed(layer.seed),
                // Unreachable for built filters; fail closed defensively
                // so a future param-validation regression yields a
                // false positive (caller does an index lookup) rather
                // than a false negative.
                Err(_) => return true,
            };

            // Re-hash to learn this layer's `start` and decide bump.
            // Throwaway fingerprint; the real probe uses `scratch`
            // inside `ribbon.contains_in`. The hasher is the one
            // BurrFilter holds — all layers' RibbonFilters were given
            // clones of THIS hasher at build time, so hashes agree by
            // construction (BuildHasher is deterministic).
            fp_throwaway[0] = 0;
            let equation = standard_equation_w64(
                &self.hasher,
                key,
                layer.seed,
                &layer_params,
                &mut fp_throwaway,
            );

            if is_bumped(&equation, &layer.thresholds, self.params.b) {
                // Bumped at build time → not in this layer's ribbon;
                // continue to the next layer.
                continue;
            }

            // Kept at this layer → ribbon authoritatively decides.
            return layer.ribbon.contains_in(key, scratch);
        }
        // Walked all layers without finding a non-bumped layer — would
        // only happen if the input was never inserted in any layer
        // (i.e. a non-member key whose hash always lands at a bumped
        // offset). Definite-not-present.
        false
    }
}

impl<S> core::fmt::Debug for BurrFilter<S> {
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
    /// that same hash directly (no re-hash via `BuildHasher`).
    #[inline]
    #[must_use]
    pub fn contains_hash(&self, hash: u64) -> bool {
        super::wire::contains_hash(&self.decoded, hash)
    }
}
