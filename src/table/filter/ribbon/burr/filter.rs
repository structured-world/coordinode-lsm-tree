use std::hash::{BuildHasher, Hash};

use super::super::builder::Scratch;
use super::super::filter::RibbonFilter;
use super::super::hashing::standard_equation_w64;
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
    #[must_use]
    pub fn to_wire_bytes(&self) -> Vec<u8> {
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
    pub fn contains<Q: Hash + ?Sized>(&self, key: &Q) -> bool {
        let mut scratch = self.new_scratch();
        self.contains_in(key, &mut scratch)
    }

    /// Allocation-free probe using a caller-provided scratch.
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
                Err(_) => continue, // unreachable for built filters
            };

            // Re-hash to learn this layer's `start` and decide bump.
            // Throwaway fingerprint buffer; the real probe uses
            // `scratch` inside `ribbon.contains_in`. The hasher is the
            // one BurrFilter holds — all layers' RibbonFilters were
            // given clones of THIS hasher at build time, so hashes
            // agree by construction (BuildHasher is deterministic).
            let stride = usize::from(self.params.r).div_ceil(64);
            let mut fp_throwaway = vec![0_u64; stride];
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
/// [`super::wire`] — intentionally distinct from the vendored
/// `ribbon-serde` repr (that one is for in-memory snapshots).
pub struct BurrFilterReader<'a> {
    decoded: super::wire::DecodedFilter<'a>,
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
    #[must_use]
    pub fn contains_hash(&self, hash: u64) -> bool {
        super::wire::contains_hash(&self.decoded, hash)
    }
}
