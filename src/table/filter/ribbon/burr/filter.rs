use std::hash::{BuildHasher, Hash};

use super::super::builder::Scratch;
use super::super::filter::RibbonFilter;
use super::params::BurrParams;

/// One layer of a built BuRR filter.
pub(crate) struct BurrLayer<S> {
    /// Slot count for this layer (== ribbon's m).
    pub(crate) m: usize,
    /// The vendored Ribbon filter holding this layer's keys.
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
    // `hasher` is held so a future per-key bumping path can re-hash bumped
    // keys with a layer-specific seed using THIS instance's hasher (rather
    // than cloning one from each ribbon). Currently unused in the MVP
    // probe path because each layer's RibbonFilter owns its own hasher
    // clone; suppress dead-field until that path lands.
    #[allow(dead_code)]
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
    pub fn contains_in<Q: Hash + ?Sized>(&self, key: &Q, scratch: &mut Scratch) -> bool {
        // MVP probe semantics: each layer absorbed its entire input
        // (failed layers were skipped and their input bumped to the next
        // layer), so a key is present iff EXACTLY ONE layer reports it.
        // Iterating until first hit is correct because builds inserted
        // each key in the first layer that successfully accepted it; all
        // earlier layers (that this key was bumped from) cannot have a
        // fingerprint match for this key's hash. False-positive layers
        // are independent so the union FPR adds — bounded by the per-
        // layer FPR times layer count (typically 1-2× the configured
        // per-layer FPR).
        for layer in &self.layers {
            if layer.ribbon.contains_in(key, scratch) {
                return true;
            }
        }
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
/// `contains_hash` lookups. The wire format is documented in
/// `super::mod` and is intentionally NOT identical to the vendored
/// `ribbon-serde` repr (that one is for in-memory snapshots).
///
/// Implementation TODO (next task in the BuRR rollout): wire-format
/// encode/decode. The Reader type is declared here so `mod.rs` can re-
/// export it; the body is filled in by the next commit in this branch.
pub struct BurrFilterReader<'a> {
    _bytes: &'a [u8],
}

impl<'a> BurrFilterReader<'a> {
    /// Parse a serialized BuRR filter. Returns an error if the buffer is
    /// truncated or the header bytes are unrecognised.
    ///
    /// TODO: implement wire format in the next commit (task #16).
    pub fn new(bytes: &'a [u8]) -> crate::Result<Self> {
        Ok(Self { _bytes: bytes })
    }

    /// Probe path consumed by the LSM filter framework's `block::FilterBlock`.
    ///
    /// TODO: implement once the wire-format decode is in place.
    #[must_use]
    pub fn contains_hash(&self, _hash: u64) -> bool {
        // Conservative placeholder: returning `true` is safe (it means
        // "may be present", which falls back to the actual block lookup).
        // Returning false would be a correctness bug (false negative).
        true
    }
}
