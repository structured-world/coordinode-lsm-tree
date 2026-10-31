#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use super::super::hashing::standard_equation_from_hash;
use super::super::params::{Mode, Params};
use super::packed::{BandWalk, SEGMENT_ROWS, walk_band};
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
    /// The bit-sliced solution holding this layer's KEPT keys, as the wire
    /// format stores it (see [`super::packed`]).
    pub(crate) z: Vec<u8>,
}

impl core::fmt::Debug for BurrLayer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BurrLayer")
            .field("m", &self.m)
            .field("z_bytes", &self.z.len())
            .finish()
    }
}

/// Whether a built BuRR stores membership fingerprints (probe → bool) or
/// caller-supplied locators (recover → value).
///
/// Recorded in the wire header as the `filter_type` byte so a retrieval
/// filter can never be mis-queried as a membership filter (or vice versa):
/// the membership decoders accept only [`Membership`], the retrieval
/// recover path only [`Retrieval`]. This is a new tag within the existing
/// wire `FORMAT_VERSION`, not a version bump.
///
/// [`Membership`]: BurrFilterKind::Membership
/// [`Retrieval`]: BurrFilterKind::Retrieval
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BurrFilterKind {
    /// RHS = hash-derived fingerprint; query compares and yields presence.
    Membership,
    /// RHS = caller locator; query recovers and yields the stored value.
    Retrieval,
}

/// A built, queryable BuRR filter.
///
/// Layers are tried in order on each probe: layer 0 first, then layer 1
/// (the bumped-from-layer-0 set), etc. A membership filter reports a key
/// "present" if any layer's Ribbon body matches (FPR ≈ 2⁻ʳ); a retrieval
/// filter recovers the stored locator at the first non-bumped layer.
pub struct BurrFilter {
    params: BurrParams,
    kind: BurrFilterKind,
    layers: Vec<BurrLayer>,
}

impl BurrFilter {
    pub(crate) fn from_layers(
        params: BurrParams,
        kind: BurrFilterKind,
        layers: Vec<BurrLayer>,
    ) -> Self {
        Self {
            params,
            kind,
            layers,
        }
    }

    /// Whether this filter stores membership fingerprints or retrieval
    /// locators. The wire encoder maps it to the `filter_type` byte.
    #[must_use]
    pub(crate) fn kind(&self) -> BurrFilterKind {
        self.kind
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
        let mut bytes = Vec::with_capacity(self.encoded_len());
        self.append_wire_bytes(&mut bytes);
        bytes
    }

    /// Appends [`Self::to_wire_bytes`] to `out`, for a caller that frames the
    /// filter inside a larger buffer.
    pub(crate) fn append_wire_bytes(&self, out: &mut Vec<u8>) {
        if !self.layers.is_empty() {
            super::wire::encode_into(self, out);
        }
    }

    /// Returns the parameters this filter was built with.
    #[must_use]
    pub fn params(&self) -> BurrParams {
        self.params
    }

    /// The exact length [`Self::to_wire_bytes`] produces, computed from the
    /// built layers without serialising them.
    ///
    /// This is the figure to account memory and filter budgets against. The
    /// pre-build `estimated_filter_size` cannot be exact, because how many
    /// keys each layer bumps is a property of the hashes, not of the count.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        super::wire::encoded_len(self)
    }

    /// Probe with a pre-computed u64 hash (e.g. xxh3 output from
    /// `crate::hash::hash64`). The hash must be produced the same way
    /// the builder's input hashes were (the on-disk LSM filter uses
    /// `crate::hash::hash64` on both build and probe), so the two stay
    /// consistent.
    #[inline]
    pub fn contains_hash(&self, hash: u64) -> bool {
        // BurrParams::with_fp_rate / with_bpk both clamp r to 1..=64, so a
        // solution row is one word and the fingerprint is a scalar. The
        // debug_assert pins the invariant — if the format ever grows to
        // r > 64 the probe path must be updated at the same time.
        debug_assert!(self.params.r <= 64, "BuRR params pin r <= 64");
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

            let equation = standard_equation_from_hash(hash, layer.seed, &layer_params);
            let fingerprint = equation.fingerprint;

            // Prefetch the band's column words before the threshold check so
            // the hash-random cold miss on them overlaps the is_bumped work. A
            // hint only: the result is unchanged.
            let columns = equation.start / SEGMENT_ROWS * usize::from(self.params.r) * 8;
            super::prefetch::prefetch_span(
                layer.z.as_ptr().wrapping_add(columns),
                usize::from(self.params.r) * 16,
            );

            if is_bumped(&equation, &layer.thresholds, self.params.b) {
                continue;
            }

            debug_assert_eq!(
                equation.coeff_hi, 0,
                "BuRR builds with w <= 64; coeff_hi must be 0",
            );

            return match walk_band::<true>(
                &layer.z,
                self.params.r,
                equation.start,
                equation.coeff_lo,
                fingerprint,
            ) {
                BandWalk::Reduced(_) | BandWalk::Truncated => true,
                BandWalk::Mismatch => false,
            };
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
    pub fn recover_value(&self, hash: u64) -> Option<u64> {
        debug_assert!(self.params.r <= 64, "BuRR params pin r <= 64");
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

            let equation = standard_equation_from_hash(hash, layer.seed, &layer_params);

            // The first layer that does NOT bump this key is the layer that
            // holds it (the builder kept it at exactly that layer). Same
            // routing as `contains_hash`.
            // Prefetch the band's column words before the threshold check so
            // the cold miss on them overlaps the is_bumped work.
            let columns = equation.start / SEGMENT_ROWS * usize::from(self.params.r) * 8;
            super::prefetch::prefetch_span(
                layer.z.as_ptr().wrapping_add(columns),
                usize::from(self.params.r) * 16,
            );

            if is_bumped(&equation, &layer.thresholds, self.params.b) {
                continue;
            }

            debug_assert_eq!(
                equation.coeff_hi, 0,
                "BuRR builds with w <= 64; coeff_hi must be 0",
            );

            // Reducing the band recovers the stored RHS = the locator.
            return match walk_band::<false>(
                &layer.z,
                self.params.r,
                equation.start,
                equation.coeff_lo,
                0,
            ) {
                BandWalk::Reduced(value) => Some(value),
                BandWalk::Mismatch | BandWalk::Truncated => None,
            };
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

/// Single-pass parse + locator recovery over a wire-format *retrieval* BuRR
/// (one serialized from a filter built via
/// [`BurrBuilder::build_from_hashes_with_values`]).
///
/// The retrieval counterpart of [`contains_hash_from_bytes`]: accepts only a
/// retrieval payload and returns the r-bit locator stored for `hash`.
/// `Ok(Some(locator))` is the value at the first non-bumped layer (exact for
/// a key in the set, unspecified for an absent key — the caller verifies the
/// key at the located slot); `Ok(None)` means the ribbon cannot answer (all
/// layers bumped, or a truncated payload) and the caller should fall back to
/// the sorted index.
///
/// # Errors
/// Returns `InvalidHeader` / `InvalidTag` if the wire prefix is unparseable
/// or is not a retrieval payload (e.g. a membership filter).
///
/// [`BurrBuilder::build_from_hashes_with_values`]: super::builder::BurrBuilder::build_from_hashes_with_values
#[inline]
pub fn recover_value_from_bytes(bytes: &[u8], hash: u64) -> crate::Result<Option<u64>> {
    super::wire::recover_value_from_bytes(bytes, hash)
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
