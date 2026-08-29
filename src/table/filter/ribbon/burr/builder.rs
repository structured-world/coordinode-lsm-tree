#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use super::super::builder::RibbonBuilder;
use super::super::error::{BuildError, ConstructionFailure};
use super::super::filter::RibbonFilter;
use super::super::hashing::StandardEquation;
use super::super::params::{Mode, Params};
use super::error::BurrBuildError;
use super::filter::{BurrFilter, BurrFilterKind, BurrLayer};
use super::params::BurrParams;
use super::threshold::{compute_thresholds, is_bumped, partition_keys_by_threshold};

/// Builds a BuRR filter from a key set.
///
/// # Construction sketch
///
/// For each layer (0 to `max_layers - 1`):
///   1. Hash every input key with the layer's derived seed to produce a
///      `StandardEquation` (gives `start = block_idx * b + offset`).
///   2. Run `compute_thresholds` over those equations to pick per-block
///      threshold `τ_i`. A key with `offset < τ_i` is KEPT in this layer;
///      a key with `offset >= τ_i` is BUMPED to the next layer.
///   3. Partition keys into `kept` and `bumped` via
///      `partition_keys_by_threshold`.
///   4. Build a vendored Standard Ribbon over `kept` — the threshold
///      scheme caps per-block load to ~90%, so this build almost always
///      succeeds outright; a key the membership solve still cannot place
///      is bumped like any other.
///   5. Push `BurrLayer { thresholds, ribbon }` onto the layer stack.
///   6. Recurse with `remaining = bumped`.
///
/// The last layer has no next layer to bump into, so it forces
/// `thresholds[..] = b` (accept everything) and is sized with an enlarged
/// `m` so the Ribbon build absorbs its residual. A key it still cannot
/// place surfaces as `LayerExhaustion` rather than going missing.
pub struct BurrBuilder {
    params: BurrParams,
}

impl core::fmt::Debug for BurrBuilder {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BurrBuilder")
            .field("params", &self.params)
            .finish()
    }
}

impl BurrBuilder {
    pub fn new(params: BurrParams) -> Result<Self, BurrBuildError> {
        if params.n == 0 {
            return Err(BurrBuildError::InvalidParams("n must be > 0"));
        }
        if !(1..=64).contains(&params.r) {
            return Err(BurrBuildError::InvalidParams("r must be in 1..=64"));
        }
        // w == 64 is a hard invariant: the BuRR probe path
        // (BurrFilter::contains_hash, wire::contains_hash) iterates set
        // bits of coeff_lo (u64) ONLY and asserts coeff_hi == 0 via
        // debug_assert. A w > 64 build would silently produce filters
        // that the probe path misses bits on → false negatives. The
        // BurrParams constructors (with_fp_rate, with_bpk) pin w to
        // 64; this check defends against callers that build params by
        // hand or via deserialisation.
        if params.w != 64 {
            return Err(BurrBuildError::InvalidParams("w must be exactly 64"));
        }
        if params.b == 0 {
            return Err(BurrBuildError::InvalidParams("b must be > 0"));
        }
        // `BurrParams::layer_m` rounds the per-layer slot count up to a
        // multiple of `b` and floors at `b`, so its result is always
        // `>= b`. But the vendored Ribbon `Params::new(m, w=64, ...)`
        // also requires `m >= w`. If `b < w` (= 64), `layer_m` can hand
        // Ribbon an `m` between `b` and `w-1`, which Ribbon rejects
        // with a "vendored ribbon param error" that hides the real
        // invariant. Enforce `b >= w` here so the floor on `layer_m`
        // is at least `w` and Ribbon never sees an undersized layer.
        if params.b < params.w {
            return Err(BurrBuildError::InvalidParams("b must be >= w"));
        }
        if params.max_layers == 0 {
            return Err(BurrBuildError::InvalidParams("max_layers must be > 0"));
        }
        Ok(Self { params })
    }

    /// Build from pre-computed u64 key hashes (e.g. xxh3 outputs from
    /// `crate::hash::hash64`).
    ///
    /// This is the entry point the LSM filter writer uses: it has
    /// already hashed every key with xxh3 for filter-block indexing
    /// and pipes those u64s directly into BuRR.
    pub fn build_from_hashes(&self, hashes: &[u64]) -> Result<BurrFilter, BurrBuildError> {
        // Borrowed-slice variant — copies the input into the per-layer
        // working buffer. Use [`Self::build_from_hashes_owned`] to move
        // an existing `Vec<u64>` instead, avoiding the up-front clone
        // on large filter partitions.
        self.build_from_hashes_owned(hashes.to_vec())
    }

    /// Same as [`Self::build_from_hashes`] but consumes a caller-owned
    /// `Vec<u64>` directly, saving the up-front `to_vec()` clone. The
    /// filter writer uses this on its accumulated `bloom_hash_buffer`
    /// so per-partition construction doesn't pay the copy cost twice
    /// (once here, once during the per-layer recursion).
    pub fn build_from_hashes_owned(&self, hashes: Vec<u64>) -> Result<BurrFilter, BurrBuildError> {
        self.build_layers(hashes, None)
    }

    /// Build a *retrieval* BuRR from pre-computed key hashes plus a parallel
    /// slice of r-bit locators. Where [`Self::build_from_hashes`] stores a
    /// hash-derived membership fingerprint per key, this stores `locators[i]`
    /// as the value recovered for `hashes[i]`.
    ///
    /// A later [`BurrFilter::recover_value`] query returns the exact locator
    /// for a key in the set; for an absent key it returns an unspecified
    /// r-bit value, which the caller distinguishes by verifying the key at
    /// the located slot (the locate step subsumes the membership check).
    ///
    /// # Errors
    /// - `InvalidParams` if `hashes` is empty, `hashes.len() != locators.len()`,
    ///   or any locator does not fit in `r` bits.
    /// - `RibbonLayerFailed` / `LayerExhaustion` on a construction failure
    ///   (same conditions as [`Self::build_from_hashes`]).
    pub fn build_from_hashes_with_values(
        &self,
        hashes: &[u64],
        locators: &[u64],
    ) -> Result<BurrFilter, BurrBuildError> {
        if hashes.len() != locators.len() {
            return Err(BurrBuildError::InvalidParams(
                "hashes and locators must have equal length",
            ));
        }
        // A locator wider than r bits would be silently truncated by the
        // ribbon and resolve to the wrong block/slot. Reject loudly at build
        // time rather than mis-locating at read time.
        let value_mask = if self.params.r == 64 {
            u64::MAX
        } else {
            (1u64 << self.params.r) - 1
        };
        if locators.iter().any(|&loc| loc & !value_mask != 0) {
            return Err(BurrBuildError::InvalidParams(
                "locator does not fit in r bits",
            ));
        }
        self.build_layers(hashes.to_vec(), Some(locators.to_vec()))
    }

    /// Shared layer-recursion driver for both the membership build
    /// (`values = None`, RHS = hash fingerprint) and the retrieval build
    /// (`values = Some`, RHS = caller locator). Keeping one loop means the
    /// threshold / partition / per-layer sizing logic cannot diverge between
    /// the two filter flavours.
    fn build_layers(
        &self,
        hashes: Vec<u64>,
        values: Option<Vec<u64>>,
    ) -> Result<BurrFilter, BurrBuildError> {
        // Empty input would produce a zero-layer filter that
        // `to_wire_bytes` correctly serialises as an empty Vec, but
        // `BurrFilterReader::new` rejects num_layers == 0 — so the
        // build → to_wire_bytes → read round-trip breaks for empty
        // input. Reject up front so callers see the error at build
        // time rather than at the first read.
        if hashes.is_empty() {
            return Err(BurrBuildError::InvalidParams("key set must be non-empty"));
        }
        // Capture the flavour before the loop drains `values`: presence of
        // a value slice means a retrieval (key → locator) build, absence a
        // membership build. The wire encoder maps it to the filter_type tag.
        let kind = if values.is_some() {
            BurrFilterKind::Retrieval
        } else {
            BurrFilterKind::Membership
        };
        let mut remaining: Vec<u64> = hashes;
        let mut remaining_values: Option<Vec<u64>> = values;
        let mut layers: Vec<BurrLayer> = Vec::with_capacity(usize::from(self.params.max_layers));

        for layer_idx in 0..self.params.max_layers {
            if remaining.is_empty() {
                break;
            }

            let is_last_layer = layer_idx + 1 == self.params.max_layers;
            let layer_seed = derive_layer_seed(self.params.seed, layer_idx);
            let layer_input = remaining.len();

            let m_target = self.params.layer_m(layer_input);
            let m = if is_last_layer {
                // Last-layer slot count, doubled then floored at `b * 4`.
                // `m_target` is bounded by the key count, so the doubling cannot
                // overflow usize on 64-bit; the saturating guards a 32-bit edge.
                let doubled = m_target.saturating_mul(2);
                doubled.max(usize::from(self.params.b) * 4)
            } else {
                m_target
            };

            let layer_w = usize::from(self.params.w);
            let layer_r = usize::from(self.params.r);
            let equation_params = Params::new(m, layer_w, layer_r, Mode::Standard)
                .map_err(static_param_err)?
                .with_seed(layer_seed);

            // Compute equations directly from hashes — skip hash_one.
            let stride = layer_r.div_ceil(64);
            let mut fp_throwaway = vec![0_u64; stride];
            let mut equations: Vec<StandardEquation> = Vec::with_capacity(remaining.len());
            for hash in &remaining {
                fp_throwaway.fill(0);
                let eq = super::super::hashing::standard_equation_from_hash(
                    *hash,
                    layer_seed,
                    &equation_params,
                    &mut fp_throwaway,
                );
                equations.push(eq);
            }

            let thresholds = if is_last_layer {
                let block_count = m.div_ceil(usize::from(self.params.b));
                vec![self.params.b; block_count]
            } else {
                compute_thresholds(&equations, m, self.params.b)
            };

            let ribbon_builder = RibbonBuilder::new(equation_params).map_err(|e| {
                BurrBuildError::RibbonLayerFailed {
                    layer_index: usize::from(layer_idx),
                    ribbon_error: e,
                }
            })?;

            // Partition the layer input into kept (built here) and bumped
            // (forwarded to the next layer). For the retrieval build each
            // locator travels with its key through the partition so the
            // bumped set carries the right values; the ribbon is then built
            // over the kept locators. For the membership build the RHS is the
            // hash fingerprint, so no values ride along.
            let mut partition = match &remaining_values {
                None => {
                    let (kept, bumped) = partition_keys_by_threshold(
                        &remaining,
                        &equations,
                        &thresholds,
                        self.params.b,
                    );
                    LayerPartition {
                        kept,
                        kept_values: None,
                        bumped,
                        bumped_values: None,
                    }
                }
                Some(vals) => {
                    let pairs: Vec<(u64, u64)> = remaining
                        .iter()
                        .copied()
                        .zip(vals.iter().copied())
                        .collect();
                    let (kept, bumped) =
                        partition_keys_by_threshold(&pairs, &equations, &thresholds, self.params.b);
                    let (kept, kept_values): (Vec<u64>, Vec<u64>) = kept.into_iter().unzip();
                    let (bumped, bumped_values): (Vec<u64>, Vec<u64>) = bumped.into_iter().unzip();
                    LayerPartition {
                        kept,
                        kept_values: Some(kept_values),
                        bumped,
                        bumped_values: Some(bumped_values),
                    }
                }
            };
            let mut thresholds = thresholds;
            let kept_ribbon = build_layer_ribbon(
                &ribbon_builder,
                usize::from(layer_idx),
                &equation_params,
                self.params.b,
                &mut thresholds,
                &mut partition,
            )?;
            remaining = partition.bumped;
            let bumped_values = partition.bumped_values;

            layers.push(BurrLayer {
                m,
                seed: layer_seed,
                thresholds,
                ribbon: kept_ribbon,
            });

            remaining_values = bumped_values;
        }

        if !remaining.is_empty() {
            return Err(BurrBuildError::LayerExhaustion {
                layers_attempted: usize::from(self.params.max_layers),
                remaining_keys: remaining.len(),
            });
        }

        Ok(BurrFilter::from_layers(self.params, kind, layers))
    }
}

/// One layer's key split while its ribbon is solved: the keys placed in this
/// layer, and the keys forwarded to the next one. A solve that cannot place a
/// key moves it across, so the two sets are only final once the layer is built.
///
/// The value vectors are `Some` exactly for a retrieval build, where each
/// locator has to travel with its key.
struct LayerPartition {
    kept: Vec<u64>,
    kept_values: Option<Vec<u64>>,
    bumped: Vec<u64>,
    bumped_values: Option<Vec<u64>>,
}

/// Builds one layer's ribbon, BUMPING a key the solve cannot place instead of
/// failing the whole filter.
///
/// An unplaceable key is an ordinary outcome of an unlucky band placement, and
/// BuRR's answer to one is the next layer.
///
/// Bumping a key means LOWERING its block's threshold to the key's offset, not
/// merely moving it across: a probe routes purely on `offset >= threshold`, so
/// a key removed from a layer whose threshold still claims it would be looked
/// up in the layer that no longer holds it and read as ABSENT. A filter that
/// answers absent for a key it holds makes the point read skip existing data,
/// so the threshold and the partition move together. The lowered threshold
/// re-classifies every kept key at or above that offset in the same block, and
/// those are repartitioned in the same pass.
///
/// Nothing is hidden by this: a key bumped past the last layer leaves
/// `remaining` non-empty and surfaces as `LayerExhaustion`, so the cost is
/// filter density, never a false negative. The loop terminates because every
/// pass strictly lowers one block's threshold, and at zero the whole block is
/// bumped.
///
/// MEMBERSHIP builds only. A RETRIEVAL build (the point-read locator) can hit
/// the same failure for a second reason: two keys sharing a hash but mapping
/// to different values, which no layer can satisfy. Its consumer TRUSTS the
/// answer and reads the addressed block without re-checking the sorted index,
/// so quietly bumping one of the pair would answer the other key with the
/// wrong block and hand back a stale value. There, failing the build is the
/// correct outcome: the caller skips the section and point reads fall back.
fn build_layer_ribbon(
    builder: &RibbonBuilder,
    layer_index: usize,
    equation_params: &Params,
    b: u8,
    thresholds: &mut [u8],
    partition: &mut LayerPartition,
) -> Result<RibbonFilter, BurrBuildError> {
    let (layer_seed, m) = (equation_params.seed, equation_params.m);
    // One scratch buffer for the whole helper, refilled per key, mirroring the
    // layer loop above: the repartition below asks for an equation per kept
    // key, and allocating one there would be an allocation per key per pass.
    let mut fingerprint = alloc::vec![0_u64; equation_params.r.div_ceil(64)];
    let equation_of = |hash: u64, scratch: &mut [u64]| {
        scratch.fill(0);
        super::super::hashing::standard_equation_from_hash(
            hash,
            layer_seed,
            equation_params,
            scratch,
        )
    };
    loop {
        let ribbon_error = match &partition.kept_values {
            None => builder.build_with_seed_verbatim_from_hashes(&partition.kept, layer_seed, m),
            Some(values) => {
                builder.build_with_seed_verbatim_from_values(&partition.kept, values, layer_seed, m)
            }
        };
        let ribbon_error = match ribbon_error {
            Ok(ribbon) => return Ok(ribbon),
            Err(e) => e,
        };
        let BuildError::ConstructionFailed {
            last_failure: ConstructionFailure::InconsistentEquation { key_index, .. },
            ..
        } = &ribbon_error
        else {
            return Err(BurrBuildError::RibbonLayerFailed {
                layer_index,
                ribbon_error,
            });
        };
        let key_index = *key_index;
        if partition.kept_values.is_some() || key_index >= partition.kept.len() {
            return Err(BurrBuildError::RibbonLayerFailed {
                layer_index,
                ribbon_error,
            });
        }
        let Some(unplaceable) = partition.kept.get(key_index).copied() else {
            return Err(BurrBuildError::RibbonLayerFailed {
                layer_index,
                ribbon_error,
            });
        };
        let equation = equation_of(unplaceable, &mut fingerprint);
        let block = equation.start / usize::from(b);
        #[expect(
            clippy::cast_possible_truncation,
            reason = "the offset is start % b, and b is a u8"
        )]
        let offset = (equation.start % usize::from(b)) as u8;
        let Some(threshold) = thresholds.get_mut(block) else {
            return Err(BurrBuildError::RibbonLayerFailed {
                layer_index,
                ribbon_error,
            });
        };
        // The key was KEPT, so its offset is below this block's threshold and
        // the assignment strictly lowers it, which is what bounds the loop.
        *threshold = offset;
        let (kept, newly_bumped): (Vec<u64>, Vec<u64>) = core::mem::take(&mut partition.kept)
            .into_iter()
            .partition(|hash| !is_bumped(&equation_of(*hash, &mut fingerprint), thresholds, b));
        partition.kept = kept;
        partition.bumped.extend(newly_bumped);
    }
}

/// Derive a per-layer seed from the root seed.
///
/// Each layer must hash to a different `(start, band, fp)` distribution
/// so that keys bumped from layer i get a fresh slot-space allocation at
/// layer i+1. Splitmix64 mixes the layer index into the root seed.
pub(crate) fn derive_layer_seed(root: u64, layer_idx: u8) -> u64 {
    let mut z = root.wrapping_add(u64::from(layer_idx).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn static_param_err(_e: super::super::error::ParamError) -> BurrBuildError {
    BurrBuildError::InvalidParams("vendored ribbon param error during burr build")
}
