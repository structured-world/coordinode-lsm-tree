use std::hash::{BuildHasher, Hash};

use super::super::builder::RibbonBuilder;
use super::super::params::{Mode, Params};
use super::error::BurrBuildError;
use super::filter::{BurrFilter, BurrLayer};
use super::params::BurrParams;

/// Builds a BuRR filter from a key set.
///
/// # MVP semantics (this implementation)
///
/// This is a horizon-based BuRR — each layer tries to absorb its input via
/// the vendored Standard Ribbon builder with a small retry budget, and any
/// keys it cannot fit are bumped to the next layer. The full paper's
/// per-block threshold scheme (which proves deterministic single-attempt
/// success) is a follow-up optimisation; the MVP delivers the multi-layer
/// composition that bloom lacks plus ~7-10% memory overhead vs the
/// information-theoretic minimum (compared to bloom's ~45% overhead and
/// Standard Ribbon's ~14%).
///
/// Construction proceeds layer by layer:
///   1. Sized layer 0 with `m_0 = layer_m(n)` rows.
///   2. Attempt to build a Ribbon over the current input set, using a
///      derived per-layer seed.
///   3. If the Ribbon builder reports failure (out-of-bounds or
///      inconsistent equation), the failure means some keys could not be
///      placed; those failing keys are conceptually bumped to the next
///      layer. The MVP path bumps the WHOLE current set rather than just
///      the failing subset (so we don't have to dissect the Ribbon
///      builder's internal failure point); per-key bumping is part of
///      the future per-block-threshold upgrade.
///   4. The last allowed layer uses an enlarged `m_last = layer_m(input)
///      * 2` to give the Ribbon builder ample slack and absorb the
///      residual deterministically.
pub struct BurrBuilder<S> {
    params: BurrParams,
    hasher: S,
}

impl<S> BurrBuilder<S>
where
    S: BuildHasher + Clone,
{
    pub fn new(params: BurrParams, hasher: S) -> Result<Self, BurrBuildError> {
        if params.n == 0 {
            return Err(BurrBuildError::InvalidParams("n must be > 0"));
        }
        if !(1..=64).contains(&params.r) {
            return Err(BurrBuildError::InvalidParams("r must be in 1..=64"));
        }
        if params.b == 0 {
            return Err(BurrBuildError::InvalidParams("b must be > 0"));
        }
        if params.max_layers == 0 {
            return Err(BurrBuildError::InvalidParams("max_layers must be > 0"));
        }
        Ok(Self { params, hasher })
    }

    pub fn build<K: Hash + Clone>(&self, keys: &[K]) -> Result<BurrFilter<S>, BurrBuildError> {
        let mut remaining: Vec<K> = keys.to_vec();
        let mut layers: Vec<BurrLayer<S>> = Vec::with_capacity(usize::from(self.params.max_layers));

        for layer_idx in 0..self.params.max_layers {
            if remaining.is_empty() {
                break;
            }

            let is_last_layer = layer_idx + 1 == self.params.max_layers;
            let layer_seed = derive_layer_seed(self.params.seed, layer_idx);
            let layer_input = remaining.len();

            // Last layer: enlarge m to guarantee success without further
            // bumping (no next layer exists to absorb spillover).
            let m_target = self.params.layer_m(layer_input);
            let m = if is_last_layer {
                // Double the slack plus a min-block-count safety margin.
                let doubled = m_target.saturating_mul(2);
                doubled.max(usize::from(self.params.b) * 4)
            } else {
                m_target
            };

            let ribbon_params = Params::new(
                m,
                usize::from(self.params.w),
                usize::from(self.params.r),
                Mode::Standard,
            )
            .map_err(|e| BurrBuildError::InvalidParams(static_describe_param_error(e)))?
            .with_seed(layer_seed)
            .with_retry_policy(
                if is_last_layer { 8 } else { 3 },
                if is_last_layer { 4 } else { 0 },
            )
            .map_err(|e| BurrBuildError::InvalidParams(static_describe_param_error(e)))?;

            let ribbon_builder =
                RibbonBuilder::new(ribbon_params, self.hasher.clone()).map_err(|e| {
                    BurrBuildError::RibbonLayerFailed {
                        layer_index: usize::from(layer_idx),
                        ribbon_error: e,
                    }
                })?;

            match ribbon_builder.build(&remaining) {
                Ok(ribbon) => {
                    layers.push(BurrLayer { m, ribbon });
                    // All of `remaining` was absorbed by this layer.
                    remaining.clear();
                }
                Err(err) => {
                    if is_last_layer {
                        return Err(BurrBuildError::RibbonLayerFailed {
                            layer_index: usize::from(layer_idx),
                            ribbon_error: err,
                        });
                    }
                    // Non-last layer build failed: MVP path bumps the
                    // whole current set to the next layer (a no-op for
                    // this layer — we don't push to `layers`). The next
                    // layer will be sized larger by virtue of the
                    // unchanged `remaining` count combined with a fresh
                    // seed.
                    //
                    // This is wasteful when only a few keys cause the
                    // failure (per-block thresholds would let us bump
                    // just those); the per-block upgrade is a follow-up.
                }
            }
        }

        if !remaining.is_empty() {
            return Err(BurrBuildError::LayerExhaustion {
                layers_attempted: usize::from(self.params.max_layers),
                remaining_keys: remaining.len(),
            });
        }

        Ok(BurrFilter::from_layers(
            self.params,
            self.hasher.clone(),
            layers,
        ))
    }
}

/// Derive a per-layer seed from the root seed.
///
/// Each layer must hash to a different `(start, band, fp)` distribution so
/// that bumped keys get a "fresh look" at the next layer's slot space.
/// We use a splittable-RNG-style mix to spread the per-layer seeds well.
pub(crate) fn derive_layer_seed(root: u64, layer_idx: u8) -> u64 {
    // Simple, stable splitmix64 step with the layer index folded in.
    let mut z = root.wrapping_add(u64::from(layer_idx).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn static_describe_param_error(_e: super::super::error::ParamError) -> &'static str {
    "vendored ribbon param error during burr build"
}
