use std::hash::{BuildHasher, Hash};

use super::super::builder::RibbonBuilder;
use super::super::hashing::{StandardEquation, standard_equation_w64};
use super::super::params::{Mode, Params};
use super::error::BurrBuildError;
use super::filter::{BurrFilter, BurrLayer};
use super::params::BurrParams;
use super::threshold::{compute_thresholds, partition_keys_by_threshold};

/// Builds a BuRR filter from a key set.
///
/// # Construction sketch
///
/// For each layer (0 to `max_layers - 1`):
///   1. Hash every input key with the layer's derived seed to produce a
///      `StandardEquation` (gives `start = block_idx * b + offset`).
///   2. Run [`compute_thresholds`] over those equations to pick per-block
///      threshold `τ_i`. A key with `offset < τ_i` is KEPT in this layer;
///      a key with `offset >= τ_i` is BUMPED to the next layer.
///   3. Partition keys into `kept` and `bumped` via
///      [`partition_keys_by_threshold`].
///   4. Build a vendored Standard Ribbon over `kept` — the threshold
///      scheme caps per-block load to ~90%, so this build succeeds with
///      negligible probability of falling into Ribbon's
///      retry-with-different-seed path.
///   5. Push `BurrLayer { thresholds, ribbon }` onto the layer stack.
///   6. Recurse with `remaining = bumped`.
///
/// The last layer cannot bump (there is no next layer), so it forces
/// `thresholds[..] = b` (accept everything) and is sized with an enlarged
/// `m` and generous retry+grow budget so the Ribbon build is guaranteed
/// to absorb its residual.
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

    /// Build from pre-computed u64 key hashes (e.g. xxh3 outputs from
    /// `crate::hash::hash64`). Bypasses `BuildHasher::hash_one` — the
    /// `S` parameter is only used as the type slot for the eventual
    /// `BurrFilter<S>` return value (which carries it for API
    /// compatibility with the key-based `contains_in`); no key →
    /// hash work happens here.
    ///
    /// This is the entry point the LSM filter writer uses: it has
    /// already hashed every key with xxh3 for filter-block indexing
    /// and pipes those u64s directly into BuRR.
    pub fn build_from_hashes(&self, hashes: &[u64]) -> Result<BurrFilter<S>, BurrBuildError> {
        let mut remaining: Vec<u64> = hashes.to_vec();
        let mut layers: Vec<BurrLayer<S>> = Vec::with_capacity(usize::from(self.params.max_layers));

        for layer_idx in 0..self.params.max_layers {
            if remaining.is_empty() {
                break;
            }

            let is_last_layer = layer_idx + 1 == self.params.max_layers;
            let layer_seed = derive_layer_seed(self.params.seed, layer_idx);
            let layer_input = remaining.len();

            let m_target = self.params.layer_m(layer_input);
            let m = if is_last_layer {
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

            let (kept, bumped) =
                partition_keys_by_threshold(&remaining, &equations, &thresholds, self.params.b);

            let ribbon_builder =
                RibbonBuilder::new(equation_params, self.hasher.clone()).map_err(|e| {
                    BurrBuildError::RibbonLayerFailed {
                        layer_index: usize::from(layer_idx),
                        ribbon_error: e,
                    }
                })?;

            let ribbon = ribbon_builder
                .build_with_seed_verbatim_from_hashes(&kept, layer_seed, m)
                .map_err(|e| BurrBuildError::RibbonLayerFailed {
                    layer_index: usize::from(layer_idx),
                    ribbon_error: e,
                })?;

            layers.push(BurrLayer {
                m,
                seed: layer_seed,
                thresholds,
                ribbon,
            });

            remaining = bumped;
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

            // Last layer: enlarge m to guarantee success even at full load
            // (no next layer to absorb spillover).
            let m_target = self.params.layer_m(layer_input);
            let m = if is_last_layer {
                let doubled = m_target.saturating_mul(2);
                doubled.max(usize::from(self.params.b) * 4)
            } else {
                m_target
            };

            // Build a Params instance reflecting THIS layer's slot count,
            // seed, and (later) retry budget — used both for equation
            // computation and for the inner RibbonBuilder.
            let layer_w = usize::from(self.params.w);
            let layer_r = usize::from(self.params.r);
            let equation_params = Params::new(m, layer_w, layer_r, Mode::Standard)
                .map_err(static_param_err)?
                .with_seed(layer_seed);

            // (1) Equations for every key in `remaining` under this
            // layer's seed/m/w/r.
            let equations =
                compute_layer_equations(&self.hasher, &remaining, &equation_params, layer_r);

            // (2) Decide per-block thresholds. Last layer uses
            // all-accepting thresholds: `b` everywhere.
            let thresholds = if is_last_layer {
                let block_count = m.div_ceil(usize::from(self.params.b));
                vec![self.params.b; block_count]
            } else {
                compute_thresholds(&equations, m, self.params.b)
            };

            // (3) Partition into kept / bumped.
            let (kept, bumped) =
                partition_keys_by_threshold(&remaining, &equations, &thresholds, self.params.b);

            // (4) Build Ribbon for kept. Use `build_with_seed_verbatim`
            // so the construction seed matches `layer_seed` exactly —
            // otherwise the vendored `RibbonBuilder.build` would mix it
            // through `derive_attempt_seed`, which would make the
            // ribbon's internal `start` values disagree with the start
            // values we used for threshold decisions (= correctness bug
            // surfaced as wire-format probe misses).
            //
            // No retry budget: the threshold scheme caps per-block load
            // at ~90%, so single-attempt construction succeeds in
            // practice. If it doesn't (parameter mistuning), the
            // resulting `RibbonLayerFailed` is the diagnostic — we
            // don't silently retry with a different seed because that
            // would invalidate the thresholds we just computed.
            let ribbon_builder =
                RibbonBuilder::new(equation_params, self.hasher.clone()).map_err(|e| {
                    BurrBuildError::RibbonLayerFailed {
                        layer_index: usize::from(layer_idx),
                        ribbon_error: e,
                    }
                })?;

            let ribbon = ribbon_builder
                .build_with_seed_verbatim(&kept, layer_seed, m)
                .map_err(|e| BurrBuildError::RibbonLayerFailed {
                    layer_index: usize::from(layer_idx),
                    ribbon_error: e,
                })?;

            layers.push(BurrLayer {
                m,
                seed: layer_seed,
                thresholds,
                ribbon,
            });

            // (5) Recurse with bumped keys.
            remaining = bumped;
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

/// Compute the equation each key would generate under the given params.
///
/// The fingerprint side-output is discarded — we only need `start` for
/// the threshold decision. The ribbon build that follows will recompute
/// equations (incl. fingerprints) using the same hasher + seed, so the
/// `start` values agree by construction.
fn compute_layer_equations<K, S>(
    hasher: &S,
    keys: &[K],
    params: &Params,
    r: usize,
) -> Vec<StandardEquation>
where
    K: Hash,
    S: BuildHasher,
{
    let stride = r.div_ceil(64);
    let mut fp_throwaway = vec![0_u64; stride];
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        fp_throwaway.fill(0);
        let eq = standard_equation_w64(hasher, key, params.seed, params, &mut fp_throwaway);
        out.push(eq);
    }
    out
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
