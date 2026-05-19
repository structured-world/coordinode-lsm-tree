use super::error::BurrBuildError;

/// Configuration for a BuRR filter.
///
/// Construction strategy:
///   * `n` keys are expected;
///   * each layer i has `m_i ≈ n_i * (1 + per_layer_overhead)` slots, where
///     `n_i` is the bumped-from-previous-layer key count (n_0 = n);
///   * blocks of size `b` within each layer drive the threshold scheme;
///   * up to `max_layers` are built — the last layer uses overhead high
///     enough to absorb its residual at threshold = b (no bumping).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BurrParams {
    /// Total expected key count (= layer 0 input size).
    pub n: usize,
    /// Fingerprint width in bits. FPR ≈ 2⁻ʳ. Must be in `1..=64` so that
    /// the fingerprint fits in a single `u64` lane (the vendored Ribbon
    /// `w=64` band assumes single-word `b` vectors).
    pub r: u8,
    /// Band width — fixed at 64 to match the vendored Ribbon
    /// `standard_equation_w64` solver.
    pub w: u8,
    /// Block size (rows per block, drives the per-block threshold byte).
    /// Default 64; must be ≤ 255 so the threshold fits one byte.
    pub b: u8,
    /// Maximum layer count. Last layer is sized for guaranteed success
    /// (no bumping); typical values 3–4.
    pub max_layers: u8,
    /// Per-layer construction overhead expressed as a fractional
    /// multiplier added to the key count: `m_i = ceil(n_i * (1 +
    /// per_layer_overhead))`. Higher overhead → fewer keys bumped → fewer
    /// layers needed but more memory.
    pub per_layer_overhead: f32,
    /// Root hash seed (combined with per-layer offsets to derive each
    /// layer's seed). Stored in the wire format header so probe-side
    /// re-derives the same seeds.
    pub seed: u64,
}

impl BurrParams {
    /// Default block size — chosen to match the band width so each block
    /// covers exactly one full band span; matches the BuRR paper's
    /// `b = w` recommendation for the homogeneous-threshold variant.
    pub const DEFAULT_B: u8 = 64;

    /// Default max layer count. 4 is enough for arbitrarily large n: each
    /// layer absorbs ~95% of incoming keys, so 4 layers reach ≈ 0.05⁴ ≈
    /// 6 × 10⁻⁶ of n. The last layer is sized for guaranteed success.
    pub const DEFAULT_MAX_LAYERS: u8 = 4;

    /// Per-layer overhead. With `b = 64`, overhead ≈ 5% leaves margin for
    /// the threshold scheme without overshooting the ~1% target overhead
    /// vs the information-theoretic minimum.
    pub const DEFAULT_PER_LAYER_OVERHEAD: f32 = 0.05;

    /// Construct params for `n` keys at a given false-positive rate.
    pub fn with_fp_rate(n: usize, fpr: f32) -> Result<Self, BurrBuildError> {
        if n == 0 {
            return Err(BurrBuildError::InvalidParams("n must be > 0"));
        }
        if !(0.0 < fpr && fpr < 1.0) {
            return Err(BurrBuildError::InvalidParams("fpr must be in (0.0, 1.0)"));
        }
        let r_f = (-fpr.log2()).ceil();
        if !r_f.is_finite() || r_f < 1.0 || r_f > 64.0 {
            return Err(BurrBuildError::InvalidParams(
                "computed r out of supported range [1, 64]",
            ));
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let r = r_f as u8;
        Ok(Self {
            n,
            r,
            w: 64,
            b: Self::DEFAULT_B,
            max_layers: Self::DEFAULT_MAX_LAYERS,
            per_layer_overhead: Self::DEFAULT_PER_LAYER_OVERHEAD,
            seed: 0,
        })
    }

    /// Construct params for `n` keys at a given bits-per-key target.
    /// Maps `bpk` directly to fingerprint width `r` since BuRR's effective
    /// storage is essentially `r` bits per key plus ~1% threshold metadata.
    pub fn with_bpk(n: usize, bpk: f32) -> Result<Self, BurrBuildError> {
        if n == 0 {
            return Err(BurrBuildError::InvalidParams("n must be > 0"));
        }
        if !(1.0..=64.0).contains(&bpk) {
            return Err(BurrBuildError::InvalidParams("bpk must be in [1.0, 64.0]"));
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let r = bpk.round().clamp(1.0, 64.0) as u8;
        Ok(Self {
            n,
            r,
            w: 64,
            b: Self::DEFAULT_B,
            max_layers: Self::DEFAULT_MAX_LAYERS,
            per_layer_overhead: Self::DEFAULT_PER_LAYER_OVERHEAD,
            seed: 0,
        })
    }

    /// Override the construction seed (deterministic builds).
    #[must_use]
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Compute slot count `m` for a layer receiving `layer_input_keys`.
    ///
    /// For non-final layers: `m = ceil(input * (1 + overhead))` rounded up
    /// to a multiple of `b`. For the final layer, the caller is expected
    /// to bump the overhead so that no keys spill over (handled by the
    /// builder, not by this helper).
    #[must_use]
    pub fn layer_m(&self, layer_input_keys: usize) -> usize {
        let overhead = f64::from(self.per_layer_overhead);
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let raw = ((layer_input_keys as f64) * (1.0 + overhead)).ceil() as usize;
        let raw = raw.max(usize::from(self.b)); // ≥ one block
        // Round UP to a multiple of b (so block_count = m / b is exact).
        let b = usize::from(self.b);
        raw.div_ceil(b) * b
    }
}
