//! Paired measurement of the engines of one head-to-head group.
//!
//! Every arm of a group is timed in the same rounds: one sample per arm per
//! round, with the order of the arms rotating from round to round. A stretch
//! of load on the host then lands on every engine of the rounds it overlaps,
//! not on whichever arm happened to be running, and divides out of the ratio
//! of two arms measured in the same round. The group reports, per arm, the
//! median time per operation and the median of its per-round ratio to the
//! baseline arm, each with a distribution-free 95% confidence interval.

use std::time::Duration;

/// Times `iters` runs of an arm's workload and returns the time they took.
/// Untimed setup a run needs (a fresh directory, an open engine) happens
/// inside, outside the clock.
pub type Routine<'a> = Box<dyn FnMut(u64) -> Duration + 'a>;

/// One engine or configuration of a group.
pub struct Arm<'a> {
    pub label: &'static str,
    pub routine: Routine<'a>,
}

impl<'a> Arm<'a> {
    pub fn new(label: &'static str, routine: impl FnMut(u64) -> Duration + 'a) -> Self {
        Self {
            label,
            routine: Box::new(routine),
        }
    }
}

/// How long a group is measured.
#[derive(Clone, Copy, Debug)]
pub struct Settings {
    /// Time each arm runs untimed before the rounds start, and from which its
    /// iterations per sample are sized.
    pub warm_up: Duration,
    /// Shortest sample: an arm runs as many iterations per sample as fit.
    pub sample_target: Duration,
    /// Time the rounds of one group and key count aim to take.
    pub budget: Duration,
    /// Rounds run however long they take.
    pub min_rounds: usize,
    /// Rounds not exceeded, however cheap the arms, unless the floor rounded
    /// up to a multiple of the arm count passes it.
    pub max_rounds: usize,
}

/// A median with its 95% confidence interval.
#[derive(Clone, Copy, Debug)]
pub struct Estimate {
    pub median: f64,
    pub lower: f64,
    pub upper: f64,
}

/// What one arm of one group and key count measured.
#[derive(Debug)]
pub struct ArmResult {
    pub group: String,
    pub label: &'static str,
    pub n: u64,
    pub rounds: usize,
    /// Nanoseconds per operation (per key of the routine's key set).
    pub ns_per_op: Estimate,
    /// This arm's time over the baseline arm's in the same round; `None` for
    /// the baseline itself and for a group without one.
    pub vs_baseline: Option<Estimate>,
}

/// Runs the arms of `group` at key count `n` in paired rounds and returns one
/// result per arm, in the order given. `baseline` names the arm the ratios are
/// taken against.
///
/// # Panics
///
/// If `n` is zero or `arms` is empty.
pub fn run(
    group: &str,
    n: u64,
    baseline: &str,
    mut arms: Vec<Arm<'_>>,
    settings: &Settings,
) -> Vec<ArmResult> {
    assert!(n > 0, "{group}: a routine covers at least one operation");
    assert!(!arms.is_empty(), "{group}: a group has at least one arm");

    // Untimed warm-up, which also measures one iteration of each arm.
    let per_iter: Vec<Duration> = arms
        .iter_mut()
        .map(|arm| {
            let (mut spent, mut iters) = (Duration::ZERO, 0_u32);
            while iters == 0 || spent < settings.warm_up {
                spent += (arm.routine)(1);
                iters += 1;
            }
            spent / iters
        })
        .collect();

    // Iterations per sample: enough to fill the sample target, at least one.
    // An iteration takes at least a nanosecond, so a sample target of seconds
    // asks for well under `u32::MAX` of them.
    let iters: Vec<u32> = per_iter
        .iter()
        .map(|t| {
            let k = settings
                .sample_target
                .as_nanos()
                .div_ceil(t.as_nanos().max(1));
            u32::try_from(k)
                .expect("iterations per sample fit u32")
                .max(1)
        })
        .collect();
    // At least one nanosecond, by the same bound.
    let round_cost: u128 = per_iter
        .iter()
        .zip(&iters)
        .map(|(t, &k)| t.as_nanos().max(1) * u128::from(k))
        .sum();
    let fit = usize::try_from(settings.budget.as_nanos() / round_cost)
        .unwrap_or(usize::MAX)
        .clamp(settings.min_rounds, settings.max_rounds);
    // A multiple of the arm count, so the rotation gives every arm every
    // position equally often: up to the next multiple, or down to the one
    // below when the next would pass the ceiling and the one below still
    // reaches the floor.
    let k = arms.len();
    let up = fit.div_ceil(k) * k;
    let down = fit / k * k;
    let rounds = if up > settings.max_rounds && down >= settings.min_rounds {
        down
    } else {
        up
    };

    // samples[arm][round], nanoseconds per operation.
    let mut samples = vec![Vec::with_capacity(rounds); arms.len()];
    for round in 0..rounds {
        for i in 0..arms.len() {
            let a = (round + i) % arms.len();
            let elapsed = (arms[a].routine)(u64::from(iters[a]));
            samples[a].push(elapsed.as_nanos() as f64 / (f64::from(iters[a]) * n as f64));
        }
    }

    let base = arms.iter().position(|arm| arm.label == baseline);
    arms.iter()
        .enumerate()
        .map(|(a, arm)| {
            let vs_baseline = base.filter(|&b| b != a).map(|b| {
                let ratios: Vec<f64> = samples[a]
                    .iter()
                    .zip(&samples[b])
                    .map(|(x, y)| x / y)
                    .collect();
                estimate(ratios)
            });
            let result = ArmResult {
                group: group.to_owned(),
                label: arm.label,
                n,
                rounds,
                ns_per_op: estimate(samples[a].clone()),
                vs_baseline,
            };
            report(&result);
            result
        })
        .collect()
}

/// One line per arm on stderr, so the job log shows what the page will.
fn report(r: &ArmResult) {
    let ratio = r.vs_baseline.map_or(String::new(), |e| {
        format!(
            "  x{:.3} [{:.3} .. {:.3}] vs baseline",
            e.median, e.lower, e.upper
        )
    });
    eprintln!(
        "{}/{}/{}: {:.1} ns/op [{:.1} .. {:.1}], {} rounds{ratio}",
        r.group, r.label, r.n, r.ns_per_op.median, r.ns_per_op.lower, r.ns_per_op.upper, r.rounds,
    );
}

/// The median of `values` and a distribution-free 95% confidence interval for
/// it: the order statistics whose ranks a Binomial(m, 1/2) count puts the true
/// median between with at least 95% probability. With fewer than six values no
/// pair of ranks reaches 95%, and the interval is the whole range.
pub fn estimate(mut values: Vec<f64>) -> Estimate {
    values.sort_by(f64::total_cmp);
    let m = values.len();
    let median = if m % 2 == 1 {
        values[m / 2]
    } else {
        (values[m / 2 - 1] + values[m / 2]) / 2.0
    };
    // Largest rank l (1-based) with P(X <= l - 1) <= 2.5%, X ~ Binomial(m, 1/2):
    // the interval [x_(l), x_(m + 1 - l)] then covers the median with at least
    // 95% probability.
    let mut pmf = 0.5_f64.powi(i32::try_from(m).unwrap_or(i32::MAX));
    let mut cdf = 0.0;
    let mut l = 1;
    for k in 0..m {
        // cdf = P(X <= k); within the bound, rank k + 1 qualifies.
        cdf += pmf;
        if cdf > 0.025 {
            break;
        }
        l = k + 1;
        pmf *= (m - k) as f64 / (k + 1) as f64;
    }
    let l = l.min(m.div_ceil(2));
    Estimate {
        median,
        lower: values[l - 1],
        upper: values[m - l],
    }
}
