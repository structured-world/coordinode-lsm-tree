//! The paired runner of the head-to-head bench, on routines whose times are
//! known: what it reports must follow from them exactly.

#[path = "../benches/paired.rs"]
mod paired;

use paired::{Arm, Settings, estimate, run};
use std::time::Duration;

fn settings(min_rounds: usize, max_rounds: usize) -> Settings {
    Settings {
        warm_up: Duration::ZERO,
        sample_target: Duration::from_millis(1),
        budget: Duration::from_millis(100),
        min_rounds,
        max_rounds,
    }
}

/// A routine that reports `per_iter` for every iteration it is asked to run.
fn fixed(per_iter: Duration) -> impl FnMut(u64) -> Duration {
    move |iters| per_iter * u32::try_from(iters).expect("iters fit u32")
}

/// Ten values give the ranks 2 and 9 (coverage 97.9%); rank 3 and 8 would
/// cover only 89%, below the 95% the interval promises.
#[test]
fn estimate_ten_values_uses_ranks_two_and_nine() {
    let e = estimate((1..=10).map(f64::from).collect());
    assert_eq!(e.median, 5.5);
    assert_eq!(e.lower, 2.0);
    assert_eq!(e.upper, 9.0);
}

/// Below six values no pair of order statistics reaches 95%, so the interval
/// is the whole range rather than a narrower one that overstates certainty.
#[test]
fn estimate_five_values_spans_the_range() {
    let e = estimate(vec![5.0, 1.0, 3.0, 2.0, 4.0]);
    assert_eq!(e.median, 3.0);
    assert_eq!(e.lower, 1.0);
    assert_eq!(e.upper, 5.0);
}

/// Thirty values: ranks 10 and 21 (P(X <= 9) = 2.1%, P(X <= 10) = 4.9%).
#[test]
fn estimate_thirty_values_uses_ranks_ten_and_twenty_one() {
    let e = estimate((1..=30).map(f64::from).collect());
    assert_eq!(e.lower, 10.0);
    assert_eq!(e.upper, 21.0);
}

/// An arm twice as slow as the baseline reports a ratio of exactly two, the
/// baseline reports none, and the time per operation divides by the key count.
#[test]
fn run_reports_ratio_to_baseline_and_time_per_operation() {
    let arms = vec![
        Arm::new("ours", fixed(Duration::from_micros(200))),
        Arm::new("rocksdb", fixed(Duration::from_micros(100))),
    ];
    let results = run("g", 100, "rocksdb", arms, &settings(10, 10));
    assert_eq!(results.len(), 2);
    let ours = &results[0];
    assert_eq!(ours.label, "ours");
    assert_eq!(ours.rounds, 10);
    assert_eq!(ours.ns_per_op.median, 2_000.0);
    let ratio = ours.vs_baseline.expect("ours has a baseline");
    assert_eq!((ratio.median, ratio.lower, ratio.upper), (2.0, 2.0, 2.0));
    assert_eq!(results[1].ns_per_op.median, 1_000.0);
    assert!(results[1].vs_baseline.is_none());
}

/// The number of rounds follows the budget over one round's cost, held
/// between the minimum and the maximum: 100 ms over 2 ms a round is 50,
/// capped at 40; and at least the minimum when one round outlasts the budget.
#[test]
fn run_sizes_rounds_by_budget_within_bounds() {
    let cheap = vec![
        Arm::new("a", fixed(Duration::from_micros(10))),
        Arm::new("b", fixed(Duration::from_micros(10))),
    ];
    assert_eq!(run("g", 1, "b", cheap, &settings(10, 40))[0].rounds, 40);

    let costly = vec![Arm::new("a", fixed(Duration::from_millis(200)))];
    assert_eq!(run("g", 1, "a", costly, &settings(10, 40))[0].rounds, 10);
}

/// Every arm takes every position equally often: the rounds are a multiple of
/// the arm count. Eight arms at a floor of ten rounds would let two arms go
/// first twice and the other six once, and any effect of position (caches,
/// the cleanup of the engine before) would land on them unevenly; the floor
/// rounds up to sixteen. A ceiling that is not a multiple rounds down when
/// that stays at or above the floor: three arms under a ceiling of forty run
/// thirty-nine rounds.
#[test]
fn run_rounds_are_a_multiple_of_the_arm_count() {
    use std::cell::RefCell;
    let first = RefCell::new(Vec::new());
    let labels: [&'static str; 8] = ["a", "b", "c", "d", "e", "f", "g", "h"];
    let arms = labels
        .iter()
        .map(|&label| {
            let first = &first;
            Arm::new(label, move |iters| {
                first.borrow_mut().push(label);
                Duration::from_millis(200) * u32::try_from(iters).expect("iters fit u32")
            })
        })
        .collect();
    let results = run("g", 1, "a", arms, &settings(10, 40));
    assert_eq!(results[0].rounds, 16);
    // After one warm-up call per arm, every eighth call opens a round.
    let calls = first.into_inner();
    for label in labels {
        let firsts = calls[8..]
            .chunks(8)
            .filter(|round| round[0] == label)
            .count();
        assert_eq!(firsts, 2, "{label} went first {firsts} times");
    }

    // A second's budget over 3 ms a round is 333 rounds, held at the ceiling
    // of forty, which three arms cannot share evenly.
    let cheap = (0..3)
        .map(|_| Arm::new("x", fixed(Duration::from_micros(10))))
        .collect();
    let mut s = settings(10, 40);
    s.budget = Duration::from_secs(1);
    assert_eq!(run("g", 1, "x", cheap, &s)[0].rounds, 39);
}

/// The arms take turns at going first: over as many rounds as arms, each arm
/// runs first exactly once, so no arm always measures right after another.
#[test]
fn run_rotates_which_arm_goes_first() {
    use std::cell::RefCell;
    let order = RefCell::new(Vec::new());
    let arm = |label: &'static str| {
        let order = &order;
        Arm::new(label, move |iters| {
            order.borrow_mut().push(label);
            Duration::from_millis(1) * u32::try_from(iters).expect("iters fit u32")
        })
    };
    let arms = vec![arm("a"), arm("b"), arm("c")];
    let mut s = settings(3, 3);
    s.sample_target = Duration::from_nanos(1);
    run("g", 1, "a", arms, &s);
    // One warm-up call per arm, then three rounds of one sample each.
    let order = order.into_inner();
    let rounds: Vec<&[&str]> = order[3..].chunks(3).collect();
    assert_eq!(
        rounds,
        [&["a", "b", "c"][..], &["b", "c", "a"], &["c", "a", "b"]]
    );
}
