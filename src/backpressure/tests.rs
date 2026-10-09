use super::*;

fn thresholds() -> BackpressureThresholds {
    BackpressureThresholds {
        l0_slowdown: Some(8),
        l0_stop: Some(16),
        bytes_slowdown: Some(1_000),
        bytes_stop: Some(4_000),
        max_slowdown: Some(Duration::from_millis(10)),
        ..BackpressureThresholds::OFF
    }
}

/// The verdict for an L0 run count and pending compaction bytes, the memtable
/// axis at zero.
fn compute(l0_runs: usize, pending: u64, thresholds: &BackpressureThresholds) -> Backpressure {
    Backpressure::compute(
        &BackpressureSignals {
            l0_runs,
            pending_compaction_bytes: pending,
            unflushed_memtable_bytes: 0,
        },
        thresholds,
    )
}

/// The verdict for unflushed memtable bytes alone.
fn compute_memtable(unflushed: u64, thresholds: &BackpressureThresholds) -> Backpressure {
    Backpressure::compute(
        &BackpressureSignals {
            unflushed_memtable_bytes: unflushed,
            ..BackpressureSignals::default()
        },
        thresholds,
    )
}

fn memtable_thresholds() -> BackpressureThresholds {
    BackpressureThresholds {
        memtable_slowdown: Some(64 << 20),
        memtable_stop: Some(256 << 20),
        max_slowdown: Some(Duration::from_millis(10)),
        ..BackpressureThresholds::OFF
    }
}

#[test]
fn off_thresholds_are_always_none() {
    let off = BackpressureThresholds::default();
    assert!(off.is_off());
    // Even at absurd signal levels, an unconfigured policy never throttles.
    assert_eq!(
        Backpressure::compute(
            &BackpressureSignals {
                l0_runs: 1_000_000,
                pending_compaction_bytes: u64::MAX,
                unflushed_memtable_bytes: u64::MAX,
            },
            &off,
        ),
        Backpressure::None
    );
}

#[test]
fn below_all_thresholds_is_none() {
    assert_eq!(compute(0, 0, &thresholds()), Backpressure::None);
    assert_eq!(compute(7, 999, &thresholds()), Backpressure::None);
}

#[test]
fn l0_count_at_slowdown_threshold_yields_zero_delay_slowdown() {
    // Exactly at the slowdown trigger: throttled, but the ramp delay is zero
    // (no overage yet), so there is no cliff entering the tier.
    let v = compute(8, 0, &thresholds());
    assert_eq!(
        v,
        Backpressure::Slowdown {
            suggested_delay: Duration::ZERO
        }
    );
}

#[test]
fn l0_count_at_stop_threshold_yields_stop() {
    assert_eq!(compute(16, 0, &thresholds()), Backpressure::Stop);
    assert_eq!(compute(100, 0, &thresholds()), Backpressure::Stop);
}

#[test]
fn bytes_axis_drives_the_verdict_independently() {
    // L0 healthy, but pending bytes past slowdown -> Slowdown.
    assert!(matches!(
        compute(0, 2_000, &thresholds()),
        Backpressure::Slowdown { .. }
    ));
    // Pending bytes at stop -> Stop regardless of L0.
    assert_eq!(compute(0, 4_000, &thresholds()), Backpressure::Stop);
}

#[test]
fn stop_dominates_slowdown_across_axes() {
    // L0 only at slowdown, bytes at stop -> Stop wins.
    assert_eq!(compute(8, 4_000, &thresholds()), Backpressure::Stop);
}

#[test]
fn slowdown_delay_grows_monotonically_with_overage() {
    let t = thresholds();
    // Walk L0 from soft (8) toward hard (16); the suggested delay must be
    // non-decreasing and stay below the cap until stop.
    let mut last = Duration::ZERO;
    for count in 8..16 {
        let Backpressure::Slowdown { suggested_delay } = compute(count, 0, &t) else {
            panic!("expected Slowdown at L0={count}");
        };
        assert!(
            suggested_delay >= last,
            "delay must not decrease (L0={count})"
        );
        assert!(
            suggested_delay < Duration::from_millis(10),
            "below cap until stop"
        );
        last = suggested_delay;
    }
}

#[test]
fn slowdown_delay_is_max_of_both_axes() {
    // L0 just past soft (small ramp) but bytes near stop (large ramp): the
    // larger ramp wins.
    let t = thresholds();
    let Backpressure::Slowdown { suggested_delay } = compute(9, 3_900, &t) else {
        panic!("expected Slowdown");
    };
    // bytes ramp at 3900/4000 of the interval [1000,4000] = ~9.67ms, well above
    // the L0 ramp at 1/8 of [8,16] = ~1.25ms.
    assert!(suggested_delay > Duration::from_millis(8));
}

#[test]
fn slowdown_without_stop_threshold_sits_at_cap() {
    // Only a slowdown trigger configured (no stop): once past it, the delay is
    // the cap (no interval to ramp over), and it never escalates to Stop.
    let t = BackpressureThresholds {
        l0_slowdown: Some(4),
        max_slowdown: Some(Duration::from_millis(5)),
        ..BackpressureThresholds::OFF
    };
    assert_eq!(
        compute(100, 0, &t),
        Backpressure::Slowdown {
            suggested_delay: Duration::from_millis(5)
        }
    );
}

#[test]
fn stop_without_slowdown_threshold_jumps_straight_to_stop() {
    let t = BackpressureThresholds {
        l0_stop: Some(10),
        ..BackpressureThresholds::OFF
    };
    assert_eq!(compute(9, 0, &t), Backpressure::None);
    assert_eq!(compute(10, 0, &t), Backpressure::Stop);
}

/// The memtable axis has the tiers of the other two: none below its slowdown
/// threshold, a slowdown from it, stop from its stop threshold.
#[test]
fn memtable_axis_drives_none_slowdown_then_stop() {
    let t = memtable_thresholds();
    assert!(
        !t.is_off(),
        "a memtable threshold alone turns the policy on"
    );
    assert_eq!(compute_memtable((64 << 20) - 1, &t), Backpressure::None);
    assert_eq!(
        compute_memtable(64 << 20, &t),
        Backpressure::Slowdown {
            suggested_delay: Duration::ZERO
        }
    );
    let Backpressure::Slowdown { suggested_delay } = compute_memtable(160 << 20, &t) else {
        panic!("halfway between the thresholds is a slowdown");
    };
    assert_eq!(suggested_delay, Duration::from_millis(5), "half the ramp");
    assert_eq!(compute_memtable(256 << 20, &t), Backpressure::Stop);
}

/// A memtable stop wins over the other axes' slowdown, and a memtable
/// slowdown does not hide another axis's stop.
#[test]
fn memtable_axis_combines_by_the_most_severe_tier() {
    let t = BackpressureThresholds {
        memtable_slowdown: Some(64 << 20),
        memtable_stop: Some(256 << 20),
        ..thresholds()
    };
    let at = |l0_runs, unflushed| {
        Backpressure::compute(
            &BackpressureSignals {
                l0_runs,
                pending_compaction_bytes: 0,
                unflushed_memtable_bytes: unflushed,
            },
            &t,
        )
    };
    assert_eq!(at(8, 256 << 20), Backpressure::Stop);
    assert_eq!(at(16, 64 << 20), Backpressure::Stop);
    assert!(matches!(at(0, 64 << 20), Backpressure::Slowdown { .. }));
}

#[test]
fn scale_ramp_stays_proportional_for_huge_cap_and_span() {
    // Regression: `scale` must apply the fraction without saturating the
    // numerator first. With a near-`Duration::MAX` cap and a span wide enough
    // that `cap_nanos * num` overflows `u128`, saturating the product first
    // collapses the ramp far below its intended proportion. A half ramp must
    // stay ~half the cap (and never exceed it).
    let cap = Duration::MAX;
    let den = u64::MAX;
    let num = den / 2;

    let got = scale(cap, num, den);

    assert!(got <= cap, "a ramped delay never exceeds the cap");
    let half_secs = cap.as_secs() / 2;
    assert!(
        got.as_secs() >= half_secs - 1,
        "a half ramp stays proportional (got {}s, expected ~{}s)",
        got.as_secs(),
        half_secs,
    );
}

#[test]
fn is_throttled_reflects_tier() {
    assert!(!Backpressure::None.is_throttled());
    assert!(
        Backpressure::Slowdown {
            suggested_delay: Duration::ZERO
        }
        .is_throttled()
    );
    assert!(Backpressure::Stop.is_throttled());
}
