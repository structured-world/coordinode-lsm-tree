use super::*;

fn thresholds() -> BackpressureThresholds {
    BackpressureThresholds {
        l0_slowdown: Some(8),
        l0_stop: Some(16),
        bytes_slowdown: Some(1_000),
        bytes_stop: Some(4_000),
        max_slowdown: Some(Duration::from_millis(10)),
    }
}

#[test]
fn off_thresholds_are_always_none() {
    let off = BackpressureThresholds::default();
    assert!(off.is_off());
    // Even at absurd signal levels, an unconfigured policy never throttles.
    assert_eq!(
        Backpressure::compute(1_000_000, u64::MAX, &off),
        Backpressure::None
    );
}

#[test]
fn below_all_thresholds_is_none() {
    assert_eq!(
        Backpressure::compute(0, 0, &thresholds()),
        Backpressure::None
    );
    assert_eq!(
        Backpressure::compute(7, 999, &thresholds()),
        Backpressure::None
    );
}

#[test]
fn l0_count_at_slowdown_threshold_yields_zero_delay_slowdown() {
    // Exactly at the slowdown trigger: throttled, but the ramp delay is zero
    // (no overage yet), so there is no cliff entering the tier.
    let v = Backpressure::compute(8, 0, &thresholds());
    assert_eq!(
        v,
        Backpressure::Slowdown {
            suggested_delay: Duration::ZERO
        }
    );
}

#[test]
fn l0_count_at_stop_threshold_yields_stop() {
    assert_eq!(
        Backpressure::compute(16, 0, &thresholds()),
        Backpressure::Stop
    );
    assert_eq!(
        Backpressure::compute(100, 0, &thresholds()),
        Backpressure::Stop
    );
}

#[test]
fn bytes_axis_drives_the_verdict_independently() {
    // L0 healthy, but pending bytes past slowdown -> Slowdown.
    assert!(matches!(
        Backpressure::compute(0, 2_000, &thresholds()),
        Backpressure::Slowdown { .. }
    ));
    // Pending bytes at stop -> Stop regardless of L0.
    assert_eq!(
        Backpressure::compute(0, 4_000, &thresholds()),
        Backpressure::Stop
    );
}

#[test]
fn stop_dominates_slowdown_across_axes() {
    // L0 only at slowdown, bytes at stop -> Stop wins.
    assert_eq!(
        Backpressure::compute(8, 4_000, &thresholds()),
        Backpressure::Stop
    );
}

#[test]
fn slowdown_delay_grows_monotonically_with_overage() {
    let t = thresholds();
    // Walk L0 from soft (8) toward hard (16); the suggested delay must be
    // non-decreasing and stay below the cap until stop.
    let mut last = Duration::ZERO;
    for count in 8..16 {
        let Backpressure::Slowdown { suggested_delay } = Backpressure::compute(count, 0, &t) else {
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
    let Backpressure::Slowdown { suggested_delay } = Backpressure::compute(9, 3_900, &t) else {
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
        l0_stop: None,
        bytes_slowdown: None,
        bytes_stop: None,
        max_slowdown: Some(Duration::from_millis(5)),
    };
    assert_eq!(
        Backpressure::compute(100, 0, &t),
        Backpressure::Slowdown {
            suggested_delay: Duration::from_millis(5)
        }
    );
}

#[test]
fn stop_without_slowdown_threshold_jumps_straight_to_stop() {
    let t = BackpressureThresholds {
        l0_slowdown: None,
        l0_stop: Some(10),
        bytes_slowdown: None,
        bytes_stop: None,
        max_slowdown: None,
    };
    assert_eq!(Backpressure::compute(9, 0, &t), Backpressure::None);
    assert_eq!(Backpressure::compute(10, 0, &t), Backpressure::Stop);
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
