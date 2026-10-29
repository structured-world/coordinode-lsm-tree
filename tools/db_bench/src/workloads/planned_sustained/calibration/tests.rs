use super::{
    BOUND, Calibrated, CallLog, Fitted, MIN_MERGES, MIN_QUERIES, MIN_STEPS, Observed, QuerySpan,
    StepSample, Timing, WEIGHT, classify, fit, held_out_error,
};
use lsm_tree::compaction::planned::Model;
use std::time::{Duration, Instant};

fn timing(nanos_each: u64, count: u64) -> Timing {
    Timing {
        nanos: nanos_each * count,
        count,
    }
}

/// Steps whose scans cost `fixed + per_run · (main + 2)` and whose point reads
/// cost `point`, the main level going from 0 to `steps - 1` runs.
fn steps(count: usize, fixed: f64, per_run: f64, point: f64) -> Vec<StepSample> {
    (0..count)
        .map(|at| {
            let main_runs = u32::try_from(at).expect("few steps");
            StepSample {
                main_runs,
                scan_nanos: fixed + per_run * (f64::from(main_runs) + 2.0),
                point_nanos: point,
            }
        })
        .collect()
}

/// Nothing observed: nothing is fitted, and a model updated by it stays the
/// prior, its scan share the counted one. Guards against a fresh tree's plan
/// moving before anything was measured.
#[test]
fn nothing_observed_leaves_the_prior() {
    let fitted = fit(&Observed::default());
    assert_eq!(fitted, Fitted::default());
    let prior = Model::default();
    let mut calibrated = Calibrated::new(prior);
    calibrated.update(&fitted);
    assert_eq!(calibrated.model, prior);
    for counted in [0.0, 0.3, 0.5, 1.0] {
        assert!(
            (calibrated.scan_share(counted) - counted).abs() < 1e-12,
            "{counted}"
        );
    }
}

/// Below each threshold nothing of that kind is fitted, however clear the
/// samples: a few merges or queries are noise, not a machine's rate.
#[test]
fn too_few_samples_are_not_fitted() {
    let observed = Observed {
        merge_bytes: 1 << 30,
        merge_nanos: 1_000_000_000,
        merges: MIN_MERGES - 1,
        idle: timing(100, MIN_QUERIES),
        during: timing(200, MIN_QUERIES - 1),
        steps: steps(MIN_STEPS - 1, 0.0, 500.0, 1_000.0),
        filter_queries: MIN_QUERIES - 1,
        filter_skips: 0,
    };
    assert_eq!(fit(&observed), Fitted::default());
}

/// With enough of each, the fit reads the merges' rate, the share queries kept
/// during a merge, and a scan's cost per run over a point read's.
#[test]
fn enough_samples_fit_each_value() {
    let observed = Observed {
        merge_bytes: 512 << 20,
        merge_nanos: 4_000_000_000,
        merges: MIN_MERGES,
        idle: timing(100, MIN_QUERIES),
        during: timing(400, MIN_QUERIES),
        steps: steps(MIN_STEPS, 3_000.0, 500.0, 1_000.0),
        filter_queries: MIN_QUERIES * 10,
        filter_skips: MIN_QUERIES * 10 - MIN_QUERIES / 10,
    };
    let fitted = fit(&observed);
    assert_eq!(fitted.encode_rate, Some(128 << 20));
    assert_eq!(fitted.merge_query_share, Some(0.25));
    assert_eq!(fitted.false_positive_rate, Some(0.01));
    let weight = fitted.scan_weight.expect("enough steps");
    assert!((weight - 0.5).abs() < 1e-9, "{weight}");
}

/// Queries faster during a merge than without one are noise: the share is
/// capped at one, not taken as a merge speeding reads up.
#[test]
fn a_share_above_one_is_capped() {
    let observed = Observed {
        idle: timing(400, MIN_QUERIES),
        during: timing(100, MIN_QUERIES),
        ..Observed::default()
    };
    assert_eq!(fit(&observed).merge_query_share, Some(1.0));
}

/// Runs that never changed, or scans that got cheaper with more runs, say
/// nothing of what a run costs: no weight is fitted from them.
#[test]
fn flat_or_falling_scans_fit_no_weight() {
    let flat: Vec<StepSample> = (0..MIN_STEPS)
        .map(|_| StepSample {
            main_runs: 3,
            scan_nanos: 5_000.0,
            point_nanos: 1_000.0,
        })
        .collect();
    let falling = steps(MIN_STEPS, 10_000.0, -100.0, 1_000.0);
    for samples in [flat, falling] {
        let observed = Observed {
            steps: samples,
            ..Observed::default()
        };
        assert_eq!(fit(&observed).scan_weight, None);
    }
}

/// An update moves a value a `WEIGHT` share of the way toward what was fitted,
/// and never past `BOUND` times the prior, however far the fit lies.
#[test]
fn an_update_takes_a_bounded_step() {
    let prior = Model::default();
    let mut calibrated = Calibrated::new(prior);
    let target = prior.encode_rate / 2;
    calibrated.update(&Fitted {
        encode_rate: Some(target),
        ..Fitted::default()
    });
    let expected = prior.encode_rate as f64 + WEIGHT * (target as f64 - prior.encode_rate as f64);
    assert_eq!(calibrated.model.encode_rate, expected as u64);
    // Far below the bound: many updates stop at it.
    for _ in 0..100 {
        calibrated.update(&Fitted {
            encode_rate: Some(1),
            merge_query_share: Some(0.0),
            scan_weight: Some(1e9),
            false_positive_rate: Some(1.0),
        });
    }
    assert_eq!(
        calibrated.model.encode_rate,
        (prior.encode_rate as f64 / BOUND) as u64
    );
    assert!((calibrated.model.merge_query_share - prior.merge_query_share / BOUND).abs() < 1e-12);
    let prior_weight = 1.0 / (1.0 + prior.false_positive_rate);
    assert!((calibrated.scan_weight - prior_weight * BOUND).abs() < 1e-9);
    assert!(
        (calibrated.model.false_positive_rate - prior.false_positive_rate * BOUND).abs() < 1e-12
    );
    // Values the fit left out were not touched.
    assert_eq!(calibrated.model.copy_rate, prior.copy_rate);
    assert_eq!(calibrated.model.write_rate, prior.write_rate);
}

/// The filters' rate is the share of queries for an absent key they let
/// through, read from the engine's counts once there are enough of them.
/// Guards against counting the turned-away queries as the mistakes.
#[test]
fn the_false_positive_rate_is_what_the_filters_let_through() {
    let observed = Observed {
        filter_queries: 1_000,
        filter_skips: 980,
        ..Observed::default()
    };
    let rate = fit(&observed).false_positive_rate.expect("enough queries");
    assert!((rate - 0.02).abs() < 1e-12, "{rate}");
    let few = Observed {
        filter_queries: MIN_QUERIES - 1,
        filter_skips: 0,
        ..Observed::default()
    };
    assert_eq!(fit(&few).false_positive_rate, None);
}

/// A scan dearer per run than the model assumes raises the share planned
/// for, a cheaper one lowers it; all points or all scans stay as counted.
#[test]
fn the_scan_share_follows_the_weight() {
    let prior = Model::default();
    let mut heavier = Calibrated::new(prior);
    heavier.scan_weight *= 4.0;
    let mut lighter = Calibrated::new(prior);
    lighter.scan_weight /= 4.0;
    assert!(heavier.scan_share(0.5) > 0.5);
    assert!(lighter.scan_share(0.5) < 0.5);
    for calibrated in [heavier, lighter] {
        assert_eq!(calibrated.scan_share(0.0), 0.0);
        assert_eq!(calibrated.scan_share(1.0), 1.0);
    }
}

/// Predictions proportional to the observations err by nothing once scaled;
/// a scale fitted on the first half carries over to the held-out second.
#[test]
fn proportional_predictions_hold_out_exactly() {
    let pairs: Vec<(f64, f64)> = (1..=10)
        .map(|x| (f64::from(x), 3.0 * f64::from(x)))
        .collect();
    let error = held_out_error(&pairs).expect("enough pairs");
    assert!(error < 1e-12, "{error}");
}

/// A prediction that ignores what changes errs on the held-out half: the
/// observations double there, the prediction does not.
#[test]
fn a_flat_prediction_errs_on_a_change() {
    let pairs: Vec<(f64, f64)> = (0..10)
        .map(|at| (1.0, if at < 5 { 10.0 } else { 20.0 }))
        .collect();
    let error = held_out_error(&pairs).expect("enough pairs");
    assert!((error - 0.5).abs() < 1e-12, "{error}");
}

/// The model's cost in point reads is the observed cost of a query at every
/// weight and run count: `(e + 2)·c·w + (1 − c)`, for scans of any share.
#[test]
fn a_unit_of_model_cost_is_the_observed_cost_in_point_reads() {
    let prior = Model::default();
    let f = prior.false_positive_rate;
    for weight in [0.25, 1.0, 4.0] {
        let mut calibrated = Calibrated::new(prior);
        calibrated.scan_weight = weight;
        for counted in [0.0, 0.1, 0.5, 0.9, 1.0] {
            let r = calibrated.scan_share(counted);
            let unit = calibrated.point_reads_per_unit(counted);
            for runs in 0..8_u32 {
                let e = f64::from(runs) + 2.0;
                let model = e * r + (1.0 - r) * (1.0 + f);
                let observed = e * counted * weight + (1.0 - counted);
                assert!(
                    (unit * model - observed).abs() < 1e-9,
                    "weight {weight} counted {counted} runs {runs}: {} vs {observed}",
                    unit * model
                );
            }
        }
    }
}

/// `count` instants a millisecond apart, the first now.
fn instants(count: u32) -> Vec<Instant> {
    let base = Instant::now();
    (0..count)
        .map(|at| base + Duration::from_millis(u64::from(at)))
        .collect()
}

/// A query beside a merge counts as one during a merge; a query beside a
/// call that merged nothing (it chose nothing, or moved tables) counts as
/// idle. Guards against taking every compaction call for a merge, which
/// fits the share queries keep beside a merge from calls that are none.
#[test]
fn only_a_merge_makes_a_query_one_beside_a_merge() {
    let t = instants(8);
    let mut log = CallLog::new(2);
    log.merges.push((t[1], t[3]));
    let mut observed = Observed::default();
    let beside = QuerySpan {
        start: t[2],
        end: t[4],
        nanos: 10,
    };
    let after = QuerySpan {
        start: t[5],
        end: t[6],
        nanos: 20,
    };
    let waiting = classify(vec![beside, after], &mut log, &mut observed, t[7]);
    assert!(waiting.is_empty());
    assert_eq!(observed.during.count, 1);
    assert_eq!(observed.during.nanos, 10);
    assert_eq!(observed.idle.count, 1);
    assert_eq!(observed.idle.nanos, 20);
}

/// A query overlapped by a call still in progress waits: the call may yet be
/// a merge. Once it ends without one the query counts as idle, and a merge
/// no query can reach any more is forgotten.
#[test]
fn a_query_beside_a_call_in_progress_waits_for_it() {
    let t = instants(8);
    let mut log = CallLog::new(2);
    log.merges.push((t[0], t[1]));
    log.running[1] = Some(t[2]);
    let mut observed = Observed::default();
    let query = QuerySpan {
        start: t[3],
        end: t[4],
        nanos: 30,
    };
    let waiting = classify(vec![query], &mut log, &mut observed, t[5]);
    assert_eq!(waiting.len(), 1);
    assert_eq!(observed.during.count + observed.idle.count, 0);
    log.running[1] = None;
    let waiting = classify(waiting, &mut log, &mut observed, t[6]);
    assert!(waiting.is_empty());
    assert_eq!(observed.idle.count, 1);
    assert!(log.merges.is_empty(), "{:?}", log.merges);
}

/// Too few steps, or nothing to scale, give no error rather than a made-up
/// one.
#[test]
fn no_error_without_enough_to_judge() {
    assert_eq!(held_out_error(&[]), None);
    assert_eq!(held_out_error(&[(1.0, 1.0)]), None);
    assert_eq!(held_out_error(&[(0.0, 1.0), (0.0, 2.0)]), None);
}
