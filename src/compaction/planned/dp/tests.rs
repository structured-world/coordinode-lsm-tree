// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::*;

fn inputs(scan_share: f64, merge_secs: f64) -> Inputs {
    Inputs {
        round: 16,
        size_ratio: 3,
        arrival_secs: 10.0,
        merge_secs,
        merge_query_share: 0.5,
        scan_share,
        false_positive_rate: 0.01,
    }
}

/// The units merged after each of the round's arrivals.
fn merges(plan: &Plan) -> Vec<u32> {
    (1..=16).map(|t| plan.merge_after(t)).collect()
}

/// Without scans the number of runs costs a query nothing: no merge pays,
/// and none is planned.
#[test]
fn without_scans_nothing_is_merged() {
    let plan = Plan::solve(&inputs(0.0, 0.1));
    assert!(merges(&plan).iter().all(|&units| units == 1), "{plan:?}");
}

/// Cheap merges and scans that pay for every run make the plan merge, and
/// merge early in the round, where a merge serves the most queries.
#[test]
fn cheap_merges_under_scans_are_planned_early() {
    let plan = Plan::solve(&inputs(0.5, 0.1));
    let merges = merges(&plan);
    let first = merges
        .iter()
        .position(|&units| units > 1)
        .expect("a merge is planned");
    assert!(first < 8, "the first merge comes early: {merges:?}");
}

/// A merge that takes longer than the arrivals it would serve costs more
/// queries than it frees, and is not planned.
#[test]
fn merges_dearer_than_their_benefit_are_not_planned() {
    let plan = Plan::solve(&inputs(0.5, 1_000.0));
    assert!(merges(&plan).iter().all(|&units| units == 1), "{plan:?}");
}

/// The same inputs give the same plan, solved apart.
#[test]
fn the_plan_is_the_same_for_the_same_inputs() {
    let a = Plan::solve(&inputs(0.3, 1.0));
    let b = Plan::solve(&inputs(0.3, 1.0));
    assert_eq!(a, b);
}

/// Past the round, nothing is merged.
#[test]
fn nothing_is_merged_past_the_round() {
    let plan = Plan::solve(&inputs(0.5, 0.1));
    assert_eq!(plan.merge_after(17), 1);
    assert_eq!(plan.merge_after(u32::MAX), 1);
}
