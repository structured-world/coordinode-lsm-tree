// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::*;

/// A figure of every kind, each distinct, so a test that swaps two of them
/// fails.
fn work(base: u64) -> Work<u64> {
    Work {
        moved: base,
        carried: base + 1,
        reprotected: base + 2,
        reencoded: base + 3,
        read: base + 4,
        written: base + 5,
    }
}

/// The ledger's measured work: re-encoded bytes are the written ones less
/// the carried ones, reads count from the pacer and from the installs alike,
/// and nothing is re-protected.
#[test]
fn ledger_report_takes_reencoded_as_written_less_carried() {
    let ledger = WorkLedger::default();
    ledger.moved(7);
    ledger.read(50);
    ledger.installed(Installed {
        written: 100,
        carried: 30,
        read: 5,
    });
    ledger.installed(Installed {
        written: 20,
        carried: 20,
        read: 0,
    });
    assert_eq!(
        ledger.report().measured,
        Work {
            moved: 7,
            carried: 50,
            reprotected: 0,
            reencoded: 70,
            read: 55,
            written: 120,
        },
    );
}

/// The ledger reports the estimate it was planned against, and a run planned
/// twice the last one.
#[test]
fn ledger_report_carries_the_last_planned_estimate() {
    let ledger = WorkLedger::default();
    assert_eq!(ledger.report().estimate, WorkEstimate::default());
    ledger.plan(WorkEstimate::exact(work(1)));
    ledger.plan(WorkEstimate::exact(work(10)));
    assert_eq!(ledger.report().estimate.expected, work(10));
}

/// The error of each kind is the measured figure less the expected one,
/// below zero where the estimate was too high.
#[test]
fn report_error_is_measured_less_expected_per_kind() {
    let report = WorkReport {
        estimate: WorkEstimate::exact(work(10)),
        measured: Work {
            moved: 10,
            carried: 0,
            reprotected: 12,
            reencoded: 100,
            read: u64::MAX,
            written: 15,
        },
    };
    assert_eq!(
        report.error(),
        Work {
            moved: 0,
            carried: -11,
            reprotected: 0,
            reencoded: 87,
            read: i128::from(u64::MAX) - 14,
            written: 0,
        },
    );
}

/// An exact estimate bounds every figure by itself: the expected work lies
/// within it, and one byte more of any kind lies outside.
#[test]
fn exact_estimate_bounds_each_figure_by_itself() {
    let estimate = WorkEstimate::exact(work(10));
    assert!(estimate.contains(&work(10)));
    assert!(estimate.contains(&Work::default()));
    let mut over = work(10);
    over.written += 1;
    assert!(!estimate.contains(&over));
    let mut over = work(10);
    over.moved += 1;
    assert!(!estimate.contains(&over));
}

/// A figure without a ceiling lies within the bounds at any size.
#[test]
fn estimate_without_a_ceiling_contains_any_figure() {
    let estimate = WorkEstimate {
        expected: work(10),
        ceiling: Work {
            reencoded: None,
            ..WorkEstimate::exact(work(10)).ceiling
        },
    };
    let mut measured = work(10);
    measured.reencoded = u64::MAX;
    assert!(estimate.contains(&measured));
    measured.carried += 1;
    assert!(!estimate.contains(&measured));
}
