// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::ProbeStats;

/// Each decay halves both counts, so activity two windows back weighs a
/// quarter and a quiet table's figures fall to zero.
#[test]
fn decay_halves_each_window() {
    let stats = ProbeStats::default();
    for _ in 0..8 {
        stats.probe();
    }
    for _ in 0..6 {
        stats.negative();
    }
    stats.decay();
    assert_eq!((stats.probes(), stats.negatives()), (4, 3));
    stats.decay();
    assert_eq!((stats.probes(), stats.negatives()), (2, 1));
    for _ in 0..4 {
        stats.decay();
    }
    assert_eq!((stats.probes(), stats.negatives()), (0, 0));
}
