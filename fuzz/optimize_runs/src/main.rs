#[macro_use]
extern crate afl;

use arbitrary::{Arbitrary, Unstructured};
use lsm_tree::{KeyRange, optimize_key_ranges};

/// Keys are single bytes `a..=z`, so ranges overlap often.
const ALPHABET: u8 = 26;

fn key_range(lo: u8, hi: u8) -> KeyRange {
    KeyRange::new((vec![b'a' + lo].into(), vec![b'a' + hi].into()))
}

fn main() {
    fuzz!(|data: &[u8]| {
        let mut unstructured = Unstructured::new(data);
        let Ok(flushes) = Vec::<(u8, u8)>::arbitrary(&mut unstructured) else {
            return;
        };

        // Inclusive key bounds per table id; ids grow with recency.
        let mut bounds: Vec<(u8, u8)> = Vec::new();
        // L0 as ids per run, newest run first.
        let mut runs: Vec<Vec<u64>> = Vec::new();

        for (a, b) in flushes.into_iter().take(64) {
            let (a, b) = (a % ALPHABET, b % ALPHABET);
            let (lo, hi) = (a.min(b), a.max(b));
            let id = bounds.len() as u64;
            bounds.push((lo, hi));

            // Each flush puts the new table in front of the previous L0.
            let mut input = vec![vec![(id, key_range(lo, hi))]];
            input.extend(runs.iter().map(|run| {
                run.iter()
                    .map(|&t| {
                        let (l, h) = bounds[t as usize];
                        (t, key_range(l, h))
                    })
                    .collect()
            }));
            runs = optimize_key_ranges(input);

            // No table lost or duplicated.
            let mut ids: Vec<u64> = runs.iter().flatten().copied().collect();
            ids.sort_unstable();
            assert_eq!(
                ids,
                (0..=id).collect::<Vec<_>>(),
                "tables lost or duplicated: {runs:?}"
            );

            // Every run is internally disjoint.
            for run in &runs {
                for (i, &x) in run.iter().enumerate() {
                    for &y in &run[i + 1..] {
                        let (xl, xh) = bounds[x as usize];
                        let (yl, yh) = bounds[y as usize];
                        assert!(
                            xh < yl || yh < xl,
                            "run {run:?} holds overlapping tables {x} and {y}"
                        );
                    }
                }
            }

            // A point read walks the runs front to back and stops at the first
            // run holding the key: that must be the newest table holding it.
            for key in 0..ALPHABET {
                let newest = bounds
                    .iter()
                    .enumerate()
                    .filter(|&(_, &(l, h))| l <= key && key <= h)
                    .map(|(t, _)| t as u64)
                    .max();
                let first_hit = runs.iter().find_map(|run| {
                    run.iter().copied().find(|&t| {
                        let (l, h) = bounds[t as usize];
                        l <= key && key <= h
                    })
                });
                assert_eq!(
                    first_hit, newest,
                    "key {key} resolves to an older table in {runs:?}"
                );
            }
        }
    });
}
