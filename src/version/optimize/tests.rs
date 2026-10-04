use super::*;
use crate::KeyRange;
use crate::comparator::DefaultUserComparator;
use test_log::test;

fn default_cmp() -> &'static DefaultUserComparator {
    &DefaultUserComparator
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FakeTable {
    id: u64,
    key_range: KeyRange,
}

impl Ranged for FakeTable {
    fn key_range(&self) -> &KeyRange {
        &self.key_range
    }
}

fn s(id: u64, min: &str, max: &str) -> FakeTable {
    FakeTable {
        id,
        key_range: KeyRange::new((min.as_bytes().into(), max.as_bytes().into())),
    }
}

#[test]
fn optimize_runs_empty() {
    let runs = vec![];
    let runs = optimize_runs::<FakeTable>(runs, default_cmp());

    assert_eq!(Vec::<Run<FakeTable>>::new(), &*runs);
}

#[test]
fn optimize_runs_one() {
    let runs = vec![Run::new(vec![s(0, "a", "b")]).unwrap()];
    let runs = optimize_runs::<FakeTable>(runs, default_cmp());

    assert_eq!(vec![Run::new(vec![s(0, "a", "b")]).unwrap()], &*runs);
}

#[test]
fn optimize_runs_two_overlap() {
    let runs = vec![
        Run::new(vec![s(0, "a", "b")]).unwrap(),
        Run::new(vec![s(1, "a", "b")]).unwrap(),
    ];
    let runs = optimize_runs::<FakeTable>(runs, default_cmp());

    assert_eq!(
        vec![
            Run::new(vec![s(0, "a", "b")]).unwrap(),
            Run::new(vec![s(1, "a", "b")]).unwrap(),
        ],
        &*runs
    );
}

#[test]
fn optimize_runs_two_overlap_2() {
    let runs = vec![
        Run::new(vec![s(0, "a", "z")]).unwrap(),
        Run::new(vec![s(1, "c", "f")]).unwrap(),
    ];
    let runs = optimize_runs::<FakeTable>(runs, default_cmp());

    assert_eq!(
        vec![
            Run::new(vec![s(0, "a", "z")]).unwrap(),
            Run::new(vec![s(1, "c", "f")]).unwrap(),
        ],
        &*runs
    );
}

#[test]
fn optimize_runs_two_overlap_3() {
    let runs = vec![
        Run::new(vec![s(0, "c", "f")]).unwrap(),
        Run::new(vec![s(1, "a", "z")]).unwrap(),
    ];
    let runs = optimize_runs::<FakeTable>(runs, default_cmp());

    assert_eq!(
        vec![
            Run::new(vec![s(0, "c", "f")]).unwrap(),
            Run::new(vec![s(1, "a", "z")]).unwrap()
        ],
        &*runs
    );
}

#[test]
fn optimize_runs_two_disjoint() {
    let runs = vec![
        Run::new(vec![s(0, "a", "c")]).unwrap(),
        Run::new(vec![s(1, "d", "f")]).unwrap(),
    ];
    let runs = optimize_runs::<FakeTable>(runs, default_cmp());

    assert_eq!(
        vec![Run::new(vec![s(0, "a", "c"), s(1, "d", "f")]).unwrap()],
        &*runs,
    );
}

/// A table disjoint from an older run must not be merged into it when a run
/// between the two overlaps the table: `C` (newest) is disjoint from `A`
/// (oldest) but overlaps `B`, and joining `A`'s run would put `C` behind `B`,
/// so a key in both `B` and `C` would resolve to `B`'s older version.
#[test]
fn optimize_runs_overlap_transitive_keeps_newest_in_front() {
    let runs = vec![
        Run::new(vec![s(2, "m", "p")]).unwrap(),
        Run::new(vec![s(1, "a", "z")]).unwrap(),
        Run::new(vec![s(0, "a", "c")]).unwrap(),
    ];
    let runs = optimize_runs::<FakeTable>(runs, default_cmp());

    assert_eq!(
        vec![
            Run::new(vec![s(2, "m", "p")]).unwrap(),
            Run::new(vec![s(1, "a", "z")]).unwrap(),
            Run::new(vec![s(0, "a", "c")]).unwrap(),
        ],
        &*runs
    );
}

/// Every table that overlaps another must sit in an earlier run than it when
/// it is newer, every run must stay internally disjoint, and no table may be
/// lost or duplicated, across a sequence of flushes that each optimize the
/// fresh table in front of the previous L0 (ids grow with recency).
#[test]
fn optimize_runs_flush_sequence_keeps_recency_order_and_disjoint_runs() {
    use proptest::prelude::*;

    fn overlaps(a: &FakeTable, b: &FakeTable) -> bool {
        a.key_range()
            .overlaps_with_key_range_cmp(b.key_range(), default_cmp())
    }

    let range = (0u8..26, 0u8..26).prop_map(|(x, y)| (x.min(y), x.max(y)));
    proptest!(|(ranges in proptest::collection::vec(range, 1..40))| {
        let mut runs: Vec<Run<FakeTable>> = Vec::new();
        for (id, (lo, hi)) in ranges.iter().enumerate() {
            let table = FakeTable {
                id: id as u64,
                key_range: KeyRange::new((
                    vec![b'a' + lo].into(),
                    vec![b'a' + hi].into(),
                )),
            };
            let mut input = vec![Run::new(vec![table]).unwrap()];
            input.extend(runs);
            runs = optimize_runs(input, default_cmp());

            let placed: Vec<(usize, &FakeTable)> = runs
                .iter()
                .enumerate()
                .flat_map(|(at, run)| run.iter().map(move |t| (at, t)))
                .collect();
            let mut ids: Vec<u64> = placed.iter().map(|(_, t)| t.id).collect();
            ids.sort_unstable();
            prop_assert_eq!(ids, (0..=id as u64).collect::<Vec<_>>());

            for (i, (run_a, a)) in placed.iter().enumerate() {
                for (run_b, b) in placed.iter().skip(i + 1) {
                    if !overlaps(a, b) {
                        continue;
                    }
                    prop_assert_ne!(run_a, run_b, "overlapping tables share a run");
                    let (newer_run, older_run) =
                        if a.id > b.id { (run_a, run_b) } else { (run_b, run_a) };
                    prop_assert!(
                        newer_run < older_run,
                        "a newer table sits behind an older one it overlaps"
                    );
                }
            }
        }
    });
}

#[test]
fn optimize_runs_two_disjoint_2() {
    let runs = vec![
        Run::new(vec![s(1, "d", "f")]).unwrap(),
        Run::new(vec![s(0, "a", "c")]).unwrap(),
    ];
    let runs = optimize_runs::<FakeTable>(runs, default_cmp());

    assert_eq!(
        vec![Run::new(vec![s(0, "a", "c"), s(1, "d", "f")]).unwrap()],
        &*runs,
    );
}
