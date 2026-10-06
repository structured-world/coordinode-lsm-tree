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

impl Aged for FakeTable {
    type Age = u64;

    fn age(&self) -> u64 {
        self.id
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

/// A flush that joined a compaction input's run leaves the newer table in a
/// run the output is then placed at; the output must still go behind the
/// newer table it overlaps, whatever slot it was given.
#[test]
fn l0_order_puts_a_newer_flush_ahead_of_an_output_given_in_front() {
    // T (newest) overlaps the output O; O was handed in ahead of it.
    let runs = [
        Run::new(vec![s(15, "a", "f")]).unwrap(),
        Run::new(vec![s(30, "d", "e")]).unwrap(),
    ];
    let runs = order_by_age(
        runs.iter().flat_map(|run| run.iter().cloned()),
        default_cmp(),
    );
    assert_eq!(
        vec![
            Run::new(vec![s(30, "d", "e")]).unwrap(),
            Run::new(vec![s(15, "a", "f")]).unwrap(),
        ],
        &*runs
    );
}

/// A manifest the earlier placement wrote may hold `[B], [A, C]` with `C`
/// newer than `B` and overlapping it; laying L0 out from ages repairs it.
#[test]
fn l0_order_repairs_a_persisted_order_with_a_newer_table_behind() {
    let runs = [
        Run::new(vec![s(1, "m", "p")]).unwrap(),
        Run::new(vec![s(0, "a", "c"), s(2, "n", "z")]).unwrap(),
    ];
    let runs = order_by_age(
        runs.iter().flat_map(|run| run.iter().cloned()),
        default_cmp(),
    );
    let run_of = |id: u64| {
        runs.iter()
            .position(|run| run.iter().any(|t| t.id == id))
            .unwrap()
    };
    assert!(
        run_of(2) < run_of(1),
        "C goes ahead of the older B it overlaps"
    );
}

/// A recovered L0 is laid out again only when it breaks recency order: the
/// earlier placement's `[B], [A, C]` with `C` newer than the `B` it overlaps
/// is caught, a layout a run per table newest first (as repair writes it) and
/// disjoint tables in one run both pass.
#[test]
fn recency_order_is_checked_only_among_overlapping_tables() {
    let broken = [
        Run::new(vec![s(1, "m", "p")]).unwrap(),
        Run::new(vec![s(0, "a", "c"), s(2, "n", "z")]).unwrap(),
    ];
    assert!(!in_recency_order(
        &broken.iter().collect::<Vec<_>>(),
        default_cmp()
    ));

    let per_table = [
        Run::new(vec![s(2, "a", "z")]).unwrap(),
        Run::new(vec![s(1, "b", "c")]).unwrap(),
        Run::new(vec![s(0, "x", "y")]).unwrap(),
    ];
    assert!(in_recency_order(
        &per_table.iter().collect::<Vec<_>>(),
        default_cmp()
    ));

    let disjoint = [Run::new(vec![s(0, "a", "b"), s(5, "c", "d"), s(3, "e", "f")]).unwrap()];
    assert!(in_recency_order(
        &disjoint.iter().collect::<Vec<_>>(),
        default_cmp()
    ));
}

/// The range-maximum check answers as comparing every overlapping pair does,
/// for L0 laid out a table per run in any order, ages in any order, and
/// tables touching at a bound.
#[test]
fn recency_order_check_matches_comparing_every_pair() {
    use proptest::prelude::*;

    let range = (0u8..12, 0u8..12).prop_map(|(x, y)| (x.min(y), x.max(y)));
    proptest!(|(tables in proptest::collection::vec((range, any::<u16>()), 0..30))| {
        // Ages are unique, as a table's id makes them: the random part orders,
        // the position breaks a tie.
        let runs: Vec<Run<FakeTable>> = tables
            .iter()
            .enumerate()
            .map(|(at, ((lo, hi), age))| {
                Run::new(vec![FakeTable {
                    id: (u64::from(*age) << 8) | at as u64,
                    key_range: KeyRange::new((vec![b'a' + lo].into(), vec![b'a' + hi].into())),
                }])
                .unwrap()
            })
            .collect();
        let refs: Vec<&Run<FakeTable>> = runs.iter().collect();

        let every_pair = refs.iter().enumerate().all(|(i, a)| {
            refs.iter().skip(i + 1).all(|b| {
                a.iter().zip(b.iter()).all(|(a, b)| {
                    !a.key_range().overlaps_with_key_range_cmp(b.key_range(), default_cmp())
                        || a.id >= b.id
                })
            })
        });
        prop_assert_eq!(in_recency_order(&refs, default_cmp()), every_pair);
    });
}

/// Whatever run order L0 is handed in, the layout keeps every newer table
/// ahead of each older one it overlaps, keeps runs disjoint and loses nothing.
#[test]
fn l0_order_holds_recency_order_for_any_input_layout() {
    use proptest::prelude::*;

    let range = (0u8..26, 0u8..26).prop_map(|(x, y)| (x.min(y), x.max(y)));
    proptest!(|(ranges in proptest::collection::vec(range, 1..40), seed in any::<u64>())| {
        let mut tables: Vec<FakeTable> = ranges
            .iter()
            .enumerate()
            .map(|(id, (lo, hi))| FakeTable {
                id: id as u64,
                key_range: KeyRange::new((vec![b'a' + lo].into(), vec![b'a' + hi].into())),
            })
            .collect();
        // Any order.
        let mut state = seed | 1;
        for i in (1..tables.len()).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let j = usize::try_from(state % (i as u64 + 1)).unwrap();
            tables.swap(i, j);
        }
        let runs = order_by_age(tables, default_cmp());

        let placed: Vec<(usize, &FakeTable)> = runs
            .iter()
            .enumerate()
            .flat_map(|(at, run)| run.iter().map(move |t| (at, t)))
            .collect();
        prop_assert_eq!(placed.len(), ranges.len());
        for (i, (run_a, a)) in placed.iter().enumerate() {
            for (run_b, b) in placed.iter().skip(i + 1) {
                if !a.key_range().overlaps_with_key_range_cmp(b.key_range(), default_cmp()) {
                    continue;
                }
                prop_assert_ne!(run_a, run_b, "overlapping tables share a run");
                let (newer_run, older_run) =
                    if a.id > b.id { (run_a, run_b) } else { (run_b, run_a) };
                prop_assert!(newer_run < older_run, "a newer table behind an older one");
            }
        }
    });
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
