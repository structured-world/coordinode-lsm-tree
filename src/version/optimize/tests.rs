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
