//! Every fixture is built and checked here, including the ones whose scenario
//! has no read path yet.
//!
//! The benchmark skips an unsupported scenario's fixture to avoid spending the
//! run writing data nothing can read. That would leave those fixtures
//! unexercised, and an unexercised fixture rots: by the time the capability
//! lands, the data it builds no longer compiles or no longer means what the
//! scenario needs. Building all of them here keeps them honest, and makes
//! enabling a scenario a one-line change rather than a fresh argument about
//! what its expected result is.
//!
//! The checks are deliberately about the ORACLE, not about read performance:
//! that the shape it describes is the shape asked for, and that an ordinary
//! read agrees with it where an ordinary read applies.

use super::fixtures::{self, Fixture};
use crate::config::{BenchConfig, Compression};
use lsm_tree::{AbstractTree, SeqNo};
use std::sync::atomic::AtomicU64;

/// Small enough to build every fixture in a test run, large enough that the
/// strided and modular write patterns still overlap the way they do at scale.
const N: u64 = 3_000;

fn config() -> BenchConfig {
    BenchConfig {
        num: N,
        key_size: 16,
        value_size: 100,
        threads: 1,
        cache_mb: 8,
        compression: Compression::None,
        block_size: 4_096,
        use_blob_tree: false,
        metadata_priority: true,
        partition_metadata: false,
    }
}

fn build(f: fixtures::FixtureFn) -> Fixture {
    let seqno = AtomicU64::new(1);
    f(&config(), &seqno).expect("fixture must build")
}

/// The ordinary read is the cross-check, not the oracle: it agrees with the
/// write history on every key, or one of the two is wrong and the test says
/// which key.
fn assert_ordinary_read_agrees(fixture: &Fixture, what: &str) {
    for row in &fixture.oracle.rows {
        let got = fixture.tree.get(&*row.key, SeqNo::MAX).expect("get");
        match (&row.expect, got) {
            (Some(expected), Some(actual)) => assert_eq!(
                &*actual,
                fixture.read_bytes(*expected).expect("frame").as_slice(),
                "{what}: value for {:?} disagrees with the write history",
                String::from_utf8_lossy(&row.key),
            ),
            (None, None) => {}
            (Some(_), None) => panic!(
                "{what}: {:?} was written and not deleted, but reads as absent",
                String::from_utf8_lossy(&row.key),
            ),
            (None, Some(_)) => panic!(
                "{what}: {:?} was deleted, but still reads as present",
                String::from_utf8_lossy(&row.key),
            ),
        }
    }
}

#[test]
fn every_fixture_ordinary_read_matches_oracle() {
    // Includes the fixtures of the unsupported scenarios: they are the ones
    // most at risk of rotting unnoticed, because the benchmark never builds
    // them.
    let all: [(&str, fixtures::FixtureFn); 8] = [
        ("narrow", fixtures::narrow),
        ("wide", fixtures::wide),
        ("mixed-sizes", fixtures::mixed_sizes),
        (
            "columnar-base-row-updates",
            fixtures::columnar_base_row_updates,
        ),
        (
            "versions-deletes-tombstones",
            fixtures::versions_deletes_tombstones,
        ),
        ("selectivity", fixtures::selectivity),
        ("blobs-well-placed", fixtures::blobs_well_placed),
        ("blobs-scattered", fixtures::blobs_scattered),
    ];

    for (what, f) in all {
        let fixture = build(f);
        assert!(
            !fixture.oracle.rows.is_empty(),
            "{what}: a fixture with no rows expects nothing and so proves nothing",
        );
        assert_ordinary_read_agrees(&fixture, what);
    }
}

#[test]
fn value_bytes_same_seed_reproduces_filter_fields() {
    // The oracle records 16 bytes per row instead of the value, so everything
    // downstream rests on the bytes being a pure function of the two numbers,
    // and on the predicate fields being readable back out of them.
    for seed in [0_u64, 1, 96, 97, 12_345] {
        let bytes = fixtures::value_bytes(seed, fixtures::HEADER_LEN + 64);
        assert_eq!(
            bytes,
            fixtures::value_bytes(seed, fixtures::HEADER_LEN + 64),
            "the same seed and length must give the same bytes",
        );
        assert_eq!(
            fixtures::field_group(&bytes),
            Some(fixtures::group_of(seed)),
            "the sparse field must survive the round trip through the value",
        );
        assert_eq!(
            fixtures::field_bucket(&bytes),
            Some(fixtures::bucket_of(seed)),
            "the near-full field must survive the round trip through the value",
        );
    }
}

#[test]
fn selectivity_fixture_sparse_vs_near_full_selects_distinct_fractions() {
    // The two scenarios over this fixture only mean something if the
    // predicates really do select very different fractions: if both matched
    // most rows, the pair would measure one case twice.
    let fixture = build(fixtures::selectivity);
    let total = fixture.oracle.rows.len() as u64;
    let sparse = fixture.oracle.selected();
    let near_full = fixture
        .oracle
        .rows
        .iter()
        .filter(|r| r.expect.is_some_and(|v| fixtures::bucket_of(v.seed) != 0))
        .count() as u64;

    assert!(
        sparse * 50 < total,
        "the sparse predicate selected {sparse} of {total}, which is not sparse",
    );
    assert!(
        near_full * 10 > total * 8,
        "the near-full predicate selected {near_full} of {total}, which is not near-full",
    );
}

#[test]
fn selective_scans_sparse_predicate_reads_less_than_near_full() {
    // The predicate is the engine's to evaluate. A pass that scanned every row
    // and filtered in the harness would read the same blocks at every
    // selectivity, and the pair would differ only in the row count the figures
    // divide by. The sparse field matches one row in 97, so most blocks hold
    // none and the zone map skips them unread; the near-full one skips none.
    let measure = |read: super::ReadFn| {
        let fixture = build(fixtures::selectivity);
        let readings =
            super::Readings::measure(&fixture.tree, || read(&fixture)).expect("scan must succeed");
        (readings.rows, readings.bytes_read)
    };
    let (sparse_rows, sparse_read) = measure(super::scan_sparse);
    let (full_rows, full_read) = measure(super::scan_near_full);

    assert!(sparse_rows > 0 && full_rows > sparse_rows * 10);
    assert!(
        sparse_read * 2 < full_read,
        "the sparse scan read {sparse_read} B against {full_read} B for the near-full \
         one; the predicate is not skipping anything",
    );
}

#[test]
fn versions_fixture_deletes_and_range_tombstone_remove_exactly_covered_rows() {
    // The shape this fixture exists for. Asserted against the operations
    // performed rather than against a read: every fifth key was deleted, and a
    // contiguous slice in the middle was covered by a range tombstone, so the
    // oracle must show both and nothing else.
    let fixture = build(fixtures::versions_deletes_tombstones);
    let n = fixture.oracle.rows.len() as u64;
    let (lo, hi) = (n / 2, n / 2 + n / 20);

    for (i, row) in fixture.oracle.rows.iter().enumerate() {
        let i = i as u64;
        let covered = i.is_multiple_of(5) || (lo..hi).contains(&i);
        assert_eq!(
            row.expect.is_none(),
            covered,
            "row {i}: expected {}, oracle says {}",
            if covered { "removed" } else { "present" },
            if row.expect.is_none() {
                "removed"
            } else {
                "present"
            },
        );
    }
    assert!(
        fixture.oracle.visible() > 0 && fixture.oracle.visible() < n,
        "the fixture must remove some rows and keep some",
    );
}

#[test]
fn blob_placement_scenarios_zero_cache_report_unsupported() {
    // Placement moves the counters only through the scan's blob prefetch,
    // which a zero-capacity cache turns off. A figure published then would
    // measure the two fixtures' version histories under the placement name.
    let placement = |config: &BenchConfig| {
        super::scenarios(config)
            .into_iter()
            .filter(|s| s.name.starts_with("blobs-") && s.name != "blobs-filtered-before-fetch")
            .map(|s| matches!(s.support, super::Support::Native(_)))
            .collect::<Vec<_>>()
    };
    let cold = BenchConfig {
        cache_mb: 0,
        ..config()
    };
    assert_eq!(placement(&cold), vec![false, false], "zero cache");
    assert_eq!(
        placement(&config()),
        vec![true, true],
        "a cache enables them"
    );
}

#[test]
fn scattered_blob_fixture_num_equal_to_preferred_stride_writes_every_key() {
    // The strided first pass must be a permutation for EVERY row count. A
    // fixed stride is not coprime with its own multiples, and at `--num 7919`
    // the old one sent every step to key 0, leaving the rows the rewrite
    // rounds skip absent and the scenario measuring a far smaller dataset.
    let seqno = AtomicU64::new(1);
    let config = BenchConfig {
        num: 7_919,
        ..config()
    };
    let fixture = fixtures::blobs_scattered(&config, &seqno).expect("fixture must build");
    let absent = fixture
        .oracle
        .rows
        .iter()
        .filter(|r| r.expect.is_none())
        .count();
    assert_eq!(
        absent, 0,
        "{absent} of 7919 keys were never written by the strided pass",
    );
}

#[test]
fn scattered_blob_fixture_rewrite_rounds_overwrite_over_a_quarter() {
    // "Scattered" is a property of the write history, not of the final
    // content: a key's live blob has to sit in whichever file its last rewrite
    // round landed in. If the rounds stopped overlapping the first pass, the
    // fixture would be the well-placed one under another name.
    let fixture = build(fixtures::blobs_scattered);
    let rewritten = fixture
        .oracle
        .rows
        .iter()
        .filter(|r| r.expect.is_some_and(|v| v.seed >= 1_000_000))
        .count();
    let total = fixture.oracle.rows.len();
    assert!(
        rewritten * 4 > total,
        "only {rewritten} of {total} rows carry a rewritten value; the rounds \
         are no longer scattering anything",
    );
}

#[test]
fn mixed_layout_more_than_one_thread_is_refused() {
    // The scenarios run one after another on one thread, and the figures are
    // bytes per row, which concurrency does not change. A run asking for more
    // threads would be recorded under a thread count it never used.
    use crate::workloads::Workload;
    assert!(
        super::MixedLayout
            .check_config(&BenchConfig {
                threads: 2,
                ..config()
            })
            .is_err(),
        "a thread count the workload ignores must be refused, not reported",
    );
    assert_eq!(super::MixedLayout.check_config(&config()), Ok(()));
}

#[test]
fn every_fixture_num_zero_builds_an_empty_oracle() {
    // `--num 0` is a valid run. Every fixture must finish with an empty oracle:
    // the scattered one searched for a stride coprime with the key count, and
    // with no keys nothing is coprime with it, so it looped instead of
    // returning.
    let empty = BenchConfig { num: 0, ..config() };
    let all: [(&str, fixtures::FixtureFn); 8] = [
        ("narrow", fixtures::narrow),
        ("wide", fixtures::wide),
        ("mixed-sizes", fixtures::mixed_sizes),
        (
            "columnar-base-row-updates",
            fixtures::columnar_base_row_updates,
        ),
        (
            "versions-deletes-tombstones",
            fixtures::versions_deletes_tombstones,
        ),
        ("selectivity", fixtures::selectivity),
        ("blobs-well-placed", fixtures::blobs_well_placed),
        ("blobs-scattered", fixtures::blobs_scattered),
    ];
    for (what, f) in all {
        let seqno = AtomicU64::new(1);
        let fixture = f(&empty, &seqno).expect("fixture must build");
        assert!(
            fixture.oracle.rows.is_empty(),
            "{what}: no keys were asked for, yet the oracle expects some",
        );
    }
}

#[test]
fn blob_fixture_compression_none_opens_its_blob_files_uncompressed() {
    // The blob scenarios are dominated by blob bytes, so a run labelled with a
    // codec has to write its blobs with that codec; otherwise every codec
    // measures the blob default and the runs cannot be compared.
    let fixture = build(fixtures::blobs_well_placed);
    let lsm_tree::AnyTree::Blob(tree) = &fixture.tree else {
        panic!("the blob fixture must open a KV-separated tree");
    };
    assert_eq!(
        tree.runtime_config().blob_compression,
        lsm_tree::CompressionType::None,
        "--compression none must reach the blob files",
    );
}
