//! End-to-end correctness of the retrieval-ribbon locator on the point-read
//! path: a tree written with the locator enabled must return byte-identical
//! results to one written without it, across multiple versions per key,
//! tombstones, and snapshot reads. The locator only changes *how* a point read
//! finds the block (O(1) ribbon resolve vs index binary search); the answer
//! must be unchanged, with a safe fall-through to the index on any miss.

use lsm_tree::config::{LocatorPolicy, LocatorPolicyEntry, LocatorPrecision};
use lsm_tree::{AbstractTree, Config, SeqNo, SequenceNumberCounter, get_tmp_folder};
use test_log::test;

const N: u64 = 1_500;

fn key_of(i: u64) -> [u8; 8] {
    i.to_be_bytes()
}

/// Build a tree (locator on/off), write a fixed multi-version + tombstone
/// corpus, flush, and compact to a single run so point reads exercise the
/// locator's block resolution over many data blocks.
fn build(precision: Option<LocatorPrecision>) -> (tempfile::TempDir, lsm_tree::AnyTree) {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let mut cfg = Config::new(folder.path(), seqno, SequenceNumberCounter::default())
        // Force many small data blocks so block_id is non-trivial.
        .data_block_size_policy(lsm_tree::config::BlockSizePolicy::all(4_096));
    if let Some(precision) = precision {
        cfg = cfg.locator_policy(LocatorPolicy::all(LocatorPolicyEntry::Enabled {
            precision,
            block_id_bits: None,
            slot_bits: None,
        }));
    }
    let tree = cfg.open().expect("open");

    for i in 0..N {
        // v1 at seqno 10 for every key.
        tree.insert(key_of(i), format!("v1-{i}").as_bytes(), 10);
        // v2 at seqno 20 for even keys.
        if i % 2 == 0 {
            tree.insert(key_of(i), format!("v2-{i}").as_bytes(), 20);
        }
        // tombstone at seqno 30 for every 5th key.
        if i % 5 == 0 {
            tree.remove(key_of(i), 30);
        }
    }
    tree.flush_active_memtable(0).expect("flush");
    tree.major_compact(u64::MAX, 0).expect("compact");

    (folder, tree)
}

/// Run the full query matrix and return the answers as bytes (None encoded as
/// an empty marker) so two trees can be compared exactly.
fn answers(tree: &lsm_tree::AnyTree) -> Vec<Option<Vec<u8>>> {
    let mut out = Vec::new();
    // Probe at three snapshots + present and absent keys.
    for snap in [15u64, 25, SeqNo::MAX] {
        for i in 0..(N + 200) {
            let v = tree
                .get(key_of(i), snap)
                .expect("get")
                .map(|slice| slice.to_vec());
            out.push(v);
        }
    }
    out
}

#[test]
fn locator_precisions_match_index_path() {
    let (_off_dir, off) = build(None);
    let off_answers = answers(&off);

    // Every precision (per-block / per-sub-block / per-key) resolves the data
    // block the same way on the read path; all must match the index baseline.
    for precision in [
        LocatorPrecision::Block,
        LocatorPrecision::Restart,
        LocatorPrecision::Entry,
    ] {
        let (_on_dir, on) = build(Some(precision));
        let on_answers = answers(&on);

        assert_eq!(
            on_answers.len(),
            off_answers.len(),
            "answer matrices must be the same shape",
        );
        for (idx, (a, b)) in on_answers.iter().zip(off_answers.iter()).enumerate() {
            assert_eq!(
                a, b,
                "locator {precision:?} vs locator-off disagree at query #{idx}",
            );
        }
    }

    // Spot-check known expectations against an enabled tree so a both-wrong
    // bug (fast path and index path corrupted identically) cannot pass.
    let (_on_dir, on) = build(Some(LocatorPrecision::Restart));
    // Spot-check a couple of known expectations so a both-wrong bug can't pass.
    // Key 0: v1@10, v2@20 (even), tombstone@30 (i%5==0) → None at MAX.
    assert_eq!(on.get(key_of(0), SeqNo::MAX).expect("get"), None);
    // Key 2: even, not i%5 → v2 at MAX.
    assert_eq!(
        on.get(key_of(2), SeqNo::MAX)
            .expect("get")
            .map(|s| s.to_vec()),
        Some(b"v2-2".to_vec()),
    );
    // Key 3: odd, not i%5 → v1 at MAX.
    assert_eq!(
        on.get(key_of(3), SeqNo::MAX)
            .expect("get")
            .map(|s| s.to_vec()),
        Some(b"v1-3".to_vec()),
    );
    // Absent key → None.
    assert_eq!(on.get(key_of(N + 50), SeqNo::MAX).expect("get"), None);
}
