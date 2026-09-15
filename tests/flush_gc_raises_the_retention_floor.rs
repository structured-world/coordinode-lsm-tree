// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A flush that collects below a watermark must record that it did.
//!
//! `AbstractTree::flush` runs the sealed memtables through the same
//! `CompactionStream` a compaction uses, with the same GC threshold, so it
//! collects the same versions. The version it installed used to be recorded
//! with `RetentionEffect::Keep`, which left the persisted retention floor
//! where it was.
//!
//! The floor is the read boundary, live and after a reopen alike. Admitting a
//! read the flush had already collected the answer for returned a silent
//! "absent" instead of `SnapshotBelowRetention`, which a consumer cannot tell
//! from a genuine delete.
//!
//! These tests pin both sides of that boundary, and the watermark-0 case that
//! must move nothing.

use lsm_tree::{AbstractTree, Config, MergeOperator, SeqNo, SequenceNumberCounter, UserValue};

/// Concatenates the base and every operand, so a fold is observable in the
/// value it produces.
struct ConcatMerge;

impl MergeOperator for ConcatMerge {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut out = base_value.unwrap_or_default().to_vec();
        for operand in operands {
            out.push(b',');
            out.extend_from_slice(operand);
        }
        Ok(out.into())
    }
}

#[test]
fn a_flush_that_collects_below_the_watermark_refuses_reads_below_it_after_a_reopen()
-> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    // The counter sits ABOVE the data seqnos, as any real deployment has it:
    // the install seqno comes from it, and the routing that hides this defect
    // while the process lives depends on the install being above the data.
    let open = || -> lsm_tree::Result<lsm_tree::AnyTree> {
        Config::new(
            dir.path(),
            SequenceNumberCounter::new(100),
            SequenceNumberCounter::new(100),
        )
        .open()
    };

    let tree = open()?;

    // Three versions of one key. Two sit below the watermark used below, so the
    // fold collects the older of those two and keeps the newer; the third is
    // above the watermark and is kept outright.
    tree.insert("k", "oldest", 2);
    tree.insert("k", "middle", 5);
    tree.insert("k", "newest", 10);

    // Establish the reads before the flush, so the test cannot pass by the key
    // never having been readable at these snapshots.
    assert_eq!(
        tree.get("k", 3)?.as_deref(),
        Some(&b"oldest"[..]),
        "precondition: snapshot 3 resolves to the version at seqno 2",
    );
    assert_eq!(
        tree.get("k", 8)?.as_deref(),
        Some(&b"middle"[..]),
        "precondition: snapshot 8 resolves to the version at seqno 5",
    );

    // Flush with a GC watermark of 8. The fold drops the version at seqno 2,
    // which is exactly what snapshot 3 resolved to.
    tree.flush_active_memtable(8)?;

    // The install has to record that, the same way a compaction at this
    // watermark does: a watermark of 8 makes every snapshot below 8 unservable,
    // so the floor is 7.
    assert_eq!(
        tree.retention_floor(),
        7,
        "a flush that collected below watermark 8 must record a floor of 7",
    );

    // The floor is the live boundary too: the read is refused already, with
    // no pre-flush version kept around to answer it.
    assert!(
        matches!(
            tree.get("k", SeqNo::from(3_u64)),
            Err(lsm_tree::Error::SnapshotBelowRetention { .. })
        ),
        "a live tree refuses what the flush collected, as a reopened one does",
    );

    drop(tree);
    let reopened = open()?;

    assert_eq!(
        reopened.retention_floor(),
        7,
        "the floor a collecting flush recorded must survive the reopen",
    );

    // The read the flush collected the answer for must be REFUSED. Answering it
    // with an absent key is indistinguishable from a genuine delete.
    assert!(
        matches!(
            reopened.get("k", SeqNo::from(3_u64)),
            Err(lsm_tree::Error::SnapshotBelowRetention { .. })
        ),
        "a read below the floor a collecting flush recorded must be refused, \
         not answered with a missing key",
    );

    // And the boundary: the floor itself is refused, while the smallest
    // admitted snapshot still resolves to the newest version below the
    // watermark, which the fold kept on purpose.
    assert!(
        matches!(
            reopened.get("k", SeqNo::from(7_u64)),
            Err(lsm_tree::Error::SnapshotBelowRetention { .. })
        ),
        "a read AT the floor must be refused",
    );
    assert_eq!(
        reopened.get("k", 8)?.as_deref(),
        Some(&b"middle"[..]),
        "the smallest admitted snapshot must still resolve to the newest \
         version below the watermark",
    );

    Ok(())
}

#[test]
fn a_flush_that_collects_nothing_leaves_the_floor_alone() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    let open = || -> lsm_tree::Result<lsm_tree::AnyTree> {
        Config::new(
            dir.path(),
            SequenceNumberCounter::new(100),
            SequenceNumberCounter::new(100),
        )
        .open()
    };

    let tree = open()?;

    // One version per key: there is no history to fold at any watermark, so
    // this flush takes nothing away from any snapshot.
    tree.insert("a", "va", 2);
    tree.insert("b", "vb", 3);

    tree.flush_active_memtable(50)?;

    assert_eq!(
        tree.retention_floor(),
        0,
        "a flush that collected nothing must not claim a floor",
    );

    drop(tree);
    let reopened = open()?;

    assert_eq!(reopened.retention_floor(), 0);
    // The reads the watermark would have refused are still answerable, and
    // must be answered: their data was never collected.
    assert_eq!(reopened.get("a", 3)?.as_deref(), Some(&b"va"[..]));
    assert_eq!(reopened.get("b", 4)?.as_deref(), Some(&b"vb"[..]));

    Ok(())
}

#[test]
fn an_rt_only_flush_leaves_the_floor_alone() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    let open = || -> lsm_tree::Result<lsm_tree::AnyTree> {
        Config::new(
            dir.path(),
            SequenceNumberCounter::new(100),
            SequenceNumberCounter::new(100),
        )
        .open()
    };

    let tree = open()?;

    // Land a value in an SST first, so there is history the floor could
    // wrongly refuse.
    tree.insert("k", "v", 2);
    tree.flush_active_memtable(0)?;

    // Now a memtable holding only a range tombstone: it produces no tables, so
    // nothing goes through the fold at all.
    tree.remove_range("x", "y", 40);
    tree.flush_active_memtable(50)?;

    assert_eq!(
        tree.retention_floor(),
        0,
        "a flush with no entries to fold must not claim a floor",
    );

    drop(tree);
    let reopened = open()?;

    assert_eq!(reopened.retention_floor(), 0);
    assert_eq!(
        reopened.get("k", 3)?.as_deref(),
        Some(&b"v"[..]),
        "the pre-existing version stays readable at a snapshot below the watermark",
    );

    Ok(())
}

#[test]
fn a_compaction_that_collects_nothing_leaves_the_floor_alone() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    let open = || -> lsm_tree::Result<lsm_tree::AnyTree> {
        Config::new(
            dir.path(),
            SequenceNumberCounter::new(100),
            SequenceNumberCounter::new(100),
        )
        .open()
    };

    let tree = open()?;

    // One version per key across two tables, so the merge has no history to
    // fold. The watermark sits BELOW both versions on purpose: at the bottom
    // level a watermark above them would also zero their seqnos, and that
    // changes what a snapshot resolves to even though no version is dropped
    // (see the zeroing test above). Touching nothing at all is the case here.
    tree.insert("a", "va", 20);
    tree.flush_active_memtable(0)?;
    tree.insert("b", "vb", 30);
    tree.flush_active_memtable(0)?;

    tree.major_compact(u64::MAX, 10)?;

    assert_eq!(
        tree.retention_floor(),
        0,
        "a compaction that collected nothing must not claim a floor",
    );

    drop(tree);
    let reopened = open()?;

    assert_eq!(reopened.retention_floor(), 0);
    assert_eq!(reopened.get("a", 21)?.as_deref(), Some(&b"va"[..]));
    assert_eq!(reopened.get("b", 31)?.as_deref(), Some(&b"vb"[..]));

    Ok(())
}

/// Bottommost seqno zeroing rewrites a below-watermark version to seqno 0. The
/// entry count is unchanged, so nothing is "collected" in the losing sense, but
/// the version now answers snapshots that predate it. The floor has to move, or
/// a reopened tree admits such a snapshot and hands it a value that did not
/// exist then.
#[test]
fn a_compaction_that_only_zeroed_seqnos_still_raises_the_floor() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    let open = || -> lsm_tree::Result<lsm_tree::AnyTree> {
        Config::new(
            dir.path(),
            SequenceNumberCounter::new(100),
            SequenceNumberCounter::new(100),
        )
        .open()
    };

    let tree = open()?;

    // One version per key, so the fold collects nothing and the balance stays
    // at zero. Both sit below the watermark used below, so the bottom level
    // zeroes them.
    tree.insert("a", "va", 2);
    tree.insert("b", "vb", 3);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        tree.get("a", 2)?.as_deref(),
        None,
        "precondition: snapshot 2 predates the version at seqno 2",
    );

    tree.major_compact(u64::MAX, 50)?;

    assert_eq!(
        tree.retention_floor(),
        49,
        "zeroing a version's seqno has to raise the floor even though no version was dropped",
    );

    drop(tree);
    let reopened = open()?;

    assert!(
        matches!(
            reopened.get("a", SeqNo::from(2_u64)),
            Err(lsm_tree::Error::SnapshotBelowRetention { .. })
        ),
        "the snapshot the zeroing would answer wrongly must be refused instead",
    );

    Ok(())
}

/// A merge fold consumes the base version inline, which is a loss the fold's
/// own drain never sees. What the recorded floor then owes is the whole point:
/// every snapshot above the watermark must still be served after a reopen, and
/// the ones below it must be refused rather than answered from a base that is
/// no longer there.
#[test]
fn a_merge_fold_compaction_serves_every_snapshot_above_the_watermark_after_a_reopen()
-> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    let open = || -> lsm_tree::Result<lsm_tree::AnyTree> {
        Config::new(
            dir.path(),
            SequenceNumberCounter::new(100),
            SequenceNumberCounter::new(100),
        )
        .with_merge_operator(Some(std::sync::Arc::new(ConcatMerge)))
        .open()
    };

    let tree = open()?;

    // A base and an operand, both below the watermark used below, so the fold
    // runs and swallows the base into the merged result.
    tree.insert("k", "base", 2);
    tree.merge("k", "op", 5);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        tree.get("k", 3)?.as_deref(),
        Some(&b"base"[..]),
        "precondition: snapshot 3 resolves to the base alone",
    );

    tree.major_compact(u64::MAX, 8)?;

    let floor = tree.retention_floor();
    assert_eq!(floor, 7, "the fold collected the base, so the floor moves");

    drop(tree);
    let reopened = open()?;

    // The contract this test exists for: above the floor, still served.
    assert_eq!(
        reopened.get("k", floor + 1)?.as_deref(),
        Some(&b"base,op"[..]),
        "the smallest admitted snapshot must still be answered after a reopen",
    );
    assert_eq!(
        reopened.get("k", SeqNo::MAX)?.as_deref(),
        Some(&b"base,op"[..]),
    );

    // And below it, refused rather than answered from a base that is gone.
    assert!(
        matches!(
            reopened.get("k", SeqNo::from(3_u64)),
            Err(lsm_tree::Error::SnapshotBelowRetention { .. })
        ),
        "a snapshot whose answer the fold consumed must be refused",
    );

    Ok(())
}

#[test]
fn a_compaction_that_collects_history_still_raises_the_floor() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::new(100),
        SequenceNumberCounter::new(100),
    )
    .open()?;

    // Two versions of one key, both below the watermark: the fold keeps the
    // newer and collects the older, so the floor must move.
    tree.insert("k", "old", 2);
    tree.flush_active_memtable(0)?;
    tree.insert("k", "new", 5);
    tree.flush_active_memtable(0)?;

    tree.major_compact(u64::MAX, 50)?;

    assert_eq!(
        tree.retention_floor(),
        49,
        "a compaction that collected history records the watermark-derived floor",
    );

    Ok(())
}

#[test]
fn a_flush_at_watermark_zero_leaves_the_floor_alone() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    let open = || -> lsm_tree::Result<lsm_tree::AnyTree> {
        Config::new(
            dir.path(),
            SequenceNumberCounter::new(100),
            SequenceNumberCounter::new(100),
        )
        .open()
    };

    let tree = open()?;

    tree.insert("k", "oldest", 2);
    tree.insert("k", "middle", 5);
    tree.insert("k", "newest", 10);

    // Watermark 0 collects nothing, so it must move nothing: the whole history
    // stays servable, which is what every in-crate flush relies on.
    tree.flush_active_memtable(0)?;

    assert_eq!(tree.retention_floor(), 0);

    drop(tree);
    let reopened = open()?;

    assert_eq!(reopened.retention_floor(), 0);
    assert_eq!(reopened.get("k", 3)?.as_deref(), Some(&b"oldest"[..]));
    assert_eq!(reopened.get("k", 8)?.as_deref(), Some(&b"middle"[..]));
    assert_eq!(
        reopened.get("k", SeqNo::MAX)?.as_deref(),
        Some(&b"newest"[..])
    );

    Ok(())
}
