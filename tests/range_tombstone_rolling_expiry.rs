//! Rolling expiry of key ranges with range tombstones under leveled
//! compaction: an expired range stays expired.

use lsm_tree::compaction::{CompactionAction, Leveled};
use lsm_tree::{AbstractTree, AnyTree, Config, Guard as _, SeqNo, SequenceNumberCounter};
use std::collections::BTreeSet;
use std::sync::Arc;

fn key(epoch: u32, i: u32) -> Vec<u8> {
    let mut key = epoch.to_be_bytes().to_vec();
    key.extend_from_slice(&i.to_be_bytes());
    key
}

fn compact_all(tree: &AnyTree, watermark: SeqNo) -> lsm_tree::Result<()> {
    let strategy = Arc::new(Leveled::default());
    while tree.compact(strategy.clone(), watermark)?.action != CompactionAction::Nothing {}
    Ok(())
}

fn epochs_visible(tree: &AnyTree) -> lsm_tree::Result<BTreeSet<u32>> {
    tree.iter(SeqNo::MAX, None)
        .map(|item| {
            let (k, _) = item.into_inner()?;
            Ok(u32::from_be_bytes(k[..4].try_into().expect("epoch")))
        })
        .collect()
}

/// Epochs are written in order, flushed and compacted as they go; each one
/// older than the retention is deleted with a range tombstone over it,
/// flushed and compacted at once. Only the retained epochs may read back.
#[test]
fn an_expired_range_stays_expired_through_leveled_compaction() -> lsm_tree::Result<()> {
    const EPOCHS: u32 = 24;
    const ROWS: u32 = 3_000;
    const RETENTION: u32 = 3;

    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(
        folder.path(),
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .table_target_size(64 * 1_024)
    .open()?;
    for epoch in 0..EPOCHS {
        if let Some(expired) = epoch.checked_sub(RETENTION + 1) {
            tree.remove_range(
                expired.to_be_bytes().to_vec(),
                (expired + 1).to_be_bytes().to_vec(),
                seqno.next(),
            );
            tree.flush_active_memtable(seqno.get())?;
            compact_all(&tree, seqno.get())?;
        }
        for i in 0..ROWS {
            tree.insert(key(epoch, i), [b'v'; 100], seqno.next());
            if i % 1_000 == 999 {
                tree.flush_active_memtable(seqno.get())?;
                compact_all(&tree, seqno.get())?;
            }
        }
    }

    let expected: BTreeSet<u32> = (EPOCHS - RETENTION - 1..EPOCHS).collect();
    assert_eq!(
        epochs_visible(&tree)?,
        expected,
        "only retained epochs read back"
    );
    Ok(())
}

/// A range tombstone of its own in L0 is compacted with newer rows elsewhere
/// in the key space while the rows it deletes sit in the last level, at a
/// watermark that keeps every version: the rows stay deleted for a scan and
/// a point read.
#[test]
fn a_tombstone_merged_away_from_the_rows_it_deletes_keeps_them_deleted() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(
        folder.path(),
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..100 {
        tree.insert(key(7, i), b"old", seqno.next());
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;

    // Newer rows of epoch 30, then the tombstone over epoch 7, in L0.
    for flush in 0..3 {
        for i in 0..100 {
            tree.insert(key(30, flush * 100 + i), b"new", seqno.next());
        }
        tree.flush_active_memtable(0)?;
    }
    tree.remove_range(
        7u32.to_be_bytes().to_vec(),
        8u32.to_be_bytes().to_vec(),
        seqno.next(),
    );
    tree.flush_active_memtable(0)?;

    // L0 into L1 only: a watermark of 0 keeps every version, so nothing is
    // garbage-collected and the tombstone must be carried.
    let strategy = Arc::new(Leveled::default().with_l0_threshold(2));
    tree.compact(strategy, 0)?;

    assert_eq!(
        epochs_visible(&tree)?,
        BTreeSet::from([30]),
        "epoch 7 stays deleted",
    );
    assert!(tree.get(key(7, 0), SeqNo::MAX)?.is_none(), "point read too");
    Ok(())
}

/// 100 bytes no codec shrinks much.
fn noisy(epoch: u32, series: u32, seq: u32) -> [u8; 100] {
    let mut s = (u64::from(epoch) << 40 ^ u64::from(series) << 20 ^ u64::from(seq))
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        | 1;
    let mut out = [0; 100];
    for byte in &mut out {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        *byte = (s >> 56) as u8;
    }
    out
}

/// The same with default table sizes, memtables flushed at 4 MiB, rows
/// interleaved across 64 series, and a share of each epoch arriving during
/// the next one.
#[test]
fn an_expired_range_stays_expired_with_interleaved_series_and_late_rows() -> lsm_tree::Result<()> {
    const EPOCHS: u32 = 30;
    const ROWS: u32 = 20_000;
    const RETENTION: u32 = 12;
    const SERIES: u32 = 64;
    const LATE_EVERY: u32 = 100;

    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(
        folder.path(),
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let settle = |tree: &AnyTree| -> lsm_tree::Result<()> {
        tree.flush_active_memtable(seqno.get())?;
        compact_all(tree, seqno.get())
    };
    let insert = |tree: &AnyTree, epoch: u32, row: u32| -> lsm_tree::Result<()> {
        let (series, seq) = (row % SERIES, row / SERIES);
        let mut k = key(epoch, series);
        k.extend_from_slice(&seq.to_be_bytes());
        tree.insert(k, noisy(epoch, series, seq), seqno.next());
        if tree.active_memtable().size() >= 4 * 1_024 * 1_024 {
            settle(tree)?;
        }
        Ok(())
    };
    for epoch in 0..EPOCHS {
        if let Some(expired) = epoch.checked_sub(RETENTION + 1) {
            tree.remove_range(
                expired.to_be_bytes().to_vec(),
                (expired + 1).to_be_bytes().to_vec(),
                seqno.next(),
            );
            settle(&tree)?;
        }
        let delays = epoch + 1 < EPOCHS;
        for t in 0..ROWS {
            if t % LATE_EVERY == 0
                && let Some(previous) = epoch.checked_sub(1)
                && t + LATE_EVERY - 1 < ROWS
            {
                insert(&tree, previous, t + LATE_EVERY - 1)?;
            }
            if delays && t % LATE_EVERY == LATE_EVERY - 1 {
                continue;
            }
            insert(&tree, epoch, t)?;
        }
        settle(&tree)?;
    }

    let expected: BTreeSet<u32> = (EPOCHS - RETENTION - 1..EPOCHS).collect();
    assert_eq!(
        epochs_visible(&tree)?,
        expected,
        "only retained epochs read back"
    );
    Ok(())
}
