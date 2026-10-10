// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::KeyBounds;
use crate::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, UserKey};
use core::cell::Cell;
use core::ops::Bound;

std::thread_local! {
    /// The most candidates a carry queue held at once on this thread: a
    /// serial compaction scans and records on the thread that runs it, and
    /// tests running alongside on other threads leave the figure alone.
    static PEAK_QUEUE: Cell<usize> = const { Cell::new(0) };

    /// The bytes of row groups read raw to be copied, on this thread.
    static RAW_READ: Cell<u64> = const { Cell::new(0) };
}

pub(super) fn note_queue_len(len: usize) {
    PEAK_QUEUE.with(|peak| peak.set(peak.get().max(len)));
}

pub(super) fn note_raw_read(len: u32) {
    RAW_READ.with(|read| read.set(read.get() + u64::from(len)));
}

/// The inputs' groups lack the per-column statistics an output keeping a
/// zone map needs, so none can be copied whole: none is read raw only to be
/// refused, and every row is written and readable.
#[test]
fn groups_without_statistics_are_not_read_for_an_output_keeping_a_zone_map() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?
    else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = false;
        cfg.data_block_compression_policy =
            crate::config::CompressionPolicy::all(crate::CompressionType::None);
    })?;
    let key = |i: u32| format!("k{i:06}").into_bytes();
    let mut seqno: SeqNo = 1;
    for range in [0..2_000u32, 2_000..4_000] {
        for i in range {
            tree.insert(key(i), vec![b'v'; 64], seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    tree.update_runtime_config(|cfg| cfg.zone_map = true)?;

    RAW_READ.with(|read| read.set(0));
    tree.major_compact(64 * 1024 * 1024, 0)?;
    let read = RAW_READ.with(Cell::get);

    assert_eq!(read, 0, "{read} bytes of groups read raw and refused");
    assert_eq!(tree.iter(SeqNo::MAX, None).count(), 4_000);
    Ok(())
}

/// A compaction that relocates blob files writes every row itself, so its
/// scans record no group: none is held for a copy that cannot happen.
#[cfg(feature = "metrics")]
#[test]
fn relocating_compaction_records_no_group() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let AnyTree::Blob(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::KvSeparationOptions::default().age_cutoff(1.0)))
    .blob_compression(crate::CompressionType::None)
    .open()?
    else {
        panic!("expected blob tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.data_block_compression_policy =
            crate::config::CompressionPolicy::all(crate::CompressionType::None);
    })?;
    let big = b"neptune!".repeat(128_000);
    let key = |i: u32| format!("k{i:06}").into_bytes();
    tree.insert("big", &big, 0);
    tree.insert("big2", &big, 0);
    for i in 0..2_000 {
        tree.insert(key(i), vec![b'v'; 64], 0);
    }
    tree.flush_active_memtable(0)?;
    tree.insert("big", b"winter!".repeat(128_000), 1);
    tree.flush_active_memtable(0)?;
    // Records the stale value; the next compaction rewrites its file.
    tree.major_compact(64_000_000, 1_000)?;
    assert_eq!(tree.metrics().blob_bytes_relocated(), 0);

    PEAK_QUEUE.with(|peak| peak.set(0));
    tree.major_compact(64_000_000, 1_000)?;
    let peak = PEAK_QUEUE.with(Cell::get);

    assert!(
        tree.metrics().blob_bytes_relocated() > 0,
        "the compaction relocated"
    );
    assert_eq!(peak, 0, "{peak} groups held for a relocating compaction");
    assert_eq!(
        tree.get("big2", SeqNo::MAX)?.as_deref(),
        Some(big.as_slice())
    );
    // `big`, `big2` and the 2 000 small keys.
    assert_eq!(tree.iter(SeqNo::MAX, None).count(), 2_002);
    Ok(())
}

/// A merge that drops every row emits nothing, so nothing it emits retires
/// the groups its inputs' scans record: they are retired as the scans move
/// on, and the queue holds a group or two per input rather than the input.
#[test]
fn carry_queue_stays_bounded_when_the_merge_drops_everything() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?
    else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.data_block_compression_policy =
            crate::config::CompressionPolicy::all(crate::CompressionType::None);
    })?;
    let key = |i: u32| format!("k{i:06}").into_bytes();
    let mut seqno: SeqNo = 1;
    for range in [0..2_000u32, 2_000..4_000] {
        for i in range {
            tree.insert(key(i), vec![b'v'; 64], seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    let groups: u64 = tree
        .current_version()
        .iter_tables()
        .map(|t| t.metadata.data_block_count)
        .sum();
    tree.remove_range(key(0), key(5_000), seqno);
    tree.flush_active_memtable(0)?;

    PEAK_QUEUE.with(|peak| peak.set(0));
    tree.major_compact(64 * 1024 * 1024, SeqNo::MAX)?;
    let peak = PEAK_QUEUE.with(Cell::get) as u64;

    assert_eq!(tree.iter(SeqNo::MAX, None).count(), 0);
    assert!(groups > 8, "the inputs span many groups: {groups}");
    assert!(peak <= 4, "{peak} of {groups} groups held at once");
    Ok(())
}

fn bounds(lo: Bound<&str>, hi: Bound<&str>) -> KeyBounds {
    KeyBounds {
        lo: lo.map(|k| UserKey::from(k.as_bytes())),
        hi: hi.map(|k| UserKey::from(k.as_bytes())),
        comparator: crate::comparator::default_comparator(),
    }
}

/// Each bound keeps exactly the keys its kind names: an included end keeps
/// the key itself, an excluded one drops it, an unbounded one keeps all.
#[test]
fn key_bounds_contains_follows_each_bound_kind() {
    let included = bounds(Bound::Included("b"), Bound::Included("d"));
    assert!(!included.contains(b"a"));
    assert!(included.contains(b"b"));
    assert!(included.contains(b"d"));
    assert!(!included.contains(b"e"));

    let excluded = bounds(Bound::Excluded("b"), Bound::Excluded("d"));
    assert!(!excluded.contains(b"b"));
    assert!(excluded.contains(b"c"));
    assert!(!excluded.contains(b"d"));

    let open = bounds(Bound::Unbounded, Bound::Unbounded);
    assert!(open.contains(b""));
    assert!(open.contains(b"zzz"));
}
