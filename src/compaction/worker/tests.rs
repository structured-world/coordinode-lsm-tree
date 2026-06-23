use super::{create_compaction_stream, pick_run_indexes};
use crate::{
    AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter, Table, TableId,
    compaction::{Choice, CompactionStrategy, Input, state::CompactionState},
    config::BlockSizePolicy,
    version::Version,
};
use std::sync::Arc;
use test_log::test;

/// Ranks keys by their first byte only, so byte-distinct keys that share a
/// first byte compare equal — exercises the comparator-aware dedup path that
/// raw `dedup()` would miss.
struct FirstByteComparator;
impl crate::comparator::UserComparator for FirstByteComparator {
    fn name(&self) -> &'static str {
        "test-first-byte"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
        a.first().cmp(&b.first())
    }
}

#[test]
fn boundary_candidates_dedups_comparator_equal_keys() {
    let cmp: crate::comparator::SharedComparator = Arc::new(FirstByteComparator);
    // "a1" and "a2" are byte-distinct but compare equal under the first-byte
    // comparator; "b1" is in a different group. Raw dedup() would keep both
    // a-keys (not byte-identical) and, after popping the global max, leave
    // two boundaries in the "a" group → overlapping sub-compaction ranges.
    let keys = vec![
        crate::UserKey::from("a1"),
        crate::UserKey::from("a2"),
        crate::UserKey::from("b1"),
    ];
    let out = super::boundary_candidates(keys, &cmp);
    assert_eq!(
        out.len(),
        1,
        "comparator-equal keys must collapse to a single boundary candidate",
    );
    assert_eq!(
        out.first().and_then(|k| k.first()),
        Some(&b'a'),
        "the surviving boundary should be from the deduped a-group",
    );
}

/// A failing sub-compaction range must abort the whole compaction, roll
/// back the finalized files of the ranges that DID succeed, and restore the
/// hidden input tables — leaving the tree fully readable with nothing
/// partially installed. Drives the parallel rollback path via the test
/// failpoint (one range errors, its siblings succeed and are rolled back).
#[cfg(feature = "parallel")]
#[test]
fn failed_subcompaction_rolls_back_and_restores_inputs() -> crate::Result<()> {
    use core::sync::atomic::Ordering;

    const N: u64 = 4_000;
    let key = |i: u64| format!("key_{i:08}");
    let val = |i: u64, generation: u64| format!("g{generation}-{i}-{}", "x".repeat(40));

    let dir = tempfile::tempdir()?;
    let config = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .compaction_threads(4)
    .subcompaction_min_bytes(0)
    // KV separation so the surviving sub-compactions also produce blob
    // files, exercising the blob-file arm of the rollback as well.
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ));
    // Share the failpoint handle before the config is consumed by open().
    let failpoint = config.fail_one_subcompaction.clone();
    let tree = config.open()?;

    // Populate the bottom level with several tables (the split boundaries).
    for i in 0..N {
        tree.insert(key(i), val(i, 0), i);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(4_096, 0)?;

    // Overwrite the whole keyspace into L0; the next compaction merges it
    // into the populated bottom and splits into parallel sub-compactions.
    for i in 0..N {
        tree.insert(key(i), val(i, 1), N + i);
    }
    tree.flush_active_memtable(0)?;
    let tables_before = tree.table_count();

    // Arm: exactly one sub-compaction range will error.
    failpoint.store(true, Ordering::SeqCst);
    let result = tree.major_compact(u64::MAX, 0);

    assert!(
        result.is_err(),
        "a failing sub-compaction range must abort the compaction",
    );
    assert!(
        !failpoint.load(Ordering::SeqCst),
        "the failpoint should have fired and disarmed itself",
    );
    assert_eq!(
        tree.table_count(),
        tables_before,
        "rollback must leave nothing partially installed",
    );
    for i in 0..N {
        assert_eq!(
            tree.get(key(i), crate::MAX_SEQNO)?.as_deref(),
            Some(val(i, 1).as_bytes()),
            "value for {} must survive the rolled-back compaction",
            key(i),
        );
    }
    Ok(())
}

/// A tight-space compaction that crashes after durably installing and
/// punching its first slice must reopen consistently: the manifest carries
/// the input's persisted key-range restriction, and recovery rebuilds the
/// restricted view so every key (those in the installed slice output AND
/// those still in the punched input's intact suffix) reads back.
#[test]
fn tight_space_crash_after_first_slice_recovers_all_keys_on_reopen() -> crate::Result<()> {
    use core::sync::atomic::Ordering;

    const N: u64 = 2_000;
    let k = |i: u64| format!("key{i:08}");

    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let config = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::new(mem.clone()));
    let failpoint = config.fail_tight_after_first_slice.clone();
    let tree = match config.open()? {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    for i in 0..N {
        tree.insert(k(i).as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    let used = tree.storage_stats()?.used_bytes;

    // Force the single-table major compaction to be gated, and opt in to
    // tight-space reclaim.
    mem.set_capacity(used + used / 4);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;

    // Crash right after the first slice is durably installed + punched.
    failpoint.store(true, Ordering::SeqCst);
    assert!(
        tree.major_compact(64 * 1024 * 1024, 0).is_err(),
        "the crash failpoint must abort the tight-space compaction",
    );
    assert!(
        !failpoint.load(Ordering::SeqCst),
        "the failpoint should have fired and disarmed",
    );
    assert!(
        mem.punched_bytes() > 0,
        "the first slice must have punched before the crash",
    );

    // Reopen on the same simulated disk: recovery must rebuild the restricted
    // input from the persisted manifest restriction.
    drop(tree);
    let reopened = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem))
    .open()?;
    for i in 0..N {
        assert!(
            reopened.get(k(i).as_bytes(), crate::MAX_SEQNO)?.is_some(),
            "key {i} lost after a crash mid tight-space compaction + reopen",
        );
    }
    Ok(())
}

/// A KV-separated tight-space compaction that RELOCATES a fragmented blob
/// file in slices and crashes after the first slice must reopen consistently:
/// the relocated entries (now in fresh compact files referenced by the
/// installed slice output) AND the not-yet-relocated entries (still in the
/// punched stale file's intact suffix) must all read their latest value.
#[test]
fn tight_space_blob_relocation_crash_after_first_slice_recovers_all_keys() -> crate::Result<()> {
    use core::sync::atomic::Ordering;

    const N: u64 = 4_000;
    let k = |i: u64| format!("key{i:08}");
    // High-entropy (xorshift) values so the blobs do NOT compress away: the
    // relocation transient must be real for the space gate to skip the full
    // merge and engage the slicing path. Deterministic per (key, generation)
    // so the post-reopen assertion can regenerate the expected bytes. Odd keys
    // keep their first-generation value (a relocated live blob); even keys are
    // overwritten (their first-gen blob is dead → in-file fragmentation).
    let val = |i: u64, generation: u8| -> Vec<u8> {
        let mut s = (i + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (u64::from(generation) << 1);
        (0..200u32)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "xorshift byte extraction; the high bits are intentionally dropped"
                )]
                let byte = (s >> 24) as u8;
                byte
            })
            .collect()
    };

    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let config = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::new(mem.clone()))
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(64)
            // Keep every stale file (default age_cutoff 0.25 would drain a
            // small candidate set to empty) and treat a lightly-dead file as
            // stale so the half-shadowed first generation is relocated.
            .age_cutoff(1.0)
            .staleness_threshold(0.1)
            // Small blob files → several per generation, so relocation has
            // multiple stale files and the merge slices across them.
            .file_target_size(48 * 1024),
    ));
    let failpoint = config.fail_tight_after_first_slice.clone();
    let tree = match config.open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };

    // Generation 1: every key → a blob.
    for i in 0..N {
        tree.insert(k(i).as_bytes(), val(i, 1), i);
    }
    tree.flush_active_memtable(0)?;
    // Generation 2: overwrite EVEN keys only, interleaved so every gen-1 blob
    // file ends up ~half dead (stale, not fully dead → eligible to relocate).
    for i in (0..N).step_by(2) {
        tree.insert(k(i).as_bytes(), val(i, 2), N + i);
    }
    tree.flush_active_memtable(0)?;

    // Blob fragmentation is only LEARNED during a merge (the drop callback
    // records each shadowed gen-1 blob as dead). Run one ample-space merge
    // first so the even-key gen-1 blobs are counted dead, leaving every gen-1
    // file ~half stale — the precondition for the next merge to RELOCATE them.
    // It also collapses the index SSTs to the bottom level. The watermark sits
    // above every live seqno so the merge actually folds the shadowed entries
    // (seqno 0 keeps all MVCC versions and records no fragmentation).
    let gc_watermark = 4 * N;
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    tree.major_compact(64 * 1024 * 1024, gc_watermark)?;

    let used = tree.storage_stats()?.used_bytes;

    // Cap so the full relocation of the now-stale generation cannot fit,
    // forcing the gate to skip and the tight loop to relocate in slices.
    mem.set_capacity(used + used / 4);
    tree.index.update_runtime_config(|c| {
        c.tight_space_compaction = true;
    })?;

    // Crash right after the first relocated slice is durably installed +
    // punched.
    failpoint.store(true, Ordering::SeqCst);
    assert!(
        tree.major_compact(64 * 1024 * 1024, gc_watermark).is_err(),
        "the crash failpoint must abort the relocating tight-space compaction",
    );
    assert!(
        !failpoint.load(Ordering::SeqCst),
        "the failpoint should have fired and disarmed",
    );
    assert!(
        mem.punched_bytes() > 0,
        "the first relocated slice must have punched a stale blob prefix",
    );

    // Reopen on the same simulated disk and verify every key reads its latest
    // value: odd keys = relocated gen-1 blob, even keys = gen-2 blob.
    drop(tree);
    let reopened = match Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(64),
    ))
    .with_shared_fs(Arc::new(mem))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    for i in 0..N {
        let expected = if i % 2 == 0 { val(i, 2) } else { val(i, 1) };
        assert_eq!(
            reopened.get(k(i).as_bytes(), crate::MAX_SEQNO)?.as_deref(),
            Some(expected.as_slice()),
            "key {i} wrong/lost after a crash mid blob-relocation + reopen",
        );
    }
    Ok(())
}

/// A range tombstone fully below the GC watermark must be applied during a
/// last-level compaction: its covered keys are physically dropped AND the
/// tombstone itself is GC'd. If the keys were only suppressed (not dropped)
/// while the tombstone was GC'd, they would resurrect — so a `None` read
/// after GC proves both the physical drop (#1) and the tombstone GC (#2).
///
/// Routed through the atomic sub-compaction path (which is where GC runs):
/// `compaction_threads > 1` + `subcompaction_min_bytes = 0` + a populated
/// bottom level (split boundaries) make the final compaction split.
#[test]
fn last_level_applies_and_gcs_below_watermark_range_tombstone() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .compaction_threads(4)
    .subcompaction_min_bytes(0)
    .open()?;

    let key = |i: u64| format!("k{i:04}");
    let val = |i: u64| format!("v{i}-{}", "x".repeat(40));

    // Step 1: populate the bottom level with several tables (split
    // boundaries the final compaction can partition on).
    for i in 0..200u64 {
        tree.insert(key(i), val(i), i);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(4_096, 0)?;

    // Delete [k0000, k0050) at seqno 1000 and overwrite the rest into L0, so
    // the next compaction merges L0 into the populated bottom and splits.
    tree.remove_range(
        crate::UserKey::from("k0000"),
        crate::UserKey::from("k0050"),
        1000,
    );
    for i in 50..200u64 {
        tree.insert(key(i), val(i), 1001 + i);
    }
    tree.flush_active_memtable(0)?;

    // Compact to the bottom with a watermark (5000) above the tombstone:
    // covered keys are physically dropped and the tombstone is GC'd.
    tree.major_compact(u64::MAX, 5000)?;

    for i in 0..50u64 {
        assert_eq!(
            tree.get(key(i), crate::MAX_SEQNO)?,
            None,
            "covered key {} must be physically gone after GC",
            key(i),
        );
    }
    for i in 50..200u64 {
        assert!(
            tree.get(key(i), crate::MAX_SEQNO)?.is_some(),
            "uncovered key {} must survive",
            key(i),
        );
    }
    let remaining = super::collect_version_tombstones(&tree.current_version());
    assert!(
        remaining.is_empty(),
        "a fully-applied below-watermark tombstone must be GC'd, found {remaining:?}",
    );
    Ok(())
}

/// An above-watermark tombstone must be retained, not GC'd: read-time
/// application still needs it for snapshots that predate the tombstone.
#[test]
fn above_watermark_range_tombstone_is_retained() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let key = |i: u64| format!("k{i:04}");
    for i in 0..50u64 {
        tree.insert(key(i), "v", i);
    }
    tree.flush_active_memtable(0)?;

    // Tombstone at seqno 100; compact with a watermark (50) BELOW it, so the
    // tombstone is neither applied nor GC'd.
    tree.remove_range(
        crate::UserKey::from("k0000"),
        crate::UserKey::from("k0025"),
        100,
    );
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 50)?;

    let remaining = super::collect_version_tombstones(&tree.current_version());
    assert!(
        !remaining.is_empty(),
        "an above-watermark tombstone must be retained, not GC'd",
    );
    Ok(())
}

/// A range tombstone whose seqno equals the GC watermark sits exactly on the
/// visibility boundary. RT visibility is strict (`visible_at` is `seqno <
/// read_seqno`), so the oldest live snapshot reading at `read_seqno ==
/// watermark` does NOT see `RT@watermark`. Compaction must therefore neither
/// apply it (physically dropping covered keys) nor GC it — doing either one
/// compaction too early makes a key that is still visible at the watermark
/// disappear. Reading the covered key at `read_seqno == watermark` (where the
/// tombstone is invisible but the key is committed) must still return it.
#[test]
fn range_tombstone_at_exact_watermark_is_not_applied_or_gced() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .compaction_threads(4)
    .subcompaction_min_bytes(0)
    .open()?;

    let key = |i: u64| format!("k{i:04}");
    let val = |i: u64| format!("v{i}-{}", "x".repeat(40));

    // Populate the bottom level (split boundaries for the final compaction).
    // Covered keys live here at low seqnos (< the watermark).
    for i in 0..200u64 {
        tree.insert(key(i), val(i), i);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(4_096, 0)?;

    // Delete [k0000, k0050) at seqno 1000 and overwrite the rest into L0.
    tree.remove_range(
        crate::UserKey::from("k0000"),
        crate::UserKey::from("k0050"),
        1000,
    );
    for i in 50..200u64 {
        tree.insert(key(i), val(i), 1001 + i);
    }
    tree.flush_active_memtable(0)?;

    // Compact to the bottom with the watermark set EXACTLY to the tombstone's
    // seqno. At this boundary the tombstone is invisible to a read at the
    // watermark, so its covered keys must be preserved, not dropped.
    tree.major_compact(u64::MAX, 1000)?;

    // Read at read_seqno == watermark: RT@1000 is invisible here
    // (`1000 < 1000` is false), and each covered key was committed at
    // seqno < 1000, so it must still be visible.
    for i in 0..50u64 {
        assert_eq!(
            tree.get(key(i), 1000)?.as_deref(),
            Some(val(i).as_bytes()),
            "covered key {} must survive: RT@watermark is invisible at read==watermark",
            key(i),
        );
    }

    // The boundary tombstone must also be retained (not GC'd one compaction
    // early), since snapshots at the watermark still rely on it.
    let remaining = super::collect_version_tombstones(&tree.current_version());
    assert!(
        !remaining.is_empty(),
        "a tombstone at the exact watermark must be retained, not GC'd",
    );
    Ok(())
}

#[test]
fn compaction_stream_run_not_found() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;

    assert!(
        create_compaction_stream(
            &tree.current_version(),
            &[666],
            0,
            None,
            crate::comparator::default_comparator()
        )?
        .is_none()
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn compaction_stream_run() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("b", "b", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("c", "c", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        Some((0, 2)),
        pick_run_indexes(
            tree.current_version()
                .level(0)
                .unwrap()
                .iter()
                .next()
                .unwrap(),
            &[0, 1, 2],
        )
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn compaction_stream_run_2() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("b", "b", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("c", "c", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        Some((0, 0)),
        pick_run_indexes(
            tree.current_version()
                .level(0)
                .unwrap()
                .iter()
                .next()
                .unwrap(),
            &[0],
        )
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn compaction_stream_run_3() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("b", "b", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("c", "c", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        Some((2, 2)),
        pick_run_indexes(
            tree.current_version()
                .level(0)
                .unwrap()
                .iter()
                .next()
                .unwrap(),
            &[2],
        )
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn compaction_stream_run_4() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("b", "b", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("c", "c", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        None,
        pick_run_indexes(
            tree.current_version()
                .level(0)
                .unwrap()
                .iter()
                .next()
                .unwrap(),
            &[4],
        )
    );

    Ok(())
}

#[test]
fn compaction_drop_tables() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.approximate_len());
    assert_eq!(0, tree.sealed_memtable_count());

    tree.insert("b", "a", 1);
    tree.flush_active_memtable(0)?;
    assert_eq!(2, tree.approximate_len());
    assert_eq!(0, tree.sealed_memtable_count());

    tree.insert("c", "a", 2);
    tree.flush_active_memtable(0)?;
    assert_eq!(3, tree.approximate_len());
    assert_eq!(0, tree.sealed_memtable_count());

    tree.compact(Arc::new(crate::compaction::Fifo::new(1, None)), 3)?;

    assert_eq!(0, tree.table_count());

    Ok(())
}

#[test]
fn blob_file_picking_simple() -> crate::Result<()> {
    struct InPlaceStrategy(Vec<TableId>);

    impl CompactionStrategy for InPlaceStrategy {
        fn get_name(&self) -> &'static str {
            "InPlaceCompaction"
        }

        fn choose(&self, _: &Version, _: &Config, _: &CompactionState) -> Choice {
            Choice::Merge(Input {
                table_ids: self.0.iter().copied().collect(),
                dest_level: 6,
                target_size: 64_000_000,
                canonical_level: 6, // We don't really care - this compaction is only used for very specific unit tests
            })
        }
    }

    let folder = tempfile::tempdir()?;

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1))
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(1)
            .age_cutoff(1.0)
            .staleness_threshold(0.01)
            .compression(crate::CompressionType::None),
    ))
    .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);
    tree.flush_active_memtable(1_000)?;
    assert_eq!(0, tree.sealed_memtable_count());
    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    tree.major_compact(1, 1_000)?;
    assert_eq!(3, tree.table_count());
    assert_eq!(1, tree.blob_file_count());
    // We now have tables [1, 2, 3] pointing into blob file 0

    tree.drop_range("a"..="a")?;
    assert_eq!(2, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    {
        assert_eq!(
            &{
                let mut map = crate::HashMap::default();
                map.insert(0, crate::blob_tree::FragmentationEntry::new(1, 1, 1));
                map
            },
            &**tree.current_version().gc_stats(),
        );
    }

    // Even though we are compacting table #2, blob file is not rewritten
    // because table #3 still points into it
    tree.compact(Arc::new(InPlaceStrategy(vec![2])), 1_000)?;
    assert_eq!(2, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    {
        assert_eq!(
            &{
                let mut map = crate::HashMap::default();
                map.insert(0, crate::blob_tree::FragmentationEntry::new(1, 1, 1));
                map
            },
            &**tree.current_version().gc_stats(),
        );
    }

    // Because tables #3 & #4 both point into the blob file
    // Only selecting both for compaction will actually rewrite the file
    tree.compact(Arc::new(InPlaceStrategy(vec![3, 4])), 1_000)?;
    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    // Fragmentation is cleared up because blob file was relocated
    {
        assert_eq!(
            crate::HashMap::default(),
            **tree.current_version().gc_stats(),
        );
    }

    Ok(())
}

#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test asserts over known-good fixtures; failure surfaces via panic"
)]
#[test]
fn narrow_merge_candidates_for_full_run_are_adjacent_pairs_sorted_ascending() -> crate::Result<()> {
    // Build a single bottom-level run of several tables (small target size
    // forces table rotation), then enumerate the narrowing candidates of a
    // whole-run merge. The gate tries them in order, so the contract is:
    // every candidate is a run-adjacent pair, and they are sorted by combined
    // SST size ascending (smallest tried first).
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .open()?;
    for i in 0..3_000u64 {
        tree.insert(format!("k{i:08}"), "v".repeat(40), i);
    }
    tree.flush_active_memtable(0)?;
    // Small target size → the major compaction emits a run of several tables.
    tree.major_compact(16 * 1024, 0)?;

    let version = tree.current_version();
    let run = version
        .iter_levels()
        .flat_map(|level| level.iter())
        .find(|run| run.len() >= 3)
        .expect("a bottom-level run with >= 3 tables");
    let ordered: Vec<(TableId, u64)> = run.iter().map(|t| (t.id(), t.file_size())).collect();

    let payload = Input {
        table_ids: ordered.iter().map(|(id, _)| *id).collect(),
        dest_level: 6,
        canonical_level: 6,
        target_size: 64 * 1024 * 1024,
    };

    let candidates = super::narrow_merge_candidates(&version, &payload);

    // One candidate per run-adjacent pair, each exactly two tables on the
    // payload's destination.
    assert_eq!(
        candidates.len(),
        ordered.len() - 1,
        "one candidate per run-adjacent pair"
    );
    for c in &candidates {
        assert_eq!(c.table_ids.len(), 2, "each candidate is an adjacent pair");
        assert_eq!(c.dest_level, 6, "destination preserved");
    }

    let combined = |c: &Input| -> u64 {
        c.table_ids
            .iter()
            .filter_map(|id| version.get_table(*id))
            .map(Table::file_size)
            .sum()
    };
    let sums: Vec<u64> = candidates.iter().map(combined).collect();

    // Sorted ascending: the gate tries the smallest-Σ pair first, then larger
    // ones (a larger pair with fewer blob rewrites can fit where the smallest
    // does not).
    let mut sorted = sums.clone();
    sorted.sort_unstable();
    assert_eq!(sums, sorted, "candidates sorted ascending by SST size");

    // The first candidate is the smallest-Σ run-adjacent pair.
    let smallest_pair = ordered
        .windows(2)
        .map(|w| w[0].1 + w[1].1)
        .min()
        .expect(">= 2 tables");
    assert_eq!(sums[0], smallest_pair, "smallest-Σ pair is tried first");

    Ok(())
}

#[test]
fn space_fits_two_layer_combines_shared_volume_outputs_and_separates_routed_ones()
-> crate::Result<()> {
    use crate::fs::MemFs;

    const MIB: u64 = 1024 * 1024;
    let dir = tempfile::tempdir()?;

    // Single volume (no routes): the SST tables folder and the blobs folder
    // share the primary filesystem, so the transient peak is their SUM on one
    // volume. An empty MemFs reports `capacity` free.
    let cfg = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(MemFs::with_capacity(100 * MIB)));

    // 60 + 60 = 120 MiB > 100 MiB free → rejected, even though each output
    // fits the volume alone. This is the single-volume over-admission the
    // two-layer model prevents (checking sst and blob independently would
    // wrongly admit it).
    assert!(
        !super::space_fits_two_layer(&cfg, u64::MAX, 60 * MIB, 6, 60 * MIB),
        "shared-volume outputs must be summed, not checked independently"
    );
    // 60 + 30 = 90 MiB (+1 MiB reserve) ≤ 100 MiB → admitted.
    assert!(super::space_fits_two_layer(
        &cfg,
        u64::MAX,
        60 * MIB,
        6,
        30 * MIB
    ));

    // Layer 1 (logical quota) caps the total regardless of physical free:
    // 50 + 40 = 90 MiB exceeds an 80 MiB quota headroom.
    assert!(!super::space_fits_two_layer(
        &cfg,
        80 * MIB,
        50 * MIB,
        6,
        40 * MIB
    ));

    // Routed to a PROVEN-independent volume: level 6 lives on its own MemFs
    // (a distinct volume id), blobs on the primary MemFs. The two outputs are
    // checked independently — 60 MiB on each of two 100 MiB volumes fits, even
    // though the sum is 120 MiB (a full cold-tier route must not stall a hot
    // merge).
    let routed = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(MemFs::with_capacity(100 * MIB)))
    .level_routes(vec![crate::config::LevelRoute {
        levels: 6..7,
        path: crate::path::PathBuf::from("/cold-tier"),
        fs: Arc::new(MemFs::with_capacity(100 * MIB)),
    }]);
    assert!(
        super::space_fits_two_layer(&routed, u64::MAX, 60 * MIB, 6, 60 * MIB),
        "proven-independent volumes are checked independently"
    );
    // A blob output that overflows the primary volume alone still fails.
    assert!(!super::space_fits_two_layer(
        &routed,
        u64::MAX,
        60 * MIB,
        6,
        130 * MIB
    ));

    // Routed but NOT proven independent: the route points at the SAME backend
    // as the primary (one shared MemFs → one volume id / one free-space pool),
    // as happens when level_routes maps a level to a directory on the same
    // mount. The SST and blob budgets must combine, so 60 + 60 = 120 MiB > 100
    // MiB free is rejected even though each fits alone — the routed
    // over-admission guard.
    // ONE `Arc<MemFs>` reused for both config slots so the primary and the
    // route are unambiguously the same backend (one volume id / one
    // free-space pool), the not-proven-independent case.
    let shared: Arc<dyn crate::fs::Fs> = Arc::new(MemFs::with_capacity(100 * MIB));
    let routed_same_mount = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&shared))
    .level_routes(vec![crate::config::LevelRoute {
        levels: 6..7,
        path: crate::path::PathBuf::from("/same-mount-subdir"),
        fs: Arc::clone(&shared),
    }]);
    assert!(
        !super::space_fits_two_layer(&routed_same_mount, u64::MAX, 60 * MIB, 6, 60 * MIB),
        "a route on the same volume must combine budgets, not admit each independently"
    );

    Ok(())
}

#[expect(
    clippy::expect_used,
    reason = "test asserts over known-good fixtures; failure surfaces via panic"
)]
#[test]
fn space_gate_for_merge_narrows_a_full_run_that_exceeds_free() -> crate::Result<()> {
    use crate::fs::MemFs;

    // Build a multi-table bottom-level run on a capped simulated disk, then
    // ask the gate to admit a whole-run merge whose transient output does NOT
    // fit free space but a run-adjacent pair does. The gate must narrow rather
    // than skip — exercising the per-payload demand, the candidate loop, and
    // the `Narrowed` return that integration tests cannot reach (the public
    // major-compaction path picks a non-narrowable multi-level merge).
    let dir = tempfile::tempdir()?;
    let mem = MemFs::with_capacity(u64::MAX);
    let any = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem.clone()))
    .data_block_size_policy(BlockSizePolicy::all(512))
    .open()?;
    let crate::AnyTree::Standard(tree) = any else {
        panic!("expected Standard tree");
    };
    for i in 0..3_000u64 {
        tree.insert(format!("k{i:08}"), "v".repeat(40), i);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(16 * 1024, 0)?;

    let version = tree.current_version();
    let run = version
        .iter_levels()
        .flat_map(|level| level.iter())
        .find(|run| run.len() >= 3)
        .expect("a bottom-level run with >= 3 tables");
    let run_sigma: u64 = run.iter().map(Table::file_size).sum();
    let payload = Input {
        table_ids: run.iter().map(Table::id).collect(),
        dest_level: 6,
        canonical_level: 6,
        target_size: 64 * 1024 * 1024,
    };

    // Free space below the full run's Σ but above a single pair: the run does
    // not fit, a run-adjacent pair does. Calibrate against the SIMULATED
    // disk's real stored bytes (manifest / WAL count too, not just live SSTs),
    // since the gate probes `available_space`, not the version footprint.
    // `run_sigma >= 1` (real SST files), so `- 1` cannot underflow.
    let probe_capacity = 1u64 << 40;
    mem.set_capacity(probe_capacity);
    let stored =
        probe_capacity - crate::fs::Fs::available_space(&mem, dir.path()).unwrap_or(probe_capacity);
    mem.set_capacity(stored + run_sigma - 1);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;

    let opts = super::Options::from_tree(
        &tree,
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024)),
    );
    match super::space_gate_for_merge(&version, &opts, &payload)? {
        super::SpaceGate::Narrowed(narrowed) => {
            assert_eq!(narrowed.table_ids.len(), 2, "narrowed to an adjacent pair");
        }
        super::SpaceGate::Run => {
            panic!("expected Narrowed, got Run (full run wrongly admitted)")
        }
        super::SpaceGate::Skip => panic!("expected Narrowed, got Skip (no pair admitted)"),
    }

    Ok(())
}
