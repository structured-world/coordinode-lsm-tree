use crate::{AbstractTree, Config, SeqNo, SequenceNumberCounter, config::CompressionPolicy};
use test_log::test;

// NOTE: Tests that versions of the same key stay
// in the same table even if it needs to be rotated
//
// This avoids tables' key ranges overlapping
//
// http://github.com/fjall-rs/lsm-tree/commit/f46b6fe26a1e90113dc2dbb0342db160a295e616
#[test]
fn table_multi_writer_same_key_norotate() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(crate::CompressionType::None))
    .index_block_compression_policy(CompressionPolicy::all(crate::CompressionType::None))
    .open()?;

    tree.insert("a", "a1".repeat(4_000), 0);
    tree.insert("a", "a2".repeat(4_000), 1);
    tree.insert("a", "a3".repeat(4_000), 2);
    tree.insert("a", "a4".repeat(4_000), 3);
    tree.insert("a", "a5".repeat(4_000), 4);
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);

    tree.major_compact(1_024, 0)?;
    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);

    Ok(())
}

// Regression (#32): compaction clip must preserve RT covering gap between
// output tables.  Before the fix, MultiWriter clipped each RT to
// [first_key, upper_bound(last_key)) — RTs in the gap were dropped by all
// tables.  The fix clips to [first_key, next_table_first_key) during
// rotation, covering the gap, and widens key_range so point reads find it.
#[test]
fn clip_preserves_rt_covering_gap_between_output_tables() -> crate::Result<()> {
    use crate::range_tombstone::RangeTombstone;
    use crate::{InternalValue, UserKey, fs::StdFs};
    use std::sync::Arc;

    let folder = tempfile::tempdir()?;
    let base_path = folder.path().join("tables");
    std::fs::create_dir_all(&base_path)?;

    let id_gen = SequenceNumberCounter::default();
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // Tiny target_size to force rotation between "l" and "q"
    let mut mw =
        super::MultiWriter::new(base_path.clone(), id_gen, 100, 1, fs)?.use_clip_range_tombstones();

    mw.set_range_tombstones(vec![RangeTombstone::new(
        UserKey::from(b"m" as &[u8]),
        UserKey::from(b"p" as &[u8]),
        20,
    )]);

    // Table 1: keys [a, l]  — values large enough to fill a 4 KiB data
    // block and push file_pos past target_size so rotation fires on "q".
    mw.write(InternalValue::from_components(
        UserKey::from(b"a" as &[u8]),
        vec![0u8; 4_000],
        1,
        crate::ValueType::Value,
    ))?;
    mw.write(InternalValue::from_components(
        UserKey::from(b"l" as &[u8]),
        vec![0u8; 4_000],
        2,
        crate::ValueType::Value,
    ))?;
    // Table 2: keys [q, z]  — rotation happens before "q"
    mw.write(InternalValue::from_components(
        UserKey::from(b"q" as &[u8]),
        vec![0u8; 4_000],
        3,
        crate::ValueType::Value,
    ))?;
    mw.write(InternalValue::from_components(
        UserKey::from(b"z" as &[u8]),
        vec![0u8; 4_000],
        4,
        crate::ValueType::Value,
    ))?;

    let results = mw.finish()?;
    assert!(
        results.len() >= 2,
        "expected 2+ output tables to verify gap, got {}",
        results.len(),
    );

    // Recover each output table and count preserved RTs
    let cache = Arc::new(crate::Cache::with_capacity_bytes(64 * 1_024));
    let comparator: crate::SharedComparator = Arc::new(crate::DefaultUserComparator);
    let mut total_rts = 0;

    for (table_id, checksum) in &results {
        let table = crate::Table::recover(crate::table::RecoverParams::new(
            base_path.join(table_id.to_string()),
            *checksum,
            *table_id,
            Arc::new(StdFs),
            comparator.clone(),
            cache.clone(),
        ))?;
        total_rts += table.range_tombstones().len();
    }

    assert!(
        total_rts > 0,
        "BUG: RT [m,p)@20 was dropped by compaction clip — \
         no output table preserved it (gap between tables)",
    );

    Ok(())
}

// Edge case (#32): RT spans past the next table's first key, so
// clipped.end == clip_upper.  Widening last_key to clip_upper would
// make adjacent tables' key_ranges overlap and break Run::get_for_key_cmp.
// Verify the RT is still written but key_range stays disjoint.
#[test]
fn clip_rt_spanning_next_table_does_not_overlap_key_ranges() -> crate::Result<()> {
    use crate::{InternalValue, UserKey, fs::StdFs};
    use std::sync::Arc;

    let folder = tempfile::tempdir()?;
    let base_path = folder.path().join("tables");
    std::fs::create_dir_all(&base_path)?;

    let id_gen = SequenceNumberCounter::default();
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    let mut mw =
        super::MultiWriter::new(base_path.clone(), id_gen, 100, 1, fs)?.use_clip_range_tombstones();

    // RT [m, r) — end "r" > next table's first key "q", so after
    // clipping to [first_key, clip_upper="q") the clipped.end == "q".
    mw.set_range_tombstones(vec![crate::range_tombstone::RangeTombstone::new(
        UserKey::from(b"m" as &[u8]),
        UserKey::from(b"r" as &[u8]),
        20,
    )]);

    // Table 1: [a, l]
    mw.write(InternalValue::from_components(
        UserKey::from(b"a" as &[u8]),
        vec![0u8; 4_000],
        1,
        crate::ValueType::Value,
    ))?;
    mw.write(InternalValue::from_components(
        UserKey::from(b"l" as &[u8]),
        vec![0u8; 4_000],
        2,
        crate::ValueType::Value,
    ))?;
    // Table 2: [q, z]
    mw.write(InternalValue::from_components(
        UserKey::from(b"q" as &[u8]),
        vec![0u8; 4_000],
        3,
        crate::ValueType::Value,
    ))?;
    mw.write(InternalValue::from_components(
        UserKey::from(b"z" as &[u8]),
        vec![0u8; 4_000],
        4,
        crate::ValueType::Value,
    ))?;

    let results = mw.finish()?;
    assert!(results.len() >= 2);

    let cache = Arc::new(crate::Cache::with_capacity_bytes(64 * 1_024));
    let comparator: crate::SharedComparator = Arc::new(crate::DefaultUserComparator);

    let mut tables = Vec::new();
    for (table_id, checksum) in &results {
        tables.push(crate::Table::recover(crate::table::RecoverParams::new(
            base_path.join(table_id.to_string()),
            *checksum,
            *table_id,
            Arc::new(StdFs),
            comparator.clone(),
            cache.clone(),
        ))?);
    }

    // Key ranges must be disjoint: table1.max < table2.min
    let t1_max = tables[0].metadata.key_range.max();
    let t2_min = tables[1].metadata.key_range.min();
    assert!(
        t1_max.as_ref() < t2_min.as_ref(),
        "key_ranges must be disjoint: table1.max={t1_max:?} must be < table2.min={t2_min:?}",
    );

    // RT must still be written to at least one output table
    let total_rts: usize = tables.iter().map(|t| t.range_tombstones().len()).sum();
    assert!(
        total_rts > 0,
        "RT [m,r)@20 must be preserved in at least one output table",
    );

    Ok(())
}

// NOTE: Follow-up fix for non-disjoint output
//
// https://github.com/fjall-rs/lsm-tree/commit/1609a57c2314420b858d826790ecd1442aa76720
#[test]
fn table_multi_writer_same_key_norotate_2() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(crate::CompressionType::None))
    .index_block_compression_policy(CompressionPolicy::all(crate::CompressionType::None))
    .open()?;

    tree.insert("a", "a1".repeat(4_000), 0);
    tree.insert("a", "a1".repeat(4_000), 1);
    tree.insert("a", "a1".repeat(4_000), 2);
    tree.insert("b", "a1".repeat(4_000), 0);
    tree.insert("c", "a1".repeat(4_000), 0);
    tree.insert("c", "a1".repeat(4_000), 1);
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.table_count());
    assert_eq!(3, tree.len(SeqNo::MAX, None)?);

    tree.major_compact(1_024, 0)?;
    assert_eq!(3, tree.table_count());
    assert_eq!(3, tree.len(SeqNo::MAX, None)?);

    Ok(())
}

// D5b round-trip: the retrieval-ribbon locator section must survive the real
// table writer + reader path. With the policy enabled every inserted key
// recovers a `(block_id, slot)` from the on-disk section (validating the
// writer's per-key block_id/slot accumulation, not just synthetic inputs);
// with the policy disabled the section is absent entirely (zero bytes, the
// byte-identical guarantee).
#[test]
#[expect(
    clippy::expect_used,
    reason = "test asserts a freshly-written section is present + recovers"
)]
fn locator_section_round_trips_through_writer() -> crate::Result<()> {
    use crate::config::{LocatorPolicyEntry, LocatorPrecision};
    use crate::table::block::BlockType;
    use crate::table::locator::locate;
    use crate::{CompressionType, InternalValue, UserKey, fs::StdFs};
    use std::sync::Arc;

    let folder = tempfile::tempdir()?;
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    // Small data blocks + many small KVs → many data blocks, so block_id is
    // non-trivial and the accumulation across block boundaries is exercised.
    let n = 2_000u64;

    let write_and_recover =
        |base: &std::path::Path, entry: LocatorPolicyEntry| -> crate::Result<crate::Table> {
            std::fs::create_dir_all(base)?;
            let mut mw = super::MultiWriter::new(
                base.to_path_buf(),
                SequenceNumberCounter::default(),
                64 * 1_024 * 1_024,
                1,
                fs.clone(),
            )?
            .use_data_block_size(4_096)
            .use_locator(entry);
            for i in 0..n {
                mw.write(InternalValue::from_components(
                    UserKey::from(i.to_be_bytes().as_slice()),
                    vec![0u8; 64],
                    1,
                    crate::ValueType::Value,
                ))?;
            }
            let results = mw.finish()?;
            assert_eq!(results.len(), 1, "single output table expected");
            let (table_id, checksum) = results[0];
            crate::Table::recover(crate::table::RecoverParams::new(
                base.join(table_id.to_string()),
                checksum,
                table_id,
                Arc::new(StdFs),
                Arc::new(crate::DefaultUserComparator),
                Arc::new(crate::Cache::with_capacity_bytes(1 << 20)),
            ))
        };

    // 1) Enabled (auto widths): section present, every key recovers.
    let base_on = folder.path().join("on");
    let table = write_and_recover(
        &base_on,
        LocatorPolicyEntry::Enabled {
            precision: LocatorPrecision::Restart,
            block_id_bits: None,
            slot_bits: None,
        },
    )?;
    let handle = table
        .regions
        .locator
        .expect("locator section must be present when enabled");
    let block = table.load_block(
        &handle,
        BlockType::Locator,
        CompressionType::None,
        #[cfg(zstd_any)]
        None,
    )?;
    let section_bytes: &[u8] = &block.data;
    let num_blocks = table.metadata.data_block_count;
    assert!(num_blocks > 1, "test should produce multiple data blocks");
    for i in 0..n {
        let h = crate::hash::hash64(&i.to_be_bytes());
        let (block_id, _slot) =
            locate(section_bytes, h)?.unwrap_or_else(|| panic!("inserted key {i} must locate"));
        assert!(
            block_id < num_blocks,
            "key {i}: block_id {block_id} >= data_block_count {num_blocks}",
        );
    }

    // 2) Disabled (default): no section (zero bytes, byte-identical).
    let base_off = folder.path().join("off");
    let table_off = write_and_recover(&base_off, LocatorPolicyEntry::None)?;
    assert!(
        table_off.regions.locator.is_none(),
        "disabled policy must emit no locator section",
    );

    Ok(())
}

/// A filter transformation whose record TRIGGERS the rotation belongs to the
/// output that would have received it — the NEW one. Rotation runs before
/// the triggering record is inserted, so at that moment the live counter
/// already carries its verdict; judging the finishing output by the live
/// counter would mark the UNTOUCHED old output transformed and leave the
/// transformed new output plain — after manifest loss, repair could then
/// discard the transformed output as derived and resurrect the pre-filter
/// value from its inputs.
#[test]
fn a_transform_on_the_rotation_boundary_belongs_to_the_new_output() -> crate::Result<()> {
    use crate::fs::StdFs;
    use crate::{InternalValue, UserKey};
    use std::sync::Arc;

    let folder = tempfile::tempdir()?;
    let base_path = folder.path().to_path_buf();
    std::fs::create_dir_all(&base_path)?;

    let id_gen = SequenceNumberCounter::default();
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let marker = Arc::new(portable_atomic::AtomicU64::new(0));

    // Tiny target_size: rotation fires on the key AFTER the 4 KiB fillers.
    let mut mw = super::MultiWriter::new(base_path.clone(), id_gen, 100, 1, fs)?
        .use_lineage(Some(vec![7, 8]))
        .use_transform_marker(Arc::clone(&marker));

    // Output 1: untouched keys.
    for key in [b"a" as &[u8], b"l"] {
        mw.write(InternalValue::from_components(
            UserKey::from(key),
            vec![0u8; 4_000],
            1,
            crate::ValueType::Value,
        ))?;
    }
    // The filter TRANSFORMS the next record (a Remove verdict replaces it
    // with a tombstone) — the adapter ticks the marker BEFORE the write, and
    // the write's rotation happens before the record is inserted.
    marker.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    mw.write(InternalValue::from_components(
        UserKey::from(b"q" as &[u8]),
        vec![],
        2,
        crate::ValueType::Tombstone,
    ))?;
    mw.write(InternalValue::from_components(
        UserKey::from(b"z" as &[u8]),
        vec![0u8; 100],
        3,
        crate::ValueType::Value,
    ))?;

    let results = mw.finish()?;
    assert_eq!(
        results.len(),
        2,
        "the boundary transform forces two outputs"
    );

    let cache = Arc::new(crate::Cache::with_capacity_bytes(64 * 1_024));
    let comparator: crate::SharedComparator = Arc::new(crate::DefaultUserComparator);
    let mut lineages = Vec::new();
    for (table_id, checksum) in &results {
        let table = crate::Table::recover(crate::table::RecoverParams::new(
            base_path.join(table_id.to_string()),
            *checksum,
            *table_id,
            Arc::new(StdFs),
            comparator.clone(),
            cache.clone(),
        ))?;
        lineages.push((
            table.metadata.lineage.clone(),
            table.metadata.lineage_transformed,
        ));
    }
    assert_eq!(
        lineages,
        vec![(Some(vec![7, 8]), false), (Some(vec![7, 8]), true),],
        "both outputs keep their lineage; only the output holding the \
         transformed record carries the transformed marker",
    );
    Ok(())
}

/// Regression: output rotation must recognise a NEW user key by byte
/// identity, not by byte order. `MultiWriter::write` used `current_key <
/// new_key` to detect the next key, which under a comparator whose order is
/// not the byte order (here: reversed) is false for every key after the
/// first, so `target_size` was never consulted and the whole stream landed
/// in one table. Keys arrive in comparator order (descending bytes), each
/// with a value that fills a whole 4 KiB data block (so the block spills and
/// the size hint grows past the 100-byte target before the next key):
/// every key after the first must open a new output.
#[test]
fn multi_writer_rotates_under_a_non_lexicographic_comparator() -> crate::Result<()> {
    use crate::fs::StdFs;
    use crate::{InternalValue, UserKey};
    use std::sync::Arc;

    #[derive(Debug)]
    struct ReverseComparator;
    impl crate::comparator::UserComparator for ReverseComparator {
        fn name(&self) -> &'static str {
            "reverse-test"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
            b.cmp(a)
        }
        fn is_lexicographic(&self) -> bool {
            false
        }
    }

    let folder = tempfile::tempdir()?;
    let base_path = folder.path().to_path_buf();
    std::fs::create_dir_all(&base_path)?;

    let id_gen = SequenceNumberCounter::default();
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let mut mw = super::MultiWriter::new(base_path, id_gen, 100, 1, fs)?
        .set_comparator(Arc::new(ReverseComparator));

    // Comparator order for a reversed comparator: byte-descending.
    for key in [b"z" as &[u8], b"l", b"a"] {
        mw.write(InternalValue::from_components(
            UserKey::from(key),
            vec![0u8; 4_096],
            1,
            crate::ValueType::Value,
        ))?;
    }

    let results = mw.finish()?;
    assert_eq!(
        results.len(),
        3,
        "each new key exceeds the 100-byte target, so every key after the first opens a new output"
    );
    Ok(())
}
