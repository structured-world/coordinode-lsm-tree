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

/// The blob files a table links are handed to its writer only when it
/// rotates, and it writes them at `finish`: the table is full once they no
/// longer fit its target, before they reach the writer.
#[test]
fn the_linked_blob_files_count_toward_a_full_table() -> crate::Result<()> {
    use crate::{InternalValue, UserKey, fs::StdFs, table::writer::LinkedFile};
    use std::sync::Arc;

    let folder = tempfile::tempdir()?;
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let mut mw = super::MultiWriter::new(
        folder.path().to_path_buf(),
        SequenceNumberCounter::default(),
        u64::MAX,
        1,
        fs,
    )?;
    // A block large enough that the table's size, not the state it holds for
    // `finish`, decides.
    mw.write(InternalValue::from_components(
        UserKey::from(b"a" as &[u8]),
        vec![0u8; 65_536],
        0,
        crate::ValueType::Value,
    ))?;
    mw.writer.spill_block()?;
    mw.target_size = mw.writer.output_size_hint() + 100;
    assert!(mw.writer.held_state_bytes() < mw.target_size);
    assert!(!mw.table_full());
    for blob_file_id in 0..10 {
        mw.linked_blobs.insert(
            blob_file_id,
            LinkedFile {
                blob_file_id,
                bytes: 1,
                on_disk_bytes: 1,
                len: 1,
            },
        );
    }
    assert!(mw.table_full(), "ten linked files take 324 bytes");
    Ok(())
}

/// Each linked blob file is an entry in the multi-writer's map and, once the
/// table rotates, a copy in its writer, far more than the 32 bytes it takes
/// on disk: the held state counts both, so a table linking many files is full
/// by its memory before its size.
#[test]
fn the_linked_blob_files_count_toward_the_held_state() -> crate::Result<()> {
    use crate::{InternalValue, UserKey, fs::StdFs, table::writer::LinkedFile};
    use std::sync::Arc;

    const FILES: u64 = 1_000;
    let folder = tempfile::tempdir()?;
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let mut mw = super::MultiWriter::new(
        folder.path().to_path_buf(),
        SequenceNumberCounter::default(),
        u64::MAX,
        1,
        fs,
    )?;
    mw.write(InternalValue::from_components(
        UserKey::from(b"a" as &[u8]),
        b"v".to_vec(),
        0,
        crate::ValueType::Value,
    ))?;
    mw.writer.spill_block()?;
    for blob_file_id in 0..FILES {
        mw.linked_blobs.insert(
            blob_file_id,
            LinkedFile {
                blob_file_id,
                bytes: 1,
                on_disk_bytes: 1,
                len: 1,
            },
        );
    }
    // Their bytes on disk fit; their entries in memory do not.
    let linked = crate::table::writer::linked_blob_files_len(mw.linked_blobs.len());
    mw.target_size = mw.writer.output_size_hint() + linked + 100;
    assert!(mw.writer.held_state_bytes() + FILES * 64 >= mw.target_size);
    assert!(mw.table_full(), "{FILES} linked files held in memory");
    Ok(())
}

/// A columnar batch lands whole, so what an output holds before it, not after,
/// is what the next output carries anyway: a first batch holding more than the
/// target fills its table, and the next batch goes to a new one.
#[cfg(feature = "columnar")]
#[test]
fn a_first_columnar_batch_past_the_target_fills_its_table() -> crate::Result<()> {
    use crate::{InternalValue, fs::StdFs, table::columnar::entries_to_column_batch};
    use std::sync::Arc;

    let folder = tempfile::tempdir()?;
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let mut mw = super::MultiWriter::new(
        folder.path().to_path_buf(),
        SequenceNumberCounter::default(),
        u64::MAX,
        1,
        fs,
    )?
    .use_columnar(true);
    let entries: Vec<InternalValue> = (0..2_000u32)
        .map(|i| {
            InternalValue::from_components(
                format!("key{i:06}").into_bytes(),
                b"v".to_vec(),
                0,
                crate::ValueType::Value,
            )
        })
        .collect();
    mw.write_columnar_batch(&entries_to_column_batch(&entries)?)?;
    // Its bytes fit the target; the state it holds for `finish` does not.
    mw.target_size = mw.writer.output_size_hint() + 1;
    assert!(mw.writer.held_state_bytes() >= mw.target_size);
    assert!(mw.table_full(), "the first batch holds past the target");
    Ok(())
}

/// Rotation hands the linked blob files to the finishing writer and frees the
/// map, so a large map does not stay allocated under the next table, where
/// nothing counts it.
#[test]
fn rotation_frees_the_linked_blob_map() -> crate::Result<()> {
    use crate::{InternalValue, UserKey, fs::StdFs, table::writer::LinkedFile};
    use std::sync::Arc;

    let folder = tempfile::tempdir()?;
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let mut mw = super::MultiWriter::new(
        folder.path().to_path_buf(),
        SequenceNumberCounter::default(),
        u64::MAX,
        1,
        fs,
    )?;
    mw.write(InternalValue::from_components(
        UserKey::from(b"a" as &[u8]),
        b"v".to_vec(),
        0,
        crate::ValueType::Value,
    ))?;
    for blob_file_id in 0..1_000 {
        mw.linked_blobs.insert(
            blob_file_id,
            LinkedFile {
                blob_file_id,
                bytes: 1,
                on_disk_bytes: 1,
                len: 1,
            },
        );
    }
    mw.current_key = Some(UserKey::from(b"b" as &[u8]));
    mw.rotate()?;
    assert_eq!(mw.linked_blobs.capacity(), 0);
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

    // A target the two 4 KiB fillers pass once their block is written, and a
    // single key with the tail every table writes does not: rotation fires on
    // the key AFTER the fillers.
    let mut mw = super::MultiWriter::new(base_path.clone(), id_gen, 12_000, 1, fs)?
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

fn recover_outputs(
    base_path: &std::path::Path,
    results: &[(crate::TableId, crate::Checksum)],
) -> crate::Result<Vec<crate::Table>> {
    use crate::fs::StdFs;
    use std::sync::Arc;

    let cache = Arc::new(crate::Cache::with_capacity_bytes(64 * 1_024));
    let comparator: crate::SharedComparator = Arc::new(crate::DefaultUserComparator);
    results
        .iter()
        .map(|(table_id, checksum)| {
            crate::Table::recover(crate::table::RecoverParams::new(
                base_path.join(table_id.to_string()),
                *checksum,
                *table_id,
                Arc::new(StdFs),
                comparator.clone(),
                cache.clone(),
            ))
        })
        .collect()
}

/// A flush cuts each range tombstone into the zones of the outputs it spans
/// instead of copying the whole set into every output: the pieces of one
/// tombstone chain up to exactly its range, the first output reaches below
/// the memtable's first key and the last one above its last key.
#[test]
fn a_flush_cuts_each_tombstone_into_the_outputs_it_spans() -> crate::Result<()> {
    use crate::{InternalValue, UserKey, fs::StdFs, range_tombstone::RangeTombstone};
    use std::sync::Arc;

    let folder = tempfile::tempdir()?;
    let base_path = folder.path().to_path_buf();
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    // Every key passes the target with the tail every table writes, and the
    // few tombstones a rotation carries stay well under half of it.
    let mut mw = super::MultiWriter::new(
        base_path.clone(),
        SequenceNumberCounter::default(),
        4_000,
        1,
        fs,
    )?;
    let tombstones = [
        (b"a" as &[u8], b"c" as &[u8], 20),
        (b"k", b"r", 21),
        (b"w", b"zz", 22),
    ];
    mw.set_range_tombstones(
        tombstones
            .iter()
            .map(|&(start, end, seqno)| {
                RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
            })
            .collect(),
    );
    // Each key fills a block of its own and passes the target once written,
    // so the outputs' zones are (.., l), [l, q), [q, x), [x, ..).
    for key in [b"b" as &[u8], b"l", b"q", b"x"] {
        mw.write(InternalValue::from_components(
            UserKey::from(key),
            vec![0u8; 5_000],
            1,
            crate::ValueType::Value,
        ))?;
    }
    let tables = recover_outputs(&base_path, &mw.finish()?)?;
    assert_eq!(tables.len(), 4);

    let mut pieces: Vec<(SeqNo, Vec<u8>, Vec<u8>)> = tables
        .iter()
        .flat_map(|table| table.range_tombstones().iter())
        .map(|rt| (rt.seqno, rt.start.to_vec(), rt.end.to_vec()))
        .collect();
    pieces.sort();
    assert_eq!(pieces.len(), 6, "1 + 3 + 2 zones spanned: {pieces:?}");
    for &(start, end, seqno) in &tombstones {
        let own: Vec<_> = pieces.iter().filter(|p| p.0 == seqno).collect();
        assert_eq!(own.first().map(|p| p.1.as_slice()), Some(start));
        assert_eq!(own.last().map(|p| p.2.as_slice()), Some(end));
        for pair in own.windows(2) {
            assert_eq!(
                pair[0].2, pair[1].1,
                "pieces of @{seqno} must chain: {own:?}"
            );
        }
    }
    assert_eq!(tables[0].metadata.key_range.min().as_ref(), b"a");
    assert_eq!(tables[3].metadata.key_range.max().as_ref(), b"zz");
    Ok(())
}

/// A flush's tombstones starting past its last key reach no key's rotation
/// check: they are checked at their starts, so they spread over outputs of
/// tombstones alone near the target instead of all landing in the last one.
#[test]
fn a_flush_splits_its_tombstones_past_the_last_key() -> crate::Result<()> {
    use crate::{InternalValue, UserKey, fs::StdFs, range_tombstone::RangeTombstone};
    use std::sync::Arc;

    // Small enough that the tail an output of tombstones alone writes is a
    // good part of it.
    const TARGET: u64 = 12 * 1_024;
    const TOMBSTONES: usize = 2_000;

    // Pseudo-random bounds, so no codec shrinks the tombstone block.
    let bound = |seed: usize, mut key: Vec<u8>| {
        let mut state = (seed as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        while key.len() < 64 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            key.extend_from_slice(&state.to_le_bytes());
        }
        key
    };
    let folder = tempfile::tempdir()?;
    let base_path = folder.path().to_path_buf();
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let mut mw = super::MultiWriter::new(
        base_path.clone(),
        SequenceNumberCounter::default(),
        TARGET,
        1,
        fs,
    )?;
    let tombstones: Vec<_> = (0..TOMBSTONES)
        .map(|i| {
            let prefix = format!("z{i:08}").into_bytes();
            let mut start = prefix.clone();
            start.push(0);
            let mut end = prefix;
            end.push(1);
            RangeTombstone::new(
                UserKey::from(bound(2 * i, start)),
                UserKey::from(bound(2 * i + 1, end)),
                5,
            )
        })
        .collect();
    mw.set_range_tombstones(tombstones.clone());
    for key in [b"a" as &[u8], b"b", b"c"] {
        mw.write(InternalValue::from_components(
            UserKey::from(key),
            b"v".to_vec(),
            1,
            crate::ValueType::Value,
        ))?;
    }
    let tables = recover_outputs(&base_path, &mw.finish()?)?;
    assert!(tables.len() > 1, "the tombstones must spread over outputs");

    let mut written: Vec<_> = tables
        .iter()
        .flat_map(|table| table.range_tombstones().iter())
        .map(|rt| (rt.start.to_vec(), rt.end.to_vec()))
        .collect();
    written.sort();
    let expected: Vec<_> = tombstones
        .iter()
        .map(|rt| (rt.start.to_vec(), rt.end.to_vec()))
        .collect();
    assert_eq!(written, expected, "each tombstone written once, whole");
    // Each output closes once it reaches the target, counting the tail, and
    // the sentinel block an output of tombstones alone writes: it passes the
    // target by the last entry at most.
    for table in &tables {
        let file_size = std::fs::metadata(base_path.join(table.id().to_string()))?.len();
        assert!(
            file_size <= TARGET + 512,
            "output of {file_size} bytes with {} tombstones overran the {TARGET}-byte target",
            table.range_tombstones().len(),
        );
    }
    Ok(())
}

/// Every table carries its tail, and closing a table does not shed it: a
/// target below the tail and a block still gives tables of a block each, not
/// of one key each.
#[test]
fn a_target_below_the_table_tail_still_fills_a_block() -> crate::Result<()> {
    use crate::{InternalValue, UserKey, fs::StdFs};
    use std::sync::Arc;

    const KEYS: u32 = 4_000;
    let folder = tempfile::tempdir()?;
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let mut mw = super::MultiWriter::new(
        folder.path().to_path_buf(),
        SequenceNumberCounter::default(),
        4_096,
        1,
        fs,
    )?;
    let mut bytes = 0;
    for i in 0..KEYS {
        let key = format!("key{i:06}").into_bytes();
        let value = random_bound_of(i as usize, Vec::new(), 24);
        bytes += key.len() + value.len();
        mw.write(InternalValue::from_components(
            UserKey::from(key),
            value,
            1,
            crate::ValueType::Value,
        ))?;
    }
    let tables = mw.finish()?.len();
    // A 4 KiB block each, and a partial one at the end.
    let blocks = bytes / 4_096 + 1;
    assert!(
        tables <= blocks,
        "{tables} tables for {KEYS} keys in {blocks} blocks",
    );
    Ok(())
}

/// An output of tombstones alone carries its synthetic table, which closing
/// it does not shed. With a target below that table, the outputs past the
/// last key still take a block's worth of tombstones each, by their bytes or
/// the entries they hold, not one tombstone each.
#[test]
fn outputs_of_tombstones_alone_below_their_fixed_table_fill_a_block() -> crate::Result<()> {
    use crate::{UserKey, range_tombstone::RangeTombstone};

    const TOMBSTONES: usize = 300;
    let tombstones: Vec<_> = (0..TOMBSTONES)
        .map(|i| {
            let prefix = format!("z{i:08}").into_bytes();
            let mut start = prefix.clone();
            start.push(0);
            let mut end = prefix;
            end.push(1);
            RangeTombstone::new(
                UserKey::from(random_bound(2 * i, start)),
                UserKey::from(random_bound(2 * i + 1, end)),
                5,
            )
        })
        .collect();
    let (_folder, tables) = flush_outputs(4_096, &[b"a"], tombstones)?;
    // A block's worth of these entries in memory is several of them. The
    // first output holds the record, the last whatever is left.
    let alone = tables
        .get(1..tables.len().saturating_sub(1))
        .unwrap_or_default();
    assert!(!alone.is_empty(), "the tombstones must spread over outputs");
    for table in alone {
        assert!(
            table.range_tombstones().len() >= 4,
            "an output of {} tombstones among {} outputs",
            table.range_tombstones().len(),
            tables.len(),
        );
    }
    Ok(())
}

/// An output of tombstones alone builds the filter over its sentinel at
/// `finish`: with an extractor yielding every prefix of a long bound, that
/// build takes a good part of the target, so such outputs close on fewer
/// tombstones than without it.
#[test]
fn outputs_of_tombstones_alone_count_their_sentinel_filter_build() -> crate::Result<()> {
    use crate::{InternalValue, UserKey, fs::StdFs, range_tombstone::RangeTombstone};
    use std::sync::Arc;

    const LEN: usize = 4_000;
    let target =
        2 * crate::table::filter::ribbon::burr::builder::build_peak_bytes(LEN + 1, false) as u64;
    let outputs = |extractor: Option<Arc<dyn crate::prefix::PrefixExtractor>>| {
        let folder = tempfile::tempdir()?;
        let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
        let mut mw = super::MultiWriter::new(
            folder.path().to_path_buf(),
            SequenceNumberCounter::default(),
            target,
            1,
            fs,
        )?
        .use_prefix_extractor(extractor);
        mw.set_range_tombstones(
            (0..64)
                .map(|i| {
                    let prefix = format!("z{i:08}").into_bytes();
                    let mut start = prefix.clone();
                    start.push(0);
                    let mut end = prefix;
                    end.push(1);
                    RangeTombstone::new(
                        UserKey::from(random_bound_of(2 * i, start, LEN)),
                        UserKey::from(random_bound_of(2 * i + 1, end, LEN)),
                        5,
                    )
                })
                .collect(),
        );
        mw.write(InternalValue::from_components(
            UserKey::from(b"a" as &[u8]),
            b"v".to_vec(),
            1,
            crate::ValueType::Value,
        ))?;
        crate::Result::Ok(mw.finish()?.len())
    };
    let plain = outputs(None)?;
    let with_prefixes = outputs(Some(Arc::new(AllPrefixes)))?;
    assert!(
        2 * with_prefixes >= 3 * plain,
        "{with_prefixes} outputs with the prefixes, {plain} without",
    );
    Ok(())
}

/// A tombstone spanning many keys is carried into every output, like the
/// tail every table ends with, and closing one sheds none of it: with a
/// target below that tail, the outputs still take a block's worth of keys
/// each, not one key each.
#[test]
fn a_carried_tombstone_does_not_close_a_table_on_every_key() -> crate::Result<()> {
    use crate::{UserKey, range_tombstone::RangeTombstone};

    const KEYS: usize = 1_000;
    let keys: Vec<Vec<u8>> = (0..KEYS).map(|i| format!("k{i:05}").into_bytes()).collect();
    let keys: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let tombstone = RangeTombstone::new(
        UserKey::from(b"k" as &[u8]),
        UserKey::from(b"l" as &[u8]),
        5,
    );
    let (_folder, tables) = flush_outputs(4_096, &keys, vec![tombstone])?;
    assert!(
        tables.len() <= KEYS / 20,
        "{} outputs for {KEYS} keys",
        tables.len(),
    );
    Ok(())
}

/// A tombstone past the last key with a long bound sizes the synthetic table
/// every output of tombstones alone is estimated with; the short ones before
/// it still pack a block's worth into each such output, not one each.
#[test]
fn one_long_trailing_bound_does_not_fragment_the_tombstones_before_it() -> crate::Result<()> {
    use crate::{UserKey, range_tombstone::RangeTombstone};

    const TOMBSTONES: usize = 200;
    let mut tombstones: Vec<_> = (0..TOMBSTONES)
        .map(|i| {
            let start = format!("x{i:05}").into_bytes();
            let mut end = start.clone();
            end.push(0);
            RangeTombstone::new(UserKey::from(start), UserKey::from(end), 5)
        })
        .collect();
    tombstones.push(RangeTombstone::new(
        UserKey::from(random_bound_of(1, b"y".to_vec(), 20_000)),
        UserKey::from(b"z" as &[u8]),
        5,
    ));
    let (_folder, tables) = flush_outputs(4_096, &[b"a"], tombstones)?;
    assert!(
        tables.len() <= TOMBSTONES / 4,
        "{} outputs for {TOMBSTONES} short tombstones",
        tables.len(),
    );
    Ok(())
}

/// The first tombstones past a flush's last key can all share one start: the
/// output holding the records has no tombstone yet, and the check at that
/// start still counts the group, so it does not land on that output.
#[test]
fn a_flush_counts_a_first_start_group_past_its_records() -> crate::Result<()> {
    use crate::{UserKey, range_tombstone::RangeTombstone};

    const TARGET: u64 = 32 * 1_024;
    let keys: Vec<Vec<u8>> = (0..100).map(|i| format!("k{i:05}").into_bytes()).collect();
    let keys: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    // The group alone passes the target.
    let group = (0..600)
        .map(|i| {
            RangeTombstone::new(
                UserKey::from(b"y" as &[u8]),
                UserKey::from(random_bound(i, b"y\x01".to_vec())),
                5,
            )
        })
        .collect();
    let (folder, tables) = flush_outputs(TARGET, &keys, group)?;
    let Some(records) = tables.first() else {
        panic!("a flush writes an output");
    };
    let file_size = std::fs::metadata(folder.path().join(records.id().to_string()))?.len();
    assert!(
        file_size <= TARGET + 4 * 1_024,
        "the output of records took {} tombstones in {file_size} bytes",
        records.range_tombstones().len(),
    );
    Ok(())
}

/// Every byte prefix of a key is a token.
struct AllPrefixes;

impl crate::prefix::PrefixExtractor for AllPrefixes {
    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new((1..=key.len()).filter_map(|end| key.get(..end)))
    }
}

/// An output of tombstones alone writes a sentinel entry, and each section
/// the writer is configured for records it: a partitioned index and filter,
/// the zone map, the seqno bounds, the locator, and a filter holding the
/// sentinel's prefixes. The split counts all of them, so such an output still
/// ends near its target.
#[test]
fn a_flush_counts_every_section_of_an_output_of_tombstones_alone() -> crate::Result<()> {
    use crate::{InternalValue, UserKey, fs::StdFs, range_tombstone::RangeTombstone};
    use std::sync::Arc;

    const TARGET: u64 = 32 * 1_024;
    let folder = tempfile::tempdir()?;
    let base_path = folder.path().to_path_buf();
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let mut mw = super::MultiWriter::new(
        base_path.clone(),
        SequenceNumberCounter::default(),
        TARGET,
        1,
        fs,
    )?
    .use_adaptive_index(0)
    .use_partitioned_filter()
    .use_zone_map(true)
    .use_seqno_in_index(true)
    .use_locator(crate::config::LocatorPolicyEntry::Enabled {
        precision: crate::config::LocatorPrecision::Entry,
        block_id_bits: None,
        slot_bits: None,
    })
    .use_prefix_extractor(Some(Arc::new(AllPrefixes)));
    let tombstones: Vec<_> = (0..40)
        .map(|i| {
            let prefix = format!("z{i:08}").into_bytes();
            let mut start = prefix.clone();
            start.push(0);
            let mut end = prefix;
            end.push(1);
            RangeTombstone::new(
                UserKey::from(random_bound_of(2 * i, start, 1_000)),
                UserKey::from(random_bound_of(2 * i + 1, end, 1_000)),
                5,
            )
        })
        .collect();
    mw.set_range_tombstones(tombstones);
    mw.write(InternalValue::from_components(
        UserKey::from(b"a" as &[u8]),
        b"v".to_vec(),
        1,
        crate::ValueType::Value,
    ))?;
    let tables = recover_outputs(&base_path, &mw.finish()?)?;
    assert!(tables.len() > 1, "the tombstones must spread over outputs");
    for table in &tables {
        let file_size = std::fs::metadata(base_path.join(table.id().to_string()))?.len();
        assert!(
            file_size <= TARGET + 512,
            "output of {file_size} bytes with {} tombstones overran the {TARGET}-byte target",
            table.range_tombstones().len(),
        );
    }
    Ok(())
}

/// `key` extended to 64 bytes with a pseudo-random tail from `seed`, so no
/// codec shrinks a block of such bounds.
fn random_bound(seed: usize, key: Vec<u8>) -> Vec<u8> {
    random_bound_of(seed, key, 64)
}

/// `key` extended to at least `len` bytes with a pseudo-random tail.
fn random_bound_of(seed: usize, mut key: Vec<u8>, len: usize) -> Vec<u8> {
    let mut state = (seed as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    while key.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        key.extend_from_slice(&state.to_le_bytes());
    }
    key
}

/// A flush over `keys` with `tombstones`, recovered.
fn flush_outputs(
    target: u64,
    keys: &[&[u8]],
    tombstones: Vec<crate::range_tombstone::RangeTombstone>,
) -> crate::Result<(tempfile::TempDir, Vec<crate::Table>)> {
    use crate::{InternalValue, UserKey, fs::StdFs};
    use std::sync::Arc;

    let folder = tempfile::tempdir()?;
    let base_path = folder.path().to_path_buf();
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let mut mw = super::MultiWriter::new(
        base_path.clone(),
        SequenceNumberCounter::default(),
        target,
        1,
        fs,
    )?;
    mw.set_range_tombstones(tombstones);
    for &key in keys {
        mw.write(InternalValue::from_components(
            UserKey::from(key),
            b"v".to_vec(),
            1,
            crate::ValueType::Value,
        ))?;
    }
    let tables = recover_outputs(&base_path, &mw.finish()?)?;
    Ok((folder, tables))
}

/// The tombstones sharing the greatest start follow no later start at which
/// the output could be checked: the check at their start counts them, so
/// they are split from what came before instead of overrunning its output.
#[test]
fn a_flush_counts_the_last_start_group_before_it_lands() -> crate::Result<()> {
    use crate::{UserKey, range_tombstone::RangeTombstone};

    const TARGET: u64 = 32 * 1_024;
    let disjoint = (0..100).map(|i| {
        let prefix = format!("x{i:08}").into_bytes();
        let mut start = prefix.clone();
        start.push(0);
        let mut end = prefix;
        end.push(1);
        RangeTombstone::new(
            UserKey::from(random_bound(2 * i, start)),
            UserKey::from(random_bound(2 * i + 1, end)),
            5,
        )
    });
    let group = (0..200).map(|i| {
        RangeTombstone::new(
            UserKey::from(b"y" as &[u8]),
            UserKey::from(random_bound(1_000 + i, b"y\x01".to_vec())),
            5,
        )
    });
    let tombstones: Vec<_> = disjoint.chain(group).collect();
    let (folder, tables) = flush_outputs(TARGET, &[b"a", b"b", b"c"], tombstones)?;
    for table in &tables {
        let file_size = std::fs::metadata(folder.path().join(table.id().to_string()))?.len();
        assert!(
            file_size <= TARGET + 4 * 1_024,
            "output of {file_size} bytes with {} tombstones overran the {TARGET}-byte target",
            table.range_tombstones().len(),
        );
    }
    Ok(())
}

/// The check at the last start counts the group starting there by its
/// entries in memory as well as its bytes: a group of short tombstones whose
/// bytes fit but whose entries do not is split from what came before.
#[test]
fn a_flush_counts_the_entries_of_the_last_start_group() -> crate::Result<()> {
    use crate::{UserKey, range_tombstone::RangeTombstone};

    const TARGET: u64 = 32 * 1_024;
    // Sized by an entry: the group alone fits the target, not with the rest.
    let entry = core::mem::size_of::<RangeTombstone>() as u64;
    let group_len = u16::try_from(TARGET * 7 / 8 / entry).unwrap_or(u16::MAX);
    let disjoint_len = u16::try_from(TARGET / 4 / entry).unwrap_or(u16::MAX);
    let disjoint = (0..disjoint_len).map(|i| {
        let mut start = b"x".to_vec();
        start.extend_from_slice(&i.to_be_bytes());
        let mut end = start.clone();
        end.push(0);
        RangeTombstone::new(UserKey::from(start), UserKey::from(end), 5)
    });
    let group = (0..group_len).map(|i| {
        let mut end = b"y\x01".to_vec();
        end.extend_from_slice(&i.to_be_bytes());
        RangeTombstone::new(UserKey::from(b"y" as &[u8]), UserKey::from(end), 5)
    });
    let tombstones: Vec<_> = disjoint.chain(group).collect();
    let (_folder, tables) = flush_outputs(TARGET, &[b"a", b"b", b"c"], tombstones)?;
    for table in &tables {
        let held = table.range_tombstones().len() as u64 * entry;
        assert!(
            held <= TARGET,
            "{} tombstone entries hold {held} bytes against the {TARGET}-byte target",
            table.range_tombstones().len(),
        );
    }
    Ok(())
}

/// Tombstones overlapping one another cannot be split below their overlap:
/// every output from their common span holds a piece of each. An output is
/// not rotated when what it would carry into the next fills half the target,
/// so such a set lands in one output instead of in one per key.
#[test]
fn an_overlapping_set_past_the_target_is_not_carried_from_output_to_output() -> crate::Result<()> {
    use crate::{UserKey, range_tombstone::RangeTombstone};

    const TOMBSTONES: usize = 3_000;
    let tombstones = (0..TOMBSTONES)
        .map(|i| {
            RangeTombstone::new(
                UserKey::from(b"a" as &[u8]),
                UserKey::from(random_bound(i, b"z".to_vec())),
                5,
            )
        })
        .collect();
    let keys: Vec<Vec<u8>> = (0..10).map(|i| format!("k{i}").into_bytes()).collect();
    let keys: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let (_folder, tables) = flush_outputs(32 * 1_024, &keys, tombstones)?;
    let pieces: usize = tables.iter().map(|t| t.range_tombstones().len()).sum();
    assert!(
        pieces <= 2 * TOMBSTONES,
        "{pieces} pieces over {} outputs for {TOMBSTONES} tombstones",
        tables.len(),
    );
    Ok(())
}

/// Short overlapping tombstones fill an output by the entries they hold in
/// memory long before their bytes do; what a rotation would carry is judged
/// the same way, so they are not carried from output to output either.
#[test]
fn short_overlapping_tombstones_are_not_carried_by_their_memory() -> crate::Result<()> {
    use crate::{UserKey, range_tombstone::RangeTombstone};

    const TOMBSTONES: u16 = 800;
    let tombstones = (0..TOMBSTONES)
        .map(|i| {
            let mut end = b"z".to_vec();
            end.extend_from_slice(&i.to_be_bytes());
            RangeTombstone::new(UserKey::from(b"a" as &[u8]), UserKey::from(end), 5)
        })
        .collect();
    let keys: Vec<Vec<u8>> = (0..10).map(|i| format!("k{i}").into_bytes()).collect();
    let keys: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let (_folder, tables) = flush_outputs(32 * 1_024, &keys, tombstones)?;
    let pieces: usize = tables.iter().map(|t| t.range_tombstones().len()).sum();
    assert!(
        pieces <= 2 * usize::from(TOMBSTONES),
        "{pieces} pieces over {} outputs for {TOMBSTONES} tombstones",
        tables.len(),
    );
    Ok(())
}

/// Each tombstone piece holds its bounds in memory besides its entry, and
/// the block buffer holds them again: an output of long-bounded tombstones
/// is full by both copies, not by its encoded bytes alone.
#[test]
fn a_flush_output_holds_its_tombstone_bounds_within_the_target() -> crate::Result<()> {
    use crate::{UserKey, range_tombstone::RangeTombstone};

    const TARGET: u64 = 64 * 1_024;
    const KEYS: usize = 400;
    let key = |i: usize| format!("{i:05}").into_bytes();
    let tombstones = (0..KEYS)
        .map(|i| {
            let mut start = key(i);
            start.push(0);
            let mut end = key(i);
            end.push(1);
            RangeTombstone::new(
                UserKey::from(random_bound_of(2 * i, start, 500)),
                UserKey::from(random_bound_of(2 * i + 1, end, 500)),
                5,
            )
        })
        .collect();
    let keys: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    let keys: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let (_folder, tables) = flush_outputs(TARGET, &keys, tombstones)?;
    let entry = core::mem::size_of::<RangeTombstone>() as u64;
    for table in &tables {
        // Entries, their bounds, and the block they are encoded into.
        let held: u64 = table
            .range_tombstones()
            .iter()
            .map(|rt| {
                let bounds = (rt.start.len() + rt.end.len()) as u64;
                entry + bounds + 12 + bounds
            })
            .sum();
        assert!(
            held <= TARGET + 2 * 1_024,
            "{} tombstones hold {held} bytes against the {TARGET}-byte target",
            table.range_tombstones().len(),
        );
    }
    Ok(())
}

/// An output of many short tombstone pieces holds each as an entry in memory
/// until `finish`, far larger than its encoded bytes: the held state counts
/// the entries, so such an output rotates by its memory near the target.
#[test]
fn a_flush_output_holds_its_tombstone_entries_within_the_target() -> crate::Result<()> {
    use crate::{UserKey, range_tombstone::RangeTombstone};

    const TARGET: u64 = 32 * 1_024;
    const KEYS: usize = 4_000;
    let key = |i: usize| format!("{i:05}").into_bytes();
    // Four short tombstones in the gap after each key.
    let tombstones = (0..KEYS)
        .flat_map(|i| {
            (0..4_u8).map(move |j| {
                let mut start = key(i);
                start.push(2 * j);
                let mut end = key(i);
                end.push(2 * j + 1);
                RangeTombstone::new(UserKey::from(start), UserKey::from(end), 5)
            })
        })
        .collect();
    let keys: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    let keys: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let (_folder, tables) = flush_outputs(TARGET, &keys, tombstones)?;
    let entry = core::mem::size_of::<RangeTombstone>() as u64;
    for table in &tables {
        let held = table.range_tombstones().len() as u64 * entry;
        assert!(
            held <= TARGET,
            "{} tombstone entries hold {held} bytes against the {TARGET}-byte target",
            table.range_tombstones().len(),
        );
    }
    Ok(())
}

/// An output of tombstones alone takes its key range from them in the
/// comparator's order, not in byte order: under a reversed comparator the
/// byte-wise least start and greatest end leave the ends of the range out.
#[test]
fn an_output_of_tombstones_alone_orders_its_range_by_the_comparator() -> crate::Result<()> {
    use crate::{UserKey, fs::StdFs, range_tombstone::RangeTombstone};
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
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let comparator: crate::SharedComparator = Arc::new(ReverseComparator);
    let mut mw = super::MultiWriter::new(
        base_path.clone(),
        SequenceNumberCounter::default(),
        u64::MAX,
        1,
        fs,
    )?
    .set_comparator(comparator.clone());
    // In reverse order "z" < "y" < "m" < "a".
    mw.set_range_tombstones(vec![
        RangeTombstone::new(
            UserKey::from(b"z" as &[u8]),
            UserKey::from(b"m" as &[u8]),
            5,
        ),
        RangeTombstone::new(
            UserKey::from(b"y" as &[u8]),
            UserKey::from(b"a" as &[u8]),
            6,
        ),
    ]);
    let results = mw.finish()?;
    let cache = Arc::new(crate::Cache::with_capacity_bytes(64 * 1_024));
    for (table_id, checksum) in &results {
        let table = crate::Table::recover(crate::table::RecoverParams::new(
            base_path.join(table_id.to_string()),
            *checksum,
            *table_id,
            Arc::new(StdFs),
            comparator.clone(),
            cache.clone(),
        ))?;
        assert_eq!(table.metadata.key_range.min().as_ref(), b"z");
        assert_eq!(table.metadata.key_range.max().as_ref(), b"a");
    }
    assert_eq!(results.len(), 1);
    Ok(())
}

/// Each output of a flush visits only the tombstones overlapping its zone, not
/// the whole set: disjoint tombstones spread over many outputs cost a bounded
/// number of comparisons each, however many outputs the flush writes.
#[test]
fn a_flush_cuts_each_zone_without_rescanning_every_tombstone() -> crate::Result<()> {
    use crate::{InternalValue, UserKey, fs::StdFs, range_tombstone::RangeTombstone};
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    #[derive(Debug, Default)]
    struct CountingComparator(AtomicU64);
    impl crate::comparator::UserComparator for CountingComparator {
        fn name(&self) -> &'static str {
            "counting-test"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
            self.0.fetch_add(1, Ordering::Relaxed);
            a.cmp(b)
        }
    }

    // Comparisons a flush of `n` keys, each followed by a tombstone of its
    // own and each filling an output, makes.
    let comparisons = |n: u32| -> crate::Result<u64> {
        let folder = tempfile::tempdir()?;
        let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
        let comparator = Arc::new(CountingComparator::default());
        let mut mw = super::MultiWriter::new(
            folder.path().to_path_buf(),
            SequenceNumberCounter::default(),
            1,
            1,
            fs,
        )?
        .set_comparator(comparator.clone());
        let key = |i: u32| format!("k{i:06}").into_bytes();
        mw.set_range_tombstones(
            (0..n)
                .map(|i| {
                    let (mut start, mut end) = (key(i), key(i));
                    start.push(0);
                    end.push(1);
                    RangeTombstone::new(UserKey::from(start), UserKey::from(end), 5)
                })
                .collect(),
        );
        for i in 0..n {
            mw.write(InternalValue::from_components(
                UserKey::from(key(i)),
                vec![0u8; 5_000],
                1,
                crate::ValueType::Value,
            ))?;
        }
        let outputs = mw.finish()?.len();
        assert!(outputs >= n as usize / 2, "{outputs} outputs for {n} keys");
        Ok(comparator.0.load(Ordering::Relaxed))
    };
    let (small, large) = (comparisons(200)?, comparisons(800)?);
    // Four times the keys, tombstones and outputs: a linear cost grows about
    // fourfold, one rescanning every tombstone per output sixteenfold.
    assert!(
        large < 6 * small,
        "{small} comparisons for 200 keys, {large} for 800"
    );
    Ok(())
}

/// Tombstones open at the first key can fill a small target before any record
/// reaches the writer; the writer still takes that record, so no output holds
/// tombstones alone, written whole past the clip, over its neighbours' keys.
#[test]
fn tombstones_open_at_the_first_key_do_not_rotate_an_empty_output() -> crate::Result<()> {
    use crate::{InternalValue, UserKey, fs::StdFs, range_tombstone::RangeTombstone};
    use std::sync::Arc;

    let folder = tempfile::tempdir()?;
    let base_path = folder.path().to_path_buf();
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let mut mw = super::MultiWriter::new(
        base_path.clone(),
        SequenceNumberCounter::default(),
        100,
        1,
        fs,
    )?
    .use_clip_range_tombstones();
    mw.set_range_tombstones(
        (0..10u8)
            .map(|i| {
                RangeTombstone::new(
                    UserKey::from(vec![b'a', i]),
                    UserKey::from(b"z" as &[u8]),
                    20,
                )
            })
            .collect(),
    );
    for key in [b"b" as &[u8], b"l", b"q"] {
        mw.write(InternalValue::from_components(
            UserKey::from(key),
            vec![0u8; 4_000],
            1,
            crate::ValueType::Value,
        ))?;
    }
    let tables = recover_outputs(&base_path, &mw.finish()?)?;
    for table in &tables {
        assert!(
            table.metadata.item_count > 0,
            "an output of tombstones alone: {:?}",
            table.metadata.key_range,
        );
    }
    for pair in tables.windows(2) {
        assert!(
            pair[0].metadata.key_range.max() < pair[1].metadata.key_range.min(),
            "clipped outputs must not overlap: {:?} then {:?}",
            pair[0].metadata.key_range,
            pair[1].metadata.key_range,
        );
    }
    Ok(())
}

/// Tombstones starting at a compaction's last key follow no later key whose
/// check would count them: the check at that key counts them, since they go
/// where the key goes, so they do not land on an output already near its
/// target.
#[test]
fn a_compaction_counts_the_tombstones_starting_at_its_last_key() -> crate::Result<()> {
    use crate::{InternalValue, UserKey, fs::StdFs, range_tombstone::RangeTombstone};
    use std::sync::Arc;

    const TARGET: u64 = 32 * 1_024;
    let folder = tempfile::tempdir()?;
    let base_path = folder.path().to_path_buf();
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let mut mw = super::MultiWriter::new(
        base_path.clone(),
        SequenceNumberCounter::default(),
        TARGET,
        1,
        fs,
    )?
    .use_clip_range_tombstones();
    // Clipped to the last key, each piece is a short block entry; a thousand
    // of them do not fit beside the keys before.
    mw.set_range_tombstones(
        (0..1_000)
            .map(|i| {
                RangeTombstone::new(
                    UserKey::from(b"m" as &[u8]),
                    UserKey::from(random_bound(i, b"m\x01".to_vec())),
                    20,
                )
            })
            .collect(),
    );
    let keys = (0..70).map(|i| format!("a{i:03}").into_bytes());
    for (i, key) in keys.chain([b"m".to_vec()]).enumerate() {
        mw.write(InternalValue::from_components(
            UserKey::from(key),
            random_bound_of(10_000 + i, Vec::new(), 250),
            1,
            crate::ValueType::Value,
        ))?;
    }
    let tables = recover_outputs(&base_path, &mw.finish()?)?;
    for table in &tables {
        let file_size = std::fs::metadata(base_path.join(table.id().to_string()))?.len();
        assert!(
            file_size <= TARGET + 4 * 1_024,
            "output of {file_size} bytes with {} tombstones overran the {TARGET}-byte target",
            table.range_tombstones().len(),
        );
    }
    Ok(())
}

/// A flush widens an output's key range to the tombstone pieces it writes, and
/// the meta block holds that range twice: an output whose tombstone reaches
/// below its first key with a long start counts the widened meta.
#[test]
fn a_flush_output_counts_its_key_range_widened_by_tombstones() -> crate::Result<()> {
    use crate::{UserKey, range_tombstone::RangeTombstone};

    const TARGET: u64 = 256 * 1_024;
    let tombstone = RangeTombstone::new(
        UserKey::from(random_bound_of(1, b"0".to_vec(), 30_000)),
        UserKey::from(b"a" as &[u8]),
        5,
    );
    let folder = tempfile::tempdir()?;
    let base_path = folder.path().to_path_buf();
    let fs: std::sync::Arc<dyn crate::fs::Fs> = std::sync::Arc::new(crate::fs::StdFs);
    let mut mw = super::MultiWriter::new(
        base_path.clone(),
        SequenceNumberCounter::default(),
        TARGET,
        1,
        fs,
    )?;
    mw.set_range_tombstones(vec![tombstone]);
    for i in 0..2_000 {
        mw.write(crate::InternalValue::from_components(
            UserKey::from(format!("a{i:05}").into_bytes()),
            random_bound_of(10_000 + i, Vec::new(), 250),
            1,
            crate::ValueType::Value,
        ))?;
    }
    let tables = recover_outputs(&base_path, &mw.finish()?)?;
    assert!(tables.len() > 1, "the keys must spread over outputs");
    for table in &tables {
        let file_size = std::fs::metadata(base_path.join(table.id().to_string()))?.len();
        assert!(
            file_size <= TARGET + 8 * 1_024,
            "output of {file_size} bytes spanning {} bytes of keys overran the {TARGET}-byte target",
            table.metadata.key_range.min().len() + table.metadata.key_range.max().len(),
        );
    }
    Ok(())
}

/// A flush output's share of the range tombstones counts toward a full table,
/// so an output carrying many of them still ends near its target.
#[test]
fn a_flush_output_counts_its_tombstones_toward_a_full_table() -> crate::Result<()> {
    use crate::{InternalValue, UserKey, fs::StdFs, range_tombstone::RangeTombstone};
    use std::sync::Arc;

    const TARGET: u64 = 32 * 1_024;
    const KEYS: usize = 2_000;

    let key = |i: usize| format!("{i:08}").into_bytes();
    // Tombstone bounds take pseudo-random tails, so no codec shrinks the
    // tombstone block below the entries it holds.
    let tail = |seed: usize, mut key: Vec<u8>, len: usize| {
        let mut state = (seed as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        while key.len() < len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            key.extend_from_slice(&state.to_le_bytes());
        }
        key
    };
    let folder = tempfile::tempdir()?;
    let base_path = folder.path().to_path_buf();
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let mut mw = super::MultiWriter::new(
        base_path.clone(),
        SequenceNumberCounter::default(),
        TARGET,
        1,
        fs,
    )?;
    // One tombstone in the gap after each key: about 140 bytes of block entry
    // against a few dozen bytes of data.
    mw.set_range_tombstones(
        (0..KEYS)
            .map(|i| {
                let mut start = key(i);
                start.push(0);
                let mut end = key(i);
                end.push(1);
                RangeTombstone::new(
                    UserKey::from(tail(2 * i, start, 64)),
                    UserKey::from(tail(2 * i + 1, end, 64)),
                    5,
                )
            })
            .collect(),
    );
    for i in 0..KEYS {
        mw.write(InternalValue::from_components(
            UserKey::from(key(i)),
            vec![0u8; 8],
            1,
            crate::ValueType::Value,
        ))?;
    }
    let tables = recover_outputs(&base_path, &mw.finish()?)?;

    let tombstones: usize = tables.iter().map(|t| t.range_tombstones().len()).sum();
    assert_eq!(tombstones, KEYS, "each tombstone lies in one zone");
    // The forming data block is outside the estimate, and the filter and index
    // are re-estimated every 256 keys, so an output may pass its target by
    // that block and those keys' entries (4 KiB covers 16 bytes a key). Each
    // output here closes on its first block, whose size is the table's data
    // size.
    for table in &tables {
        assert_eq!(table.metadata.data_block_count, 1);
        let file_size = std::fs::metadata(base_path.join(table.id().to_string()))?.len();
        assert!(
            file_size <= TARGET + table.metadata.file_size + 4_096,
            "output of {file_size} bytes with {} tombstones and {} data bytes \
             overran the {TARGET}-byte target",
            table.range_tombstones().len(),
            table.metadata.file_size,
        );
    }
    Ok(())
}
