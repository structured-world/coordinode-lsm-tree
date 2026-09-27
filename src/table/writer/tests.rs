use super::*;
use crate::fs::StdFs;
use test_log::test;

#[test]
fn finish_rejects_a_delete_bitmap_without_a_zone_map() -> crate::Result<()> {
    // The positional mask resolves each block's start row from the zone map,
    // so a segment that marks deletes must also carry one. The writer must
    // reject the misconfiguration at finish() rather than emit an SST that
    // then fails to open.
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("1");
    let mut writer = Writer::new(path, 1, 0, Arc::new(StdFs))?;
    writer.write(InternalValue::from_components(
        b"a",
        b"v",
        1,
        ValueType::Value,
    ))?;
    // Mark a delete, but never enable the zone map.
    writer.delete_bitmap_mut().insert(0);
    match writer.finish() {
        Ok(_) => panic!("must reject a delete-bitmap without a zone map"),
        Err(err) => assert!(
            matches!(err, crate::Error::InvalidHeader(_)),
            "expected an InvalidHeader error, got {err:?}",
        ),
    }
    Ok(())
}

#[test]
fn table_writer_count() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("1");
    let mut writer = Writer::new(path, 1, 0, Arc::new(StdFs))?;

    assert_eq!(0, writer.meta.key_count);
    assert_eq!(0, writer.chunk_size);

    writer.write(InternalValue::from_components(
        b"a",
        b"a",
        0,
        ValueType::Value,
    ))?;
    assert_eq!(1, writer.meta.key_count);
    assert_eq!(2, writer.chunk_size);

    writer.write(InternalValue::from_components(
        b"b",
        b"b",
        0,
        ValueType::Value,
    ))?;
    assert_eq!(2, writer.meta.key_count);
    assert_eq!(4, writer.chunk_size);

    writer.write(InternalValue::from_components(
        b"c",
        b"c",
        0,
        ValueType::Value,
    ))?;
    assert_eq!(3, writer.meta.key_count);
    assert_eq!(6, writer.chunk_size);

    writer.spill_block()?;
    assert_eq!(0, writer.chunk_size);

    Ok(())
}

/// A shard scheme whose parity trailer for a block would exceed the payload
/// hard cap (256 MiB) must be rejected at WRITE time: the out-of-band
/// verifier bounds the trailer at that cap before reserving its buffer, so a
/// writer that could emit a larger one would produce an SST the verifier
/// falsely flags as corrupt. Rejecting the write keeps the cap an invariant —
/// every SST in existence stays within the verifier's supported envelope.
#[cfg(feature = "page_ecc")]
#[test]
fn writer_rejects_a_block_whose_parity_exceeds_the_hard_cap() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("1");
    // RS(1,255): every payload byte amplified 255x into parity. Keep the
    // payload JUST above the 256 MiB cap (1_100_000 × 255 ≈ 268 MiB) so a
    // regression of the pre-encoding guard allocates barely past the cap
    // instead of half a gigabyte destabilizing CI.
    let mut writer = Writer::new(path, 1, 0, Arc::new(StdFs))?
        .use_ecc(Some(crate::table::block::EccParams::try_new(1, 255)?));
    let write_result = writer.write(InternalValue::from_components(
        b"k".as_slice(),
        vec![0xABu8; 1_100_000],
        0,
        ValueType::Value,
    ));
    let result = match write_result {
        Ok(()) => writer.finish().map(|_| ()),
        Err(e) => Err(e),
    };
    assert!(
        matches!(result, Err(crate::Error::FeatureUnsupported(_))),
        "an over-cap parity trailer must fail the write loudly, got {result:?}",
    );
    Ok(())
}

#[test]
#[should_panic(expected = "index block restart interval must be greater than zero")]
fn writer_rejects_zero_index_block_restart_interval() {
    let dir = match tempfile::tempdir() {
        Ok(dir) => dir,
        Err(e) => panic!("tempdir should be created: {e}"),
    };
    let path = dir.path().join("1");
    let writer = match Writer::new(path, 1, 0, Arc::new(StdFs)) {
        Ok(writer) => writer,
        Err(e) => panic!("writer should be created: {e}"),
    };
    let _writer = writer.use_index_block_restart_interval(0);
}

#[test]
#[should_panic(expected = "data block restart interval must be greater than zero")]
fn writer_rejects_zero_data_block_restart_interval() {
    let dir = match tempfile::tempdir() {
        Ok(dir) => dir,
        Err(e) => panic!("tempdir should be created: {e}"),
    };
    let path = dir.path().join("1");
    let writer = match Writer::new(path, 1, 0, Arc::new(StdFs)) {
        Ok(writer) => writer,
        Err(e) => panic!("writer should be created: {e}"),
    };
    let _writer = writer.use_data_block_restart_interval(0);
}

#[test]
#[should_panic(expected = "data block restart interval must be configured before writing starts")]
fn writer_rejects_data_block_restart_interval_change_after_write() {
    let dir = match tempfile::tempdir() {
        Ok(dir) => dir,
        Err(e) => panic!("tempdir should be created: {e}"),
    };
    let path = dir.path().join("1");
    let mut writer = match Writer::new(path, 1, 0, Arc::new(StdFs)) {
        Ok(writer) => writer,
        Err(e) => panic!("writer should be created: {e}"),
    };
    if let Err(e) = writer.write(InternalValue::from_components(
        b"a",
        b"v",
        0,
        ValueType::Value,
    )) {
        panic!("write should succeed: {e}");
    }
    let _writer = writer.use_data_block_restart_interval(2);
}

#[test]
#[should_panic(expected = "index block restart interval must be configured before writing starts")]
fn writer_rejects_index_block_restart_interval_change_after_write() {
    let dir = match tempfile::tempdir() {
        Ok(dir) => dir,
        Err(e) => panic!("tempdir should be created: {e}"),
    };
    let path = dir.path().join("1");
    let mut writer = match Writer::new(path, 1, 0, Arc::new(StdFs)) {
        Ok(writer) => writer,
        Err(e) => panic!("writer should be created: {e}"),
    };
    if let Err(e) = writer.write(InternalValue::from_components(
        b"a",
        b"v",
        0,
        ValueType::Value,
    )) {
        panic!("write should succeed: {e}");
    }
    let _writer = writer.use_index_block_restart_interval(2);
}

#[test]
#[should_panic(expected = "partitioned index must be configured before writing starts")]
fn writer_rejects_partitioned_index_switch_after_write() {
    let dir = match tempfile::tempdir() {
        Ok(dir) => dir,
        Err(e) => panic!("tempdir should be created: {e}"),
    };
    let path = dir.path().join("1");
    let mut writer = match Writer::new(path, 1, 0, Arc::new(StdFs)) {
        Ok(writer) => writer,
        Err(e) => panic!("writer should be created: {e}"),
    };
    if let Err(e) = writer.write(InternalValue::from_components(
        b"a",
        b"v",
        0,
        ValueType::Value,
    )) {
        panic!("write should succeed: {e}");
    }
    let _writer = writer.use_partitioned_index();
}

#[test]
fn writer_meta_partition_size_is_chainable_with_full_index_writer() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("full-index");
    let mut writer = Writer::new(path, 1, 0, Arc::new(StdFs))?.use_meta_partition_size(8_192);

    writer.write(InternalValue::from_components(
        b"k",
        b"v",
        0,
        ValueType::Value,
    ))?;
    writer.spill_block()?;

    Ok(())
}

#[test]
#[should_panic(expected = "partitioned filter must be configured before writing starts")]
fn writer_rejects_partitioned_filter_switch_after_write() {
    let dir = match tempfile::tempdir() {
        Ok(dir) => dir,
        Err(e) => panic!("tempdir should be created: {e}"),
    };
    let path = dir.path().join("1");
    let mut writer = match Writer::new(path, 1, 0, Arc::new(StdFs)) {
        Ok(writer) => writer,
        Err(e) => panic!("writer should be created: {e}"),
    };
    if let Err(e) = writer.write(InternalValue::from_components(
        b"a",
        b"v",
        0,
        ValueType::Value,
    )) {
        panic!("write should succeed: {e}");
    }
    let _writer = writer.use_partitioned_filter();
}

/// A block re-emitted through the verbatim columnar path can hold several MVCC
/// versions of one user key (same key, descending seqno). Unlike bulk ingest,
/// that path must NOT reject equal user keys — only strictly-unique keys are an
/// ingest contract. Regression for the verbatim salvage re-emit path.
#[cfg(feature = "columnar")]
#[test]
fn write_columnar_block_verbatim_accepts_mvcc_duplicate_keys() -> crate::Result<()> {
    use crate::comparator::default_comparator;
    use crate::table::columnar::entries_to_column_batch;

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("1");
    let cmp = default_comparator();
    let mut writer = Writer::new(path, 1, 0, Arc::new(StdFs))?.use_columnar(true);

    // Two MVCC versions of "dup" (valid block order: user key ascending, seqno
    // descending within a key) — NOT strictly unique.
    let entries = alloc::vec![
        InternalValue::from_components(b"dup".to_vec(), b"v3".to_vec(), 3, ValueType::Value),
        InternalValue::from_components(b"dup".to_vec(), b"v1".to_vec(), 1, ValueType::Value),
    ];
    let batch = entries_to_column_batch(&entries)?;
    writer.write_columnar_block_verbatim(&batch, &cmp)?;
    assert!(
        writer.finish()?.is_some(),
        "the verbatim block writes and finishes"
    );
    Ok(())
}

/// When the same user key spans two consecutive direct blocks, the boundary
/// must keep the internal order `(user_key asc, seqno desc)`: a second block
/// whose FIRST version of the shared key carries a seqno >= the previous
/// block's LAST version is a tampered / malformed block and must be rejected,
/// exactly like an in-block inversion. A correctly descending boundary still
/// passes.
#[cfg(feature = "columnar")]
#[test]
fn write_columnar_block_verbatim_rejects_an_equal_key_boundary_seqno_inversion() -> crate::Result<()>
{
    use crate::comparator::default_comparator;
    use crate::table::columnar::entries_to_column_batch;

    let dir = tempfile::tempdir()?;
    let cmp = default_comparator();

    // Inverted boundary: block 1 ends with ("dup", 5); block 2 begins with
    // ("dup", 6) — a NEWER version sorting after an older one.
    let mut writer = Writer::new(dir.path().join("inv"), 1, 0, Arc::new(StdFs))?.use_columnar(true);
    let first = entries_to_column_batch(&alloc::vec![InternalValue::from_components(
        b"dup".to_vec(),
        b"v5".to_vec(),
        5,
        ValueType::Value,
    )])?;
    writer.write_columnar_block_verbatim(&first, &cmp)?;
    let inverted = entries_to_column_batch(&alloc::vec![InternalValue::from_components(
        b"dup".to_vec(),
        b"v6".to_vec(),
        6,
        ValueType::Value,
    )])?;
    assert!(
        writer
            .write_columnar_block_verbatim(&inverted, &cmp)
            .is_err(),
        "an equal-key boundary whose seqno does not decrease is rejected",
    );

    // Valid boundary: the shared key's versions keep strictly decreasing
    // across the block edge.
    let mut ok_writer =
        Writer::new(dir.path().join("ok"), 1, 0, Arc::new(StdFs))?.use_columnar(true);
    let first = entries_to_column_batch(&alloc::vec![InternalValue::from_components(
        b"dup".to_vec(),
        b"v5".to_vec(),
        5,
        ValueType::Value,
    )])?;
    ok_writer.write_columnar_block_verbatim(&first, &cmp)?;
    let descending = entries_to_column_batch(&alloc::vec![InternalValue::from_components(
        b"dup".to_vec(),
        b"v3".to_vec(),
        3,
        ValueType::Value,
    )])?;
    ok_writer.write_columnar_block_verbatim(&descending, &cmp)?;
    assert!(
        ok_writer.finish()?.is_some(),
        "a correctly descending equal-key boundary still writes and finishes",
    );
    Ok(())
}

/// The columnar bulk-ingest contract is enforced by `write_columnar_batch`:
/// a row-mode writer, a non-zero per-row seqno, or non-increasing keys are each
/// rejected before any block is written.
#[cfg(feature = "columnar")]
#[test]
fn write_columnar_batch_enforces_the_ingest_contract() -> crate::Result<()> {
    use crate::comparator::default_comparator;
    use crate::table::columnar::entries_to_column_batch;

    let dir = tempfile::tempdir()?;
    let cmp = default_comparator();
    let good = entries_to_column_batch(&alloc::vec![InternalValue::from_components(
        b"a".to_vec(),
        b"v".to_vec(),
        0,
        ValueType::Value,
    )])?;

    // 1. Row-mode writer (no `use_columnar`) rejects a columnar batch.
    let mut row_writer = Writer::new(dir.path().join("row"), 1, 0, Arc::new(StdFs))?;
    assert!(
        row_writer.write_columnar_batch(&good, &cmp).is_err(),
        "a columnar batch on a row-mode writer is rejected",
    );

    // 2. A non-zero per-row seqno is rejected (ingest assigns the seqno).
    let mut w2 = Writer::new(dir.path().join("s"), 1, 0, Arc::new(StdFs))?.use_columnar(true);
    let nonzero = entries_to_column_batch(&alloc::vec![InternalValue::from_components(
        b"a".to_vec(),
        b"v".to_vec(),
        7,
        ValueType::Value,
    )])?;
    assert!(
        w2.write_columnar_batch(&nonzero, &cmp).is_err(),
        "a non-zero per-row seqno is rejected on bulk ingest",
    );

    // 3. Non-increasing keys within the batch are rejected.
    let mut w3 = Writer::new(dir.path().join("k"), 1, 0, Arc::new(StdFs))?.use_columnar(true);
    let unsorted = entries_to_column_batch(&alloc::vec![
        InternalValue::from_components(b"b".to_vec(), b"v".to_vec(), 0, ValueType::Value),
        InternalValue::from_components(b"a".to_vec(), b"v".to_vec(), 0, ValueType::Value),
    ])?;
    assert!(
        w3.write_columnar_batch(&unsorted, &cmp).is_err(),
        "non-increasing keys are rejected on bulk ingest",
    );
    Ok(())
}

/// A columnar batch is written and registered before its locator slots are
/// folded in; the estimates the table rotates on must still count them.
#[cfg(feature = "columnar")]
#[test]
fn a_columnar_batch_leaves_the_estimates_current() -> crate::Result<()> {
    use crate::comparator::default_comparator;
    use crate::config::{LocatorPolicyEntry, LocatorPrecision};
    use crate::table::columnar::entries_to_column_batch;

    let dir = tempfile::tempdir()?;
    let mut writer = Writer::new(dir.path().join("1"), 1, 0, Arc::new(StdFs))?
        .use_columnar(true)
        .use_locator(LocatorPolicyEntry::Enabled {
            precision: LocatorPrecision::Entry,
            block_id_bits: None,
            slot_bits: None,
        });
    // One row group: its slots are the table's first, so they grow the
    // locator's width from nothing.
    let entries: alloc::vec::Vec<InternalValue> = (0..300u32)
        .map(|i| {
            InternalValue::from_components(
                format!("key{i:06}").into_bytes(),
                b"v".to_vec(),
                0,
                ValueType::Value,
            )
        })
        .collect();
    writer.write_columnar_batch(&entries_to_column_batch(&entries)?, &default_comparator())?;
    assert_eq!(writer.meta.data_block_count, 1);
    let (held, hint) = (writer.held_state_bytes(), writer.output_size_hint());
    writer.refresh_state_estimates();
    assert_eq!(held, writer.held_state_bytes(), "held state");
    assert_eq!(hint, writer.output_size_hint(), "size hint");
    Ok(())
}

/// Columnar bulk ingest with an `Entry`-precision locator records a per-key
/// locator slot for every distinct key (the per-entry-index arm of the direct
/// block accounting).
#[cfg(feature = "columnar")]
#[test]
fn write_columnar_batch_records_entry_precision_locator() -> crate::Result<()> {
    use crate::comparator::default_comparator;
    use crate::config::{LocatorPolicyEntry, LocatorPrecision};
    use crate::table::columnar::entries_to_column_batch;

    let dir = tempfile::tempdir()?;
    let cmp = default_comparator();
    let mut writer = Writer::new(dir.path().join("1"), 1, 0, Arc::new(StdFs))?
        .use_columnar(true)
        .use_locator(LocatorPolicyEntry::Enabled {
            precision: LocatorPrecision::Entry,
            block_id_bits: None,
            slot_bits: None,
        });
    let entries: alloc::vec::Vec<InternalValue> = (0..8u32)
        .map(|i| {
            InternalValue::from_components(
                format!("key{i:03}").into_bytes(),
                b"v".to_vec(),
                0,
                ValueType::Value,
            )
        })
        .collect();
    let batch = entries_to_column_batch(&entries)?;
    writer.write_columnar_batch(&batch, &cmp)?;

    // Validate the recorded locator slots, not just that finish() succeeds. All
    // eight keys land in the single direct block (ordinal 0), and Entry
    // precision records each key's row index as its slot, so the accumulated
    // triples are exactly `(hash64(key), 0, row)`.
    assert_eq!(
        writer.locators.len(),
        entries.len(),
        "one locator slot per distinct key",
    );
    for (row, recorded) in writer.locators.iter().enumerate() {
        let key = format!("key{row:03}");
        assert_eq!(
            *recorded,
            (crate::hash::hash64(key.as_bytes()), 0, row as u64),
            "key at row {row} maps to (hash, direct block 0, slot == row index)",
        );
    }

    assert!(
        writer.finish()?.is_some(),
        "the columnar SST with an entry-precision locator finishes",
    );
    Ok(())
}

/// The default must stay the codec's own behaviour: whoever asks for level 22
/// is asking for ratio, and gets it without opting into anything. Pinned here
/// because the cost of the opposite default is silent: the output stays valid,
/// only slightly larger, so nothing else would notice the flip.
#[cfg(zstd_any)]
#[test]
fn writer_keeps_the_two_pass_seed_on_by_default_across_subwriter_swaps() -> crate::Result<()> {
    assert!(
        crate::runtime_config::RuntimeConfig::default().zstd_two_pass_seed,
        "the runtime config ships with the seed on",
    );

    let dir = tempfile::tempdir()?;
    let writer = Writer::new(dir.path().join("1"), 1, 0, Arc::new(StdFs))?;
    assert!(
        writer.zstd_two_pass_seed,
        "a fresh table writer starts with the seed on",
    );

    // Selecting another index or filter layout replaces the subwriter, and the
    // default has to survive that too.
    let writer = writer.use_partitioned_index().use_partitioned_filter();
    assert!(
        writer.zstd_two_pass_seed,
        "swapping subwriters leaves the default in force",
    );

    Ok(())
}

/// The layouts whose finish-time sections are estimated differently.
#[derive(Clone, Copy, Debug)]
enum StateLayout {
    Full,
    Partitioned,
    Locator,
}

fn state_writer(path: crate::path::PathBuf, layout: StateLayout) -> crate::Result<Writer> {
    let writer = Writer::new(path, 1, 0, Arc::new(StdFs))?;
    Ok(match layout {
        StateLayout::Full => writer,
        StateLayout::Partitioned => writer.use_partitioned_index().use_partitioned_filter(),
        StateLayout::Locator => writer.use_locator(crate::config::LocatorPolicyEntry::Enabled {
            precision: crate::config::LocatorPrecision::Restart,
            block_id_bits: None,
            slot_bits: None,
        }),
    })
}

/// Writes `n` keys with `value_len`-byte values and spills the last block.
fn write_keys(writer: &mut Writer, n: u32, value_len: usize) -> crate::Result<()> {
    let value = alloc::vec![0x5a; value_len];
    for i in 0..n {
        writer.write(InternalValue::from_components(
            format!("key{i:010}").into_bytes(),
            value.clone(),
            0,
            ValueType::Value,
        ))?;
    }
    writer.spill_block()
}

/// A writer that has seen no key holds no state for `finish` and expects it to
/// append nothing past what is on disk.
#[test]
fn a_fresh_writer_holds_no_state_for_finish() -> crate::Result<()> {
    for layout in [
        StateLayout::Full,
        StateLayout::Partitioned,
        StateLayout::Locator,
    ] {
        let dir = tempfile::tempdir()?;
        let writer = state_writer(dir.path().join("1"), layout)?;
        assert_eq!(writer.held_state_bytes(), 0, "{layout:?}");
        assert_eq!(writer.finish_metadata_bytes, 0, "{layout:?}");
    }
    Ok(())
}

/// A table rotates on the size hint before `finish` has written its filter,
/// index and locator, so the hint must already count them: it lands close to
/// the finished file, which only adds the meta block and the table of contents.
#[test]
fn the_size_hint_before_finish_is_close_to_the_finished_table() -> crate::Result<()> {
    for layout in [
        StateLayout::Full,
        StateLayout::Partitioned,
        StateLayout::Locator,
    ] {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("1");
        let mut writer = state_writer(path.clone(), layout)?;
        write_keys(&mut writer, 50_000, 8)?;
        let hint = writer.output_size_hint();
        let data = *writer.meta.file_pos;
        assert!(
            hint > data,
            "{layout:?}: the hint counts the sections to come"
        );
        writer.finish()?;
        let size = std::fs::metadata(&path)?.len();
        assert!(
            hint * 10 >= size * 9 && hint * 10 <= size * 11,
            "{layout:?}: hint {hint} for a {size}-byte table",
        );
    }
    Ok(())
}

/// A table rotates on the estimates as they stand after its last block, so
/// they must already count that block's index entry and section entries:
/// recomputing them changes nothing.
#[test]
fn the_estimates_count_the_block_just_written() -> crate::Result<()> {
    for layout in [
        StateLayout::Full,
        StateLayout::Partitioned,
        StateLayout::Locator,
    ] {
        let dir = tempfile::tempdir()?;
        let mut writer = state_writer(dir.path().join("1"), layout)?
            .use_zone_map(true)
            .use_seqno_in_index(true);
        write_keys(&mut writer, 1_000, 8)?;
        let (held, hint) = (writer.held_state_bytes(), writer.output_size_hint());
        writer.refresh_state_estimates();
        assert_eq!(held, writer.held_state_bytes(), "{layout:?}: held state");
        assert_eq!(hint, writer.output_size_hint(), "{layout:?}: size hint");
    }
    Ok(())
}

/// The state a table holds for `finish` grows with its keys, not with its
/// bytes: the same keys with values a hundred times larger hold about the same
/// state, while their rows are twenty times larger. Only the index grows with the data,
/// by one entry per block.
#[test]
fn held_state_follows_the_keys_not_the_value_bytes() -> crate::Result<()> {
    for layout in [
        StateLayout::Full,
        StateLayout::Partitioned,
        StateLayout::Locator,
    ] {
        let dir = tempfile::tempdir()?;
        let mut small = state_writer(dir.path().join("1"), layout)?;
        let mut large = state_writer(dir.path().join("2"), layout)?;
        write_keys(&mut small, 20_000, 4)?;
        write_keys(&mut large, 20_000, 400)?;
        let (small_state, large_state) = (small.held_state_bytes(), large.held_state_bytes());
        assert!(
            small_state > 20_000 * 8,
            "{layout:?}: {small_state} bytes for 20 000 keys"
        );
        assert!(
            large_state < 2 * small_state,
            "{layout:?}: {large_state} bytes held for 400-byte values, {small_state} for 4-byte",
        );
        // 13-byte keys: rows of 17 and 413 bytes.
        assert!(
            large.output_size_hint() > 10 * small.output_size_hint(),
            "{layout:?}: the data grew with the values",
        );
    }
    Ok(())
}

/// Writes `n` keys of `key_len` bytes with 8-byte values and spills the last
/// block.
fn write_long_keys(writer: &mut Writer, n: u32, key_len: usize) -> crate::Result<()> {
    for i in 0..n {
        writer.write(InternalValue::from_components(
            format!("{i:0key_len$}").into_bytes(),
            b"value---".to_vec(),
            0,
            ValueType::Value,
        ))?;
    }
    writer.spill_block()
}

/// The size hint before `finish` lands within 10% of the finished table.
fn assert_hint_matches_the_table(writer: Writer, path: &std::path::Path) -> crate::Result<()> {
    let hint = writer.output_size_hint();
    writer.finish()?;
    let size = std::fs::metadata(path)?.len();
    assert!(
        hint * 10 >= size * 9 && hint * 10 <= size * 11,
        "hint {hint} for a {size}-byte table",
    );
    Ok(())
}

/// The top-level index is written twice, at its place and mirrored at the
/// tail. Small blocks under long keys make it a large share of the table, so
/// the hint lands near the finished size only if both copies are counted.
#[test]
fn the_size_hint_counts_the_mirrored_index() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("1");
    let mut writer = Writer::new(path.clone(), 1, 0, Arc::new(StdFs))?.use_data_block_size(64);
    write_long_keys(&mut writer, 5_000, 200)?;
    assert_hint_matches_the_table(writer, &path)
}

/// A prefix shared by every key hashes to the same token once per key. The
/// filter is built from distinct tokens, so the estimates, counted from every
/// buffered token, stay above what `finish` builds and writes.
#[test]
fn a_prefix_shared_by_every_key_is_built_once() -> crate::Result<()> {
    struct UpToColon;
    impl crate::prefix::PrefixExtractor for UpToColon {
        fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
            let end = key.iter().position(|b| *b == b':').map_or(0, |i| i + 1);
            Box::new(key.get(..end).into_iter())
        }
    }
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("1");
    let mut writer = Writer::new(path.clone(), 1, 0, Arc::new(StdFs))?
        .use_prefix_extractor(Some(Arc::new(UpToColon)));
    for i in 0..20_000u32 {
        writer.write(InternalValue::from_components(
            format!("p:{i:08}").into_bytes(),
            b"v".to_vec(),
            0,
            ValueType::Value,
        ))?;
    }
    writer.spill_block()?;
    let hint = writer.output_size_hint();
    writer.finish()?;
    let size = std::fs::metadata(&path)?.len();
    assert!(hint >= size, "hint {hint} for a {size}-byte table");
    Ok(())
}

/// Every table ends with sections `finish` always writes: two copies of the
/// meta block, the version byte, the 4 KiB separator between them, the table
/// of contents and the trailer. A table of a few keys is mostly these.
#[test]
fn the_size_hint_counts_the_tail_every_table_writes() -> crate::Result<()> {
    for key_len in [8, 1_000] {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("1");
        let mut writer = Writer::new(path.clone(), 1, 0, Arc::new(StdFs))?;
        write_long_keys(&mut writer, 3, key_len)?;
        assert_hint_matches_the_table(writer, &path)?;
    }
    Ok(())
}

/// A table may close before its first block is cut, when what the multi-writer
/// adds at rotation fills it: the tail is counted from the first key on.
#[test]
fn the_size_hint_counts_the_tail_before_a_block_is_cut() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut writer = Writer::new(dir.path().join("1"), 1, 0, Arc::new(StdFs))?;
    writer.write(InternalValue::from_components(
        b"key".to_vec(),
        b"v".to_vec(),
        0,
        ValueType::Value,
    ))?;
    assert_eq!(writer.meta.data_block_count, 0);
    assert!(
        writer.output_size_hint() > FIXED_TAIL_LEN,
        "hint {} before the first block",
        writer.output_size_hint(),
    );
    Ok(())
}

/// A large block gathers filter and locator state for many keys before it is
/// cut; the estimates count that state within the block, not only once the
/// block is written.
#[test]
fn the_estimates_follow_the_keys_of_a_block_not_yet_cut() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut writer =
        Writer::new(dir.path().join("1"), 1, 0, Arc::new(StdFs))?.use_data_block_size(4 << 20);
    for i in 0..20_000u32 {
        writer.write(InternalValue::from_components(
            format!("key{i:06}").into_bytes(),
            b"v".to_vec(),
            0,
            ValueType::Value,
        ))?;
    }
    assert_eq!(writer.meta.data_block_count, 0);
    // One 8-byte hash per key, less the keys since the last refresh.
    assert!(
        writer.held_state_bytes() >= 19_000 * 8,
        "{} held for 20 000 keys",
        writer.held_state_bytes(),
    );
    Ok(())
}

/// `finish` encodes each section into a buffer it keeps to the end and frames
/// it into another, while the section itself is still held: a large section
/// counts three times at that point.
#[test]
fn the_held_state_counts_the_section_encoding_buffers() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut writer = Writer::new(dir.path().join("1"), 1, 0, Arc::new(StdFs))?
        .use_data_block_size(256)
        .use_zone_map(true);
    write_long_keys(&mut writer, 5_000, 200)?;
    assert!(
        writer.held_state_bytes() >= 3 * writer.zone_map_bytes,
        "{} held for {} bytes of zone-map bounds",
        writer.held_state_bytes(),
        writer.zone_map_bytes,
    );
    Ok(())
}

/// A delete bitmap stores each touched chunk with its index, kind and count,
/// so sparse deletes cost more than their row count: both estimates count the
/// bitmap as it will be encoded.
#[test]
fn the_estimates_count_a_sparse_delete_bitmap_as_encoded() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut writer = Writer::new(dir.path().join("1"), 1, 0, Arc::new(StdFs))?.use_zone_map(true);
    write_keys(&mut writer, 100, 8)?;
    let (held, hint) = (writer.held_state_bytes(), writer.output_size_hint());
    for chunk in 0..1_000 {
        writer
            .delete_bitmap_mut()
            .insert(chunk * crate::table::delete_bitmap::CHUNK_ROWS);
    }
    writer.refresh_state_estimates();
    let encoded = writer.delete_bitmap.encode().len() as u64;
    assert!(
        writer.output_size_hint() - hint >= encoded,
        "the hint grew by {} for a {encoded}-byte bitmap",
        writer.output_size_hint() - hint,
    );
    assert!(writer.held_state_bytes() - held >= encoded);
    Ok(())
}

/// Zone-map entries own copies of each block's bounds; under long keys those
/// dominate the section, and both estimates have to count them.
#[test]
fn the_estimates_count_the_zone_map_bounds() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("1");
    let mut writer = Writer::new(path.clone(), 1, 0, Arc::new(StdFs))?
        .use_data_block_size(256)
        .use_zone_map(true);
    write_long_keys(&mut writer, 5_000, 200)?;
    let blocks = writer.zone_map_section.len() as u64;
    assert!(
        writer.held_state_bytes() > blocks * 2 * 200,
        "{} bytes held for {blocks} blocks of 200-byte bounds",
        writer.held_state_bytes(),
    );
    assert_hint_matches_the_table(writer, &path)
}

/// Under page ECC every block `finish` writes carries a parity trailer, which
/// the hint has to count: long keys over small blocks make the index a large
/// share of the table.
#[cfg(feature = "page_ecc")]
#[test]
fn the_size_hint_counts_the_parity_of_the_blocks_to_come() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("1");
    let mut writer = Writer::new(path.clone(), 1, 0, Arc::new(StdFs))?
        .use_data_block_size(64)
        .use_ecc(Some(crate::table::block::EccParams::RS_4_2));
    write_long_keys(&mut writer, 5_000, 200)?;
    assert_hint_matches_the_table(writer, &path)
}

/// A table of exactly `2^k` blocks fits explicit `k`-bit block ids, so its
/// locator section is written and both estimates count it, although the next
/// block ordinal no longer fits.
#[test]
fn a_locator_filled_to_its_last_block_id_is_counted() -> crate::Result<()> {
    let locator = crate::config::LocatorPolicyEntry::Enabled {
        precision: crate::config::LocatorPrecision::Block,
        block_id_bits: Some(1),
        slot_bits: None,
    };
    let dir = tempfile::tempdir()?;
    let mut with = Writer::new(dir.path().join("1"), 1, 0, Arc::new(StdFs))?.use_locator(locator);
    let mut without = Writer::new(dir.path().join("2"), 1, 0, Arc::new(StdFs))?;
    for writer in [&mut with, &mut without] {
        for block in 0..2 {
            for i in 0..10 {
                writer.write(InternalValue::from_components(
                    format!("key{block}{i:02}").into_bytes(),
                    b"value---".to_vec(),
                    0,
                    ValueType::Value,
                ))?;
            }
            writer.spill_block()?;
        }
    }
    assert_eq!(with.meta.data_block_count, 2);
    assert!(
        with.held_state_bytes() > without.held_state_bytes(),
        "{} held with the locator, {} without",
        with.held_state_bytes(),
        without.held_state_bytes(),
    );
    assert!(with.output_size_hint() > without.output_size_hint());
    Ok(())
}

/// A compaction's lineage lists its inputs and stays in the writer to the end;
/// the meta block then copies it into the parameters, the encoded ids and the
/// meta entry, so the held state counts it once held and again encoded.
#[test]
fn the_held_state_counts_the_lineage_and_its_meta_encoding() -> crate::Result<()> {
    const INPUTS: u64 = 100_000;
    let dir = tempfile::tempdir()?;
    let mut writer = Writer::new(dir.path().join("1"), 1, 0, Arc::new(StdFs))?
        .use_lineage(Some((0..INPUTS).collect()));
    writer.write(InternalValue::from_components(
        b"key".to_vec(),
        b"v".to_vec(),
        0,
        ValueType::Value,
    ))?;
    writer.spill_block()?;
    let ids = INPUTS * 8;
    assert!(
        writer.held_state_bytes() >= 4 * ids,
        "{} held for a lineage of {ids} bytes",
        writer.held_state_bytes(),
    );
    Ok(())
}

/// Blocks in flight on the parallel pipeline are counted by the frames they
/// will be written as, not by their payload: once drained, the bytes they
/// take on disk stay within what the estimate counted for them.
#[cfg(feature = "parallel")]
#[test]
fn blocks_in_flight_are_counted_by_their_frames() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let spawner = Arc::new(super::RayonSpawner::with_threads(4)?);
    let mut writer = Writer::new(dir.path().join("1"), 1, 0, Arc::new(StdFs))?
        .use_data_block_compression(crate::CompressionType::None)
        .use_parallel_compression(spawner, 4);
    for block in 0..3u32 {
        for i in 0..10u32 {
            writer.write(InternalValue::from_components(
                format!("key{block}{i:02}").into_bytes(),
                b"value---".to_vec(),
                0,
                ValueType::Value,
            ))?;
        }
        writer.spill_block()?;
    }
    assert_eq!(*writer.meta.file_pos, 0, "all three blocks are in flight");
    let counted = writer.output_size_hint() - writer.finish_metadata_bytes;
    for _ in 0..3 {
        writer.drain_one_parallel()?;
    }
    assert_eq!(writer.meta.data_block_count, 3);
    assert!(
        *writer.meta.file_pos <= counted,
        "{} bytes written for {counted} counted in flight",
        *writer.meta.file_pos,
    );
    Ok(())
}

/// Within a block not yet cut, an entry-precise locator records a slot per key,
/// so its section needs slot bits a block-precise one does not: the estimates
/// count the open block's slots, not only those of blocks already cut.
#[test]
fn the_estimates_count_the_locator_slots_of_a_block_not_yet_cut() -> crate::Result<()> {
    let locator = |precision| crate::config::LocatorPolicyEntry::Enabled {
        precision,
        block_id_bits: None,
        slot_bits: None,
    };
    let dir = tempfile::tempdir()?;
    let hint = |name: &str, precision| -> crate::Result<u64> {
        let mut writer = Writer::new(dir.path().join(name), 1, 0, Arc::new(StdFs))?
            .use_data_block_size(4 << 20)
            .use_locator(locator(precision));
        for i in 0..20_000u32 {
            writer.write(InternalValue::from_components(
                format!("key{i:06}").into_bytes(),
                b"v".to_vec(),
                0,
                ValueType::Value,
            ))?;
        }
        assert_eq!(writer.meta.data_block_count, 0);
        Ok(writer.output_size_hint())
    };
    let entry = hint("1", crate::config::LocatorPrecision::Entry)?;
    let block = hint("2", crate::config::LocatorPrecision::Block)?;
    // Slots up to 19 999 take 15 bits a key; count at least 14.
    assert!(
        entry >= block + 20_000 * 14 / 8,
        "entry-precise hint {entry} against block-precise {block}",
    );
    Ok(())
}

/// Only a key's newest version gets a locator entry, so blocks holding older
/// versions alone add no block id. Explicit widths that fit every recorded id
/// keep the locator however many such blocks follow.
#[test]
fn blocks_of_older_versions_do_not_outgrow_the_locator() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut writer = Writer::new(dir.path().join("1"), 1, 0, Arc::new(StdFs))?.use_locator(
        crate::config::LocatorPolicyEntry::Enabled {
            precision: crate::config::LocatorPrecision::Block,
            block_id_bits: Some(1),
            slot_bits: None,
        },
    );
    // One key, its versions spread over four blocks.
    for block in 0..4_u64 {
        for version in 0..10 {
            writer.write(InternalValue::from_components(
                b"key".to_vec(),
                b"value---".to_vec(),
                100 - (block * 10 + version),
                ValueType::Value,
            ))?;
        }
        writer.spill_block()?;
    }
    assert_eq!(writer.meta.data_block_count, 4);
    assert_eq!(writer.locators.len(), 1, "the locator was dropped");
    Ok(())
}

/// Explicit locator widths too narrow for the table skip its section at
/// `finish`. The widths only grow with the table, so once they no longer fit
/// the writer holds nothing for the locator and charges nothing for it.
#[test]
fn a_locator_too_narrow_for_the_table_stops_holding_state() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut narrow = Writer::new(dir.path().join("1"), 1, 0, Arc::new(StdFs))?
        .use_data_block_size(64)
        .use_locator(crate::config::LocatorPolicyEntry::Enabled {
            precision: crate::config::LocatorPrecision::Block,
            block_id_bits: Some(1),
            slot_bits: None,
        });
    let mut plain =
        Writer::new(dir.path().join("2"), 1, 0, Arc::new(StdFs))?.use_data_block_size(64);
    write_keys(&mut narrow, 5_000, 8)?;
    write_keys(&mut plain, 5_000, 8)?;
    assert!(
        narrow.locators.is_empty(),
        "{} triples held",
        narrow.locators.len()
    );
    assert_eq!(narrow.held_state_bytes(), plain.held_state_bytes());
    assert_eq!(narrow.output_size_hint(), plain.output_size_hint());
    Ok(())
}
