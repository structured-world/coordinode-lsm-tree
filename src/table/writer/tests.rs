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
