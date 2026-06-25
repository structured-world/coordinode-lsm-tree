use super::{compute_table_checksum, highest_existing_version_id};
use crate::fs::StdFs;
use test_log::test;

#[test]
fn compute_table_checksum_matches_oneshot_xxh3() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("000007");
    // Larger than the 256 KiB read buffer so the chunked read loop is
    // exercised across multiple iterations.
    let payload: Vec<u8> = (0..600_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(&path, &payload)?;

    let got = compute_table_checksum(&StdFs, &path)?;
    let expected = xxhash_rust::xxh3::xxh3_128(&payload);
    assert_eq!(
        got, expected,
        "streamed digest must equal the one-shot xxh3-128 digest",
    );
    Ok(())
}

#[test]
fn highest_existing_version_id_picks_the_max_and_ignores_non_versions() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    for name in ["v2", "v10", "v3", "current", "vNaN", "notaversion"] {
        std::fs::write(dir.path().join(name), b"x")?;
    }
    assert_eq!(highest_existing_version_id(&StdFs, dir.path())?, Some(10));
    Ok(())
}

#[test]
fn highest_existing_version_id_none_when_no_versions_present() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("current"), b"x")?;
    assert_eq!(highest_existing_version_id(&StdFs, dir.path())?, None);
    Ok(())
}

/// `repair_with_salvage` on an SST whose ONLY data block is corrupt: whole-file
/// recovery still succeeds (the data section is read lazily) but verification
/// fails, and block-salvage finds nothing recoverable, so the table is reported
/// unreadable rather than kept as one that errors on every read.
#[test]
fn repair_with_salvage_reports_a_sole_corrupt_block_as_unsalvageable() -> crate::Result<()> {
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // A handful of short keys fit in a single data block: no second block for
    // salvage to fall back on.
    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?;
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Resolve the sole data block's offset from the intact index, then flip a
    // byte just past its header so the block fails its checksum. The container,
    // index and meta stay intact, so whole-file recovery still opens it (data is
    // read lazily) and only verification trips.
    let offset = {
        let checksum = crate::Checksum::from_raw(compute_table_checksum(&*fs, &sst)?);
        let table = crate::table::Table::recover(
            sst.clone(),
            checksum,
            0,
            0,
            0,
            Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
            None,
            Arc::clone(&fs),
            false,
            false,
            None,
            #[cfg(zstd_any)]
            None,
            crate::comparator::default_comparator(),
            #[cfg(feature = "metrics")]
            Arc::new(crate::Metrics::default()),
        )?;
        let offsets: alloc::vec::Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|kh| *kh.as_ref().offset())
            .collect();
        let [only] = offsets.as_slice() else {
            panic!("expected a single data block, got {offsets:?}");
        };
        *only
    };
    let flip = usize::try_from(offset).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(&sst)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.salvaged, 0,
        "the sole block is corrupt: nothing to salvage",
    );
    assert_eq!(report.recovered, 0, "no table joins the rebuilt manifest");
    assert_eq!(
        report.unreadable, 1,
        "the unsalvageable SST is reported: {:?}",
        report.unreadable_files,
    );
    let [(_, reason)] = report.unreadable_files.as_slice() else {
        panic!(
            "expected exactly one unreadable file, got {:?}",
            report.unreadable_files,
        );
    };
    assert!(
        reason.contains("nothing salvageable"),
        "the reason names the empty salvage, got: {reason}",
    );
    Ok(())
}

/// `repair_with_salvage` on a columnar SST whose delete-bitmap AND sole data
/// block are both corrupt: whole-file recovery refuses it (the corrupt bitmap
/// would resurrect deleted rows) and block-salvage, though it opens in salvage
/// mode, finds the one block unreadable, so the table is reported unreadable
/// rather than half-recovered.
#[cfg(feature = "columnar")]
#[test]
fn repair_with_salvage_reports_a_corrupt_bitmap_and_block_sst_as_unsalvageable() -> crate::Result<()>
{
    use crate::config::DeleteStrategy;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // A small columnar SST (single data block) carrying a delete-bitmap.
    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?
            .use_columnar(true)
            .use_zone_map(true)
            .delete_strategy(DeleteStrategy::MergeOnRead);
        for i in 0..8u32 {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        for pos in [2u32, 6] {
            w.delete_bitmap_mut().insert(pos);
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Resolve the sole data block's offset from the intact index before any
    // corruption shifts nothing (the flip is in place, lengths are unchanged).
    let block_offset = {
        let checksum = crate::Checksum::from_raw(compute_table_checksum(&*fs, &sst)?);
        let table = crate::table::Table::recover(
            sst.clone(),
            checksum,
            0,
            0,
            0,
            Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
            None,
            Arc::clone(&fs),
            false,
            false,
            None,
            #[cfg(zstd_any)]
            None,
            crate::comparator::default_comparator(),
            #[cfg(feature = "metrics")]
            Arc::new(crate::Metrics::default()),
        )?;
        let offsets: alloc::vec::Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|kh| *kh.as_ref().offset())
            .collect();
        let [only] = offsets.as_slice() else {
            panic!("expected a single data block, got {offsets:?}");
        };
        *only
    };
    let bitmap = {
        let mut f = std::fs::File::open(&sst)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"delete_bitmap") else {
            panic!("the SST must carry a delete_bitmap section");
        };
        usize::try_from(entry.pos() + entry.len() / 2).unwrap_or(0)
    };

    // Corrupt the sole data block (so salvage recovers nothing) and the bitmap
    // (so whole-file recovery refuses to open it at all).
    let mut bytes = std::fs::read(&sst)?;
    if let Some(b) = bytes.get_mut(usize::try_from(block_offset).unwrap_or(0) + 16) {
        *b ^= 0xFF;
    }
    if let Some(b) = bytes.get_mut(bitmap) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.salvaged, 0,
        "the sole block is corrupt: nothing to salvage"
    );
    assert_eq!(report.recovered, 0, "no table joins the rebuilt manifest");
    let [(_, reason)] = report.unreadable_files.as_slice() else {
        panic!(
            "expected exactly one unreadable file, got {:?}",
            report.unreadable_files,
        );
    };
    assert!(
        reason.contains("nothing salvageable"),
        "the reason names the empty salvage, got: {reason}",
    );
    Ok(())
}

/// `repair_with_salvage` recovers an SST that normal recovery refuses: a
/// columnar segment whose delete-bitmap section is corrupt fails whole-file
/// recovery (it would resurrect deleted rows), but salvage degrades it to "all
/// rows live" and the rebuilt manifest references the recovered table.
#[cfg(feature = "columnar")]
#[test]
fn repair_with_salvage_recovers_a_corrupt_delete_bitmap_sst() -> crate::Result<()> {
    use crate::config::DeleteStrategy;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // A columnar SST (table id 0) carrying a delete-bitmap.
    let n = 200u32;
    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?
            .use_columnar(true)
            .use_zone_map(true)
            .delete_strategy(DeleteStrategy::MergeOnRead);
        for i in 0..n {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        for pos in [5u32, 50, 150] {
            w.delete_bitmap_mut().insert(pos);
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Corrupt the middle of the delete_bitmap section so normal recovery refuses
    // the SST (the data blocks stay intact).
    let (pos, len) = {
        let mut f = std::fs::File::open(&sst)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"delete_bitmap") else {
            panic!("the SST must carry a delete_bitmap section");
        };
        (entry.pos(), entry.len())
    };
    let flip = usize::try_from(pos + len / 2).unwrap_or(0);
    let mut bytes = std::fs::read(&sst)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.salvaged, 1,
        "the corrupt-bitmap SST is salvaged: {:?}",
        report.unreadable_files,
    );
    assert_eq!(report.recovered, 1, "the salvaged table joins the manifest");
    Ok(())
}
