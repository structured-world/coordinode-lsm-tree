use super::salvage_sst;
use crate::comparator::default_comparator;
use crate::fs::{Fs, StdFs};
use crate::table::{Table, Writer};
use crate::{InternalValue, ValueType};
use alloc::sync::Arc;
use tempfile::tempdir;
use test_log::test;

fn iv(i: u32) -> InternalValue {
    InternalValue::from_components(
        format!("key{i:05}").into_bytes(),
        format!("val{i:05}").into_bytes(),
        1,
        ValueType::Value,
    )
}

/// Opens an SST as a `Table`, stamping the open with the file's current digest
/// (the source may be corrupt; per-block checksums catch the actual damage).
fn open(path: std::path::PathBuf, fs: &Arc<dyn Fs>) -> crate::Result<Table> {
    let checksum = crate::verify::stream_checksum(&path)?;
    Table::recover(
        path,
        checksum,
        0,
        0,
        0,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(fs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )
}

/// A reopen of a salvaged SST: recover it and return its live item count.
fn reopen_item_count(path: std::path::PathBuf, fs: &Arc<dyn Fs>) -> crate::Result<u64> {
    Ok(open(path, fs)?.metadata.item_count)
}

#[test]
fn salvage_of_a_healthy_sst_recovers_every_block() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // Build a multi-block source SST: small data blocks force several blocks so
    // the per-block walk has more than one block to recover.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    let report = salvage_sst(&source, dest.clone(), &fs)?;

    assert!(
        report.is_complete(),
        "a healthy SST salvages with no dropped blocks: {report:?}",
    );
    assert!(
        report.blocks_total >= 2,
        "256-byte blocks over 200 entries should yield several data blocks, got {}",
        report.blocks_total,
    );
    assert_eq!(
        report.blocks_salvaged, report.blocks_total,
        "every block of a healthy SST is salvaged",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every entry is recovered",
    );
    assert_eq!(
        report.salvaged_path.as_deref(),
        Some(dest.as_path()),
        "a salvaged file is written when at least one block is recovered",
    );

    // The salvaged copy is a valid SST that reopens and holds every key.
    assert_eq!(
        reopen_item_count(dest, &fs)?,
        u64::from(n),
        "the salvaged SST reopens with the full item count",
    );
    Ok(())
}

/// One deliberately corrupted data block: salvage drops exactly that block
/// (naming its key range) and recovers every other block, instead of failing
/// the whole file. This is the core block-granular contract.
#[test]
fn salvage_drops_a_corrupted_block_and_keeps_the_rest() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Resolve the second data block's on-disk offset from the (intact) index,
    // then flip a byte a little past its header so the block's data checksum
    // fails on load. load_data_block reads the block by the index handle's size,
    // so the corruption surfaces as that one block failing, not a desync.
    let target = {
        let table = open(source.clone(), &fs)?;
        let offsets: alloc::vec::Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|kh| *kh.as_ref().offset())
            .collect();
        let Some(&second) = offsets.get(1) else {
            panic!("source SST must have at least two data blocks, got {offsets:?}");
        };
        second
    };
    let flip = usize::try_from(target).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;

    assert!(
        !report.is_complete(),
        "a corrupted block must be reported as dropped: {report:?}",
    );
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the one corrupted block is dropped: {report:?}",
    );
    assert_eq!(
        report.blocks_salvaged,
        report.blocks_total - 1,
        "every block but the corrupted one is recovered",
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );
    assert!(
        report
            .dropped
            .first()
            .is_some_and(|d| d.key_range.is_some()),
        "the dropped block names the key range it lost",
    );
    assert_eq!(report.salvaged_path.as_deref(), Some(dest.as_path()));

    // The salvaged copy reopens and holds exactly the recovered entries.
    assert_eq!(
        reopen_item_count(dest, &fs)?,
        report.entries_salvaged,
        "the salvaged SST holds exactly the entries the report counted",
    );
    Ok(())
}

/// A columnar source with one corrupted PAX data block: the columnar loader
/// fails to reconstruct that block (a torn sub-column frame), so salvage drops
/// it and recovers every other block, writing the survivors as a plain row SST.
#[cfg(feature = "columnar")]
#[test]
fn salvage_drops_a_corrupted_columnar_block_and_keeps_the_rest() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A columnar SST (PAX blocks + zone map), no deletes so there is no
    // delete-bitmap section to worry about here.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar SST is non-empty"
    );

    // Corrupt the second columnar data block's bytes (offset from the intact
    // index, a little past its header) so its reconstruction fails on load.
    let target = {
        let table = open(source.clone(), &fs)?;
        let offsets: alloc::vec::Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|kh| *kh.as_ref().offset())
            .collect();
        let Some(&second) = offsets.get(1) else {
            panic!("source columnar SST must have at least two data blocks, got {offsets:?}");
        };
        second
    };
    let flip = usize::try_from(target).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;

    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the one corrupted columnar block is dropped: {report:?}",
    );
    assert_eq!(
        report.blocks_salvaged,
        report.blocks_total - 1,
        "every columnar block but the corrupted one is recovered",
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );
    assert_eq!(report.salvaged_path.as_deref(), Some(dest.as_path()));

    // The salvaged copy is a valid (row-format) SST holding the recovered rows.
    assert_eq!(reopen_item_count(dest, &fs)?, report.entries_salvaged);
    Ok(())
}

/// A columnar source carrying deletes whose `delete_bitmap` section is
/// corrupted (data blocks intact): normal recovery refuses to open it (opening
/// would resurrect deleted rows), but salvage degrades to "all rows live" and
/// recovers every block.
#[cfg(feature = "columnar")]
#[test]
fn salvage_tolerates_a_corrupt_delete_bitmap_as_all_live() -> crate::Result<()> {
    use crate::config::DeleteStrategy;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    // Mark a few positions deleted so a delete-bitmap section is co-written.
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Corrupt the middle of the `delete_bitmap` SFA section (the data blocks
    // stay intact, so only the sidecar is damaged).
    let (db_pos, db_len) = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"delete_bitmap") else {
            panic!("source must carry a delete_bitmap section");
        };
        (entry.pos(), entry.len())
    };
    let flip = usize::try_from(db_pos + db_len / 2).unwrap_or(0);
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    // Normal recovery fails closed: a corrupt bitmap would resurrect deleted rows.
    assert!(
        open(source.clone(), &fs).is_err(),
        "normal recovery must fail closed on a corrupt delete-bitmap",
    );

    // Salvage degrades to "all rows live": every block recovers, nothing masked.
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "the data blocks are intact; only the sidecar was corrupt: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every row is recovered live, the corrupt bitmap is ignored",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n));
    Ok(())
}

/// When the source cannot be opened at all (a corrupt SFA trailer makes even
/// salvage-mode recovery fail), `salvage_sst` returns an error rather than
/// writing a partial file.
#[test]
fn salvage_sst_errors_when_the_source_cannot_be_opened() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0..50 {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Truncate away the tail (SFA trailer + section mirrors) so the container is
    // unparseable and even salvage-mode recovery cannot open it.
    let mut bytes = std::fs::read(&source)?;
    bytes.truncate(bytes.len() / 2);
    std::fs::write(&source, &bytes)?;

    assert!(
        salvage_sst(&source, dest.clone(), &fs).is_err(),
        "an unparseable container must fail salvage, not write a partial file",
    );
    assert!(
        !dest.exists(),
        "no destination is written on an open failure"
    );
    Ok(())
}

/// A single-block SST whose only data block is corrupt salvages nothing: no
/// destination file is written and the report records the dropped block.
#[test]
fn salvage_sst_recovers_nothing_when_the_only_block_is_corrupt() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A handful of small keys fit in one default-sized data block.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0..8 {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Corrupt the sole data block (offset from the intact index).
    let target = {
        let table = open(source.clone(), &fs)?;
        let offsets: alloc::vec::Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|kh| *kh.as_ref().offset())
            .collect();
        let Some(&only) = offsets.first() else {
            panic!("expected a single data block, got {offsets:?}");
        };
        assert_eq!(
            offsets.len(),
            1,
            "expected a single data block, got {offsets:?}"
        );
        only
    };
    let flip = usize::try_from(target).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(report.blocks_salvaged, 0, "the only block was corrupt");
    assert_eq!(report.entries_salvaged, 0, "no entries recovered");
    assert_eq!(report.dropped.len(), 1, "the dropped block is reported");
    assert!(
        report.salvaged_path.is_none(),
        "nothing recoverable means no file is written",
    );
    assert!(!dest.exists(), "no destination file on an empty salvage");
    Ok(())
}
