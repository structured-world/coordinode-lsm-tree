use super::{BlobDropReason, DropReason, salvage_blob_file, salvage_sst};
// The options-bearing entry is exercised only by the encrypted / dictionary /
// delete-resurrection salvage tests, which are themselves feature-gated.
#[cfg(any(feature = "encryption", feature = "columnar", zstd_any))]
use super::{SalvageOptions, salvage_sst_with_options};
use crate::comparator::default_comparator;
use crate::fs::{Fs, StdFs};
use crate::table::{Table, Writer};
use crate::{InternalValue, ValueType};
use alloc::sync::Arc;
use tempfile::tempdir;
use test_log::test;

/// Regression: a data block can hold several MVCC versions of one user key
/// (same key, descending seqno). The verbatim copy-through path must accept
/// equal user keys — only columnar *ingest* requires strictly-unique keys — so
/// salvaging such a block recovers every version instead of erroring.
#[test]
fn salvage_recovers_a_block_with_multiple_versions_of_one_key() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // One block holding the same user key at several seqnos, surrounded by unique
    // keys. Valid SST order: user key ascending, seqno descending within a key.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    writer.write(InternalValue::from_components(
        b"a".to_vec(),
        b"a".to_vec(),
        1,
        ValueType::Value,
    ))?;
    for seqno in [3u64, 2, 1] {
        writer.write(InternalValue::from_components(
            b"dup".to_vec(),
            format!("v{seqno}").into_bytes(),
            seqno,
            ValueType::Value,
        ))?;
    }
    writer.write(InternalValue::from_components(
        b"z".to_vec(),
        b"z".to_vec(),
        1,
        ValueType::Value,
    ))?;
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "a healthy SST with MVCC duplicates salvages cleanly: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged, 5,
        "every version is recovered, including all 3 of `dup`: {report:?}",
    );

    let recovered = open(dest, &fs)?;
    assert_eq!(
        recovered.metadata.item_count, 5,
        "all 5 entries (3 versions of `dup`) are recovered",
    );
    Ok(())
}

/// A block where a weak tombstone is immediately followed by a value for the
/// same key (a reclaimable pair) salvages verbatim and recovers both entries —
/// exercising the reclaimable-weak-tombstone accounting on the copy-through path.
#[test]
fn salvage_recovers_a_reclaimable_weak_tombstone_pair() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // SST order is user key ascending, seqno descending: the weak tombstone
    // (higher seqno) precedes the value it reclaims (lower seqno) for `dup`.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    writer.write(InternalValue::from_components(
        b"a".to_vec(),
        b"a".to_vec(),
        1,
        ValueType::Value,
    ))?;
    writer.write(InternalValue::from_components(
        b"dup".to_vec(),
        b"".to_vec(),
        3,
        ValueType::WeakTombstone,
    ))?;
    writer.write(InternalValue::from_components(
        b"dup".to_vec(),
        b"v1".to_vec(),
        1,
        ValueType::Value,
    ))?;
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    let report = salvage_sst(&source, dest, &fs)?;
    assert!(
        report.is_complete(),
        "healthy SST salvages cleanly: {report:?}"
    );
    assert_eq!(
        report.entries_salvaged, 3,
        "the weak tombstone and both values are recovered: {report:?}",
    );
    assert!(
        report.blocks_copied_verbatim >= 1,
        "the clean block is copied verbatim: {report:?}",
    );
    Ok(())
}

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
    open_with_id(path, fs, 0)
}

/// As [`open`] but under an explicit expected table id (the recover
/// cross-checks it against the SST's stored `table_id`).
fn open_with_id(
    path: std::path::PathBuf,
    fs: &Arc<dyn Fs>,
    table_id: crate::TableId,
) -> crate::Result<Table> {
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&**fs, &path)?);
    Table::recover(
        path,
        checksum,
        0,
        0,
        table_id,
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

/// Point-reads `key` from the SST at `path` at the latest snapshot — the
/// LOGICAL visibility check behind the physical row counts (a delete either
/// masks the key or, under the resurrection opt-in, leaves it readable).
fn reopen_get(
    path: std::path::PathBuf,
    fs: &Arc<dyn Fs>,
    key: &[u8],
) -> crate::Result<Option<crate::InternalValue>> {
    open(path, fs)?.get(key, crate::MAX_SEQNO, crate::hash::hash64(key))
}

/// An SST from a KV-separated tree carries a `linked_blob_files` section
/// naming every blob file its `ValueHandle`s point into; blob GC / relocation
/// consults it (via `list_blob_file_references`) to decide whether a blob is
/// still referenced. The salvaged copy must carry the SOURCE's links —
/// omitting the section would let GC rewrite or delete a blob that only this
/// table still references, silently breaking its indirections.
#[test]
fn salvage_preserves_the_source_linked_blob_files() -> crate::Result<()> {
    use crate::AbstractTree;

    let dir = tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A KV-separated tree: large values go to a blob file, the SST holds
    // indirections plus a linked_blob_files section.
    let crate::AnyTree::Blob(tree) = crate::Config::new(
        dir.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
    .open()?
    else {
        unreachable!("kv separation configured");
    };
    let big = |i: u32| format!("{i:08}").repeat(512);
    for i in 0u32..10 {
        tree.insert(format!("key{i:05}"), big(i), u64::from(i) + 1);
    }
    tree.flush_active_memtable(10)?;
    let source = {
        let binding = tree.index.version_history.read().latest_version();
        let Some(table) = binding.version.iter_tables().next() else {
            panic!("flush produced one table");
        };
        (*table.path).clone()
    };
    drop(tree);

    let dest = dir.path().join("salvaged");
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(report.is_complete(), "healthy SST: {report:?}");

    // The source's blob links survive into the recovered copy.
    let Some(source_links) = open(source, &fs)?.list_blob_file_references()? else {
        panic!("the source carries a linked_blob_files section");
    };
    assert!(!source_links.is_empty(), "the source references blob files");
    let Some(recovered_links) = open(dest, &fs)?.list_blob_file_references()? else {
        panic!("the salvaged copy carries a linked_blob_files section");
    };
    assert_eq!(
        recovered_links, source_links,
        "the salvaged copy references the same blob files as the source",
    );
    Ok(())
}

/// A `linked_blob_files` section that PARSES but under-reports its contents
/// (count word forged to 0, record bytes left in place) must not be trusted
/// as the sole source of the recovered copy's links: the recovered entries
/// still hold `ValueHandle` indirections into the blob file, and blob GC /
/// relocation consults the links to decide liveness — an under-reported list
/// would let GC delete or rewrite a blob the copy still references. Salvage
/// derives the links from the recovered indirections and unions them with
/// the source list.
#[test]
fn salvage_rebuilds_blob_links_from_recovered_indirections() -> crate::Result<()> {
    use crate::AbstractTree;

    let dir = tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let crate::AnyTree::Blob(tree) = crate::Config::new(
        dir.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
    .open()?
    else {
        unreachable!("kv separation configured");
    };
    let big = |i: u32| format!("{i:08}").repeat(512);
    for i in 0u32..10 {
        tree.insert(format!("key{i:05}"), big(i), u64::from(i) + 1);
    }
    tree.flush_active_memtable(10)?;
    let source = {
        let binding = tree.index.version_history.read().latest_version();
        let Some(table) = binding.version.iter_tables().next() else {
            panic!("flush produced one table");
        };
        (*table.path).clone()
    };
    drop(tree);

    // The TRUE links, before the forgery.
    let Some(true_links) = open(source.clone(), &fs)?.list_blob_file_references()? else {
        panic!("the source carries a linked_blob_files section");
    };
    assert!(!true_links.is_empty(), "the source references blob files");

    // Forge the count word to 0: the section still parses (the bound check
    // passes trivially) but reports NO links.
    let pos = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader
            .toc()
            .iter()
            .find(|e| e.name() == b"linked_blob_files")
        else {
            panic!("the source must carry a linked_blob_files section");
        };
        usize::try_from(entry.pos()).unwrap_or(usize::MAX)
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(count) = bytes.get_mut(pos..pos + 4) else {
        panic!("linked_blob_files count header within the file");
    };
    count.copy_from_slice(&0u32.to_le_bytes());
    std::fs::write(&source, &bytes)?;

    // Sanity: the forgery took — the source now under-reports.
    let Some(forged) = open(source.clone(), &fs)?.list_blob_file_references()? else {
        panic!("the forged section still parses");
    };
    assert!(forged.is_empty(), "the forged count hides every link");

    let dest = dir.path().join("salvaged");
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(report.is_complete(), "data blocks are healthy: {report:?}");

    // The copy's links are derived from its recovered indirections, not
    // parroted from the forged source list.
    let Some(recovered_links) = open(dest, &fs)?.list_blob_file_references()? else {
        panic!("the salvaged copy must carry links derived from its indirections");
    };
    for link in &true_links {
        assert!(
            recovered_links
                .iter()
                .any(|l| l.blob_file_id == link.blob_file_id),
            "blob file {} is referenced by recovered indirections but missing \
             from the copy's links: {recovered_links:?}",
            link.blob_file_id,
        );
    }
    Ok(())
}

/// A KV-separated source whose `linked_blob_files` section is unreadable (a
/// count header claiming more records than the section holds) fails the
/// salvage — but must NOT leave a partial destination behind: the links are
/// read before anything is created at `dest`, so a failed salvage leaves no
/// stale output for a later repair or retry to trip over.
#[test]
fn salvage_leaves_no_destination_when_the_blob_links_are_unreadable() -> crate::Result<()> {
    use crate::AbstractTree;

    let dir = tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let crate::AnyTree::Blob(tree) = crate::Config::new(
        dir.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
    .open()?
    else {
        unreachable!("kv separation configured");
    };
    let big = |i: u32| format!("{i:08}").repeat(512);
    for i in 0u32..10 {
        tree.insert(format!("key{i:05}"), big(i), u64::from(i) + 1);
    }
    tree.flush_active_memtable(10)?;
    let source = {
        let binding = tree.index.version_history.read().latest_version();
        let Some(table) = binding.version.iter_tables().next() else {
            panic!("flush produced one table");
        };
        (*table.path).clone()
    };
    drop(tree);

    // Overwrite the linked_blob_files count header (leading LE u32) with a
    // value far larger than the section can hold, so parsing the records
    // hits EOF and list_blob_file_references errors.
    let pos = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader
            .toc()
            .iter()
            .find(|e| e.name() == b"linked_blob_files")
        else {
            panic!("the source must carry a linked_blob_files section");
        };
        usize::try_from(entry.pos()).unwrap_or(usize::MAX)
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(count) = bytes.get_mut(pos..pos + 4) else {
        panic!("linked_blob_files count header within the file");
    };
    count.copy_from_slice(&u32::MAX.to_le_bytes());
    std::fs::write(&source, &bytes)?;

    let dest = dir.path().join("salvaged");
    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "unreadable blob links fail the salvage: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind by the failed salvage",
    );
    Ok(())
}

/// Standalone salvage preserves the SOURCE's persisted table id: an
/// unencrypted SST written under a non-zero id salvages WITHOUT the caller
/// supplying that id (the salvage-mode open reads it from the metadata
/// instead of failing the id cross-check against the options default of 0),
/// and the recovered copy is stamped with the source's id — so it keeps its
/// identity when an operator swaps it in for the original.
#[test]
fn salvage_preserves_a_nonzero_source_table_id() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    const TID: crate::TableId = 7;
    let mut writer = Writer::new(source.clone(), TID, 0, Arc::clone(&fs))?.use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Default options (table_id = 0): the salvage must still open the source
    // and carry its real id through.
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "a healthy non-zero-id SST salvages completely: {report:?}",
    );

    // The recovered copy reopens under the SOURCE's id (the recover
    // cross-checks the stored table_id against the expected one).
    let recovered = open_with_id(dest, &fs, TID)?;
    assert_eq!(
        recovered.metadata.id, TID,
        "the salvaged copy is stamped with the source's table id",
    );
    assert_eq!(recovered.metadata.item_count, u64::from(n));
    Ok(())
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

    // Every block of a healthy SST reads back clean, so every salvaged block is
    // copied through verbatim — none re-encoded.
    assert_eq!(
        report.blocks_copied_verbatim, report.blocks_salvaged,
        "a healthy SST's blocks are all copied verbatim",
    );

    // The salvaged copy is a valid SST that reopens and holds every key.
    assert_eq!(
        reopen_item_count(dest, &fs)?,
        u64::from(n),
        "the salvaged SST reopens with the full item count",
    );
    Ok(())
}

/// A clean block is byte-copied verbatim, not decoded and re-encoded: its raw
/// on-disk bytes in the salvaged SST are identical to the source's, and the walk
/// reports it under `blocks_copied_verbatim`.
#[test]
fn salvage_copies_a_clean_block_verbatim() -> crate::Result<()> {
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

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "healthy SST salvages clean: {report:?}"
    );
    assert_eq!(
        report.blocks_copied_verbatim, report.blocks_total,
        "every clean block is copied verbatim, none re-encoded",
    );

    // The first data block's raw on-disk bytes must be byte-identical between the
    // source and the salvaged copy (each resolved through its own intact index).
    let first_block = |path: &std::path::Path| -> crate::Result<(usize, usize)> {
        let table = open(path.to_path_buf(), &fs)?;
        let Some(kh) = table.data_block_handles().find_map(Result::ok) else {
            panic!("a non-empty SST has at least one data block");
        };
        let off = usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX);
        Ok((off, kh.as_ref().size() as usize))
    };
    let (src_off, src_size) = first_block(&source)?;
    let (dst_off, dst_size) = first_block(&dest)?;
    assert_eq!(
        src_size, dst_size,
        "the verbatim copy preserves the block's on-disk size",
    );

    let src_bytes = std::fs::read(&source)?;
    let dst_bytes = std::fs::read(&dest)?;
    let src_block = src_bytes.get(src_off..src_off + src_size);
    let dst_block = dst_bytes.get(dst_off..dst_off + dst_size);
    assert!(
        src_block.is_some() && src_block == dst_block,
        "the clean block is copied byte-for-byte into the salvaged SST",
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
        report.dropped.first().is_some_and(|d| {
            matches!(d.reason, DropReason::ChecksumMismatch) && d.key_range.is_some()
        }),
        "the dropped block reports a checksum mismatch and names the key range it lost: {report:?}",
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

/// A data block that needs ECC recovery to read is NOT copied verbatim — its
/// on-disk bytes are faulty, so propagating them would carry the corruption into
/// the recovered copy. Salvage re-encodes the healed payload instead (clean bytes
/// in the copy), while the surrounding clean blocks are still copied verbatim.
#[cfg(feature = "page_ecc")]
#[test]
fn salvage_reencodes_an_ecc_recovered_block_rather_than_copying_it() -> crate::Result<()> {
    use crate::table::block::{EccParams, Header};

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // Data blocks carry RS(4,2) parity, so a small corruption is healed on read
    // rather than failing the block.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_ecc(Some(EccParams::RS_4_2));
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Flip one payload byte of the FIRST data block so reading it must repair via
    // RS parity (an ECC-recovered read, not a clean one).
    let first_off = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().find_map(Result::ok) else {
            panic!("a non-empty SST has at least one data block");
        };
        *kh.as_ref().offset()
    };
    let pos = usize::try_from(first_off).unwrap_or(usize::MAX) + Header::MIN_LEN + 3;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(pos) {
        *b ^= 0x80;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;

    // The healed block is recovered, not dropped — nothing is lost.
    assert!(
        report.is_complete(),
        "an ECC-recoverable block is healed, not dropped: {report:?}",
    );
    assert_eq!(
        report.blocks_salvaged, report.blocks_total,
        "every block is recovered",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every entry is recovered",
    );
    // Exactly the healed block was re-encoded; the rest were copied verbatim.
    assert_eq!(
        report.blocks_copied_verbatim,
        report.blocks_salvaged - 1,
        "the ECC-recovered block is re-encoded, not copied verbatim",
    );

    // The salvaged copy reopens with every key; its bytes are freshly encoded, so
    // they no longer need ECC repair.
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n));
    Ok(())
}

/// Bit rot confined to a block's PARITY trailer reads as clean (the payload
/// checksum passes and parity is only consulted on a mismatch), so a verbatim
/// copy would carry the rotted parity into the salvaged SST as latent ECC
/// corruption. Salvage must verify the trailer before copying and re-encode
/// (regenerating fresh parity) when it disagrees: every data block of the
/// recovered copy must carry parity that matches its payload.
#[cfg(feature = "page_ecc")]
#[test]
fn salvage_regenerates_a_rotted_parity_trailer_rather_than_copying_it() -> crate::Result<()> {
    use crate::coding::Decode;
    use crate::table::block::{EccParams, Header, expected_parity_len};

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_ecc(Some(EccParams::RS_4_2));
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Flip one byte INSIDE the first data block's parity trailer (right after
    // its `data_length` payload). The payload checksum still verifies, so the
    // block reads back clean with no ECC recovery.
    let first_off = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().find_map(Result::ok) else {
            panic!("a non-empty SST has at least one data block");
        };
        usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX)
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(mut cursor) = bytes.get(first_off..) else {
        panic!("first data block within the file");
    };
    let header = Header::decode_from(&mut cursor)?;
    let trailer_pos =
        first_off + Header::header_len(header.block_type) + header.data_length as usize;
    let Some(slot) = bytes.get_mut(trailer_pos) else {
        panic!("parity trailer within the file");
    };
    *slot ^= 0xFF;
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "the payload is intact, every block is recovered: {report:?}",
    );
    assert_eq!(reopen_item_count(dest.clone(), &fs)?, u64::from(n));

    // Every data block of the salvaged copy carries parity that matches its
    // payload — the rotted trailer was regenerated, not byte-copied.
    let dest_bytes = std::fs::read(&dest)?;
    let dest_table = open(dest, &fs)?;
    let (ds, ps) = EccParams::RS_4_2.as_shards();
    for kh in dest_table.data_block_handles() {
        let kh = kh?;
        let off = usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX);
        let Some(mut cursor) = dest_bytes.get(off..) else {
            panic!("block at offset {off} within the file");
        };
        let hdr = Header::decode_from(&mut cursor)?;
        let hlen = Header::header_len(hdr.block_type);
        let dl = hdr.data_length as usize;
        let Some(payload) = dest_bytes.get(off + hlen..off + hlen + dl) else {
            panic!("payload of block at offset {off} within the file");
        };
        let plen = expected_parity_len(hdr.data_length, EccParams::RS_4_2) as usize;
        let Some(trailer) = dest_bytes.get(off + hlen + dl..off + hlen + dl + plen) else {
            panic!("parity trailer of block at offset {off} within the file");
        };
        let fresh = crate::ecc::encode_parity(payload, ds, ps)?;
        assert_eq!(
            trailer,
            fresh.as_slice(),
            "block at offset {off}: the salvaged copy's parity matches its payload",
        );
    }
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

    // The salvaged copy stays COLUMNAR (mirrored from the source) and holds the
    // recovered rows — no longer degraded to a row-major copy.
    let recovered = open(dest, &fs)?;
    assert_eq!(recovered.metadata.item_count, report.entries_salvaged);
    assert!(
        recovered.metadata.columnar,
        "a columnar source salvages into a columnar copy, not a row-major one",
    );
    Ok(())
}

/// A columnar block whose outer `ColumnBatch` frame decodes but whose row
/// materialization fails (an invalid value-type byte in an otherwise
/// checksum-consistent block) is dropped like any other block-local decode
/// failure — one malformed block must not abort the whole salvage and discard
/// the destination while later blocks are still recoverable.
#[cfg(feature = "columnar")]
#[test]
fn salvage_drops_a_columnar_block_with_an_invalid_value_type() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;
    use crate::table::columnar::{CodecId, ColumnBatch};

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar SST is non-empty"
    );

    // Poison the SECOND data block: decode its ColumnBatch, stamp an invalid
    // value-type tag into the first row, re-encode under the writer's Plain
    // codec (byte-identical framing => same length), and re-stamp the header
    // checksum. The block stays checksum-consistent, so the failure surfaces
    // in row materialization — not as an ordinary checksum drop.
    let (block_off, block_size) = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().filter_map(Result::ok).nth(1) else {
            panic!("source must have at least two data blocks");
        };
        (
            usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX),
            kh.as_ref().size() as usize,
        )
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(block) = bytes.get(block_off..block_off + block_size) else {
        panic!("block range within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let Some(payload) = block.get(header_len..header_len + header.data_length as usize) else {
        panic!("payload range within the block");
    };
    let mut batch = ColumnBatch::decode(payload)?;
    let poisoned_rows = u64::from(batch.row_count);
    // Columns are ordered (key, seqno, value-type, values...); 0xFF is not a
    // defined ValueType tag.
    let Some(vt_byte) = batch.columns.get_mut(2).and_then(|col| col.data.get_mut(0)) else {
        panic!("value-type column present and non-empty");
    };
    *vt_byte = 0xFF;
    let new_payload = batch.encode(CodecId::Plain)?;
    assert_eq!(
        new_payload.len(),
        payload.len(),
        "a one-byte in-place mutation re-encodes to the same length",
    );
    let new_header = Header {
        checksum: crate::Checksum::from_raw(crate::hash::hash128(&new_payload)),
        ..header
    };
    let mut new_block = Vec::with_capacity(header_len + new_payload.len());
    new_header.encode_into(&mut new_block)?;
    assert_eq!(
        new_block.len(),
        header_len,
        "header re-encodes to its length"
    );
    new_block.extend_from_slice(&new_payload);
    let Some(target) = bytes.get_mut(block_off..block_off + new_block.len()) else {
        panic!("block range within the file");
    };
    target.copy_from_slice(&new_block);
    std::fs::write(&source, &bytes)?;

    // Salvage drops exactly the poisoned block and recovers every other one.
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the poisoned block is dropped: {report:?}",
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(DropReason::DecodeError(_))
        ),
        "the invalid value-type tag classifies as a decode error: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n) - poisoned_rows,
        "every row outside the poisoned block is recovered",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n) - poisoned_rows);
    Ok(())
}

/// A checksum-consistent columnar block whose entries are OUT OF internal-key
/// order (two adjacent keys swapped) must be dropped, not emitted: verbatim
/// paths skip the ingest ordering checks, so an unvalidated malformed block
/// would register a wrong last-key in the recovered SST's index and corrupt
/// binary search / scan order. The rest of the SST still salvages.
#[cfg(feature = "columnar")]
#[test]
fn salvage_drops_a_columnar_block_with_out_of_order_keys() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;
    use crate::table::columnar::{CodecId, ColumnBatch};

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar SST is non-empty"
    );

    // Poison the SECOND data block: swap the first two rows' user keys inside
    // the key column (equal-length keys keep the Bytes framing intact),
    // re-encode, and re-stamp the header checksum. The block stays
    // checksum-consistent and its rows materialize fine — only the ordering
    // invariant is broken.
    let (block_off, block_size) = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().filter_map(Result::ok).nth(1) else {
            panic!("source must have at least two data blocks");
        };
        (
            usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX),
            kh.as_ref().size() as usize,
        )
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(block) = bytes.get(block_off..block_off + block_size) else {
        panic!("block range within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let Some(payload) = block.get(header_len..header_len + header.data_length as usize) else {
        panic!("payload range within the block");
    };
    let mut batch = ColumnBatch::decode(payload)?;
    let poisoned_rows = u64::from(batch.row_count);
    assert!(batch.row_count >= 2, "block holds at least two rows");
    {
        // Key column framing: (row_count + 1) LE u32 offsets, then payload.
        let Some(key_col) = batch.columns.first_mut() else {
            panic!("key column present");
        };
        let table_len = (batch.row_count as usize + 1) * 4;
        let off = |data: &[u8], idx: usize| -> usize {
            let Some(b) = data.get(idx * 4..idx * 4 + 4) else {
                panic!("offset {idx} within the frame table");
            };
            u32::from_le_bytes(b.try_into().unwrap_or([0; 4])) as usize
        };
        let (o0, o1, o2) = (
            off(&key_col.data, 0),
            off(&key_col.data, 1),
            off(&key_col.data, 2),
        );
        assert_eq!(o1 - o0, o2 - o1, "adjacent keys are equal-length");
        let len = o1 - o0;
        let Some(first) = key_col.data.get(table_len + o0..table_len + o0 + len) else {
            panic!("first key within the column");
        };
        let first = first.to_vec();
        let Some(second) = key_col.data.get(table_len + o1..table_len + o1 + len) else {
            panic!("second key within the column");
        };
        let second = second.to_vec();
        let Some(dst0) = key_col.data.get_mut(table_len + o0..table_len + o0 + len) else {
            panic!("first key range within the column");
        };
        dst0.copy_from_slice(&second);
        let Some(dst1) = key_col.data.get_mut(table_len + o1..table_len + o1 + len) else {
            panic!("second key range within the column");
        };
        dst1.copy_from_slice(&first);
    }
    let new_payload = batch.encode(CodecId::Plain)?;
    assert_eq!(
        new_payload.len(),
        payload.len(),
        "an in-place key swap re-encodes to the same length",
    );
    let new_header = Header {
        checksum: crate::Checksum::from_raw(crate::hash::hash128(&new_payload)),
        ..header
    };
    let mut new_block = Vec::with_capacity(header_len + new_payload.len());
    new_header.encode_into(&mut new_block)?;
    new_block.extend_from_slice(&new_payload);
    let Some(target) = bytes.get_mut(block_off..block_off + new_block.len()) else {
        panic!("block range within the file");
    };
    target.copy_from_slice(&new_block);
    std::fs::write(&source, &bytes)?;

    // Salvage drops exactly the out-of-order block and recovers every other one.
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the out-of-order block is dropped: {report:?}",
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(DropReason::DecodeError(_))
        ),
        "the ordering violation classifies as a decode error: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n) - poisoned_rows,
        "every row outside the poisoned block is recovered",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n) - poisoned_rows);
    Ok(())
}

/// A checksum-clean columnar block that decodes as a ZERO-ROW `ColumnBatch`
/// is malformed input (a real writer never emits an empty block): the writer
/// primitive emits nothing for it, so counting it as salvaged would let an
/// SST whose only block is empty report `salvaged_path = Some(dest)` while
/// the empty-table `finish` REMOVES `dest` — and a mixed SST would
/// under-report its dropped key ranges. Such a block must be dropped as a
/// decode error.
#[cfg(feature = "columnar")]
#[test]
fn salvage_drops_a_zero_row_columnar_block() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;
    use crate::table::columnar::{
        COL_SEQNO, COL_USER_KEY, COL_VALUE, COL_VALUE_TYPE, CodecId, Column, ColumnBatch, TypeTag,
    };

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A single-block columnar SST (few rows, default block size). An ODD row
    // count lets the retry below flip the payload-length parity by growing
    // every value one byte.
    let n = 9u32;
    let build = |value_pad: usize| -> crate::Result<()> {
        let _ = std::fs::remove_file(&source);
        let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_columnar(true);
        for i in 0..n {
            writer.write(InternalValue::from_components(
                format!("key{i:05}").into_bytes(),
                format!("val{i:05}{}", "x".repeat(value_pad)).into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        assert!(writer.finish()?.is_some(), "source SST is non-empty");
        Ok(())
    };
    build(0)?;

    // The zero-row replacement encodes to 8 (row/column counts) + 44 bytes of
    // intrinsic + value column headers, padded to the ORIGINAL payload length
    // with extra empty value sub-columns (Fixed = 10 bytes, Bytes = 14 — every
    // reachable length is even, so an odd source payload is rebuilt one byte
    // per value larger to flip its parity).
    let payload_len = |src: &std::path::Path| -> crate::Result<usize> {
        let bytes = std::fs::read(src)?;
        let mut cursor = bytes.as_slice();
        let header = Header::decode_from(&mut cursor)?;
        Ok(header.data_length as usize)
    };
    let mut target_len = payload_len(&source)?;
    if target_len % 2 != 0 {
        build(1)?;
        target_len = payload_len(&source)?;
    }
    assert_eq!(target_len % 2, 0, "an even payload length is reachable");

    let empty_fixed = |id: u16, width: u8| Column {
        column_id: id,
        type_tag: TypeTag::Fixed(width),
        validity: None,
        data: Vec::new(),
    };
    let mut columns = vec![
        Column {
            column_id: COL_USER_KEY,
            type_tag: TypeTag::Bytes,
            validity: None,
            // A zero-row Bytes column is exactly its (row_count + 1) * 4 = 4
            // byte offset table.
            data: vec![0u8; 4],
        },
        empty_fixed(COL_SEQNO, 8),
        empty_fixed(COL_VALUE_TYPE, 1),
        empty_fixed(COL_VALUE, 1),
    ];
    let Some(mut rem) = target_len.checked_sub(8 + 14 + 10 + 10 + 10) else {
        panic!("source payload larger than the zero-row skeleton");
    };
    let mut next_id = COL_VALUE + 1;
    // Greedy fill: Bytes columns (+14) until the remainder is divisible by
    // 10, then Fixed columns (+10).
    while rem % 10 != 0 {
        columns.push(Column {
            column_id: next_id,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: vec![0u8; 4],
        });
        next_id += 1;
        let Some(next_rem) = rem.checked_sub(14) else {
            panic!("remainder covers a Bytes column");
        };
        rem = next_rem;
    }
    while rem > 0 {
        columns.push(empty_fixed(next_id, 1));
        next_id += 1;
        rem -= 10;
    }
    let batch = ColumnBatch {
        row_count: 0,
        columns,
    };
    let new_payload = batch.encode(CodecId::Plain)?;
    assert_eq!(
        new_payload.len(),
        target_len,
        "the zero-row batch pads to the original payload length",
    );

    // Splice it under a re-stamped checksum (frame length unchanged).
    let mut bytes = std::fs::read(&source)?;
    let mut cursor = bytes.as_slice();
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let new_header = Header {
        checksum: crate::Checksum::from_raw(crate::hash::hash128(&new_payload)),
        ..header
    };
    let mut new_block: Vec<u8> = Vec::with_capacity(header_len + new_payload.len());
    new_header.encode_into(&mut new_block)?;
    new_block.extend_from_slice(&new_payload);
    let Some(target) = bytes.get_mut(..new_block.len()) else {
        panic!("block range within the file");
    };
    target.copy_from_slice(&new_block);
    std::fs::write(&source, &bytes)?;

    // The zero-row block is DROPPED, so nothing is recoverable: no destination
    // is left behind and no salvaged path is reported (the pre-fix behavior
    // counted it as salvaged, reporting Some(dest) for a file the empty-table
    // finish had just removed).
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "the zero-row block is dropped as malformed: {report:?}",
    );
    assert_eq!(report.blocks_salvaged, 0, "{report:?}");
    assert_eq!(
        report.salvaged_path, None,
        "an SST whose only block is empty reports nothing salvaged",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind",
    );
    Ok(())
}

/// The ROW-source twin of the columnar out-of-order drop: a checksum-clean
/// row block with two adjacent keys swapped passes frame decode and row
/// materialization, so the ordering guard before the emit is the only thing
/// standing between it and a recovered SST with a corrupt index order. The
/// rejection is block-local: the block drops, the rest still salvages.
#[test]
fn salvage_drops_a_row_block_with_out_of_order_keys() -> crate::Result<()> {
    use crate::coding::Encode;
    use crate::comparator::default_comparator;
    use crate::table::block::Header;
    use crate::table::block::decoder::ParsedItem as _;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    // Restart interval 1 + no hash index: every entry stores its full key, so
    // swapping two equal-length keys re-encodes to a byte-identical length.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_data_block_restart_interval(1)
        .use_data_block_hash_ratio(0.0);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Poison the SECOND data block: decode its entries, swap the first two
    // (equal-length keys), re-encode under the same block parameters, and
    // re-stamp the header checksum.
    let (block_off, poisoned_rows, new_block) = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().filter_map(Result::ok).nth(1) else {
            panic!("source must have at least two data blocks");
        };
        let block_off = usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX);
        let sb = table.salvage_load_block(kh.as_ref(), crate::table::block::BlockType::Data)?;
        let header = sb.block.header;
        let db = crate::table::DataBlock::from_loaded(sb.block, false)?;
        let iter = db.try_iter(default_comparator())?;
        let mut entries: alloc::vec::Vec<crate::InternalValue> =
            iter.map(|p| p.materialize(db.as_slice())).collect();
        assert!(entries.len() >= 2, "block holds at least two rows");
        entries.swap(0, 1);
        let mut new_payload: alloc::vec::Vec<u8> = alloc::vec::Vec::new();
        crate::table::DataBlock::encode_into(&mut new_payload, &entries, 1, 0.0)?;
        assert_eq!(
            new_payload.len(),
            header.data_length as usize,
            "an adjacent equal-length key swap re-encodes to the same length",
        );
        let header_len = Header::header_len(header.block_type);
        let new_header = Header {
            checksum: crate::Checksum::from_raw(crate::hash::hash128(&new_payload)),
            ..header
        };
        let mut new_block: alloc::vec::Vec<u8> =
            alloc::vec::Vec::with_capacity(header_len + new_payload.len());
        new_header.encode_into(&mut new_block)?;
        new_block.extend_from_slice(&new_payload);
        (block_off, entries.len() as u64, new_block)
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(target) = bytes.get_mut(block_off..block_off + new_block.len()) else {
        panic!("block range within the file");
    };
    target.copy_from_slice(&new_block);
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the out-of-order block is dropped: {report:?}",
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(DropReason::DecodeError(_))
        ),
        "the ordering violation classifies as a decode error: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n) - poisoned_rows,
        "every row outside the poisoned block is recovered",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n) - poisoned_rows);
    Ok(())
}

/// The DELETE-BEARING twin of the columnar out-of-order drop: the swapped
/// keys keep every block's row count intact, so the delete positions still
/// verify and the walk takes the masked re-emit — whose writer then rejects
/// the broken ordering. The rejection stays block-local: only the poisoned
/// block drops, deletes still apply to every other block.
#[cfg(feature = "columnar")]
#[test]
fn salvage_drops_an_out_of_order_columnar_block_in_a_delete_bearing_sst() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::config::DeleteStrategy;
    use crate::table::block::Header;
    use crate::table::columnar::{CodecId, ColumnBatch};

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
    let deletes = [5u32, 50, 150];
    for pos in deletes {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Poison the SECOND data block exactly like the delete-free variant: swap
    // the first two rows' user keys inside the key column and re-stamp the
    // checksum. Row counts stay intact, so the delete positions still verify.
    let (block_off, block_size) = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().filter_map(Result::ok).nth(1) else {
            panic!("source must have at least two data blocks");
        };
        (
            usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX),
            kh.as_ref().size() as usize,
        )
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(block) = bytes.get(block_off..block_off + block_size) else {
        panic!("block range within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let Some(payload) = block.get(header_len..header_len + header.data_length as usize) else {
        panic!("payload range within the block");
    };
    let mut batch = ColumnBatch::decode(payload)?;
    let poisoned_rows = u64::from(batch.row_count);
    assert!(batch.row_count >= 2, "block holds at least two rows");
    {
        let Some(key_col) = batch.columns.first_mut() else {
            panic!("key column present");
        };
        let table_len = (batch.row_count as usize + 1) * 4;
        let off = |data: &[u8], idx: usize| -> usize {
            let Some(b) = data.get(idx * 4..idx * 4 + 4) else {
                panic!("offset {idx} within the frame table");
            };
            u32::from_le_bytes(b.try_into().unwrap_or([0; 4])) as usize
        };
        let (o0, o1, o2) = (
            off(&key_col.data, 0),
            off(&key_col.data, 1),
            off(&key_col.data, 2),
        );
        assert_eq!(o1 - o0, o2 - o1, "adjacent keys are equal-length");
        let len = o1 - o0;
        let Some(first) = key_col.data.get(table_len + o0..table_len + o0 + len) else {
            panic!("first key within the column");
        };
        let first = first.to_vec();
        let Some(second) = key_col.data.get(table_len + o1..table_len + o1 + len) else {
            panic!("second key within the column");
        };
        let second = second.to_vec();
        let Some(dst0) = key_col.data.get_mut(table_len + o0..table_len + o0 + len) else {
            panic!("first key range within the column");
        };
        dst0.copy_from_slice(&second);
        let Some(dst1) = key_col.data.get_mut(table_len + o1..table_len + o1 + len) else {
            panic!("second key range within the column");
        };
        dst1.copy_from_slice(&first);
    }
    let new_payload = batch.encode(CodecId::Plain)?;
    assert_eq!(
        new_payload.len(),
        payload.len(),
        "an in-place key swap re-encodes to the same length",
    );
    let new_header = Header {
        checksum: crate::Checksum::from_raw(crate::hash::hash128(&new_payload)),
        ..header
    };
    let mut new_block = Vec::with_capacity(header_len + new_payload.len());
    new_header.encode_into(&mut new_block)?;
    new_block.extend_from_slice(&new_payload);
    let Some(target) = bytes.get_mut(block_off..block_off + new_block.len()) else {
        panic!("block range within the file");
    };
    target.copy_from_slice(&new_block);
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the out-of-order block is dropped: {report:?}",
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(DropReason::DecodeError(_))
        ),
        "the ordering violation classifies as a decode error: {report:?}",
    );
    // Every row outside the poisoned block is recovered, minus the deletes
    // that fall outside it (none of 5 / 50 / 150 land in the second block,
    // which spans rows ~17..34 at 256-byte blocks).
    assert_eq!(
        report.entries_salvaged,
        u64::from(n) - poisoned_rows - deletes.len() as u64,
        "rows outside the poisoned block are recovered with deletes applied",
    );
    // LOGICAL visibility: the deletes were applied faithfully, so the deleted
    // keys stay masked while a neighbouring live key reads back.
    for pos in deletes {
        assert!(
            reopen_get(dest.clone(), &fs, format!("key{pos:05}").as_bytes())?.is_none(),
            "the deleted key at position {pos} stays masked in the recovered copy",
        );
    }
    assert!(
        reopen_get(dest, &fs, b"key00051")?.is_some(),
        "a neighbouring live key reads back from the recovered copy",
    );
    Ok(())
}

/// A DESTINATION write failure mid-walk is a hard error, not a dropped block:
/// the salvage propagates it and removes the partial destination so a retry
/// or repair caller never sees half-written output.
#[test]
fn salvage_sst_errors_and_discards_the_dest_on_a_write_failure() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);

    // Values big enough that the destination's buffered writer flushes
    // mid-walk (so the failure surfaces through a block emit, not finish).
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0..200u32 {
        writer.write(InternalValue::from_components(
            format!("key{i:05}").into_bytes(),
            vec![0xAB; 1_024],
            1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    injector.arm(
        FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other))
            .on_path("salvaged")
            .once(),
    );
    let result = salvage_sst(&source, dest.clone(), &fs);
    injector.clear();

    assert!(
        result.is_err(),
        "a destination write failure errors the whole salvage: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "the partial destination is removed on a write failure",
    );
    Ok(())
}

/// By default salvage FAILS CLOSED on a delete-bearing SST whose delete
/// bitmap is unreadable: recovering "all rows live" would resurrect
/// positionally-deleted rows, so that degradation requires the caller's
/// explicit [`SalvageOptions::allow_delete_resurrection`] opt-in. No
/// destination file is left behind.
#[cfg(feature = "columnar")]
#[test]
fn salvage_fails_closed_on_a_corrupt_delete_bitmap_by_default() -> crate::Result<()> {
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
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Corrupt the `delete_bitmap` SFA section (data blocks stay intact).
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

    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "default salvage refuses to resurrect deleted rows: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind by the refused salvage",
    );
    Ok(())
}

/// Same fail-closed default for the other degradation: a READABLE delete
/// bitmap whose positioning zone map is corrupt cannot be applied, so the
/// default salvage refuses rather than recovering all rows live.
#[cfg(feature = "columnar")]
#[test]
fn salvage_fails_closed_on_an_unpositionable_delete_bitmap_by_default() -> crate::Result<()> {
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
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Corrupt the `zone_map` section (the bitmap stays readable but can no
    // longer be positioned).
    let (zm_pos, zm_len) = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"zone_map") else {
            panic!("source must carry a zone_map section");
        };
        (entry.pos(), entry.len())
    };
    let flip = usize::try_from(zm_pos + zm_len / 2).unwrap_or(0);
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "default salvage refuses to resurrect deleted rows: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind by the refused salvage",
    );
    Ok(())
}

/// A columnar source carrying deletes whose `delete_bitmap` section is
/// corrupted (data blocks intact): normal recovery refuses to open it (opening
/// would resurrect deleted rows) and default salvage fails closed, but a
/// caller who explicitly opts into [`SalvageOptions::allow_delete_resurrection`]
/// degrades to "all rows live" and recovers every block.
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

    // With the explicit opt-in, salvage degrades to "all rows live": every
    // block recovers, nothing masked.
    let options = SalvageOptions {
        allow_delete_resurrection: true,
        ..SalvageOptions::default()
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert!(
        report.is_complete(),
        "the data blocks are intact; only the sidecar was corrupt: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every row is recovered live, the corrupt bitmap is ignored",
    );
    // The SST was written WITH deletes (it carries a delete-bitmap section), so
    // even though the degraded bitmap reads as empty, salvage must NOT take the
    // verbatim copy-through fast path: that would byte-copy the physical blocks
    // (including positionally-deleted rows) without the bitmap. It re-emits
    // instead, so nothing is copied verbatim here.
    assert_eq!(
        report.blocks_copied_verbatim, 0,
        "a delete-bearing SST is never copied verbatim, even with a degraded bitmap: {report:?}",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n));
    Ok(())
}

/// An UNREADABLE data block inside a delete-bearing columnar SST makes every
/// later delete position unverifiable: the block's actual row count is
/// unknowable, and trusting the zone map's claim for it would let a
/// checksum-repatched count on exactly that block shift the masks of all
/// later readable blocks undetected. Default salvage must fail closed; the
/// explicit [`SalvageOptions::allow_delete_resurrection`] opt-in recovers the
/// readable rows live (never masking against unverified positions).
#[cfg(feature = "columnar")]
#[test]
fn salvage_fails_closed_on_an_unreadable_block_in_a_delete_bearing_sst() -> crate::Result<()> {
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
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Corrupt the SECOND data block's bytes (a plain checksum break): the
    // block becomes unreadable, so its actual row count is unknowable.
    let target = {
        let table = open(source.clone(), &fs)?;
        let offsets: alloc::vec::Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|kh| *kh.as_ref().offset())
            .collect();
        let Some(&second) = offsets.get(1) else {
            panic!("source must have at least two data blocks, got {offsets:?}");
        };
        second
    };
    let flip = usize::try_from(target).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    // Default: fail closed — the delete positions past the unreadable block
    // cannot be proven faithful.
    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "unverifiable delete positions fail the default salvage: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind by the refused salvage",
    );

    // Explicit opt-in: the readable rows are recovered LIVE; only the corrupt
    // block's rows are lost.
    let options = SalvageOptions {
        allow_delete_resurrection: true,
        ..SalvageOptions::default()
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the corrupt block is dropped: {report:?}",
    );
    assert!(
        report.entries_salvaged < u64::from(n),
        "the dropped block's rows are lost: {report:?}",
    );
    assert_eq!(
        reopen_item_count(dest.clone(), &fs)?,
        report.entries_salvaged,
        "the recovered copy reopens with every salvaged row live",
    );
    // LOGICAL visibility of the resurrection: the deleted positions live
    // outside the corrupt block, so under the opt-in their keys read back.
    for pos in [5u32, 50, 150] {
        assert!(
            reopen_get(dest.clone(), &fs, format!("key{pos:05}").as_bytes())?.is_some(),
            "the opt-in resurrects the deleted key at position {pos}",
        );
    }
    Ok(())
}

/// A delete-bearing columnar SST where one block was REPLACED by a zero-row
/// batch and the zone map's claim for it patched to 0: the position verifier
/// would accept the block (decoded count 0 matches the claim) while the walk
/// drops it as malformed — leaving later blocks masked at starts that no
/// longer reflect the ORIGINAL row layout the bitmap was built against.
/// A zero-row batch is malformed input everywhere else in the salvage
/// pipeline, so the verifier must reject it too and fail the salvage closed.
#[cfg(feature = "columnar")]
#[test]
fn salvage_fails_closed_on_a_zero_row_block_in_a_delete_bearing_sst() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::config::DeleteStrategy;
    use crate::table::block::Header;
    use crate::table::columnar::{
        COL_SEQNO, COL_USER_KEY, COL_VALUE, COL_VALUE_TYPE, CodecId, Column, ColumnBatch, TypeTag,
    };

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
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Replace the SECOND data block with a length-preserving ZERO-ROW batch
    // (skeleton + padding columns, checksum re-stamped) — the same forgery
    // the plain zero-row test uses.
    let (block_off, block_size) = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().filter_map(Result::ok).nth(1) else {
            panic!("source must have at least two data blocks");
        };
        (
            usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX),
            kh.as_ref().size() as usize,
        )
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(block) = bytes.get(block_off..block_off + block_size) else {
        panic!("block range within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let target_len = header.data_length as usize;
    assert_eq!(
        target_len % 2,
        0,
        "the padded skeleton needs an even length"
    );
    let empty_fixed = |id: u16, width: u8| Column {
        column_id: id,
        type_tag: TypeTag::Fixed(width),
        validity: None,
        data: Vec::new(),
    };
    let mut columns = vec![
        Column {
            column_id: COL_USER_KEY,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: vec![0u8; 4],
        },
        empty_fixed(COL_SEQNO, 8),
        empty_fixed(COL_VALUE_TYPE, 1),
        empty_fixed(COL_VALUE, 1),
    ];
    let Some(mut rem) = target_len.checked_sub(8 + 14 + 10 + 10 + 10) else {
        panic!("source payload larger than the zero-row skeleton");
    };
    let mut next_id = COL_VALUE + 1;
    while rem % 10 != 0 {
        columns.push(Column {
            column_id: next_id,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: vec![0u8; 4],
        });
        next_id += 1;
        let Some(next_rem) = rem.checked_sub(14) else {
            panic!("remainder covers a Bytes column");
        };
        rem = next_rem;
    }
    while rem > 0 {
        columns.push(empty_fixed(next_id, 1));
        next_id += 1;
        rem -= 10;
    }
    let new_payload = ColumnBatch {
        row_count: 0,
        columns,
    }
    .encode(CodecId::Plain)?;
    assert_eq!(new_payload.len(), target_len, "length-preserving forgery");
    let new_header = Header {
        checksum: crate::Checksum::from_raw(crate::hash::hash128(&new_payload)),
        ..header
    };
    let mut new_block: Vec<u8> = Vec::with_capacity(header_len + new_payload.len());
    new_header.encode_into(&mut new_block)?;
    new_block.extend_from_slice(&new_payload);
    let Some(target) = bytes.get_mut(block_off..block_off + new_block.len()) else {
        panic!("block range within the file");
    };
    target.copy_from_slice(&new_block);

    // Patch the zone map's row_count claim for the second block to 0 (the
    // first column's count drives the derived delete starts) and re-stamp
    // the zone-map block checksum, so the tampered chain is self-consistent.
    let zm_pos = {
        let mut f = std::io::Cursor::new(&bytes);
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"zone_map") else {
            panic!("source must carry a zone_map section");
        };
        usize::try_from(entry.pos()).unwrap_or(usize::MAX)
    };
    let Some(mut zm_cursor) = bytes.get(zm_pos..) else {
        panic!("zone_map section within the file");
    };
    let zm_header = Header::decode_from(&mut zm_cursor)?;
    let zm_header_len = Header::header_len(zm_header.block_type);
    let zm_payload_range =
        zm_pos + zm_header_len..zm_pos + zm_header_len + zm_header.data_length as usize;
    {
        let Some(payload) = bytes.get_mut(zm_payload_range.clone()) else {
            panic!("zone_map payload within the file");
        };
        // Walk the wire layout (count u32; per block: block_offset u64 +
        // n_columns u16; per column: id u32 + type u8 + codec u8 +
        // null_count u32 + row_count u32 + min_len u32 + min + max_len u32
        // + max) to the SECOND block's FIRST column row_count — the field
        // the derived delete starts are built from.
        let read_u32 = |data: &[u8], at: usize| -> u32 {
            let Some(b) = data.get(at..at + 4) else {
                panic!("u32 at {at} within the zone map payload");
            };
            u32::from_le_bytes(b.try_into().unwrap_or([0; 4]))
        };
        let read_u16 = |data: &[u8], at: usize| -> u16 {
            let Some(b) = data.get(at..at + 2) else {
                panic!("u16 at {at} within the zone map payload");
            };
            u16::from_le_bytes(b.try_into().unwrap_or([0; 2]))
        };
        let mut at = 4; // past the block count
        // Skip block 1 entirely: offset u64 + n_columns u16, then each
        // column's fixed 14 bytes + variable min/max.
        at += 8;
        let block1_cols = read_u16(payload, at);
        at += 2;
        for _ in 0..block1_cols {
            at += 10; // id + type + codec + null_count
            at += 4; // row_count
            let min_len = read_u32(payload, at) as usize;
            at += 4 + min_len;
            let max_len = read_u32(payload, at) as usize;
            at += 4 + max_len;
        }
        // Block 2: seek to its first column's row_count and zero it.
        at += 8; // block_offset
        at += 2; // n_columns
        at += 10; // first column's id + type + codec + null_count
        let claimed = read_u32(payload, at);
        assert!(claimed > 0, "the second block originally holds rows");
        let Some(rc) = payload.get_mut(at..at + 4) else {
            panic!("second block's first row_count within the zone map payload");
        };
        rc.copy_from_slice(&0u32.to_le_bytes());
    }
    let new_zm_checksum = crate::Checksum::from_raw(crate::hash::hash128(
        bytes.get(zm_payload_range).unwrap_or(&[]),
    ));
    let new_zm_header = Header {
        checksum: new_zm_checksum,
        ..zm_header
    };
    let mut zm_hdr_bytes: Vec<u8> = Vec::with_capacity(zm_header_len);
    new_zm_header.encode_into(&mut zm_hdr_bytes)?;
    let Some(zm_dst) = bytes.get_mut(zm_pos..zm_pos + zm_header_len) else {
        panic!("zone_map header within the file");
    };
    zm_dst.copy_from_slice(&zm_hdr_bytes);
    std::fs::write(&source, &bytes)?;

    // Default: fail closed — the zero-row block is unpositionable input (the
    // walk drops it while later blocks would be masked at starts the bitmap
    // was never built against).
    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "a zero-row block in a delete-bearing SST fails the default salvage: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind by the refused salvage",
    );
    Ok(())
}

/// A footer-bearing row SST (per-KV checksums) whose block checksum was
/// re-stamped over a tampered entry: the BLOCK checksum verifies, but the
/// entry no longer matches its per-KV digest. Salvage must verify the footer
/// before emitting (verbatim or re-encoded) — otherwise it recovers a block
/// the live per-KV scrub would reject, laundering the corruption into a
/// "fully valid" copy.
#[test]
fn salvage_drops_a_row_block_with_a_stale_kv_digest() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::runtime_config::{ChecksumAlgorithm, KvChecksumPolicy};
    use crate::table::block::Header;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_kv_checksums(KvChecksumPolicy::AllLevels, ChecksumAlgorithm::Xxh3_64);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Tamper one entry byte inside the SECOND block's inner payload and
    // re-stamp the BLOCK checksum: the frame verifies clean, but the entry's
    // stored per-KV digest no longer matches its bytes.
    let (block_off, block_size) = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().filter_map(Result::ok).nth(1) else {
            panic!("source must have at least two data blocks");
        };
        (
            usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX),
            kh.as_ref().size() as usize,
        )
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(block) = bytes.get(block_off..block_off + block_size) else {
        panic!("block range within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let payload_range =
        block_off + header_len..block_off + header_len + header.data_length as usize;
    {
        let Some(payload) = bytes.get_mut(payload_range.clone()) else {
            panic!("payload range within the block");
        };
        // A byte inside the first entry's value bytes (past the entry header),
        // well before the per-KV footer at the payload tail.
        let Some(b) = payload.get_mut(12) else {
            panic!("entry byte within the payload");
        };
        *b ^= 0xFF;
    }
    let new_checksum = crate::Checksum::from_raw(crate::hash::hash128(
        bytes.get(payload_range).unwrap_or(&[]),
    ));
    let new_header = Header {
        checksum: new_checksum,
        ..header
    };
    let mut hdr_bytes: Vec<u8> = Vec::with_capacity(header_len);
    new_header.encode_into(&mut hdr_bytes)?;
    let Some(hdr_dst) = bytes.get_mut(block_off..block_off + header_len) else {
        panic!("block header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "the block with a stale per-KV digest is dropped: {report:?}",
    );
    assert!(
        report.entries_salvaged < u64::from(n),
        "the tampered block's rows are not laundered into the copy: {report:?}",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, report.entries_salvaged);
    Ok(())
}

/// A delete-bearing columnar SST with a checksum-clean block whose
/// `ColumnBatch` does NOT decode (a repatched tamper that keeps the leading
/// row-count u32 intact but breaks the column framing): the block's ACTUAL row
/// count is unknowable, so every later block's delete positions are
/// unverifiable — the position verifier must fully decode each block rather
/// than trust the leading four bytes, and the default salvage must fail
/// closed instead of dropping the block and masking later rows at positions
/// it could not prove.
#[cfg(feature = "columnar")]
#[test]
fn salvage_fails_closed_on_an_undecodable_checksum_clean_block_with_deletes() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::config::DeleteStrategy;
    use crate::table::block::Header;

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
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Poison the SECOND data block: stamp an invalid type tag into the first
    // column's header (payload layout: row_count u32, column_count u32, then
    // per column id u16 + type u8 + ... — so the first type byte sits at
    // payload offset 10) and re-stamp the block checksum. The leading
    // row-count u32 stays intact, the frame stays checksum-consistent, but
    // `ColumnBatch::decode` fails on the unknown tag.
    let (block_off, block_size) = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().filter_map(Result::ok).nth(1) else {
            panic!("source must have at least two data blocks");
        };
        (
            usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX),
            kh.as_ref().size() as usize,
        )
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(block) = bytes.get(block_off..block_off + block_size) else {
        panic!("block range within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let payload_range =
        block_off + header_len..block_off + header_len + header.data_length as usize;
    {
        let Some(payload) = bytes.get_mut(payload_range.clone()) else {
            panic!("payload range within the block");
        };
        let Some(tag) = payload.get_mut(10) else {
            panic!("first column's type byte within the payload");
        };
        *tag = 0xEE;
    }
    let new_checksum = crate::Checksum::from_raw(crate::hash::hash128(
        bytes.get(payload_range).unwrap_or(&[]),
    ));
    let new_header = Header {
        checksum: new_checksum,
        ..header
    };
    let mut hdr_bytes = Vec::with_capacity(header_len);
    new_header.encode_into(&mut hdr_bytes)?;
    let Some(hdr_dst) = bytes.get_mut(block_off..block_off + header_len) else {
        panic!("block header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);
    std::fs::write(&source, &bytes)?;

    // Default: fail closed — the block reads back checksum-clean but cannot
    // be decoded, so its actual row count (and every later block's delete
    // positions) cannot be proven faithful.
    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "unverifiable delete positions fail the default salvage: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind by the refused salvage",
    );

    // Explicit opt-in: the poisoned block is dropped, every other row is
    // recovered LIVE (never masked against unproven positions).
    let options = SalvageOptions {
        allow_delete_resurrection: true,
        ..SalvageOptions::default()
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the poisoned block is dropped: {report:?}",
    );
    assert!(
        report.entries_salvaged < u64::from(n),
        "the poisoned block's rows are lost: {report:?}",
    );
    assert_eq!(
        reopen_item_count(dest.clone(), &fs)?,
        report.entries_salvaged,
        "the recovered copy reopens with every salvaged row live",
    );
    // LOGICAL visibility of the resurrection: the deleted positions live
    // outside the poisoned block, so under the opt-in their keys read back.
    for pos in [5u32, 50, 150] {
        assert!(
            reopen_get(dest.clone(), &fs, format!("key{pos:05}").as_bytes())?.is_some(),
            "the opt-in resurrects the deleted key at position {pos}",
        );
    }
    Ok(())
}

/// A zone map that DECODES but carries wrong per-block row counts (a
/// checksum-repatched tamper) would misposition the delete bitmap: the masked
/// re-emit derives each block's start row from the zone map, so deletes land
/// on the wrong rows — deleted rows resurrect AND live rows vanish, silently.
/// Salvage must cross-check the claimed positions against the actual decoded
/// row counts and fail closed on a mismatch; with the explicit
/// [`SalvageOptions::allow_delete_resurrection`] opt-in it recovers all rows
/// live instead of masking against the wrong positions.
#[cfg(feature = "columnar")]
#[test]
fn salvage_fails_closed_on_a_zone_map_with_wrong_row_counts() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::config::DeleteStrategy;
    use crate::table::block::Header;

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
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Tamper the FIRST block's row count inside the zone map (wire layout:
    // count u32, then block_offset u64 + n_columns u16, then per column
    // id u32 + type_tag u8 + codec_id u8 + null_count u32 + row_count u32 —
    // so the first row_count sits at payload bytes 24..28) and re-stamp the
    // section block's checksum. The zone map still DECODES — only its claimed
    // positions are shifted for every block after the first.
    let zm_pos = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"zone_map") else {
            panic!("source must carry a zone_map section");
        };
        usize::try_from(entry.pos()).unwrap_or(usize::MAX)
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(mut cursor) = bytes.get(zm_pos..) else {
        panic!("zone_map section within the file");
    };
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let payload_range = zm_pos + header_len..zm_pos + header_len + header.data_length as usize;
    {
        let Some(payload) = bytes.get_mut(payload_range.clone()) else {
            panic!("zone_map payload within the file");
        };
        let Some(rc) = payload.get_mut(24..28) else {
            panic!("first row_count within the zone map payload");
        };
        let claimed = u32::from_le_bytes(rc.try_into().unwrap_or([0; 4]));
        assert!(claimed >= 2, "the first block holds at least two rows");
        rc.copy_from_slice(&(claimed - 1).to_le_bytes());
    }
    let new_checksum = crate::Checksum::from_raw(crate::hash::hash128(
        bytes.get(payload_range).unwrap_or(&[]),
    ));
    let new_header = Header {
        checksum: new_checksum,
        ..header
    };
    let mut hdr_bytes = Vec::with_capacity(header_len);
    new_header.encode_into(&mut hdr_bytes)?;
    let Some(hdr_dst) = bytes.get_mut(zm_pos..zm_pos + header_len) else {
        panic!("zone_map header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);
    std::fs::write(&source, &bytes)?;

    // Default: fail closed — masking against the shifted positions would
    // silently corrupt visibility in the recovered SST.
    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "a mispositioning zone map fails the default salvage: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind by the refused salvage",
    );

    // Explicit opt-in: recover all rows LIVE (never mask against the wrong
    // positions).
    let options = SalvageOptions {
        allow_delete_resurrection: true,
        ..SalvageOptions::default()
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert!(
        report.is_complete(),
        "the data blocks are intact; only the zone map lies: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every row is recovered live under the opt-in",
    );
    assert_eq!(reopen_item_count(dest.clone(), &fs)?, u64::from(n));
    // LOGICAL visibility of the resurrection: "all rows live" means the keys
    // at the deleted positions read back.
    for pos in [5u32, 50, 150] {
        assert!(
            reopen_get(dest.clone(), &fs, format!("key{pos:05}").as_bytes())?.is_some(),
            "the opt-in resurrects the deleted key at position {pos}",
        );
    }
    Ok(())
}

/// A columnar SST with deletes whose ZONE MAP is corrupt (the bitmap stays
/// readable): the bitmap cannot be positioned without the zone map, so normal
/// recovery and default salvage fail closed, but a caller who explicitly opts
/// into [`SalvageOptions::allow_delete_resurrection`] ignores the bitmap
/// ("all rows live") and recovers every row.
#[cfg(feature = "columnar")]
#[test]
fn salvage_ignores_a_delete_bitmap_without_a_readable_zone_map() -> crate::Result<()> {
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
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Corrupt the zone_map section (the bitmap stays intact). The zone map
    // degrades to empty, leaving a readable bitmap that cannot be positioned.
    let (zm_pos, zm_len) = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"zone_map") else {
            panic!("source must carry a zone_map section");
        };
        (entry.pos(), entry.len())
    };
    let flip = usize::try_from(zm_pos + zm_len / 2).unwrap_or(0);
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    // Normal recovery fails closed: a bitmap with no positioning zone map.
    assert!(
        open(source.clone(), &fs).is_err(),
        "normal recovery must reject a bitmap with no readable zone map",
    );

    // With the explicit opt-in, salvage ignores the unpositionable bitmap and
    // recovers every row live.
    let options = SalvageOptions {
        allow_delete_resurrection: true,
        ..SalvageOptions::default()
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert!(
        report.is_complete(),
        "the data blocks are intact; only the zone map was corrupt: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every row is recovered live once the unpositionable bitmap is ignored",
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

/// A columnar source whose delete-bitmap wholly covers its leading data
/// block(s): those blocks carry no live rows, so salvage skips them (nothing
/// salvaged, nothing dropped) and recovers the live rows of the rest.
#[cfg(feature = "columnar")]
#[test]
fn salvage_skips_a_wholly_deleted_block() -> crate::Result<()> {
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
    // Delete the first 60 row positions: with 256-byte blocks this wholly covers
    // the leading data block(s), which then load as "no live rows".
    let deleted = 60u32;
    for pos in 0..deleted {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "wholly-deleted blocks are skipped, not dropped: {report:?}",
    );
    assert!(
        report.blocks_salvaged < report.blocks_total,
        "at least one leading block was wholly deleted and skipped: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n - deleted),
        "every live row is recovered, the deleted prefix is skipped",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n - deleted));
    Ok(())
}

/// An SST carrying range tombstones cannot be salvaged: the positional KV walk
/// re-emits only point entries, so the tombstones would be silently dropped and
/// lower-level keys they cover could reappear after repair. Until the writer
/// path re-emits them, salvage fails closed.
#[test]
fn salvage_rejects_an_sst_with_range_tombstones() -> crate::Result<()> {
    use crate::UserKey;
    use crate::range_tombstone::RangeTombstone;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0..20 {
        writer.write(iv(i))?;
    }
    // A range tombstone over part of the key space: the salvaged copy must not
    // silently drop it.
    writer.write_range_tombstone(RangeTombstone::new(
        UserKey::from(b"key00005".as_slice()),
        UserKey::from(b"key00010".as_slice()),
        2,
    ));
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        matches!(result, Err(crate::Error::FeatureUnsupported(_))),
        "an SST with range tombstones must fail closed, got {result:?}",
    );
    assert!(
        !dest.exists(),
        "no salvaged file is written when salvage fails closed",
    );
    Ok(())
}

/// Salvage drives every read and write through the injected `Fs`: an SST that
/// lives only in an in-memory backend (never on the real filesystem) salvages
/// and reopens purely through that backend. A source-digest path that bypassed
/// `fs` and read through `std::fs` would fail to find the file at all.
#[test]
fn salvage_sst_reads_and_writes_through_the_injected_fs() -> crate::Result<()> {
    use crate::fs::MemFs;

    let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    // `Writer::new` rewrites its path through `std::path::absolute`, which on
    // Windows resolves a `/`-rooted path against the current drive (`/memfs` ->
    // `D:\memfs`). Create the parent under that same absolutized form so the
    // writer's parent-directory check finds it on every platform (on Unix
    // `absolute` is a no-op, so this is just `/memfs`).
    let dir = std::path::absolute("/memfs")?;
    fs.create_dir_all(&dir)?;
    let source = dir.join("source");
    let dest = dir.join("salvaged");

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "in-memory source SST is non-empty"
    );

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "a healthy in-memory SST salvages with no dropped blocks: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every entry is recovered through the in-memory backend",
    );
    assert_eq!(report.salvaged_path.as_deref(), Some(dest.as_path()));
    assert_eq!(
        reopen_item_count(dest, &fs)?,
        u64::from(n),
        "the salvaged SST reopens through the same in-memory backend",
    );
    Ok(())
}

// --- Forwarded recovery context: encrypted + dictionary-compressed sources ---

/// Reads the second data block's on-disk offset from a context-aware reopen of
/// `source`, then flips a byte just past that block's header so its checksum /
/// AEAD tag fails on load while every other block stays intact.
#[cfg(any(feature = "encryption", zstd_any))]
fn corrupt_second_data_block(
    source: &std::path::Path,
    fs: &Arc<dyn Fs>,
    table_id: crate::table::TableId,
    encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
    #[cfg(zstd_any)] zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
) -> crate::Result<()> {
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&**fs, source)?);
    let table = Table::recover(
        source.to_path_buf(),
        checksum,
        0,
        0,
        // Open under the source's table id so an encrypted index (AAD binds the
        // id) decrypts when reading the block offsets.
        table_id,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(fs),
        false,
        false,
        encryption,
        #[cfg(zstd_any)]
        zstd_dictionary,
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )?;
    let offsets: alloc::vec::Vec<u64> = table
        .data_block_handles()
        .filter_map(Result::ok)
        .map(|kh| *kh.as_ref().offset())
        .collect();
    let Some(&second) = offsets.get(1) else {
        panic!("source SST must have at least two data blocks, got {offsets:?}");
    };
    let flip = usize::try_from(second).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(source, &bytes)?;
    Ok(())
}

/// An encrypted source: salvage cannot open it without the provider (the gap this
/// closes), but with the provider in `SalvageOptions` it block-salvages like a
/// plain SST and the recovered copy reopens under the same encryption.
#[cfg(feature = "encryption")]
#[test]
fn salvage_recovers_an_encrypted_sst_with_the_provider() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let enc: Arc<dyn crate::encryption::EncryptionProvider> =
        Arc::new(crate::encryption::Aes256GcmProvider::new(&[0x42; 32]));

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_encryption(Some(Arc::clone(&enc)));
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source encrypted SST is non-empty"
    );

    corrupt_second_data_block(
        &source,
        &fs,
        0,
        Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        None,
    )?;

    // Without the provider, the encrypted source cannot even be opened.
    assert!(
        salvage_sst(&source, dest.clone(), &fs).is_err(),
        "an encrypted SST must not salvage without the provider",
    );

    // With the provider, it block-salvages: the corrupt block is dropped, the
    // rest recovered, and the copy is written encrypted.
    let options = SalvageOptions {
        encryption: Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        zstd_dictionary: None,
        table_id: 0,
        allow_delete_resurrection: false,
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the corrupt block drops: {report:?}"
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );

    // The salvaged copy reopens UNDER ENCRYPTION (a plaintext copy would fail the
    // encrypted reopen) and holds exactly the recovered entries.
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&*fs, &dest)?);
    let reopened = Table::recover(
        dest,
        checksum,
        0,
        0,
        0,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(&fs),
        false,
        false,
        Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        None,
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )?;
    assert_eq!(
        reopened.metadata.item_count, report.entries_salvaged,
        "the encrypted salvaged copy reopens with exactly the recovered entries",
    );
    Ok(())
}

/// A zstd-dictionary-compressed source: salvage cannot decompress it without the
/// dictionary, but with the dictionary in `SalvageOptions` it block-salvages and
/// the recovered copy reopens under the same dictionary.
#[cfg(zstd_any)]
#[test]
fn salvage_recovers_a_dictionary_sst_with_the_dictionary() -> crate::Result<()> {
    use crate::CompressionType;
    use crate::compression::ZstdDictionary;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A small training corpus so the dictionary has content to match against.
    let samples: alloc::vec::Vec<u8> = (0..4000u32).map(|i| (i % 251) as u8).collect();
    let dict = Arc::new(ZstdDictionary::new(&samples));
    let compression = CompressionType::ZstdDict {
        level: 3,
        dict_id: dict.id(),
    };

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_data_block_compression(compression)
        .use_zstd_dictionary(Some(Arc::clone(&dict)));
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source dictionary SST is non-empty"
    );

    corrupt_second_data_block(&source, &fs, 0, None, Some(Arc::clone(&dict)))?;

    // Without the dictionary, the source cannot even be opened: `recover_inner`
    // fail-fasts on the ZstdDict-id mismatch at open time (before any block
    // walk), so salvage returns `Err`, not a zero-recovered report.
    assert!(
        salvage_sst(&source, dest.clone(), &fs).is_err(),
        "a dictionary SST must not salvage without the dictionary",
    );

    let options = SalvageOptions {
        encryption: None,
        zstd_dictionary: Some(Arc::clone(&dict)),
        table_id: 0,
        allow_delete_resurrection: false,
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the corrupt block drops: {report:?}"
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );

    // The salvaged copy reopens UNDER THE DICTIONARY with the recovered entries.
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&*fs, &dest)?);
    let reopened = Table::recover(
        dest,
        checksum,
        0,
        0,
        0,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(&fs),
        false,
        false,
        None,
        Some(Arc::clone(&dict)),
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )?;
    assert_eq!(
        reopened.metadata.item_count, report.entries_salvaged,
        "the dictionary salvaged copy reopens with exactly the recovered entries",
    );
    Ok(())
}

/// An encrypted source sealed under a NON-ZERO table id: the encrypted-block AAD
/// binds the table id, so salvage must be given that id. With the wrong id the
/// AAD-bound blocks cannot be decrypted (the gap repair hit when it passed a
/// hardcoded `0`); with the right id it block-salvages and the copy reopens.
#[cfg(feature = "encryption")]
#[test]
fn salvage_recovers_an_encrypted_sst_with_a_nonzero_table_id() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let enc: Arc<dyn crate::encryption::EncryptionProvider> =
        Arc::new(crate::encryption::Aes256GcmProvider::new(&[0x37; 32]));
    const TID: crate::table::TableId = 7;

    let mut writer = Writer::new(source.clone(), TID, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_encryption(Some(Arc::clone(&enc)));
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source encrypted SST is non-empty"
    );

    corrupt_second_data_block(
        &source,
        &fs,
        TID,
        Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        None,
    )?;

    // Wrong table id (the legacy hardcoded 0): the AAD-bound blocks cannot be
    // decrypted, so nothing is recovered (salvage either fails to open or drops
    // every block).
    let wrong = SalvageOptions {
        encryption: Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        zstd_dictionary: None,
        table_id: 0,
        allow_delete_resurrection: false,
    };
    let recovered_wrong = salvage_sst_with_options(&source, dest.clone(), &fs, &wrong)
        .map_or(0, |r| r.entries_salvaged);
    assert_eq!(
        recovered_wrong, 0,
        "the wrong table id cannot decrypt the AAD-bound encrypted source",
    );

    // Right table id: block-salvages, dropping only the corrupt block.
    let options = SalvageOptions {
        encryption: Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        zstd_dictionary: None,
        table_id: TID,
        allow_delete_resurrection: false,
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the corrupt block drops: {report:?}"
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );

    // The recovered copy reopens under the same table id + encryption.
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&*fs, &dest)?);
    let reopened = Table::recover(
        dest,
        checksum,
        0,
        0,
        TID,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(&fs),
        false,
        false,
        Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        None,
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )?;
    assert_eq!(
        reopened.metadata.item_count, report.entries_salvaged,
        "the recovered copy reopens under the same table id with the recovered entries",
    );
    Ok(())
}

// --- Blob (vlog) file record-granular salvage ---

use crate::vlog::blob_file::scanner::Scanner as BlobScanner;
use crate::vlog::blob_file::writer::Writer as BlobWriter;

/// Builds a blob file at `path` from `(key, value)` records (seqno 0, no
/// compression).
fn build_blob(
    path: &std::path::Path,
    fs: &Arc<dyn Fs>,
    records: &[(&[u8], &[u8])],
) -> crate::Result<()> {
    let mut writer = BlobWriter::new(path, 0, 0, &**fs)?;
    for (k, v) in records {
        writer.write(k, 0, v)?;
    }
    writer.finish()?;
    Ok(())
}

/// Scans a blob file into its `(key, value)` records (Ok records only).
fn scan_blob(path: &std::path::Path, fs: &Arc<dyn Fs>) -> crate::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    Ok(BlobScanner::new(path, &**fs, 0)?
        .filter_map(Result::ok)
        .map(|e| (e.key.to_vec(), e.value.to_vec()))
        .collect())
}

#[test]
fn salvage_blob_file_recovers_every_record_of_a_healthy_file() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let records: Vec<(&[u8], &[u8])> = vec![
        (b"k0", b"v0"),
        (b"k1", b"v1"),
        (b"k2", b"v2"),
        (b"k3", b"v3"),
    ];
    build_blob(&source, &fs, &records)?;

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert!(
        report.is_complete(),
        "a healthy blob file drops nothing: {report:?}"
    );
    assert_eq!(report.records_salvaged, 4);
    assert_eq!(report.salvaged_path.as_deref(), Some(dest.as_path()));

    let recovered = scan_blob(&dest, &fs)?;
    let expected: Vec<(Vec<u8>, Vec<u8>)> = records
        .iter()
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect();
    assert_eq!(
        recovered, expected,
        "every record round-trips through salvage"
    );
    Ok(())
}

/// The TOCTOU variant of the pre-existing-destination guarantee: a file that
/// appears at `dest` AFTER any existence probe but BEFORE the writer's
/// `create_new` open (a concurrent worker winning the destination) must also
/// survive the failed salvage. Ownership is decided by `create_new` alone —
/// when it fails, this call created nothing and must remove nothing. The
/// injected `Metadata` fault materializes the race window deterministically:
/// the probe cannot see the file, yet `create_new` finds it.
#[test]
fn salvage_blob_file_keeps_a_racing_dest_created_after_the_existence_probe() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_dest");
    let plain: Arc<dyn Fs> = Arc::new(StdFs);
    build_blob(&source, &plain, &[(b"k0", b"v0")])?;

    // The "racing" worker's file is already at dest, but any metadata probe of
    // dest fails — exactly the window where the file lands between a stat and
    // the `create_new` open.
    std::fs::write(&dest, b"racing worker's blob")?;
    let fault = FaultFs::new(StdFs);
    fault.injector().arm(
        FaultRule::new(FaultOp::Metadata, Fault::Error(ErrorKind::NotFound)).on_path("blob_dest"),
    );
    let fs: Arc<dyn Fs> = Arc::new(fault);

    let result = salvage_blob_file(&source, dest.clone(), &fs, 0);
    assert!(
        result.is_err(),
        "the destination is taken, the salvage fails: {result:?}",
    );
    assert_eq!(
        std::fs::read(&dest)?,
        b"racing worker's blob",
        "the racing worker's file survives the failed salvage",
    );
    Ok(())
}

/// A transient I/O failure on the verbatim REREAD must not drop the block:
/// the first, recovery-aware read has already produced a verified decoded
/// block, so the loader falls back to the re-encode path (`verbatim = None`)
/// exactly like a checksum / parity mismatch on the re-read frame. Reserving
/// `Err` for the initial verified read keeps one flaky pread from discarding
/// a block that is provably recoverable.
#[test]
fn salvage_load_block_reencodes_when_the_verbatim_reread_fails() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;
    use crate::table::block::BlockType;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0..50u32 {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Collect the first data block's handle BEFORE arming the fault (the
    // open + index walk issue their own reads).
    let table = open(source, &fs)?;
    let Some(kh) = table.data_block_handles().find_map(Result::ok) else {
        panic!("source has at least one data block");
    };
    let handle = *kh.as_ref();

    // Within `salvage_load_block` the FIRST positional read is the verified
    // recovery-aware load; the SECOND is the raw verbatim re-read. Fail
    // exactly that second read, once.
    injector.arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Other))
            .on_path("source")
            .skip(1)
            .once(),
    );
    let result = table.salvage_load_block(&handle, BlockType::Data);
    injector.clear();

    let sb = match result {
        Ok(sb) => sb,
        Err(e) => panic!("a failed verbatim re-read falls back to re-encode, got Err({e:?})"),
    };
    assert!(
        sb.verbatim.is_none(),
        "the re-read was never verified, so the block must not be byte-copied",
    );
    assert!(
        !sb.block.data.is_empty(),
        "the verified first read's decoded payload is preserved for re-encoding",
    );
    Ok(())
}

/// `salvage_blob_file` must not delete a pre-existing file at `dest` when the
/// destination cannot be created (the writer's `create_new` open fails because
/// the path already exists): the error-path cleanup is only for a partial file
/// THIS call created. Deleting a pre-existing destination would turn an
/// argument mistake (a stale path collision, or `source == dest`) into data
/// loss.
#[test]
fn salvage_blob_file_keeps_a_preexisting_dest_on_open_failure() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_dest");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    build_blob(&source, &fs, &[(b"k0", b"v0")])?;
    std::fs::write(&dest, b"pre-existing destination bytes")?;

    let result = salvage_blob_file(&source, dest.clone(), &fs, 0);
    assert!(
        result.is_err(),
        "an already-existing destination fails the salvage: {result:?}",
    );
    assert_eq!(
        std::fs::read(&dest)?,
        b"pre-existing destination bytes",
        "a pre-existing destination file survives the failed salvage",
    );
    Ok(())
}

/// The salvaged blob file is COMPACTED: after a dropped record every later
/// record shifts to a new offset, and existing SST `ValueHandle::offset`
/// values point into the SOURCE. The report's `offset_remap` must map every
/// salvaged record's source frame offset to its offset in the recovered file
/// (and omit the dropped one), so a caller can re-target handles before
/// swapping the file in.
#[test]
fn salvage_blob_file_reports_an_offset_remap_for_every_salvaged_record() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let records: Vec<(&[u8], &[u8])> = vec![
        (b"k0", b"v0-payload"),
        (b"k1", b"v1-payload"),
        (b"k2", b"v2-payload"),
        (b"k3", b"v3-payload"),
    ];
    build_blob(&source, &fs, &records)?;

    // Source frame offsets, in order, from a clean pre-corruption scan.
    let source_offsets: Vec<u64> = BlobScanner::new(&source, &*fs, 0)?
        .filter_map(Result::ok)
        .map(|e| e.offset)
        .collect();
    assert_eq!(source_offsets.len(), 4, "four source records");

    // Corrupt the SECOND record's value bytes (a checksum break): the scanner
    // re-syncs at the next frame, so records 0, 2, 3 survive.
    {
        let Some(&second) = source_offsets.get(1) else {
            panic!("second record offset");
        };
        // Past the frame header, inside key/value bytes.
        let flip = usize::try_from(second).unwrap_or(0) + 45;
        let mut bytes = std::fs::read(&source)?;
        if let Some(b) = bytes.get_mut(flip) {
            *b ^= 0xFF;
        }
        std::fs::write(&source, &bytes)?;
    }

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert_eq!(report.records_salvaged, 3, "{report:?}");
    assert_eq!(report.dropped.len(), 1, "{report:?}");

    // The remap covers exactly the salvaged records, keyed by their SOURCE
    // offsets, and its targets are the actual frame offsets in the recovered
    // file (verified against a scan of the destination).
    let dest_offsets: Vec<u64> = BlobScanner::new(&dest, &*fs, 0)?
        .filter_map(Result::ok)
        .map(|e| e.offset)
        .collect();
    let expected: Vec<(u64, u64)> = [0usize, 2, 3]
        .iter()
        .zip(&dest_offsets)
        .map(|(&src_idx, &new)| {
            (
                source_offsets.get(src_idx).copied().unwrap_or(u64::MAX),
                new,
            )
        })
        .collect();
    assert_eq!(
        report.offset_remap, expected,
        "the remap maps each surviving source frame to its compacted target",
    );
    // The dropped record's source offset is NOT in the map: its handle is lost.
    let Some(&dropped_src) = source_offsets.get(1) else {
        panic!("second record offset");
    };
    assert!(
        report
            .offset_remap
            .iter()
            .all(|(src, _)| *src != dropped_src),
        "a dropped record has no remap target: {report:?}",
    );
    Ok(())
}

/// When a record write to the destination fails mid-salvage, `salvage_blob_file`
/// must error AND remove the partial destination it created, so a retry / repair
/// caller never finds a half-written blob file.
#[test]
fn salvage_blob_file_removes_the_partial_dest_when_a_write_fails() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);

    let records: Vec<(&[u8], &[u8])> = vec![(b"k0", b"v0"), (b"k1", b"v1")];
    build_blob(&source, &fs, &records)?;

    // Fail every write to the destination file: the first recovered record's
    // write-back errors.
    injector.arm(
        FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other)).on_path("blob_salvaged"),
    );

    let result = salvage_blob_file(&source, dest.clone(), &fs, 0);
    assert!(
        result.is_err(),
        "a failed destination write must error the salvage",
    );
    assert!(
        !std::path::Path::new(&dest).exists(),
        "the partial destination is removed on a write failure",
    );
    Ok(())
}

#[test]
fn salvage_blob_file_drops_a_corrupt_record_and_keeps_the_rest() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let records: Vec<(&[u8], &[u8])> = vec![
        (b"k0", b"value-zero"),
        (b"k1", b"value-one"),
        (b"k2", b"value-two"),
        (b"k3", b"value-three"),
    ];
    build_blob(&source, &fs, &records)?;

    // Flip the last byte of the second record's value: the checksum (over
    // key + value) fails, but the frame header (lengths, magic) stays intact, so
    // the scanner reports a checksum mismatch and re-syncs at the next record.
    let Some(second_frame_end) = BlobScanner::new(&source, &*fs, 0)?
        .filter_map(Result::ok)
        .nth(1)
        .map(|e| e.frame_end)
    else {
        panic!("source blob must have at least two records");
    };
    let flip = usize::try_from(second_frame_end - 1).unwrap_or(0);
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the corrupt record drops: {report:?}"
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(BlobDropReason::ChecksumMismatch)
        ),
        "the dropped record reports a checksum mismatch: {report:?}",
    );
    assert_eq!(
        report.records_salvaged, 3,
        "the other three records are recovered"
    );

    // The salvaged file holds every record except the corrupted k1.
    let recovered = scan_blob(&dest, &fs)?;
    let keys: Vec<Vec<u8>> = recovered.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(
        keys,
        vec![b"k0".to_vec(), b"k2".to_vec(), b"k3".to_vec()],
        "the corrupt record's key is the only one missing",
    );
    Ok(())
}

/// A blob file where EVERY record is corrupt salvages nothing: the report
/// carries only drops, `salvaged_path` is `None`, and the empty destination
/// placeholder the writer created is removed (a repair caller would otherwise
/// re-quarantine a stray zero-record blob file in its place).
#[test]
fn salvage_blob_file_removes_the_empty_dest_when_nothing_is_recoverable() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let records: Vec<(&[u8], &[u8])> = vec![(b"k0", b"value-zero"), (b"k1", b"value-one")];
    build_blob(&source, &fs, &records)?;

    // Flip the last value byte of BOTH records: each frame header stays
    // intact, so the scanner reports one checksum mismatch per record and
    // re-syncs — leaving zero salvageable records.
    let frame_ends: Vec<u64> = BlobScanner::new(&source, &*fs, 0)?
        .filter_map(Result::ok)
        .map(|e| e.frame_end)
        .collect();
    assert_eq!(frame_ends.len(), 2, "source blob holds two records");
    let mut bytes = std::fs::read(&source)?;
    for end in frame_ends {
        let flip = usize::try_from(end - 1).unwrap_or(0);
        if let Some(b) = bytes.get_mut(flip) {
            *b ^= 0xFF;
        }
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert_eq!(report.records_salvaged, 0, "{report:?}");
    assert_eq!(report.dropped.len(), 2, "both records drop: {report:?}");
    assert_eq!(
        report.salvaged_path, None,
        "nothing recoverable yields no salvaged path",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "the empty destination placeholder is removed",
    );
    Ok(())
}

/// A STRUCTURAL failure mid-walk (a record frame whose magic bytes are gone,
/// not a checksum miss) terminates the blob walk: the scanner cannot re-sync
/// past it, so the salvage records one `Corrupt` drop for the unreadable tail
/// and keeps everything scanned before it.
#[test]
fn salvage_blob_file_stops_at_a_smashed_frame_and_keeps_the_prefix() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let records: Vec<(&[u8], &[u8])> = vec![
        (b"k0", b"value-zero"),
        (b"k1", b"value-one"),
        (b"k2", b"value-two"),
    ];
    build_blob(&source, &fs, &records)?;

    // Smash the LAST record's frame magic (the file structure and trailer
    // stay intact): the scanner reports it as a structural InvalidHeader it
    // cannot re-sync from, unlike a checksum miss.
    let Some(last_start) = BlobScanner::new(&source, &*fs, 0)?
        .filter_map(Result::ok)
        .nth(1)
        .map(|e| e.frame_end)
    else {
        panic!("source blob must have at least two records");
    };
    let mut bytes = std::fs::read(&source)?;
    let at = usize::try_from(last_start).unwrap_or(0);
    let Some(magic) = bytes.get_mut(at..at + 4) else {
        panic!("last record's frame magic within the file");
    };
    magic.copy_from_slice(b"????");
    std::fs::write(&source, &bytes)?;

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert_eq!(
        report.records_salvaged, 2,
        "the records before the smashed frame are recovered: {report:?}",
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(BlobDropReason::Corrupt(_))
        ),
        "the truncated tail is recorded as a structural drop: {report:?}",
    );
    let recovered = scan_blob(&dest, &fs)?;
    assert_eq!(recovered.len(), 2, "the salvaged copy holds the prefix");
    Ok(())
}

/// A blob frame whose header CRC and data checksum are internally consistent
/// but whose `key_len` is ZERO yields an Ok scanner entry with an empty key —
/// which the blob writer's ingest asserts against. Salvage must route such a
/// frame through the corrupt-record path (dropped, walk continues) instead of
/// panicking in the writer and leaving a partial destination behind.
#[test]
fn salvage_blob_file_drops_an_empty_key_frame() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    build_blob(&source, &fs, &[(b"k", b"vvvv"), (b"k2", b"second")])?;

    // Re-frame the FIRST record as `key_len = 0`: its key byte becomes the
    // first value byte (the hashed key||value byte span is unchanged), with
    // the header CRC and data checksum recomputed so the frame stays
    // internally consistent. V4 frame layout from offset 0:
    //   magic 4 | checksum 16 | seqno 8 | key_len 2 | real_val_len 4 |
    //   on_disk_val_len 4 | header_crc 4 | key | value.
    let mut bytes = std::fs::read(&source)?;
    let seqno = {
        let Some(b) = bytes.get(20..28) else {
            panic!("seqno within the first frame");
        };
        u64::from_le_bytes(b.try_into().unwrap_or([0; 8]))
    };
    // header_crc = truncated xxh3 over (seqno, key_len, real_val_len,
    // on_disk_val_len), matching the writer's framing.
    let new_hcrc = {
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        hasher.update(&seqno.to_le_bytes());
        hasher.update(&0u16.to_le_bytes());
        hasher.update(&5u32.to_le_bytes());
        hasher.update(&5u32.to_le_bytes());
        #[expect(
            clippy::cast_possible_truncation,
            reason = "intentionally truncated to the 4-byte header CRC"
        )]
        {
            hasher.digest() as u32
        }
    };
    // data checksum = xxh3_128(key || value || header_crc_le); with the empty
    // key the hashed span is the same "kvvvv" bytes plus the NEW header CRC.
    let new_checksum = {
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        hasher.update(b"kvvvv");
        hasher.update(&new_hcrc.to_le_bytes());
        hasher.digest128()
    };
    let patch = |bytes: &mut Vec<u8>, range: core::ops::Range<usize>, val: &[u8]| {
        let Some(slot) = bytes.get_mut(range) else {
            panic!("patch range within the first frame");
        };
        slot.copy_from_slice(val);
    };
    patch(&mut bytes, 4..20, &new_checksum.to_le_bytes());
    patch(&mut bytes, 28..30, &0u16.to_le_bytes());
    patch(&mut bytes, 30..34, &5u32.to_le_bytes());
    patch(&mut bytes, 34..38, &5u32.to_le_bytes());
    patch(&mut bytes, 38..42, &new_hcrc.to_le_bytes());
    std::fs::write(&source, &bytes)?;

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "the empty-key frame drops as corrupt: {report:?}",
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(BlobDropReason::Corrupt(_))
        ),
        "the drop reason names the malformed frame: {report:?}",
    );
    assert_eq!(
        report.records_salvaged, 1,
        "the record after the malformed frame is still recovered",
    );
    let recovered = scan_blob(&dest, &fs)?;
    assert_eq!(
        recovered,
        vec![(b"k2".to_vec(), b"second".to_vec())],
        "the salvaged copy holds exactly the healthy record",
    );
    Ok(())
}

/// A compressed blob source is rejected (fail-closed): the scanner yields on-disk
/// compressed bytes that this path cannot faithfully re-emit yet.
#[cfg(feature = "lz4")]
#[test]
fn salvage_blob_file_rejects_a_compressed_source() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    {
        let mut writer =
            BlobWriter::new(&source, 0, 0, &*fs)?.use_compression(crate::CompressionType::Lz4);
        writer.write(b"k0", 0, b"some compressible value aaaaaaaaaaaaaaaa")?;
        writer.finish()?;
    }

    assert!(
        matches!(
            salvage_blob_file(&source, dest, &fs, 0),
            Err(crate::Error::FeatureUnsupported(_)),
        ),
        "a compressed blob file must be rejected rather than mis-salvaged",
    );
    Ok(())
}

/// A columnar source carrying a per-field value sub-column salvages into a copy
/// that KEEPS the sub-column (verbatim `ColumnBatch` re-emit), instead of
/// collapsing it into a single value column via a row round-trip.
#[cfg(feature = "columnar")]
#[test]
fn salvage_preserves_columnar_value_subcolumns() -> crate::Result<()> {
    use crate::table::columnar::{Column, TypeTag, entries_to_column_batch};

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let cmp = default_comparator();

    // Two columnar blocks whose value is a single fixed-4 sub-column (id 3),
    // written verbatim through the ingest batch path (per-row seqno 0).
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true);
    for block in 0..2u32 {
        let entries: Vec<InternalValue> = (0..4u32)
            .map(|i| {
                let k = format!("k{:04}", block * 4 + i);
                InternalValue::from_components(k.into_bytes(), b"x".to_vec(), 0, ValueType::Value)
            })
            .collect();
        let mut batch = entries_to_column_batch(&entries)?;
        batch.columns.pop();
        let mut data = Vec::new();
        for i in 0..4u32 {
            data.extend_from_slice(&(block * 4 + i).to_le_bytes());
        }
        batch.columns.push(Column {
            column_id: 3,
            type_tag: TypeTag::Fixed(4),
            validity: None,
            data,
        });
        writer.write_columnar_batch(&batch, &cmp)?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar SST is non-empty"
    );

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "a healthy columnar SST drops nothing: {report:?}"
    );
    // No deletes + clean blocks: each columnar block is copied through verbatim,
    // which is exactly why the per-field sub-columns survive byte-for-byte.
    assert_eq!(
        report.blocks_copied_verbatim, report.blocks_salvaged,
        "clean columnar blocks are copied verbatim",
    );

    // Reopen and project sub-column 3 via the per-SST scan: it survives as a
    // sub-column. A row round-trip would have collapsed it into the value column.
    let recovered = open(dest, &fs)?;
    assert!(
        recovered.metadata.columnar,
        "the recovered copy stays columnar"
    );
    let batches = recovered.columnar_scan(&[3], None)?;
    let rows: u32 = batches.iter().map(|b| b.row_count).sum();
    assert_eq!(rows, 8, "every row's sub-column is recovered");
    assert!(
        batches
            .iter()
            .all(|b| b.columns.iter().all(|c| c.column_id == 3)),
        "the value sub-column (id 3) is preserved verbatim, not collapsed",
    );
    Ok(())
}

/// A columnar Page-ECC SST with a single-byte RS-recoverable fault in a data
/// block (no deletes): salvage recovers the block from parity and **re-encodes**
/// the healed batch rather than copying the faulty on-disk bytes verbatim, so the
/// recovered copy carries clean bytes. The clean block around it is still copied
/// verbatim.
#[cfg(all(feature = "columnar", feature = "page_ecc"))]
#[test]
fn salvage_reencodes_an_ecc_recovered_columnar_block() -> crate::Result<()> {
    use crate::table::block::{EccParams, Header};
    use crate::table::columnar::entries_to_column_batch;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let cmp = default_comparator();

    // Two columnar blocks under RS(4,2) parity, no deletes (so the no-deletes
    // copy-through / recover path is taken, not the delete-masked one).
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_ecc(Some(EccParams::RS_4_2));
    for block in 0..2u32 {
        let entries: Vec<InternalValue> = (0..4u32)
            .map(|i| {
                let k = format!("k{:04}", block * 4 + i);
                InternalValue::from_components(k.into_bytes(), b"x".to_vec(), 0, ValueType::Value)
            })
            .collect();
        let batch = entries_to_column_batch(&entries)?;
        writer.write_columnar_batch(&batch, &cmp)?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar SST is non-empty"
    );

    // Flip one byte of the first columnar data block (RS(4,2) recovers a single
    // byte error).
    let first_off = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().find_map(Result::ok) else {
            panic!("a non-empty SST has at least one data block");
        };
        usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX)
    };
    let pos = first_off + Header::MIN_LEN + 3;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(pos) {
        *b ^= 0x80;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "an RS-recoverable columnar block is healed, not dropped: {report:?}",
    );
    assert_eq!(
        report.blocks_salvaged, report.blocks_total,
        "every block is recovered",
    );
    // The recovered block was re-encoded (verbatim:None), so fewer verbatim copies
    // than salvaged blocks; the other (clean) block is copied verbatim.
    assert!(
        report.blocks_copied_verbatim < report.blocks_salvaged,
        "the ECC-recovered columnar block is re-encoded, not copied verbatim: {report:?}",
    );

    let recovered = open(dest, &fs)?;
    assert!(
        recovered.metadata.columnar,
        "the recovered copy stays columnar"
    );
    assert_eq!(recovered.metadata.item_count, 8, "every row is recovered");
    Ok(())
}
