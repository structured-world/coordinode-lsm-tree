use super::*;
use crate::{AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter};
use alloc::sync::Arc;
use std::collections::BTreeMap;
use test_log::test;

fn open(folder: &std::path::Path, kv: Option<KvSeparationOptions>) -> crate::Result<AnyTree> {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(kv)
    .open()
}

/// The manifest layout of `tree` as the open tree holds it.
fn layout(tree: &AnyTree) -> Vec<Vec<Vec<TableRecord>>> {
    tree.current_version()
        .iter_levels()
        .map(|level| {
            level
                .iter()
                .map(|run| {
                    run.iter()
                        .map(|t| TableRecord {
                            id: t.id(),
                            checksum: t.checksum().into_u128(),
                            global_seqno: t.global_seqno(),
                        })
                        .collect()
                })
                .collect()
        })
        .collect()
}

/// Every file under `dir` with its bytes, to tell whether a read changed
/// anything on disk.
fn snapshot(dir: &std::path::Path) -> std::io::Result<BTreeMap<std::path::PathBuf, Vec<u8>>> {
    let mut files = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let bytes = std::fs::read(&path)?;
                files.insert(path, bytes);
            }
        }
    }
    Ok(files)
}

/// The exported state places every table where the open tree places it, with
/// the tree's version id and type, after flushes that left several L0 runs
/// and a compaction that moved tables down.
#[test]
fn read_manifest_places_every_table_where_the_tree_does() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open(folder.path(), None)?;
    let seqno = SequenceNumberCounter::default();
    for round in 0..3u32 {
        for i in 0..50u32 {
            tree.insert(format!("k{i:03}-{round}"), "v", seqno.next());
        }
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(u64::MAX, 0)?;
    for i in 0..20u32 {
        tree.insert(format!("z{i:03}"), "v", seqno.next());
    }
    tree.flush_active_memtable(0)?;

    let expected = layout(&tree);
    let version_id = tree.current_version().id();
    drop(tree);

    let state = read_manifest(folder.path(), &crate::fs::StdFs, None)?;
    assert_eq!(state.tree_type, TreeType::Standard);
    assert_eq!(state.version_id, version_id);
    assert_eq!(state.levels, expected);
    assert!(
        state.levels.iter().flatten().flatten().count() > 1,
        "the fixture has tables in more than one place"
    );
    assert!(state.blob_files.is_empty());
    Ok(())
}

/// A blob tree's blob files and their garbage statistics come back by id,
/// matching the open tree's version.
#[test]
fn read_manifest_carries_blob_files_and_their_gc_stats() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open(
        folder.path(),
        Some(KvSeparationOptions::default().separation_threshold(16)),
    )?;
    let seqno = SequenceNumberCounter::default();
    for round in 0..2u32 {
        for i in 0..40u32 {
            tree.insert(
                format!("k{i:03}"),
                format!("{round}").repeat(64),
                seqno.next(),
            );
        }
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(u64::MAX, seqno.get())?;

    let version = tree.current_version();
    let mut expected_blobs: Vec<BlobFileRecord> = version
        .blob_files
        .iter()
        .map(|b| BlobFileRecord {
            id: b.id(),
            checksum: b.checksum().into_u128(),
        })
        .collect();
    expected_blobs.sort_unstable_by_key(|b| b.id);
    let mut expected_stats: Vec<BlobGcStats> = version
        .gc_stats()
        .iter()
        .map(|(id, e)| BlobGcStats {
            id: *id,
            stale_items: e.len as u64,
            stale_bytes: e.bytes,
            stale_on_disk_bytes: e.on_disk_bytes,
        })
        .collect();
    expected_stats.sort_unstable_by_key(|s| s.id);
    drop(version);
    drop(tree);

    let state = read_manifest(folder.path(), &crate::fs::StdFs, None)?;
    assert_eq!(state.tree_type, TreeType::Blob);
    assert_eq!(state.blob_files, expected_blobs);
    assert_eq!(state.blob_gc_stats, expected_stats);
    assert!(
        !state.blob_gc_stats.is_empty(),
        "the overwrite round left garbage in the first blob file"
    );
    Ok(())
}

fn table_context() -> TableContext {
    TableContext {
        fs: Arc::new(crate::fs::StdFs),
        encryption: None,
        #[cfg(zstd_any)]
        dictionaries: crate::compression::ZstdDictionaries::new(),
        comparator: crate::comparator::default_comparator(),
    }
}

/// Every table the manifest of `folder` names, opened for export.
fn export_tables(folder: &std::path::Path) -> crate::Result<Vec<TableExport>> {
    let state = read_manifest(folder, &crate::fs::StdFs, None)?;
    let context = table_context();
    state
        .levels
        .iter()
        .flatten()
        .flatten()
        .map(|record| {
            let path = folder
                .join(crate::file::TABLES_FOLDER)
                .join(record.id.to_string());
            TableExport::open(&path, record, None, &context)
        })
        .collect()
}

/// The decoded payload of the unencrypted block at `offset`.
fn block_payload(
    path: &std::path::Path,
    table_id: TableId,
    offset: u64,
    size: u32,
    block_type: crate::table::block::BlockType,
) -> crate::Result<Vec<u8>> {
    use crate::fs::Fs as _;
    let file = crate::fs::StdFs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
    let block = crate::table::Block::from_file(
        &*file,
        crate::table::BlockHandle::new(crate::table::BlockOffset(offset), size),
        crate::table::block::BlockIdentity {
            table_id,
            block_type,
            dict_id: 0,
            window_log: 0,
        },
        &crate::table::block::BlockTransform::PLAIN,
    )?;
    Ok(block.data.to_vec())
}

/// `solution` written back in the layout it was read from, so a lossless
/// decode reproduces the stored payload byte for byte.
fn encode_burr(solution: &BurrSolution) -> Vec<u8> {
    let mut out = crate::file::MAGIC_BYTES.to_vec();
    out.push(match solution.kind {
        BurrKind::Membership => 2,
        BurrKind::Retrieval => 3,
    });
    out.push(1);
    out.extend([
        solution.r,
        solution.w,
        solution.b,
        u8::try_from(solution.layers.len()).unwrap(),
    ]);
    out.extend(solution.root_seed.to_le_bytes());
    for layer in &solution.layers {
        out.extend(layer.m.to_le_bytes());
        out.extend(u32::try_from(layer.thresholds.len()).unwrap().to_le_bytes());
        out.extend((layer.m * 8).to_le_bytes());
        out.extend(&layer.thresholds);
        for row in &layer.rows {
            out.extend(row.to_le_bytes());
        }
    }
    out
}

fn section<'a>(sections: &'a [Section], name: &[u8]) -> Option<&'a Section> {
    sections.iter().find(|s| s.name == name)
}

/// The rows the export reads, block by block, are every entry the table
/// holds, versions and tombstones included, in the order the table's own
/// iterator yields them; the meta block's items include the format markers
/// the read path checks.
#[test]
fn table_export_rows_and_meta_match_the_table() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(crate::config::BlockSizePolicy::all(512))
    .open()?;
    let seqno = SequenceNumberCounter::default();
    for i in 0..300u32 {
        tree.insert(format!("k{i:04}"), format!("v1-{i}"), seqno.next());
        if i % 3 == 0 {
            tree.insert(format!("k{i:04}"), format!("v2-{i}"), seqno.next());
        }
        if i % 7 == 0 {
            tree.remove(format!("k{i:04}"), seqno.next());
        }
    }
    tree.flush_active_memtable(0)?;
    let version = tree.current_version();
    let table = version.iter_tables().next().unwrap().clone();
    let expected: Vec<crate::InternalValue> = table.iter().collect::<crate::Result<_>>()?;
    drop(version);
    drop(tree);

    let exports = export_tables(folder.path())?;
    assert_eq!(exports.len(), 1);
    let export = &exports[0];
    let blocks = export.data_blocks()?;
    assert!(blocks.len() > 1, "the fixture spans several data blocks");
    let mut rows = Vec::new();
    for block in &blocks {
        rows.extend(export.rows(block)?);
    }
    assert_eq!(rows, expected);

    let meta = export.meta()?;
    let get = |key: &[u8]| {
        meta.iter()
            .find(|(k, _)| &**k == key)
            .map(|(_, v)| v.to_vec())
    };
    assert_eq!(get(b"table_version"), Some(vec![3]));
    assert_eq!(
        get(b"item_count"),
        Some(
            u64::try_from(expected.len())
                .unwrap()
                .to_le_bytes()
                .to_vec()
        )
    );
    assert!(
        meta.windows(2).all(|w| w[0].0 < w[1].0),
        "meta items come back in key order"
    );
    Ok(())
}

/// A full filter decodes losslessly: written back in the layout it came from,
/// it is the stored payload byte for byte. Read as a retrieval solution, the
/// same payload is refused.
#[test]
fn table_export_full_filter_round_trips() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open(folder.path(), None)?;
    let seqno = SequenceNumberCounter::default();
    for i in 0..2_000u32 {
        tree.insert(format!("k{i:05}"), "v", seqno.next());
    }
    tree.flush_active_memtable(0)?;
    drop(tree);

    let exports = export_tables(folder.path())?;
    let export = &exports[0];
    let Some(Filter::Full(solution)) = export.filter()? else {
        panic!("the default policy writes a full filter");
    };
    assert!(
        solution.layers.len() > 1,
        "the build bumped keys into a later layer"
    );
    let sections = export.sections()?;
    let filter = section(&sections, b"filter").unwrap();
    let path = folder
        .path()
        .join(crate::file::TABLES_FOLDER)
        .join(export.id().to_string());
    let stored = block_payload(
        &path,
        export.id(),
        filter.offset,
        u32::try_from(filter.len).unwrap(),
        crate::table::block::BlockType::Filter,
    )?;
    assert_eq!(encode_burr(&solution), stored);
    assert!(decode_burr(&stored, BurrKind::Retrieval).is_err());
    Ok(())
}

/// A partitioned filter comes back as every partition its index lists, in
/// key order, each decoding losslessly from the block its entry addresses.
#[test]
fn table_export_partitioned_filter_round_trips() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .filter_block_partitioning_policy(crate::config::PinningPolicy::all(true))
    .open()?;
    let seqno = SequenceNumberCounter::default();
    for i in 0..20_000u32 {
        tree.insert(format!("k{i:06}"), "v", seqno.next());
    }
    tree.flush_active_memtable(0)?;
    drop(tree);

    let exports = export_tables(folder.path())?;
    let export = &exports[0];
    let Some(Filter::Partitioned(partitions)) = export.filter()? else {
        panic!("the partitioning policy writes a partitioned filter");
    };
    assert!(partitions.len() > 1, "the fixture fills several partitions");
    assert!(
        partitions
            .windows(2)
            .all(|w| w[0].entry.end_key < w[1].entry.end_key),
        "partitions come back in key order"
    );
    let path = folder
        .path()
        .join(crate::file::TABLES_FOLDER)
        .join(export.id().to_string());
    for partition in &partitions {
        let stored = block_payload(
            &path,
            export.id(),
            partition.entry.offset,
            partition.entry.size,
            crate::table::block::BlockType::Filter,
        )?;
        assert_eq!(encode_burr(&partition.solution), stored);
    }
    Ok(())
}

/// The locator section decodes losslessly, header fields included.
#[test]
fn table_export_locator_round_trips() -> crate::Result<()> {
    use crate::config::{LocatorPolicy, LocatorPolicyEntry, LocatorPrecision};

    let folder = crate::get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(crate::config::BlockSizePolicy::all(1_024))
    .locator_policy(LocatorPolicy::all(LocatorPolicyEntry::Enabled {
        precision: LocatorPrecision::Entry,
        block_id_bits: None,
        slot_bits: None,
    }))
    .open()?;
    let seqno = SequenceNumberCounter::default();
    for i in 0..3_000u32 {
        tree.insert(format!("k{i:05}"), "v", seqno.next());
    }
    tree.flush_active_memtable(0)?;
    drop(tree);

    let exports = export_tables(folder.path())?;
    let export = &exports[0];
    let locator = export.locator()?.expect("the policy writes a locator");
    assert_eq!(locator.precision, 1);
    assert_eq!(locator.solution.kind, BurrKind::Retrieval);
    let sections = export.sections()?;
    let stored_section = section(&sections, b"locator").unwrap();
    let path = folder
        .path()
        .join(crate::file::TABLES_FOLDER)
        .join(export.id().to_string());
    let stored = block_payload(
        &path,
        export.id(),
        stored_section.offset,
        u32::try_from(stored_section.len).unwrap(),
        crate::table::block::BlockType::Locator,
    )?;
    let mut rebuilt = vec![
        1,
        locator.precision,
        locator.block_id_bits,
        locator.slot_bits,
    ];
    rebuilt.extend(encode_burr(&locator.solution));
    assert_eq!(rebuilt, stored);
    Ok(())
}

/// A data block whose stored payload took a bit flip comes back through the
/// export repaired by its parity trailer: the same payload it had before the
/// flip, reported as corrected, while the file keeps its damaged byte (the
/// export writes nothing). Damage past what the parity covers is refused.
#[cfg(feature = "page_ecc")]
#[test]
fn table_export_frame_repairs_damage_through_the_parity_trailer() -> crate::Result<()> {
    use crate::table::block::{BlockType, EccStatus, Header};

    let folder = crate::get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .page_ecc(true)
    .data_block_size_policy(crate::config::BlockSizePolicy::all(1_024))
    .open()?;
    let seqno = SequenceNumberCounter::default();
    for i in 0..500u32 {
        tree.insert(format!("k{i:04}"), format!("value-{i}"), seqno.next());
    }
    tree.flush_active_memtable(0)?;
    drop(tree);

    let exports = export_tables(folder.path())?;
    let export = &exports[0];
    let block = export.data_blocks()?.into_iter().nth(1).unwrap();
    let clean = export.frame(block.offset, block.size, BlockType::Data)?;
    assert_eq!(clean.ecc_status, EccStatus::Ok);

    let path = folder
        .path()
        .join(crate::file::TABLES_FOLDER)
        .join(export.id().to_string());
    let mut damaged = std::fs::read(&path)?;
    let at = usize::try_from(block.offset).unwrap() + Header::header_len(BlockType::Data) + 5;
    damaged[at] ^= 0x10;
    std::fs::write(&path, &damaged)?;

    let healed = export.frame(block.offset, block.size, BlockType::Data)?;
    assert_eq!(healed.payload, clean.payload);
    assert_eq!(healed.ecc_status, EccStatus::Corrected);
    assert!(healed.ecc_recovery.is_some());
    assert_eq!(
        std::fs::read(&path)?,
        damaged,
        "the export heals nothing on disk"
    );

    let end = at + usize::try_from(block.size).unwrap() / 2;
    for byte in &mut damaged[at..end] {
        *byte = !*byte;
    }
    std::fs::write(&path, &damaged)?;
    assert!(
        export
            .frame(block.offset, block.size, BlockType::Data)
            .is_err()
    );
    Ok(())
}

/// A columnar table carrying positional deletes over several blocks: the
/// batches hold every row, the deleted ones included, the rows the export
/// reads are those same rows, and each block's deleted rows, indexed within
/// the block, are exactly its rows the range tombstone relocated into the
/// bitmap.
#[cfg(feature = "columnar")]
#[test]
fn table_export_columnar_batches_keep_deleted_rows_and_their_positions() -> crate::Result<()> {
    use crate::config::{DeleteStrategy, DeleteStrategyPolicy};

    const ROWS: u32 = 2_000;
    let key = |i: u32| format!("k{i:04}").into_bytes();
    let folder = crate::get_tmp_folder();
    let any = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(crate::config::BlockSizePolicy::all(1_024))
    .open()?;
    let AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
        cfg.delete_strategy = DeleteStrategyPolicy::all(DeleteStrategy::Adaptive {
            purge_threshold_percent: 90,
        });
    })?;
    for i in 0..ROWS {
        tree.insert(key(i), vec![b'v'; 16], u64::from(i) + 1);
    }
    tree.remove_range(UserKey::from(key(150)), UserKey::from(key(1_150)), 10_000);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64 * 1024 * 1024, 20_000)?;
    drop(any);

    let exports = export_tables(folder.path())?;
    assert_eq!(exports.len(), 1);
    let export = &exports[0];
    assert!(export.is_columnar());

    let blocks = export.data_blocks()?;
    assert!(blocks.len() > 2, "the deletes span several blocks");
    let mut keys = Vec::new();
    let mut batch_rows = 0u32;
    let mut deleted_keys = Vec::new();
    for block in &blocks {
        batch_rows += export.columnar_batch(block)?.row_count;
        let rows = export.rows(block)?;
        for local in export.deleted_rows_in(block)? {
            deleted_keys.push(rows[local as usize].key.user_key.to_vec());
        }
        keys.extend(rows.into_iter().map(|r| r.key.user_key.to_vec()));
    }
    assert_eq!(batch_rows, ROWS, "the batches keep the deleted rows");
    assert_eq!(keys, (0..ROWS).map(key).collect::<Vec<_>>());
    assert_eq!(deleted_keys, (150..1_150).map(key).collect::<Vec<_>>());
    Ok(())
}

/// An encrypted tree exports through the same reads: the manifest, the meta
/// block, the rows and every verified frame all decrypt under the tree's
/// provider. Without the provider the table does not open.
#[cfg(feature = "encryption")]
#[test]
fn table_export_reads_an_encrypted_tree() -> crate::Result<()> {
    let provider: Arc<dyn crate::EncryptionProvider> =
        Arc::new(crate::Aes256GcmProvider::new(&[7; 32]));
    let folder = crate::get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_encryption(Some(provider.clone()))
    .open()?;
    let seqno = SequenceNumberCounter::default();
    for i in 0..200u32 {
        tree.insert(format!("k{i:03}"), format!("v{i}"), seqno.next());
    }
    tree.flush_active_memtable(0)?;
    drop(tree);

    let state = read_manifest(folder.path(), &crate::fs::StdFs, Some(provider.clone()))?;
    let record = *state.levels.iter().flatten().flatten().next().unwrap();
    let path = folder
        .path()
        .join(crate::file::TABLES_FOLDER)
        .join(record.id.to_string());
    let context = TableContext {
        encryption: Some(provider),
        ..table_context()
    };
    let export = TableExport::open(&path, &record, None, &context)?;
    assert!(!export.meta()?.is_empty());
    let mut keys = Vec::new();
    for block in export.data_blocks()? {
        let frame = export.frame(
            block.offset,
            block.size,
            crate::table::block::BlockType::Data,
        )?;
        assert!(!frame.payload.is_empty());
        keys.extend(export.rows(&block)?.into_iter().map(|r| r.key.user_key));
    }
    assert_eq!(keys.len(), 200);

    let plain = TableExport::open(&path, &record, None, &table_context());
    assert!(
        plain.is_err(),
        "the table does not open without its provider"
    );
    Ok(())
}

/// A table's range tombstones come back as written, bounds and seqno.
#[test]
fn table_export_carries_range_tombstones() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open(folder.path(), None)?;
    for i in 0..20u32 {
        tree.insert(format!("k{i:03}"), "v", u64::from(i) + 1);
    }
    tree.remove_range(UserKey::from("k005"), UserKey::from("k010"), 100);
    tree.flush_active_memtable(0)?;
    drop(tree);

    let exports = export_tables(folder.path())?;
    assert_eq!(
        exports[0].range_tombstones(),
        vec![RangeDelete {
            start: UserKey::from("k005"),
            end: UserKey::from("k010"),
            seqno: 100,
        }]
    );
    Ok(())
}

/// A blob file's frames come back in file order, one per value written,
/// keyed and sequenced as written, covering the data section end to end;
/// the table that points into it lists it. A frame damaged after the open is
/// refused rather than handed on.
#[test]
fn blob_file_export_frames_cover_every_value() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open(
        folder.path(),
        Some(KvSeparationOptions::default().separation_threshold(16)),
    )?;
    let seqno = SequenceNumberCounter::default();
    let mut written = Vec::new();
    for i in 0..50u32 {
        let s = seqno.next();
        tree.insert(format!("k{i:03}"), format!("{i}").repeat(40), s);
        written.push((format!("k{i:03}").into_bytes(), s));
    }
    tree.flush_active_memtable(0)?;
    drop(tree);

    let state = read_manifest(folder.path(), &crate::fs::StdFs, None)?;
    assert_eq!(state.blob_files.len(), 1);
    let record = state.blob_files[0];
    let path = folder
        .path()
        .join(crate::file::BLOBS_FOLDER)
        .join(record.id.to_string());
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(crate::fs::StdFs);
    let blob = BlobFileExport::open(&path, &record, 0, fs)?;

    let frames = blob.frames()?;
    let got: Vec<(Vec<u8>, u64)> = frames.iter().map(|f| (f.key.to_vec(), f.seqno)).collect();
    assert_eq!(got, written);
    assert!(frames.windows(2).all(|w| w[0].frame_end == w[1].offset));
    let data = section(blob.sections(), b"data").unwrap();
    assert_eq!(frames[0].offset, data.offset);
    assert_eq!(frames.last().unwrap().frame_end, data.offset + data.len);
    let item_count = blob
        .meta()
        .iter()
        .find(|(k, _)| &**k == b"item_count")
        .map(|(_, v)| v.to_vec());
    assert_eq!(item_count, Some(50u64.to_le_bytes().to_vec()));

    let exports = export_tables(folder.path())?;
    let linked = exports[0].linked_blob_files()?.unwrap();
    assert_eq!(linked.len(), 1);
    assert_eq!(linked[0].blob_file_id, record.id);

    let mut bytes = std::fs::read(&path)?;
    let at = usize::try_from(frames[3].frame_end).unwrap() - 2;
    bytes[at] ^= 0x01;
    std::fs::write(&path, &bytes)?;
    assert!(blob.frames().is_err(), "a damaged frame is refused");
    Ok(())
}

/// A table that took parity-repairable damage before the export opened it
/// still opens: the digest the repairs would restore is the manifest's, so the
/// damage is accounted for, and the damaged block comes back repaired. Damage
/// the parity cannot repair leaves the digest unexplained and the table is
/// refused.
#[cfg(feature = "page_ecc")]
#[test]
fn table_export_opens_a_table_whose_damage_the_parity_repairs() -> crate::Result<()> {
    use crate::table::block::{BlockType, EccStatus, Header};

    let folder = crate::get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .page_ecc(true)
    .data_block_size_policy(crate::config::BlockSizePolicy::all(1_024))
    .open()?;
    let seqno = SequenceNumberCounter::default();
    for i in 0..500u32 {
        tree.insert(format!("k{i:04}"), format!("value-{i}"), seqno.next());
    }
    tree.flush_active_memtable(0)?;
    drop(tree);

    let clean = export_tables(folder.path())?;
    let block = clean[0].data_blocks()?.into_iter().nth(1).unwrap();
    let expected = clean[0].frame(block.offset, block.size, BlockType::Data)?;
    drop(clean);

    let state = read_manifest(folder.path(), &crate::fs::StdFs, None)?;
    let record = *state.levels.iter().flatten().flatten().next().unwrap();
    let path = folder
        .path()
        .join(crate::file::TABLES_FOLDER)
        .join(record.id.to_string());
    let mut bytes = std::fs::read(&path)?;
    let at = usize::try_from(block.offset).unwrap() + Header::header_len(BlockType::Data) + 5;
    bytes[at] ^= 0x10;
    std::fs::write(&path, &bytes)?;

    let export = TableExport::open(&path, &record, None, &table_context())?;
    let healed = export.frame(block.offset, block.size, BlockType::Data)?;
    assert_eq!(healed.payload, expected.payload);
    assert_eq!(healed.ecc_status, EccStatus::Corrected);
    drop(export);

    let end = at + usize::try_from(block.size).unwrap() / 2;
    for byte in &mut bytes[at..end] {
        *byte = !*byte;
    }
    std::fs::write(&path, &bytes)?;
    let refused = TableExport::open(&path, &record, None, &table_context()).err();
    assert!(
        matches!(refused, Some(crate::Error::PageEccUnrecoverable { .. })),
        "damage the parity cannot repair is refused at open, naming it, got {refused:?}"
    );
    Ok(())
}

/// Keys in descending byte order, under a name of its own.
struct Reverse;

impl crate::comparator::UserComparator for Reverse {
    fn name(&self) -> &'static str {
        "reverse"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
        b.cmp(a)
    }
}

/// The manifest's header comes back with the state: the comparator name the
/// tree was written under and its level count. A table context is built only
/// for that comparator, so a tree is never read in another key order.
#[test]
fn table_context_requires_the_comparator_the_tree_was_written_under() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let reverse: crate::comparator::SharedComparator = Arc::new(Reverse);
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .comparator(reverse.clone())
    .open()?;
    let seqno = SequenceNumberCounter::default();
    for i in 0..50u32 {
        tree.insert(format!("k{i:03}"), "v", seqno.next());
    }
    tree.flush_active_memtable(0)?;
    drop(tree);

    let state = read_manifest(folder.path(), &crate::fs::StdFs, None)?;
    assert_eq!(state.comparator_name, "reverse");
    assert_eq!(state.level_count, 7);

    let fs: Arc<dyn crate::fs::Fs> = Arc::new(crate::fs::StdFs);
    let wrong = TableContext::new(
        &state,
        fs.clone(),
        None,
        #[cfg(zstd_any)]
        crate::compression::ZstdDictionaries::new(),
        crate::comparator::default_comparator(),
    );
    assert!(matches!(
        wrong,
        Err(crate::Error::ComparatorMismatch { ref stored, supplied: "default" }) if stored == "reverse"
    ));

    let context = TableContext::new(
        &state,
        fs,
        None,
        #[cfg(zstd_any)]
        crate::compression::ZstdDictionaries::new(),
        reverse,
    )?;
    let record = *state.levels.iter().flatten().flatten().next().unwrap();
    let path = folder
        .path()
        .join(crate::file::TABLES_FOLDER)
        .join(record.id.to_string());
    let export = TableExport::open(&path, &record, None, &context)?;
    let mut keys = Vec::new();
    for block in export.data_blocks()? {
        keys.extend(
            export
                .rows(&block)?
                .into_iter()
                .map(|r| r.key.user_key.to_vec()),
        );
    }
    let expected: Vec<Vec<u8>> = (0..50u32)
        .rev()
        .map(|i| format!("k{i:03}").into_bytes())
        .collect();
    assert_eq!(keys, expected);
    Ok(())
}

/// A table an in-place heal rewrote before it could refresh the manifest
/// opens: the attestation the heal left binds the bytes on disk to the digest
/// the manifest still holds. The same bytes without the attestation are
/// refused.
#[test]
fn table_export_accepts_bytes_a_heal_attestation_binds_to_the_manifest() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open(folder.path(), None)?;
    let seqno = SequenceNumberCounter::default();
    for i in 0..200u32 {
        tree.insert(format!("k{i:03}"), "v", seqno.next());
    }
    tree.flush_active_memtable(0)?;
    drop(tree);

    let state = read_manifest(folder.path(), &crate::fs::StdFs, None)?;
    let record = *state.levels.iter().flatten().flatten().next().unwrap();
    let path = folder
        .path()
        .join(crate::file::TABLES_FOLDER)
        .join(record.id.to_string());
    // Stand-in for the healed bytes, in a data block the open does not read:
    // the manifest no longer describes them.
    let block = TableExport::open(&path, &record, None, &table_context())?
        .data_blocks()?
        .remove(0);
    let mut bytes = std::fs::read(&path)?;
    let at = usize::try_from(block.offset).unwrap() + usize::try_from(block.size).unwrap() - 1;
    bytes[at] ^= 0x01;
    std::fs::write(&path, &bytes)?;
    let current = crate::repair::compute_table_checksum(&crate::fs::StdFs, &path)?;

    assert!(matches!(
        TableExport::open(&path, &record, None, &table_context()),
        Err(crate::Error::ChecksumMismatch { .. })
    ));

    crate::scrub::heal_attest::write(
        &crate::fs::StdFs,
        &path,
        None,
        record.id,
        crate::Checksum::from_raw(record.checksum),
        crate::Checksum::from_raw(current),
    )?;
    TableExport::open(&path, &record, None, &table_context())?;
    Ok(())
}

/// Damage the parity covers in the side sections and the meta block is
/// accounted for at open like damage in a data block: both copies of the
/// top-level index and the tail meta block are repaired in the digest the
/// open checks, and the table opens.
#[cfg(feature = "page_ecc")]
#[test]
fn table_export_opens_a_table_whose_side_section_damage_the_parity_repairs() -> crate::Result<()> {
    use crate::table::block::{BlockType, Header};

    let folder = crate::get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .page_ecc(true)
    .open()?;
    let seqno = SequenceNumberCounter::default();
    for i in 0..500u32 {
        tree.insert(format!("k{i:04}"), format!("value-{i}"), seqno.next());
    }
    tree.flush_active_memtable(0)?;
    drop(tree);

    let state = read_manifest(folder.path(), &crate::fs::StdFs, None)?;
    let record = *state.levels.iter().flatten().flatten().next().unwrap();
    let path = folder
        .path()
        .join(crate::file::TABLES_FOLDER)
        .join(record.id.to_string());
    let sections = TableExport::open(&path, &record, None, &table_context())?.sections()?;

    let mut bytes = std::fs::read(&path)?;
    for (name, block_type) in [
        (&b"tli"[..], BlockType::Index),
        (&b"tli_tail"[..], BlockType::Index),
        (&b"meta"[..], BlockType::Meta),
    ] {
        let at = section(&sections, name).unwrap().offset;
        let at = usize::try_from(at).unwrap() + Header::header_len(block_type) + 3;
        bytes[at] ^= 0x20;
    }
    std::fs::write(&path, &bytes)?;

    let export = TableExport::open(&path, &record, None, &table_context())?;
    let mut rows = 0;
    for block in export.data_blocks()? {
        rows += export.rows(&block)?.len();
    }
    assert_eq!(rows, 500);
    Ok(())
}

/// A table whose bytes no longer hash to the manifest's checksum is refused
/// at open, before any of its parts is read.
#[test]
fn table_export_refuses_a_file_the_manifest_checksum_does_not_match() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open(folder.path(), None)?;
    let seqno = SequenceNumberCounter::default();
    for i in 0..100u32 {
        tree.insert(format!("k{i:03}"), "v", seqno.next());
    }
    tree.flush_active_memtable(0)?;
    drop(tree);

    let state = read_manifest(folder.path(), &crate::fs::StdFs, None)?;
    let record = state.levels.iter().flatten().flatten().next().unwrap();
    let path = folder
        .path()
        .join(crate::file::TABLES_FOLDER)
        .join(record.id.to_string());
    let tampered = TableRecord {
        checksum: record.checksum ^ 1,
        ..*record
    };
    let result = TableExport::open(&path, &tampered, None, &table_context());
    assert!(
        matches!(result, Err(crate::Error::ChecksumMismatch { .. })),
        "a digest mismatch is refused"
    );
    Ok(())
}

/// Reading the manifest changes no byte of the tree's directory, edit log
/// included: the export is for a store the converter has not decided to touch.
#[test]
fn read_manifest_leaves_the_directory_byte_identical() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open(folder.path(), None)?;
    let seqno = SequenceNumberCounter::default();
    for round in 0..3u32 {
        tree.insert(format!("k{round}"), "v", seqno.next());
        tree.flush_active_memtable(0)?;
    }
    drop(tree);

    let before = snapshot(folder.path())?;
    read_manifest(folder.path(), &crate::fs::StdFs, None)?;
    for export in export_tables(folder.path())? {
        export.sections()?;
        export.meta()?;
        for block in export.data_blocks()? {
            export.rows(&block)?;
        }
        export.filter()?;
        export.locator()?;
    }
    assert_eq!(snapshot(folder.path())?, before);
    Ok(())
}
