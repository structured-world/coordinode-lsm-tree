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

/// A scan from an included lower bound skips the groups that end below it
/// and starts at the group holding the bound itself, whose rows below the
/// bound come with it.
#[test]
fn scan_carrying_from_an_included_bound_starts_at_its_group() -> crate::Result<()> {
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
    tree.update_runtime_config(|cfg| cfg.columnar = true)?;
    let key = |i: u32| format!("k{i:06}");
    for i in 0..4_000u32 {
        tree.insert(key(i), vec![b'v'; 64], 1);
    }
    tree.flush_active_memtable(0)?;
    let Some(table) = tree.current_version().iter_tables().next().cloned() else {
        panic!("one table");
    };
    assert!(table.metadata.data_block_count > 2, "several groups");

    let lo = key(2_000);
    let Some(scan) = table.scan_carrying(
        bounds(Bound::Included(lo.as_str()), Bound::Unbounded),
        crate::table::group_carry::CarryQueue::default(),
        None,
    )?
    else {
        panic!("the bound lies in the table");
    };
    let rows = scan.collect::<crate::Result<Vec<_>>>()?;

    let first = rows.first().map(|row| row.key.user_key.clone());
    assert!(first.as_deref() <= Some(lo.as_bytes()), "{first:?}");
    assert!(first.as_deref() > Some(key(0).as_bytes()), "{first:?}");
    assert_eq!(
        rows.last().map(|row| row.key.user_key.clone()).as_deref(),
        Some(key(3_999).as_bytes())
    );
    Ok(())
}

/// A damaged group fails a scan that records its groups as it fails any
/// scan: the rows before it come out, then the error, and nothing after it,
/// so no group past the damage is handed out or recorded.
#[test]
fn scan_carrying_stops_at_a_damaged_group() -> crate::Result<()> {
    use std::io::{Read, Seek, SeekFrom, Write};

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
    tree.update_runtime_config(|cfg| cfg.columnar = true)?;
    let key = |i: u32| format!("k{i:06}");
    for i in 0..4_000u32 {
        tree.insert(key(i), vec![b'v'; 64], 1);
    }
    tree.flush_active_memtable(0)?;
    let Some(table) = tree.current_version().iter_tables().next().cloned() else {
        panic!("one table");
    };
    let groups = table
        .maintenance_index_walk()
        .map(|keyed| keyed.map(|keyed| *keyed.as_ref()))
        .collect::<crate::Result<Vec<_>>>()?;
    let Some(second) = groups.get(1) else {
        panic!("several groups");
    };

    // One byte in the middle of the second group, past its directory.
    let at = second.offset().0 + u64::from(second.size()) / 2;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&*table.path)?;
    let mut byte = [0u8; 1];
    file.seek(SeekFrom::Start(at))?;
    file.read_exact(&mut byte)?;
    byte[0] ^= 0x01;
    file.seek(SeekFrom::Start(at))?;
    file.write_all(&byte)?;
    file.sync_all()?;

    let Some(scan) = table.scan_carrying(
        bounds(Bound::Unbounded, Bound::Unbounded),
        crate::table::group_carry::CarryQueue::default(),
        None,
    )?
    else {
        panic!("the table has groups");
    };
    let items: Vec<_> = scan.collect();
    let failed = items.iter().position(Result::is_err);
    assert_eq!(
        failed,
        Some(items.len() - 1),
        "the error comes last: {} items",
        items.len()
    );
    assert!(items.len() > 1, "the first group's rows come out first");
    assert!(items.len() < 4_000, "no row past the damaged group");
    Ok(())
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

/// A plain columnar table of 256 rows in row groups of several row pages,
/// and the candidate of its first group, recorded as a compaction's scan
/// records it.
fn first_candidate(dir: &std::path::Path) -> crate::Result<super::CarryCandidate> {
    let fs: alloc::sync::Arc<dyn crate::fs::Fs> = alloc::sync::Arc::new(crate::fs::StdFs);
    let path = dir.join("source");
    let mut writer = crate::table::Writer::new(path.clone(), 1, 0, alloc::sync::Arc::clone(&fs))?
        .use_columnar(true)
        .use_row_group_size(4_096)
        .use_columnar_page_size(512);
    for i in 0..256u32 {
        writer.write(crate::InternalValue::from_components(
            format!("k{i:06}").into_bytes(),
            vec![b'v'; 32],
            1,
            crate::ValueType::Value,
        ))?;
    }
    let Some((id, checksum)) = writer.finish()? else {
        panic!("the source table is written");
    };
    let table = crate::Table::recover(crate::table::RecoverParams::new(
        path,
        checksum,
        id,
        fs,
        crate::comparator::default_comparator(),
        alloc::sync::Arc::new(crate::Cache::with_capacity_bytes(1 << 20)),
    ))?;
    let queue = super::CarryQueue::default();
    let scan = table.scan_carrying(
        bounds(Bound::Unbounded, Bound::Unbounded),
        alloc::sync::Arc::clone(&queue),
        None,
    )?;
    assert!(scan.is_some(), "the table has groups");
    let Some(candidate) = queue.lock().pop_front() else {
        panic!("the first group is recorded");
    };
    Ok(candidate)
}

/// The candidate's group as it lies on disk, and where it lay.
fn raw_group(
    candidate: &super::CarryCandidate,
) -> crate::Result<(crate::Slice, crate::table::writer::VerbatimSource)> {
    Ok((
        candidate.table.read_row_group_raw(&candidate.group, None)?,
        crate::table::writer::VerbatimSource {
            table_id: candidate.table.id(),
            offset: candidate.group.offset().0,
        },
    ))
}

/// A fresh columnar table writer in `dir`.
fn columnar_writer(
    dir: &std::path::Path,
    id: crate::TableId,
) -> crate::Result<crate::table::Writer> {
    Ok(crate::table::Writer::new(
        dir.join(id.to_string()),
        id,
        0,
        alloc::sync::Arc::new(crate::fs::StdFs),
    )?
    .use_columnar(true))
}

/// Bytes that do not tile into whole blocks are no group: a copy of them is
/// refused with an error rather than written as a group the reader would
/// misframe.
#[test]
fn a_carried_group_cut_short_is_refused() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let candidate = first_candidate(dir.path())?;
    let (raw, source) = raw_group(&candidate)?;
    let Some(cut) = raw.get(..raw.len() - 1) else {
        panic!("the group has bytes");
    };
    let mut writer = columnar_writer(dir.path(), 2)?;

    let result = writer.append_carried_row_group(
        (cut, source),
        candidate.row_group,
        crate::table::meta::ValueLayout::Whole,
        &candidate.rows,
        None,
        &crate::comparator::default_comparator(),
    );

    assert!(
        matches!(result, Err(crate::Error::InvalidHeader(_))),
        "{result:?}"
    );
    Ok(())
}

/// A group rebuilt around copied pages is refused, writing nothing, by a
/// table that cannot hold it so: a row-major one, one already holding the
/// group's tag, or rows cut into other row pages than the source's. A fresh
/// columnar table takes it.
#[test]
fn a_partly_carried_group_is_refused_where_it_cannot_land() -> crate::Result<()> {
    use crate::coding::Decode;
    use crate::table::block::{
        Block, BlockIdentity, BlockTransform, BlockType, ChecksumAt, Header,
    };

    let dir = tempfile::tempdir()?;
    let candidate = first_candidate(dir.path())?;
    let (raw, source) = raw_group(&candidate)?;
    let directory_block = Block::from_reader(
        &mut &raw[..],
        BlockIdentity {
            table_id: candidate.table.id(),
            block_type: BlockType::ColumnPageDirectory,
            dict_id: 0,
            window_log: 0,
        },
        &BlockTransform::PLAIN,
        ChecksumAt::table(candidate.table.id(), source.offset),
    )?;
    let directory = crate::table::column_page::PageDirectory::decode(&directory_block.data)?;
    assert!(directory.row_pages().len() > 1, "several row pages");
    let carry = |row_pages: Vec<u32>| crate::table::writer::PageCarry {
        tag: candidate.row_group.tag.get(),
        copied: vec![true; row_pages.len()],
        row_pages,
        raw: &raw,
        source,
        directory: &directory,
        directory_len: Header::decode_from(&mut &raw[..])
            .map(|header| header.on_disk_size_with(None))
            .unwrap_or_default(),
    };
    let cmp = crate::comparator::default_comparator();
    let pages = directory.row_pages().to_vec();

    let mut row_major = crate::table::Writer::new(
        dir.path().join("2"),
        2,
        0,
        alloc::sync::Arc::new(crate::fs::StdFs),
    )?;
    assert!(!row_major.append_partly_carried_row_group(
        &candidate.rows,
        &carry(pages.clone()),
        &cmp
    )?);

    let mut holding_the_tag = columnar_writer(dir.path(), 3)?;
    assert!(holding_the_tag.append_carried_row_group(
        (&raw, source),
        candidate.row_group,
        crate::table::meta::ValueLayout::Whole,
        &candidate.rows,
        None,
        &cmp,
    )?);
    assert!(!holding_the_tag.append_partly_carried_row_group(
        &candidate.rows,
        &carry(pages.clone()),
        &cmp
    )?);

    let mut other_pages = pages.clone();
    other_pages.push(0);
    let mut fresh = columnar_writer(dir.path(), 4)?;
    assert!(!fresh.append_partly_carried_row_group(&candidate.rows, &carry(other_pages), &cmp)?);
    assert!(fresh.append_partly_carried_row_group(&candidate.rows, &carry(pages), &cmp)?);
    Ok(())
}

/// A run copies a group only where its rows could be written as they are:
/// it refuses, writing nothing, no rows at all, pages under another codec
/// than its own, and a group continuing the key it wrote last; a fresh run
/// of the source's codec takes the group, and its table reads back every
/// row.
#[test]
fn a_run_copies_a_group_only_where_its_rows_could_go() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let candidate = first_candidate(dir.path())?;
    let (raw, source) = raw_group(&candidate)?;
    let layout = crate::table::meta::ValueLayout::Whole;
    let codec = candidate.table.metadata.data_block_compression;
    let run = |name: &str| -> crate::Result<crate::table::multi_writer::MultiWriter> {
        let folder = dir.path().join(name);
        std::fs::create_dir_all(&folder)?;
        Ok(crate::table::multi_writer::MultiWriter::new(
            folder,
            SequenceNumberCounter::default(),
            u64::MAX,
            0,
            alloc::sync::Arc::new(crate::fs::StdFs),
        )?
        .use_columnar(true))
    };

    let mut empty = run("empty")?;
    assert!(!empty.carry_row_group(
        (&raw, source),
        candidate.row_group,
        (codec, layout),
        &[],
        None
    )?);

    #[cfg(feature = "lz4")]
    {
        let mut other_codec = run("codec")?;
        assert!(!other_codec.carry_row_group(
            (&raw, source),
            candidate.row_group,
            (crate::CompressionType::Lz4, layout),
            &candidate.rows,
            None,
        )?);
    }

    let mut continuing = run("continuing")?;
    let Some(first) = candidate.rows.first() else {
        panic!("a candidate holds rows");
    };
    continuing.write(first.clone())?;
    assert!(!continuing.carry_row_group(
        (&raw, source),
        candidate.row_group,
        (codec, layout),
        &candidate.rows,
        None,
    )?);

    let mut fresh = run("fresh")?;
    assert!(fresh.carry_row_group(
        (&raw, source),
        candidate.row_group,
        (codec, layout),
        &candidate.rows,
        None,
    )?);
    let written = fresh.finish()?;
    let [(id, checksum)] = written.as_slice() else {
        panic!("one table: {written:?}");
    };
    let table = crate::Table::recover(crate::table::RecoverParams::new(
        dir.path().join("fresh").join(id.to_string()),
        *checksum,
        *id,
        alloc::sync::Arc::new(crate::fs::StdFs),
        crate::comparator::default_comparator(),
        alloc::sync::Arc::new(crate::Cache::with_capacity_bytes(1 << 20)),
    ))?;
    let rows = table.scan()?.collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(rows.len(), candidate.rows.len());
    Ok(())
}
