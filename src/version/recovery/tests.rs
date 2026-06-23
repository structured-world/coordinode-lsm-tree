use super::*;
use crate::coding::Encode;
use crate::fs::{FsOpenOptions, MemFs};
use crate::io::{LittleEndian, WriteBytesExt};
use crate::version::edit::{AddedBlobFile, ChangedLevel, TableDesc, VersionEdit};
use std::io::Write;

/// A snapshot-state `Recovery` with the given level layout and no blobs /
/// GC stats — the starting point an edit is applied on top of.
fn recovery_with(version_id: u64, table_ids: Vec<Vec<Vec<RecoveredTable>>>) -> Recovery {
    Recovery {
        tree_type: TreeType::Standard,
        snapshot_id: version_id,
        curr_version_id: version_id,
        table_ids,
        blob_file_ids: Vec::new(),
        gc_stats: crate::blob_tree::FragmentationMap::default(),
        restrictions: crate::HashMap::default(),
        stats: RecoveryStats::default(),
    }
}

fn rtable(id: u64, seqno: u64) -> RecoveredTable {
    RecoveredTable {
        id,
        checksum: Checksum::from_raw(u128::from(id) * 31),
        global_seqno: seqno,
    }
}

fn tdesc(id: u64, seqno: u64) -> TableDesc {
    TableDesc {
        id,
        checksum: u128::from(id) * 31,
        global_seqno: seqno,
    }
}

#[test]
fn apply_replaces_a_changed_levels_run_layout_wholesale() {
    // L0 starts with one run; the edit gives it a two-run layout.
    let mut rec = recovery_with(1, vec![vec![vec![rtable(1, 10)]]]);
    let edit = VersionEdit {
        new_version_id: 2,
        changed_levels: vec![ChangedLevel {
            level: 0,
            runs: vec![vec![tdesc(1, 10)], vec![tdesc(2, 11)]],
        }],
        ..Default::default()
    };
    rec.apply_edit(&edit).expect("apply");

    assert_eq!(rec.curr_version_id, 2);
    assert_eq!(
        rec.table_ids,
        vec![vec![vec![rtable(1, 10)], vec![rtable(2, 11)]]],
        "changed level's run grouping must be reconstructed exactly",
    );
}

#[test]
fn apply_leaves_unmentioned_levels_untouched() {
    let mut rec = recovery_with(1, vec![vec![vec![rtable(1, 10)]], vec![vec![rtable(9, 5)]]]);
    // Edit only changes L0; L1 must survive verbatim.
    let edit = VersionEdit {
        new_version_id: 2,
        changed_levels: vec![ChangedLevel {
            level: 0,
            runs: vec![vec![tdesc(3, 12)]],
        }],
        ..Default::default()
    };
    rec.apply_edit(&edit).expect("apply");

    assert_eq!(rec.table_ids[0], vec![vec![rtable(3, 12)]]);
    assert_eq!(
        rec.table_ids[1],
        vec![vec![rtable(9, 5)]],
        "a level the edit does not mention is left as-is",
    );
}

#[test]
fn apply_empties_a_drained_level() {
    let mut rec = recovery_with(1, vec![vec![vec![rtable(1, 10)]]]);
    let edit = VersionEdit {
        new_version_id: 2,
        changed_levels: vec![ChangedLevel {
            level: 0,
            runs: vec![],
        }],
        ..Default::default()
    };
    rec.apply_edit(&edit).expect("apply");
    assert!(
        rec.table_ids[0].is_empty(),
        "a compaction that drains a level leaves zero runs",
    );
}

#[test]
fn apply_grows_levels_for_a_higher_index() {
    // Recovery snapshot only has L0; edit targets L2 (compaction output).
    let mut rec = recovery_with(1, vec![vec![vec![rtable(1, 10)]]]);
    let edit = VersionEdit {
        new_version_id: 2,
        changed_levels: vec![ChangedLevel {
            level: 2,
            runs: vec![vec![tdesc(5, 20)]],
        }],
        ..Default::default()
    };
    rec.apply_edit(&edit).expect("apply");
    assert_eq!(rec.table_ids.len(), 3, "levels grew to fit index 2");
    assert!(rec.table_ids[1].is_empty(), "the gap level is empty");
    assert_eq!(rec.table_ids[2], vec![vec![rtable(5, 20)]]);
}

#[test]
fn apply_edit_merges_and_advances_restrictions() {
    let mut rec = recovery_with(1, vec![vec![vec![rtable(1, 10)]]]);
    assert!(rec.restrictions.is_empty(), "starts unrestricted");

    // First slice restricts table 1 at "ccc".
    rec.apply_edit(&VersionEdit {
        new_version_id: 2,
        restrictions: vec![(1, crate::UserKey::from(&b"ccc"[..]))],
        ..Default::default()
    })
    .expect("apply");
    assert_eq!(
        rec.restrictions.get(&1),
        Some(&crate::UserKey::from(&b"ccc"[..])),
    );

    // Next slice advances the same table's bound to "mmm" (overwrite).
    rec.apply_edit(&VersionEdit {
        new_version_id: 3,
        restrictions: vec![(1, crate::UserKey::from(&b"mmm"[..]))],
        ..Default::default()
    })
    .expect("apply");
    assert_eq!(
        rec.restrictions.get(&1),
        Some(&crate::UserKey::from(&b"mmm"[..])),
        "a later slice's higher bound overwrites the earlier one",
    );
}

#[test]
fn parse_restrictions_section_roundtrips_entries() {
    // Build the on-disk section bytes the way `Version::encode_into` does:
    // count, then per entry (id u64, key_len u32, key bytes).
    let mut bytes = Vec::new();
    bytes
        .write_u32::<LittleEndian>(2)
        .expect("encode test bytes");
    bytes
        .write_u64::<LittleEndian>(7)
        .expect("encode test bytes");
    bytes
        .write_u32::<LittleEndian>(3)
        .expect("encode test bytes");
    bytes.write_all(b"mmm").expect("encode test bytes");
    bytes
        .write_u64::<LittleEndian>(42)
        .expect("encode test bytes");
    bytes
        .write_u32::<LittleEndian>(4)
        .expect("encode test bytes");
    bytes.write_all(b"zzzz").expect("encode test bytes");

    let map = parse_restrictions_section(&bytes).expect("parse");
    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&7), Some(&crate::UserKey::from(&b"mmm"[..])));
    assert_eq!(map.get(&42), Some(&crate::UserKey::from(&b"zzzz"[..])));
}

#[test]
fn parse_restrictions_section_rejects_a_truncated_key() {
    // count=1, id, key_len=8, but only 2 key bytes present.
    let mut bytes = Vec::new();
    bytes
        .write_u32::<LittleEndian>(1)
        .expect("encode test bytes");
    bytes
        .write_u64::<LittleEndian>(1)
        .expect("encode test bytes");
    bytes
        .write_u32::<LittleEndian>(8)
        .expect("encode test bytes");
    bytes.write_all(b"xy").expect("encode test bytes");
    assert!(
        parse_restrictions_section(&bytes).is_err(),
        "a key shorter than its length prefix must not silently un-clamp",
    );
}

#[test]
fn parse_restrictions_section_rejects_a_duplicate_table_id() {
    // count=2, but both entries name table id 5. A duplicate could silently
    // lower an already-advanced bound, so it must abort recovery.
    let mut bytes = Vec::new();
    bytes
        .write_u32::<LittleEndian>(2)
        .expect("encode test bytes");
    bytes
        .write_u64::<LittleEndian>(5)
        .expect("encode test bytes");
    bytes
        .write_u32::<LittleEndian>(3)
        .expect("encode test bytes");
    bytes.write_all(b"mmm").expect("encode test bytes");
    bytes
        .write_u64::<LittleEndian>(5)
        .expect("encode test bytes");
    bytes
        .write_u32::<LittleEndian>(3)
        .expect("encode test bytes");
    bytes.write_all(b"ccc").expect("encode test bytes");
    assert!(
        parse_restrictions_section(&bytes).is_err(),
        "a duplicate table id must not silently un-clamp an advanced bound",
    );
}

#[test]
fn apply_adds_updates_and_removes_blob_files() {
    let mut rec = recovery_with(1, vec![]);
    rec.blob_file_ids = vec![(100, Checksum::from_raw(1)), (200, Checksum::from_raw(2))];
    let edit = VersionEdit {
        new_version_id: 2,
        added_blob_files: vec![
            // New blob 300, plus an in-place checksum update of 100.
            AddedBlobFile {
                id: 300,
                checksum: 9,
            },
            AddedBlobFile {
                id: 100,
                checksum: 7,
            },
        ],
        removed_blob_file_ids: vec![200],
        ..Default::default()
    };
    rec.apply_edit(&edit).expect("apply");

    assert!(
        !rec.blob_file_ids.iter().any(|(id, _)| *id == 200),
        "removed blob is gone",
    );
    assert_eq!(
        rec.blob_file_ids
            .iter()
            .find(|(id, _)| *id == 100)
            .map(|(_, c)| *c),
        Some(Checksum::from_raw(7)),
        "existing blob's checksum updated in place",
    );
    assert!(
        rec.blob_file_ids
            .iter()
            .any(|(id, c)| *id == 300 && *c == Checksum::from_raw(9)),
        "new blob appended",
    );
}

#[test]
fn apply_overwrites_gc_stats_when_present() {
    let mut rec = recovery_with(1, vec![]);
    let mut gc = crate::blob_tree::FragmentationMap::default();
    gc.insert(42, crate::blob_tree::FragmentationEntry::new(2, 50, 60));
    let mut bytes = Vec::new();
    gc.encode_into(&mut bytes).expect("encode gc");

    let edit = VersionEdit {
        new_version_id: 2,
        gc_stats: Some(bytes),
        ..Default::default()
    };
    rec.apply_edit(&edit).expect("apply");
    assert_eq!(rec.gc_stats, gc, "GC stats overwritten from the edit");
}

/// Write a CURRENT pointer so `recover()` can find the version file.
///
/// Must be called AFTER the `v{id}` manifest file exists — the
/// pointer's checksum is the canonical digest derived from the
/// manifest's parsed footer (TOC + per-section XXH3-128s) via
/// [`crate::manifest_blocks::current_digest::compute`]. Fixtures
/// that test corruption-recovery typically write the corrupted
/// manifest first, then call this to stamp the CURRENT pointer
/// — the digest binds the TOC (which the corruption inside a
/// section payload doesn't touch), so `get_current_version`
/// accepts the pointer and the per-Block / per-record check
/// downstream is the one that surfaces the corruption.
fn write_current(folder: &Path, version_id: u64, fs: &dyn Fs) -> crate::Result<()> {
    let manifest_path = folder.join(format!("v{version_id}"));
    let archive = crate::manifest_blocks::reader::ManifestArchiveReader::open(
        &manifest_path,
        fs,
        alloc::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
        None,
    )?;
    let checksum = crate::manifest_blocks::current_digest::compute(version_id, archive.footer())?;
    let path = folder.join(CURRENT_VERSION_FILE);
    let mut f = fs.open(
        &path,
        &FsOpenOptions::new().write(true).create(true).truncate(true),
    )?;
    f.write_u64::<LittleEndian>(version_id)?;
    f.write_u128::<LittleEndian>(checksum)?;
    f.write_u8(0)?; // checksum type
    Ok(())
}

type FixtureWriter = crate::manifest_blocks::writer::ManifestArchiveWriter;

/// Open a Blocks-based manifest writer at `folder/v{id}` with
/// the default runtime config. Centralizes the create-new +
/// runtime-snapshot boilerplate every fixture would otherwise
/// repeat verbatim.
fn open_fixture_writer(folder: &Path, id: u64, fs: &dyn Fs) -> crate::Result<FixtureWriter> {
    let path = folder.join(format!("v{id}"));
    FixtureWriter::create(
        &path,
        fs,
        alloc::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
        None,
        crate::fs::SyncMode::Normal,
    )
}

/// Append the standard `tree_type` section (Standard = 0). Every
/// recovery fixture in this module needs one — varying the
/// `tree_type` byte itself is not what these tests exercise.
fn write_tree_type(w: &mut FixtureWriter) -> crate::Result<()> {
    w.start("tree_type")?;
    w.write_u8(0)?;
    Ok(())
}

/// Append an empty `blob_files` section (count = 0). The tables-
/// corruption fixtures don't exercise blob recovery, so they
/// stamp this trivial payload to satisfy `recover()`'s
/// "section must exist" check.
fn write_empty_blob_files(w: &mut FixtureWriter) -> crate::Result<()> {
    w.start("blob_files")?;
    w.write_u32::<LittleEndian>(0)?;
    Ok(())
}

/// Append an empty `blob_gc_stats` section (count = 0). Same
/// rationale as [`write_empty_blob_files`].
fn write_empty_blob_gc_stats(w: &mut FixtureWriter) -> crate::Result<()> {
    w.start("blob_gc_stats")?;
    w.write_u32::<LittleEndian>(0)?;
    Ok(())
}

/// Write a version sfa archive with a corrupt `table_count` (`u32::MAX`).
///
/// All four sfa sections are written because `recover()` requires them
/// all — only the tables section carries the corrupt payload.
fn write_corrupt_table_count(folder: &Path, id: u64, fs: &dyn Fs) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_tree_type(&mut w)?;

    w.start("tables")?;
    w.write_u8(1)?; // 1 level
    w.write_u8(1)?; // 1 run
    w.write_u32::<LittleEndian>(u32::MAX)?; // corrupt: exceeds section length

    write_empty_blob_files(&mut w)?;
    write_empty_blob_gc_stats(&mut w)?;

    w.finish()?;
    Ok(())
}

/// Write a version sfa archive with a corrupt `blob_file_count` (`u32::MAX`).
///
/// All four sfa sections required by `recover()` are present — only the
/// `blob_files` section carries the corrupt payload.
fn write_corrupt_blob_count(folder: &Path, id: u64, fs: &dyn Fs) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_tree_type(&mut w)?;

    w.start("tables")?;
    w.write_u8(0)?; // 0 levels

    w.start("blob_files")?;
    w.write_u32::<LittleEndian>(u32::MAX)?; // corrupt

    w.start("blob_gc_stats")?;
    w.write_u32::<LittleEndian>(0)?;

    w.finish()?;
    Ok(())
}

#[test]
fn recover_rejects_corrupt_table_count() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/corrupt/tables");
    fs.create_dir_all(folder)?;

    write_corrupt_table_count(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let Err(err) = recover(folder, &fs, ManifestRecoveryMode::AbsoluteConsistency, None) else {
        panic!("corrupt table_count should fail");
    };
    assert!(
        matches!(err, crate::Error::Unrecoverable),
        "expected Unrecoverable, got: {err:?}"
    );

    Ok(())
}

#[test]
fn recover_rejects_corrupt_blob_file_count() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/corrupt/blobs");
    fs.create_dir_all(folder)?;

    write_corrupt_blob_count(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let Err(err) = recover(folder, &fs, ManifestRecoveryMode::AbsoluteConsistency, None) else {
        panic!("corrupt blob_file_count should fail");
    };
    assert!(
        matches!(err, crate::Error::Unrecoverable),
        "expected Unrecoverable, got: {err:?}"
    );

    Ok(())
}

/// Writes a `vN` archive whose `tables` section claims more table
/// entries than are actually present (count says 5, only 1 full
/// entry's worth of bytes is written, the next entry's `id: u64`
/// is cut off mid-stream). Used to exercise the
/// `TolerateCorruptedTailRecords` recovery path.
///
/// The `id` parameter selects the version file name; the function
/// otherwise produces a deterministic shape: 1 level, 1 run,
/// declared count = `declared`, actual full entries = `actual`.
fn write_truncated_tables_tail(
    folder: &Path,
    id: u64,
    declared: u32,
    actual: u32,
    fs: &dyn Fs,
) -> crate::Result<()> {
    assert!(
        actual < declared,
        "actual must be < declared for truncation"
    );
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_tree_type(&mut w)?;

    w.start("tables")?;
    w.write_u8(1)?; // 1 level
    w.write_u8(1)?; // 1 run
    w.write_u32::<LittleEndian>(declared)?;
    // Write `actual` complete framed entries, then stop. SFA pads
    // the section length to whatever bytes we wrote — the
    // truncation surfaces inside the per-entry decode loop, not
    // at the SFA layer.
    for entry_id in 0..actual {
        crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
            payload.write_u64::<LittleEndian>(u64::from(entry_id))?;
            payload.write_u8(0)?; // checksum_type
            payload.write_u128::<LittleEndian>(0)?; // checksum
            payload.write_u64::<LittleEndian>(0)?; // global_seqno
            Ok(())
        })?;
    }

    write_empty_blob_files(&mut w)?;
    write_empty_blob_gc_stats(&mut w)?;

    w.finish()?;
    Ok(())
}

#[test]
fn recover_absolute_consistency_rejects_truncated_tables_tail() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/absolute/tail");
    fs.create_dir_all(folder)?;

    // Section declares 5 entries, only 1 actually written.
    write_truncated_tables_tail(folder, 1, 5, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let result = recover(folder, &fs, ManifestRecoveryMode::AbsoluteConsistency, None);
    let err = result.expect_err("truncated tail must abort under AbsoluteConsistency");
    // Either Io(UnexpectedEof) (from the byteorder read) — both are
    // acceptable strict-mode failures. The contract is: SOMETHING
    // surfaces, the open does not silently succeed with partial data.
    assert!(
        matches!(&err, crate::Error::Io(e) if e.kind() == crate::io::ErrorKind::UnexpectedEof)
            || matches!(err, crate::Error::Unrecoverable),
        "expected UnexpectedEof or Unrecoverable, got: {err:?}",
    );
    Ok(())
}

#[test]
fn recover_tolerate_tail_keeps_consistent_prefix_of_tables() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/tolerate/tail");
    fs.create_dir_all(folder)?;

    // Section declares 5 entries, only 1 actually written → expect
    // 1 entry recovered, 4 silently dropped + warn logged.
    write_truncated_tables_tail(folder, 1, 5, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let recovery = recover(
        folder,
        &fs,
        ManifestRecoveryMode::TolerateCorruptedTailRecords,
        None,
    )?;
    assert_eq!(
        recovery.table_ids.len(),
        1,
        "expected 1 level, got {}: {:?}",
        recovery.table_ids.len(),
        recovery.table_ids.iter().map(Vec::len).collect::<Vec<_>>(),
    );
    let level = &recovery.table_ids[0];
    assert_eq!(level.len(), 1, "expected 1 run in the recovered level");
    assert_eq!(
        level[0].len(),
        1,
        "expected 1 table record (the consistent prefix); got {}",
        level[0].len(),
    );
    Ok(())
}

#[test]
fn recover_tolerate_tail_does_not_swallow_invalid_tag() -> crate::Result<()> {
    // A corrupt non-zero `checksum_type` byte is NOT a clean tail
    // truncation; the tail-tolerant mode must still abort on it.
    // Otherwise it'd silently drop the bad record plus everything
    // after it on bit-rot, which is the opposite of the documented
    // contract (tail-tolerance is for write-incomplete scenarios,
    // not for arbitrary corruption).
    let fs = MemFs::new();
    let folder = Path::new("/tolerate/bad_tag");
    fs.create_dir_all(folder)?;

    let mut w = open_fixture_writer(folder, 1, &fs)?;
    write_tree_type(&mut w)?;
    w.start("tables")?;
    w.write_u8(1)?; // 1 level
    w.write_u8(1)?; // 1 run
    w.write_u32::<LittleEndian>(1)?; // 1 entry
    // Framed record with a corrupt `checksum_type` byte in the
    // payload. The framing XXH3 still covers the payload, so
    // the record decodes cleanly at the framing layer; the
    // InvalidTag surfaces from `decode_table_entry_payload`.
    // Handling per mode:
    //   - AbsoluteConsistency:           aborts (used by this test)
    //   - TolerateCorruptedTailRecords:  also aborts — tail-
    //     tolerance is for write-incomplete tail scenarios, not
    //     arbitrary in-section corruption.
    //   - PointInTimeRecovery:           truncates the
    //     recovered tree at the corrupt record's level/run.
    //   - SkipAnyCorruptedRecords:       skips this record,
    //     continues with the rest of the section.
    // This fixture exercises the first row; PIT/SkipAny
    // behaviour is covered by separate test fixtures below.
    crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
        payload.write_u64::<LittleEndian>(0)?; // id
        payload.write_u8(0xFF)?; // corrupt checksum_type
        payload.write_u128::<LittleEndian>(0)?;
        payload.write_u64::<LittleEndian>(0)?;
        Ok(())
    })?;
    write_empty_blob_files(&mut w)?;
    w.start("blob_gc_stats")?;
    w.write_u32::<LittleEndian>(0)?;
    w.finish()?;
    write_current(folder, 1, &fs)?;

    let result = recover(
        folder,
        &fs,
        ManifestRecoveryMode::TolerateCorruptedTailRecords,
        None,
    );
    let err = result.expect_err("InvalidTag must still abort under TolerateCorruptedTailRecords");
    assert!(
        matches!(err, crate::Error::InvalidTag(("ChecksumType", 0xFF))),
        "expected InvalidTag, got: {err:?}",
    );
    Ok(())
}

/// Writes a `vN` archive with two runs in one level:
/// run #0 is a complete entry (declared = actual = 1),
/// run #1 declares 3 entries but the section is cut so the
/// `table_count` u32 of run #1 reads partially → EOF.
/// Exercises the tail-tolerant case where the cut happens mid-RUN
/// inside an otherwise-valid level. The consistent prefix (run #0
/// of level #0) MUST survive in the recovered Version.
fn write_truncated_at_second_run(folder: &Path, id: u64, fs: &dyn Fs) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_tree_type(&mut w)?;
    w.start("tables")?;
    w.write_u8(1)?; // 1 level
    w.write_u8(2)?; // 2 runs in that level
    // run #0: declared=1, actual=1 — the consistent prefix.
    w.write_u32::<LittleEndian>(1)?;
    crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
        payload.write_u64::<LittleEndian>(42)?; // id
        payload.write_u8(0)?; // checksum_type
        payload.write_u128::<LittleEndian>(0)?;
        payload.write_u64::<LittleEndian>(0)?;
        Ok(())
    })?;
    // run #1: only 2 of the 4 bytes of `table_count` are written.
    // The reader gets `UnexpectedEof` on the u32 read.
    w.write_u8(0xAA)?;
    w.write_u8(0xBB)?;
    write_empty_blob_files(&mut w)?;
    w.start("blob_gc_stats")?;
    w.write_u32::<LittleEndian>(0)?;
    w.finish()?;
    Ok(())
}

#[test]
fn recover_tolerate_tail_keeps_consistent_prefix_within_a_level() -> crate::Result<()> {
    // Regression: when the EOF cut happens between two runs of
    // the same level, the tail-tolerant break must push the
    // partial level into `levels` so the consistent prefix
    // (run #0 with 1 entry) survives. Earlier behaviour broke
    // out of `'levels` without pushing, silently dropping the
    // already-decoded run.
    let fs = MemFs::new();
    let folder = Path::new("/tolerate/midlevel");
    fs.create_dir_all(folder)?;

    write_truncated_at_second_run(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let recovery = recover(
        folder,
        &fs,
        ManifestRecoveryMode::TolerateCorruptedTailRecords,
        None,
    )?;
    assert_eq!(
        recovery.table_ids.len(),
        1,
        "expected the partially-decoded level to be present, got {} levels",
        recovery.table_ids.len(),
    );
    let level = &recovery.table_ids[0];
    assert_eq!(
        level.len(),
        1,
        "expected 1 surviving run (the consistent prefix), got {}",
        level.len(),
    );
    assert_eq!(
        level[0].len(),
        1,
        "expected the run to contain its 1 fully-decoded entry",
    );
    assert_eq!(level[0][0].id, 42, "wrong entry id recovered");
    Ok(())
}

/// Writes a `vN` archive whose `blob_files` section declares
/// `declared` entries but only writes `actual` complete 25-byte
/// records, cutting mid-stream after that. Mirrors the analogous
/// `tables` fixture for the blob-files surface.
fn write_truncated_blob_tail(
    folder: &Path,
    id: u64,
    declared: u32,
    actual: u32,
    fs: &dyn Fs,
) -> crate::Result<()> {
    assert!(
        actual < declared,
        "actual must be < declared for truncation"
    );
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_tree_type(&mut w)?;
    w.start("tables")?;
    w.write_u8(0)?; // 0 levels
    w.start("blob_files")?;
    w.write_u32::<LittleEndian>(declared)?;
    for entry_id in 0..actual {
        crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
            payload.write_u64::<LittleEndian>(u64::from(entry_id))?;
            payload.write_u8(0)?; // checksum_type
            payload.write_u128::<LittleEndian>(0)?; // checksum
            Ok(())
        })?;
    }
    w.start("blob_gc_stats")?;
    w.write_u32::<LittleEndian>(0)?;
    w.finish()?;
    Ok(())
}

#[test]
fn recover_tolerate_tail_keeps_consistent_prefix_of_blob_files() -> crate::Result<()> {
    // Companion to `recover_tolerate_tail_keeps_consistent_prefix_of_tables`
    // for the blob_files surface. Without this test, a regression
    // in the blob-files tail-tolerant path would slip through
    // because the tables-only test already passes.
    let fs = MemFs::new();
    let folder = Path::new("/tolerate/blob_tail");
    fs.create_dir_all(folder)?;

    write_truncated_blob_tail(folder, 1, 5, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let recovery = recover(
        folder,
        &fs,
        ManifestRecoveryMode::TolerateCorruptedTailRecords,
        None,
    )?;
    assert_eq!(
        recovery.blob_file_ids.len(),
        1,
        "expected 1 surviving blob_file entry (the consistent prefix), got {}",
        recovery.blob_file_ids.len(),
    );
    assert_eq!(
        recovery.blob_file_ids[0].0, 0,
        "wrong blob_file id recovered",
    );
    Ok(())
}

/// Writes a `vN` archive whose `blob_gc_stats` section is
/// truncated to zero bytes. Mimics a power-loss right after the
/// `blob_files` section was committed but before the
/// `blob_gc_stats` payload landed — `FragmentationMap::decode_from`
/// hits `UnexpectedEof` on the first byte.
fn write_truncated_blob_gc_stats(folder: &Path, id: u64, fs: &dyn Fs) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_tree_type(&mut w)?;
    w.start("tables")?;
    w.write_u8(0)?; // 0 levels
    w.start("blob_files")?;
    w.write_u32::<LittleEndian>(0)?;
    // blob_gc_stats section started but no payload written —
    // section.len() == 0, FragmentationMap::decode_from will
    // surface UnexpectedEof on its first read.
    w.start("blob_gc_stats")?;
    w.finish()?;
    Ok(())
}

#[test]
fn recover_tolerate_tail_handles_truncated_blob_gc_stats() -> crate::Result<()> {
    // Tail-tolerant mode must extend beyond the record-list
    // sections (tables / blob_files): a power-loss between the
    // blob_files commit and the blob_gc_stats payload is the
    // exact "writer never finished" shape this mode is meant to
    // salvage. Strict mode still aborts; tolerant mode emits a
    // warn and uses a default (empty) FragmentationMap.
    let fs = MemFs::new();
    let folder = Path::new("/tolerate/gc_stats");
    fs.create_dir_all(folder)?;
    write_truncated_blob_gc_stats(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    // Strict mode: hard fail.
    let strict = recover(folder, &fs, ManifestRecoveryMode::AbsoluteConsistency, None);
    assert!(
        strict.is_err(),
        "strict mode must abort on truncated blob_gc_stats; got Ok",
    );

    // Tolerant mode: succeeds with empty gc_stats. Compare via
    // `Default` (FragmentationMap implements PartialEq) — the
    // type does not expose an `is_empty()` accessor.
    let lenient = recover(
        folder,
        &fs,
        ManifestRecoveryMode::TolerateCorruptedTailRecords,
        None,
    )?;
    assert_eq!(
        lenient.gc_stats,
        crate::blob_tree::FragmentationMap::default(),
        "tolerant mode must produce default (empty) gc_stats on truncated section",
    );
    Ok(())
}

// ====================================================================
// PointInTimeRecovery + SkipAnyCorruptedRecords corruption-matrix tests
// ====================================================================
//
// The fixtures below write a complete framed manifest, then pick one
// specific framed record and emit it with a deliberately-wrong XXH3
// digest. That gives the reader a `FramedRecordOutcome::ChecksumMismatch`
// at a known position so each mode's per-record dispatch is exercised
// end-to-end, not just at the framing-helper unit level.

/// Writes one framed table record with a CORRECT XXH3 digest.
fn write_good_table_record<W: std::io::Write>(w: &mut W, id: u64) -> crate::Result<()> {
    crate::version::framing::write_framed_record(w, &mut Vec::new(), |payload| {
        payload.write_u64::<LittleEndian>(id)?;
        payload.write_u8(0)?; // checksum_type
        payload.write_u128::<LittleEndian>(0)?;
        payload.write_u64::<LittleEndian>(0)?;
        Ok(())
    })
}

/// Writes one framed table record but with an INTENTIONALLY WRONG
/// XXH3 digest in the framing header — emulates payload bit-rot
/// inside an otherwise structurally-valid record. The `len` field
/// of the header is correct (so the reader's `BadHeader` path does
/// NOT trigger), which means the `ChecksumMismatch` arm is the one
/// being exercised.
fn write_bad_table_record<W: std::io::Write>(w: &mut W, id: u64) -> crate::Result<()> {
    let mut payload: Vec<u8> = Vec::new();
    payload.write_u64::<LittleEndian>(id)?;
    payload.write_u8(0)?;
    payload.write_u128::<LittleEndian>(0)?;
    payload.write_u64::<LittleEndian>(0)?;

    #[expect(
        clippy::cast_possible_truncation,
        reason = "payload is 33 bytes — fits in u32"
    )]
    let len = payload.len() as u32;
    w.write_u32::<LittleEndian>(len)?;
    // INTENTIONALLY WRONG digest. Real one would be
    // `xxh3_64(&payload)`; using `0xDEAD_BEEF_DEAD_BEEF` instead
    // so the reader's mismatch arm fires deterministically.
    w.write_u64::<LittleEndian>(0xDEAD_BEEF_DEAD_BEEF)?;
    w.write_all(&payload)?;
    Ok(())
}

/// Builds a manifest with two levels: level 0 has one run with three
/// table records, where the MIDDLE record carries a corrupt XXH3
/// digest. Level 1 has one run with two good records.
///
/// The shape lets a test observe three distinct recovery outcomes
/// from the same on-disk bytes:
/// - `AbsoluteConsistency` aborts on the corrupt record
/// - `PointInTimeRecovery` keeps level 0 record #0 only (dropping
///   #1 and #2 of the current run, and dropping level 1 entirely)
/// - `SkipAnyCorruptedRecords` keeps level 0 records #0 and #2
///   (skipping #1) AND keeps level 1
fn write_manifest_with_mid_record_corruption(
    folder: &Path,
    id: u64,
    fs: &dyn Fs,
) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_tree_type(&mut w)?;

    w.start("tables")?;
    w.write_u8(2)?; // 2 levels
    // Level 0: 1 run, 3 records, middle one is corrupt.
    w.write_u8(1)?;
    w.write_u32::<LittleEndian>(3)?;
    write_good_table_record(&mut w, 100)?;
    write_bad_table_record(&mut w, 101)?;
    write_good_table_record(&mut w, 102)?;
    // Level 1: 1 run, 2 records, both good.
    w.write_u8(1)?;
    w.write_u32::<LittleEndian>(2)?;
    write_good_table_record(&mut w, 200)?;
    write_good_table_record(&mut w, 201)?;

    write_empty_blob_files(&mut w)?;
    w.start("blob_gc_stats")?;
    w.write_u32::<LittleEndian>(0)?;
    w.finish()?;
    Ok(())
}

#[test]
fn recover_absolute_consistency_rejects_mid_record_corruption() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/absolute/mid_corrupt");
    fs.create_dir_all(folder)?;
    write_manifest_with_mid_record_corruption(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let err = recover(folder, &fs, ManifestRecoveryMode::AbsoluteConsistency, None)
        .expect_err("corrupt record must abort AbsoluteConsistency");
    assert!(
        matches!(
            err,
            crate::Error::ManifestFrameChecksumMismatch {
                section: "tables",
                ..
            }
        ),
        "expected ManifestFrameChecksumMismatch on the tables section, got: {err:?}",
    );
    Ok(())
}

#[test]
fn recover_pit_truncates_at_corrupt_record() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/pit/mid_corrupt");
    fs.create_dir_all(folder)?;
    write_manifest_with_mid_record_corruption(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let recovery = recover(folder, &fs, ManifestRecoveryMode::PointInTimeRecovery, None)?;

    // PIT contract: drop in-progress run contents + all
    // subsequent records; the recovered prefix is level 0 with
    // run 0 containing only the consistent prefix record (id=100).
    // Level slots are preserved (persisted level_count is 2, so
    // 2 level slots survive — level 1 padded empty to keep
    // downstream level_count() invariants intact).
    assert_eq!(
        recovery.table_ids.len(),
        2,
        "expected both persisted level slots to survive (level 1 padded \
         empty after PIT truncated its records); got {}",
        recovery.table_ids.len(),
    );
    let level = &recovery.table_ids[0];
    assert_eq!(level.len(), 1, "expected 1 run in level 0");
    let run = &level[0];
    assert_eq!(
        run.len(),
        1,
        "expected only the pre-corruption record to survive in run 0; got {} records",
        run.len(),
    );
    assert!(
        recovery.table_ids[1].is_empty(),
        "expected level 1 to be empty (PIT dropped its records); got {} runs",
        recovery.table_ids[1].len(),
    );
    assert_eq!(run[0].id, 100, "expected id=100 (the good prefix record)");
    Ok(())
}

#[test]
fn recover_skip_any_skips_corrupt_record_and_keeps_neighbours() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/skip_any/mid_corrupt");
    fs.create_dir_all(folder)?;
    write_manifest_with_mid_record_corruption(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let recovery = recover(
        folder,
        &fs,
        ManifestRecoveryMode::SkipAnyCorruptedRecords,
        None,
    )?;

    // SkipAny contract: log the single bad record, advance past
    // it via the framing length header, keep going. Both
    // surrounding records and the entire next level survive.
    assert_eq!(
        recovery.table_ids.len(),
        2,
        "expected both levels to survive under SkipAny",
    );
    let l0_run = &recovery.table_ids[0][0];
    assert_eq!(
        l0_run.len(),
        2,
        "expected 2 records in level-0 run 0 (id=100 + id=102, skipping the corrupt id=101); got {}",
        l0_run.len(),
    );
    assert_eq!(l0_run[0].id, 100);
    assert_eq!(l0_run[1].id, 102);

    let l1_run = &recovery.table_ids[1][0];
    assert_eq!(
        l1_run.len(),
        2,
        "expected level 1 to recover its full 2 records under SkipAny",
    );
    assert_eq!(l1_run[0].id, 200);
    assert_eq!(l1_run[1].id, 201);
    Ok(())
}

/// Builds a manifest where a `blob_files` record (not a table
/// record) carries the corrupt XXH3 digest. Exercises the same
/// per-mode dispatch on the `blob_files` section to confirm the
/// reader's PIT / `SkipAny` logic was wired through symmetrically.
fn write_manifest_with_corrupt_blob_record(
    folder: &Path,
    id: u64,
    fs: &dyn Fs,
) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_tree_type(&mut w)?;

    w.start("tables")?;
    w.write_u8(0)?; // 0 levels (focus is on blob_files section)

    w.start("blob_files")?;
    w.write_u32::<LittleEndian>(3)?;
    // good, bad, good
    crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
        payload.write_u64::<LittleEndian>(10)?;
        payload.write_u8(0)?;
        payload.write_u128::<LittleEndian>(0)?;
        Ok(())
    })?;
    // Corrupt the middle blob record: write a framed header with
    // a wrong digest but a correct length, so the reader treats
    // it as ChecksumMismatch.
    let mut payload: Vec<u8> = Vec::new();
    payload.write_u64::<LittleEndian>(11)?;
    payload.write_u8(0)?;
    payload.write_u128::<LittleEndian>(0)?;
    #[expect(
        clippy::cast_possible_truncation,
        reason = "payload is 25 bytes — fits in u32"
    )]
    let len = payload.len() as u32;
    w.write_u32::<LittleEndian>(len)?;
    w.write_u64::<LittleEndian>(0xDEAD_BEEF_DEAD_BEEF)?;
    w.write_all(&payload)?;
    crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
        payload.write_u64::<LittleEndian>(12)?;
        payload.write_u8(0)?;
        payload.write_u128::<LittleEndian>(0)?;
        Ok(())
    })?;

    w.start("blob_gc_stats")?;
    w.write_u32::<LittleEndian>(0)?;
    w.finish()?;
    Ok(())
}

#[test]
fn recover_skip_any_skips_corrupt_blob_record() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/skip_any/blob_mid_corrupt");
    fs.create_dir_all(folder)?;
    write_manifest_with_corrupt_blob_record(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let recovery = recover(
        folder,
        &fs,
        ManifestRecoveryMode::SkipAnyCorruptedRecords,
        None,
    )?;
    let ids: Vec<u64> = recovery.blob_file_ids.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        ids,
        vec![10, 12],
        "expected SkipAny to keep ids 10 and 12 while skipping the corrupt id 11",
    );
    Ok(())
}

/// Builds a manifest where level 0 has one good record but level 1's
/// FIRST table record carries a corrupt XXH3 digest. With PIT or
/// `SkipAny` + `BadHeader` handling, the early-exit branches push the
/// (empty) in-progress run into the level — and the (empty) level
/// into the levels vec — before breaking out. The recovered
/// `Recovery` then carries an empty run, which `Version::from_recovery`
/// later rejects via `Run::new(...).expect("persisted runs should not
/// be empty")` — a panic in code that was supposed to be the tolerant
/// path.
fn write_manifest_with_corrupt_first_record_of_second_level(
    folder: &Path,
    id: u64,
    fs: &dyn Fs,
) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_tree_type(&mut w)?;

    w.start("tables")?;
    w.write_u8(2)?; // 2 levels
    // Level 0: 1 run, 1 good record (so the consistent prefix is
    // non-empty and the PIT/SkipAny path will reach level 1).
    w.write_u8(1)?;
    w.write_u32::<LittleEndian>(1)?;
    write_good_table_record(&mut w, 100)?;
    // Level 1: 1 run, 1 corrupt record AS THE FIRST AND ONLY
    // record. Under PIT this triggers the corruption-handling
    // arm BEFORE any record has been pushed into the new run —
    // run.len() == 0 when the branch pushes it into level.
    w.write_u8(1)?;
    w.write_u32::<LittleEndian>(1)?;
    write_bad_table_record(&mut w, 200)?;

    write_empty_blob_files(&mut w)?;
    w.start("blob_gc_stats")?;
    w.write_u32::<LittleEndian>(0)?;
    w.finish()?;
    Ok(())
}

/// Regression test for review finding on PR #342: tolerant/PIT
/// early-exit branches push the in-progress `run` into the current
/// `level` regardless of whether the run is empty, and push the
/// `level` regardless of whether it has any runs. When the FIRST
/// record of a new run is the corrupt one, the run is empty at the
/// time the branch fires — `Recovery::table_ids` then carries an
/// empty inner vec, which `Version::from_recovery` later panics on
/// via `Run::new(...).expect("persisted runs should not be empty")`.
/// Invariants under test:
/// 1. `recover()` must never produce empty RUNS in
///    `table_ids` — an empty inner-inner vec fails downstream
///    at `Run::new(...).expect("persisted runs should not be
///    empty")` inside `Version::from_recovery`.
/// 2. Empty LEVELS (an outer slot containing zero runs) ARE
///    expected and permitted — they're the canonical
///    representation for a level that PIT / `SkipAny` /
///    tail-truncation cleared of all surviving records. The
///    slot survives so the `level_count` stays preserved;
///    the placeholder is "no runs in this slot", NOT "one
///    placeholder run with no tables inside".
/// 3. The number of level SLOTS in `table_ids` must equal the
///    persisted `level_count` — downstream code
///    (compaction/leveled asserts
///    `version.level_count() == config.level_count`)
///    reads `levels.len()` directly and shrinking it crashes
///    the tree.
#[test]
fn recover_pit_drops_empty_run_when_corruption_hits_first_record() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/pit/empty_run");
    fs.create_dir_all(folder)?;
    write_manifest_with_corrupt_first_record_of_second_level(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let recovery = recover(folder, &fs, ManifestRecoveryMode::PointInTimeRecovery, None)?;

    // The persisted manifest declared 2 levels. The recovered
    // shape must keep that count — level 1 just has no
    // surviving runs after PIT dropped its only (corrupt) record.
    assert_eq!(
        recovery.table_ids.len(),
        2,
        "expected the recovered shape to preserve the persisted \
         level_count (2); got {} levels",
        recovery.table_ids.len(),
    );

    // No empty runs anywhere in the recovered shape.
    for (level_idx, level) in recovery.table_ids.iter().enumerate() {
        for (run_idx, run) in level.iter().enumerate() {
            assert!(
                !run.is_empty(),
                "level {level_idx} run {run_idx} is empty — \
                 Version::from_recovery calls Run::new on this and \
                 panics via the .expect(\"persisted runs should not \
                 be empty\")",
            );
        }
    }

    // Level 0 survived intact with its one good record.
    assert_eq!(recovery.table_ids[0].len(), 1, "level 0 should have 1 run");
    assert_eq!(recovery.table_ids[0][0][0].id, 100);
    // Level 1 had its only record dropped → 0 runs (the slot
    // survives empty, not as a placeholder containing an empty run).
    assert!(
        recovery.table_ids[1].is_empty(),
        "level 1 should have no runs after PIT dropped its corrupt-only run",
    );
    Ok(())
}

/// Builds a manifest where level 0 has ONE run of ONE corrupt
/// record. Under `SkipAnyCorruptedRecords` the single record is
/// skipped → the run is empty when the per-run record loop
/// completes → the unconditional `level.push(run)` at line 498
/// produces an empty run in `Recovery::table_ids`.
fn write_manifest_with_all_records_in_run_corrupt(
    folder: &Path,
    id: u64,
    fs: &dyn Fs,
) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_tree_type(&mut w)?;

    w.start("tables")?;
    w.write_u8(2)?; // 2 levels persisted
    // Level 0: 1 run, 1 record, all corrupt.
    w.write_u8(1)?;
    w.write_u32::<LittleEndian>(1)?;
    write_bad_table_record(&mut w, 100)?;
    // Level 1: 1 run, 1 good record (so the recovered shape
    // still has surviving content + a non-trivial level slot).
    w.write_u8(1)?;
    w.write_u32::<LittleEndian>(1)?;
    write_good_table_record(&mut w, 200)?;

    write_empty_blob_files(&mut w)?;
    w.start("blob_gc_stats")?;
    w.write_u32::<LittleEndian>(0)?;
    w.finish()?;
    Ok(())
}

/// Regression test for the second review finding on PR #342:
/// after the per-run record loop, `level.push(run)` runs
/// unconditionally. Under `SkipAnyCorruptedRecords` every record
/// in a run can be `ChecksumMismatched` → run stays empty → the
/// unconditional push produces an empty run in Recovery.
/// Same downstream panic as the first finding: `from_recovery`'s
/// `Run::new(empty).expect()` aborts the tolerant path.
#[test]
fn recover_skip_any_drops_run_when_all_records_corrupt() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/skip_any/all_corrupt_run");
    fs.create_dir_all(folder)?;
    write_manifest_with_all_records_in_run_corrupt(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let recovery = recover(
        folder,
        &fs,
        ManifestRecoveryMode::SkipAnyCorruptedRecords,
        None,
    )?;

    // 2 levels persisted, both survive as slots.
    assert_eq!(recovery.table_ids.len(), 2);
    // Level 0's only run had its only record skipped → no
    // surviving runs (the empty-run placeholder must not be
    // pushed; the level slot itself stays as the structural
    // record that "the writer persisted a level here").
    for (run_idx, run) in recovery.table_ids[0].iter().enumerate() {
        assert!(
            !run.is_empty(),
            "level 0 run {run_idx} is empty in Recovery — \
             Version::from_recovery's Run::new(empty).expect() panics here",
        );
    }
    // Level 1 survived with its one good record.
    assert_eq!(recovery.table_ids[1].len(), 1);
    assert_eq!(recovery.table_ids[1][0][0].id, 200);
    Ok(())
}

#[test]
fn recover_pit_truncates_remaining_blob_records_on_corruption() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/pit/blob_mid_corrupt");
    fs.create_dir_all(folder)?;
    write_manifest_with_corrupt_blob_record(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let recovery = recover(folder, &fs, ManifestRecoveryMode::PointInTimeRecovery, None)?;
    let ids: Vec<u64> = recovery.blob_file_ids.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        ids,
        vec![10],
        "PIT must drop the corrupt blob record AND every blob record after it; \
         expected only id=10 (the good prefix), got {ids:?}",
    );
    Ok(())
}

/// Builds a manifest where level 0 declares `table_count = 3` but
/// only writes 1 good record + 1 corrupt record before truncating
/// (the writer was killed mid-record). Used to exercise the
/// `SkipAny` + `TailTruncation` accounting fix: previously
/// `tables_dropped_to_tail` was computed from `run.len()` alone
/// (= 1), which re-counted the already-skipped corrupt record as
/// a tail drop; the correct math subtracts BOTH
/// successfully-decoded AND skipped-corrupt records.
fn write_manifest_skip_any_then_tail_truncated(
    folder: &Path,
    id: u64,
    fs: &dyn Fs,
) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_tree_type(&mut w)?;

    w.start("tables")?;
    w.write_u8(1)?; // 1 level
    w.write_u8(1)?; // 1 run
    w.write_u32::<LittleEndian>(3)?; // declared 3 records...
    // ...but only 2 actually written (good + corrupt). The third
    // is implicitly truncated — reader hits UnexpectedEof at the
    // 3rd record's frame header.
    write_good_table_record(&mut w, 100)?;
    write_bad_table_record(&mut w, 101)?;

    write_empty_blob_files(&mut w)?;
    w.start("blob_gc_stats")?;
    w.write_u32::<LittleEndian>(0)?;
    w.finish()?;
    Ok(())
}

/// Regression test for review finding on PR #342:
/// `tables_dropped_to_tail` was being computed from `run.len()`
/// alone, but under `SkipAnyCorruptedRecords` previously-skipped
/// corrupt records are NOT in `run` — they were already counted
/// in `tables_dropped_to_corruption`. The pre-fix math would
/// double-count them at `TailTruncation`, reporting
/// `tail = table_count - run.len() = 3 - 1 = 2` when the correct
/// breakdown is `tail = 1, corruption = 1` (the corrupt record
/// goes to corruption, only the genuinely missing trailing
/// record goes to tail).
#[test]
fn recover_skip_any_then_tail_accounts_corruption_separately() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/skip_any/then_tail");
    fs.create_dir_all(folder)?;
    write_manifest_skip_any_then_tail_truncated(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let recovery = recover(
        folder,
        &fs,
        ManifestRecoveryMode::SkipAnyCorruptedRecords,
        None,
    )?;

    assert_eq!(
        recovery.stats.tables_dropped_to_corruption, 1,
        "the corrupt record must land in the corruption counter",
    );
    assert_eq!(
        recovery.stats.tables_dropped_to_tail, 1,
        "exactly one trailing record was truncated; the previously-skipped \
         corrupt record must NOT be re-counted here as tail (pre-fix value \
         would be 2)",
    );
    Ok(())
}

/// Companion fixture for the blob-side accounting regression
/// test below. Mirrors `write_manifest_skip_any_then_tail_truncated`
/// (a good record + a corrupt record + truncated tail) but in
/// the `blob_files` section so the read path's `SkipAny` +
/// `TailTruncation` arm fires on blob counters instead of table
/// counters.
fn write_manifest_blob_skip_any_then_tail_truncated(
    folder: &Path,
    id: u64,
    fs: &dyn Fs,
) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_tree_type(&mut w)?;

    w.start("tables")?;
    w.write_u8(0)?; // 0 levels — focus is on blob_files

    w.start("blob_files")?;
    w.write_u32::<LittleEndian>(3)?; // declared 3...
    // ...but only 2 written: good, bad. The third is implicitly
    // truncated (reader hits UnexpectedEof at the 3rd frame
    // header).
    crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
        payload.write_u64::<LittleEndian>(10)?;
        payload.write_u8(0)?;
        payload.write_u128::<LittleEndian>(0)?;
        Ok(())
    })?;
    // Corrupt second blob record (wrong xxh3, correct length).
    let mut payload: Vec<u8> = Vec::new();
    payload.write_u64::<LittleEndian>(11)?;
    payload.write_u8(0)?;
    payload.write_u128::<LittleEndian>(0)?;
    #[expect(
        clippy::cast_possible_truncation,
        reason = "payload is 25 bytes — fits in u32"
    )]
    let len = payload.len() as u32;
    w.write_u32::<LittleEndian>(len)?;
    w.write_u64::<LittleEndian>(0xDEAD_BEEF_DEAD_BEEF)?;
    w.write_all(&payload)?;

    w.start("blob_gc_stats")?;
    w.write_u32::<LittleEndian>(0)?;
    w.finish()?;
    Ok(())
}

/// Regression test for the blob-side counterpart of the
/// accounting fix from `fd44c376` / 52db0ccd. Manifest declares
/// 3 blob records: 1 good (id=10) + 1 corrupt (id=11) +
/// 1 truncated (never written to disk). Under
/// `SkipAnyCorruptedRecords` the corrupt record skip-arm
/// increments `blob_dropped_to_corruption` and `blob_corrupted`
/// to 1 each; the `TailTruncation` arm then attributes the
/// remaining 1 unread record to `blob_dropped_to_tail`. Without
/// the `processed = recovered + blob_corrupted` accounting the
/// tail value would be 2 (overcount).
#[test]
fn recover_skip_any_then_tail_accounts_blob_corruption_separately() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/skip_any/blob_then_tail");
    fs.create_dir_all(folder)?;
    write_manifest_blob_skip_any_then_tail_truncated(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    let recovery = recover(
        folder,
        &fs,
        ManifestRecoveryMode::SkipAnyCorruptedRecords,
        None,
    )?;

    assert_eq!(
        recovery.stats.blob_dropped_to_corruption, 1,
        "the corrupt blob record must land in the corruption counter",
    );
    assert_eq!(
        recovery.stats.blob_dropped_to_tail, 1,
        "exactly one trailing blob record was truncated; the previously-skipped \
         corrupt record must NOT be re-counted here as tail (pre-fix value \
         would be 2)",
    );
    Ok(())
}
