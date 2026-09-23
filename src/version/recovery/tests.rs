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
        blob_restrictions: crate::HashMap::default(),
        retention_floor: 0,
        dicts: Vec::new(),
    }
}

/// An edit that raised the retention floor carries the new absolute value;
/// one that did not leaves the recovered floor untouched.
#[test]
fn apply_edit_overwrites_the_retention_floor_only_when_carried() {
    let mut rec = recovery_with(1, vec![]);
    rec.apply_edit(&VersionEdit {
        new_version_id: 2,
        retention_floor: Some(41),
        ..Default::default()
    })
    .expect("apply");
    assert_eq!(rec.retention_floor, 41);

    rec.apply_edit(&VersionEdit {
        new_version_id: 3,
        retention_floor: None,
        ..Default::default()
    })
    .expect("apply");
    assert_eq!(rec.retention_floor, 41, "an edit without a floor keeps it");

    rec.apply_edit(&VersionEdit {
        new_version_id: 4,
        retention_floor: Some(99),
        ..Default::default()
    })
    .expect("apply");
    assert_eq!(rec.retention_floor, 99);
}

/// The snapshot section is exactly one `u64`; a short or long one is a
/// corrupt floor and must abort rather than read as a lower boundary.
#[test]
fn parse_retention_floor_section_is_strict() {
    let mut bytes = Vec::new();
    bytes.write_u64::<LittleEndian>(77).expect("write");
    assert_eq!(parse_retention_floor_section(&bytes).expect("parse"), 77);
    assert!(parse_retention_floor_section(&bytes[..7]).is_err(), "short");
    bytes.push(0);
    assert!(
        parse_retention_floor_section(&bytes).is_err(),
        "trailing byte"
    );
}

/// The ids the section actually holds are read back in order.
#[test]
fn parse_dicts_section_reads_every_id() {
    let mut bytes = Vec::new();
    bytes.write_u32::<LittleEndian>(2).expect("write");
    bytes.write_u32::<LittleEndian>(7).expect("write");
    bytes.write_u32::<LittleEndian>(9).expect("write");
    assert_eq!(parse_dicts_section(&bytes).expect("parse"), vec![7, 9]);
}

/// A count larger than the section can hold must be refused BEFORE anything is
/// reserved for it. The ids are fixed-width, so the section's own length says
/// how many there can be; trusting the count instead lets a corrupt manifest
/// name four billion ids and turn a recovery into a multi-gigabyte allocation.
#[test]
fn parse_dicts_section_rejects_a_count_the_section_cannot_hold() {
    let mut bytes = Vec::new();
    bytes.write_u32::<LittleEndian>(u32::MAX).expect("write");
    bytes.write_u32::<LittleEndian>(7).expect("write");

    assert!(matches!(
        parse_dicts_section(&bytes),
        Err(crate::Error::InvalidHeader("dicts section")),
    ));
}

/// A count SHORT of the ids present is a corrupt section too: the strict tail
/// check catches it, so no id is silently dropped.
#[test]
fn parse_dicts_section_rejects_a_count_below_the_ids_present() {
    let mut bytes = Vec::new();
    bytes.write_u32::<LittleEndian>(1).expect("write");
    bytes.write_u32::<LittleEndian>(7).expect("write");
    bytes.write_u32::<LittleEndian>(9).expect("write");

    assert!(parse_dicts_section(&bytes).is_err());
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
fn apply_edit_advances_restrictions_per_slice() {
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

/// Blob frontiers replay exactly like table restrictions: each relocation
/// slice records the file's new (higher) first-live byte, and a later slice
/// overwrites the earlier one. Losing this would make a reopened tree hash a
/// reclaimed blob file whole and report it corrupt.
#[test]
fn apply_edit_advances_blob_restrictions_per_slice() {
    let mut rec = recovery_with(1, vec![vec![vec![rtable(1, 10)]]]);
    assert!(rec.blob_restrictions.is_empty(), "starts unreclaimed");

    rec.apply_edit(&VersionEdit {
        new_version_id: 2,
        blob_restrictions: vec![(9, 4_096)],
        ..Default::default()
    })
    .expect("apply");
    assert_eq!(rec.blob_restrictions.get(&9), Some(&4_096));

    rec.apply_edit(&VersionEdit {
        new_version_id: 3,
        blob_restrictions: vec![(9, 65_536)],
        ..Default::default()
    })
    .expect("apply");
    assert_eq!(
        rec.blob_restrictions.get(&9),
        Some(&65_536),
        "a later slice's higher frontier overwrites the earlier one",
    );
}

/// Every edit carries the FULL restriction set of its version (the encoder
/// derives it by iterating the version's tables / blob files), so replay must
/// REPLACE the maps, not merge into them: an entry absent from a later edit
/// was lifted. A merged-in stale blob frontier is not harmless — blob file ids
/// are reused (the id counter reseeds from the maximum live id), so a removed
/// restricted file's frontier would attach to an unrelated whole file added
/// later under the same id, making integrity checks hash only its suffix.
#[test]
fn apply_edit_clears_a_removed_blob_files_frontier() {
    let mut rec = recovery_with(1, vec![vec![vec![rtable(1, 10)]]]);

    rec.apply_edit(&VersionEdit {
        new_version_id: 2,
        added_blob_files: vec![AddedBlobFile { id: 9, checksum: 1 }],
        blob_restrictions: vec![(9, 4_096)],
        ..Default::default()
    })
    .expect("apply");
    assert_eq!(rec.blob_restrictions.get(&9), Some(&4_096));

    // The restricted file is removed; the edit's full frontier set is empty.
    rec.apply_edit(&VersionEdit {
        new_version_id: 3,
        removed_blob_file_ids: vec![9],
        ..Default::default()
    })
    .expect("apply");
    assert!(
        rec.blob_restrictions.is_empty(),
        "a removed blob file's frontier must not outlive it: {:?}",
        rec.blob_restrictions,
    );

    // Id 9 is reused by a NEW, unrestricted whole file: it must reopen with
    // frontier 0, not the removed file's stale suffix frontier.
    rec.apply_edit(&VersionEdit {
        new_version_id: 4,
        added_blob_files: vec![AddedBlobFile { id: 9, checksum: 2 }],
        ..Default::default()
    })
    .expect("apply");
    assert_eq!(
        rec.blob_restrictions.get(&9),
        None,
        "a reused id's whole replacement file must not inherit a stale frontier",
    );
}

/// The SST analogue: a table whose restriction was lifted (the punched
/// survivor was rewritten away) stops appearing in later edits' full sets, so
/// replay must drop its entry rather than carry it forever.
#[test]
fn apply_edit_drops_restrictions_absent_from_a_later_edit() {
    let mut rec = recovery_with(1, vec![vec![vec![rtable(1, 10)]]]);

    rec.apply_edit(&VersionEdit {
        new_version_id: 2,
        restrictions: vec![(1, crate::UserKey::from(&b"ccc"[..]))],
        ..Default::default()
    })
    .expect("apply");
    assert!(rec.restrictions.contains_key(&1));

    rec.apply_edit(&VersionEdit {
        new_version_id: 3,
        ..Default::default()
    })
    .expect("apply");
    assert!(
        rec.restrictions.is_empty(),
        "a lifted restriction must not survive replay: {:?}",
        rec.restrictions,
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

/// Append the sections every manifest opens with: `format_version` (the
/// current format) and `tree_type` (Standard = 0). Varying either byte is not
/// what these fixtures exercise; [`write_header_at`] writes another version.
fn write_header(w: &mut FixtureWriter) -> crate::Result<()> {
    write_header_at(w, crate::FormatVersion::V6.into())
}

fn write_header_at(w: &mut FixtureWriter, format_version: u8) -> crate::Result<()> {
    w.start("format_version")?;
    w.write_u8(format_version)?;
    w.start("tree_type")?;
    w.write_u8(0)?;
    Ok(())
}

/// Start the `tables` section with the number of levels its layout spans;
/// the records follow.
fn start_tables(w: &mut FixtureWriter, levels: u8) -> crate::Result<()> {
    w.start("tables")?;
    w.write_u8(levels)?;
    Ok(())
}

/// Append a `tables` section of one level and no records, for fixtures whose
/// subject is another section.
fn write_empty_tables(w: &mut FixtureWriter) -> crate::Result<()> {
    start_tables(w, 1)
}

/// The payload of a table record at `level` / `run`, as the writer lays it
/// out.
fn table_payload(level: u8, run: u32, id: u64, checksum_type: u8) -> crate::Result<Vec<u8>> {
    let mut payload = Vec::new();
    payload.write_u8(level)?;
    payload.write_u32::<LittleEndian>(run)?;
    payload.write_u64::<LittleEndian>(id)?;
    payload.write_u8(checksum_type)?;
    payload.write_u128::<LittleEndian>(0)?;
    payload.write_u64::<LittleEndian>(0)?;
    Ok(payload)
}

/// Appends the first `keep` bytes of a well-formed framed table record: a
/// section that ends inside a record.
fn write_torn_table_record(w: &mut FixtureWriter, keep: usize) -> crate::Result<()> {
    let mut framed = Vec::new();
    write_good_table_record(&mut framed, 0, 0, 999)?;
    let Some(head) = framed.get(..keep) else {
        panic!("a torn record keeps fewer bytes than a whole one");
    };
    w.write_all(head)?;
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

/// Write a version archive whose `tables` section holds the given records,
/// each `(level, run, id)`, under a `level_count` of `levels`.
fn write_tables(
    folder: &Path,
    id: u64,
    levels: u8,
    records: &[(u8, u32, u64)],
    fs: &dyn Fs,
) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_header(&mut w)?;
    start_tables(&mut w, levels)?;
    for &(level, run, table) in records {
        write_good_table_record(&mut w, level, run, table)?;
    }
    write_empty_blob_files(&mut w)?;
    write_empty_blob_gc_stats(&mut w)?;
    w.finish()?;
    Ok(())
}

/// Recovers a `tables` section of `records` under `AbsoluteConsistency`.
fn recover_tables(levels: u8, records: &[(u8, u32, u64)]) -> crate::Result<Recovery> {
    let fs = MemFs::new();
    let folder = Path::new("/tables");
    fs.create_dir_all(folder)?;
    write_tables(folder, 1, levels, records, &fs)?;
    write_current(folder, 1, &fs)?;
    recover(folder, &fs, ManifestRecoveryMode::AbsoluteConsistency, None)
}

/// Records group back into the levels and runs they name, in order, and a
/// level without records survives as an empty slot.
#[test]
fn recover_groups_table_records_by_the_place_they_name() -> crate::Result<()> {
    let recovery = recover_tables(3, &[(0, 0, 1), (0, 0, 2), (0, 1, 3), (2, 0, 4)])?;
    assert_eq!(
        recovery.table_ids,
        vec![
            vec![vec![rtable_zero(1), rtable_zero(2)], vec![rtable_zero(3)]],
            vec![],
            vec![vec![rtable_zero(4)]],
        ],
    );
    Ok(())
}

/// A table as the fixtures here write it: zero checksum and seqno.
fn rtable_zero(id: u64) -> RecoveredTable {
    RecoveredTable {
        id,
        checksum: Checksum::from_raw(0),
        global_seqno: 0,
    }
}

/// A record placed where no writer puts one has no correct reading: a
/// skipped run ordinal, a place behind the previous record, or a level
/// beyond the level count. Each is refused rather than regrouped.
#[test]
fn recover_rejects_a_table_record_out_of_place() {
    let out_of_place = |records: &[(u8, u32, u64)]| {
        matches!(
            recover_tables(2, records),
            Err(crate::Error::InvalidHeader("tables record out of place"))
        )
    };
    assert!(out_of_place(&[(0, 1, 1)]), "a first run numbered 1");
    assert!(
        out_of_place(&[(0, 0, 1), (0, 2, 2)]),
        "a skipped run ordinal"
    );
    assert!(
        out_of_place(&[(1, 0, 1), (0, 0, 2)]),
        "a level going backwards"
    );
    assert!(
        out_of_place(&[(0, 1, 1), (0, 0, 2)]),
        "a run going backwards"
    );
    assert!(out_of_place(&[(1, 1, 1)]), "a later level opening at run 1");
    assert!(out_of_place(&[(2, 0, 1)]), "a level beyond the level count");
}

/// Run ordinals are `u32`, so a level of more runs than any narrower width
/// counts places each record in its own run.
#[test]
fn place_table_accepts_run_ordinals_past_u16() -> crate::Result<()> {
    let mut levels = vec![Vec::new()];
    let mut previous = Some(TablePlace {
        level: 0,
        run: 69_999,
    });
    levels[0] = vec![vec![rtable_zero(1)]; 70_000];
    place_table(
        &mut levels,
        &mut previous,
        TablePlace {
            level: 0,
            run: 70_000,
        },
        rtable_zero(2),
    )?;
    assert_eq!(levels[0].len(), 70_001);
    assert_eq!(levels[0][70_000], vec![rtable_zero(2)]);
    Ok(())
}

/// Write a version sfa archive with a corrupt `blob_file_count` (`u32::MAX`).
///
/// All four sfa sections required by `recover()` are present — only the
/// `blob_files` section carries the corrupt payload.
fn write_corrupt_blob_count(folder: &Path, id: u64, fs: &dyn Fs) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_header(&mut w)?;
    write_empty_tables(&mut w)?;

    w.start("blob_files")?;
    w.write_u32::<LittleEndian>(u32::MAX)?; // corrupt

    w.start("blob_gc_stats")?;
    w.write_u32::<LittleEndian>(0)?;

    w.finish()?;
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
        matches!(err, crate::Error::InvalidHeader("blob_files section")),
        "expected the blob_files section refusal, got: {err:?}"
    );

    Ok(())
}

/// Recovers the archive at `folder` under each recovery mode and asserts every
/// one refuses it. The snapshot's sections are read strictly whatever the
/// mode: a section is one checksummed block, so a defect inside it is a
/// writer defect or a forgery, and opening past it would drop what the
/// manifest committed.
fn assert_refused_in_every_mode(
    folder: &Path,
    fs: &dyn Fs,
    refused: impl Fn(&crate::Error) -> bool,
) {
    for mode in [
        ManifestRecoveryMode::AbsoluteConsistency,
        ManifestRecoveryMode::TolerateCorruptedTailRecords,
    ] {
        match recover(folder, fs, mode, None) {
            Err(e) if refused(&e) => {}
            Err(e) => panic!("{mode:?}: wrong refusal: {e:?}"),
            Ok(_) => panic!("{mode:?}: a defective snapshot section must not recover"),
        }
    }
}

/// Writes a `vN` archive whose `tables` section holds `complete` whole
/// records in level 0, run 0, and then ends inside the next record (its
/// frame header and part of its payload).
fn write_truncated_tables_tail(
    folder: &Path,
    id: u64,
    complete: u64,
    fs: &dyn Fs,
) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_header(&mut w)?;
    start_tables(&mut w, 1)?;
    for entry_id in 0..complete {
        write_good_table_record(&mut w, 0, 0, entry_id)?;
    }
    write_torn_table_record(&mut w, 20)?;

    write_empty_blob_files(&mut w)?;
    write_empty_blob_gc_stats(&mut w)?;

    w.finish()?;
    Ok(())
}

/// A `tables` section that ends inside a record is refused in every mode: a
/// torn write cannot produce it (the section is one checksummed block), so
/// accepting the records before the cut would drop tables the writer did
/// commit.
#[test]
fn recover_rejects_a_truncated_tables_section_in_every_mode() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/truncated/tables");
    fs.create_dir_all(folder)?;

    write_truncated_tables_tail(folder, 1, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    assert_refused_in_every_mode(
        folder,
        &fs,
        |e| matches!(e, crate::Error::Io(io) if io.kind() == crate::io::ErrorKind::UnexpectedEof),
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
    write_header(&mut w)?;
    start_tables(&mut w, 1)?;
    // Framed record with a corrupt `checksum_type` byte in the
    // payload. The framing XXH3 still covers the payload, so
    // the record decodes cleanly at the framing layer; the
    // InvalidTag surfaces from `decode_table_entry_payload`.
    let corrupt = table_payload(0, 0, 0, 0xFF)?;
    crate::version::framing::write_framed_record(&mut w, &mut Vec::new(), |payload| {
        payload.extend_from_slice(&corrupt);
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

/// Writes a `vN` archive with one whole record in run #0 of level 0,
/// then the section ends two bytes into the next record's frame header.
fn write_truncated_at_second_run(folder: &Path, id: u64, fs: &dyn Fs) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_header(&mut w)?;
    start_tables(&mut w, 1)?;
    write_good_table_record(&mut w, 0, 0, 42)?;
    write_torn_table_record(&mut w, 2)?;
    write_empty_blob_files(&mut w)?;
    w.start("blob_gc_stats")?;
    w.write_u32::<LittleEndian>(0)?;
    w.finish()?;
    Ok(())
}

/// A cut inside a record's frame header, not only inside its payload, is
/// refused in every mode as well.
#[test]
fn recover_rejects_a_tables_section_cut_inside_a_frame_header() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/truncated/header");
    fs.create_dir_all(folder)?;

    write_truncated_at_second_run(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    assert_refused_in_every_mode(
        folder,
        &fs,
        |e| matches!(e, crate::Error::Io(io) if io.kind() == crate::io::ErrorKind::UnexpectedEof),
    );
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
    write_header(&mut w)?;
    write_empty_tables(&mut w)?;
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

/// The `blob_files` companion: a count the records do not fill is refused in
/// every mode.
#[test]
fn recover_rejects_a_truncated_blob_files_section_in_every_mode() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/truncated/blobs");
    fs.create_dir_all(folder)?;

    write_truncated_blob_tail(folder, 1, 5, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    assert_refused_in_every_mode(folder, &fs, |e| {
        matches!(e, crate::Error::InvalidHeader("blob_files section"))
    });
    Ok(())
}

/// Writes a `vN` archive whose `blob_gc_stats` section is empty, so
/// `FragmentationMap::decode_from` hits `UnexpectedEof` on the first byte.
fn write_truncated_blob_gc_stats(folder: &Path, id: u64, fs: &dyn Fs) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_header(&mut w)?;
    write_empty_tables(&mut w)?;
    w.start("blob_files")?;
    w.write_u32::<LittleEndian>(0)?;
    // blob_gc_stats section started but no payload written —
    // section.len() == 0, FragmentationMap::decode_from will
    // surface UnexpectedEof on its first read.
    w.start("blob_gc_stats")?;
    w.finish()?;
    Ok(())
}

/// A `blob_gc_stats` section the writer did not finish is refused in every
/// mode: sections are sealed one checksummed block at a time, so an empty
/// one is not the shape a power loss leaves.
#[test]
fn recover_rejects_a_truncated_blob_gc_stats_section_in_every_mode() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/truncated/gc_stats");
    fs.create_dir_all(folder)?;
    write_truncated_blob_gc_stats(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    assert_refused_in_every_mode(
        folder,
        &fs,
        |e| matches!(e, crate::Error::Io(io) if io.kind() == crate::io::ErrorKind::UnexpectedEof),
    );
    Ok(())
}

// ====================================================================
// Corrupt records inside snapshot sections
// ====================================================================
//
// The fixtures below write a complete framed manifest, then pick one
// specific framed record and emit it with a deliberately-wrong XXH3
// digest, so the reader meets a `FramedRecordOutcome::ChecksumMismatch`
// at a known position. No mode may open past it.

/// Writes one framed table record at `level` / `run` with a CORRECT XXH3
/// digest.
fn write_good_table_record<W: std::io::Write>(
    w: &mut W,
    level: u8,
    run: u32,
    id: u64,
) -> crate::Result<()> {
    let payload = table_payload(level, run, id, 0)?;
    crate::version::framing::write_framed_record(w, &mut Vec::new(), |out| {
        out.extend_from_slice(&payload);
        Ok(())
    })
}

/// Writes one framed table record but with an INTENTIONALLY WRONG
/// XXH3 digest in the framing header — emulates payload bit-rot
/// inside an otherwise structurally-valid record. The `len` field
/// of the header is correct (so the reader's `BadHeader` path does
/// NOT trigger), which means the `ChecksumMismatch` arm is the one
/// being exercised.
fn write_bad_table_record<W: std::io::Write>(
    w: &mut W,
    level: u8,
    run: u32,
    id: u64,
) -> crate::Result<()> {
    let payload = table_payload(level, run, id, 0)?;
    let len = u32::try_from(payload.len()).expect("a table record fits a u32 length");
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
fn write_manifest_with_mid_record_corruption(
    folder: &Path,
    id: u64,
    fs: &dyn Fs,
) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_header(&mut w)?;

    start_tables(&mut w, 2)?;
    // Level 0: 1 run, 3 records, middle one is corrupt.
    write_good_table_record(&mut w, 0, 0, 100)?;
    write_bad_table_record(&mut w, 0, 0, 101)?;
    write_good_table_record(&mut w, 0, 0, 102)?;
    // Level 1: 1 run, 2 records, both good.
    write_good_table_record(&mut w, 1, 0, 200)?;
    write_good_table_record(&mut w, 1, 0, 201)?;

    write_empty_blob_files(&mut w)?;
    w.start("blob_gc_stats")?;
    w.write_u32::<LittleEndian>(0)?;
    w.finish()?;
    Ok(())
}

/// A corrupt table record in the middle of the section fails the open in
/// every mode, naming the digests that disagreed. Opening past it would drop
/// a table the writer committed, and the open would then delete its file.
#[test]
fn recover_rejects_a_corrupt_table_record_in_every_mode() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/corrupt/table_record");
    fs.create_dir_all(folder)?;
    write_manifest_with_mid_record_corruption(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    assert_refused_in_every_mode(folder, &fs, |e| {
        matches!(
            e,
            crate::Error::ManifestFrameChecksumMismatch {
                section: "tables",
                ..
            }
        )
    });
    Ok(())
}

/// Builds a manifest where a `blob_files` record (not a table
/// record) carries the corrupt XXH3 digest.
fn write_manifest_with_corrupt_blob_record(
    folder: &Path,
    id: u64,
    fs: &dyn Fs,
) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, id, fs)?;
    write_header(&mut w)?;

    write_empty_tables(&mut w)?;

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

/// The `blob_files` companion: a corrupt blob-file record fails the open in
/// every mode.
#[test]
fn recover_rejects_a_corrupt_blob_file_record_in_every_mode() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/corrupt/blob_record");
    fs.create_dir_all(folder)?;
    write_manifest_with_corrupt_blob_record(folder, 1, &fs)?;
    write_current(folder, 1, &fs)?;

    assert_refused_in_every_mode(folder, &fs, |e| {
        matches!(
            e,
            crate::Error::ManifestFrameChecksumMismatch {
                section: "blob_files",
                ..
            }
        )
    });
    Ok(())
}

/// A manifest otherwise readable by this engine, labelled with another
/// format version. Every section after the label is in the current layout, so
/// only the label can refuse it.
fn write_manifest_labelled(folder: &Path, fs: &dyn Fs, format_version: u8) -> crate::Result<()> {
    let mut w = open_fixture_writer(folder, 1, fs)?;
    write_header_at(&mut w, format_version)?;
    write_empty_tables(&mut w)?;
    write_empty_blob_files(&mut w)?;
    write_empty_blob_gc_stats(&mut w)?;
    w.finish()?;
    write_current(folder, 1, fs)
}

/// Recovery reads the format version before any section: a manifest of
/// another format is refused as such, rather than parsed as the current one
/// and either misread or reported as damage.
#[test]
fn recover_refuses_a_manifest_of_another_format_version() -> crate::Result<()> {
    let fs = MemFs::new();
    let folder = Path::new("/format/v5");
    fs.create_dir_all(folder)?;
    write_manifest_labelled(folder, &fs, 5)?;

    let err = recover(folder, &fs, ManifestRecoveryMode::AbsoluteConsistency, None)
        .expect_err("a V5 manifest must be refused");
    assert!(
        matches!(err, crate::Error::InvalidVersion(5)),
        "expected InvalidVersion(5), got {err:?}",
    );
    Ok(())
}

/// Repair consults the committed manifest before rebuilding, and a manifest
/// of another format must stop it there. Read as "no manifest", the rebuild
/// would republish the old store in the current format and drop what only the
/// manifest held (restrictions, retention floor, blob GC state): a migration
/// that belongs to the offline converter, done silently and lossily.
#[test]
fn repair_refuses_a_store_of_another_format_version() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = crate::fs::StdFs;
    fs.create_dir_all(&dir.path().join(crate::file::TABLES_FOLDER))?;
    write_manifest_labelled(dir.path(), &fs, 5)?;

    let err = crate::Config::new(
        dir.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .repair()
    .expect_err("repair must not rebuild a V5 store");
    assert!(
        matches!(err, crate::Error::InvalidVersion(5)),
        "expected InvalidVersion(5), got {err:?}",
    );
    Ok(())
}
