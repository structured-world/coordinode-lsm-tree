// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::*;

fn a_report() -> RepairReport {
    RepairReport {
        recovered: 3,
        salvaged: 1,
        unreadable: 1,
        unreadable_files: vec![(PathBuf::from("/db/tables/7"), "bad footer".into())],
        excluded_files: vec![(PathBuf::from("/db/tables/4"), "covered by 9".into())],
        lost_coverage: vec![
            (
                PathBuf::from("/db/tables/7"),
                UserKey::from(&b"a"[..]),
                UserKey::from(&b"m"[..]),
                Some(42),
            ),
            (
                PathBuf::from("/db/tables/8"),
                UserKey::from(&b"n"[..]),
                UserKey::from(&b"z"[..]),
                None,
            ),
        ],
        unknowable_losses: vec![PathBuf::from("/db/tables/11")],
        blob_files_salvaged: vec![(PathBuf::from("/db/blobs/2"), "salvaged".into())],
        damaged_dictionaries: vec![(PathBuf::from("/db/dicts/5.damaged"), "moved".into())],
        method: crate::repair::REPAIR_METHOD,
        warnings: vec![
            crate::repair::WARN_ALL_AT_L0,
            crate::repair::WARN_BLOB_FRAGMENTATION,
        ],
        id: 0x1234_5678_9abc_def0_0fed_cba9_8765_4321,
        includes: vec![7, 9],
    }
}

/// An encoded report decodes as the report it was encoded from, field for
/// field, its id and the ids it takes in included.
#[test]
fn an_encoded_report_decodes_as_itself() {
    let report = a_report();
    let back = decode_owed(&encode(&report), Path::new("/db"));
    assert_eq!(format!("{back:?}"), format!("{report:?}"));
}

/// Bytes that no longer check out still say a repair lost something: the
/// loss is unscoped, so the whole history is replayed.
#[test]
fn a_damaged_report_leaves_the_loss_unscoped() {
    let mut bytes = encode(&a_report());
    if let Some(byte) = bytes.get_mut(12) {
        *byte ^= 0xFF;
    }
    let back = decode_owed(&bytes, Path::new("/db"));
    assert_eq!(back.wal_replay_scope(), crate::WalReplayScope::FullHistory);
    assert_eq!(back.unknowable_losses, vec![PathBuf::from("/db")]);
    assert_eq!(back.warnings, vec![crate::repair::WARN_REPORT_COPY_DAMAGED]);
}

/// `body` closed by its checksum, as an encoded report is: a decode that
/// refuses it then refuses its shape, not its checksum.
fn sealed(mut body: Vec<u8>) -> Vec<u8> {
    let checksum = crate::hash::hash64(&body);
    put_u64(&mut body, checksum);
    body
}

/// The encoding of `report` without its checksum.
fn body_of(report: &RepairReport) -> Vec<u8> {
    let mut encoded = encode(report);
    encoded.truncate(encoded.len() - CHECKSUM_LEN);
    encoded
}

/// An encoding whose checksum holds yet whose shape is not the one this
/// engine writes (another magic, a bound tag outside 0/1, a warning it does
/// not know, bytes after the last field) is refused rather than misread: each
/// would otherwise hand out a report that names other losses than the
/// repair's.
#[test]
fn a_checksummed_report_of_another_shape_is_refused() {
    let report = RepairReport {
        includes: Vec::new(),
        ..a_report()
    };
    assert!(
        decode(&sealed(body_of(&report))).is_some(),
        "self-check: the untouched body decodes"
    );

    let mut other_magic = b"LSMRPRT2".to_vec();
    other_magic.extend(body_of(&report).iter().skip(MAGIC.len()));
    assert!(decode(&sealed(other_magic)).is_none(), "another magic");

    let mut trailing = body_of(&report);
    trailing.push(0);
    assert!(
        decode(&sealed(trailing)).is_none(),
        "bytes after the last field"
    );

    // The warnings are followed by the 16-byte id and the 8-byte count of no
    // included ids: the byte before those is the last warning's index.
    let mut unknown_warning = body_of(&report);
    let last_warning = unknown_warning.len() - 16 - 8 - 1;
    let Some(last) = unknown_warning.get_mut(last_warning) else {
        panic!("self-check: the body holds the warnings");
    };
    *last = u8::MAX;
    assert!(
        decode(&sealed(unknown_warning)).is_none(),
        "an unknown warning"
    );

    // One loss and nothing else before it: its bound tag sits right after
    // the three counted fields and the loss's path and keys.
    let one_loss = RepairReport {
        recovered: 0,
        salvaged: 0,
        unreadable: 0,
        unreadable_files: Vec::new(),
        excluded_files: Vec::new(),
        lost_coverage: vec![(
            PathBuf::from("/p"),
            UserKey::from(&b"a"[..]),
            UserKey::from(&b"b"[..]),
            None,
        )],
        unknowable_losses: Vec::new(),
        blob_files_salvaged: Vec::new(),
        damaged_dictionaries: Vec::new(),
        method: crate::repair::REPAIR_METHOD,
        warnings: Vec::new(),
        id: 0,
        includes: Vec::new(),
    };
    let mut bad_tag = body_of(&one_loss);
    let tag_at = MAGIC.len() + 8 + 8 + 8 + 8 + 8 + (8 + 2) + (8 + 1) + (8 + 1);
    let Some(tag) = bad_tag.get_mut(tag_at) else {
        panic!("self-check: the body holds the bound tag");
    };
    assert_eq!(*tag, 0, "self-check: the bound tag");
    *tag = 2;
    assert!(
        decode(&sealed(bad_tag)).is_none(),
        "a bound tag outside 0/1"
    );
}

/// Two repairs never share an id: it digests what the report says, the
/// version it was rebuilt as and the time, so even an identical report of
/// another version gets another one.
#[test]
fn reports_of_two_versions_get_two_ids() {
    let report = a_report();
    assert_ne!(id_for(&report, 9, None), id_for(&report, 10, None));
}

/// A later repair of a salvaged replacement names the same path with a
/// narrower range: folding keeps the earlier, wider loss beside it, so the
/// replay still reaches every key and the higher bound, and keeps each note
/// of a blob file salvaged twice.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn folding_keeps_every_distinct_loss_under_one_path() {
    let narrower = (
        PathBuf::from("/db/tables/7"),
        UserKey::from(&b"c"[..]),
        UserKey::from(&b"d"[..]),
        Some(10),
    );
    let mut later = RepairReport {
        recovered: 1,
        salvaged: 1,
        unreadable: 0,
        unreadable_files: Vec::new(),
        excluded_files: Vec::new(),
        lost_coverage: vec![narrower.clone()],
        unknowable_losses: Vec::new(),
        blob_files_salvaged: vec![(PathBuf::from("/db/blobs/2"), "again".into())],
        damaged_dictionaries: Vec::new(),
        method: crate::repair::REPAIR_METHOD,
        warnings: Vec::new(),
        id: 1,
        includes: Vec::new(),
    };
    let earlier = a_report();
    let wider = earlier
        .lost_coverage
        .first()
        .cloned()
        .expect("the earlier report names /db/tables/7");
    crate::repair::fold_report(&mut later, earlier);
    assert!(later.lost_coverage.contains(&narrower));
    assert!(
        later.lost_coverage.contains(&wider),
        "the earlier, wider loss of the same path survives"
    );
    assert_eq!(
        later.blob_files_salvaged,
        vec![
            (PathBuf::from("/db/blobs/2"), "again".into()),
            (PathBuf::from("/db/blobs/2"), "salvaged".into()),
        ],
    );

    // The same entry twice is listed once.
    let mut twice = a_report();
    crate::repair::fold_report(&mut twice, a_report());
    assert_eq!(twice.lost_coverage, a_report().lost_coverage);
    assert_eq!(twice.unreadable, 1);
}

/// Folding an earlier report adds its losses once, keeps the counts of the
/// later run, keeps the later run's entries first, and records the earlier
/// report's id and every id it took in, each once: a caller that applied one
/// of them knows its losses are among these.
#[test]
fn folding_adds_the_earlier_losses_once() {
    let mut later = RepairReport {
        recovered: 5,
        salvaged: 0,
        unreadable: 0,
        unreadable_files: Vec::new(),
        excluded_files: Vec::new(),
        lost_coverage: Vec::new(),
        unknowable_losses: vec![PathBuf::from("/db/tables/11")],
        blob_files_salvaged: vec![(PathBuf::from("/db/blobs/3"), "later".into())],
        damaged_dictionaries: Vec::new(),
        method: crate::repair::REPAIR_METHOD,
        warnings: vec![crate::repair::WARN_ALL_AT_L0],
        id: 1,
        includes: vec![9],
    };
    let earlier = a_report();
    let earlier_id = earlier.id;
    crate::repair::fold_report(&mut later, earlier);
    assert_eq!(later.recovered, 5);
    assert_eq!(later.salvaged, 0);
    assert_eq!(later.unreadable, 1);
    assert_eq!(later.lost_coverage.len(), 2);
    assert_eq!(
        later.unknowable_losses,
        vec![PathBuf::from("/db/tables/11")],
        "a path named twice is listed once"
    );
    assert_eq!(
        later
            .blob_files_salvaged
            .iter()
            .map(|(p, _)| p.clone())
            .collect::<Vec<_>>(),
        vec![PathBuf::from("/db/blobs/3"), PathBuf::from("/db/blobs/2")],
    );
    assert_eq!(
        later.warnings,
        vec![
            crate::repair::WARN_ALL_AT_L0,
            crate::repair::WARN_BLOB_FRAGMENTATION
        ],
    );
    assert_eq!(later.id, 1, "the later report keeps its own id");
    assert_eq!(later.includes, vec![9, earlier_id, 7]);
}
