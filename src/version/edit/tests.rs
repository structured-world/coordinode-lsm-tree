use super::*;
use crate::config::ManifestRecoveryMode;

fn sample() -> VersionEdit {
    VersionEdit {
        new_version_id: 42,
        changed_levels: vec![
            ChangedLevel {
                level: 0,
                // L0: two single-table runs (one per flush), overlapping.
                runs: vec![
                    vec![TableDesc {
                        id: 7,
                        checksum: 0x1122_3344_5566_7788_99AA_BBCC_DDEE_FF00,
                        global_seqno: 100,
                    }],
                    vec![TableDesc {
                        id: 8,
                        checksum: 1,
                        global_seqno: 101,
                    }],
                ],
            },
            ChangedLevel {
                level: 3,
                // Tiered level: one run holding two sorted tables.
                runs: vec![vec![
                    TableDesc {
                        id: 10,
                        checksum: 2,
                        global_seqno: 50,
                    },
                    TableDesc {
                        id: 11,
                        checksum: 3,
                        global_seqno: 51,
                    },
                ]],
            },
        ],
        added_blob_files: vec![AddedBlobFile {
            id: 9,
            checksum: 0xDEAD_BEEF,
        }],
        removed_blob_file_ids: vec![4],
        gc_stats: Some(vec![0xAB; 20]),
        // Two tight-space restrictions, so the framed round-trip exercises
        // the variable-length restriction codec (ids + length-prefixed keys).
        restrictions: vec![
            (7, UserKey::from(&b"mmm"[..])),
            (10, UserKey::from(&b"zzzz"[..])),
        ],
        // Two blob frontiers, so the round-trip also covers the fixed-width
        // blob-restriction codec (id + offset).
        blob_restrictions: vec![(3, 4_096), (4, 1_048_576)],
        // A raised retention floor, so the round-trip covers the trailing
        // appended section as well.
        retention_floor: Some(77),
        dicts: None,
    }
}

#[test]
fn retention_floor_roundtrips_with_and_without_blob_frontiers() {
    // With frontiers: blob section, then the floor.
    let with_frontiers = sample();
    let mut payload = Vec::new();
    with_frontiers.encode_payload(&mut payload).expect("encode");
    assert_eq!(
        VersionEdit::decode_payload(&payload).expect("decode"),
        with_frontiers
    );

    // Without frontiers: the encoder still writes the (empty) blob section so
    // the floor cannot be mistaken for a blob count.
    let mut floor_only = sample();
    floor_only.blob_restrictions = vec![];
    let mut payload = Vec::new();
    floor_only.encode_payload(&mut payload).expect("encode");
    let decoded = VersionEdit::decode_payload(&payload).expect("decode");
    assert!(decoded.blob_restrictions.is_empty());
    assert_eq!(decoded.retention_floor, Some(77));

    // Neither: the payload ends after the restrictions (the pre-floor shape).
    let mut plain = sample();
    plain.blob_restrictions = vec![];
    plain.retention_floor = None;
    let mut payload = Vec::new();
    plain.encode_payload(&mut payload).expect("encode");
    let decoded = VersionEdit::decode_payload(&payload).expect("decode");
    assert!(decoded.blob_restrictions.is_empty());
    assert_eq!(decoded.retention_floor, None);
}

#[test]
fn payload_with_frontiers_but_no_floor_decodes_floor_as_none() {
    // Edits written between the two appended sections end after the blob
    // frontiers; the floor must read as absent, not as a decode error.
    let mut edit = sample();
    edit.retention_floor = None;
    let mut payload = Vec::new();
    edit.encode_payload(&mut payload).expect("encode");
    let decoded = VersionEdit::decode_payload(&payload).expect("decode");
    assert_eq!(decoded.blob_restrictions, edit.blob_restrictions);
    assert_eq!(decoded.retention_floor, None);
}

#[test]
fn dictionary_ids_roundtrip_in_registration_order_independent_form() {
    let mut edit = sample();
    edit.dicts = Some(vec![3, 17, 4096]);
    let mut payload = Vec::new();
    edit.encode_payload(&mut payload).expect("encode");

    let decoded = VersionEdit::decode_payload(&payload).expect("decode");
    assert_eq!(decoded.dicts, Some(vec![3, 17, 4096]));
    assert_eq!(decoded.retention_floor, Some(77));
    assert_eq!(decoded, edit);
}

#[test]
fn an_edit_that_collected_every_dictionary_carries_the_empty_set() {
    // The distinction that makes the section wholesale rather than additive:
    // "no dictionaries any more" must be expressible, and it is NOT the same
    // as "unchanged".
    let mut edit = sample();
    edit.dicts = Some(vec![]);
    let mut payload = Vec::new();
    edit.encode_payload(&mut payload).expect("encode");

    let decoded = VersionEdit::decode_payload(&payload).expect("decode");
    assert_eq!(
        decoded.dicts,
        Some(vec![]),
        "an empty set is carried, not read back as unchanged",
    );
}

#[test]
fn a_payload_without_the_dictionary_section_decodes_it_as_unchanged() {
    // The pre-dictionary shape: an edit written before the section existed
    // ends after the floor and must decode as "set unchanged", not as an
    // empty set (which would drop every registration on replay).
    let mut edit = sample();
    edit.dicts = None;
    let mut payload = Vec::new();
    edit.encode_payload(&mut payload).expect("encode");

    let decoded = VersionEdit::decode_payload(&payload).expect("decode");
    assert_eq!(decoded.dicts, None);
}

#[test]
fn a_dictionary_section_without_a_floor_is_refused_by_the_encoder() {
    // The appended sections are positional with no presence markers, so the
    // dictionary ids are only findable after a floor. Encoding them without one
    // produces a record the decoder reads as "floor = <count><first id>", which
    // applies a fabricated retention floor and drops the whole set. Refuse to
    // write it rather than emit bytes that decode into something else.
    let mut edit = sample();
    edit.retention_floor = None;
    edit.dicts = Some(vec![7, 9]);

    let mut payload = Vec::new();
    assert!(edit.encode_payload(&mut payload).is_err());

    let mut buf = Vec::new();
    let mut scratch = Vec::new();
    assert!(edit.append_to(&mut buf, &mut scratch).is_err());
    assert!(
        buf.is_empty(),
        "no framed record is emitted for a refused edit"
    );
}

#[test]
fn a_truncated_dictionary_section_is_rejected() {
    // A count that outruns the payload is format drift, not power loss: the
    // framing checksum already passed. Reject rather than adopt a short set,
    // which would silently forget dictionaries the tables still reference.
    let mut edit = sample();
    edit.dicts = Some(vec![1, 2, 3]);
    let mut payload = Vec::new();
    edit.encode_payload(&mut payload).expect("encode");
    payload.truncate(payload.len() - 4);

    assert!(VersionEdit::decode_payload(&payload).is_err());
}

#[test]
fn framed_roundtrip_recovers_the_edit() {
    let edit = sample();
    let mut buf = Vec::new();
    let mut scratch = Vec::new();
    edit.append_to(&mut buf, &mut scratch).expect("append");

    let mut payload = Vec::new();
    let outcome =
        framing::read_framed_record(&mut &buf[..], u64::MAX, None, &mut payload).expect("read");
    assert!(
        matches!(outcome, framing::FramedRecordOutcome::Ok),
        "clean record must decode Ok, got {outcome:?}",
    );
    let decoded = VersionEdit::decode_payload(&payload).expect("decode");
    assert_eq!(decoded, edit);
}

#[test]
fn decode_rejects_a_truncated_restriction_key() {
    // The last section is the restriction codec; dropping the tail leaves the
    // final restriction's key shorter than its length prefix, which must be
    // rejected rather than silently un-clamping a punched table.
    let edit = sample(); // ends with restriction (10, "zzzz")
    let mut buf = Vec::new();
    edit.append_to(&mut buf, &mut Vec::new()).expect("append");
    let mut payload = Vec::new();
    framing::read_framed_record(&mut &buf[..], u64::MAX, None, &mut payload).expect("read");

    payload.truncate(payload.len() - 2); // chop 2 of the 4 key bytes
    assert!(
        VersionEdit::decode_payload(&payload).is_err(),
        "a restriction key shorter than its length prefix must be rejected",
    );
}

#[test]
fn empty_level_layout_roundtrips() {
    // A compaction that drains a level emits an empty-runs ChangedLevel.
    let mut edit = sample();
    edit.changed_levels.push(ChangedLevel {
        level: 2,
        runs: vec![],
    });
    let mut buf = Vec::new();
    edit.append_to(&mut buf, &mut Vec::new()).expect("append");
    let mut payload = Vec::new();
    framing::read_framed_record(&mut &buf[..], u64::MAX, None, &mut payload).expect("read");
    assert_eq!(VersionEdit::decode_payload(&payload).expect("decode"), edit);
}

#[test]
fn empty_gc_stats_roundtrips_as_none() {
    let mut edit = sample();
    edit.gc_stats = None;
    let mut buf = Vec::new();
    edit.append_to(&mut buf, &mut Vec::new()).expect("append");
    let mut payload = Vec::new();
    framing::read_framed_record(&mut &buf[..], u64::MAX, None, &mut payload).expect("read");
    assert_eq!(VersionEdit::decode_payload(&payload).expect("decode"), edit);
}

#[test]
fn payload_without_the_blob_frontier_section_decodes_as_empty() {
    // Edits written before blob frontiers existed end right after `restrictions`.
    // Reading such a payload must yield an empty blob-frontier list, not an
    // InvalidHeader — otherwise every pre-existing manifest becomes unopenable.
    let mut edit = sample();
    edit.blob_restrictions = vec![];
    edit.retention_floor = None;
    let mut payload = Vec::new();
    edit.encode_payload(&mut payload).expect("encode");
    let decoded = VersionEdit::decode_payload(&payload).expect("decode");
    assert!(decoded.blob_restrictions.is_empty());

    // ...and an edit that carries frontiers still round-trips them.
    let mut with_frontiers = sample();
    with_frontiers.blob_restrictions = vec![(7, 4096), (9, 1 << 20)];
    let mut payload = Vec::new();
    with_frontiers
        .encode_payload(&mut payload)
        .expect("encode with frontiers");
    assert_eq!(
        VersionEdit::decode_payload(&payload)
            .expect("decode with frontiers")
            .blob_restrictions,
        vec![(7, 4096), (9, 1 << 20)],
    );
}

#[test]
fn truncated_trailing_record_is_detected() {
    // A power-loss-truncated edit must NOT decode as Ok — replay stops here.
    let edit = sample();
    let mut buf = Vec::new();
    edit.append_to(&mut buf, &mut Vec::new()).expect("append");
    buf.truncate(buf.len() - 5); // chop the tail
    let mut payload = Vec::new();
    let outcome = framing::read_framed_record(&mut &buf[..], u64::MAX, None, &mut payload)
        .expect("read does not error on truncation");
    assert!(
        !matches!(outcome, framing::FramedRecordOutcome::Ok),
        "truncated record must not be Ok, got {outcome:?}",
    );
}

#[test]
fn bitflip_in_payload_fails_checksum() {
    let edit = sample();
    let mut buf = Vec::new();
    edit.append_to(&mut buf, &mut Vec::new()).expect("append");
    // Flip a byte in the payload region (past the 12-byte framing header).
    let last = buf.len() - 1;
    buf[last] ^= 0xFF;
    let mut payload = Vec::new();
    let outcome =
        framing::read_framed_record(&mut &buf[..], u64::MAX, None, &mut payload).expect("read");
    assert!(
        matches!(
            outcome,
            framing::FramedRecordOutcome::ChecksumMismatch { .. }
        ),
        "bit-flip must surface as ChecksumMismatch, got {outcome:?}",
    );
}

#[test]
fn replay_recovers_all_durable_edits_in_order() {
    let mut log = Vec::new();
    let mut scratch = Vec::new();
    let edits: Vec<VersionEdit> = (0..5)
        .map(|i| {
            let mut e = sample();
            e.new_version_id = 100 + i;
            e
        })
        .collect();
    for e in &edits {
        e.append_to(&mut log, &mut scratch).expect("append");
    }
    let replayed =
        replay_edits(&mut &log[..], ManifestRecoveryMode::AbsoluteConsistency).expect("replay");
    assert_eq!(replayed, edits, "replay must recover every edit in order");
}

#[test]
fn replay_stops_at_torn_tail_keeping_clean_prefix() {
    // Two clean edits + a truncated third: replay keeps the first two.
    let mut log = Vec::new();
    let mut scratch = Vec::new();
    let mut e0 = sample();
    e0.new_version_id = 1;
    let mut e1 = sample();
    e1.new_version_id = 2;
    e0.append_to(&mut log, &mut scratch).expect("append e0");
    e1.append_to(&mut log, &mut scratch).expect("append e1");
    let clean_len = log.len();
    // Append a third edit, then chop its tail (simulated power loss).
    let mut e2 = sample();
    e2.new_version_id = 3;
    e2.append_to(&mut log, &mut scratch).expect("append e2");
    log.truncate(clean_len + 6); // partial third record

    // A writer-incomplete (truncated) tail is rolled back under every mode
    // except AbsoluteConsistency; TolerateCorruptedTailRecords is the mode
    // dedicated to exactly this salvage.
    let replayed = replay_edits(
        &mut &log[..],
        ManifestRecoveryMode::TolerateCorruptedTailRecords,
    )
    .expect("replay");
    assert_eq!(replayed, vec![e0, e1], "torn tail dropped, prefix kept");
}

#[test]
fn replay_stops_at_bitflipped_record_under_corruption_tolerant_mode() {
    // A bit-flip in the second record is corruption of committed bytes (a
    // fully-framed record with a bad checksum), not a writer-incomplete tail.
    // Only the corruption-tolerant modes (PIT / SkipAny) roll it back.
    let mut log = Vec::new();
    let mut scratch = Vec::new();
    let mut e0 = sample();
    e0.new_version_id = 1;
    let mut e1 = sample();
    e1.new_version_id = 2;
    e0.append_to(&mut log, &mut scratch).expect("append e0");
    let after_e0 = log.len();
    e1.append_to(&mut log, &mut scratch).expect("append e1");
    // Corrupt a payload byte of the second record (past its framing header).
    let target = after_e0 + framing::FRAME_HEADER_LEN + 2;
    log[target] ^= 0xFF;

    let replayed =
        replay_edits(&mut &log[..], ManifestRecoveryMode::PointInTimeRecovery).expect("replay");
    assert_eq!(replayed, vec![e0], "PIT drops the corrupted record");
}

#[test]
fn bitflipped_tail_aborts_under_tolerate_corrupted_tail() {
    // The mode distinction the bool collapsed: TolerateCorruptedTailRecords
    // salvages writer-incomplete tails ONLY, so a fully-framed but bit-rotted
    // trailing record (corruption of committed bytes) must abort, not roll
    // back. PIT / SkipAny roll it back (covered above); this guards the
    // boundary so the two policies never merge again.
    let mut log = Vec::new();
    let mut scratch = Vec::new();
    let mut e0 = sample();
    e0.new_version_id = 1;
    let mut e1 = sample();
    e1.new_version_id = 2;
    e0.append_to(&mut log, &mut scratch).expect("append e0");
    let after_e0 = log.len();
    e1.append_to(&mut log, &mut scratch).expect("append e1");
    let target = after_e0 + framing::FRAME_HEADER_LEN + 2;
    log[target] ^= 0xFF;

    let err = replay_edits(
        &mut &log[..],
        ManifestRecoveryMode::TolerateCorruptedTailRecords,
    )
    .expect_err("tolerate-tail must reject committed bit-rot");
    assert!(
        matches!(
            err,
            crate::Error::TornManifestEditLog {
                kind: "checksum-mismatch"
            }
        ),
        "expected TornManifestEditLog(checksum-mismatch), got {err:?}",
    );
}

#[test]
fn replay_of_empty_log_is_empty() {
    let replayed =
        replay_edits(&mut &[][..], ManifestRecoveryMode::AbsoluteConsistency).expect("replay");
    assert!(replayed.is_empty(), "empty log → no edits");
}

#[test]
fn decode_rejects_unknown_table_checksum_type() {
    // Flip the table record's checksum_type byte to a non-XXH3 value: the
    // framing checksum still matches (we corrupt the decoded payload, not
    // the wire), so only the in-decode validation can catch it.
    let edit = sample();
    let mut payload = Vec::new();
    edit.encode_payload(&mut payload).expect("encode");
    // Layout: new_version_id(8) | changed_level_count(4) | level(1) |
    // run_count(4) | table_count(4) | id(8) | checksum_type(1) | ...
    let cs_type_off = 8 + 4 + 1 + 4 + 4 + 8;
    payload[cs_type_off] = 0xEE; // not CHECKSUM_TYPE_XXH3
    assert!(
        matches!(
            VersionEdit::decode_payload(&payload),
            Err(crate::Error::InvalidHeader("VersionEdit"))
        ),
        "an unknown table checksum_type tag must be rejected",
    );
}

#[test]
fn decode_rejects_trailing_garbage() {
    // A well-formed edit followed by extra bytes is a malformed record.
    let edit = sample();
    let mut payload = Vec::new();
    edit.encode_payload(&mut payload).expect("encode");
    payload.extend_from_slice(&[0xAB, 0xCD]);
    assert!(
        matches!(
            VersionEdit::decode_payload(&payload),
            Err(crate::Error::InvalidHeader("VersionEdit"))
        ),
        "trailing bytes after a complete edit must be rejected",
    );
}

#[test]
fn decode_rejects_truncated_payload() {
    let edit = sample();
    let mut payload = Vec::new();
    edit.encode_payload(&mut payload).expect("encode");
    payload.truncate(payload.len() / 2);
    assert!(
        matches!(
            VersionEdit::decode_payload(&payload),
            Err(crate::Error::InvalidHeader("VersionEdit"))
        ),
        "a truncated payload must surface InvalidHeader",
    );
}
