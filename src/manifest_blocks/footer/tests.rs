use super::*;
use crate::manifest_blocks::FLAG_FOOTER_MIRROR_ENABLED;

fn sample_payload() -> FooterPayload {
    // Mirrors the actual section set the manifest writer emits
    // (smaller than the full 8-section list, enough to exercise
    // ordering + lookup). Concrete offsets are arbitrary —
    // encode/decode only cares about byte-roundtrip fidelity.
    FooterPayload::new(
        FLAG_FOOTER_MIRROR_ENABLED,
        vec![
            TocEntry {
                name: "format_version".to_string(),
                block_offset: 4096,
                block_size: 64,
                section_checksum: 0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF_u128,
            },
            TocEntry {
                name: "tables".to_string(),
                block_offset: 4160,
                block_size: 512,
                section_checksum: 0xCAFE_BABE_CAFE_BABE_CAFE_BABE_CAFE_BABE_u128,
            },
        ],
    )
}

#[test]
fn footer_payload_roundtrip_preserves_all_fields() {
    // The wire format is the contract between writer and reader
    // — every field that round-trips on write/read must survive
    // byte-identical. Locks: layout_version, flags, section
    // order, and per-entry (name, offset, size). Drop any of
    // these and reader will pick up garbage offsets and the
    // section-block reads will fail XXH3.
    let original = sample_payload();
    let mut buf = Vec::new();
    original.encode(&mut buf).expect("encode succeeds");

    let decoded = FooterPayload::decode(&buf[..]).expect("decode succeeds");
    assert_eq!(decoded, original);
}

#[test]
fn footer_payload_section_lookup_finds_by_name() {
    // Section lookup is how the reader resolves logical name
    // (e.g. "tables") to a concrete (offset, size) pair before
    // seeking. Ordering is preserved but lookup is by name —
    // this asserts the by-name path.
    let payload = sample_payload();
    let tables = payload.section("tables").expect("tables section exists");
    assert_eq!(tables.block_offset, 4160);
    assert_eq!(tables.block_size, 512);
    assert!(payload.section("nonexistent").is_none());
}

#[test]
fn footer_decode_rejects_unknown_layout_version() {
    // Forward-incompatible manifest written by a future binary:
    // older reader must refuse rather than parse-best-effort
    // (which would mis-locate sections at the byte level).
    let mut buf = Vec::new();
    buf.write_u8(2).unwrap(); // unknown layout version
    buf.write_u8(0).unwrap();
    buf.write_u16::<LittleEndian>(0).unwrap();
    let err = FooterPayload::decode(&buf[..]).expect_err("must reject");
    assert!(matches!(err, crate::Error::ManifestFooterInvalid(_)));
}

#[test]
fn footer_decode_rejects_empty_section_name() {
    // Empty name is structurally meaningless (lookup-by-name
    // would match any empty-keyed query, and the writer never
    // emits one) — defensive reject so a corrupted length field
    // surfaces as a parse error rather than poisoning the TOC.
    let mut buf = Vec::new();
    buf.write_u8(MANIFEST_LAYOUT_VERSION_V1).unwrap();
    buf.write_u8(0).unwrap();
    buf.write_u16::<LittleEndian>(1).unwrap();
    buf.write_u16::<LittleEndian>(0).unwrap(); // empty name
    buf.write_u64::<LittleEndian>(0).unwrap();
    buf.write_u32::<LittleEndian>(0).unwrap();
    let err = FooterPayload::decode(&buf[..]).expect_err("must reject");
    assert!(matches!(err, crate::Error::ManifestFooterInvalid(_)));
}

#[test]
fn footer_decode_rejects_duplicate_section_names() {
    // Duplicate names break the section() lookup contract (it
    // returns the first match — silent shadowing). Writer
    // never emits duplicates; reader rejects them defensively.
    let entries = vec![
        TocEntry {
            name: "tables".to_string(),
            block_offset: 4096,
            block_size: 64,
            section_checksum: 0,
        },
        TocEntry {
            name: "tables".to_string(), // duplicate
            block_offset: 4160,
            block_size: 64,
            section_checksum: 0,
        },
    ];
    let payload = FooterPayload::new(0, entries);
    let mut buf = Vec::new();
    // Bypass encode's no-duplicate validation by hand-encoding;
    // exercises the reader-side check (writer-side is locked by
    // the start_section path in writer.rs which uses a HashSet).
    buf.write_u8(payload.layout_version).unwrap();
    buf.write_u8(payload.flags).unwrap();
    buf.write_u16::<LittleEndian>(payload.sections.len() as u16)
        .unwrap();
    for e in &payload.sections {
        buf.write_u16::<LittleEndian>(e.name.len() as u16).unwrap();
        buf.write_all(e.name.as_bytes()).unwrap();
        buf.write_u64::<LittleEndian>(e.block_offset).unwrap();
        buf.write_u32::<LittleEndian>(e.block_size).unwrap();
        buf.write_u128::<LittleEndian>(e.section_checksum).unwrap();
    }
    let err = FooterPayload::decode(&buf[..]).expect_err("must reject");
    assert!(matches!(err, crate::Error::ManifestFooterInvalid(_)));
}

#[test]
fn footer_decode_rejects_oversized_name_length() {
    // Length larger than MAX_SECTION_NAME_BYTES is either a
    // bug (writer never emits one this big) or a forged
    // manifest. Either way the reader refuses to allocate the
    // buffer.
    let mut buf = Vec::new();
    buf.write_u8(MANIFEST_LAYOUT_VERSION_V1).unwrap();
    buf.write_u8(0).unwrap();
    buf.write_u16::<LittleEndian>(1).unwrap();
    #[expect(
        clippy::cast_possible_truncation,
        reason = "test crafts bad input deliberately"
    )]
    let oversized = (MAX_SECTION_NAME_BYTES + 1) as u16;
    buf.write_u16::<LittleEndian>(oversized).unwrap();
    let err = FooterPayload::decode(&buf[..]).expect_err("must reject");
    assert!(matches!(err, crate::Error::ManifestFooterInvalid(_)));
}

#[test]
fn footer_encode_rejects_empty_section_name() {
    // Symmetric to the decode-side check — writer refuses to
    // emit an unparseable manifest in the first place.
    let payload = FooterPayload::new(
        0,
        vec![TocEntry {
            name: String::new(),
            block_offset: 0,
            block_size: 0,
            section_checksum: 0,
        }],
    );
    let mut buf = Vec::new();
    let err = payload.encode(&mut buf).expect_err("must reject");
    assert!(matches!(err, crate::Error::ManifestFooterInvalid(_)));
}

#[test]
fn footer_encode_rejects_oversized_section_name() {
    // Symmetric write-side check.
    let oversized_name = "x".repeat(MAX_SECTION_NAME_BYTES + 1);
    let payload = FooterPayload::new(
        0,
        vec![TocEntry {
            name: oversized_name,
            block_offset: 0,
            block_size: 0,
            section_checksum: 0,
        }],
    );
    let mut buf = Vec::new();
    let err = payload.encode(&mut buf).expect_err("must reject");
    assert!(matches!(err, crate::Error::ManifestFooterInvalid(_)));
}
