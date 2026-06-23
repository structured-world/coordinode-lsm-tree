use super::BlockType;

#[test]
fn block_type_wire_tags_roundtrip_all_variants() {
    // Every variant must survive a u8 -> BlockType -> u8 round-trip
    // on its locked, contiguous wire tag. Per-KV checking is a
    // transform flag (header block_flags), not a block role, so
    // there is no checked-twin variant here — a checked data block
    // is BlockType::Data with the KV_CHECKSUM_FOOTER bit set.
    for (tag, variant) in [
        (0u8, BlockType::Data),
        (1, BlockType::Index),
        (2, BlockType::Filter),
        (3, BlockType::Meta),
        (4, BlockType::RangeTombstone),
        (5, BlockType::Manifest),
        (6, BlockType::ManifestFooter),
        (7, BlockType::BlockLayout),
        (8, BlockType::Locator),
        (9, BlockType::SeqnoBounds),
        (10, BlockType::ZoneMap),
        (11, BlockType::Columnar),
        (12, BlockType::DeleteBitmap),
    ] {
        assert_eq!(
            u8::from(variant),
            tag,
            "{variant:?} must encode to wire tag {tag}"
        );
        assert_eq!(
            BlockType::try_from(tag).expect("known tag must decode"),
            variant,
            "wire tag {tag} must decode to {variant:?}"
        );
    }
}

#[test]
fn block_type_rejects_unknown_wire_tag() {
    // Forward-incompatibility guard: a tag this build doesn't know
    // (newer writer, older reader) must surface as a typed error,
    // not a silent coercion to a known variant. 13 is the first
    // unused tag past the contiguous range.
    assert!(BlockType::try_from(13).is_err());
    assert!(BlockType::try_from(255).is_err());
}
