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
        (12, BlockType::DeleteBitmap),
        (13, BlockType::ColumnPageDirectory),
        (14, BlockType::ColumnPage),
        (15, BlockType::ColumnZones),
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
    // not a silent coercion to a known variant. 16 is the first
    // unused tag past the contiguous range.
    assert!(BlockType::try_from(16).is_err());
    assert!(BlockType::try_from(255).is_err());
}

#[test]
fn the_retired_single_block_columnar_tag_is_not_a_known_role() {
    // Tag 11 tagged a columnar row group stored as one block, the layout
    // pages replaced. It must read as UNKNOWN rather than decode to any role:
    // a block in that layout parsed as a page directory, say, would fail
    // somewhere inside the directory decode and point an operator at the
    // wrong problem. Unknown is the honest answer, and the table-level format
    // stamp is what turns it into "convert this store".
    assert!(BlockType::try_from(11).is_err());
}
