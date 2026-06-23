use super::*;

fn identity(table_id: u64, bt: BlockType) -> BlockIdentity {
    BlockIdentity {
        table_id,
        block_type: bt,
        dict_id: 0,
        window_log: 0,
    }
}

#[test]
fn aad_len_matches_spec() {
    // Hard-coded to catch any drift between `AAD_LEN` and the spec's
    // 23-byte total (block offset + tree id both unbound). The
    // encoder/decoder rely on this exact size.
    assert_eq!(AAD_LEN, 23);
}

#[test]
fn magic_bytes_are_little_endian_for_skippable_frame() {
    // RFC 8878 skippable frames carry their magic in little-endian
    // form; the spec's `0x184D2A50` therefore appears on disk as
    // `50 2A 4D 18` and must appear identically in the AAD so an
    // attacker cannot port the metadata into a future format with a
    // different magic and reuse the AEAD tag.
    assert_eq!(MAGIC_METADATA_LE, [0x50, 0x2A, 0x4D, 0x18]);
    assert_eq!(u32::from_le_bytes(MAGIC_METADATA_LE), 0x184D_2A50);
}

#[test]
fn header_byte_v1_packs_version_in_high_nibble() {
    // High nibble = 1, low nibble = 0. The decoder shifts right by 4
    // to extract the version, so 0x10 must equal version 1.
    assert_eq!(HEADER_BYTE_V1, 0x10);
    assert_eq!(HEADER_BYTE_V1 >> 4, FORMAT_VERSION_V1);
    assert_eq!(HEADER_BYTE_V1 & 0x0F, 0);
}

#[test]
fn suite_nonce_lengths_match_registry() {
    // Locks the §7 registry into code so a future suite addition
    // forces a deliberate update here.
    assert_eq!(SuiteId::Aes256Gcm.nonce_len(), 12);
    assert_eq!(SuiteId::ChaCha20Poly1305.nonce_len(), 12);
}

#[test]
fn suite_id_byte_round_trip() {
    for suite in [SuiteId::Aes256Gcm, SuiteId::ChaCha20Poly1305] {
        assert_eq!(SuiteId::try_from(suite.as_byte()), Ok(suite));
    }
}

#[test]
fn suite_id_rejects_unknown_byte() {
    // 0x00 / 0x01 / 0x04..=0xFF are reserved per the §7 registry.
    for byte in [0u8, 1, 4, 0x10, 0xFF] {
        assert_eq!(SuiteId::try_from(byte), Err(byte));
    }
}

#[test]
fn aad_layout_is_byte_exact_for_a_concrete_block() {
    // Synthesise a concrete AAD payload and check every offset
    // matches the spec. The values picked here are non-degenerate:
    // each field uses a recognisable bit pattern so a layout mistake
    // (e.g. swapping table_id and the codec context) is immediately
    // visible in the diff.
    let ctx = EncryptionContext::v1(
        0x55, // key_epoch
        SuiteId::ChaCha20Poly1305,
        3,    // CompressionType::Zstd
        0x05, // block_flags: KV_CHECKSUM_FOOTER | COMPRESSED (bits 0,2)
    );
    let identity = BlockIdentity {
        table_id: 0x1112_1314_1516_1718,
        block_type: BlockType::Index, // 1
        dict_id: 0xDEAD_BEEF,
        window_log: 21,
    };

    let aad = build(&ctx, &identity);

    // MagicMetadata (LE): 50 2A 4D 18
    assert_eq!(&aad[0..4], &[0x50, 0x2A, 0x4D, 0x18]);
    // HeaderByte: 0x10 (v1)
    assert_eq!(aad[4], 0x10);
    // KeyEpoch
    assert_eq!(aad[5], 0x55);
    // BlockType: Index = 1
    assert_eq!(aad[6], 1);
    // SuiteID: ChaCha20-Poly1305 = 0x03
    assert_eq!(aad[7], 0x03);
    // TableID (BE) — neither block offset nor tree id is bound, so the
    // table id sits right after the 8-byte preamble.
    assert_eq!(&aad[8..16], &0x1112_1314_1516_1718u64.to_be_bytes());
    // CompressionType
    assert_eq!(aad[16], 3);
    // DictID (BE)
    assert_eq!(&aad[17..21], &0xDEAD_BEEFu32.to_be_bytes());
    // WindowLog
    assert_eq!(aad[21], 21);
    // BlockFlags
    assert_eq!(aad[22], 0x05);
}

#[test]
fn aad_for_zero_identity_is_well_formed() {
    // The all-zero codec / block-type happy path produces a valid
    // 23-byte AAD; the cross-tree defence relies on per-tree key
    // isolation, not on AAD bytes. The fixture also pins the
    // AES-256-GCM byte at the SuiteID offset.
    let ctx = EncryptionContext::v1(0, SuiteId::Aes256Gcm, 0, 0);
    let id = identity(0, BlockType::Data);
    let aad = build(&ctx, &id);
    assert_eq!(aad.len(), AAD_LEN);
    assert_eq!(&aad[0..4], &MAGIC_METADATA_LE);
    assert_eq!(aad[4], HEADER_BYTE_V1);
    assert_eq!(aad[5], 0); // KeyEpoch
    assert_eq!(aad[6], 0); // BlockType::Data
    assert_eq!(aad[7], 0x02); // SuiteId::Aes256Gcm
    assert!(aad[8..].iter().all(|&b| b == 0));
}

#[test]
fn aad_changes_when_block_type_changes() {
    // Block-type-relabel defence: same identity except for
    // `block_type` must produce a different AAD, so an attacker
    // cannot relabel a Data block as an Index block to bypass
    // type-specific decode paths.
    let ctx = EncryptionContext::v1(1, SuiteId::Aes256Gcm, 0, 0);
    let a = build(&ctx, &identity(2, BlockType::Data));
    let b = build(&ctx, &identity(2, BlockType::Index));
    assert_ne!(a, b);
    // Only the block_type byte (offset 6) differs.
    assert_eq!(&a[..6], &b[..6]);
    assert_eq!(&a[7..], &b[7..]);
}

#[test]
fn aad_changes_when_block_flags_changes() {
    // Transform-relabel defence: same identity + codec, differing only
    // in `block_flags`, must produce a different AAD — so an attacker
    // cannot flip a transform bit (e.g. clear the per-KV footer bit)
    // and still pass AEAD verification.
    let a = build(
        &EncryptionContext::v1(1, SuiteId::Aes256Gcm, 0, 0),
        &identity(2, BlockType::Data),
    );
    let b = build(
        &EncryptionContext::v1(1, SuiteId::Aes256Gcm, 0, 0x01),
        &identity(2, BlockType::Data),
    );
    assert_ne!(a, b);
    // Only the block_flags byte (offset 22, the AAD's last byte) differs.
    assert_eq!(&a[..22], &b[..22]);
    assert_ne!(a[22], b[22]);
}
