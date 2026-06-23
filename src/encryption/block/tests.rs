use super::*;
use crate::encryption::key_chain::StaticKeyChain;
use crate::table::block::BlockType;

const TEST_KEY: [u8; 32] = [0x42; 32];
const TEST_KEY_OTHER: [u8; 32] = [0x55; 32];

fn id() -> BlockIdentity {
    BlockIdentity {
        table_id: 0x1234_5678_9ABC_DEF0,
        block_type: BlockType::Data,
        dict_id: 0,
        window_log: 0,
    }
}

fn ctx() -> EncryptionContext {
    EncryptionContext::v1(0, SuiteId::Aes256Gcm, 0, 0)
}

fn chain() -> StaticKeyChain {
    StaticKeyChain::new().with_key(0, TEST_KEY)
}

#[test]
fn parse_metadata_is_key_free_and_matches_seal() {
    // Forensic parse needs no key: seal a block, then read its
    // MetadataPayload structurally with `parse_encrypted_block_metadata`
    // (no KeyChain) and confirm the fields mirror what was sealed.
    let plaintext = b"forensic payload bytes";
    let sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();

    let meta = parse_encrypted_block_metadata(&sealed).unwrap();
    assert_eq!(meta.format_version, 1);
    assert_eq!(meta.key_epoch, 0);
    assert_eq!(meta.suite_id, SuiteId::Aes256Gcm);
    assert_eq!(meta.block_type, u8::from(BlockType::Data));
    assert_eq!(meta.compression_type, 0);
    assert_eq!(meta.dict_id, 0);
    assert_eq!(meta.window_log, 0);
    assert!(meta.ciphertext_len > 0, "body must carry ciphertext");

    // Suite is reflected for ChaCha too (proves it reads the on-disk
    // SuiteID byte, not a hard-coded default).
    let chacha_ctx = EncryptionContext::v1(0, SuiteId::ChaCha20Poly1305, 0, 0);
    let sealed_cc = encrypt_block(plaintext, &id(), &chacha_ctx, &chain()).unwrap();
    let meta_cc = parse_encrypted_block_metadata(&sealed_cc).unwrap();
    assert_eq!(meta_cc.suite_id, SuiteId::ChaCha20Poly1305);

    // Garbage / truncated input is a typed error, never a panic.
    assert!(parse_encrypted_block_metadata(b"not a frame").is_err());
    assert!(parse_encrypted_block_metadata(&sealed[..10]).is_err());
}

#[test]
fn parse_metadata_rejects_truncated_body() {
    // Regression: the forensic parser reads only the BodyFrame header (for
    // ciphertext_len) without the payload. A block cut off right after that
    // header — full MetadataFrame + BodyFrame header, but zero ciphertext
    // bytes — must still be rejected, not reported as structurally valid
    // with a ciphertext_len for bytes that aren't there.
    let sealed = encrypt_block(b"forensic payload bytes", &id(), &ctx(), &chain()).unwrap();
    // MetadataFrame = 8-byte SFA header + 39-byte payload; BodyFrame header
    // = 8 bytes. Cut to exactly that boundary: header present, body absent.
    let cut = 8 + METADATA_PAYLOAD_LEN_V1 as usize + 8;
    assert!(
        sealed.len() > cut,
        "test setup: sealed block must extend past the body header",
    );
    #[expect(
        clippy::expect_used,
        reason = "test asserts the truncated frame is rejected"
    )]
    let err = parse_encrypted_block_metadata(&sealed[..cut])
        .expect_err("truncated body must be rejected");
    assert!(
        matches!(err, DecryptError::MalformedBodyFrame(_)),
        "expected MalformedBodyFrame for a truncated body, got {err:?}",
    );
}

#[test]
fn reconstruct_aad_matches_seal_with_correct_table_id() {
    // The AAD is never on disk; offline AEAD verification needs it rebuilt.
    // Reconstructing with the SAME table_id the block was sealed under must
    // yield byte-for-byte the AAD encrypt_block used.
    let sealed = encrypt_block(b"forensic payload bytes", &id(), &ctx(), &chain()).unwrap();
    let expected = build(&ctx(), &id());
    let got = reconstruct_block_aad(&sealed, id().table_id).unwrap();
    assert_eq!(got.len(), AAD_LEN);
    assert_eq!(
        got, expected,
        "reconstructed AAD must match the sealing AAD"
    );

    // A different table_id binds to a different AAD (table_id IS in the AAD).
    let other = reconstruct_block_aad(&sealed, id().table_id ^ 1).unwrap();
    assert_ne!(got, other, "table_id must affect the reconstructed AAD");

    // Malformed input is a typed error, not a panic.
    assert!(reconstruct_block_aad(b"not a frame", 0).is_err());
}

#[test]
fn roundtrip_aes_recovers_plaintext() {
    let plaintext = b"the quick brown fox";
    let sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
    let recovered = decrypt_block(&sealed, &id(), &chain()).unwrap();
    assert_eq!(&recovered.plaintext[..], plaintext);
    // Codec context echoes back from the MetadataPayload — the
    // caller is expected to thread these through structured-zstd's
    // FrameDecoder::expect_dict_id / expect_window_log
    // setters when feeding the plaintext into a zstd decode.
    assert_eq!(recovered.compression_type, 0);
    assert_eq!(recovered.dict_id, 0);
    assert_eq!(recovered.window_log, 0);
}

#[test]
fn roundtrip_chacha_recovers_plaintext() {
    let plaintext = b"the quick brown fox";
    let chacha_ctx = EncryptionContext::v1(0, SuiteId::ChaCha20Poly1305, 0, 0);
    let sealed = encrypt_block(plaintext, &id(), &chacha_ctx, &chain()).unwrap();
    let recovered = decrypt_block(&sealed, &id(), &chain()).unwrap();
    assert_eq!(&recovered.plaintext[..], plaintext);
}

#[test]
fn wrong_key_in_chain_surfaces_aead_failure() {
    let plaintext = b"the quick brown fox";
    let sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
    // Reader's chain has a DIFFERENT 32-byte key under the same epoch.
    let wrong = StaticKeyChain::new().with_key(0, TEST_KEY_OTHER);
    let err = decrypt_block(&sealed, &id(), &wrong).unwrap_err();
    assert!(
        matches!(err, DecryptError::AeadVerificationFailed),
        "expected AeadVerificationFailed, got {err:?}",
    );
}

#[test]
fn missing_key_epoch_surfaces_unknown_key_epoch() {
    let plaintext = b"the quick brown fox";
    // Writer uses epoch 0; reader's chain has epoch 1 only.
    let sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
    let no_epoch_zero = StaticKeyChain::new().with_key(1, TEST_KEY);
    let err = decrypt_block(&sealed, &id(), &no_epoch_zero).unwrap_err();
    assert!(
        matches!(err, DecryptError::UnknownKeyEpoch { key_epoch: 0 }),
        "expected UnknownKeyEpoch {{ key_epoch: 0 }}, got {err:?}",
    );
}

#[test]
fn cross_identity_substitution_surfaces_aead_failure() {
    // Same plaintext, sealed under one BlockIdentity. Reader
    // attempts to decrypt with a DIFFERENT BlockIdentity (table_id
    // flipped). AAD binds table_id (not tree_id — that is no longer
    // part of the identity), so the mismatch surfaces as AEAD failure.
    let plaintext = b"the quick brown fox";
    let sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
    let mut wrong_id = id();
    wrong_id.table_id ^= 0x1; // flip one bit of the table id
    let err = decrypt_block(&sealed, &wrong_id, &chain()).unwrap_err();
    assert!(matches!(err, DecryptError::AeadVerificationFailed));
}

#[test]
fn trailing_bytes_after_body_are_rejected() {
    // The encrypted-block format is exactly MetadataFrame ‖
    // BodyFrame; nothing follows. A well-formed block with extra
    // bytes appended (e.g. a stray skippable frame from a
    // retired extension, or junk) must be rejected, not silently
    // accepted by ignoring the tail.
    let plaintext = b"the quick brown fox";
    let mut sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
    // The clean block round-trips.
    assert!(decrypt_block(&sealed, &id(), &chain()).is_ok());
    // Append trailing bytes; decrypt must now reject.
    sealed.extend_from_slice(&[0xAB, 0xCD, 0xEF, 0x01]);
    let err = decrypt_block(&sealed, &id(), &chain()).unwrap_err();
    assert!(
        matches!(err, DecryptError::MalformedBodyFrame(_)),
        "expected MalformedBodyFrame for trailing bytes, got {err:?}",
    );
}

#[test]
fn truncated_input_surfaces_malformed_metadata() {
    let plaintext = b"the quick brown fox";
    let sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
    // Cut to just the first frame header (no payload).
    let truncated = &sealed[..6];
    let err = decrypt_block(truncated, &id(), &chain()).unwrap_err();
    assert!(matches!(err, DecryptError::MalformedMetadataFrame(_)));
}

#[test]
fn encrypt_block_rejects_empty_plaintext() {
    // Spec docs/aad-block-format.md §5.3 row "BodyFrame
    // PayloadLen": valid range [1, 256 MiB] for v1 suites.
    // encrypt_block enforces the >= 1 floor so an empty input
    // can't produce a sealed block the decoder would reject.
    let err = encrypt_block(&[], &id(), &ctx(), &chain()).unwrap_err();
    assert!(
        matches!(err, crate::Error::Encrypt(_)),
        "expected Error::Encrypt for empty plaintext, got {err:?}",
    );
}

#[test]
fn encrypt_block_rejects_unknown_block_flags_bit() {
    // Symmetric to the decrypt-side rejection: encrypt_block must refuse to
    // PRODUCE a block whose BlockFlags carry a bit outside the KNOWN mask,
    // so this version never seals something its own decrypt rejects as
    // forward-incompatible.
    let mut c = ctx();
    c.block_flags = 0x10; // reserved bit, outside KNOWN
    let err = encrypt_block(b"payload", &id(), &c, &chain()).unwrap_err();
    assert!(
        matches!(err, crate::Error::Encrypt(_)),
        "expected Error::Encrypt for unknown BlockFlags bit, got {err:?}",
    );
}

#[test]
fn invalid_window_log_surfaces_malformed_metadata() {
    // WindowLog spec: 0 (no enforcement) or 10..=31. Tamper a
    // sealed block to put a forbidden value (9) in the
    // WindowLog byte; the decoder must reject before any AEAD
    // work even though the AEAD tag is over the AAD that
    // includes window_log (so a subsequent tag-verify would
    // ALSO fail, but the structural check fires first).
    let plaintext = b"the quick brown fox";
    let mut sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
    // MetadataFrame layout: [4 magic][4 PayloadLen][1 HeaderByte]
    // [1 KeyEpoch][1 BlockType][1 SuiteID][1 CompressionType]
    // [4 DictID][1 WindowLog][...]. WindowLog is at offset 8 + 9
    // = 17 from the start of the sealed bytes.
    sealed[17] = 9; // invalid (< 10, not zero)
    let err = decrypt_block(&sealed, &id(), &chain()).unwrap_err();
    assert!(
        matches!(err, DecryptError::MalformedMetadataFrame(_)),
        "expected MalformedMetadataFrame for WindowLog=9, got {err:?}",
    );
}

#[test]
fn oversized_body_payload_len_rejected_before_alloc() {
    // Forge the BodyFrame's PayloadLen to advertise the maximum
    // legal u32 — a naive decoder would try to allocate ~4 GiB
    // before realising the underlying reader has no such data.
    // The upfront cap rejects before any allocation.
    let plaintext = b"the quick brown fox";
    let mut sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
    // MetadataFrame total size = 8 (framing) + METADATA_PAYLOAD_LEN_V1.
    // BodyFrame starts right after it; its PayloadLen is at frame offset 4.
    // Derive from the constant so this keeps hitting PayloadLen (not the
    // BodyFrame magic) if the payload length ever changes again.
    let metadata_frame_len = 8 + METADATA_PAYLOAD_LEN_V1 as usize;
    let body_payload_len_at = metadata_frame_len + 4;
    sealed[body_payload_len_at..body_payload_len_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    let err = decrypt_block(&sealed, &id(), &chain()).unwrap_err();
    assert!(
        matches!(err, DecryptError::MalformedBodyFrame(_)),
        "expected MalformedBodyFrame for oversized BodyFrame PayloadLen, got {err:?}",
    );
}

#[test]
fn unknown_block_flags_bit_rejected_before_aead() {
    // For an encrypted block the BlockFlags byte is the only transform
    // descriptor the reader can trust. A byte with a reserved bit set (a
    // forward-incompatible transform stack this build cannot process) must
    // be rejected structurally — before the AEAD runs — not authenticated
    // and then mis-processed.
    let plaintext = b"the quick brown fox";
    let mut sealed = encrypt_block(plaintext, &id(), &ctx(), &chain()).unwrap();
    // BlockFlags sits at MetadataFrame payload offset 10 → absolute offset
    // 8 (framing) + 10 = 18. Set a reserved bit (1<<4) outside KNOWN.
    const BLOCK_FLAGS_AT: usize = 8 + 10;
    sealed[BLOCK_FLAGS_AT] |= 0x10;
    let err = decrypt_block(&sealed, &id(), &chain()).unwrap_err();
    assert!(
        matches!(err, DecryptError::MalformedMetadataFrame(_)),
        "expected MalformedMetadataFrame for unknown BlockFlags bit, got {err:?}",
    );
}
