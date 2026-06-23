use super::*;
use crate::{
    fs::MemFs, manifest_blocks::writer::ManifestArchiveWriter, runtime_config::RuntimeConfig,
};
use std::sync::Arc;

fn fresh_fs() -> MemFs {
    let fs = MemFs::new();
    fs.create_dir_all(Path::new("/m")).unwrap();
    fs
}

#[test]
fn validate_block_header_fits_rejects_understated_exact_slot() {
    use crate::coding::Encode;
    use crate::table::block::{BlockType, Header};

    // A valid (checksummed) header declaring data_length = 4 with no
    // parity, handed a slot LARGER than the block's on-disk size (extra
    // trailing bytes). For an exact-fit context the understated header
    // must be rejected: otherwise `Block::from_reader` consumes only the
    // declared bytes and silently ignores the trailing remainder (e.g. a
    // stripped ECC_PARITY trailer), which `Block::from_file` rejects.
    let header = Header {
        data_length: 4,
        uncompressed_length: 4,
        ..Header::test_dummy(BlockType::ManifestFooter)
    };
    let mut buf = header.encode_into_vec();
    buf.extend_from_slice(&[0u8; 4]); // declared payload
    buf.extend_from_slice(&[0xAB, 0xCD]); // extra trailing bytes beyond the block

    // on_disk_size = header (34) + data_length (4) = 38; buf.len() = 40.
    assert!(
        validate_block_header_fits(&buf, HeaderContext::SectionExact).is_err(),
        "an understated header in an exact-fit section slot must be rejected",
    );
    assert!(
        validate_block_header_fits(&buf, HeaderContext::FooterExact).is_err(),
        "an understated header in an exact-fit footer slot must be rejected",
    );
    // The padded head-mirror context tolerates a smaller declared size
    // ONLY when the trailing bytes are genuine zero padding. Here the
    // trailing bytes are non-zero (0xAB, 0xCD), so even the padded context
    // must reject them — otherwise a forged smaller declared size could
    // hide non-zero remainder (e.g. a stripped ECC trailer) as "padding".
    assert!(
        validate_block_header_fits(&buf, HeaderContext::FooterPadded).is_err(),
        "padded slot must reject non-zero bytes past the declared block size",
    );

    // Same understated header but with genuine zero padding past the
    // declared block: the padded head-mirror context accepts it.
    let mut zero_padded = header.encode_into_vec();
    zero_padded.extend_from_slice(&[0u8; 4]); // declared payload
    zero_padded.extend_from_slice(&[0u8; 2]); // zero pad beyond the block
    assert!(
        validate_block_header_fits(&zero_padded, HeaderContext::FooterPadded).is_ok(),
        "padded slot accepts a smaller declared size with genuine zero padding",
    );
}

fn write_manifest(fs: &MemFs, path: &Path, runtime: RuntimeConfig, sections: &[(&str, &[u8])]) {
    let mut w = ManifestArchiveWriter::create(
        path,
        fs,
        Arc::new(runtime),
        None,
        crate::fs::SyncMode::Normal,
    )
    .unwrap();
    for (name, data) in sections {
        w.start(name).unwrap();
        use std::io::Write;
        w.write_all(data).unwrap();
    }
    w.finish().unwrap();
}

#[test]
fn reader_opens_clean_manifest_via_tail() {
    // Happy path: writer emits a valid manifest, reader picks
    // it up via the tail (no head fallback needed). Locks the
    // primary read path and verifies source reporting.
    let fs = fresh_fs();
    let path = Path::new("/m/clean");
    write_manifest(
        &fs,
        path,
        RuntimeConfig::default(),
        &[("format_version", &[5]), ("tree_type", &[0])],
    );

    let reader = ManifestArchiveReader::open(
        path,
        &fs,
        std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
        None,
    )
    .unwrap();
    assert_eq!(reader.source(), FooterSource::Tail);
    assert!(reader.section("format_version").is_some());
    assert!(reader.section("tree_type").is_some());
    assert!(reader.section("nonexistent").is_none());
}

#[test]
fn reader_reads_section_bytes_verbatim() {
    // Section roundtrip: bytes written via Write::write_all
    // come back byte-for-byte via read_section. Locks the
    // section payload contract — without this, manifest's
    // logical metadata (FormatVersion, TreeType, etc.) would
    // drift between write and read.
    let fs = fresh_fs();
    let path = Path::new("/m/roundtrip");
    write_manifest(
        &fs,
        path,
        RuntimeConfig::default(),
        &[
            ("format_version", &[5]),
            ("comparator_name", b"u64-big-endian"),
        ],
    );

    let mut reader = ManifestArchiveReader::open(
        path,
        &fs,
        std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
        None,
    )
    .unwrap();
    assert_eq!(reader.read_section("format_version").unwrap(), vec![5]);
    assert_eq!(
        reader.read_section("comparator_name").unwrap(),
        b"u64-big-endian".to_vec(),
    );
}

#[test]
fn reader_falls_back_to_head_mirror_when_tail_corrupt() {
    // Simulate tail corruption by clobbering the last bytes of
    // the file (which includes the trailing size hint AND the
    // tail footer Block bytes). The reader's tail path must
    // fail, then succeed via the head mirror. Locks the core
    // partial-write recovery guarantee.
    let fs = fresh_fs();
    let path = Path::new("/m/tail_corrupt");
    write_manifest(
        &fs,
        path,
        RuntimeConfig::default(), // manifest_footer_mirror = true (default)
        &[("format_version", &[5])],
    );

    // Corrupt the trailing bytes: overwrite the size hint with
    // garbage that points outside the file.
    let mut file = fs
        .open(path, &FsOpenOptions::new().write(true).read(true))
        .unwrap();
    let size = file.metadata().unwrap().len;
    file.seek(SeekFrom::Start(size - 4)).unwrap();
    use std::io::Write;
    file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let reader = ManifestArchiveReader::open(
        path,
        &fs,
        std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
        None,
    )
    .unwrap();
    assert_eq!(
        reader.source(),
        FooterSource::Head,
        "reader should have fallen back to the head mirror"
    );
    assert!(reader.section("format_version").is_some());
}

#[test]
fn reader_fails_when_tail_corrupt_and_no_mirror() {
    // With manifest_footer_mirror=false, the head reservation
    // is all zeros — corruption of the tail leaves no fallback,
    // so the reader must return ManifestFooterInvalid (not
    // crash, not return an inconsistent manifest).
    let fs = fresh_fs();
    let path = Path::new("/m/tail_corrupt_no_mirror");
    let runtime = RuntimeConfig {
        manifest_footer_mirror: false,
        ..RuntimeConfig::default()
    };
    write_manifest(&fs, path, runtime, &[("format_version", &[5])]);

    // Same tail corruption as above.
    let mut file = fs
        .open(path, &FsOpenOptions::new().write(true).read(true))
        .unwrap();
    let size = file.metadata().unwrap().len;
    file.seek(SeekFrom::Start(size - 4)).unwrap();
    use std::io::Write;
    file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let err = ManifestArchiveReader::open(
        path,
        &fs,
        std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
        None,
    )
    .expect_err("must reject");
    assert!(matches!(err, crate::Error::ManifestFooterInvalid(_)));
}

#[test]
fn reader_rejects_request_for_missing_section() {
    // read_section on an unknown name returns
    // ManifestSectionInvalid rather than Io or a panic. Locks
    // the contract that callers can probe sections defensively
    // and distinguish per-section errors from footer-load
    // failures.
    let fs = fresh_fs();
    let path = Path::new("/m/missing");
    write_manifest(&fs, path, RuntimeConfig::default(), &[("a", &[1])]);

    let mut reader = ManifestArchiveReader::open(
        path,
        &fs,
        std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
        None,
    )
    .unwrap();
    let err = reader
        .read_section("does_not_exist")
        .expect_err("missing section must error");
    assert!(matches!(err, crate::Error::ManifestSectionInvalid(_)));
}

#[test]
fn reader_isolates_corruption_to_one_section_other_sections_readable() {
    // Acceptance criterion (#297 corruption matrix): a bit-flip
    // inside one section Block must fail verification for THAT
    // section while leaving the other sections + footer readable.
    // The TOC lives in the (separate) footer Block; per-section
    // XXH3 isolates the failure radius to a single Block.
    let fs = fresh_fs();
    let path = Path::new("/m/isolated");
    write_manifest(
        &fs,
        path,
        RuntimeConfig::default(),
        &[
            ("a", &[1, 2, 3, 4]),
            ("b", &[5, 6, 7, 8]),
            ("c", &[9, 10, 11, 12]),
        ],
    );

    // Reader before corruption: open + look up TOC entry for "b"
    // so we know the on-disk offset to flip a byte at.
    let b_offset = {
        let reader =
            ManifestArchiveReader::open(path, &fs, Arc::new(RuntimeConfig::default()), None)
                .unwrap();
        let entry = reader.section("b").expect("b section is in TOC");
        entry.block_offset
    };

    // Flip the first byte of section b's payload region (right after
    // the Block header) so the bit lands in the XXH3-checksummed
    // payload, not the header or an adjacent section. Manifest section
    // blocks are `BlockType::Manifest`, which carries the block_flags
    // byte, so the header is `header_len(Manifest)` bytes.
    let payload_off =
        b_offset + Header::header_len(crate::table::block::BlockType::Manifest) as u64;
    {
        let mut file = fs
            .open(path, &FsOpenOptions::new().write(true).read(true))
            .unwrap();
        file.seek(SeekFrom::Start(payload_off)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        file.seek(SeekFrom::Start(payload_off)).unwrap();
        file.write_all(&[byte[0] ^ 0xFF]).unwrap();
        file.sync_all().unwrap();
    }

    // Reopen — footer + TOC still load (different Block), sections
    // a and c still verify, only b fails.
    let mut reader =
        ManifestArchiveReader::open(path, &fs, Arc::new(RuntimeConfig::default()), None).unwrap();
    let a_bytes = reader.read_section("a").unwrap();
    assert_eq!(a_bytes, vec![1, 2, 3, 4], "section a survives");
    let c_bytes = reader.read_section("c").unwrap();
    assert_eq!(c_bytes, vec![9, 10, 11, 12], "section c survives");
    let b_err = reader
        .read_section("b")
        .expect_err("section b decoded but should have failed XXH3");
    // Don't pin the exact error variant — different bit positions
    // surface as ChecksumMismatch, InvalidHeader, or Io depending
    // on what got flipped. The contract is: SOMETHING surfaces
    // and the other sections still work.
    log::debug!("section b corruption surfaced as: {b_err:?}");
}

#[test]
fn reader_rejects_files_smaller_than_head_reservation() {
    // A file shorter than HEAD_FOOTER_RESERVED_SIZE (4 KiB)
    // has no head mirror to fall back to, so neither recovery
    // path can run — `Unrecoverable` rather than
    // `ManifestFooterInvalid` so callers can route "file is
    // truncated / not a manifest" differently from "file is a
    // manifest but its structure is bad". Files between
    // `HEAD_FOOTER_RESERVED_SIZE` and `HEAD + HINT` are NOT
    // rejected here — they reach the head-mirror fallback per
    // `reader_recovers_from_head_when_tail_hint_missing`.
    let fs = fresh_fs();
    let path = Path::new("/m/too_small");
    let mut file = fs
        .open(path, &FsOpenOptions::new().write(true).create_new(true))
        .unwrap();
    use std::io::Write;
    file.write_all(&[0u8; 100]).unwrap(); // way under 4 KiB + 4 bytes
    file.sync_all().unwrap();
    drop(file);

    let err = ManifestArchiveReader::open(
        path,
        &fs,
        std::sync::Arc::new(crate::runtime_config::RuntimeConfig::default()),
        None,
    )
    .expect_err("must reject");
    assert!(matches!(err, crate::Error::Unrecoverable));
}

/// Future-regression guard: today the AEAD path in
/// `EncryptionProvider::{encrypt, decrypt}` consumes only
/// plaintext + nonce — AAD-binding to `BlockIdentity` (including
/// `block_offset`) exists in `encryption::aad::build` /
/// `encryption::block::{encrypt_block, decrypt_block}` but is
/// not yet wired into the `Block::write_into` path the manifest
/// writer uses. Because of that, byte-copying the tail footer
/// Block into the head reservation today works fine: same nonce
/// + same tag → head decrypts cleanly.
///
/// This test asserts the head-mirror fallback round-trips with
/// encryption on, locking that behaviour today. The day
/// `Block::write_into` consumes the AAD-bound path (tracked
/// separately) the head-mirror byte-copy must change to a fresh
/// `Block::write_into` call at `block_offset = 0`, otherwise
/// the head-slot decrypt will fail against AAD bound to the
/// tail offset — and THIS TEST will be the one that catches it.
/// See the parallel writer comment in `manifest_blocks::writer`
/// for the exact code site to update.
#[cfg(feature = "encryption")]
#[test]
fn reader_falls_back_to_head_mirror_for_encrypted_manifest() {
    use crate::encryption::{Aes256GcmProvider, EncryptionProvider};

    let fs = fresh_fs();
    let path = Path::new("/m/enc_tail_corrupt");
    let key = [42u8; 32];
    let enc: Arc<dyn EncryptionProvider> = Arc::new(Aes256GcmProvider::new(&key));

    let mut w = ManifestArchiveWriter::create(
        path,
        &fs,
        Arc::new(RuntimeConfig::default()),
        Some(Arc::clone(&enc)),
        crate::fs::SyncMode::Normal,
    )
    .unwrap();
    w.start("format_version").unwrap();
    use std::io::Write;
    w.write_all(&[5u8]).unwrap();
    w.finish().unwrap();

    // Same tail-corruption pattern as the plaintext head-mirror
    // test: clobber the trailing size hint so the tail path
    // fails and the reader has no choice but to fall back to
    // the head mirror.
    let mut file = fs
        .open(path, &FsOpenOptions::new().write(true).read(true))
        .unwrap();
    let size = file.metadata().unwrap().len;
    file.seek(SeekFrom::Start(size - 4)).unwrap();
    file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let reader =
        ManifestArchiveReader::open(path, &fs, Arc::new(RuntimeConfig::default()), Some(enc))
            .expect("encrypted head-mirror fallback must decrypt cleanly");
    assert_eq!(
        reader.source(),
        FooterSource::Head,
        "reader should have fallen back to the head mirror"
    );
    assert!(reader.section("format_version").is_some());
}

/// Regression: a manifest truncated to exactly
/// `HEAD_FOOTER_RESERVED_SIZE` (4 KiB) has no tail footer Block
/// and no trailing size hint, but the head mirror at offset 0
/// is fully populated by the writer when
/// `manifest_footer_mirror` is on. The reader should reach the
/// head fallback in that state and succeed; rejecting with
/// `Unrecoverable` at the front gate (because file is too short
/// for a tail hint) defeats the advertised partial-write
/// recovery contract. Only files shorter than the head
/// reservation itself should bypass the head fallback.
#[test]
fn reader_recovers_from_head_when_tail_hint_missing() {
    let fs = fresh_fs();
    let path = Path::new("/m/head_only");
    write_manifest(
        &fs,
        path,
        RuntimeConfig::default(),
        &[("format_version", &[5])],
    );

    // Truncate the file back to exactly HEAD_FOOTER_RESERVED_SIZE.
    // After this, the head mirror is intact but the tail footer
    // Block + size hint are gone — the exact failure mode the
    // recovery contract has to handle.
    let file = fs
        .open(path, &FsOpenOptions::new().write(true).read(true))
        .unwrap();
    file.set_len(HEAD_FOOTER_RESERVED_SIZE).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let reader = ManifestArchiveReader::open(path, &fs, Arc::new(RuntimeConfig::default()), None)
        .expect("head-only manifest must recover via head fallback");
    assert_eq!(
        reader.source(),
        FooterSource::Head,
        "reader should have fallen back to the head mirror"
    );
    assert!(reader.section("format_version").is_some());
}

/// With `manifest_ecc` on (`RuntimeConfig` `page_ecc = true`), manifest
/// blocks are written with a fixed RS(4,2) parity trailer and the reader
/// must size + skip it to read the section bytes back verbatim. Exercises
/// the `PlainEcc` arm of both the writer's and reader's transform matrix.
#[cfg(feature = "page_ecc")]
#[test]
fn reader_reads_section_when_manifest_ecc_enabled_returns_verbatim_bytes() {
    let fs = fresh_fs();
    let path = Path::new("/m/ecc_plain");
    let runtime = RuntimeConfig {
        page_ecc: true,
        ..RuntimeConfig::default()
    };
    write_manifest(
        &fs,
        path,
        runtime.clone(),
        &[("format_version", &[5]), ("tree_type", &[0])],
    );

    let mut reader = ManifestArchiveReader::open(path, &fs, Arc::new(runtime), None).unwrap();
    assert_eq!(reader.read_section("format_version").unwrap(), vec![5]);
    assert_eq!(reader.read_section("tree_type").unwrap(), vec![0]);
}

/// `manifest_ecc` on AND encryption configured: the parity trailer sits
/// over the ciphertext. Exercises the `EncryptedEcc` arm of both the
/// writer's and reader's transform matrix.
#[cfg(all(feature = "page_ecc", feature = "encryption"))]
#[test]
fn reader_reads_encrypted_section_when_manifest_ecc_enabled_returns_verbatim_bytes() {
    use crate::encryption::{Aes256GcmProvider, EncryptionProvider};

    let fs = fresh_fs();
    let path = Path::new("/m/ecc_enc");
    let enc: Arc<dyn EncryptionProvider> = Arc::new(Aes256GcmProvider::new(&[7u8; 32]));
    let runtime = RuntimeConfig {
        page_ecc: true,
        ..RuntimeConfig::default()
    };

    let mut w = ManifestArchiveWriter::create(
        path,
        &fs,
        Arc::new(runtime.clone()),
        Some(Arc::clone(&enc)),
        crate::fs::SyncMode::Normal,
    )
    .unwrap();
    w.start("format_version").unwrap();
    use std::io::Write;
    w.write_all(&[5u8]).unwrap();
    w.finish().unwrap();

    let mut reader = ManifestArchiveReader::open(path, &fs, Arc::new(runtime), Some(enc)).unwrap();
    assert_eq!(reader.read_section("format_version").unwrap(), vec![5]);
}
