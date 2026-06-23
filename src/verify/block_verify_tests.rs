use super::*;
// `AbstractTree` looks unused at a glance but the test bodies below
// call `.insert()`, `.flush_active_memtable()`, and
// `.current_version()` on `AnyTree` values — those are trait
// methods, not inherent ones, so the trait MUST be in scope for
// method resolution. Removing the import is a compile error, not
// a clippy nit.
use crate::{
    AbstractTree, Config, SequenceNumberCounter, compression::CompressionType,
    config::CompressionPolicy,
};
use std::io::{Read, Seek, SeekFrom, Write};
// Shadows the built-in `#[test]` so `#[test]`-annotated functions
// below resolve to `test_log::test` (which wires up logging for
// failing tests). This matches every other test module in the
// crate — the import looks unused at a glance but the proc-macro
// attribute name resolution consumes it.
use test_log::test;

fn populate_tree(dir: &std::path::Path, items: usize) {
    let cfg = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None));
    let tree = cfg.open().unwrap();
    for i in 0u64..items as u64 {
        let key = format!("k{i:08}");
        let val = format!("v{i:08}");
        tree.insert(key.as_bytes(), val.as_bytes(), 1 + i);
    }
    tree.flush_active_memtable(1 + items as u64).unwrap();
    // Drop the tree so all files are closed before the test that
    // mutates SST bytes on disk reopens them via Verify.
    drop(tree);
}

fn reopen_tree(dir: &std::path::Path) -> crate::AnyTree {
    Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .open()
    .unwrap()
}

/// Populates a tree with per-KV checksums enabled (`AllLevels`) so the
/// flushed SST carries data blocks with the `KV_CHECKSUM_FOOTER` flag
/// set and a per-entry checksum footer.
fn populate_tree_kv_checked(dir: &std::path::Path, items: usize) {
    use crate::AbstractTree;
    use crate::runtime_config::KvChecksumPolicy;

    let cfg = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None));
    let any = cfg.open().unwrap();
    let crate::AnyTree::Standard(tree) = any else {
        panic!("expected Standard tree");
    };
    tree.update_runtime_config(|c| {
        c.kv_checksums = KvChecksumPolicy::AllLevels;
    })
    .unwrap();
    for i in 0u64..items as u64 {
        let key = format!("k{i:08}");
        let val = format!("v{i:08}");
        tree.insert(key.as_bytes(), val.as_bytes(), 1 + i);
    }
    tree.flush_active_memtable(1 + items as u64).unwrap();
    drop(tree);
}

#[test]
fn verify_block_checksums_clean_tree_has_no_errors() {
    let dir = tempfile::tempdir().unwrap();
    populate_tree(dir.path(), 1_000);

    let tree = reopen_tree(dir.path());
    let report = verify_block_checksums(&tree);
    assert!(
        report.is_ok(),
        "expected clean tree to verify with zero errors, got {:?}",
        report.errors
    );
    assert!(
        report.blocks_scanned > 0,
        "expected at least one block scanned",
    );
    assert!(
        report.sst_files_scanned >= 1,
        "expected at least one SST scanned",
    );
}

#[cfg(feature = "page_ecc")]
#[test]
fn verify_block_checksums_clean_page_ecc_tree_has_no_errors() {
    // Regression: with page_ecc on, every SST data / index / filter block
    // carries a Reed-Solomon parity trailer after its payload. SST blocks
    // omit the block_flags byte, so the scrub learns parity presence from
    // the per-SST descriptor and must skip `expected_parity_len(data_length)`
    // bytes per block. Without that skip the walk mis-reads parity as the
    // next block's header and reports spurious HeaderCorrupted. Enough items
    // to spill multiple data blocks so cross-block alignment is exercised.
    use crate::AbstractTree;

    let dir = tempfile::tempdir().unwrap();
    {
        let any = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        .page_ecc(true)
        .ecc_scheme(crate::runtime_config::EccScheme::ReedSolomon {
            data_shards: 4,
            parity_shards: 2,
        })
        .open()
        .unwrap();
        for i in 0u64..2_000 {
            let key = format!("k{i:08}");
            let val = format!("v{i:08}");
            any.insert(key.as_bytes(), val.as_bytes(), 1 + i);
        }
        any.flush_active_memtable(2_001).unwrap();
        drop(any);
    }

    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .page_ecc(true)
    .ecc_scheme(crate::runtime_config::EccScheme::ReedSolomon {
        data_shards: 4,
        parity_shards: 2,
    })
    .open()
    .unwrap();
    let report = verify_block_checksums(&tree);
    assert!(
        report.is_ok(),
        "page_ecc tree must verify with zero errors (parity trailers skipped \
         per block), got {:?}",
        report.errors,
    );
    assert!(
        report.blocks_scanned > 1,
        "expected multiple blocks scanned to exercise cross-block alignment",
    );
}

#[cfg(feature = "page_ecc")]
#[test]
fn verify_block_checksums_clean_nondefault_ecc_tree_has_no_errors() {
    // Regression: the scrub must size each SST's parity trailer from the
    // per-SST descriptor scheme, NOT a hardcoded RS(4,2). A table written
    // with a non-default scheme (RS(8,2), different shard size → different
    // trailer length) is mis-walked if the scrub assumes RS(4,2): the
    // wrong trailer length mis-aligns the next block and reports spurious
    // corruption. With descriptor-driven sizing the walk stays aligned.
    use crate::AbstractTree;

    let dir = tempfile::tempdir().unwrap();
    {
        let any = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        .page_ecc(true)
        .ecc_scheme(crate::runtime_config::EccScheme::ReedSolomon {
            data_shards: 8,
            parity_shards: 2,
        })
        .open()
        .unwrap();
        for i in 0u64..2_000 {
            let key = format!("k{i:08}");
            let val = format!("v{i:08}");
            any.insert(key.as_bytes(), val.as_bytes(), 1 + i);
        }
        any.flush_active_memtable(2_001).unwrap();
        drop(any);
    }

    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .page_ecc(true)
    .ecc_scheme(crate::runtime_config::EccScheme::ReedSolomon {
        data_shards: 8,
        parity_shards: 2,
    })
    .open()
    .unwrap();
    let report = verify_block_checksums(&tree);
    assert!(
        report.is_ok(),
        "non-default-scheme ECC tree must verify with zero errors \
         (parity sized from the descriptor, not RS(4,2)), got {:?}",
        report.errors,
    );
    assert!(
        report.blocks_scanned > 1,
        "expected multiple blocks scanned to exercise cross-block alignment",
    );
}

/// Returns the on-disk path of the first SST registered with the
/// tree's current version. Drops the tree before returning so the
/// caller can mutate the file safely (no descriptor cache, no
/// file lock). Going through `current_version().iter_tables()`
/// instead of a filesystem walk keeps the test coupled to the
/// verifier's actual input set — a new on-disk file under the
/// tree directory cannot accidentally become the corruption
/// target.
fn pick_first_sst_path(dir: &std::path::Path) -> std::path::PathBuf {
    let tree = reopen_tree(dir);
    let path = tree
        .current_version()
        .iter_tables()
        .next()
        .map(|table| (*table.path).clone())
        .expect("at least one populated SST file");
    drop(tree);
    path
}

#[test]
fn verify_block_checksums_detects_flipped_byte_in_data_block() {
    use crate::table::block::Header;
    let dir = tempfile::tempdir().unwrap();
    populate_tree(dir.path(), 1_000);

    let sst_path = pick_first_sst_path(dir.path());

    // The flip target is the first byte AFTER the first block's
    // Header — that lands squarely inside the data segment of the
    // first data block, so the header's own XXH3 stays valid (no
    // HeaderCorrupted) but the data XXH3 will now mismatch.
    let flip_offset = Header::MIN_LEN as u64;
    {
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&sst_path)
            .unwrap();
        f.seek(SeekFrom::Start(flip_offset)).unwrap();
        let mut byte = [0u8; 1];
        f.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF;
        f.seek(SeekFrom::Start(flip_offset)).unwrap();
        f.write_all(&byte).unwrap();
        f.sync_all().unwrap();
    }

    let tree = reopen_tree(dir.path());
    let report = verify_block_checksums(&tree);
    assert!(
        !report.is_ok(),
        "expected corruption to surface as report errors, got {report:?}",
    );
    let has_data_corruption = report.errors.iter().any(|e| {
        matches!(
            e,
            BlockVerifyError::DataCorrupted { path, .. } if path == &sst_path,
        )
    });
    assert!(
        has_data_corruption,
        "expected a DataCorrupted error for {}, got {:?}",
        sst_path.display(),
        report.errors,
    );
}

#[test]
fn verify_kv_checksums_clean_kv_checked_tree_passes() {
    // A tree written with per-KV checksums enabled must pass the
    // per-KV scrub with no error.
    let dir = tempfile::tempdir().unwrap();
    populate_tree_kv_checked(dir.path(), 500);

    let tree = reopen_tree(dir.path());
    let crate::AnyTree::Standard(tree) = tree else {
        panic!("expected Standard tree");
    };
    verify_kv_checksums(&tree).expect("clean kv-checked tree must pass per-KV scrub");
}

#[test]
fn verify_kv_checked_detects_corrupted_digest_under_valid_block_checksum() {
    // The per-KV verifier must catch a divergence that the block-level
    // XXH3 does NOT: corrupt a stored digest, then write the block so its
    // block-level checksum is computed over the already-corrupted bytes.
    // The block therefore loads cleanly (block checksum valid) and only
    // the per-KV recompute disagrees. (Flipping a payload byte in the
    // file without recomputing the block checksum would be caught at
    // load, never reaching the per-KV path — which would prove nothing.)
    use crate::InternalValue;
    use crate::ValueType::Value;
    use crate::comparator::default_comparator;
    use crate::runtime_config::ChecksumAlgorithm;
    use crate::table::block::header::block_flags;
    use crate::table::block::{Block, BlockIdentity, BlockTransform, BlockType, kv_checksum};
    use crate::table::data_block::DataBlock;

    let algo = ChecksumAlgorithm::Xxh3_64;
    let items = [
        InternalValue::from_components(b"alpha".to_vec(), b"one".to_vec(), 3, Value),
        InternalValue::from_components(b"bravo".to_vec(), b"two".to_vec(), 2, Value),
    ];
    let digests: Vec<u64> = items
        .iter()
        .map(|it| kv_checksum::kv_digest(it, algo).expect("xxh3 always available"))
        .collect();

    let mut payload = Vec::new();
    DataBlock::encode_kv_checked_into(&mut payload, &items, &digests, algo, 2, 0.0).unwrap();

    // Corrupt the first stored digest (its first byte sits right after the
    // inner payload, where the digest array begins).
    let inner_len = kv_checksum::split_inner(&payload).unwrap().len();
    *payload.get_mut(inner_len).expect("digest array byte") ^= 0xFF;

    // Write with a VALID block-level checksum over the corrupted payload.
    // Data blocks omit the `block_flags` byte (footer presence is a per-SST
    // descriptor property), so the KV_CHECKSUM_FOOTER flag passed here is
    // dropped on encode — the footer rides inside the payload structurally,
    // and `verify_kv_checked` splits it without consulting the header bit.
    let id = BlockIdentity::for_test(0, BlockType::Data);
    let mut buf = Vec::new();
    Block::write_into_with_flags(
        &mut buf,
        &payload,
        id,
        &BlockTransform::PLAIN,
        block_flags::KV_CHECKSUM_FOOTER,
    )
    .unwrap();

    // Block loads fine (block-level checksum matches the corrupted bytes).
    let block = Block::from_reader(&mut &buf[..], id, &BlockTransform::PLAIN).unwrap();

    // Only the per-KV verifier catches the bad digest. `None` skips the
    // algorithm cross-check — this test exercises the digest-mismatch path.
    let err = DataBlock::verify_kv_checked(&block.data, block.header, default_comparator(), None)
        .expect_err("corrupted stored digest must fail the per-KV verifier");
    assert!(
        matches!(err, crate::Error::ChecksumMismatch { .. }),
        "expected ChecksumMismatch, got {err:?}"
    );
}

#[test]
fn verify_kv_checked_rejects_non_data_block_type() {
    // The scrub verifies only Data blocks. A header whose block_type is
    // not Data (corruption or a caller bug) must be rejected with
    // InvalidTag, not silently coerced to Data and verified as if it were
    // a data block — coercion would defeat the scrub's purpose.
    use crate::InternalValue;
    use crate::ValueType::Value;
    use crate::comparator::default_comparator;
    use crate::runtime_config::ChecksumAlgorithm;
    use crate::table::block::header::block_flags;
    use crate::table::block::{Block, BlockIdentity, BlockTransform, BlockType, kv_checksum};
    use crate::table::data_block::DataBlock;

    let algo = ChecksumAlgorithm::Xxh3_64;
    let items = [
        InternalValue::from_components(b"alpha".to_vec(), b"one".to_vec(), 3, Value),
        InternalValue::from_components(b"bravo".to_vec(), b"two".to_vec(), 2, Value),
    ];
    let digests: Vec<u64> = items
        .iter()
        .map(|it| kv_checksum::kv_digest(it, algo).expect("xxh3 always available"))
        .collect();

    let mut payload = Vec::new();
    DataBlock::encode_kv_checked_into(&mut payload, &items, &digests, algo, 2, 0.0).unwrap();

    let id = BlockIdentity::for_test(0, BlockType::Data);
    let mut buf = Vec::new();
    Block::write_into_with_flags(
        &mut buf,
        &payload,
        id,
        &BlockTransform::PLAIN,
        block_flags::KV_CHECKSUM_FOOTER,
    )
    .unwrap();
    let block = Block::from_reader(&mut &buf[..], id, &BlockTransform::PLAIN).unwrap();

    // The footer + inner bytes form a valid data block, so only the
    // block_type gate can catch a tampered type: flip it to a non-Data
    // variant and require InvalidTag.
    let mut bad_header = block.header;
    bad_header.block_type = BlockType::Index;
    let err = DataBlock::verify_kv_checked(&block.data, bad_header, default_comparator(), None)
        .expect_err("non-Data block_type must be rejected, not coerced");
    assert!(
        matches!(err, crate::Error::InvalidTag(("BlockType", _))),
        "expected InvalidTag(BlockType), got {err:?}"
    );
}

/// Exercises the out-of-band wrapper on a real clean SST file.
/// `verify_sst_file` is the entry point sst-dump calls; this pins
/// that it stamps `sst_files_scanned = 1`, reports no errors on a
/// healthy file, and propagates the block count through the
/// `StdFs` -> `scan_sst_blocks` -> `BlockVerifyReport` path.
#[test]
fn verify_sst_file_clean_file_has_no_errors() {
    let dir = tempfile::tempdir().unwrap();
    populate_tree(dir.path(), 1_000);
    let sst_path = pick_first_sst_path(dir.path());

    let report = verify_sst_file(&sst_path);
    assert!(
        report.is_ok(),
        "expected clean SST to verify with zero errors, got {:?}",
        report.errors,
    );
    assert_eq!(
        report.sst_files_scanned, 1,
        "wrapper must always stamp sst_files_scanned = 1",
    );
    assert!(
        report.blocks_scanned > 0,
        "expected at least one block scanned in a populated SST",
    );
}

/// Exercises the file-open failure branch (the only path through
/// `verify_sst_file` that converts an underlying `io::Error` into
/// a `BlockVerifyError::SstFileUnreadable`). A missing file is the
/// simplest trigger; an unreadable-due-to-permissions trigger
/// would require root or chmod-induced state and is overkill for
/// pinning the variant routing.
#[test]
fn verify_sst_file_missing_file_reports_unreadable() {
    // Build the missing-file path under a fresh tempdir so it
    // resolves the same way on Linux / macOS / Windows runners.
    // A hardcoded Unix-style absolute path would either skip the
    // test on Windows (no `/this/...` semantics) or risk a flaky
    // pass if the path happened to exist.
    let dir = tempfile::tempdir().unwrap();
    let missing_path = dir.path().join("does-not-exist-sst-12345.sst");
    // Sanity: tempdir() guarantees the directory is empty.
    assert!(
        !missing_path.exists(),
        "tempdir entry must be absent for this test to exercise the missing-file branch",
    );

    let report = verify_sst_file(&missing_path);
    assert_eq!(
        report.sst_files_scanned, 1,
        "wrapper stamps sst_files_scanned = 1 even on file-open failure \
         so callers see the attempt was made",
    );
    assert_eq!(
        report.blocks_scanned, 0,
        "no blocks could be walked because the file couldn't be opened",
    );
    assert_eq!(
        report.errors.len(),
        1,
        "expected exactly one error, got {:?}",
        report.errors,
    );
    let err = report.errors.first().unwrap();
    assert!(
        matches!(
            err,
            BlockVerifyError::SstFileUnreadable { table_id: 0, path, .. }
                if path == &missing_path,
        ),
        "expected SstFileUnreadable for {}, got {err:?}",
        missing_path.display(),
    );
}

/// Pins the routing of post-header short-read failures to
/// `BlockVerifyError::DataReadError`. Regression guard for #315:
/// a refactor that collapses the `read_exact` failure branch back
/// into `HeaderCorrupted` (which is what a naive "any read error
/// inside the walker is a header problem" cleanup would do) loses
/// the distinction between "the file's TOC lies about where the
/// section ends" and "the header itself fails its own XXH3", and
/// downstream tooling (`sst-dump`, `repair_db`, lazy block repair)
/// pattern-matches on the variant to decide whether the block is
/// recoverable. Demoting truncated-data to `HeaderCorrupted` would
/// make those tools fall back to whole-section discard instead of
/// per-block surgery.
///
/// Setup forges an SFA archive whose `data` TOC entry claims a
/// section length large enough for one full block (header + N
/// bytes), but the underlying file contains only the header.
/// Result: `Header::decode_from` succeeds (the header's XXH3
/// matches its own bytes), the bounds check passes (`data_length`
/// fits within the lied section length), and the data-segment
/// `read_exact` hits EOF after consuming a handful of trailing
/// TOC + trailer bytes. The only valid landing variant is
/// `DataReadError`.
#[test]
#[expect(
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "synthetic SFA forgery — offsets are all in-bounds by \
              construction (we just wrote the bytes ourselves), and \
              the u64 -> usize cast cannot overflow on any target \
              the test runs on (the forged archive is < 1 KiB)"
)]
fn walk_block_region_reports_data_read_error_on_truncated_data_segment() {
    use crate::coding::Encode;
    use crate::fs::{Fs, FsOpenOptions, MemFs};
    use crate::table::block::{BlockType, Header};

    // Trailer layout (38 bytes at the tail of an SFA archive):
    //   MAGIC(4) | version(1) | csum_type(1) | toc_checksum(16) | toc_pos(8) | toc_len(8)
    const TRAILER_LEN: usize = 4 + 1 + 1 + 16 + 8 + 8;
    const DATA_LENGTH: u32 = 4096;
    const HEADER_LEN: u64 = Header::MIN_LEN as u64;

    let header = Header {
        // Arbitrary sentinel; the walker reaches `read_exact` and
        // bails BEFORE any data-segment XXH3 comparison, so this
        // value is never checked.
        checksum: Checksum::from_raw(0xDEAD_BEEF_DEAD_BEEF),
        data_length: DATA_LENGTH,
        uncompressed_length: DATA_LENGTH,
        ..Header::test_dummy(BlockType::Data)
    };

    // Build a minimal SFA archive: one section "data" containing
    // exactly one Header (33 bytes) and zero following data bytes.
    let mut archive_bytes: Vec<u8> = Vec::new();
    {
        let mut writer = crate::sfa::Writer::from_writer(std::io::Cursor::new(&mut archive_bytes));
        writer.start("data").unwrap();
        writer.write_all(&header.encode_into_vec()).unwrap();
        writer.finish().unwrap();
    }

    // Parse the trailer at the file tail.
    let trailer_start = archive_bytes.len() - TRAILER_LEN;
    let toc_pos_bytes: [u8; 8] = archive_bytes[trailer_start + 22..trailer_start + 30]
        .try_into()
        .unwrap();
    let toc_len_bytes: [u8; 8] = archive_bytes[trailer_start + 30..trailer_start + 38]
        .try_into()
        .unwrap();
    let toc_pos = u64::from_le_bytes(toc_pos_bytes) as usize;
    let toc_len = u64::from_le_bytes(toc_len_bytes) as usize;

    // TOC payload layout: `TOC!`(4) | entry_count(4 LE) | entries.
    // Each entry: pos(8 LE) | len(8 LE) | name_len(2 LE) | name.
    // The first (only) entry begins at toc_pos + 8.
    let first_entry_offset = toc_pos + 4 + 4;
    let len_field_offset = first_entry_offset + 8;

    // Inflate the section length so end_offset = HEADER_LEN +
    // DATA_LENGTH. The walker then computes remaining = DATA_LENGTH
    // (passes the bounds check), tries to `read_exact(DATA_LENGTH)`,
    // and hits EOF after the few trailing TOC + trailer bytes.
    let lied_len: u64 = HEADER_LEN + u64::from(DATA_LENGTH);
    archive_bytes[len_field_offset..len_field_offset + 8].copy_from_slice(&lied_len.to_le_bytes());

    // Recompute the TOC checksum (xxh3_128 over the TOC payload)
    // and patch the trailer's stored checksum so crate::sfa::Reader still
    // accepts the file.
    let new_toc_checksum = crate::hash::hash128(&archive_bytes[toc_pos..toc_pos + toc_len]);
    let csum_field_offset = trailer_start + 4 + 1 + 1;
    archive_bytes[csum_field_offset..csum_field_offset + 16]
        .copy_from_slice(&new_toc_checksum.to_le_bytes());

    // Materialize the forged archive in MemFs and run the scanner.
    let fs = MemFs::new();
    let path = std::path::Path::new("/forged.sst");
    {
        let mut f = fs
            .open(
                path,
                &FsOpenOptions::new().write(true).create(true).truncate(true),
            )
            .unwrap();
        f.write_all(&archive_bytes).unwrap();
    }

    let table_id: TableId = 42;
    let scan = scan_sst_blocks(&fs, path, table_id, 0, None, false)
        .expect("forged SFA must parse cleanly");
    assert_eq!(
        scan.errors.len(),
        1,
        "expected exactly one error, got {:?}",
        scan.errors,
    );
    let err = scan.errors.first().unwrap();
    assert!(
        matches!(
            err,
            BlockVerifyError::DataReadError {
                table_id: t,
                offset: 0,
                data_length: d,
                ..
            } if *t == table_id && *d == DATA_LENGTH,
        ),
        "expected DataReadError {{ table_id: {table_id}, offset: 0, \
         data_length: {DATA_LENGTH}, .. }}; got {err:?}",
    );
    assert_eq!(
        scan.blocks_scanned, 1,
        "header decoded successfully, so blocks_scanned must count this block \
         even though the data segment read failed",
    );
}

/// The parity-trailer drain reports a truncated read when an SST whose ECC
/// descriptor claims per-block parity is missing those trailer bytes. Forges
/// a `data` section of header + its full payload (so the data read and its
/// checksum both pass), then scans it as an RS(4,2) table: the walk drains
/// `expected_parity_len` bytes, hits EOF after the short SFA tail, and
/// surfaces a `DataReadError` for the short parity read rather than
/// mis-reading the tail as the next block.
#[test]
#[expect(
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "synthetic SFA forgery — offsets are in-bounds by construction (we wrote the \
              bytes ourselves) and the archive is < 8 KiB, so the casts cannot overflow"
)]
fn walk_block_region_reports_data_read_error_on_truncated_parity_trailer() {
    use crate::coding::Encode;
    use crate::fs::{Fs, FsOpenOptions, MemFs};
    use crate::table::block::{BlockType, EccParams, Header, expected_parity_len};

    // Trailer layout (38 bytes at the tail of an SFA archive):
    //   MAGIC(4) | version(1) | csum_type(1) | toc_checksum(16) | toc_pos(8) | toc_len(8)
    const TRAILER_LEN: usize = 4 + 1 + 1 + 16 + 8 + 8;
    const DATA_LENGTH: u32 = 4096;
    const HEADER_LEN: u64 = Header::MIN_LEN as u64;

    let data = vec![0xABu8; DATA_LENGTH as usize];
    let header = Header {
        checksum: Checksum::from_raw(crate::hash::hash128(&data)),
        data_length: DATA_LENGTH,
        uncompressed_length: DATA_LENGTH,
        ..Header::test_dummy(BlockType::Data)
    };

    // One `data` section: header + full payload, but no parity trailer.
    let mut archive_bytes: Vec<u8> = Vec::new();
    {
        let mut writer = crate::sfa::Writer::from_writer(std::io::Cursor::new(&mut archive_bytes));
        writer.start("data").unwrap();
        writer.write_all(&header.encode_into_vec()).unwrap();
        writer.write_all(&data).unwrap();
        writer.finish().unwrap();
    }

    // Inflate the section length to header + payload + parity so the walker's
    // `data_length + parity_len <= remaining` bounds check passes; the parity
    // bytes were never written, so the drain hits EOF instead. Recompute the
    // TOC checksum afterwards so crate::sfa::Reader still accepts the file.
    let parity_len = u64::from(expected_parity_len(DATA_LENGTH, EccParams::RS_4_2));
    let trailer_start = archive_bytes.len() - TRAILER_LEN;
    let toc_pos = u64::from_le_bytes(
        archive_bytes[trailer_start + 22..trailer_start + 30]
            .try_into()
            .unwrap(),
    ) as usize;
    let toc_len = u64::from_le_bytes(
        archive_bytes[trailer_start + 30..trailer_start + 38]
            .try_into()
            .unwrap(),
    ) as usize;
    let len_field_offset = toc_pos + 4 + 4 + 8;
    let lied_len: u64 = HEADER_LEN + u64::from(DATA_LENGTH) + parity_len;
    archive_bytes[len_field_offset..len_field_offset + 8].copy_from_slice(&lied_len.to_le_bytes());
    let new_toc_checksum = crate::hash::hash128(&archive_bytes[toc_pos..toc_pos + toc_len]);
    let csum_field_offset = trailer_start + 4 + 1 + 1;
    archive_bytes[csum_field_offset..csum_field_offset + 16]
        .copy_from_slice(&new_toc_checksum.to_le_bytes());

    let fs = MemFs::new();
    let path = std::path::Path::new("/forged-parity.sst");
    {
        let mut f = fs
            .open(
                path,
                &FsOpenOptions::new().write(true).create(true).truncate(true),
            )
            .unwrap();
        f.write_all(&archive_bytes).unwrap();
    }

    // Scan as an RS(4,2) table: a non-zero parity_len is drained after the
    // (clean) payload, hitting EOF in the short SFA tail.
    let table_id: TableId = 7;
    let scan = scan_sst_blocks(&fs, path, table_id, 0, Some(EccParams::RS_4_2), false)
        .expect("forged SFA must parse cleanly");
    assert!(
        scan.errors.iter().any(|e| matches!(
            e,
            BlockVerifyError::DataReadError { table_id: t, offset: 0, error, .. }
                if *t == table_id && error.kind() == crate::io::ErrorKind::UnexpectedEof
        )),
        "expected a truncated-parity DataReadError, got {:?}",
        scan.errors,
    );
}

/// A block header whose own bytes extend past the section boundary must be
/// reported as `HeaderCorrupted`, not slip through with a clamped-to-zero
/// remaining payload.
///
/// Setup forges a section whose lied length is exactly `Header::MIN_LEN`
/// (so the `< MIN_LEN` guard passes) and stores a `Meta` block, whose
/// `header_len` is `MIN_LEN + 1`. `Header::decode_from` reads the full
/// `MIN_LEN + 1` header bytes from the file (they are physically present,
/// followed by the TOC), but those bytes cross the section boundary, so the
/// boundary guard fires.
#[test]
#[expect(
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "synthetic SFA forgery — offsets are in-bounds by construction \
              and the forged archive is < 1 KiB"
)]
fn walk_block_region_reports_header_crossing_section_boundary() {
    use crate::coding::Encode;
    use crate::fs::{Fs, FsOpenOptions, MemFs};
    use crate::table::block::{BlockType, Header};

    const TRAILER_LEN: usize = 4 + 1 + 1 + 16 + 8 + 8;

    // `Meta` blocks carry the block_flags byte, so header_len == MIN_LEN + 1.
    let header = Header {
        checksum: Checksum::from_raw(0xDEAD_BEEF_DEAD_BEEF),
        data_length: 0,
        uncompressed_length: 0,
        ..Header::test_dummy(BlockType::Meta)
    };
    assert_eq!(
        Header::header_len(BlockType::Meta) as u64,
        Header::MIN_LEN as u64 + 1,
    );

    let mut archive_bytes: Vec<u8> = Vec::new();
    {
        let mut writer = crate::sfa::Writer::from_writer(std::io::Cursor::new(&mut archive_bytes));
        writer.start("data").unwrap();
        writer.write_all(&header.encode_into_vec()).unwrap();
        writer.finish().unwrap();
    }

    let trailer_start = archive_bytes.len() - TRAILER_LEN;
    let toc_pos_bytes: [u8; 8] = archive_bytes[trailer_start + 22..trailer_start + 30]
        .try_into()
        .unwrap();
    let toc_len_bytes: [u8; 8] = archive_bytes[trailer_start + 30..trailer_start + 38]
        .try_into()
        .unwrap();
    let toc_pos = u64::from_le_bytes(toc_pos_bytes) as usize;
    let toc_len = u64::from_le_bytes(toc_len_bytes) as usize;

    let first_entry_offset = toc_pos + 4 + 4;
    let len_field_offset = first_entry_offset + 8;

    // Lie that the section is exactly MIN_LEN bytes: one byte short of the
    // Meta header, so the header decode crosses the section boundary.
    let lied_len: u64 = Header::MIN_LEN as u64;
    archive_bytes[len_field_offset..len_field_offset + 8].copy_from_slice(&lied_len.to_le_bytes());

    let new_toc_checksum = crate::hash::hash128(&archive_bytes[toc_pos..toc_pos + toc_len]);
    let csum_field_offset = trailer_start + 4 + 1 + 1;
    archive_bytes[csum_field_offset..csum_field_offset + 16]
        .copy_from_slice(&new_toc_checksum.to_le_bytes());

    let fs = MemFs::new();
    let path = std::path::Path::new("/forged-boundary.sst");
    {
        let mut f = fs
            .open(
                path,
                &FsOpenOptions::new().write(true).create(true).truncate(true),
            )
            .unwrap();
        f.write_all(&archive_bytes).unwrap();
    }

    let table_id: TableId = 7;
    let scan = scan_sst_blocks(&fs, path, table_id, 0, None, false)
        .expect("forged SFA must parse cleanly");
    assert_eq!(
        scan.errors.len(),
        1,
        "expected exactly one error, got {:?}",
        scan.errors,
    );
    let err = scan.errors.first().unwrap();
    assert!(
        matches!(
            err,
            BlockVerifyError::HeaderCorrupted { table_id: t, offset: 0, reason, .. }
                if *t == table_id && reason.contains("extends past the section end"),
        ),
        "expected a section-boundary HeaderCorrupted; got {err:?}",
    );
}

/// Builds a tree with `batches` separate L0 SSTs (one flush per batch) so
/// the parallel scrubber actually has multiple files to fan out over.
fn populate_multi_sst(dir: &std::path::Path, batches: usize, per_batch: usize) {
    let cfg = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None));
    let tree = cfg.open().unwrap();
    let mut seqno = 1u64;
    for b in 0..batches {
        for i in 0..per_batch {
            let key = format!("b{b:03}k{i:08}");
            tree.insert(key.as_bytes(), b"v".as_slice(), seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(seqno).unwrap();
        seqno += 1;
    }
    drop(tree);
}

#[test]
fn verify_checksum_method_on_clean_tree_is_ok() {
    let dir = tempfile::tempdir().unwrap();
    populate_tree(dir.path(), 500);
    let tree = reopen_tree(dir.path());
    let report = tree.verify_checksum();
    assert!(report.is_ok(), "clean tree must verify clean: {report:?}");
    assert!(report.sst_files_scanned >= 1);
    assert!(report.blocks_scanned >= 1);
}

#[test]
fn verify_checksum_with_parallel_matches_sequential() {
    let dir = tempfile::tempdir().unwrap();
    populate_multi_sst(dir.path(), 5, 300);
    let tree = reopen_tree(dir.path());

    let seq = tree.verify_checksum_with(&VerifyOptions::default());
    let par = tree.verify_checksum_with(&VerifyOptions::default().parallelism(4));

    assert!(
        seq.sst_files_scanned >= 2,
        "need >1 SST to exercise parallelism, got {}",
        seq.sst_files_scanned,
    );
    // Parallel run reports the SAME findings as sequential — only order may
    // differ. Counts must match exactly.
    assert_eq!(seq.sst_files_scanned, par.sst_files_scanned);
    assert_eq!(seq.blocks_scanned, par.blocks_scanned);
    assert_eq!(seq.errors.len(), par.errors.len());
    assert!(
        seq.is_ok() && par.is_ok(),
        "clean tree: seq={seq:?} par={par:?}"
    );
}

#[test]
fn verify_checksum_with_throttle_runs_inter_sst_pause() {
    // A non-zero throttle on the default (serial) path exercises the
    // inter-SST pause between tables. The smallest possible delay keeps the
    // test fast while still hitting the sleep branch.
    let dir = tempfile::tempdir().unwrap();
    populate_multi_sst(dir.path(), 3, 300);
    let tree = reopen_tree(dir.path());

    let report = tree.verify_checksum_with(
        &VerifyOptions::default().throttle(std::time::Duration::from_nanos(1)),
    );
    assert!(
        report.sst_files_scanned >= 2,
        "need >1 SST to exercise the inter-SST throttle, got {}",
        report.sst_files_scanned,
    );
    assert!(report.is_ok(), "clean tree must verify clean: {report:?}");
}

#[test]
fn verify_checksum_with_parallel_detects_corruption() {
    use crate::table::block::Header;
    let dir = tempfile::tempdir().unwrap();
    populate_multi_sst(dir.path(), 4, 300);

    let sst_path = pick_first_sst_path(dir.path());
    let flip_offset = Header::MIN_LEN as u64;
    {
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&sst_path)
            .unwrap();
        f.seek(SeekFrom::Start(flip_offset)).unwrap();
        let mut byte = [0u8; 1];
        f.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF;
        f.seek(SeekFrom::Start(flip_offset)).unwrap();
        f.write_all(&byte).unwrap();
        f.sync_all().unwrap();
    }

    let tree = reopen_tree(dir.path());
    let report = tree.verify_checksum_with(&VerifyOptions::default().parallelism(4));
    assert!(
        !report.is_ok(),
        "parallel scrub must surface the flipped byte: {report:?}",
    );
}

#[test]
fn verify_checksum_with_throttle_completes_clean() {
    let dir = tempfile::tempdir().unwrap();
    populate_multi_sst(dir.path(), 3, 200);
    let tree = reopen_tree(dir.path());
    let opts = VerifyOptions::default()
        .parallelism(2)
        .throttle(std::time::Duration::from_millis(1));
    let report = tree.verify_checksum_with(&opts);
    assert!(
        report.is_ok(),
        "throttled scrub must still verify clean: {report:?}"
    );
    assert!(report.sst_files_scanned >= 2);
}

#[test]
fn verify_checksum_with_throttle_does_not_sleep_after_last_sst() {
    // Regression: the throttle is an INTER-SST pause and must not fire after
    // the final SST. A single-SST tree scrubbed with a large throttle must
    // return promptly; the bug slept one full throttle interval after the
    // only table, making a finished scrub look hung. Sequential path
    // (parallelism 1, one table) so this pins the single-worker loop.
    let dir = tempfile::tempdir().unwrap();
    populate_multi_sst(dir.path(), 1, 50);
    let tree = reopen_tree(dir.path());
    let throttle = std::time::Duration::from_millis(400);
    let opts = VerifyOptions::default().parallelism(1).throttle(throttle);
    let start = std::time::Instant::now();
    let report = tree.verify_checksum_with(&opts);
    let elapsed = start.elapsed();
    assert!(report.is_ok(), "clean single-SST scrub: {report:?}");
    assert_eq!(report.sst_files_scanned, 1, "test needs exactly one SST");
    assert!(
        elapsed < throttle / 2,
        "a single-SST scrub must not sleep the inter-SST throttle after the \
         last table: took {elapsed:?} with a {throttle:?} throttle",
    );
}
