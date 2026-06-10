// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! End-to-end smoke tests for the `dump-block` forensic subcommand.
//!
//! Two block shapes are exercised against the compiled `sst-dump` binary:
//!
//! - **Encrypted** blocks (built via `with_encryption`): `dump-block` parses
//!   the AAD-bound `MetadataFrame` envelope WITHOUT the key and reports the
//!   cryptographic parameters; the inner zstd-block census is intentionally
//!   `null` because the compressed stream is sealed inside the AEAD ciphertext.
//! - **Non-encrypted** blocks: `dump-block` reports the plaintext structure —
//!   for a zstd-compressed block it walks the inner block chain key-free
//!   (no decompression) and prints a per-block census; for an uncompressed
//!   block it reports the payload as non-zstd.
//!
//! Page-ECC presence is reported from the SST's own metadata: a known value
//! (`false`) for a non-encrypted SST written without ECC, and `unknown` for an
//! encrypted SST whose meta block (and ECC descriptor) is sealed under AEAD.

use lsm_tree::{
    AbstractTree, Aes256GcmProvider, Config, SequenceNumberCounter, compression::CompressionType,
    config::CompressionPolicy,
};
use std::process::Command;
use std::sync::Arc;

const SST_DUMP_BIN: &str = env!("CARGO_BIN_EXE_sst-dump");

fn first_sst(dir: &std::path::Path) -> std::path::PathBuf {
    let tables = dir.join("tables");
    std::fs::read_dir(&tables)
        .expect("tables dir")
        .filter_map(Result::ok)
        .find(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .expect("at least one SST")
        .path()
}

fn populate(tree: &impl AbstractTree) {
    for i in 0u64..200 {
        tree.insert(format!("key-{i:06}"), format!("value-{i}"), 1 + i);
    }
    tree.flush_active_memtable(0).expect("flush");
}

/// Builds a single-SST tree with the given compression policy and optional
/// encryption, returning the temp dir (kept alive) and the first SST's path.
fn build_sst(
    compression: CompressionType,
    encrypt: bool,
) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(compression));
    if encrypt {
        let key = [0x42u8; 32];
        config = config.with_encryption(Some(Arc::new(Aes256GcmProvider::new(&key))));
    }
    let tree = config.open().expect("open tree");
    populate(&tree);
    drop(tree);
    let sst = first_sst(dir.path());
    (dir, sst)
}

fn dump_block_at0(sst: &std::path::Path, extra: &[&str]) -> std::process::Output {
    let mut cmd = Command::new(SST_DUMP_BIN);
    cmd.arg(sst).arg("dump-block").arg("0");
    for a in extra {
        cmd.arg(a);
    }
    cmd.output().expect("spawn sst-dump")
}

#[test]
fn dump_block_on_encrypted_block_prints_metadata_key_free() {
    let (_dir, sst) = build_sst(CompressionType::None, true);
    let out = dump_block_at0(&sst, &[]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "expected exit 0; stdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(stdout.contains("encrypted: true"), "got:\n{stdout}");
    assert!(stdout.contains("format_version: 1"), "got:\n{stdout}");
    assert!(stdout.contains("suite: Aes256Gcm"), "got:\n{stdout}");
    assert!(stdout.contains("nonce: \""), "got:\n{stdout}");
    assert!(stdout.contains("aead_tag: \""), "got:\n{stdout}");
    // tree_id / table_id are not on disk and were not supplied.
    assert!(stdout.contains("tree_id: null"), "got:\n{stdout}");
    // The inner zstd structure is opaque without the key.
    assert!(
        stdout.contains("inner_zstd_frame: null"),
        "encrypted block must report opaque inner frame; got:\n{stdout}",
    );
    // ECC descriptor lives in the AEAD-sealed meta, so it is unknown key-free.
    assert!(stdout.contains("page_ecc: unknown"), "got:\n{stdout}");
}

#[test]
fn dump_block_echoes_caller_supplied_ids() {
    let (_dir, sst) = build_sst(CompressionType::None, true);
    let out = dump_block_at0(&sst, &["--tree-id", "7", "--table-id", "42"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "expected exit 0; got:\n{stdout}");
    assert!(stdout.contains("tree_id: 7"), "got:\n{stdout}");
    assert!(stdout.contains("table_id: 42"), "got:\n{stdout}");
}

#[test]
fn dump_block_reconstruct_aad_emits_hex() {
    let (_dir, sst) = build_sst(CompressionType::None, true);
    let out = dump_block_at0(&sst, &["--table-id", "42", "--reconstruct-aad"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "got:\n{stdout}");
    // The v1 AAD is 23 bytes → 46 lowercase hex chars between the quotes.
    let hex = stdout
        .lines()
        .find_map(|l| l.strip_prefix("reconstructed_aad: \""))
        .and_then(|rest| rest.strip_suffix('"'))
        .unwrap_or_else(|| panic!("no reconstructed_aad line; got:\n{stdout}"));
    assert_eq!(
        hex.len(),
        46,
        "AAD must be 23 bytes (46 hex chars); got {hex:?}"
    );
    assert!(
        hex.bytes().all(|b| b.is_ascii_hexdigit()),
        "AAD must be lowercase hex; got {hex:?}",
    );
}

#[test]
fn dump_block_reconstruct_aad_requires_table_id() {
    let (_dir, sst) = build_sst(CompressionType::None, true);
    let out = dump_block_at0(&sst, &["--reconstruct-aad"]);
    assert!(
        !out.status.success(),
        "--reconstruct-aad without --table-id must exit non-zero",
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("requires --table-id"),
        "expected the requires-table-id error; got:\n{stderr}",
    );
}

#[test]
fn dump_block_on_plain_uncompressed_block_reports_structure() {
    let (_dir, sst) = build_sst(CompressionType::None, false);
    let out = dump_block_at0(&sst, &[]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "a non-encrypted block now dumps its structure; stdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(stdout.contains("encrypted: false"), "got:\n{stdout}");
    // Uncompressed payload: no zstd frame to walk.
    assert!(
        stdout.contains("inner_zstd_frame: null"),
        "uncompressed block has no zstd frame; got:\n{stdout}",
    );
    // Non-encrypted SST meta is readable key-free → ECC presence is known.
    assert!(stdout.contains("page_ecc: false"), "got:\n{stdout}");
}

#[test]
fn dump_block_on_plain_zstd_block_lists_inner_blocks() {
    let (_dir, sst) = build_sst(CompressionType::Zstd(3), false);
    let out = dump_block_at0(&sst, &[]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "stdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(stdout.contains("encrypted: false"), "got:\n{stdout}");
    assert!(stdout.contains("compression: zstd"), "got:\n{stdout}");
    // The inner-block census is present with at least one block.
    assert!(stdout.contains("inner_zstd_frame:"), "got:\n{stdout}");
    assert!(stdout.contains("block_count:"), "got:\n{stdout}");
    let block_count = stdout
        .lines()
        .find_map(|l| l.trim().strip_prefix("block_count: "))
        .and_then(|n| n.parse::<u32>().ok())
        .unwrap_or_else(|| panic!("no block_count line; got:\n{stdout}"));
    assert!(block_count >= 1, "expected >=1 inner block; got:\n{stdout}");
    // At least one per-block census row is emitted.
    assert!(
        stdout.contains("    - {index: 0,"),
        "expected a per-block census row; got:\n{stdout}",
    );
}

#[test]
fn dump_block_on_encrypted_zstd_block_reports_codec_but_opaque_inner() {
    // Encrypted + zstd: the envelope reports compression_type Zstd, but the
    // inner frame stays opaque (sealed in ciphertext).
    let (_dir, sst) = build_sst(CompressionType::Zstd(3), true);
    let out = dump_block_at0(&sst, &[]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "got:\n{stdout}");
    assert!(stdout.contains("encrypted: true"), "got:\n{stdout}");
    assert!(stdout.contains("compression_type: Zstd"), "got:\n{stdout}");
    assert!(stdout.contains("inner_zstd_frame: null"), "got:\n{stdout}");
}

#[test]
fn dump_block_corpus_mixed_compression_and_encryption() {
    // Round-trip the dump over the achievable corpus: every compression type
    // crossed with encrypted (AES-256-GCM) and non-encrypted. (ChaCha20-Poly1305
    // is a defined suite but has no public provider, so the suite axis is
    // single-suite here; see #256.)
    for compression in [
        CompressionType::None,
        CompressionType::Lz4,
        CompressionType::Zstd(3),
    ] {
        for encrypt in [false, true] {
            let (_dir, sst) = build_sst(compression, encrypt);
            let out = dump_block_at0(&sst, &[]);
            let stdout = String::from_utf8_lossy(&out.stdout);
            assert!(
                out.status.success(),
                "dump-block failed for compression={compression:?} encrypt={encrypt}; \
                 stdout:\n{stdout}\nstderr:\n{}",
                String::from_utf8_lossy(&out.stderr),
            );
            let expected_enc = if encrypt {
                "encrypted: true"
            } else {
                "encrypted: false"
            };
            assert!(
                stdout.contains(expected_enc),
                "compression={compression:?} encrypt={encrypt}: missing `{expected_enc}`; got:\n{stdout}",
            );
        }
    }
}
