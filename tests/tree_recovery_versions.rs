use byteorder::{LittleEndian, ReadBytesExt};
use lsm_tree::{AbstractTree, Config, SequenceNumberCounter, get_tmp_folder};
use std::{fs::File, path::Path};
use test_log::test;

/// Read the `format_version` byte from the current manifest by
/// opening it through the (private) `ManifestArchiveReader` test
/// entry point. The byte sits inside a `BlockType::Manifest` Block
/// whose XXH3 covers it; the reader does the verification + payload
/// extraction so the test cannot accidentally observe a corrupted
/// byte that the Block layer would have rejected.
fn read_manifest_format_version(path: &Path) -> lsm_tree::Result<u8> {
    let curr_version_id = File::open(path.join("current"))?.read_u64::<LittleEndian>()?;
    let manifest_path = path.join(format!("v{curr_version_id}"));
    let mut archive = lsm_tree::manifest_blocks::reader::ManifestArchiveReader::open(
        &manifest_path,
        &lsm_tree::fs::StdFs,
        std::sync::Arc::new(lsm_tree::runtime_config::RuntimeConfig::default()),
        None,
    )?;
    let bytes = archive.read_section("format_version")?;
    #[expect(
        clippy::expect_used,
        reason = "test fixture should contain format_version"
    )]
    Ok(*bytes.first().expect("format_version section is non-empty"))
}

/// Overwrite the `format_version` section of the current manifest by
/// constructing a fresh Blocks-based manifest at the same path. Used
/// by the version-rejection tests to land a manifest with an
/// arbitrary `format_version` byte where the surrounding Block
/// remains valid so the rejection surfaces at the version-policy
/// layer rather than at the Block-XXH3 layer.
fn rewrite_manifest_format_version(path: &Path, version: u8) -> lsm_tree::Result<()> {
    use std::io::Write;
    let curr_version_id = File::open(path.join("current"))?.read_u64::<LittleEndian>()?;
    let manifest_path = path.join(format!("v{curr_version_id}"));

    // Reconstruct: read every existing section, drop the file, then
    // rewrite with the same sections except `format_version` carries
    // the requested byte. Keeps the on-disk surface valid (Block
    // XXH3 over each section) while letting the test target the
    // version-policy code path specifically.
    let mut archive = lsm_tree::manifest_blocks::reader::ManifestArchiveReader::open(
        &manifest_path,
        &lsm_tree::fs::StdFs,
        std::sync::Arc::new(lsm_tree::runtime_config::RuntimeConfig::default()),
        None,
    )?;
    let mut sections: Vec<(String, Vec<u8>)> = Vec::new();
    for entry in archive.footer().sections.clone() {
        let payload = if entry.name == "format_version" {
            vec![version]
        } else {
            archive.read_section(&entry.name)?
        };
        sections.push((entry.name, payload));
    }
    drop(archive);
    std::fs::remove_file(&manifest_path)?;

    let mut w = lsm_tree::manifest_blocks::writer::ManifestArchiveWriter::create(
        &manifest_path,
        &lsm_tree::fs::StdFs,
        std::sync::Arc::new(lsm_tree::runtime_config::RuntimeConfig::default()),
        None,
        lsm_tree::fs::SyncMode::Normal,
    )?;
    for (name, payload) in sections {
        w.start(&name)?;
        w.write_all(&payload)?;
    }
    w.finish()?;

    // CURRENT pointer carries the canonical digest over the
    // manifest's parsed footer (TOC + per-section XXH3-128s) —
    // recompute it from the rewritten manifest so
    // `get_current_version` lets the test reach the version-policy
    // code path it actually wants to exercise (otherwise the digest
    // mismatch surfaces first as ChecksumMismatch).
    use byteorder::WriteBytesExt as _;
    let archive = lsm_tree::manifest_blocks::reader::ManifestArchiveReader::open(
        &manifest_path,
        &lsm_tree::fs::StdFs,
        std::sync::Arc::new(lsm_tree::runtime_config::RuntimeConfig::default()),
        None,
    )?;
    let checksum =
        lsm_tree::manifest_blocks::current_digest::compute(curr_version_id, archive.footer())?;
    let mut content: Vec<u8> = Vec::new();
    content.write_u64::<byteorder::LittleEndian>(curr_version_id)?;
    content.write_u128::<byteorder::LittleEndian>(checksum)?;
    content.write_u8(0)?; // xxh3
    let current_path = path.join("current");
    let mut current_file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(&current_path)?;
    current_file.write_all(&content)?;
    current_file.sync_all()?;

    Ok(())
}

#[test]
fn tree_writes_v5_manifest_and_recovers_it() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(5, read_manifest_format_version(path)?);
    }

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        assert_eq!(Some("a".as_bytes().into()), tree.get("a", 1)?);
        assert_eq!(5, read_manifest_format_version(path)?);
    }

    Ok(())
}

#[test]
fn tree_rejects_pre_v5_manifest() -> lsm_tree::Result<()> {
    // V5 introduces per-block Reed-Solomon Page ECC (alongside the
    // BuRR filter format): the block header gains a `block_flags` byte
    // (whose ECC_PARITY bit marks a parity trailer) and the block magic
    // is bumped so a pre-V5 reader rejects V5 blocks immediately at
    // header decode. Pre-V5 tables on disk
    // cannot be read by this version and vice versa — opening a
    // manifest tagged with any pre-V5 version must fail with
    // InvalidVersion at recovery time rather than silently
    // misreading block bytes later. We assert V3 AND V4 explicitly
    // so the boundary stays exact and a future "accept V4 if …"
    // relaxation lights up the test rather than passing quietly.
    for pre_v5 in [3_u8, 4_u8] {
        let folder = get_tmp_folder();
        let path = folder.path();

        {
            let tree = Config::new(
                path,
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .open()?;

            tree.insert("a", "a", 0);
            tree.flush_active_memtable(0)?;

            assert_eq!(5, read_manifest_format_version(path)?);
            rewrite_manifest_format_version(path, pre_v5)?;
            assert_eq!(pre_v5, read_manifest_format_version(path)?);
        }

        let reopened = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open();

        match reopened {
            Err(lsm_tree::Error::InvalidVersion(v)) => {
                assert_eq!(
                    v, pre_v5,
                    "V{pre_v5} manifest must be rejected with the right version",
                );
            }
            Err(other) => panic!("expected InvalidVersion({pre_v5}), got: {other:?}"),
            Ok(_) => panic!("V{pre_v5} manifest must be rejected by V5 binary"),
        }
    }

    Ok(())
}

#[test]
fn tree_rejects_unsupported_manifest_version() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(5, read_manifest_format_version(path)?);
        rewrite_manifest_format_version(path, 99)?;
        assert_eq!(99, read_manifest_format_version(path)?);
    }

    let reopened = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open();

    match reopened {
        Err(lsm_tree::Error::InvalidVersion(v)) => {
            assert_eq!(v, 99, "rejected version should match the tampered value");
        }
        Err(other) => panic!("expected InvalidVersion(99), got: {other:?}"),
        Ok(_) => panic!("unsupported manifest version must be rejected"),
    }

    Ok(())
}

#[test]
fn tree_recovery_version_free_list() -> lsm_tree::Result<()> {
    // Under the incremental manifest a version upgrade appends a VersionEdit to
    // the snapshot's `edits-{snapshot_id}` log rather than writing a full
    // `v{id}` per version. So only the snapshot file (`v0`, which CURRENT points
    // at) exists on disk; intermediate versions live in the log. The in-memory
    // free list still tracks every version for MVCC. On reopen the snapshot is
    // loaded and the log replayed, and orphan-cleanup keeps exactly that one
    // snapshot generation (`v0` + `edits-0`).
    let folder = get_tmp_folder();

    let path = folder.path();

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        assert!(
            path.join("v0").try_exists()?,
            "create writes the v0 snapshot"
        );

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.version_free_list_len());
        // The flush appended an edit to the log, not a new snapshot file.
        assert!(
            path.join("edits-0").try_exists()?,
            "first upgrade appends to the snapshot's edit log"
        );
        assert!(
            !path.join("v1").try_exists()?,
            "no per-version snapshot file is written on append"
        );

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(2, tree.version_free_list_len());
        assert!(
            !path.join("v2").try_exists()?,
            "second upgrade also appends, no v2 snapshot"
        );
    }

    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        assert_eq!(0, tree.version_free_list_len());
        // CURRENT still points at the v0 snapshot, with its edit log layered on
        // top — both survive orphan cleanup; there is no v1/v2 to clean.
        assert!(
            path.join("v0").try_exists()?,
            "the snapshot CURRENT references is preserved across reopen"
        );
        assert!(
            path.join("edits-0").try_exists()?,
            "its edit log is preserved"
        );
        assert!(!path.join("v1").try_exists()?);
        assert!(!path.join("v2").try_exists()?);

        // The replayed edits reconstruct both flushed tables.
        assert!(tree.contains_key("a", 1)?);
        assert!(tree.contains_key("b", 1)?);
    }

    Ok(())
}

/// Crash safety of the incremental manifest: a power-loss-truncated trailing
/// edit in the log is dropped on recovery, and the tree recovers exactly the
/// durable prefix — the state after the last fully-fsynced edit.
///
/// Two flushes append two edits to `edits-0`. Truncating the file mid-second
/// record simulates a crash between the second flush's edit being partially
/// written and its fsync completing. On reopen the snapshot (`v0`, empty) plus
/// the first edit are replayed; the torn second edit is discarded. So `a` (first
/// flush) is present and `b` (second flush) is gone — the second flush was never
/// durably committed to the manifest, which is the contract: an unacknowledged
/// write may be lost, but the recovered state is always internally consistent.
#[test]
fn tree_recovers_durable_prefix_when_edit_log_tail_is_torn() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let clean_after_first;
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        // Size of the log after exactly one durable edit — the boundary the torn
        // tail must collapse back to.
        clean_after_first = std::fs::metadata(path.join("edits-0"))?.len();

        tree.insert("b", "b", 1);
        tree.flush_active_memtable(1)?;
    }

    // The second edit made the log strictly longer; chop part of it off so the
    // trailing record is incomplete (a framing-checksum / truncation failure on
    // replay, not a clean record boundary).
    let full = std::fs::metadata(path.join("edits-0"))?.len();
    assert!(
        full > clean_after_first,
        "second flush must have appended a second edit ({full} > {clean_after_first})"
    );
    let torn_len = clean_after_first + (full - clean_after_first) / 2;
    let log = std::fs::OpenOptions::new()
        .write(true)
        .open(path.join("edits-0"))?;
    log.set_len(torn_len)?;
    log.sync_all()?;
    drop(log);

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        assert!(
            tree.contains_key("a", 2)?,
            "the first flush's edit was durable — its key must survive"
        );
        assert!(
            !tree.contains_key("b", 2)?,
            "the second flush's edit was torn off — its key must be dropped"
        );
    }

    Ok(())
}

/// Rotation: with a tiny rotate threshold every upgrade after the first writes
/// a fresh full snapshot, repoints CURRENT, and garbage-collects the previous
/// snapshot + its edit log — leaving exactly one live `v{id}` generation. After
/// several rotating flushes the tree reopens cleanly and all keys read back.
///
/// `manifest_log_rotate_bytes(0)` forces a rotation on every upgrade (any
/// non-empty log already exceeds 0), so this exercises the rotation path — and
/// its old-generation cleanup — cheaply, without writing a megabyte of edits.
#[test]
fn tree_rotates_snapshot_and_gcs_old_generation() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .manifest_log_rotate_bytes(0)
        .open()?;

        // Threshold 0 means `log_size < 0` is never true, so every upgrade takes
        // the rotation branch. Each rotation must leave only one snapshot file.
        for i in 0u64..4 {
            tree.insert(format!("k{i}"), format!("v{i}"), i);
            tree.flush_active_memtable(i)?;

            // Exactly one `v{id}` snapshot file exists at any time after a
            // rotation (the old one is deleted once CURRENT is repointed).
            let snapshots = std::fs::read_dir(path)?
                .filter_map(Result::ok)
                .filter(|e| {
                    let n = e.file_name();
                    let n = n.to_string_lossy();
                    n.starts_with('v') && n[1..].bytes().all(|c| c.is_ascii_digit())
                })
                .count();
            assert_eq!(
                snapshots, 1,
                "rotation must leave exactly one live snapshot file after flush {i}"
            );
        }
    }

    // Reopen: the single surviving snapshot is the full current state (its log is
    // empty just after a rotation), so every key reads back.
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        for i in 0u64..4 {
            assert_eq!(
                Some(format!("v{i}").as_bytes().into()),
                tree.get(format!("k{i}"), 100)?,
                "key k{i} must survive rotation + reopen"
            );
        }
    }

    Ok(())
}

/// Regression test for the runtime gate at `Tree::open`: a tree
/// opened with `Config::page_ecc(true)` on a build that lacks the
/// `page_ecc` cargo feature must fail with
/// [`lsm_tree::Error::PageEccUnsupported`]. Locks down the
/// build-time feature isolation — without this gate, the tree
/// would open with `Config::page_ecc(true)` accepted but never
/// actually emit parity (the `BlockTransform::*Ecc` variants do
/// not exist on a `--no-features` build), silently downgrading
/// the integrity guarantee the caller asked for.
/// Round-trip + reopen sanity for `Config::page_ecc(true)`:
/// inserts, flushes, reads back, reopens, reads back again. With
/// the writer wiring correct, the read path verifies the parity
/// trailer internally on every block load and returns the
/// original plaintext.
///
/// NOTE: this test alone is necessary but not sufficient — the
/// reader path accepts blocks without a parity trailer too (the
/// `ECC_PARITY` flag clear means none follows), so a regression that
/// silently emitted non-ECC blocks would still pass the round
/// trip. The strict on-disk check that locks down "writer MUST
/// emit a parity trailer" lives in
/// [`tree_page_ecc_emits_parity_trailer_on_disk`] below.
#[cfg(feature = "page_ecc")]
#[test]
fn tree_page_ecc_roundtrips_through_flush_and_reopen() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .page_ecc(true)
        .ecc_scheme(lsm_tree::runtime_config::EccScheme::ReedSolomon {
            data_shards: 4,
            parity_shards: 2,
        })
        .open()?;

        tree.insert("k1", "v1", 0);
        tree.insert("k2", "v2", 1);
        tree.flush_active_memtable(0)?;

        assert_eq!(Some("v1".as_bytes().into()), tree.get("k1", 2)?);
        assert_eq!(Some("v2".as_bytes().into()), tree.get("k2", 2)?);
    }

    // Reopen with the same flag — the on-disk blocks MUST have
    // the parity trailer the writer is supposed to emit, otherwise
    // the read path (which derives the trailer length from
    // expected_parity_len(data_length) whenever the ECC_PARITY flag is
    // set) would mis-size the block on load and the reopen would fail.
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .page_ecc(true)
        .ecc_scheme(lsm_tree::runtime_config::EccScheme::ReedSolomon {
            data_shards: 4,
            parity_shards: 2,
        })
        .open()?;

        assert_eq!(Some("v1".as_bytes().into()), tree.get("k1", 2)?);
        assert_eq!(Some("v2".as_bytes().into()), tree.get("k2", 2)?);
    }

    Ok(())
}

/// A NON-default ECC scheme (XOR single-parity over 8 data shards)
/// round-trips end-to-end through the per-SST descriptor.
///
/// The writer records the chosen scheme in `descriptor#page_ecc`; the
/// reader re-derives the parity layout from that descriptor, NOT from
/// the runtime config. To prove this, the reopen below uses a DEFAULT
/// config (no `page_ecc`): the only way the blocks read back correctly
/// is if the reader sized + skipped the XOR(8,1) parity trailer using
/// the on-disk descriptor. A wrong scheme would mis-size the trailer
/// and mis-align the next block on load. This is the "flexible config"
/// acceptance: an arbitrary scheme, not the legacy RS(4,2), works.
#[cfg(feature = "page_ecc")]
#[test]
fn tree_page_ecc_nondefault_scheme_roundtrips_via_descriptor() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .page_ecc(true)
        .ecc_scheme(lsm_tree::runtime_config::EccScheme::ReedSolomon {
            data_shards: 8,
            parity_shards: 2,
        })
        .open()?;

        for i in 0u64..2_000 {
            tree.insert(format!("k{i:08}"), format!("v{i:08}"), i);
        }
        tree.flush_active_memtable(2_000)?;
    }

    // Reopen and read every key back. The blocks were written with a
    // non-default RS(8,2) scheme (25% overhead, two-shard tolerance); the
    // reader must size + skip each block's parity trailer using the
    // per-SST descriptor scheme. A wrong scheme would mis-size the trailer
    // and fail the block load on recovery — this is the "flexible config"
    // acceptance: an arbitrary scheme, not just the legacy RS(4,2), works
    // end-to-end through the descriptor.
    {
        // Reopen with a DEFAULT config: no `page_ecc`, no `ecc_scheme`. The
        // reader must source the RS(8,2) layout from the on-disk descriptor,
        // NOT from the runtime config — so a reopen that omits the ECC config
        // entirely is the load-bearing proof. If the reader fell back to the
        // (now default-off) runtime config, it would mis-size the parity
        // trailer and fail the block load.
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        for i in 0u64..2_000 {
            assert_eq!(
                Some(format!("v{i:08}").as_bytes().into()),
                tree.get(format!("k{i:08}"), 2_001)?,
                "key k{i:08} must read back under the on-disk RS(8,2) scheme",
            );
        }
    }

    Ok(())
}

/// The XOR single-parity scheme (RAID-5, `parity_shards == 1`) round-trips
/// end-to-end through the per-SST descriptor.
///
/// XOR takes a different write path than Reed-Solomon: the writer maps it to
/// the `Xor` descriptor kind (not `ReedSolomon`) and the codec computes parity
/// directly without the RS engine. Reopening with a default config proves the
/// reader sources the XOR(8,1) layout from the descriptor, not the runtime
/// config.
#[cfg(feature = "page_ecc")]
#[test]
fn tree_page_ecc_xor_scheme_roundtrips_via_descriptor() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .page_ecc(true)
        .ecc_scheme(lsm_tree::runtime_config::EccScheme::Xor { data_shards: 8 })
        .open()?;

        for i in 0u64..2_000 {
            tree.insert(format!("k{i:08}"), format!("v{i:08}"), i);
        }
        tree.flush_active_memtable(2_000)?;
    }

    {
        // Default config reopen: the reader must size + skip each block's
        // single XOR parity shard from the on-disk descriptor.
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        for i in 0u64..2_000 {
            assert_eq!(
                Some(format!("v{i:08}").as_bytes().into()),
                tree.get(format!("k{i:08}"), 2_001)?,
                "key k{i:08} must read back under the on-disk XOR(8,1) scheme",
            );
        }
    }

    Ok(())
}

/// Strict on-disk check that `Config::page_ecc(true)` produces
/// SST data blocks carrying a parity trailer (not just that the round
/// trip succeeds).
///
/// Locks down the writer wiring against silent regressions where
/// the flag would be accepted at `Tree::open` but never propagate
/// to the emit path — the round-trip test above would still pass
/// in that case because the reader accepts blocks without parity,
/// so by itself it isn't sufficient. Reads the first
/// freshly-flushed SST file from disk, parses its SFA trailer to
/// locate the `data` section, then decodes the first `Header` and
/// asserts the derived on-disk size exceeds header + payload (i.e. a
/// parity trailer is present). The parity LENGTH is not stored; it is
/// derived from `data_length` + the `ECC_PARITY` flag, so
/// `on_disk_size()` is the public way to observe the trailer.
#[cfg(feature = "page_ecc")]
#[test]
fn tree_page_ecc_emits_parity_trailer_on_disk() -> lsm_tree::Result<()> {
    use lsm_tree::coding::Decode;
    use lsm_tree::table::block::Header;
    use std::fs;

    let folder = get_tmp_folder();
    let path = folder.path();

    let tree = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .page_ecc(true)
    .ecc_scheme(lsm_tree::runtime_config::EccScheme::ReedSolomon {
        data_shards: 4,
        parity_shards: 2,
    })
    .open()?;

    tree.insert("a", "alpha", 0);
    tree.insert("b", "bravo", 1);
    tree.flush_active_memtable(0)?;

    // Find the freshly-flushed SST. The default layout writes
    // table files into `<path>/tables/<table_id>`; numeric file
    // names only (the layout invariant the rest of the crate
    // relies on too).
    let tables_dir = path.join("tables");
    let mut sst_path: Option<std::path::PathBuf> = None;
    for entry in fs::read_dir(&tables_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_string_lossy().chars().all(|c| c.is_ascii_digit()) {
            sst_path = Some(entry.path());
            break;
        }
    }
    #[expect(
        clippy::expect_used,
        reason = "test asserts a fresh SST exists after flush"
    )]
    let sst_path = sst_path.expect("flush should produce at least one SST file");

    let reader = lsm_tree::sfa::Reader::new(&sst_path)?;
    #[expect(
        clippy::expect_used,
        reason = "every lsm-tree SST has a `data` section"
    )]
    let data_section = reader
        .toc()
        .section(b"data")
        .expect("data section must exist in a valid SST");

    use std::io::Read as _;
    let mut section_bytes = Vec::new();
    data_section
        .buf_reader(&sst_path)?
        .read_to_end(&mut section_bytes)?;

    // Decode the FIRST data block's header. SST data blocks omit the
    // `block_flags` byte, so the header alone no longer carries the
    // ECC_PARITY bit — parity presence is a per-SST descriptor property and
    // is not derivable from the header in isolation.
    let mut cursor = &section_bytes[..];
    let header = Header::decode_from(&mut cursor)?;
    // Sanity-check that data_length is non-trivial so we know we decoded a
    // real block header (not a zero-init buffer).
    assert!(
        header.data_length > 0,
        "first data block header should have non-zero data_length"
    );

    // A block written WITHOUT a parity trailer occupies exactly
    // `MIN_LEN + data_length` bytes. With page_ecc(true) the writer appends a
    // Reed-Solomon parity trailer, so the bytes immediately after the first
    // block's payload are parity — and decoding a `Header` there MUST fail
    // (parity is not a valid block). This is robust to a fixture that ever
    // spills more than one data block: a second block WOULD decode as a valid
    // header at this offset, so the failure also proves the single-block
    // premise a bare section-length comparison would silently depend on.
    let after_first_payload = Header::MIN_LEN + header.data_length as usize;
    assert!(
        section_bytes.len() > after_first_payload,
        "page_ecc(true) tree must emit a parity trailer after the first data \
         block, got none in {}",
        sst_path.display(),
    );
    let mut trailing = section_bytes.get(after_first_payload..).unwrap_or(&[]);
    assert!(
        Header::decode_from(&mut trailing).is_err(),
        "bytes after the first data block's payload must be a parity trailer. A \
         successful Header decode here most likely means the parity trailer was \
         NOT emitted (a page_ecc regression); less likely, the fixture spilled a \
         second data block (which would invalidate this single-block assertion).",
    );

    Ok(())
}

/// The on-disk parity overhead matches the CONFIGURED scheme, not a fixed
/// layout: a single-block SST written with `Xor { data_shards: 8 }` carries
/// exactly one XOR parity shard whose length follows the scheme's formula
/// (`shard_bytes = ceil(N / 8)` rounded up to even). This pins the acceptance
/// criterion "measured parity overhead matches the configured scheme".
#[cfg(feature = "page_ecc")]
#[test]
fn tree_page_ecc_parity_overhead_matches_scheme() -> lsm_tree::Result<()> {
    use lsm_tree::coding::Decode;
    use lsm_tree::table::block::Header;
    use std::fs;
    use std::io::Read as _;

    let folder = get_tmp_folder();
    let path = folder.path();
    const DATA_SHARDS: usize = 8;
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .page_ecc(true)
        .ecc_scheme(lsm_tree::runtime_config::EccScheme::Xor {
            data_shards: DATA_SHARDS as u8,
        })
        .open()?;
        // A handful of entries → a single small data block.
        tree.insert("a", "alpha", 0);
        tree.insert("b", "bravo", 1);
        tree.flush_active_memtable(0)?;
    }

    let tables_dir = path.join("tables");
    let mut sst_path: Option<std::path::PathBuf> = None;
    for entry in fs::read_dir(&tables_dir)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .chars()
            .all(|c| c.is_ascii_digit())
        {
            sst_path = Some(entry.path());
            break;
        }
    }
    let sst_path = sst_path.unwrap_or_else(|| panic!("flush should produce an SST"));

    let reader = lsm_tree::sfa::Reader::new(&sst_path)?;
    let data_section = reader
        .toc()
        .section(b"data")
        .unwrap_or_else(|| panic!("data section must exist"));
    let mut section_bytes = Vec::new();
    data_section
        .buf_reader(&sst_path)?
        .read_to_end(&mut section_bytes)?;

    let mut cursor = &section_bytes[..];
    let header = Header::decode_from(&mut cursor)?;
    let data_len = header.data_length as usize;
    assert!(data_len > 0, "decoded a real data block");

    // Single block ⇒ everything after `header + payload` is the parity trailer.
    let parity_bytes = section_bytes
        .len()
        .checked_sub(Header::MIN_LEN + data_len)
        .unwrap_or_else(|| panic!("section shorter than header + payload"));

    // Xor{D} writes exactly one parity shard: shard_bytes = ceil(N / D) rounded
    // up to even. This is the scheme's overhead — assert it byte-for-byte.
    let ceil = data_len.div_ceil(DATA_SHARDS);
    let expected_parity = if ceil.is_multiple_of(2) {
        ceil
    } else {
        ceil + 1
    };
    assert_eq!(
        parity_bytes, expected_parity,
        "Xor({DATA_SHARDS}) on-disk parity ({parity_bytes} B) must equal the \
         scheme's shard size ({expected_parity} B) for data_length {data_len}",
    );

    Ok(())
}

#[cfg(not(feature = "page_ecc"))]
#[test]
fn tree_open_with_page_ecc_on_feature_off_build_errors() {
    let folder = get_tmp_folder();
    let path = folder.path();

    let result = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .page_ecc(true)
    .open();

    match result {
        Ok(_) => panic!(
            "Config::page_ecc(true) on a build without the page_ecc \
             feature must NOT open — that would silently downgrade \
             integrity, since the BlockTransform::*Ecc variants \
             don't exist in this build"
        ),
        Err(lsm_tree::Error::PageEccUnsupported) => {}
        Err(e) => panic!("expected PageEccUnsupported, got {e:?}"),
    }
}

#[test]
fn tree_open_with_missing_manifest_but_present_current_errors_not_recreates() -> lsm_tree::Result<()>
{
    // Regression: CURRENT pointing at a missing v{N} manifest file
    // is half-applied recovery / corruption, NOT a fresh-init
    // signal. Tree::open used to silently fall through its
    // `Err(Io(NotFound)) => create_new` arm in this state because
    // the manifest open inside `get_current_version` surfaced the
    // same NotFound the CURRENT-absent path uses — and the
    // has_existing_version_state probe didn't catch the case
    // because CURRENT itself was still present. Result: opening a
    // tree with a deleted manifest would clobber CURRENT with a
    // fresh v0 instead of erroring, turning a recoverable failure
    // into silent data loss. The fix in `get_current_version`
    // rewraps the manifest open's NotFound as
    // ManifestFooterInvalid so the outer Tree::open match never
    // mistakes it for "no CURRENT".
    let folder = get_tmp_folder();
    let path = folder.path();

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        tree.insert("k", "v", 0);
        tree.flush_active_memtable(0)?;
    }

    let curr_version_id = File::open(path.join("current"))?.read_u64::<LittleEndian>()?;
    let manifest_path = path.join(format!("v{curr_version_id}"));
    std::fs::remove_file(&manifest_path)?;

    let result = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open();

    match result {
        Ok(_) => panic!(
            "Tree::open with CURRENT present but manifest v{curr_version_id} deleted \
             MUST surface an error — silently re-creating from scratch would clobber \
             the user's CURRENT pointer and lose the half-applied recovery signal"
        ),
        Err(lsm_tree::Error::ManifestFooterInvalid(_)) => {}
        Err(e) => panic!("expected ManifestFooterInvalid for missing manifest, got {e:?}"),
    }

    // The whole point of the regression is preventing CURRENT
    // clobber. Erroring is necessary but not sufficient — a future
    // implementation could satisfy the error-variant check above
    // and still rewrite the pointer on its way out. Re-read CURRENT
    // and assert it still points at the original version.
    let still_current = File::open(path.join("current"))?.read_u64::<LittleEndian>()?;
    assert_eq!(
        still_current, curr_version_id,
        "failed Tree::open must not rewrite the CURRENT pointer"
    );

    Ok(())
}
