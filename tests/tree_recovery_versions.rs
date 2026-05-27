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
    )?;
    for (name, payload) in sections {
        w.start(&name)?;
        w.write_all(&payload)?;
    }
    w.finish()?;

    // CURRENT pointer carries the manifest's section-bytes hash —
    // recompute it for the rewritten manifest so
    // `get_current_version` lets the test reach the version-policy
    // code path it actually wants to exercise (otherwise the hash
    // mismatch surfaces first as ChecksumMismatch).
    use byteorder::{ReadBytesExt as _, WriteBytesExt as _};
    use std::io::Seek as _;
    let file_len = std::fs::metadata(&manifest_path)?.len();
    let mut mf = std::fs::File::open(&manifest_path)?;
    mf.seek(std::io::SeekFrom::Start(
        file_len - lsm_tree::manifest_blocks::TAIL_FOOTER_SIZE_HINT_BYTES,
    ))?;
    let footer_size = u64::from(mf.read_u32::<byteorder::LittleEndian>()?);
    drop(mf);
    // Use the production constants so a future bump of the head
    // reservation or size-hint width doesn't quietly desync this
    // test fixture.
    let head_reservation = lsm_tree::manifest_blocks::HEAD_FOOTER_RESERVED_SIZE;
    let section_end =
        file_len - lsm_tree::manifest_blocks::TAIL_FOOTER_SIZE_HINT_BYTES - footer_size;
    let section_length = section_end.saturating_sub(head_reservation);
    let checksum = lsm_tree::file::hash_file_range_xxh3(
        &lsm_tree::fs::StdFs,
        &manifest_path,
        head_reservation,
        section_length,
    )?;
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
    // BuRR filter format): the block header gains an `ecc_length`
    // field and the block magic is bumped so a pre-V5 reader rejects
    // V5 blocks immediately at header decode. Pre-V5 tables on disk
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
    let folder = get_tmp_folder();

    let path = folder.path();

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        assert!(path.join("v0").try_exists()?);

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.version_free_list_len());
        assert!(path.join("v1").try_exists()?);

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(2, tree.version_free_list_len());
        assert!(path.join("v2").try_exists()?);
    }

    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        assert_eq!(0, tree.version_free_list_len());
        assert!(!path.join("v0").try_exists()?);
        assert!(!path.join("v1").try_exists()?);
        assert!(path.join("v2").try_exists()?);

        assert!(tree.contains_key("a", 1)?);
        assert!(tree.contains_key("b", 1)?);
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
/// reader path accepts `ecc_length = 0` blocks too (only validates
/// parity length when `ecc_length != 0`), so a regression that
/// silently emitted non-ECC blocks would still pass the round
/// trip. The strict on-disk check that locks down "writer MUST
/// emit non-zero `ecc_length`" lives in
/// [`tree_page_ecc_emits_nonzero_ecc_length_on_disk`] below.
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
        .open()?;

        tree.insert("k1", "v1", 0);
        tree.insert("k2", "v2", 1);
        tree.flush_active_memtable(0)?;

        assert_eq!(Some("v1".as_bytes().into()), tree.get("k1", 2)?);
        assert_eq!(Some("v2".as_bytes().into()), tree.get("k2", 2)?);
    }

    // Reopen with the same flag — the on-disk blocks MUST have
    // the parity trailer the writer is supposed to emit, otherwise
    // the read path (which verifies ecc_length matches
    // expected_parity_len(data_length)) would reject the block on
    // load and the reopen would fail.
    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .page_ecc(true)
        .open()?;

        assert_eq!(Some("v1".as_bytes().into()), tree.get("k1", 2)?);
        assert_eq!(Some("v2".as_bytes().into()), tree.get("k2", 2)?);
    }

    Ok(())
}

/// Strict on-disk check that `Config::page_ecc(true)` produces
/// SST data blocks with `Header::ecc_length > 0` (not just that
/// the round trip succeeds).
///
/// Locks down the writer wiring against silent regressions where
/// the flag would be accepted at `Tree::open` but never propagate
/// to the emit path — the round-trip test above would still pass
/// in that case because the reader accepts `ecc_length = 0` blocks,
/// so by itself it isn't sufficient. Reads the first
/// freshly-flushed SST file from disk, parses its SFA trailer to
/// locate the `data` section, then decodes the first `Header` and
/// asserts the parity trailer length is non-zero.
#[cfg(feature = "page_ecc")]
#[test]
fn tree_page_ecc_emits_nonzero_ecc_length_on_disk() -> lsm_tree::Result<()> {
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

    let mut data_reader = data_section.buf_reader(&sst_path)?;
    // Decode the FIRST data block's header. With `page_ecc(true)`
    // the writer must emit a non-zero `ecc_length` for every block
    // it produces.
    let header = Header::decode_from(&mut data_reader)?;
    assert!(
        header.ecc_length > 0,
        "page_ecc(true) tree must emit blocks with parity trailer, \
         got ecc_length=0 in first data block of {}",
        sst_path.display(),
    );
    // Defensive: also sanity-check that data_length is non-trivial
    // so we know we actually decoded a real block header (not a
    // zero-init buffer).
    assert!(
        header.data_length > 0,
        "first data block header should have non-zero data_length"
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

    Ok(())
}
