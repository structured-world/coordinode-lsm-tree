use byteorder::{LittleEndian, ReadBytesExt};
use lsm_tree::{AbstractTree, Config, SequenceNumberCounter, get_tmp_folder};
use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
    path::Path,
};
use test_log::test;

fn read_manifest_format_version(path: &Path) -> lsm_tree::Result<u8> {
    // read_u64 takes &mut self, but calling it on an owned File from `?` is
    // valid Rust — the compiler auto-borrows &mut on the owned temporary.
    let curr_version_id = File::open(path.join("current"))?.read_u64::<LittleEndian>()?;
    let manifest_path = path.join(format!("v{curr_version_id}"));
    let reader = sfa::Reader::new(&manifest_path)?;

    #[expect(
        clippy::expect_used,
        reason = "test fixture should contain format_version"
    )]
    let section = reader
        .toc()
        .section(b"format_version")
        .expect("format_version section should exist");

    Ok(section.buf_reader(&manifest_path)?.read_u8()?)
}

fn rewrite_manifest_format_version(path: &Path, version: u8) -> lsm_tree::Result<()> {
    let curr_version_id = File::open(path.join("current"))?.read_u64::<LittleEndian>()?;
    let manifest_path = path.join(format!("v{curr_version_id}"));
    let reader = sfa::Reader::new(&manifest_path)?;

    #[expect(
        clippy::expect_used,
        reason = "test fixture should contain format_version"
    )]
    let section = reader
        .toc()
        .section(b"format_version")
        .expect("format_version section should exist");

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&manifest_path)?;
    file.seek(SeekFrom::Start(section.pos()))?;
    file.write_all(&[version])?;
    file.flush()?;

    Ok(())
}

#[test]
fn tree_writes_v6_manifest_and_recovers_it() -> lsm_tree::Result<()> {
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

        assert_eq!(6, read_manifest_format_version(path)?);
    }

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        assert_eq!(Some("a".as_bytes().into()), tree.get("a", 1)?);
        assert_eq!(6, read_manifest_format_version(path)?);
    }

    Ok(())
}

#[test]
fn tree_rejects_pre_v6_manifest() -> lsm_tree::Result<()> {
    // V6 introduces per-block Reed-Solomon Page ECC: the block header
    // gains an `ecc_length` field and the block magic is bumped so a
    // pre-V6 reader rejects V6 blocks immediately at header decode.
    // Pre-V6 tables on disk cannot be read by this version and vice
    // versa — opening a manifest tagged with any pre-V6 version must
    // fail with InvalidVersion at recovery time rather than silently
    // misreading block bytes later. We assert V3, V4 AND V5 explicitly
    // so the boundary stays exact and a future "accept V5 if …"
    // relaxation lights up the test rather than passing quietly.
    for pre_v6 in [3_u8, 4_u8, 5_u8] {
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

            assert_eq!(6, read_manifest_format_version(path)?);
            rewrite_manifest_format_version(path, pre_v6)?;
            assert_eq!(pre_v6, read_manifest_format_version(path)?);
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
                    v, pre_v6,
                    "V{pre_v6} manifest must be rejected with the right version",
                );
            }
            Err(other) => panic!("expected InvalidVersion({pre_v6}), got: {other:?}"),
            Ok(_) => panic!("V{pre_v6} manifest must be rejected by V6 binary"),
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

        assert_eq!(6, read_manifest_format_version(path)?);
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

    let reader = sfa::Reader::new(&sst_path)?;
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
