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
