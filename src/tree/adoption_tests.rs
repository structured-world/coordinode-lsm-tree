use super::fails_adoption;

/// A blob file of the latest flush whose own dictionary did not reach the
/// disk, its frames and index whole, leaves that flush out, and the open
/// reads without it. Guards against the flush being taken in on its frames
/// and the open then failing on the dictionary of a file only that flush
/// names.
#[cfg(zstd_any)]
#[test]
fn a_flush_whose_blob_dictionary_did_not_reach_the_disk_is_left_out() -> crate::Result<()> {
    use crate::fs::{CrashFs, Fault, FaultFs, FaultOp, FaultRule, Fs, FsOpenOptions, MemFs};
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use alloc::sync::Arc;

    let root = std::path::absolute("/db")?;
    let open = |fs: Arc<dyn Fs>| {
        Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs)
        .table_file_pool(0)
        .blob_compression(crate::CompressionType::Zstd(3))
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .separation_threshold(1)
                .file_dictionary(4 * 1_024),
        ))
        .open()
    };
    let value = |i: u32| format!("record {i:08} status=ok region=eu-west ").repeat(4);

    let crash = CrashFs::new(MemFs::new());
    let fault = Arc::new(FaultFs::new(crash.clone()));
    let tree = open(fault.clone())?;
    tree.insert("a", "kept", 0);
    tree.flush_active_memtable(0)?;
    tree.persist_flushes()?;
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    let before: Vec<String> = crash
        .read_dir(&blobs)?
        .into_iter()
        .map(|entry| entry.file_name)
        .collect();
    for i in 0..2_000u32 {
        tree.insert(format!("k{i:06}"), value(i), 1 + u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    let torn = crash
        .read_dir(&blobs)?
        .into_iter()
        .map(|entry| entry.file_name)
        .find(|name| !before.contains(name))
        .expect("self-check: the flush wrote a blob file");

    // The power goes: nothing the closing tree does reaches the disk.
    for op in [FaultOp::Open, FaultOp::RemoveFile, FaultOp::Rename] {
        fault.injector().arm(FaultRule::new(
            op,
            Fault::Error(crate::io::ErrorKind::Other),
        ));
    }
    crash.crash();
    drop(tree);

    // Its dictionary's bytes are lost, its frames and index whole.
    let path = blobs.join(&torn);
    let (pos, len) = {
        let mut file = crash.open(&path, &FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        let section = reader
            .toc()
            .section(b"dict")
            .expect("self-check: the blob file stores its own dictionary");
        (section.pos(), section.len())
    };
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = crash.open(&path, &FsOpenOptions::new().write(true))?;
        file.seek(SeekFrom::Start(pos))?;
        file.write_all(&vec![0; usize::try_from(len).expect("fits")])?;
        file.sync_all()?;
    }

    let tree = open(Arc::new(crash))?;
    assert_eq!(
        tree.get("k000000", u64::MAX)?,
        None,
        "the flush is left out"
    );
    assert_eq!(tree.get("a", u64::MAX)?.as_deref(), Some(&b"kept"[..]));
    Ok(())
}

/// A flush table whose blob links rotted into the id of a blob file the
/// manifest already names, its blocks whole, leaves that flush out. Guards
/// against the flush being taken in on its blocks alone: its own blob file
/// then looks orphaned and is deleted, and the table's values with it.
#[test]
fn a_flush_whose_blob_links_rotted_is_left_out() -> crate::Result<()> {
    use crate::fs::{CrashFs, Fault, FaultFs, FaultOp, FaultRule, Fs, FsOpenOptions, MemFs};
    use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use alloc::sync::Arc;
    use std::io::{Read, Seek, SeekFrom, Write};

    let root = std::path::absolute("/db")?;
    let open = |fs: Arc<dyn Fs>| {
        Config::new(
            &root,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(fs)
        .table_file_pool(0)
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()
    };
    let names = |fs: &CrashFs, folder: &std::path::Path| -> crate::Result<Vec<String>> {
        Ok(fs
            .read_dir(folder)?
            .into_iter()
            .map(|entry| entry.file_name)
            .collect())
    };
    let blobs = root.join(crate::file::BLOBS_FOLDER);
    let tables = root.join(crate::file::TABLES_FOLDER);

    let crash = CrashFs::new(MemFs::new());
    let fault = Arc::new(FaultFs::new(crash.clone()));
    let tree = open(fault.clone())?;
    tree.insert("a", "kept", 0);
    tree.flush_active_memtable(0)?;
    tree.persist_flushes()?;
    let Some(named) = names(&crash, &blobs)?
        .first()
        .and_then(|name| name.parse::<u64>().ok())
    else {
        panic!("self-check: the first flush wrote a blob file");
    };
    let tables_before = names(&crash, &tables)?;
    tree.insert("b", "lost", 1);
    tree.flush_active_memtable(0)?;
    let Some(table) = names(&crash, &tables)?
        .into_iter()
        .find(|name| !tables_before.contains(name))
    else {
        panic!("self-check: the second flush wrote a table");
    };

    // The power goes: nothing the closing tree does reaches the disk.
    for op in [FaultOp::Open, FaultOp::RemoveFile, FaultOp::Rename] {
        fault.injector().arm(FaultRule::new(
            op,
            Fault::Error(crate::io::ErrorKind::Other),
        ));
    }
    crash.crash();
    drop(tree);

    // The table's one link now names the manifest's blob file.
    let path = tables.join(&table);
    let pos = {
        let mut file = crash.open(&path, &FsOpenOptions::new().read(true))?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        let Some(section) = reader.toc().section(b"linked_blob_files") else {
            panic!("self-check: the table links its blob file");
        };
        section.pos()
    };
    {
        let mut file = crash.open(&path, &FsOpenOptions::new().read(true).write(true))?;
        file.seek(SeekFrom::Start(pos + 4))?;
        let mut own = [0; 8];
        file.read_exact(&mut own)?;
        assert_ne!(u64::from_le_bytes(own), named, "self-check: two blob files");
        file.seek(SeekFrom::Start(pos + 4))?;
        file.write_all(&named.to_le_bytes())?;
        file.sync_all()?;
    }

    let tree = open(Arc::new(crash))?;
    assert_eq!(tree.get("b", u64::MAX)?, None, "the flush is left out");
    assert_eq!(tree.get("a", u64::MAX)?.as_deref(), Some(&b"kept"[..]));
    Ok(())
}

/// A flush that collected history and is installed after the sequence
/// counter moved on keeps one retention floor: the one its table describes,
/// which an open restores after a power loss. Guards against the live tree
/// refusing a snapshot the reopened one serves.
#[test]
fn a_flush_keeps_its_floor_across_a_power_loss() -> crate::Result<()> {
    use crate::fs::{CrashFs, Fault, FaultFs, FaultOp, FaultRule, Fs, MemFs};
    use crate::{AbstractTree, Config, SequenceNumberCounter};
    use alloc::sync::Arc;

    let root = std::path::absolute("/db")?;
    let open = |fs: Arc<dyn Fs>, seqno: SequenceNumberCounter| {
        Config::new(&root, seqno, SequenceNumberCounter::default())
            .with_shared_fs(fs)
            .table_file_pool(0)
            .open()
    };

    let crash = CrashFs::new(MemFs::new());
    let fault = Arc::new(FaultFs::new(crash.clone()));
    let seqno = SequenceNumberCounter::default();
    let tree = open(fault.clone(), seqno.clone())?;
    for value in ["old", "new"] {
        tree.insert("k", value, seqno.next());
    }
    // Writers move the counter on while the flush's files are synced.
    tree.tree_config().arm_before_output_install(move || {
        for _ in 0..5 {
            let _ = seqno.next();
        }
    });
    tree.flush_active_memtable(100)?;
    let live = tree.oldest_retained_seqno();

    // The power goes: nothing the closing tree does reaches the disk.
    for op in [FaultOp::Open, FaultOp::RemoveFile, FaultOp::Rename] {
        fault.injector().arm(FaultRule::new(
            op,
            Fault::Error(crate::io::ErrorKind::Other),
        ));
    }
    crash.crash();
    drop(tree);

    let tree = open(Arc::new(crash), SequenceNumberCounter::default())?;
    assert_eq!(tree.oldest_retained_seqno(), live);
    Ok(())
}

/// A table this build cannot read for lack of a feature (a columnar table
/// opened without `columnar`) fails the open: its bytes are whole. Guards
/// against such a table being taken for a cut-short flush, its tables set
/// aside and the watermark raised past them, so that not even a build with
/// the feature takes them in afterwards.
#[test]
fn a_missing_feature_fails_the_open_rather_than_set_a_flush_aside() {
    assert!(fails_adoption(&crate::Error::FeatureUnsupported(
        "columnar"
    )));
}

/// Damaged bytes tell that a flush does not read whole: the open goes on
/// without it.
#[test]
fn damaged_bytes_do_not_fail_the_open() {
    assert!(!fails_adoption(&crate::Error::Unrecoverable));
}
