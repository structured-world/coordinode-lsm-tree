// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov
//
// End-to-end tests for `AbstractTree::pin`: a point-in-time view that holds the
// tree's files in place without file I/O, and `Config::restore_pinned`, which
// rebuilds the tree from it elsewhere.

use lsm_tree::{
    AbstractTree, AnyTree, Config, Guard, KvSeparationOptions, PinnedVersion, SeqNo,
    SequenceNumberCounter,
    fs::{Fault, FaultFs, FaultInjector, FaultOp, FaultRule, StdFs},
    io::ErrorKind,
};
use std::{path::Path, sync::Arc};
use test_log::test;

/// A tree at `path`, a blob tree when `blob`, whose visible seqno `visible`
/// drives.
fn open(path: &Path, blob: bool, visible: &SequenceNumberCounter) -> lsm_tree::Result<AnyTree> {
    let config = Config::new(path, SequenceNumberCounter::default(), visible.clone());
    if blob {
        config
            .with_kv_separation(Some(
                KvSeparationOptions::default().separation_threshold(64),
            ))
            .open()
    } else {
        config.open()
    }
}

/// A value of `len` bytes that names `key` and `seqno`.
fn value(key: &str, seqno: SeqNo, len: usize) -> Vec<u8> {
    format!("{key}@{seqno}:").into_bytes().repeat(len / 8 + 1)
}

/// Writes a history into `tree`: tables with overwrites, tombstones and a range
/// tombstone, and a memtable holding more of each. Returns the next seqno.
fn write_history(tree: &AnyTree, visible: &SequenceNumberCounter) -> lsm_tree::Result<SeqNo> {
    let mut seqno = 0;
    let step = |seqno: &mut SeqNo| {
        *seqno += 1;
        visible.set(*seqno);
        *seqno - 1
    };
    for i in 0..40 {
        let key = format!("k{i:02}");
        let s = step(&mut seqno);
        tree.insert(key.as_str(), value(&key, s, 8 + i * 10), s);
    }
    tree.flush_active_memtable(0)?;
    for i in (0..40).step_by(3) {
        let key = format!("k{i:02}");
        let s = step(&mut seqno);
        tree.insert(key.as_str(), value(&key, s, 200), s);
    }
    let s = step(&mut seqno);
    tree.remove("k05", s);
    let s = step(&mut seqno);
    let _ = tree.remove_range("k30", "k33", s);
    tree.flush_active_memtable(0)?;
    // Left in the memtable: the pin seals it.
    for i in (1..40).step_by(4) {
        let key = format!("k{i:02}");
        let s = step(&mut seqno);
        tree.insert(key.as_str(), value(&key, s, 120), s);
    }
    let s = step(&mut seqno);
    tree.remove("k08", s);
    let s = step(&mut seqno);
    let _ = tree.remove_range("k20", "k22", s);
    Ok(seqno)
}

/// Every key and value `tree` reads at snapshot `seqno`.
fn read_at(tree: &AnyTree, seqno: SeqNo) -> lsm_tree::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    tree.iter(seqno, None)
        .map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.to_vec(), value.to_vec()))
        })
        .collect()
}

/// Copies the pin's files into the tree folders under `root`, as a consumer
/// streaming them would.
fn ship(pin: &PinnedVersion, root: &Path) -> lsm_tree::Result<()> {
    for (folder, files) in [
        ("tables", pin.tables()),
        ("blobs", pin.blob_files()),
        ("dicts", pin.dictionaries()),
    ] {
        // A folder the pin has no files for is left out, as a consumer would.
        if files.is_empty() {
            continue;
        }
        std::fs::create_dir_all(root.join(folder))?;
        for file in files {
            std::fs::copy(file.path(), root.join(folder).join(file.id().to_string()))?;
        }
    }
    Ok(())
}

/// The bytes of every file `pin` lists.
fn contents(pin: &PinnedVersion) -> lsm_tree::Result<Vec<(std::path::PathBuf, Vec<u8>)>> {
    pin.tables()
        .iter()
        .chain(pin.blob_files())
        .map(|file| Ok((file.path().to_path_buf(), std::fs::read(file.path())?)))
        .collect()
}

/// Waits for `path` to be removed; a removal may be handed to a background
/// deleter.
fn gone(path: &Path) -> bool {
    for _ in 0..500 {
        if !path.exists() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    false
}

/// Whether `tree`'s current version still lists `path`.
fn current(tree: &AnyTree, path: &Path) -> bool {
    let version = tree.current_version();
    version.iter_tables().any(|table| &**table.path == path)
        || version.blob_files.iter().any(|blob| blob.path() == path)
}

#[test]
fn pin_performs_no_file_io() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let injector = Arc::new(FaultInjector::new());
    let visible = SequenceNumberCounter::default();
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        visible.clone(),
    )
    .with_fs(FaultFs::with_injector(StdFs, Arc::clone(&injector)))
    .open()?;
    write_history(&tree, &visible)?;

    for op in [
        FaultOp::Open,
        FaultOp::CreateDir,
        FaultOp::CreateDirAll,
        FaultOp::RemoveFile,
        FaultOp::Rename,
        FaultOp::SyncDirectory,
        FaultOp::Write,
        FaultOp::Flush,
        FaultOp::SyncAll,
        FaultOp::SyncData,
        FaultOp::SetLen,
        FaultOp::HardLink,
        FaultOp::Reflink,
        FaultOp::PunchHole,
    ] {
        injector.arm(FaultRule::new(op, Fault::Error(ErrorKind::Other)));
    }
    let opens = injector.open_count();

    let pin = tree.pin();
    assert_eq!(injector.open_count(), opens, "the pin opened no file");
    assert_eq!(pin.tables().len(), 2);
    assert!(
        pin.memtable_entries().count() > 0,
        "the memtable was sealed"
    );
    assert!(!pin.layout()?.is_empty(), "the layout is built in memory");

    injector.clear();
    Ok(())
}

/// Every file a pin lists keeps its bytes while a flush and a major compaction
/// supersede it, and goes once the pin drops.
fn superseded_files_stay_until_the_pin_drops(blob: bool) -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let visible = SequenceNumberCounter::default();
    let tree = open(dir.path(), blob, &visible)?;
    let mut seqno = write_history(&tree, &visible)?;

    let pin = tree.pin();
    let pinned = contents(&pin)?;

    for i in 0..40 {
        let key = format!("k{i:02}");
        tree.insert(key.as_str(), value(&key, seqno, 300), seqno);
        seqno += 1;
    }
    visible.set(seqno);
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, seqno)?;

    let superseded: Vec<&Path> = pinned
        .iter()
        .map(|(path, _)| path.as_path())
        .filter(|path| !current(&tree, path))
        .collect();
    assert!(
        !superseded.is_empty(),
        "the compaction superseded pinned files"
    );
    for (path, bytes) in &pinned {
        assert_eq!(
            &std::fs::read(path)?,
            bytes,
            "{} changed under the pin",
            path.display()
        );
    }

    drop(pin);
    for path in superseded {
        assert!(gone(path), "{} outlived the pin", path.display());
    }
    Ok(())
}

#[test]
fn pinned_tables_stay_until_the_pin_drops() -> lsm_tree::Result<()> {
    superseded_files_stay_until_the_pin_drops(false)
}

#[test]
fn pinned_blob_files_stay_until_the_pin_drops() -> lsm_tree::Result<()> {
    superseded_files_stay_until_the_pin_drops(true)
}

/// A tree restored from a pin reads as the source did at the pin, for every
/// key and every snapshot up to the pin's seqno.
fn restore_matches_the_source(blob: bool) -> lsm_tree::Result<()> {
    let src = tempfile::tempdir()?;
    let dst = tempfile::tempdir()?;
    let visible = SequenceNumberCounter::default();
    let tree = open(src.path(), blob, &visible)?;
    let mut seqno = write_history(&tree, &visible)?;

    let pin = tree.pin();
    assert_eq!(pin.seqno(), seqno);
    let expected: Vec<_> = (0..=pin.seqno())
        .map(|at| read_at(&tree, at))
        .collect::<lsm_tree::Result<_>>()?;

    // The source moves on; the pin does not.
    for i in 0..10 {
        let key = format!("k{i:02}");
        tree.insert(key.as_str(), value(&key, seqno, 90), seqno);
        seqno += 1;
    }
    visible.set(seqno);
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;

    ship(&pin, dst.path())?;
    let restored_visible = SequenceNumberCounter::new(pin.seqno());
    let config = Config::new(
        dst.path(),
        SequenceNumberCounter::new(pin.seqno()),
        restored_visible.clone(),
    );
    let config = if blob {
        config.with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(64),
        ))
    } else {
        config
    };
    let layout = pin.layout()?;
    let restored = config.restore_pinned(
        &layout,
        pin.memtable_entries(),
        pin.memtable_range_tombstones(),
    )?;
    drop(pin);

    for (at, expected) in expected.iter().enumerate() {
        let at = SeqNo::try_from(at).unwrap_or(SeqNo::MAX);
        assert_eq!(&read_at(&restored, at)?, expected, "snapshot {at}");
        assert_eq!(&read_at(&tree, at)?, expected, "source snapshot {at}");
    }
    Ok(())
}

#[test]
fn restored_tree_reads_as_the_source_at_the_pin() -> lsm_tree::Result<()> {
    restore_matches_the_source(false)
}

#[test]
fn restored_blob_tree_reads_as_the_source_at_the_pin() -> lsm_tree::Result<()> {
    restore_matches_the_source(true)
}

/// A restore never publishes over a tree already in the target folder, closed
/// or open: it refuses before writing anything, and that tree stays as it was.
#[test]
fn restore_refuses_a_folder_holding_a_tree() -> lsm_tree::Result<()> {
    let src = tempfile::tempdir()?;
    let dst = tempfile::tempdir()?;
    let visible = SequenceNumberCounter::default();
    let tree = open(src.path(), false, &visible)?;
    write_history(&tree, &visible)?;
    let pin = tree.pin();
    let layout = pin.layout()?;

    {
        let existing = open(dst.path(), false, &SequenceNumberCounter::default())?;
        existing.insert("own", "kept", 0);
        existing.flush_active_memtable(0)?;
    }
    let restore = || {
        Config::new(
            dst.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .restore_pinned(&layout, [], [])
    };
    assert!(restore().is_err(), "a closed tree is refused");
    let existing = open(dst.path(), false, &SequenceNumberCounter::default())?;
    assert_eq!(
        existing.get("own", SeqNo::MAX)?.as_deref(),
        Some(&b"kept"[..])
    );

    // The manifest generation the open tree is on: its snapshots and logs.
    let manifest = || -> std::io::Result<Vec<(String, Vec<u8>)>> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dst.path())? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('v') || name.starts_with("edits-") {
                files.push((name, std::fs::read(entry.path())?));
            }
        }
        files.sort();
        Ok(files)
    };
    let before = manifest()?;
    assert!(restore().is_err(), "an open tree is refused");
    assert_eq!(manifest()?, before, "the open tree's manifest is untouched");
    Ok(())
}

/// A write made on the restored tree with its own counters lands above every
/// restored version, and reads back.
#[test]
fn restored_tree_writes_above_the_restored_data() -> lsm_tree::Result<()> {
    let src = tempfile::tempdir()?;
    let dst = tempfile::tempdir()?;
    let visible = SequenceNumberCounter::default();
    let tree = open(src.path(), false, &visible)?;
    write_history(&tree, &visible)?;
    let pin = tree.pin();
    ship(&pin, dst.path())?;

    let seqno = SequenceNumberCounter::default();
    let restored_visible = SequenceNumberCounter::default();
    let restored = Config::new(dst.path(), seqno.clone(), restored_visible.clone())
        .restore_pinned(
            &pin.layout()?,
            pin.memtable_entries(),
            pin.memtable_range_tombstones(),
        )?;
    assert!(
        restored_visible.get() >= pin.seqno(),
        "the restored writes are published"
    );

    let at = seqno.next();
    restored.insert("k01", "after", at);
    restored_visible.fetch_max(at + 1);
    assert_eq!(
        restored.get("k01", SeqNo::MAX)?.as_deref(),
        Some(&b"after"[..])
    );
    Ok(())
}

/// A pin keeps the source folder locked: the tree it came from may be dropped,
/// but no other opener gets the folder (and a registry of its own that knows
/// nothing of the pin) until the pin drops.
#[test]
fn a_pin_keeps_the_source_folder_locked() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let visible = SequenceNumberCounter::default();
    let tree = open(dir.path(), false, &visible)?;
    write_history(&tree, &visible)?;
    let pin = tree.pin();
    drop(tree);

    assert!(
        matches!(
            open(dir.path(), false, &SequenceNumberCounter::default()),
            Err(lsm_tree::Error::Locked(_))
        ),
        "the pin holds the folder"
    );
    drop(pin);
    open(dir.path(), false, &SequenceNumberCounter::default())?;
    Ok(())
}

/// A restore interrupted after it published the manifest but before the pinned
/// memtable contents were flushed leaves a folder an ordinary open refuses,
/// and a second restore finishes it.
#[test]
fn an_interrupted_restore_is_refused_and_resumed() -> lsm_tree::Result<()> {
    let src = tempfile::tempdir()?;
    let dst = tempfile::tempdir()?;
    let visible = SequenceNumberCounter::default();
    let tree = open(src.path(), false, &visible)?;
    write_history(&tree, &visible)?;
    let pin = tree.pin();
    let expected = read_at(&tree, pin.seqno())?;
    assert!(pin.memtable_entries().count() > 0);
    ship(&pin, dst.path())?;
    let layout = pin.layout()?;

    let injector = Arc::new(FaultInjector::new());
    // The edit log is created before the manifest is published and next
    // opened by the open that follows it: failing that open stops the restore
    // between the publication and the memtable flush.
    injector.arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Other))
            .on_path("edits-")
            .skip(1),
    );
    let interrupted = Config::new(
        dst.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(FaultFs::with_injector(StdFs, Arc::clone(&injector)))
    .restore_pinned(
        &layout,
        pin.memtable_entries(),
        pin.memtable_range_tombstones(),
    );
    assert!(interrupted.is_err(), "the fault stopped the restore");
    drop(interrupted);

    assert!(
        open(dst.path(), false, &SequenceNumberCounter::default()).is_err(),
        "an ordinary open does not serve the partial tree"
    );
    let restored = Config::new(
        dst.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .restore_pinned(
        &layout,
        pin.memtable_entries(),
        pin.memtable_range_tombstones(),
    )?;
    assert_eq!(read_at(&restored, pin.seqno())?, expected);
    Ok(())
}

/// A pinned file describes itself: a table has a level, a blob file none, the
/// size and the bytes read through the pin are the file's, and a table's heal
/// attestation is reported when one lies beside it.
#[test]
fn pinned_files_describe_themselves() -> lsm_tree::Result<()> {
    use std::io::Read;

    let dir = tempfile::tempdir()?;
    let visible = SequenceNumberCounter::default();
    let tree = open(dir.path(), true, &visible)?;
    write_history(&tree, &visible)?;
    let pin = tree.pin();
    assert!(!pin.tables().is_empty() && !pin.blob_files().is_empty());

    for file in pin.tables().iter().chain(pin.blob_files()) {
        let on_disk = std::fs::read(file.path())?;
        assert_eq!(file.file_size()?, on_disk.len() as u64);
        let mut read = Vec::new();
        file.open()?.read_to_end(&mut read)?;
        assert_eq!(read, on_disk, "{file:?}");
        assert_eq!(file.heal_attestation()?, None);
    }
    assert!(pin.tables().iter().all(|file| file.level().is_some()));
    assert!(pin.blob_files().iter().all(|file| file.level().is_none()));

    let table = pin.tables().first().expect("a table");
    let mut attestation = table.path().as_os_str().to_owned();
    attestation.push(".heal-attest");
    std::fs::write(&attestation, b"attestation")?;
    assert_eq!(
        table.heal_attestation()?.as_deref(),
        Some(Path::new(&attestation))
    );
    let blob_file = pin.blob_files().first().expect("a blob file");
    let mut beside = blob_file.path().as_os_str().to_owned();
    beside.push(".heal-attest");
    std::fs::write(&beside, b"not a table's")?;
    assert_eq!(blob_file.heal_attestation()?, None, "only a table has one");
    assert!(format!("{table:?}").contains("PinnedFile"));
    Ok(())
}

/// A pin lists the compression dictionaries its tables were written against,
/// and a tree restored with them reads every key.
#[cfg(feature = "zstd")]
#[test]
fn pin_lists_dictionaries_and_restores_with_them() -> lsm_tree::Result<()> {
    use lsm_tree::{CompressionType, ZstdDictionary, config::CompressionPolicy};

    let samples: Vec<u8> = (0..500u32)
        .flat_map(|i| format!("key-{i:05}value-{i:05}-padding").into_bytes())
        .collect();
    let dictionary = Arc::new(ZstdDictionary::new(&samples));
    let configure = |path: &Path, visible: &SequenceNumberCounter| -> lsm_tree::Result<Config> {
        Ok(
            Config::new(path, SequenceNumberCounter::default(), visible.clone())
                .data_block_compression_policy(CompressionPolicy::all(CompressionType::zstd_dict(
                    3,
                    dictionary.id(),
                )?))
                .zstd_dictionary(Some(Arc::clone(&dictionary))),
        )
    };

    let src = tempfile::tempdir()?;
    let dst = tempfile::tempdir()?;
    let visible = SequenceNumberCounter::default();
    let tree = configure(src.path(), &visible)?.open()?;
    for i in 0..100u32 {
        tree.insert(
            format!("key-{i:05}"),
            format!("value-{i:05}-padding"),
            u64::from(i),
        );
    }
    visible.set(100);
    tree.flush_active_memtable(0)?;

    let pin = tree.pin();
    assert_eq!(pin.dictionaries().len(), 1);
    assert_eq!(pin.dictionaries()[0].id(), u64::from(dictionary.id()));
    ship(&pin, dst.path())?;
    let restored = configure(dst.path(), &SequenceNumberCounter::default())?.restore_pinned(
        &pin.layout()?,
        pin.memtable_entries(),
        pin.memtable_range_tombstones(),
    )?;
    for i in 0..100u32 {
        assert_eq!(
            restored.get(format!("key-{i:05}"), SeqNo::MAX)?.as_deref(),
            Some(format!("value-{i:05}-padding").as_bytes())
        );
    }
    Ok(())
}

/// Pins release independently: a file stays while any pin listing it lives.
#[test]
fn concurrent_pins_release_independently() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let visible = SequenceNumberCounter::default();
    let tree = open(dir.path(), false, &visible)?;
    let mut seqno = write_history(&tree, &visible)?;

    let mut rewrite = |tree: &AnyTree| -> lsm_tree::Result<()> {
        for i in 0..40 {
            let key = format!("k{i:02}");
            tree.insert(key.as_str(), value(&key, seqno, 50), seqno);
            seqno += 1;
        }
        visible.set(seqno);
        tree.flush_active_memtable(0)?;
        tree.major_compact(u64::MAX, seqno)?;
        Ok(())
    };

    let first = tree.pin();
    rewrite(&tree)?;
    let second = tree.pin();
    rewrite(&tree)?;

    let first_only: Vec<_> = first
        .tables()
        .iter()
        .map(|file| file.path().to_path_buf())
        .collect();
    let second_only: Vec<_> = second
        .tables()
        .iter()
        .map(|file| file.path().to_path_buf())
        .collect();
    assert!(first_only.iter().all(|path| !second_only.contains(path)));
    assert!(
        first_only
            .iter()
            .chain(&second_only)
            .all(|path| path.exists())
    );

    drop(first);
    for path in &first_only {
        assert!(gone(path), "{} outlived its only pin", path.display());
    }
    assert!(
        second_only.iter().all(|path| path.exists()),
        "the second pin still holds its files"
    );

    drop(second);
    for path in &second_only {
        assert!(gone(path), "{} outlived its only pin", path.display());
    }
    Ok(())
}
