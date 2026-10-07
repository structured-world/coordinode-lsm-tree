use super::*;

/// Keys and values in key order.
type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// Every key and value a 5.x store at `folder` holds, newest versions.
fn read_v5(folder: &Path) -> lsm5::Result<Entries> {
    use lsm5::{AbstractTree as _, Guard as _};
    let tree = lsm5::Config::new(
        folder,
        lsm5::SequenceNumberCounter::default(),
        lsm5::SequenceNumberCounter::default(),
    )
    .open()?;
    tree.range::<&[u8], _>(.., u64::MAX, None)
        .map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.to_vec(), value.to_vec()))
        })
        .collect()
}

/// [`read_v5`] for a 6.0 store.
fn read_v6(folder: &Path) -> lsm6::Result<Entries> {
    use lsm6::{AbstractTree as _, Guard as _};
    let tree = lsm6::Config::new(
        folder,
        lsm6::SequenceNumberCounter::default(),
        lsm6::SequenceNumberCounter::default(),
    )
    .open()?;
    tree.range::<&[u8], _>(.., u64::MAX, None)
        .map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.to_vec(), value.to_vec()))
        })
        .collect()
}

/// A small 5.x store over two flushes, and what it holds.
fn small_store(folder: &Path) -> Result<Entries, Box<dyn std::error::Error>> {
    use lsm5::AbstractTree as _;
    let tree = lsm5::Config::new(
        folder,
        lsm5::SequenceNumberCounter::default(),
        lsm5::SequenceNumberCounter::default(),
    )
    .open()?;
    for round in 0..2u64 {
        for i in 0..200u64 {
            tree.insert(format!("k{i:04}"), format!("v{round}"), round * 200 + i + 1);
        }
        tree.flush_active_memtable(0)?;
    }
    drop(tree);
    Ok(read_v5(folder)?)
}

/// A switch stopped before each of its changes in turn is finished by the
/// next run, whichever change it stopped at: the folder ends up the converted
/// store holding the source's data, the backup the source, and nothing of the
/// conversion is left behind. Stops before the converted store is complete
/// start over from the source; the rest roll forward.
#[test]
fn an_interrupted_switch_is_finished_by_the_next_run() -> Result<(), Box<dyn std::error::Error>> {
    let mut stop_at = 0;
    loop {
        let folder = tempfile::tempdir()?;
        let expected = small_store(folder.path())?;
        prepare(folder.path(), &Options::default())?;

        let mut steps = 0;
        let interrupted = switch(folder.path(), &mut || {
            steps += 1;
            if steps > stop_at {
                Err(std::io::Error::other("interrupted"))
            } else {
                Ok(())
            }
        });
        let finished_unbroken = interrupted.is_ok();

        if !finished_unbroken {
            let report = convert(folder.path(), &Options::default())?;
            assert_eq!(
                report.resumed,
                stop_at > 0,
                "a run after the ready marker resumes the switch (stopped at {stop_at})"
            );
        }
        assert_eq!(read_v6(folder.path())?, expected, "stopped at {stop_at}");
        assert_eq!(read_v5(&folder.path().join(BACKUP))?, expected);
        for leftover in [STAGING, READY, SWAPPING] {
            assert!(
                !folder.path().join(leftover).exists(),
                "{leftover} is gone (stopped at {stop_at})"
            );
        }

        if finished_unbroken {
            assert!(stop_at > 3, "the switch takes several steps");
            return Ok(());
        }
        stop_at += 1;
    }
}

/// A store whose folder holds the backup of an earlier conversion is not
/// converted over it: the switch would mix two sources in one backup.
#[test]
fn a_store_holding_an_earlier_backup_is_refused() -> Result<(), Box<dyn std::error::Error>> {
    let folder = tempfile::tempdir()?;
    small_store(folder.path())?;
    std::fs::create_dir(folder.path().join(BACKUP))?;
    std::fs::write(folder.path().join(BACKUP).join("current"), b"x")?;
    assert!(matches!(
        convert(folder.path(), &Options::default()),
        Err(Error::Unsupported(_))
    ));
    assert!(!folder.path().join(STAGING).exists(), "nothing was built");
    Ok(())
}

/// The store's own entries are recognized by name, and nothing else in the
/// folder is: the lock, the conversion's own entries and foreign files stay.
#[test]
fn store_entries_are_the_pointer_the_manifest_and_the_file_folders() {
    for name in [
        "current", "v0", "v17", "edits-0", "edits-17", "tables", "blobs", "dicts",
    ] {
        assert!(is_store_entry(name), "{name}");
    }
    for name in [
        "LOCK",
        "v",
        "edits-",
        "v6-convert",
        "v5-backup",
        "v6-convert.ready",
        "v6-convert.swapping",
        "v1.tmp",
        "notes.txt",
    ] {
        assert!(!is_store_entry(name), "{name}");
    }
}
