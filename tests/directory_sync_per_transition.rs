// A transition that writes several tables into one directory syncs that
// directory once, before the manifest edit that names them, not once per table.

use lsm_tree::{
    AbstractTree, Config, SequenceNumberCounter,
    fs::{Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, StdFs},
    io,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

/// A backend recording the directories it syncs, delegating to [`StdFs`].
struct SyncRecordFs(Arc<Mutex<Vec<PathBuf>>>);

impl Fs for SyncRecordFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        StdFs.open(path, opts)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        StdFs.create_dir_all(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        StdFs.read_dir(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        StdFs.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        StdFs.remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        StdFs.rename(from, to)
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        StdFs.metadata(path)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(path.to_path_buf());
        StdFs.sync_directory(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        StdFs.exists(path)
    }
}

/// A compaction cut into several output tables syncs the tables folder once.
#[test]
fn a_compaction_with_several_outputs_syncs_their_folder_once() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let synced = Arc::new(Mutex::new(Vec::new()));
    let fs: Arc<dyn Fs> = Arc::new(SyncRecordFs(Arc::clone(&synced)));
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs)
    .open()?;
    let mut seqno = 0;
    for _ in 0..2 {
        for key in 0..2_000u32 {
            tree.insert(format!("{key:06}"), vec![b'v'; 100], seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }

    synced
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clear();
    tree.major_compact(16 * 1_024, 0)?;
    let synced = synced
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();

    assert!(
        tree.table_count() > 2,
        "the compaction is cut into several tables"
    );
    let tables = dir.path().join("tables");
    assert_eq!(
        synced.iter().filter(|path| **path == tables).count(),
        1,
        "{} output tables: {synced:?}",
        tree.table_count()
    );
    Ok(())
}
