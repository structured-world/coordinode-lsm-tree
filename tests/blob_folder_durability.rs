// A blob tree's `blobs` folder is a directory entry of the tree folder: it is
// durable only once the tree folder is synced after it was made, and the blob
// files inside it are only as durable as the folder's own name.

use lsm_tree::{
    Config, SequenceNumberCounter,
    fs::{Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, StdFs},
    io,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

/// A namespace call a [`RecordFs`] saw.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    CreateDir(PathBuf),
    SyncDir(PathBuf),
}

/// A backend recording directory creations and syncs, delegating to [`StdFs`].
struct RecordFs(Arc<Mutex<Vec<Call>>>);

impl RecordFs {
    fn record(&self, call: Call) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(call);
    }
}

impl Fs for RecordFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        StdFs.open(path, opts)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.record(Call::CreateDir(path.to_path_buf()));
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
        self.record(Call::SyncDir(path.to_path_buf()));
        StdFs.sync_directory(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        StdFs.exists(path)
    }
}

/// Opening a new blob tree makes its `blobs` folder and then syncs the tree
/// folder, so the folder's name is durable before any blob file the manifest
/// will name is written into it.
#[test]
fn a_new_blob_folder_is_made_durable_in_the_tree_folder() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let fs: Arc<dyn Fs> = Arc::new(RecordFs(Arc::clone(&calls)));
    let path = dir.path().join("tree");
    let tree = Config::new(
        &path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs)
    .with_kv_separation(Some(Default::default()))
    .open()?;
    drop(tree);

    let calls = calls.lock().unwrap_or_else(PoisonError::into_inner).clone();
    let blobs = path.join("blobs");
    let made = calls
        .iter()
        .position(|call| *call == Call::CreateDir(blobs.clone()))
        .expect("the blobs folder was made");
    assert!(
        calls[made..].contains(&Call::SyncDir(path.clone())),
        "the tree folder was not synced after its blobs folder was made: {calls:?}"
    );
    Ok(())
}
