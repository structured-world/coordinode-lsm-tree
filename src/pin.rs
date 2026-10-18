// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A pinned version: a point-in-time view of a tree that holds its files in
//! place without copying, linking or syncing anything, and the means to rebuild
//! a tree from it elsewhere.
//!
//! [`AbstractTree::pin`](crate::AbstractTree::pin) seals the active memtable
//! and returns a [`PinnedVersion`] holding the tree's tables, blob files and
//! sealed memtables as they were at the call. The files stay on disk with
//! unchanged bytes until the last pin listing them drops, so a consumer can
//! stream them at its own pace (a Raft snapshot sent to a lagging follower);
//! [`Config::restore_pinned`](crate::Config::restore_pinned) builds the tree
//! on the receiving side from the streamed files, [`PinnedVersion::layout`]
//! and the memtable contents.

use crate::{
    SeqNo, UserKey, UserValue, ValueType,
    deletion_pause::DeletionPause,
    encryption::EncryptionProvider,
    fs::{Fs, FsFile, FsOpenOptions, MemFs, SyncMode},
    path::{Path, PathBuf},
    runtime_config::RuntimeConfig,
    tree::sealed::SealedMemtables,
    version::Version,
};
use alloc::sync::Arc;
use std::io::Read;

/// A file a [`PinnedVersion`] holds in place: a table, a blob file or a
/// compression dictionary.
#[derive(Clone)]
pub struct PinnedFile {
    id: u64,
    level: Option<usize>,
    path: PathBuf,
    fs: Arc<dyn Fs>,
}

impl PinnedFile {
    /// The file's id: the table, blob file or dictionary id it is named by.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The level a table sits in; `None` for a blob file or a dictionary.
    #[must_use]
    pub fn level(&self) -> Option<usize> {
        self.level
    }

    /// Where the file lies on the tree's filesystem.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The file's length in bytes.
    ///
    /// # Errors
    ///
    /// Propagates the filesystem's metadata failure.
    pub fn file_size(&self) -> crate::Result<u64> {
        Ok(self.fs.metadata(&self.path)?.len)
    }

    /// Opens the file for reading through the filesystem the tree uses for it,
    /// to stream it byte for byte.
    ///
    /// # Errors
    ///
    /// Propagates the filesystem's open failure.
    pub fn open(&self) -> crate::Result<Box<dyn FsFile>> {
        Ok(self.fs.open(&self.path, &FsOpenOptions::new().read(true))?)
    }

    /// The heal attestation lying beside a table, if it has one: the record of
    /// an in-place heal whose digest the version does not hold yet. Shipped
    /// beside the table (same name with `.heal-attest` appended), it lets the
    /// restored tree reconcile that table as the source would.
    ///
    /// # Errors
    ///
    /// Propagates the filesystem's probe failure.
    pub fn heal_attestation(&self) -> crate::Result<Option<PathBuf>> {
        if self.level.is_none() {
            return Ok(None);
        }
        let path = crate::scrub::heal_attest::attest_path(&self.path);
        Ok(self.fs.exists(&path)?.then_some(path))
    }
}

impl core::fmt::Debug for PinnedFile {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PinnedFile")
            .field("id", &self.id)
            .field("level", &self.level)
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

/// One entry of a pinned memtable, as a flush would write it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PinnedEntry {
    /// The user key.
    pub key: UserKey,
    /// The value (empty for a tombstone).
    pub value: UserValue,
    /// The entry's sequence number.
    pub seqno: SeqNo,
    /// What the entry is: a value, a tombstone, a merge operand, a cell row.
    pub value_type: ValueType,
}

/// A range tombstone of a pinned memtable, deleting `[start, end)` below
/// `seqno`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PinnedRangeTombstone {
    /// First key deleted.
    pub start: UserKey,
    /// First key past the deleted range.
    pub end: UserKey,
    /// The tombstone's sequence number.
    pub seqno: SeqNo,
}

/// A point-in-time view of a tree that keeps its files in place.
///
/// Taken by [`AbstractTree::pin`](crate::AbstractTree::pin). While it lives,
/// every file it lists stays on disk with unchanged bytes, however the tree
/// flushes and compacts meanwhile: the removal of a superseded file, an
/// in-place heal and a tight-space hole punch of a listed file wait for the
/// last pin listing it. Dropping it releases them; a restart drops every pin.
///
/// Pins are independent: each lists its own files and releases them on its
/// own drop.
pub struct PinnedVersion {
    version: Version,
    sealed: Arc<SealedMemtables>,
    seqno: SeqNo,
    tables: Vec<PinnedFile>,
    blob_files: Vec<PinnedFile>,
    dictionaries: Vec<PinnedFile>,
    pause: Arc<DeletionPause>,
    /// The source tree's directory lock: while the pin lives, no other opener
    /// gets the folder (with a registry of its own that knows nothing of the
    /// pin), even after the tree drops.
    _directory_lock: Option<Arc<dyn FsFile>>,
    comparator_name: Arc<str>,
    runtime: Arc<RuntimeConfig>,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    /// The source tree's folder and its filesystem: where the repair report
    /// the tree still owes lies, which the layout carries along.
    tree_fs: Arc<dyn Fs>,
    tree_path: PathBuf,
}

impl PinnedVersion {
    /// Pins `tree`'s current state; see [`AbstractTree::pin`](crate::AbstractTree::pin).
    pub(crate) fn take(tree: &crate::Tree) -> Self {
        let pause = Arc::clone(&tree.deletion_pause);
        // No in-place mutation may start, and none may still be running, while
        // the version is captured and its files registered.
        let pinning = pause.begin_pin();

        // The version lock is held only to seal, capture and register the
        // dictionaries: writers wait on it, and the listings below can be long.
        let (seqno, version, sealed, dictionaries) = {
            let mut history = tree.version_history.write();
            // Every write below this seqno has reached the active memtable:
            // the insert runs under this lock's read side.
            let seqno = tree.config.visible_seqno.get();
            tree.rotate_memtable_locked(&mut history);
            let super_version = history.latest_version_ref();
            let version = super_version.version.clone();
            let sealed = Arc::clone(&super_version.sealed_memtables);

            let folder = tree.config.path.join(crate::file::DICTS_FOLDER);
            let dictionaries: Vec<PinnedFile> = version
                .dicts()
                .iter()
                .map(|&id| PinnedFile {
                    id: u64::from(id),
                    level: None,
                    path: folder.join(id.to_string()),
                    fs: Arc::clone(&tree.config.fs),
                })
                .collect();
            // Under the version lock: the dictionary collection, which runs
            // under it too, sees the pin or has finished. A table or blob file
            // needs no such window: the captured version holds it, and no
            // mutation starts before `pinning` drops.
            pause.pin_files(dictionaries.iter().map(PinnedFile::path));
            drop(history);
            (seqno, version, sealed, dictionaries)
        };

        let tables: Vec<PinnedFile> = version
            .iter_levels()
            .enumerate()
            .flat_map(|(level, runs)| {
                runs.iter()
                    .flat_map(|run| run.iter())
                    .map(move |table| PinnedFile {
                        id: table.id(),
                        level: Some(level),
                        path: (*table.path).clone(),
                        fs: Arc::clone(&table.fs),
                    })
            })
            .collect();
        let blob_files: Vec<PinnedFile> = version
            .blob_files
            .iter()
            .map(|blob_file| PinnedFile {
                id: blob_file.id(),
                level: None,
                path: blob_file.0.path.clone(),
                fs: Arc::clone(&blob_file.0.fs),
            })
            .collect();
        pause.pin_files(tables.iter().chain(&blob_files).map(PinnedFile::path));
        drop(pinning);

        Self {
            version,
            sealed,
            seqno,
            tables,
            blob_files,
            dictionaries,
            pause,
            _directory_lock: tree.directory_lock.clone(),
            comparator_name: tree.config.comparator.name().into(),
            runtime: tree.runtime_config.load_full(),
            encryption: tree.config.encryption.clone(),
            tree_fs: Arc::clone(&tree.config.fs),
            tree_path: tree.config.path.clone(),
        }
    }

    /// The sequence number the pin was taken at: it holds every write below
    /// it, and reads at any snapshot up to it see what the tree served then.
    #[must_use]
    pub fn seqno(&self) -> SeqNo {
        self.seqno
    }

    /// The tables, with their levels.
    #[must_use]
    pub fn tables(&self) -> &[PinnedFile] {
        &self.tables
    }

    /// The blob files.
    #[must_use]
    pub fn blob_files(&self) -> &[PinnedFile] {
        &self.blob_files
    }

    /// The compression dictionaries the tables and blob files were written
    /// against.
    #[must_use]
    pub fn dictionaries(&self) -> &[PinnedFile] {
        &self.dictionaries
    }

    /// The version descriptor a manifest would hold for the pinned files
    /// (levels, runs, restrictions, blob files, dictionaries, the retention
    /// floor), serialized in memory, sealed with the tree's encryption
    /// provider when it has one, with the repair report the tree still owes
    /// when an ordinary open kept one for the next repairing open: the
    /// pinned files hold that repair's data, so the restored tree owes it
    /// too. Opaque: hand it to
    /// [`Config::restore_pinned`](crate::Config::restore_pinned).
    ///
    /// # Errors
    ///
    /// Propagates an encoding or encryption failure, or a failure to read
    /// the owed report.
    pub fn layout(&self) -> crate::Result<Vec<u8>> {
        let fs = MemFs::new();
        let path = Path::new("/layout");
        let mut writer = crate::manifest_blocks::writer::ManifestArchiveWriter::create(
            path,
            &fs,
            Arc::clone(&self.runtime),
            self.encryption.clone(),
            SyncMode::Normal,
        )?;
        self.version
            .encode_into(&mut writer, &self.comparator_name)?;
        writer.finish()?;

        // The pin's seqno rides along: the restored tree's counters start at
        // it, so a later write there lands above what the pin served.
        let mut layout = self.version.id().to_le_bytes().to_vec();
        layout.extend_from_slice(&self.seqno.to_le_bytes());
        // The owed report, sealed as its copies lie, behind its length; an
        // empty one when none is owed.
        let report =
            crate::file::owed_repair_report(&*self.tree_fs, &self.tree_path)?.unwrap_or_default();
        layout.extend_from_slice(&(report.len() as u64).to_le_bytes());
        layout.extend_from_slice(&report);
        fs.open(path, &FsOpenOptions::new().read(true))?
            .read_to_end(&mut layout)?;
        Ok(layout)
    }

    /// The entries of the sealed memtables, every version of every key below
    /// [`seqno`](Self::seqno), with their sequence numbers and value types.
    pub fn memtable_entries(&self) -> impl Iterator<Item = PinnedEntry> + '_ {
        let seqno = self.seqno;
        self.sealed
            .iter()
            .flat_map(|memtable| memtable.iter())
            .filter(move |entry| entry.key.seqno < seqno)
            .map(|entry| PinnedEntry {
                key: entry.key.user_key,
                value: entry.value,
                seqno: entry.key.seqno,
                value_type: entry.key.value_type,
            })
    }

    /// The range tombstones of the sealed memtables below
    /// [`seqno`](Self::seqno).
    #[must_use]
    pub fn memtable_range_tombstones(&self) -> Vec<PinnedRangeTombstone> {
        self.sealed
            .iter()
            .flat_map(|memtable| memtable.range_tombstones_sorted())
            .filter(|tombstone| tombstone.seqno < self.seqno)
            .map(|tombstone| PinnedRangeTombstone {
                start: tombstone.start,
                end: tombstone.end,
                seqno: tombstone.seqno,
            })
            .collect()
    }
}

impl Drop for PinnedVersion {
    fn drop(&mut self) {
        // Before the version's handles drop, so the files whose last view goes
        // with them are removed the ordinary way.
        self.pause.unpin_files(
            self.tables
                .iter()
                .chain(&self.blob_files)
                .chain(&self.dictionaries)
                .map(PinnedFile::path),
        );
    }
}

/// Builds the tree at `config.path` from a pin's files, its
/// [`layout`](PinnedVersion::layout) and its memtable contents; see
/// [`Config::restore_pinned`](crate::Config::restore_pinned).
pub fn restore(
    config: crate::Config,
    layout: &[u8],
    entries: impl IntoIterator<Item = PinnedEntry>,
    range_tombstones: impl IntoIterator<Item = PinnedRangeTombstone>,
) -> crate::Result<crate::AnyTree> {
    use crate::AbstractTree;
    use crate::file::{BLOBS_FOLDER, DICTS_FOLDER, TABLES_FOLDER, fsync_directory};
    use std::io::Write;

    let (id, rest) = layout
        .split_first_chunk::<8>()
        .ok_or(crate::Error::InvalidHeader("pinned layout"))?;
    let (pin_seqno, rest) = rest
        .split_first_chunk::<8>()
        .ok_or(crate::Error::InvalidHeader("pinned layout"))?;
    let (report_len, rest) = rest
        .split_first_chunk::<8>()
        .ok_or(crate::Error::InvalidHeader("pinned layout"))?;
    let (report, manifest) = usize::try_from(u64::from_le_bytes(*report_len))
        .ok()
        .and_then(|len| rest.split_at_checked(len))
        .ok_or(crate::Error::InvalidHeader("pinned layout"))?;
    let version_id = u64::from_le_bytes(*id);
    let pin_seqno = u64::from_le_bytes(*pin_seqno);

    let fs = Arc::clone(&config.fs);
    let root = config.path.clone();
    let sync_mode = config.sync_mode;

    // Exclusive from the first write to the open: no other opener may see the
    // folder before the memtable contents are in.
    fs.create_dir_all(&root)?;
    let lock = crate::config::acquire_directory_lock(&*fs, &root, config.directory_lock)?;
    let marker = root.join(crate::file::RESTORE_MARKER_FILE);
    // A folder an interrupted restore left is this restore's to finish: its
    // manifest goes, and the open below sweeps what that restore's flush
    // wrote. Any other tree is never published over: its open would sweep
    // that tree's manifest and tables as orphans of the restored layout.
    let resuming = fs.exists(&marker)?;
    for entry in fs.read_dir(&root)? {
        let name = &entry.file_name;
        let manifest_file = name.starts_with("edits-")
            || name
                .strip_prefix('v')
                .is_some_and(|id| !id.is_empty() && id.bytes().all(|c| c.is_ascii_digit()));
        if !manifest_file {
            continue;
        }
        if !resuming {
            return Err(crate::Error::from(crate::io::Error::new(
                crate::io::ErrorKind::AlreadyExists,
                format!(
                    "restore_pinned: {} already holds a tree ({name})",
                    root.display()
                ),
            )));
        }
        fs.remove_file(&entry.path)?;
    }

    // The streamed files first: the manifest installed below must never name a
    // file a power loss can still take back.
    for folder in [TABLES_FOLDER, BLOBS_FOLDER, DICTS_FOLDER] {
        let folder = root.join(folder);
        if !fs.exists(&folder)? {
            continue;
        }
        for entry in fs.read_dir(&folder)? {
            if entry.is_dir {
                continue;
            }
            // Write access for the flush alone: Windows refuses to flush a
            // read-only handle. Nothing is written through it.
            let file = fs.open(&entry.path, &FsOpenOptions::new().read(true).write(true))?;
            FsFile::sync_all_with(&*file, sync_mode)?;
        }
        fsync_directory(&folder, &*fs, sync_mode)?;
    }

    // Durable before the manifest is: a crash from here until the restored
    // memtable contents are flushed leaves a folder an ordinary open refuses.
    if !resuming {
        let file = fs.open(&marker, &FsOpenOptions::new().write(true).create_new(true))?;
        FsFile::sync_all_with(&*file, sync_mode)?;
        drop(file);
        fsync_directory(&root, &*fs, sync_mode)?;
    }

    // Written under a temporary name and renamed once its bytes are synced:
    // the name installs the snapshot, and one directory sync makes it and the
    // empty log durable.
    let temporary = root.join(format!(
        "v{version_id}{}",
        crate::version::SNAPSHOT_TEMPORARY_SUFFIX
    ));
    if let Err(e) = fs.remove_file(&temporary)
        && e.kind() != crate::io::ErrorKind::NotFound
    {
        return Err(e.into());
    }
    let mut file = fs.open(
        &temporary,
        &FsOpenOptions::new().write(true).create_new(true),
    )?;
    file.write_all(manifest)?;
    file.flush()?;
    FsFile::sync_data_with(&*file, sync_mode)?;
    drop(file);
    crate::version::edit_log::create_log(&*fs, &root, version_id, sync_mode)?;
    // The report the source still owed, under the restored snapshot's id, so
    // a repairing open of the restored tree hands it out; named before the
    // snapshot, so the directory sync below makes both durable together.
    if !report.is_empty() {
        crate::file::write_repair_report(&*fs, &root, version_id, report, sync_mode)?;
    }
    fs.rename(&temporary, &root.join(format!("v{version_id}")))?;
    fsync_directory(&root, &*fs, sync_mode)?;

    let seqno = Arc::clone(&config.seqno);
    let visible_seqno = Arc::clone(&config.visible_seqno);
    let tree = config.open_with(crate::config::DirectoryLock::Held(lock))?;

    // A restricted table keeps its bound beside it, as on the source, so a
    // manifest repair of the restored tree recovers the restriction.
    let version = tree.current_version();
    for table in version.iter_tables() {
        if let Some(bound) = table.restrict_lower_bound() {
            crate::restrict_bound::write(
                &*table.fs,
                &table.path,
                table.encryption.as_deref(),
                table.id(),
                bound,
                sync_mode,
            )?;
        }
    }

    let index = match &tree {
        crate::AnyTree::Standard(tree) => tree,
        crate::AnyTree::Blob(tree) => &tree.index,
    };
    for entry in entries {
        // The tree is not shared yet: the memtable size needs no backpressure,
        // and the flush below takes everything.
        let _ = index.append_entry(crate::InternalValue::from_components(
            entry.key,
            entry.value,
            entry.seqno,
            entry.value_type,
        ));
    }
    let memtable = index.active_memtable();
    for tombstone in range_tombstones {
        let _ = memtable.insert_range_tombstone(tombstone.start, tombstone.end, tombstone.seqno);
    }
    drop(memtable);
    // Collect nothing: the restored tree serves every snapshot the source did.
    tree.flush_active_memtable(0)?;

    // A write on the restored tree must land above every restored version, and
    // reads see what the pin served. The tables may hold a write at or above
    // the pin's seqno (flushed before its seqno was published).
    let next = tree
        .get_highest_seqno()
        .map_or(pin_seqno, |highest| pin_seqno.max(highest + 1));
    seqno.fetch_max(next);
    visible_seqno.fetch_max(pin_seqno);

    // The flush made the restored contents durable: the tree is whole now.
    fs.remove_file(&marker)?;
    fsync_directory(&root, &*fs, sync_mode)?;
    Ok(tree)
}
