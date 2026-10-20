// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Table files created ahead of the tables written into them.
//!
//! A new table's name is durable only once its folder is synced, and the edit
//! that names the table must not outlive that name. A tables folder therefore
//! keeps a pool of empty files under the names of tables to come, made a batch
//! at a time with one folder sync for the batch: a table written into one is
//! durable with its own data sync, and the install that names it syncs no
//! folder.
//!
//! A pool file is empty until it is claimed, and a table never is, so a scan
//! tells the two apart by length: an empty file under a table name that no
//! version names is a pool file, adopted again by the next open.

use crate::{
    SequenceNumberCounter, TableId,
    fs::{Fs, FsFile, FsOpenOptions, SyncMode},
    path::{Path, PathBuf},
};
#[cfg(not(feature = "std"))]
use alloc::string::ToString;
use alloc::{boxed::Box, collections::VecDeque, sync::Arc, vec::Vec};
// no-std: spin::Mutex (parking_lot is std-only).
#[cfg(feature = "std")]
use parking_lot::Mutex;
#[cfg(not(feature = "std"))]
use spin::Mutex;

/// Files a refill creates when the tree's configuration does not say.
pub const DEFAULT_TABLE_FILE_POOL: usize = 16;

/// An empty table file, open for writing, under the name of the table about to
/// be written into it.
pub struct ClaimedFile {
    pub id: TableId,
    pub path: PathBuf,
    pub file: Box<dyn FsFile>,
    /// Whether the directory entry naming the file is durable already, so
    /// the install that names its table need not sync the folder for it.
    pub durable_entry: bool,
}

/// The pool of one tables folder.
struct FolderPool {
    fs: Arc<dyn Fs>,
    folder: PathBuf,
    /// The pooled ids, ascending.
    ids: VecDeque<TableId>,
    /// Whether every entry in `ids` is known durable: `false` for the files
    /// an open adopted or a refill made before its folder sync failed, until
    /// a sync of the folder succeeds.
    synced: bool,
    /// Set once the backend reports it cannot count a file's links: no
    /// pooled file can be told unshared, so every file is made for its table.
    unpooled: bool,
}

/// The pools of a tree's tables folders, and the ids their files take.
pub struct TableFilePools {
    /// The tree's table id counter: a refill reserves the ids of its files
    /// from it, so they are never handed to any other table.
    ids: SequenceNumberCounter,
    /// Files a refill creates; `0` creates every table file when its table
    /// is written, with no pool.
    batch: usize,
    sync_mode: SyncMode,
    folders: Mutex<Vec<FolderPool>>,
}

impl TableFilePools {
    pub fn new(ids: SequenceNumberCounter, batch: usize, sync_mode: SyncMode) -> Self {
        Self {
            ids,
            batch,
            sync_mode,
            folders: Mutex::new(Vec::new()),
        }
    }

    /// Pools `ids`, the empty table files an open found in `folder` that no
    /// version names. Their entries may never have been synced, so the first
    /// claim syncs the folder.
    ///
    /// An id another pool holds already is skipped: two routes that name one
    /// folder (another handle on its backend, another spelling of its path)
    /// see its files twice, and two pools handing out one file would let
    /// one remove or overwrite the table the other wrote into it.
    pub fn adopt(&self, folder: &Path, fs: &Arc<dyn Fs>, mut ids: Vec<TableId>) {
        if self.batch == 0 {
            return;
        }
        let mut folders = self.folders.lock();
        ids.retain(|id| !folders.iter().any(|pool| pool.ids.contains(id)));
        if ids.is_empty() {
            return;
        }
        ids.sort_unstable();
        let pool = pool_of(&mut folders, folder, fs);
        pool.ids.extend(ids);
        pool.synced = false;
        drop(folders);
    }

    /// An empty file for the next table written into `folder`, its id above
    /// `above` when given: a compaction output's id is above its inputs', so
    /// an output and the input whose age it takes are told apart by id.
    ///
    /// The pool is refilled only when `may_refill`: a write's first table. A
    /// later table of a write that has run the pool dry gets a file made for
    /// it, since the install that names it syncs the folder once for all of
    /// them, where refills would sync it once per batch.
    ///
    /// # Errors
    ///
    /// An I/O error creating or opening a file, or syncing the folder.
    pub fn claim(
        &self,
        folder: &Path,
        fs: &Arc<dyn Fs>,
        above: Option<TableId>,
        may_refill: bool,
    ) -> crate::Result<ClaimedFile> {
        if self.batch == 0 {
            return self.create(folder, fs);
        }
        // Held across the refill and the open, so two claims never take one
        // file.
        let mut folders = self.folders.lock();
        let pool = pool_of(&mut folders, folder, fs);
        let claimed = if pool.unpooled {
            Ok(None)
        } else {
            self.claim_pooled(pool, above, may_refill)
        };
        drop(folders);
        match claimed? {
            Some(claimed) => Ok(claimed),
            // The pool is dry and is not to be refilled, or the backend
            // cannot tell whether a file is shared, so no pooled file will be
            // used.
            None => self.create(folder, fs),
        }
    }

    /// The next file of `pool` to write a table into, refilled when it runs
    /// out and `may_refill`; `None` when it is dry and may not be, a fresh
    /// batch held no file that can be used, or the backend cannot count a
    /// file's links, which gives the pool up.
    fn claim_pooled(
        &self,
        pool: &mut FolderPool,
        above: Option<TableId>,
        may_refill: bool,
    ) -> crate::Result<Option<ClaimedFile>> {
        let mut refilled = false;
        loop {
            if !pool.synced && !pool.ids.is_empty() {
                crate::file::fsync_directory(&pool.folder, &*pool.fs, self.sync_mode)?;
                pool.synced = true;
            }
            let Some(id) = pool.ids.pop_front() else {
                if refilled || !may_refill {
                    return Ok(None);
                }
                // Synced by the refill itself, or left unsynced for the
                // next claim when its sync fails.
                self.refill(pool)?;
                refilled = true;
                continue;
            };
            let path = pool.folder.join(id.to_string());
            if above.is_some_and(|above| id <= above) {
                discard(&*pool.fs, &path);
                continue;
            }
            match open_unshared_empty(&*pool.fs, &path)? {
                Opened::Unshared(file) => {
                    return Ok(Some(ClaimedFile {
                        id,
                        path,
                        file,
                        durable_entry: true,
                    }));
                }
                Opened::Unusable => {}
                Opened::LinksUncountable => {
                    // Every other file would be refused alike: removed now,
                    // rather than kept for claims that never come.
                    for id in pool.ids.drain(..) {
                        discard(&*pool.fs, &pool.folder.join(id.to_string()));
                    }
                    pool.unpooled = true;
                    return Ok(None);
                }
            }
        }
    }

    /// A file created for the next table, its entry not yet durable.
    fn create(&self, folder: &Path, fs: &Arc<dyn Fs>) -> crate::Result<ClaimedFile> {
        let id = self.ids.next();
        let path = folder.join(id.to_string());
        let file = fs.open(&path, &FsOpenOptions::new().write(true).create_new(true))?;
        Ok(ClaimedFile {
            id,
            path,
            file,
            durable_entry: false,
        })
    }

    /// Creates a batch of empty files in `pool`'s folder and syncs it once.
    /// Whatever was created stays pooled when a later step fails, unsynced, so
    /// the next claim syncs the folder before handing one out.
    fn refill(&self, pool: &mut FolderPool) -> crate::Result<()> {
        pool.synced = false;
        for _ in 0..self.batch {
            let id = self.ids.next();
            let path = pool.folder.join(id.to_string());
            drop(
                pool.fs
                    .open(&path, &FsOpenOptions::new().write(true).create_new(true))?,
            );
            pool.ids.push_back(id);
        }
        crate::file::fsync_directory(&pool.folder, &*pool.fs, self.sync_mode)?;
        pool.synced = true;
        Ok(())
    }
}

/// The pool of `folder` on `fs`, made empty when there is none yet.
fn pool_of<'a>(
    folders: &'a mut Vec<FolderPool>,
    folder: &Path,
    fs: &Arc<dyn Fs>,
) -> &'a mut FolderPool {
    let at = folders
        .iter()
        .position(|pool| Arc::ptr_eq(&pool.fs, fs) && pool.folder == folder)
        .unwrap_or_else(|| {
            folders.push(FolderPool {
                fs: Arc::clone(fs),
                folder: folder.to_path_buf(),
                ids: VecDeque::new(),
                synced: true,
                unpooled: false,
            });
            folders.len() - 1
        });
    #[expect(
        clippy::indexing_slicing,
        reason = "`at` is a position found in, or just pushed onto, `folders`"
    )]
    &mut folders[at]
}

/// What a claim finds at a pooled name.
enum Opened {
    /// An empty file no other name links to, open for writing.
    Unshared(Box<dyn FsFile>),
    /// Gone, holding bytes, or linked from elsewhere: not written into.
    Unusable,
    /// The backend cannot count a file's links at all.
    LinksUncountable,
}

/// Opens the pool file at `path` for writing when it can be written into.
/// One that holds bytes is not a pool file: only this tree writes a pool
/// file, and only once it is claimed, so it is left to the open that finds it
/// unnamed. One a capture hard-linked is unlinked from the pool: its inode is
/// shared with another tree, which must never see a write. A link count that
/// cannot be read is treated as shared.
fn open_unshared_empty(fs: &dyn Fs, path: &Path) -> crate::Result<Opened> {
    let file = match fs.open(path, &FsOpenOptions::new().write(true)) {
        Ok(file) => file,
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => return Ok(Opened::Unusable),
        Err(e) => return Err(e.into()),
    };
    // Checked first: whatever the link count says, a file with bytes is never
    // removed here.
    if file.metadata()?.len != 0 {
        return Ok(Opened::Unusable);
    }
    match file.hard_link_count() {
        Ok(1) => Ok(Opened::Unshared(file)),
        Err(e) if e.kind() == crate::io::ErrorKind::Unsupported => {
            drop(file);
            discard(fs, path);
            Ok(Opened::LinksUncountable)
        }
        Ok(_) | Err(_) => {
            drop(file);
            discard(fs, path);
            Ok(Opened::Unusable)
        }
    }
}

/// Removes a pool file that will not be written into. Nothing names it, so a
/// removal that fails or that a crash takes back leaves an empty file the next
/// open adopts again.
fn discard(fs: &dyn Fs, path: &Path) {
    if let Err(e) = fs.remove_file(path)
        && e.kind() != crate::io::ErrorKind::NotFound
    {
        log::warn!("could not remove pooled table file {}: {e}", path.display());
    }
}

#[cfg(test)]
mod tests;
