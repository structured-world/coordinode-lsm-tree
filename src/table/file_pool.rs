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
    /// The file's place among those its folder is writing, held until its
    /// table is written and synced.
    pub writing: Writing,
}

/// The most ids a table records of the files its folder was still writing
/// below it: the lowest ones. One it leaves out reads to a repair as written,
/// which can only report a loss that is none, and the bound keeps the table's
/// metadata a size known in advance.
pub const WRITING_BELOW_CAP: usize = 16;

/// A file its folder is writing a table into: listed among the folder's
/// files being written until dropped.
///
/// A table records the ids below its own still being written in its folder
/// when it is finished. After a power loss without a manifest, an empty file
/// under such an id is no table that lost its bytes, as one finished before
/// that table was, but an output the power loss cut short.
pub struct Writing {
    folder: Arc<Mutex<Vec<TableId>>>,
    id: TableId,
}

impl Writing {
    fn new(folder: &Arc<Mutex<Vec<TableId>>>, id: TableId) -> Self {
        folder.lock().push(id);
        Self {
            folder: Arc::clone(folder),
            id,
        }
    }

    /// A file of a folder that writes nothing else: for a claim a test makes
    /// up.
    #[cfg(test)]
    pub fn alone(id: TableId) -> Self {
        Self::new(&Arc::new(Mutex::new(Vec::new())), id)
    }

    /// The ids below this file's that its folder is writing, ascending, at
    /// most [`WRITING_BELOW_CAP`] of the lowest.
    pub fn below(&self) -> Vec<TableId> {
        let mut ids: Vec<TableId> = self
            .folder
            .lock()
            .iter()
            .copied()
            .filter(|id| *id < self.id)
            .collect();
        ids.sort_unstable();
        ids.truncate(WRITING_BELOW_CAP);
        ids
    }
}

impl Drop for Writing {
    fn drop(&mut self) {
        let mut ids = self.folder.lock();
        if let Some(at) = ids.iter().position(|id| *id == self.id) {
            ids.swap_remove(at);
        }
    }
}

/// The pool of one tables folder.
///
/// One per physical folder, however many routes name it: two pools of one
/// folder would number its files apart, and a table written from one would
/// sit below files pooled by the other, which a repair reads as tables that
/// lost their bytes.
struct FolderPool {
    /// The routes that name the folder (a backend handle and a spelling of
    /// its path each). A claim works through its own route.
    routes: Vec<(Arc<dyn Fs>, PathBuf)>,
    /// The pooled ids, ascending.
    ids: VecDeque<TableId>,
    /// Whether every entry in `ids` is known durable: `false` for the files
    /// an open adopted or a refill made before its folder sync failed, until
    /// a sync of the folder succeeds.
    synced: bool,
    /// Set once the backend reports it cannot count a file's links: no
    /// pooled file can be told unshared, so every file is made for its table.
    unpooled: bool,
    /// The ids of the files a table is being written into (see [`Writing`]).
    writing: Arc<Mutex<Vec<TableId>>>,
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
    /// folder without the backend telling so see its files twice, and two
    /// pools handing out one file would let one remove or overwrite the table
    /// the other wrote into it.
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
        // Held across the refill and the open, so two claims never take one
        // file.
        let mut folders = self.folders.lock();
        let pool = pool_of(&mut folders, folder, fs);
        let writing = Arc::clone(&pool.writing);
        let claimed = if pool.unpooled || self.batch == 0 {
            Ok(None)
        } else {
            self.claim_pooled(pool, folder, &**fs, above, may_refill)
        };
        drop(folders);
        match claimed? {
            Some(claimed) => Ok(claimed),
            // No pool, the pool is dry and is not to be refilled, or the
            // backend cannot tell whether a file is shared, so no pooled file
            // will be used.
            None => self.create(folder, fs, &writing),
        }
    }

    /// The next file of `pool` to write a table into, through the route
    /// `folder` on `fs`, refilled when it runs out and `may_refill`; `None`
    /// when it is dry and may not be, a fresh batch held no file that can be
    /// used, or the backend cannot count a file's links, which gives the pool
    /// up.
    fn claim_pooled(
        &self,
        pool: &mut FolderPool,
        folder: &Path,
        fs: &dyn Fs,
        above: Option<TableId>,
        may_refill: bool,
    ) -> crate::Result<Option<ClaimedFile>> {
        let mut refilled = false;
        loop {
            if !pool.synced && !pool.ids.is_empty() {
                crate::file::fsync_directory(folder, fs, self.sync_mode)?;
                pool.synced = true;
            }
            let Some(id) = pool.ids.pop_front() else {
                if refilled || !may_refill {
                    return Ok(None);
                }
                // Synced by the refill itself, or left unsynced for the
                // next claim when its sync fails.
                self.refill(pool, folder, fs)?;
                refilled = true;
                continue;
            };
            let path = folder.join(id.to_string());
            if above.is_some_and(|above| id <= above) {
                discard(fs, &path);
                continue;
            }
            match open_unshared_empty(fs, &path)? {
                Opened::Unshared(file) => {
                    return Ok(Some(ClaimedFile {
                        id,
                        path,
                        file,
                        durable_entry: true,
                        writing: Writing::new(&pool.writing, id),
                    }));
                }
                Opened::Unusable => {}
                Opened::LinksUncountable => {
                    // Every other file would be refused alike: removed now,
                    // rather than kept for claims that never come.
                    for id in pool.ids.drain(..) {
                        discard(fs, &folder.join(id.to_string()));
                    }
                    pool.unpooled = true;
                    return Ok(None);
                }
            }
        }
    }

    /// A file created for the next table, its entry not yet durable, listed
    /// in `writing` before it exists.
    fn create(
        &self,
        folder: &Path,
        fs: &Arc<dyn Fs>,
        writing: &Arc<Mutex<Vec<TableId>>>,
    ) -> crate::Result<ClaimedFile> {
        let id = self.ids.next();
        let writing = Writing::new(writing, id);
        let path = folder.join(id.to_string());
        let file = fs.open(&path, &FsOpenOptions::new().write(true).create_new(true))?;
        Ok(ClaimedFile {
            id,
            path,
            file,
            durable_entry: false,
            writing,
        })
    }

    /// Creates a batch of empty files in `folder` on `fs`, `pool`'s folder,
    /// and syncs it once. Whatever was created stays pooled when a later step
    /// fails, unsynced, so the next claim syncs the folder before handing one
    /// out.
    fn refill(&self, pool: &mut FolderPool, folder: &Path, fs: &dyn Fs) -> crate::Result<()> {
        pool.synced = false;
        for _ in 0..self.batch {
            let id = self.ids.next();
            let path = folder.join(id.to_string());
            drop(fs.open(&path, &FsOpenOptions::new().write(true).create_new(true))?);
            pool.ids.push_back(id);
        }
        crate::file::fsync_directory(folder, fs, self.sync_mode)?;
        pool.synced = true;
        Ok(())
    }
}

/// The pool of the folder `folder` on `fs` names, made empty when there is
/// none yet. A route seen for the first time joins the pool of a folder it
/// is another name for.
fn pool_of<'a>(
    folders: &'a mut Vec<FolderPool>,
    folder: &Path,
    fs: &Arc<dyn Fs>,
) -> &'a mut FolderPool {
    let known = folders.iter().position(|pool| {
        pool.routes
            .iter()
            .any(|(route_fs, route)| Arc::ptr_eq(route_fs, fs) && route == folder)
    });
    let at = known.unwrap_or_else(|| {
        let alias = folders.iter().position(|pool| {
            pool.routes
                .first()
                .is_some_and(|(route_fs, route)| same_folder(route_fs, route, fs, folder))
        });
        let route = (Arc::clone(fs), folder.to_path_buf());
        if let Some((at, pool)) = alias.and_then(|at| Some((at, folders.get_mut(at)?))) {
            pool.routes.push(route);
            at
        } else {
            folders.push(FolderPool {
                routes: alloc::vec![route],
                ids: VecDeque::new(),
                synced: true,
                unpooled: false,
                writing: Arc::new(Mutex::new(Vec::new())),
            });
            folders.len() - 1
        }
    });
    #[expect(
        clippy::indexing_slicing,
        reason = "`at` is a position found in, or just pushed onto, `folders`"
    )]
    &mut folders[at]
}

/// Whether `folder` on `fs` is the folder `route` on `route_fs` names: one
/// backend namespace and one directory in it. A backend that cannot tell, or
/// a probe that fails, keeps the two apart: their pools then number files
/// apart, which a repair may read as lost tables, but never hand out one file
/// twice (see [`TableFilePools::adopt`]).
fn same_folder(route_fs: &Arc<dyn Fs>, route: &Path, fs: &Arc<dyn Fs>, folder: &Path) -> bool {
    let one_namespace = match (route_fs.backend_id(), fs.backend_id()) {
        (Some(a), Some(b)) => a == b,
        _ => Arc::ptr_eq(route_fs, fs),
    };
    one_namespace && fs.same_file(route, folder).unwrap_or(false)
}

/// What a claim finds at a pooled name.
enum Opened {
    /// An empty file no other name links to, open for writing.
    Unshared(Box<dyn FsFile>),
    /// Gone, holding bytes, no regular file, or linked from elsewhere: not
    /// written into.
    Unusable,
    /// The backend cannot count a file's links at all.
    LinksUncountable,
}

/// Opens the pool file at `path` for writing when it can be written into.
/// One that holds bytes is not a pool file: only this tree writes a pool
/// file, and only once it is claimed, so it is left to the open that finds it
/// unnamed. So is a symbolic link put under the name since the tree made the
/// file: the open does not follow it, as a table's own `create_new` would
/// not, so no table is written into a file the tree never made. Nor does it
/// wait on a named pipe put there, which `create_new` would have refused
/// rather than block the write on a reader that never comes. One a capture
/// hard-linked is unlinked from the pool: its inode is shared with another
/// tree, which must never see a write. A link count that cannot be read is
/// treated as shared.
fn open_unshared_empty(fs: &dyn Fs, path: &Path) -> crate::Result<Opened> {
    let opts = FsOpenOptions::new()
        .write(true)
        .no_follow(true)
        .no_wait(true);
    let file = match fs.open(path, &opts) {
        Ok(file) => file,
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => return Ok(Opened::Unusable),
        // The open a link or a pipe with no reader refuses fails with no
        // error kind of its own: what the name holds tells.
        Err(e) => {
            if let Ok(Some(_)) = fs.read_link(path) {
                return Ok(Opened::Unusable);
            }
            return match fs.metadata(path) {
                Ok(meta) if !meta.is_file => Ok(Opened::Unusable),
                _ => Err(e.into()),
            };
        }
    };
    // Checked first: whatever the link count says, a file with bytes, or the
    // link itself where a backend opens one, is never removed here.
    let meta = file.metadata()?;
    if !meta.is_file || meta.len != 0 {
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
