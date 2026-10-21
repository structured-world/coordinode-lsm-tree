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
//!
//! With `std` a tree's pools refill off the writer's thread: a claim that
//! leaves half a batch starts a refill, long before the pool runs dry, so a
//! write waits on a batch only when it comes before the first one is ready.
//! A claim that finds the pool dry with no refill under way makes its file
//! and starts one.

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
    /// The file's place among those of its folder that hold no table yet,
    /// held until its table is written and synced.
    pub writing: Writing,
}

/// The most ids a table records of the files of its folder below it that
/// held no table yet: the lowest ones. One it leaves out reads to a repair as
/// written, which can only report a loss that is none, and the bound keeps
/// the table's metadata a size known in advance.
pub const WRITING_BELOW_CAP: usize = 16;

/// The ids of a folder's files that hold no table yet: pooled, being made by a
/// refill, or being written into.
type Unwritten = Arc<Mutex<Vec<TableId>>>;

/// A file its folder is writing a table into: listed among the folder's files
/// that hold no table until dropped.
///
/// A table records the ids below its own of the files of its folder that held
/// no table when it was finished. After a power loss without a manifest, an
/// empty file under such an id is no table that lost its bytes, as one
/// finished before that table was, but a pooled file or an output the power
/// loss cut short.
pub struct Writing {
    folder: Unwritten,
    id: TableId,
}

impl Writing {
    /// A file made for its table, listed from here.
    fn new(folder: &Unwritten, id: TableId) -> Self {
        folder.lock().push(id);
        Self::listed(folder, id)
    }

    /// A pooled file, listed already.
    fn listed(folder: &Unwritten, id: TableId) -> Self {
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

    /// The ids below this file's of the files its folder holds no table in,
    /// ascending, at most [`WRITING_BELOW_CAP`] of the lowest.
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
        unlist(&self.folder, self.id);
    }
}

/// Takes `id` off the list of a folder's files that hold no table.
fn unlist(folder: &Unwritten, id: TableId) {
    let mut ids = folder.lock();
    if let Some(at) = ids.iter().position(|listed| *listed == id) {
        ids.swap_remove(at);
    }
}

/// The pool of one tables folder.
///
/// One per physical folder, however many routes name it: two pools of one
/// folder would number its files apart, and a table written from one would
/// sit above files pooled by the other.
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
    /// Set when the backend's folder sync does nothing, which a pooled file
    /// would save, or once it reports it cannot count a file's links, so no
    /// pooled file can be told unshared: every file is made for its table.
    unpooled: bool,
    /// The ids of the folder's files that hold no table yet (see
    /// [`Writing`]).
    writing: Unwritten,
    /// Whether a refill off the claiming thread is under way.
    refilling: bool,
}

/// What every pool of a tree shares, with the refills running off the
/// claiming thread.
struct Pools {
    /// The tree's table id counter: a refill reserves the ids of its files
    /// from it, so they are never handed to any other table.
    ids: SequenceNumberCounter,
    /// Files a refill creates; `0` creates every table file when its table
    /// is written, with no pool.
    batch: usize,
    sync_mode: SyncMode,
    /// Whether a refill runs off the claiming thread.
    background: bool,
    folders: Mutex<Vec<FolderPool>>,
    /// Woken when a refill off the claiming thread ends, for a claim that
    /// found its pool dry while it ran.
    #[cfg(feature = "std")]
    refilled: parking_lot::Condvar,
}

/// Where a table file pool is refilled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RefillOn {
    /// Off the thread of the write that claims from it: no write waits on a
    /// batch.
    #[default]
    Background,
    /// On that thread: a write waits on the batch it starts, and every sync
    /// the pool issues falls inside one write.
    ClaimingThread,
}

/// A refill to run off the claiming thread: the pool it fills, the route it
/// makes its files through and the ids reserved for them.
struct Refill {
    at: usize,
    folder: PathBuf,
    fs: Arc<dyn Fs>,
    ids: Vec<TableId>,
}

/// The pools of a tree's tables folders, and the ids their files take.
pub struct TableFilePools {
    pools: Arc<Pools>,
    /// The refills running off the claiming thread, joined before the pools
    /// go: none makes a file in a folder after its tree is closed.
    #[cfg(feature = "std")]
    refills: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl TableFilePools {
    /// Pools that refill on the claiming thread.
    #[cfg(test)]
    pub fn new(ids: SequenceNumberCounter, batch: usize, sync_mode: SyncMode) -> Self {
        Self::with(ids, batch, sync_mode, false)
    }

    /// Pools that refill off the claiming thread: no claim waits on a batch.
    #[cfg(all(test, feature = "std"))]
    pub fn refilling_in_background(
        ids: SequenceNumberCounter,
        batch: usize,
        sync_mode: SyncMode,
    ) -> Self {
        Self::with(ids, batch, sync_mode, true)
    }

    /// A tree's pools: refilling where `refill` says, and on the claiming
    /// thread without `std`.
    pub fn for_tree(
        ids: SequenceNumberCounter,
        batch: usize,
        sync_mode: SyncMode,
        refill: RefillOn,
    ) -> Self {
        let background = cfg!(feature = "std") && refill == RefillOn::Background;
        Self::with(ids, batch, sync_mode, background)
    }

    fn with(
        ids: SequenceNumberCounter,
        batch: usize,
        sync_mode: SyncMode,
        background: bool,
    ) -> Self {
        Self {
            pools: Arc::new(Pools {
                ids,
                batch,
                sync_mode,
                background,
                folders: Mutex::new(Vec::new()),
                #[cfg(feature = "std")]
                refilled: parking_lot::Condvar::new(),
            }),
            #[cfg(feature = "std")]
            refills: Mutex::new(Vec::new()),
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
        if self.pools.batch == 0 {
            return;
        }
        let mut folders = self.pools.folders.lock();
        ids.retain(|id| !folders.iter().any(|pool| pool.ids.contains(id)));
        if ids.is_empty() {
            return;
        }
        ids.sort_unstable();
        let at = pool_of(&mut folders, folder, fs);
        let Some(pool) = folders.get_mut(at) else {
            return;
        };
        // A folder without a pool keeps no empty file: each is an orphan.
        if pool.unpooled {
            for id in ids {
                discard(&**fs, &folder.join(id.to_string()));
            }
            return;
        }
        pool.writing.lock().extend(ids.iter().copied());
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
    /// them, where refills would sync it once per batch. Refilling off the
    /// claiming thread, such a first claim that finds the pool dry makes its
    /// file and starts a refill, as does one that leaves half a batch.
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
        let mut folders = self.pools.folders.lock();
        let at = pool_of(&mut folders, folder, fs);
        // A dry pool a refill is filling is waited for rather than passed
        // over: a file made now would take an id above the batch, and the
        // ids a folder hands out would depend on how far the refill got.
        // Only a write that comes before the first batch is ready waits; one
        // started at half a batch is ready long before the pool runs dry.
        #[cfg(feature = "std")]
        while folders
            .get(at)
            .is_some_and(|pool| pool.refilling && pool.ids.is_empty())
        {
            self.pools.refilled.wait(&mut folders);
        }
        let Some(pool) = folders.get_mut(at) else {
            return Err(crate::Error::Unrecoverable);
        };
        let (claimed, due) = if pool.unpooled || self.pools.batch == 0 {
            (Ok(None), false)
        } else {
            self.pools.claim_pooled(pool, folder, fs, above, may_refill)
        };
        // No pool, the pool is dry, or the backend cannot tell whether a file
        // is shared: a file is made for the table. It takes its id, listed,
        // before a batch reserved now, so every pooled id stays above the
        // tables written into the folder.
        let pooled = claimed?.ok_or_else(|| Writing::new(&pool.writing, self.pools.ids.next()));
        let refill = due.then(|| self.pools.reserve(pool, at, folder, fs));
        drop(folders);
        if let Some(refill) = refill {
            self.start(refill);
        }
        match pooled {
            Ok(claimed) => Ok(claimed),
            Err(made) => Pools::create(folder, fs, made),
        }
    }

    /// Runs `refill` off this thread. A thread that cannot be started leaves
    /// the pool as it was, for the next claim to start again.
    #[cfg(feature = "std")]
    fn start(&self, refill: Refill) {
        let (at, ids) = (refill.at, refill.ids.clone());
        let pools = Arc::clone(&self.pools);
        match std::thread::Builder::new()
            .name("lsm-table-files".into())
            .spawn(move || {
                let ran =
                    std::panic::catch_unwind(core::panic::AssertUnwindSafe(|| pools.run(&refill)));
                if let Err(panic) = ran {
                    // A refill the backend panicked in never pools its batch:
                    // withdrawn, it leaves no claim waiting on it. The panic
                    // still reaches whoever joins the thread.
                    pools.withdraw(refill.at, &refill.ids);
                    std::panic::resume_unwind(panic);
                }
            }) {
            Ok(handle) => {
                let mut refills = self.refills.lock();
                refills.retain(|refill| !refill.is_finished());
                refills.push(handle);
            }
            Err(e) => {
                log::warn!("could not start refilling a table file pool: {e}");
                self.pools.withdraw(at, &ids);
            }
        }
    }

    /// No refill runs off the claiming thread without `std`.
    #[cfg(not(feature = "std"))]
    fn start(&self, refill: Refill) {
        self.pools.withdraw(refill.at, &refill.ids);
    }

    /// Waits for the refills running off the claiming thread to finish.
    pub fn wait_for_refills(&self) {
        #[cfg(feature = "std")]
        {
            let refills = core::mem::take(&mut *self.refills.lock());
            for refill in refills {
                if refill.join().is_err() {
                    log::warn!("a table file pool refill panicked");
                }
            }
        }
    }
}

impl Drop for TableFilePools {
    fn drop(&mut self) {
        self.wait_for_refills();
    }
}

impl Pools {
    /// The next file of `pool`, the pool at `at`, to write a table into,
    /// through the route `folder` on `fs`; `None` when it is dry, a fresh
    /// batch held no file that can be used, or the backend cannot count a
    /// file's links, which gives the pool up. With it, whether a refill off
    /// this thread is due.
    fn claim_pooled(
        &self,
        pool: &mut FolderPool,
        folder: &Path,
        fs: &Arc<dyn Fs>,
        above: Option<TableId>,
        may_refill: bool,
    ) -> (crate::Result<Option<ClaimedFile>>, bool) {
        let mut refilled = false;
        let claimed = loop {
            if !pool.synced && !pool.ids.is_empty() {
                if let Err(e) = crate::file::fsync_directory(folder, &**fs, self.sync_mode) {
                    break Err(e.into());
                }
                pool.synced = true;
            }
            let Some(id) = pool.ids.pop_front() else {
                if self.background || refilled || !may_refill {
                    break Ok(None);
                }
                // Synced by the refill itself, or left unsynced for the
                // next claim when its sync fails.
                if let Err(e) = self.refill(pool, folder, &**fs) {
                    break Err(e);
                }
                refilled = true;
                continue;
            };
            let path = folder.join(id.to_string());
            if above.is_some_and(|above| id <= above) {
                unlist(&pool.writing, id);
                discard(&**fs, &path);
                continue;
            }
            match open_unshared_empty(&**fs, &path) {
                Ok(Opened::Unshared(file)) => {
                    break Ok(Some(ClaimedFile {
                        id,
                        path,
                        file,
                        durable_entry: true,
                        writing: Writing::listed(&pool.writing, id),
                    }));
                }
                Ok(Opened::Unusable) => unlist(&pool.writing, id),
                Ok(Opened::LinksUncountable) => {
                    // Every other file would be refused alike: removed now,
                    // rather than kept for claims that never come.
                    unlist(&pool.writing, id);
                    for id in pool.ids.drain(..) {
                        unlist(&pool.writing, id);
                        discard(&**fs, &folder.join(id.to_string()));
                    }
                    pool.unpooled = true;
                    break Ok(None);
                }
                Err(e) => {
                    unlist(&pool.writing, id);
                    break Err(e);
                }
            }
        };
        // Only a write's first table starts a refill, as on the claiming
        // thread: the install of a write that runs the pool dry syncs the
        // folder once for all its further files, where refills started by
        // them would sync it once per batch and keep them waiting on batches.
        let due = self.background
            && may_refill
            && !pool.unpooled
            && !pool.refilling
            && pool.ids.len() <= self.batch / 2;
        (claimed, due)
    }

    /// Reserves a batch of ids for a refill of `pool`, the pool at `at`, off
    /// the claiming thread, listing them among the folder's files that hold
    /// no table from here.
    fn reserve(&self, pool: &mut FolderPool, at: usize, folder: &Path, fs: &Arc<dyn Fs>) -> Refill {
        pool.refilling = true;
        let ids: Vec<TableId> = (0..self.batch).map(|_| self.ids.next()).collect();
        pool.writing.lock().extend(ids.iter().copied());
        Refill {
            at,
            folder: folder.to_path_buf(),
            fs: Arc::clone(fs),
            ids,
        }
    }

    /// Makes the files of `refill` and syncs their folder once, then pools
    /// them. What could not be made is taken off the folder's list; files
    /// made before a failed sync are pooled unsynced, so the next claim syncs
    /// the folder before handing one out.
    #[cfg(feature = "std")]
    fn run(&self, refill: &Refill) {
        let mut made = Vec::with_capacity(refill.ids.len());
        let mut synced = true;
        for &id in &refill.ids {
            let path = refill.folder.join(id.to_string());
            match refill
                .fs
                .open(&path, &FsOpenOptions::new().write(true).create_new(true))
            {
                Ok(file) => {
                    drop(file);
                    made.push(id);
                }
                Err(e) => {
                    log::warn!("could not make pooled table file {}: {e}", path.display());
                    synced = false;
                    break;
                }
            }
        }
        if !made.is_empty()
            && let Err(e) =
                crate::file::fsync_directory(&refill.folder, &*refill.fs, self.sync_mode)
        {
            log::warn!(
                "could not sync {} for its pooled table files: {e}",
                refill.folder.display()
            );
            synced = false;
        }

        if let Some(pool) = self.folders.lock().get_mut(refill.at) {
            Self::pool_made(pool, refill, made, synced);
        }
        self.refilled.notify_all();
    }

    /// Pools `made`, the files `refill` made into `pool`, `synced` when their
    /// folder sync succeeded; takes the ids it could not make off the folder's
    /// list, and the made ones too where the pool was given up meanwhile.
    #[cfg(feature = "std")]
    fn pool_made(pool: &mut FolderPool, refill: &Refill, made: Vec<TableId>, synced: bool) {
        pool.refilling = false;
        for id in refill.ids.iter().filter(|id| !made.contains(id)) {
            unlist(&pool.writing, *id);
        }
        if pool.unpooled {
            for id in made {
                unlist(&pool.writing, id);
                discard(&*refill.fs, &refill.folder.join(id.to_string()));
            }
            return;
        }
        if !made.is_empty() {
            pool.synced &= synced;
            pool.ids.extend(made);
        }
    }

    /// Gives up a refill of the pool at `at` that never ran or never finished;
    /// a file it made stays empty on disk, for the next open to pool.
    fn withdraw(&self, at: usize, ids: &[TableId]) {
        if let Some(pool) = self.folders.lock().get_mut(at) {
            pool.refilling = false;
            for id in ids {
                unlist(&pool.writing, *id);
            }
        }
        #[cfg(feature = "std")]
        self.refilled.notify_all();
    }

    /// A file created for the next table under the id `writing` lists, its
    /// entry not yet durable.
    fn create(folder: &Path, fs: &Arc<dyn Fs>, writing: Writing) -> crate::Result<ClaimedFile> {
        let id = writing.id;
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
            pool.writing.lock().push(id);
            pool.ids.push_back(id);
        }
        crate::file::fsync_directory(folder, fs, self.sync_mode)?;
        pool.synced = true;
        Ok(())
    }
}

/// The position of the pool of the folder `folder` on `fs` names, made empty
/// when there is none yet. A route seen for the first time joins the pool of
/// a folder it is another name for.
fn pool_of(folders: &mut Vec<FolderPool>, folder: &Path, fs: &Arc<dyn Fs>) -> usize {
    let known = folders.iter().position(|pool| {
        pool.routes
            .iter()
            .any(|(route_fs, route)| Arc::ptr_eq(route_fs, fs) && route == folder)
    });
    known.unwrap_or_else(|| {
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
                // A pooled file saves its table's folder sync, so a backend
                // whose folder sync does nothing gains nothing from a pool and
                // pays for every file made ahead.
                unpooled: fs.capabilities(folder).directory_sync_noop,
                writing: Arc::new(Mutex::new(Vec::new())),
                refilling: false,
            });
            folders.len() - 1
        }
    })
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
