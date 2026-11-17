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
//! leaves half a batch reserves a refill, long before the pool runs dry, and a
//! claim that finds the pool dry with no refill under way makes its file and
//! reserves one. The refill starts at once, or, on a device whose I/O
//! scheduler idles between queues, once the write's install has synced, so it
//! never runs beside the write's own syncs there. Refilling on the writer's thread
//! is an option: the write that finds its pool dry makes the batch, takes its
//! file from it, and the install of its table syncs the folder among its own
//! syncs, which makes the whole batch durable.

use crate::{
    SequenceNumberCounter, TableId,
    fs::{Fs, FsFile, FsOpenOptions, SyncMode, WriteOrigin},
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
#[cfg(feature = "std")]
pub(crate) const DEFAULT_TABLE_FILE_POOL: usize = 16;

/// An empty table file, open for writing, under the name of the table about to
/// be written into it.
pub(crate) struct ClaimedFile {
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
pub(crate) const WRITING_BELOW_CAP: usize = 16;

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
pub(crate) struct Writing {
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
    pub(crate) fn alone(id: TableId) -> Self {
        Self::new(&Arc::new(Mutex::new(Vec::new())), id)
    }

    /// The ids below this file's of the files its folder holds no table in,
    /// ascending, at most [`WRITING_BELOW_CAP`] of the lowest.
    pub(crate) fn below(&self) -> Vec<TableId> {
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
    /// The first id of the batch a claim made last on the claiming thread:
    /// the claim took it, and the install of its table syncs the folder, which
    /// makes every pooled file made before durable by name (see
    /// [`TableFilePools::synced_with`]).
    latest_batch: Option<TableId>,
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
pub(crate) enum RefillOn {
    /// Off the thread of the write that claims from it, once that write's
    /// install has synced: no write waits on a batch, and none syncs beside
    /// one. The default.
    #[default]
    Background,
    /// On that thread: the write that starts a batch makes its files, and the
    /// install of its table syncs their folder among its own syncs. Measured
    /// 20% slower than off it on the first flush of a new tree, whatever the
    /// I/O scheduler.
    ClaimingThread,
}

/// A refill to run off the claiming thread: the pool it fills, the route it
/// makes its files through and the ids reserved for them.
struct Refill {
    at: usize,
    #[cfg_attr(
        not(feature = "std"),
        expect(dead_code, reason = "only the std refill makes the files")
    )]
    folder: PathBuf,
    #[cfg_attr(
        not(feature = "std"),
        expect(dead_code, reason = "only the std refill makes the files")
    )]
    fs: Arc<dyn Fs>,
    ids: Vec<TableId>,
}

/// The pools of a tree's tables folders, and the ids their files take.
pub(crate) struct TableFilePools {
    pools: Arc<Pools>,
    /// The refills running off the claiming thread, joined before the pools
    /// go: none makes a file in a folder after its tree is closed.
    #[cfg(feature = "std")]
    refills: Mutex<Vec<std::thread::JoinHandle<()>>>,
    /// The refills claims reserved, held until the install of the write that
    /// claimed has synced (see [`Self::start_pending`]).
    #[cfg(feature = "std")]
    pending: Mutex<Vec<Refill>>,
    /// The liveness of the tree whose folders these are, given up only once
    /// the last refill is joined.
    life: Option<Arc<crate::close::Life>>,
}

impl TableFilePools {
    /// Pools that refill on the claiming thread, with no tree to keep open.
    pub(crate) fn new(ids: SequenceNumberCounter, batch: usize, sync_mode: SyncMode) -> Self {
        Self::with(ids, batch, sync_mode, false, None)
    }

    /// Pools that refill off the claiming thread: no claim waits on a batch.
    #[cfg(all(test, feature = "std"))]
    pub(crate) fn refilling_in_background(
        ids: SequenceNumberCounter,
        batch: usize,
        sync_mode: SyncMode,
    ) -> Self {
        Self::with(ids, batch, sync_mode, true, None)
    }

    /// A tree's pools: refilling where `refill` says, and on the claiming
    /// thread without `std`, keeping the tree open until dropped.
    pub(crate) fn for_tree(
        ids: SequenceNumberCounter,
        batch: usize,
        sync_mode: SyncMode,
        refill: RefillOn,
        life: Arc<crate::close::Life>,
    ) -> Self {
        let background = cfg!(feature = "std") && refill == RefillOn::Background;
        Self::with(ids, batch, sync_mode, background, Some(life))
    }

    fn with(
        ids: SequenceNumberCounter,
        batch: usize,
        sync_mode: SyncMode,
        background: bool,
        life: Option<Arc<crate::close::Life>>,
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
            #[cfg(feature = "std")]
            pending: Mutex::new(Vec::new()),
            life,
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
    pub(crate) fn adopt(&self, folder: &Path, fs: &Arc<dyn Fs>, mut ids: Vec<TableId>) {
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
    /// The file is opened for writing as `origin`'s; the empty files a refill
    /// makes ahead are [`WriteOrigin::Metadata`]. A refill the claim reserves
    /// starts at once, or waits for [`Self::start_pending`] when
    /// `refill_after_install` (see `IoStrategy::refill_after_install`).
    ///
    /// # Errors
    ///
    /// An I/O error creating or opening a file, or syncing the folder.
    pub(crate) fn claim_as(
        &self,
        folder: &Path,
        fs: &Arc<dyn Fs>,
        above: Option<TableId>,
        may_refill: bool,
        origin: WriteOrigin,
        refill_after_install: bool,
    ) -> crate::Result<ClaimedFile> {
        // A refill held for a write that has not installed yet would leave a
        // dry pool waiting on a batch nobody makes.
        self.start_pending();
        // Held across the refill and the open, so two claims never take one
        // file.
        let mut folders = self.pools.folders.lock();
        let at = pool_of(&mut folders, folder, fs);
        // A dry pool a refill is filling is waited for rather than passed
        // over: a file made now would take an id above the batch, and the
        // ids a folder hands out would depend on how far the refill got.
        // Only a write that comes before the first batch is ready waits; one
        // started at half a batch is ready long before the pool runs dry.
        //
        // A refill another claim holds for its write's install may be the one
        // filling it: that write cannot install while this claim waits, so the
        // refill is started here first. It is held under this lock, so the
        // check cannot miss it.
        #[cfg(feature = "std")]
        while folders
            .get(at)
            .is_some_and(|pool| pool.refilling && pool.ids.is_empty())
        {
            if self.pending.lock().is_empty() {
                self.pools.refilled.wait(&mut folders);
            } else {
                parking_lot::MutexGuard::unlocked(&mut folders, || self.start_pending());
            }
        }
        let Some(pool) = folders.get_mut(at) else {
            return Err(crate::Error::Unrecoverable);
        };
        let (claimed, due) = if pool.unpooled || self.pools.batch == 0 {
            (Ok(None), false)
        } else {
            self.pools
                .claim_pooled(pool, folder, fs, above, may_refill, origin)
        };
        // No pool, the pool is dry, or the backend cannot tell whether a file
        // is shared: a file is made for the table. It takes its id, listed,
        // before a batch reserved now, so every pooled id stays above the
        // tables written into the folder.
        let pooled = claimed?.ok_or_else(|| Writing::new(&pool.writing, self.pools.ids.next()));
        let refill = due.then(|| self.pools.reserve(pool, at, folder, fs));
        // Held under the pools' lock, with the reservation: a claim that finds
        // the pool dry sees the refill it waits on.
        #[cfg(feature = "std")]
        let refill = match refill {
            Some(refill) if refill_after_install => {
                self.defer(refill);
                None
            }
            refill => refill,
        };
        drop(folders);
        // The write's own file is made before the refill starts, so the batch
        // never reaches the backend ahead of the file the write waits on.
        let claimed = match pooled {
            Ok(claimed) => Ok(claimed),
            Err(made) => Pools::create(folder, fs, made, origin),
        };
        if let Some(refill) = refill {
            if refill_after_install {
                self.defer(refill);
            } else {
                self.start(refill);
            }
        }
        claimed
    }

    /// Holds `refill` until [`Self::start_pending`] starts it.
    #[cfg(feature = "std")]
    fn defer(&self, refill: Refill) {
        self.pending.lock().push(refill);
    }

    /// No refill runs off the claiming thread without `std`.
    #[cfg(not(feature = "std"))]
    fn defer(&self, refill: Refill) {
        self.start(refill);
    }

    /// Starts the refills claims reserved and held. The install of a write's
    /// tables calls it once their syncs are done, so a batch the write's claim
    /// started never runs beside them: on a scheduler that idles for each
    /// thread's next request (BFQ) the two took the first flush of a new tree
    /// to 105 ms at p99, against 7 with the batch after it. A claim starts any
    /// still held before it looks at its pool, so a refill held by a write
    /// that never installed is not lost.
    pub(crate) fn start_pending(&self) {
        #[cfg(feature = "std")]
        {
            let pending = core::mem::take(&mut *self.pending.lock());
            for refill in pending {
                self.start(refill);
            }
        }
    }

    /// [`Self::claim_as`] for a flush's table.
    #[cfg(test)]
    pub(crate) fn claim(
        &self,
        folder: &Path,
        fs: &Arc<dyn Fs>,
        above: Option<TableId>,
        may_refill: bool,
    ) -> crate::Result<ClaimedFile> {
        self.claim_as(folder, fs, above, may_refill, WriteOrigin::FlushTable, true)
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

    /// Tells the pool of `folder` on `fs` that the install of the table `id`
    /// synced the folder. When that table took the first file of the latest
    /// batch a claim made, every pooled file was made before that claim
    /// returned, so before the sync began, and is durable by name: the next
    /// claim need not sync the folder again. A batch made since then is not
    /// covered, and the pool stays as it was.
    pub(crate) fn synced_with(&self, folder: &Path, fs: &Arc<dyn Fs>, id: TableId) {
        if let Some(pool) = self.pools.folders.lock().iter_mut().find(|pool| {
            pool.routes
                .iter()
                .any(|(route_fs, route)| Arc::ptr_eq(route_fs, fs) && route == folder)
        }) && pool.latest_batch == Some(id)
        {
            pool.synced = true;
        }
    }

    /// Starts the refills held and waits for every refill running off the
    /// claiming thread to finish.
    pub(crate) fn wait_for_refills(&self) {
        self.start_pending();
        self.join_refills();
    }

    /// Waits for the refills running off the claiming thread to finish.
    fn join_refills(&self) {
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
        // A refill still held makes nothing for a tree that is closing: its
        // ids are given back, as for one that could not start.
        #[cfg(feature = "std")]
        {
            let pending = core::mem::take(&mut *self.pending.lock());
            for refill in pending {
                self.pools.withdraw(refill.at, &refill.ids);
            }
        }
        self.join_refills();
        // Only now: no refill makes a file once the tree may close.
        drop(self.life.take());
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
        origin: WriteOrigin,
    ) -> (crate::Result<Option<ClaimedFile>>, bool) {
        let mut refilled = false;
        let claimed = loop {
            let Some(id) = pool.ids.pop_front() else {
                if self.background || refilled || !may_refill {
                    break Ok(None);
                }
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
            // Only a file handed out needs its name durable. A batch this
            // claim made is synced by the install of its table instead.
            if !pool.synced && !refilled {
                if let Err(e) = crate::file::fsync_directory(folder, &**fs, self.sync_mode) {
                    pool.ids.push_front(id);
                    break Err(e.into());
                }
                pool.synced = true;
            }
            match open_unshared_empty(&**fs, &path, origin) {
                Ok(Opened::Unshared(file)) => {
                    break Ok(Some(ClaimedFile {
                        id,
                        path,
                        file,
                        durable_entry: !refilled,
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
            match refill.fs.open(&path, &pool_file()) {
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
    /// entry not yet durable, opened as `origin`'s.
    fn create(
        folder: &Path,
        fs: &Arc<dyn Fs>,
        writing: Writing,
        origin: WriteOrigin,
    ) -> crate::Result<ClaimedFile> {
        let id = writing.id;
        let path = folder.join(id.to_string());
        let file = fs.open(
            &path,
            &FsOpenOptions::new()
                .write(true)
                .create_new(true)
                .write_origin(origin),
        )?;
        Ok(ClaimedFile {
            id,
            path,
            file,
            durable_entry: false,
            writing,
        })
    }

    /// Creates a batch of empty files in `folder` on `fs`, `pool`'s folder,
    /// without syncing it: the claim that refilled hands its file out with
    /// the entry not yet durable, so the install of its table syncs the
    /// folder among its own syncs, and that sync makes the whole batch
    /// durable. Until a claim sees it synced the pool stays unsynced, so the
    /// next claim syncs the folder before handing a file out (cheap by then).
    /// Whatever was created stays pooled when a later step fails.
    fn refill(&self, pool: &mut FolderPool, folder: &Path, fs: &dyn Fs) -> crate::Result<()> {
        pool.synced = false;
        pool.latest_batch = None;
        for at in 0..self.batch {
            let id = self.ids.next();
            if at == 0 {
                pool.latest_batch = Some(id);
            }
            let path = folder.join(id.to_string());
            drop(fs.open(&path, &pool_file())?);
            pool.writing.lock().push(id);
            pool.ids.push_back(id);
        }
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
                latest_batch: None,
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
fn open_unshared_empty(fs: &dyn Fs, path: &Path, origin: WriteOrigin) -> crate::Result<Opened> {
    let opts = FsOpenOptions::new()
        .write(true)
        .no_follow(true)
        .no_wait(true)
        .write_origin(origin);
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

/// How a refill makes a pool file: empty, for tables of any origin to come.
fn pool_file() -> FsOpenOptions {
    FsOpenOptions::new()
        .write(true)
        .create_new(true)
        .write_origin(WriteOrigin::Metadata)
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
