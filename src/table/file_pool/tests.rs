use super::*;
use crate::fs::{
    Fault, FaultFs, FaultOp, FaultRule, FsCapabilities, FsDirEntry, FsMetadata, MemFs, SyncKind,
};
use std::io::Write;

type Pools = (TableFilePools, Arc<FaultFs<MemFs>>, Arc<dyn Fs>, PathBuf);

/// A pool over a fresh tables folder `/t` on a fault-injecting in-memory
/// backend, making `batch` files at a time.
fn pools(batch: usize) -> crate::Result<Pools> {
    let fault = Arc::new(FaultFs::new(MemFs::new()));
    let fs: Arc<dyn Fs> = fault.clone();
    let folder = PathBuf::from("/t");
    fs.create_dir_all(&folder)?;
    Ok((
        TableFilePools::new(SequenceNumberCounter::default(), batch, SyncMode::Normal),
        fault,
        fs,
        folder,
    ))
}

fn directory_syncs(fault: &FaultFs<MemFs>) -> usize {
    fault
        .injector()
        .take_syncs()
        .iter()
        .filter(|(_, kind)| *kind == SyncKind::Directory)
        .count()
}

fn exists(fs: &Arc<dyn Fs>, folder: &Path, id: TableId) -> crate::Result<bool> {
    Ok(fs.exists(&folder.join(id.to_string()))?)
}

/// The first claim makes a batch with one folder sync and hands out its
/// lowest id, durable by name; the claims after it use the batch, syncing
/// nothing, until it runs out.
#[test]
fn claims_use_one_folder_sync_per_batch() -> crate::Result<()> {
    let (pools, fault, fs, folder) = pools(3)?;

    let mut ids = Vec::new();
    for _ in 0..3 {
        let claimed = pools.claim(&folder, &fs, None, true)?;
        assert!(claimed.durable_entry, "a pooled file's entry is durable");
        ids.push(claimed.id);
    }
    assert_eq!(ids, vec![0, 1, 2], "ascending, in the order made");
    assert_eq!(directory_syncs(&fault), 1, "one sync for the batch");

    let claimed = pools.claim(&folder, &fs, None, true)?;
    assert_eq!(claimed.id, 3);
    assert_eq!(directory_syncs(&fault), 1, "a new batch, synced once");
    for id in 4..6 {
        assert!(exists(&fs, &folder, id)?, "the rest of the batch is pooled");
    }
    Ok(())
}

/// A claim above an id passes over the pooled files at or below it and
/// removes them, so a folder whose files only ever go to compactions does not
/// collect files no claim can take.
#[test]
fn a_claim_above_an_id_removes_the_pooled_files_below_it() -> crate::Result<()> {
    let (pools, _fault, fs, folder) = pools(4)?;
    assert_eq!(pools.claim(&folder, &fs, None, true)?.id, 0);

    let claimed = pools.claim(&folder, &fs, Some(2), true)?;
    assert_eq!(claimed.id, 3, "the lowest pooled id above 2");
    for id in [1, 2] {
        assert!(!exists(&fs, &folder, id)?, "pooled file {id} is removed");
    }
    Ok(())
}

/// A claim above every pooled id makes a new batch, whose ids are all above
/// it: an id a table holds was taken from the counter the batch takes from.
#[test]
fn a_claim_above_every_pooled_id_makes_a_new_batch() -> crate::Result<()> {
    let (pools, fault, fs, folder) = pools(2)?;
    pools.claim(&folder, &fs, None, true)?;
    directory_syncs(&fault);

    let claimed = pools.claim(&folder, &fs, Some(1), true)?;
    assert_eq!(claimed.id, 2, "the new batch's first id");
    assert!(claimed.durable_entry);
    assert_eq!(directory_syncs(&fault), 1, "the new batch's sync");
    Ok(())
}

/// The files an open adopts may never have had their entries synced, so the
/// first claim syncs the folder before it hands one out, lowest id first.
#[test]
fn an_adopted_file_is_synced_before_its_first_claim() -> crate::Result<()> {
    let (pools, fault, fs, folder) = pools(4)?;
    for id in [7, 5] {
        drop(fs.open(
            &folder.join(id.to_string()),
            &FsOpenOptions::new().write(true).create_new(true),
        )?);
    }
    pools.adopt(&folder, &fs, vec![7, 5]);
    directory_syncs(&fault);

    let claimed = pools.claim(&folder, &fs, None, true)?;
    assert_eq!(claimed.id, 5);
    assert!(claimed.durable_entry);
    assert_eq!(directory_syncs(&fault), 1, "the adopted entries are synced");

    assert_eq!(pools.claim(&folder, &fs, None, true)?.id, 7);
    assert_eq!(directory_syncs(&fault), 0, "once is enough");
    Ok(())
}

/// A pooled file whose link count cannot be read is treated as shared with a
/// capture: it is unlinked rather than written, and the next one is used.
#[test]
fn a_pooled_file_that_may_be_shared_is_unlinked_not_written() -> crate::Result<()> {
    let (pools, fault, fs, folder) = pools(3)?;
    fault.injector().arm(
        FaultRule::new(
            FaultOp::HardLinkCount,
            Fault::Error(crate::io::ErrorKind::Other),
        )
        .once(),
    );

    let claimed = pools.claim(&folder, &fs, None, true)?;
    assert_eq!(claimed.id, 1, "the next pooled file");
    assert!(!exists(&fs, &folder, 0)?, "the refused one is unlinked");
    Ok(())
}

/// A backend that can never tell a file's link count gets a file all the
/// same, made as its table is written, its entry not yet durable.
#[test]
fn a_backend_that_cannot_count_links_still_gets_a_file() -> crate::Result<()> {
    let (pools, fault, fs, folder) = pools(2)?;
    fault.injector().arm(FaultRule::new(
        FaultOp::HardLinkCount,
        Fault::Error(crate::io::ErrorKind::Unsupported),
    ));

    let claimed = pools.claim(&folder, &fs, None, true)?;
    assert!(!claimed.durable_entry, "made for its table, not pooled");
    assert!(exists(&fs, &folder, claimed.id)?);
    directory_syncs(&fault);

    // The pool is given up on, not refilled by every write only to be
    // removed again.
    let next = pools.claim(&folder, &fs, None, true)?;
    assert!(!next.durable_entry);
    assert_eq!(directory_syncs(&fault), 0, "no batch made and dropped");
    assert_eq!(
        fs.read_dir(&folder)?.len(),
        2,
        "the two tables' files, nothing pooled"
    );
    Ok(())
}

/// One namespace behind a backend that does not report it, so two handles on
/// it cannot be told to name one folder, and that reports the capabilities it
/// is given.
struct Opaque(MemFs, FsCapabilities);

impl Fs for Opaque {
    fn capabilities(&self, _path: &Path) -> FsCapabilities {
        self.1
    }
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> crate::io::Result<Box<dyn FsFile>> {
        self.0.open(path, opts)
    }
    fn create_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        self.0.create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> crate::io::Result<Vec<FsDirEntry>> {
        self.0.read_dir(path)
    }
    fn remove_file(&self, path: &Path) -> crate::io::Result<()> {
        self.0.remove_file(path)
    }
    fn remove_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        self.0.remove_dir_all(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> crate::io::Result<()> {
        self.0.rename(from, to)
    }
    fn metadata(&self, path: &Path) -> crate::io::Result<FsMetadata> {
        self.0.metadata(path)
    }
    fn sync_directory(&self, path: &Path) -> crate::io::Result<()> {
        self.0.sync_directory(path)
    }
    fn exists(&self, path: &Path) -> crate::io::Result<bool> {
        self.0.exists(path)
    }
}

/// Two handles on one backend name one folder, so they share its pool: one
/// batch serves the tables of both, numbered together, and neither keeps
/// pooled files below a table the other wrote.
#[test]
fn two_handles_on_one_folder_share_its_pool() -> crate::Result<()> {
    let mem = MemFs::new();
    let fs_a: Arc<dyn Fs> = Arc::new(mem.clone());
    let fs_b: Arc<dyn Fs> = Arc::new(mem);
    let folder = PathBuf::from("/t");
    fs_a.create_dir_all(&folder)?;
    let pools = TableFilePools::new(SequenceNumberCounter::default(), 4, SyncMode::Normal);

    assert_eq!(pools.claim(&folder, &fs_a, None, true)?.id, 0);
    assert_eq!(
        pools.claim(&folder, &fs_b, None, true)?.id,
        1,
        "the same batch"
    );
    assert_eq!(fs_a.read_dir(&folder)?.len(), 4, "one batch made");
    Ok(())
}

/// Two spellings of one folder (one through a symbolic link to it) share its
/// pool, as two handles on it do.
#[cfg(all(unix, feature = "std"))]
#[test]
fn two_spellings_of_one_folder_share_its_pool() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let folder = dir.path().join("t");
    let alias = dir.path().join("alias");
    std::fs::create_dir(&folder)?;
    std::os::unix::fs::symlink(&folder, &alias)?;
    let fs: Arc<dyn Fs> = Arc::new(crate::fs::StdFs);
    let pools = TableFilePools::new(SequenceNumberCounter::default(), 4, SyncMode::Normal);

    assert_eq!(pools.claim(&folder, &fs, None, true)?.id, 0);
    assert_eq!(
        pools.claim(&alias, &fs, None, true)?.id,
        1,
        "the same batch"
    );
    assert_eq!(std::fs::read_dir(&folder)?.count(), 4, "one batch made");
    Ok(())
}

/// A symbolic link put under a pooled name after the tree made the file is
/// not written through: the claim does not follow it and takes the next file.
#[cfg(all(unix, feature = "std"))]
#[test]
fn a_symlink_put_under_a_pooled_name_is_never_written_through() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let folder = dir.path().join("t");
    std::fs::create_dir(&folder)?;
    let outside = dir.path().join("outside");
    std::fs::write(&outside, b"")?;
    let fs: Arc<dyn Fs> = Arc::new(crate::fs::StdFs);
    let pools = TableFilePools::new(SequenceNumberCounter::default(), 2, SyncMode::Normal);
    assert_eq!(pools.claim(&folder, &fs, None, true)?.id, 0);
    std::fs::remove_file(folder.join("1"))?;
    std::os::unix::fs::symlink(&outside, folder.join("1"))?;

    let mut claimed = pools.claim(&folder, &fs, None, true)?;
    assert_ne!(claimed.id, 1, "the link is passed over");
    claimed.file.write_all(b"a table")?;
    drop(claimed);
    assert_eq!(
        std::fs::metadata(&outside)?.len(),
        0,
        "nothing is written through the link"
    );
    Ok(())
}

/// A named pipe put under a pooled name does not hold the claim waiting for
/// a reader: the claim passes it over, as `create_new` would have refused
/// it, and takes the next file.
#[cfg(all(unix, feature = "std"))]
#[test]
fn a_pipe_put_under_a_pooled_name_does_not_hold_the_claim() -> crate::Result<()> {
    use std::os::unix::fs::FileTypeExt;

    let dir = tempfile::tempdir()?;
    let folder = dir.path().join("t");
    std::fs::create_dir(&folder)?;
    let fs: Arc<dyn Fs> = Arc::new(crate::fs::StdFs);
    let pools = TableFilePools::new(SequenceNumberCounter::default(), 2, SyncMode::Normal);
    assert_eq!(pools.claim(&folder, &fs, None, true)?.id, 0);
    let pipe = folder.join("1");
    std::fs::remove_file(&pipe)?;
    let name = std::ffi::CString::new(pipe.as_os_str().as_encoded_bytes())
        .map_err(|e| crate::io::Error::new(crate::io::ErrorKind::InvalidInput, e.to_string()))?;
    // SAFETY: `name` is a NUL-terminated path the call only reads.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0, "mkfifo");

    let mut claimed = pools.claim(&folder, &fs, None, true)?;
    assert_ne!(claimed.id, 1, "the pipe is passed over");
    claimed.file.write_all(b"a table")?;
    assert!(
        std::fs::symlink_metadata(&pipe)?.file_type().is_fifo(),
        "the pipe is left as found"
    );
    Ok(())
}

/// A backend whose directory sync does nothing gets no pool: the sync a
/// pooled file saves is not there to save, and making files ahead only costs.
/// Every claim makes its file, and the empty files an open found are removed,
/// not pooled.
#[test]
fn a_backend_whose_folder_sync_does_nothing_gets_no_pool() -> crate::Result<()> {
    let mem = MemFs::new();
    let fs: Arc<dyn Fs> = Arc::new(Opaque(
        mem.clone(),
        FsCapabilities {
            directory_sync_noop: true,
            ..FsCapabilities::default()
        },
    ));
    let folder = PathBuf::from("/t");
    fs.create_dir_all(&folder)?;
    drop(fs.open(
        &folder.join("0"),
        &FsOpenOptions::new().write(true).create_new(true),
    )?);
    let pools = TableFilePools::new(SequenceNumberCounter::new(1), 4, SyncMode::Normal);
    pools.adopt(&folder, &fs, vec![0]);
    assert!(!exists(&fs, &folder, 0)?, "a found empty file is removed");

    for _ in 0..2 {
        let claimed = pools.claim(&folder, &fs, None, true)?;
        assert!(!claimed.durable_entry, "a file made for its table");
    }
    let names: Vec<String> = mem
        .read_dir(&folder)?
        .into_iter()
        .map(|entry| entry.file_name)
        .collect();
    assert_eq!(names.len(), 2, "no batch made ahead: {names:?}");
    Ok(())
}

/// Pools refilling off the claiming thread over a fresh tables folder `/t`
/// on a fault-injecting in-memory backend, making `batch` files at a time.
#[cfg(feature = "std")]
fn background_pools(batch: usize) -> crate::Result<Pools> {
    let (_, fault, fs, folder) = pools(batch)?;
    Ok((
        TableFilePools::refilling_in_background(
            SequenceNumberCounter::default(),
            batch,
            SyncMode::Normal,
        ),
        fault,
        fs,
        folder,
    ))
}

/// The ids of the empty files in `folder`.
#[cfg(feature = "std")]
fn empty_files(fs: &Arc<dyn Fs>, folder: &Path) -> crate::Result<Vec<TableId>> {
    let mut ids: Vec<TableId> = fs
        .read_dir(folder)?
        .into_iter()
        .filter(|entry| fs.metadata(&entry.path).is_ok_and(|meta| meta.len == 0))
        .filter_map(|entry| entry.file_name.parse().ok())
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

/// The ids the one folder of `pools` lists as holding no table, ascending.
#[cfg(feature = "std")]
fn listed(pools: &TableFilePools) -> Vec<TableId> {
    let mut ids = pools
        .pools
        .folders
        .lock()
        .first()
        .map(|pool| pool.writing.lock().clone())
        .unwrap_or_default();
    ids.sort_unstable();
    ids
}

/// Refilling off the claiming thread, a claim that finds the pool dry makes
/// its own file, its entry not durable, and starts a refill rather than wait
/// on one: the batch and its single folder sync come off that thread.
#[cfg(feature = "std")]
#[test]
fn a_dry_pool_makes_the_claim_its_file_and_refills_off_the_thread() -> crate::Result<()> {
    let (pools, fault, fs, folder) = background_pools(4)?;
    let first = pools.claim(&folder, &fs, None, true)?;
    assert!(!first.durable_entry, "made for its table");
    pools.wait_for_refills();
    assert_eq!(directory_syncs(&fault), 1, "the refill's one folder sync");
    let pooled = empty_files(&fs, &folder)?;
    assert_eq!(pooled.len(), 5, "the claim's file and a batch: {pooled:?}");
    assert_eq!(
        pooled.first(),
        Some(&first.id),
        "the claim's file, still empty"
    );
    assert!(
        pooled.iter().skip(1).all(|id| *id > first.id),
        "the batch is numbered after the claim's file: {pooled:?}"
    );

    let next = pools.claim(&folder, &fs, None, true)?;
    assert!(next.durable_entry, "a pooled file");
    assert_eq!(Some(next.id), pooled.get(1).copied());
    Ok(())
}

/// A claim that leaves half a batch starts the next refill, so the pool is
/// topped up before a write finds it dry.
#[cfg(feature = "std")]
#[test]
fn a_claim_that_leaves_half_a_batch_starts_a_refill() -> crate::Result<()> {
    let (pools, fault, fs, folder) = background_pools(4)?;
    drop(pools.claim(&folder, &fs, None, true)?);
    pools.wait_for_refills();
    let _ = directory_syncs(&fault);

    // Four pooled: the first claim leaves three, the second two, half the
    // batch, which starts the refill.
    let first = pools.claim(&folder, &fs, None, true)?;
    pools.wait_for_refills();
    assert_eq!(directory_syncs(&fault), 0, "three left, no refill yet");
    let second = pools.claim(&folder, &fs, None, true)?;
    pools.wait_for_refills();
    assert_eq!(directory_syncs(&fault), 1, "two left, a refill ran");
    assert!(first.durable_entry && second.durable_entry);
    let pooled = empty_files(&fs, &folder)?;
    assert_eq!(
        pooled.len(),
        1 + 2 + 2 + 4,
        "the first claim's file, two claimed, two left, a batch"
    );
    Ok(())
}

/// The pooled files and the ones a refill is making hold no table, and are
/// listed with the files being written: a table finished above one records
/// it, so a repair never reads an empty pooled file below it as a table that
/// lost its bytes.
#[cfg(feature = "std")]
#[test]
fn pooled_files_are_listed_as_holding_no_table() -> crate::Result<()> {
    let (pools, _fault, fs, folder) = background_pools(4)?;
    let made = pools.claim(&folder, &fs, None, true)?;
    pools.wait_for_refills();
    let pooled = empty_files(&fs, &folder)?;
    assert_eq!(
        listed(&pools),
        pooled,
        "the claimed file and every pooled one"
    );
    drop(made);

    let claimed = pools.claim(&folder, &fs, None, true)?;
    drop(claimed);
    let now = listed(&pools);
    let rest: Vec<TableId> = pooled.iter().copied().skip(2).collect();
    assert!(
        rest.iter().all(|id| now.contains(id)),
        "{now:?} still lists the unclaimed {rest:?}"
    );
    Ok(())
}

/// An in-memory backend that panics on every folder sync.
#[cfg(feature = "std")]
struct PanicsOnFolderSync(MemFs);

#[cfg(feature = "std")]
impl Fs for PanicsOnFolderSync {
    fn capabilities(&self, path: &Path) -> FsCapabilities {
        self.0.capabilities(path)
    }
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> crate::io::Result<Box<dyn FsFile>> {
        self.0.open(path, opts)
    }
    fn create_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        self.0.create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> crate::io::Result<Vec<FsDirEntry>> {
        self.0.read_dir(path)
    }
    fn remove_file(&self, path: &Path) -> crate::io::Result<()> {
        self.0.remove_file(path)
    }
    fn remove_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        self.0.remove_dir_all(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> crate::io::Result<()> {
        self.0.rename(from, to)
    }
    fn metadata(&self, path: &Path) -> crate::io::Result<FsMetadata> {
        self.0.metadata(path)
    }
    fn sync_directory(&self, path: &Path) -> crate::io::Result<()> {
        panic!("the backend fails syncing {}", path.display());
    }
    fn exists(&self, path: &Path) -> crate::io::Result<bool> {
        self.0.exists(path)
    }
}

/// A refill whose backend panics ends there, and a claim that finds the pool
/// dry goes on rather than wait for a refill that will never finish: it gets
/// a file made for its table.
#[cfg(feature = "std")]
#[test]
fn a_refill_that_panics_leaves_no_claim_waiting() -> crate::Result<()> {
    let fs: Arc<dyn Fs> = Arc::new(PanicsOnFolderSync(MemFs::new()));
    let folder = PathBuf::from("/t");
    fs.create_dir_all(&folder)?;
    let pools = TableFilePools::refilling_in_background(
        SequenceNumberCounter::default(),
        4,
        SyncMode::Normal,
    );

    let first = pools.claim(&folder, &fs, None, true)?;
    let second = pools.claim(&folder, &fs, None, true)?;
    assert!(!second.durable_entry, "made for its table, no batch pooled");
    assert!(second.id > first.id, "{} above {}", second.id, first.id);
    Ok(())
}

/// Pools that go wait for their refills: no file is made in a folder after
/// its tree is closed.
#[cfg(feature = "std")]
#[test]
fn closing_the_pools_waits_for_their_refills() -> crate::Result<()> {
    let (pools, _fault, fs, folder) = background_pools(8)?;
    drop(pools.claim(&folder, &fs, None, true)?);
    drop(pools);
    assert_eq!(
        empty_files(&fs, &folder)?.len(),
        1 + 8,
        "the claim's file and the whole batch"
    );
    Ok(())
}

/// A table records the files its folder is writing below it, and a file
/// leaves that list once its claim is dropped.
#[test]
fn a_claim_lists_the_files_its_folder_writes_below_it() -> crate::Result<()> {
    let (pools, _fault, fs, folder) = pools(4)?;
    let first = pools.claim(&folder, &fs, None, true)?;
    let second = pools.claim(&folder, &fs, None, false)?;
    let third = pools.claim(&folder, &fs, None, false)?;
    assert_eq!(third.writing.below(), vec![first.id, second.id]);
    assert!(first.writing.below().is_empty(), "nothing below the first");
    drop(second);
    assert_eq!(
        third.writing.below(),
        vec![first.id],
        "a dropped claim leaves"
    );
    Ok(())
}

/// Two routes on a backend that cannot tell they name one folder keep two
/// pools of it. An empty file the open found goes to one of them only: the
/// other would hand out the file the first one is about to write a table
/// into, while it is still empty.
#[test]
fn a_folder_two_pools_name_pools_each_file_once() -> crate::Result<()> {
    let mem = MemFs::new();
    let fs_a: Arc<dyn Fs> = Arc::new(Opaque(mem.clone(), FsCapabilities::default()));
    let fs_b: Arc<dyn Fs> = Arc::new(Opaque(mem, FsCapabilities::default()));
    let folder = PathBuf::from("/t");
    fs_a.create_dir_all(&folder)?;
    drop(fs_a.open(
        &folder.join("5"),
        &FsOpenOptions::new().write(true).create_new(true),
    )?);
    // An open starts the counter above every file it adopts.
    let pools = TableFilePools::new(SequenceNumberCounter::new(6), 4, SyncMode::Normal);
    pools.adopt(&folder, &fs_a, vec![5]);
    pools.adopt(&folder, &fs_b, vec![5]);

    let mut claimed = pools.claim(&folder, &fs_a, None, false)?;
    assert_eq!(claimed.id, 5);
    // Claimed before the first table is written, the file is still empty
    // and unshared: only its pool tells the second claim it is taken.
    let other = pools.claim(&folder, &fs_b, None, false)?;
    assert_ne!(other.id, 5, "one file handed to two tables");
    claimed.file.write_all(b"a table")?;
    drop(claimed);
    assert_eq!(
        fs_a.metadata(&folder.join("5"))?.len,
        7,
        "the table written into the file stays"
    );
    Ok(())
}

/// A pooled file gone from the folder (removed by hand) is passed over for
/// the next one.
#[test]
fn a_pooled_file_gone_from_the_folder_is_passed_over() -> crate::Result<()> {
    let (_, _fault, fs, folder) = pools(2)?;
    let pools = TableFilePools::new(SequenceNumberCounter::new(8), 2, SyncMode::Normal);
    drop(fs.open(
        &folder.join("7"),
        &FsOpenOptions::new().write(true).create_new(true),
    )?);
    pools.adopt(&folder, &fs, vec![6, 7]);

    assert_eq!(pools.claim(&folder, &fs, None, false)?.id, 7);
    Ok(())
}

/// A pooled file that cannot be opened fails the claim: the write that
/// needed it reports the error rather than writing elsewhere.
#[test]
fn a_pooled_file_that_cannot_be_opened_fails_the_claim() -> crate::Result<()> {
    let (pools, fault, fs, folder) = pools(2)?;
    pools.claim(&folder, &fs, None, true)?;
    fault.injector().arm(
        FaultRule::new(FaultOp::Open, Fault::Error(crate::io::ErrorKind::Other))
            .on_path(folder.join("1").to_string_lossy().into_owned())
            .once(),
    );

    assert!(pools.claim(&folder, &fs, None, true).is_err());
    Ok(())
}

/// A pooled file that may be shared and cannot be removed is still never
/// written: the claim moves on, and the file stays for the next open, which
/// pools it again.
#[test]
fn a_shared_pooled_file_that_cannot_be_removed_is_left() -> crate::Result<()> {
    let (pools, fault, fs, folder) = pools(3)?;
    fault.injector().arm(
        FaultRule::new(
            FaultOp::HardLinkCount,
            Fault::Error(crate::io::ErrorKind::Other),
        )
        .once(),
    );
    fault.injector().arm(
        FaultRule::new(
            FaultOp::RemoveFile,
            Fault::Error(crate::io::ErrorKind::PermissionDenied),
        )
        .once(),
    );

    assert_eq!(pools.claim(&folder, &fs, None, true)?.id, 1);
    assert!(exists(&fs, &folder, 0)?, "the file that was refused stays");
    Ok(())
}

/// A file under a pooled name that holds bytes is not a pool file: it is
/// neither written into nor removed.
#[test]
fn a_pooled_name_holding_bytes_is_left_alone() -> crate::Result<()> {
    let (_, _fault, fs, folder) = pools(2)?;
    // An open starts the counter above every file it adopts.
    let pools = TableFilePools::new(SequenceNumberCounter::new(1), 2, SyncMode::Normal);
    drop(fs.open(
        &folder.join("0"),
        &FsOpenOptions::new().write(true).create_new(true),
    )?);
    pools.adopt(&folder, &fs, vec![0]);
    fs.open(&folder.join("0"), &FsOpenOptions::new().write(true))?
        .write_all(b"bytes")?;

    let claimed = pools.claim(&folder, &fs, None, true)?;
    assert_ne!(claimed.id, 0);
    assert_eq!(fs.metadata(&folder.join("0"))?.len, 5, "left as found");
    Ok(())
}

/// Without a pool every claim makes its file, its entry not durable, and
/// syncs no folder.
#[test]
fn without_a_pool_each_file_is_made_for_its_table() -> crate::Result<()> {
    let (pools, fault, fs, folder) = pools(0)?;
    pools.adopt(&folder, &fs, vec![9]);

    let claimed = pools.claim(&folder, &fs, None, true)?;
    assert_eq!(claimed.id, 0);
    assert!(!claimed.durable_entry);
    assert_eq!(directory_syncs(&fault), 0);
    Ok(())
}

/// A later table of a write that ran the pool dry gets a file made for it,
/// not a new batch: the install naming the write's tables syncs the folder
/// once for all of them, where refills would sync it once per batch.
#[test]
fn a_write_that_runs_the_pool_dry_makes_its_further_files() -> crate::Result<()> {
    let (pools, fault, fs, folder) = pools(2)?;
    let mut durable = Vec::new();
    for may_refill in [true, false, false, false] {
        durable.push(pools.claim(&folder, &fs, None, may_refill)?.durable_entry);
    }
    assert_eq!(durable, vec![true, true, false, false]);
    assert_eq!(directory_syncs(&fault), 1, "the first batch alone");
    Ok(())
}

/// A batch whose folder sync fails stays pooled, its entries not known
/// durable: the next claim syncs the folder before handing one out.
#[test]
fn a_batch_whose_sync_failed_is_synced_by_the_next_claim() -> crate::Result<()> {
    let (pools, fault, fs, folder) = pools(2)?;
    fault.injector().arm(
        FaultRule::new(
            FaultOp::SyncDirectory,
            Fault::Error(crate::io::ErrorKind::Other),
        )
        .once(),
    );
    assert!(pools.claim(&folder, &fs, None, true).is_err());
    directory_syncs(&fault);

    let claimed = pools.claim(&folder, &fs, None, true)?;
    assert_eq!(claimed.id, 0, "the batch made before the failure");
    assert!(claimed.durable_entry);
    assert_eq!(directory_syncs(&fault), 1, "synced before it is used");
    Ok(())
}
