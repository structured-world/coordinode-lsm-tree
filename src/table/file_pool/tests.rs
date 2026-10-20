use super::*;
use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, MemFs, SyncKind};
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
        let claimed = pools.claim(&folder, &fs, None, 1)?;
        assert!(claimed.durable_entry, "a pooled file's entry is durable");
        ids.push(claimed.id);
    }
    assert_eq!(ids, vec![0, 1, 2], "ascending, in the order made");
    assert_eq!(directory_syncs(&fault), 1, "one sync for the batch");

    let claimed = pools.claim(&folder, &fs, None, 1)?;
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
    assert_eq!(pools.claim(&folder, &fs, None, 1)?.id, 0);

    let claimed = pools.claim(&folder, &fs, Some(2), 1)?;
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
    pools.claim(&folder, &fs, None, 1)?;
    directory_syncs(&fault);

    let claimed = pools.claim(&folder, &fs, Some(1), 1)?;
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

    let claimed = pools.claim(&folder, &fs, None, 1)?;
    assert_eq!(claimed.id, 5);
    assert!(claimed.durable_entry);
    assert_eq!(directory_syncs(&fault), 1, "the adopted entries are synced");

    assert_eq!(pools.claim(&folder, &fs, None, 1)?.id, 7);
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

    let claimed = pools.claim(&folder, &fs, None, 1)?;
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

    let claimed = pools.claim(&folder, &fs, None, 1)?;
    assert!(!claimed.durable_entry, "made for its table, not pooled");
    assert!(exists(&fs, &folder, claimed.id)?);
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

    let claimed = pools.claim(&folder, &fs, None, 1)?;
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

    let claimed = pools.claim(&folder, &fs, None, 1)?;
    assert_eq!(claimed.id, 0);
    assert!(!claimed.durable_entry);
    assert_eq!(directory_syncs(&fault), 0);
    Ok(())
}

/// A refill makes at least as many files as the claim says the write
/// expects to take, so a write of many tables syncs the folder once.
#[test]
fn a_refill_makes_room_for_the_files_a_write_expects() -> crate::Result<()> {
    let (pools, fault, fs, folder) = pools(2)?;
    for wanted in (1..=5).rev() {
        let claimed = pools.claim(&folder, &fs, None, wanted)?;
        assert!(claimed.durable_entry);
    }
    assert_eq!(directory_syncs(&fault), 1, "one batch of five");
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
    assert!(pools.claim(&folder, &fs, None, 1).is_err());
    directory_syncs(&fault);

    let claimed = pools.claim(&folder, &fs, None, 1)?;
    assert_eq!(claimed.id, 0, "the batch made before the failure");
    assert!(claimed.durable_entry);
    assert_eq!(directory_syncs(&fault), 1, "synced before it is used");
    Ok(())
}
