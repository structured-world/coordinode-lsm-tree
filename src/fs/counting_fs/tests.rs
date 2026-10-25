// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::{CountingFs, FileWrites, WriteOrigin};
use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, FsOpenOptions, MemFs};
use std::io::{Read as _, Write as _};
use std::path::Path;
use std::sync::Arc;

fn writable(origin: Option<WriteOrigin>) -> FsOpenOptions {
    let opts = FsOpenOptions::new().write(true).create_new(true);
    match origin {
        Some(origin) => opts.write_origin(origin),
        None => opts,
    }
}

/// Bytes, write calls, opens, creations and syncs land under the origin a
/// file was opened with, each byte once; a file opened without one is counted
/// as unattributed, and the sum over every slot is everything written.
#[test]
fn writes_are_counted_under_the_origin_they_were_opened_with() -> crate::io::Result<()> {
    let fs = CountingFs::new(Arc::new(MemFs::new()));
    fs.create_dir_all(Path::new("/db"))?;

    let mut table = fs.open(Path::new("/db/1"), &writable(Some(WriteOrigin::FlushTable)))?;
    table.write_all(&[7; 100])?;
    table.write_all(&[8; 28])?;
    table.sync_all()?;
    let mut stray = fs.open(Path::new("/db/2"), &writable(None))?;
    stray.write_all(&[9; 5])?;

    let reading = fs.stats().snapshot();
    let flush = reading.origin(WriteOrigin::FlushTable);
    assert_eq!(flush.bytes, 128);
    assert_eq!(flush.writes, 2);
    assert_eq!(flush.opened, 1);
    assert_eq!(flush.created, 1);
    assert_eq!(flush.syncs, 1);
    assert_eq!(reading.unattributed.bytes, 5);
    assert_eq!(
        reading.origin(WriteOrigin::CompactionTable),
        FileWrites::default()
    );
    assert_eq!(reading.files().bytes, 133);
    assert_eq!(reading.directory.dirs_created, 1);
    Ok(())
}

/// A file opened only for reading is the backend's own handle: nothing about
/// it is counted, and it reads what was written.
#[test]
fn a_file_opened_for_reading_is_not_counted() -> crate::io::Result<()> {
    let fs = CountingFs::new(Arc::new(MemFs::new()));
    fs.create_dir_all(Path::new("/db"))?;
    let mut file = fs.open(Path::new("/db/1"), &writable(Some(WriteOrigin::Manifest)))?;
    file.write_all(b"abc")?;
    drop(file);
    let before = fs.stats().snapshot();

    let mut file = fs.open(Path::new("/db/1"), &FsOpenOptions::new().read(true))?;
    let mut read = Vec::new();
    file.read_to_end(&mut read)?;

    assert_eq!(read, b"abc");
    assert_eq!(fs.stats().snapshot(), before);
    Ok(())
}

/// A write the backend takes only part of counts the bytes it took, once, and
/// the short call: a writer retrying the rest is not charged for it twice.
#[test]
fn a_short_write_counts_the_bytes_taken_once() -> crate::io::Result<()> {
    let faulty = FaultFs::new(MemFs::new());
    faulty
        .injector()
        .arm(FaultRule::new(FaultOp::Write, Fault::ShortWrite(3)).once());
    let fs = CountingFs::new(Arc::new(faulty));
    fs.create_dir_all(Path::new("/db"))?;
    let mut file = fs.open(Path::new("/db/1"), &writable(Some(WriteOrigin::Repair)))?;

    file.write_all(&[1; 10])?;

    let repair = fs.stats().snapshot().origin(WriteOrigin::Repair);
    assert_eq!(repair.bytes, 10, "3 taken short, the other 7 on the retry");
    assert_eq!(repair.writes, 2);
    assert_eq!(repair.short_writes, 1);
    assert_eq!(repair.write_errors, 0);
    Ok(())
}

/// A failed write and a failed sync are counted as failures, and the bytes
/// the failed write was handed are not counted as written.
#[test]
fn failed_writes_and_syncs_are_counted_as_failures() -> crate::io::Result<()> {
    let faulty = FaultFs::new(MemFs::new());
    let injector = faulty.injector();
    let fs = CountingFs::new(Arc::new(faulty));
    fs.create_dir_all(Path::new("/db"))?;
    let mut file = fs.open(Path::new("/db/1"), &writable(Some(WriteOrigin::Metadata)))?;

    injector.arm(FaultRule::new(FaultOp::Write, Fault::Error(crate::io::ErrorKind::Other)).once());
    assert!(file.write(&[1; 4]).is_err());
    injector
        .arm(FaultRule::new(FaultOp::SyncAll, Fault::Error(crate::io::ErrorKind::Other)).once());
    assert!(file.sync_all().is_err());
    file.sync_all()?;

    let metadata = fs.stats().snapshot().origin(WriteOrigin::Metadata);
    assert_eq!(metadata.bytes, 0);
    assert_eq!(metadata.write_errors, 1);
    assert_eq!(metadata.syncs, 2);
    assert_eq!(metadata.sync_errors, 1);
    Ok(())
}

/// A writeback started ahead of a sync is counted apart from the syncs: it is
/// no durability point and writes no byte the writes did not.
#[test]
fn a_started_writeback_is_counted_apart_from_syncs() -> crate::io::Result<()> {
    let fs = CountingFs::new(Arc::new(MemFs::new()));
    fs.create_dir_all(Path::new("/db"))?;
    let mut file = fs.open(
        Path::new("/db/1"),
        &writable(Some(WriteOrigin::CompactionTable)),
    )?;
    file.write_all(&[1; 64])?;

    file.start_writeback(0, 64)?;
    file.sync_data()?;

    let compaction = fs.stats().snapshot().origin(WriteOrigin::CompactionTable);
    assert_eq!(compaction.writebacks, 1);
    assert_eq!(compaction.syncs, 1);
    assert_eq!(compaction.bytes, 64);
    Ok(())
}

/// Directory operations are counted once each when they succeed, a directory
/// sync also when it fails; a hard link copies no byte.
#[test]
fn directory_operations_are_counted() -> crate::io::Result<()> {
    let faulty = FaultFs::new(MemFs::new());
    let injector = faulty.injector();
    let fs = CountingFs::new(Arc::new(faulty));
    fs.create_dir_all(Path::new("/db"))?;
    let mut file = fs.open(Path::new("/db/1"), &writable(Some(WriteOrigin::Checkpoint)))?;
    file.write_all(&[0; 64])?;
    drop(file);

    fs.hard_link(Path::new("/db/1"), Path::new("/db/2"))?;
    fs.rename(Path::new("/db/2"), Path::new("/db/3"))?;
    fs.remove_file(Path::new("/db/3"))?;
    assert!(fs.remove_file(Path::new("/db/absent")).is_err());
    fs.sync_directory(Path::new("/db"))?;
    injector.arm(
        FaultRule::new(
            FaultOp::SyncDirectory,
            Fault::Error(crate::io::ErrorKind::Other),
        )
        .once(),
    );
    assert!(fs.sync_directory(Path::new("/db")).is_err());
    fs.sync_device(Path::new("/db"))?;
    injector.arm(
        FaultRule::new(
            FaultOp::SyncDevice,
            Fault::Error(crate::io::ErrorKind::Other),
        )
        .once(),
    );
    assert!(fs.sync_device(Path::new("/db")).is_err());

    let reading = fs.stats().snapshot();
    assert_eq!(reading.directory.device_syncs, 2);
    assert_eq!(reading.directory.device_sync_errors, 1);
    assert_eq!(reading.directory.linked, 1);
    assert_eq!(reading.directory.renamed, 1);
    assert_eq!(
        reading.directory.removed, 1,
        "the failed removal is not one"
    );
    assert_eq!(reading.directory.syncs, 2);
    assert_eq!(reading.directory.sync_errors, 1);
    assert_eq!(reading.files().bytes, 64, "the link copied nothing");
    assert_eq!(reading.syncs(), 4);
    Ok(())
}

/// The difference of two readings is what happened between them; readings
/// from two lifetimes of the counters have none.
#[test]
fn readings_from_two_lifetimes_have_no_difference() -> crate::io::Result<()> {
    let backend: Arc<dyn Fs> = Arc::new(MemFs::new());
    backend.create_dir_all(Path::new("/db"))?;
    let first = CountingFs::new(Arc::clone(&backend));
    let mut file = first.open(Path::new("/db/1"), &writable(Some(WriteOrigin::Manifest)))?;
    file.write_all(&[0; 10])?;
    let early = first.stats().snapshot();
    file.write_all(&[0; 6])?;
    let late = first.stats().snapshot();

    let between = late
        .since(&early)
        .map(|d| d.origin(WriteOrigin::Manifest).bytes);
    assert_eq!(between, Some(6), "one lifetime");

    let reopened = CountingFs::new(backend).stats().snapshot();
    assert!(reopened.since(&late).is_none(), "the counters started over");
    assert!(reopened.host_write_amplification().is_none(), "no payload");
    Ok(())
}

/// Each origin has its own name, the one a machine-readable export uses.
#[test]
fn every_origin_has_its_own_name() {
    let names: Vec<&str> = WriteOrigin::ALL.iter().map(|o| o.name()).collect();
    assert_eq!(
        names,
        [
            "flush_table",
            "flush_blob",
            "ingest_table",
            "ingest_blob",
            "compaction_table",
            "compaction_blob",
            "manifest",
            "metadata",
            "repair",
            "checkpoint",
        ]
    );
}

/// What the wrapper only hands on reaches the backend and returns its answer
/// unchanged: batched and descriptor reads, links, clones, hole probes and the
/// file calls besides writes. A vectored write is counted as a write.
#[test]
fn what_the_wrapper_hands_on_answers_as_the_backend_does() -> crate::io::Result<()> {
    use crate::fs::{BlockBuf, BlockRead};
    use std::io::{IoSlice, Seek as _, SeekFrom};

    let backend: Arc<dyn Fs> = Arc::new(MemFs::new());
    let fs = CountingFs::new(Arc::clone(&backend));
    fs.create_dir_all(Path::new("/db"))?;
    let mut file = fs.open(
        Path::new("/db/1"),
        &writable(Some(WriteOrigin::Repair)).read(true),
    )?;
    let written = file.write_vectored(&[IoSlice::new(b"ab"), IoSlice::new(b"cd")])?;
    file.write_all(b"abcd".get(written..).unwrap_or_default())?;
    file.seek(SeekFrom::Start(0))?;
    let mut read = [0; 4];
    file.read_exact(&mut read)?;
    assert_eq!(&read, b"abcd");

    let (mut a, mut b) = ([0; 2], [0; 2]);
    file.read_many(&mut [(0, &mut a[..]), (2, &mut b[..])])?;
    assert_eq!((&a, &b), (b"ab", b"cd"));
    let inner = backend.open(Path::new("/db/1"), &FsOpenOptions::new().read(true))?;
    assert_eq!(file.backing_fd(), inner.backing_fd());
    file.lock_exclusive()?;

    let mut buf = [0; 4];
    let mut reqs = [BlockRead {
        file: &*inner,
        offset: 0,
        buf: BlockBuf::new(&mut buf),
    }];
    fs.read_blocks_batched(&mut reqs)?;
    assert_eq!(&buf, b"abcd");
    assert_eq!(fs.reads_by_descriptor(), backend.reads_by_descriptor());
    let check = |fs: &dyn Fs| {
        let mut buf = [0; 4];
        let reqs = [BlockRead {
            file: &*inner,
            offset: 0,
            buf: BlockBuf::new(&mut buf),
        }];
        fs.check_descriptor_batch(&reqs).is_ok()
    };
    assert_eq!(check(&fs), check(&*backend));

    let link = (
        fs.symlink(Path::new("/db/1"), Path::new("/db/l"), false)
            .is_ok(),
        fs.symlink_to_dir(Path::new("/db/l")).ok(),
    );
    let clone = fs
        .reflink_file(Path::new("/db/1"), Path::new("/db/c"))
        .is_ok();
    let holes = (
        fs.extent_is_hole(Path::new("/db/1"), 0, 4).ok(),
        fs.extent_contains_hole(Path::new("/db/1"), 0, 4).ok(),
    );
    let reading = fs.stats().snapshot();
    assert_eq!(reading.directory.symlinked, u64::from(link.0));
    assert_eq!(reading.directory.cloned, u64::from(clone));
    assert_eq!(
        holes,
        (
            backend.extent_is_hole(Path::new("/db/1"), 0, 4).ok(),
            backend.extent_contains_hole(Path::new("/db/1"), 0, 4).ok(),
        )
    );

    let repair = reading.origin(WriteOrigin::Repair);
    assert_eq!(repair.bytes, 4);
    assert_eq!(repair.writes, 1 + u64::from(written < 4));
    Ok(())
}
