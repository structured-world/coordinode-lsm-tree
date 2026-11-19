// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A backend that reports [`FsCapabilities::serial_syncs`] gets a synced
//! flush's file syncs one after another: its table and its blob file are
//! never synced at the same time. A backend that does not report it gets them
//! side by side. A synced flush writes no manifest edit, and folder syncs are
//! not counted here.

use lsm_tree::fs::{Fs, FsCapabilities, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, MemFs};
use lsm_tree::io;
use lsm_tree::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;
use test_log::test;

/// How long a sync stays in flight where none waits for another: time for a
/// second one issued beside it to be seen, if one is.
const SYNC_TIME: Duration = Duration::from_millis(20);

/// How long a sync waits for a second one to join it, where syncs are
/// expected side by side: bounded, so a sync that stays alone (a flush has
/// one) or a backend that serializes them costs this once rather than hangs,
/// and far above the time a second thread takes to start.
const RENDEZVOUS: Duration = Duration::from_secs(1);

/// The syncs in flight now, and the most ever in flight at once.
#[derive(Default)]
struct InFlight {
    /// `(now, most)`.
    counts: Mutex<(usize, usize)>,
    arrived: Condvar,
    /// Whether a sync waits for a second one to be in flight with it.
    meet: bool,
}

impl InFlight {
    fn sync(&self) {
        let mut counts = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
        counts.0 += 1;
        counts.1 = counts.1.max(counts.0);
        self.arrived.notify_all();
        if self.meet {
            counts = self
                .arrived
                .wait_timeout_while(counts, RENDEZVOUS, |counts| counts.1 < 2)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        } else {
            drop(counts);
            std::thread::sleep(SYNC_TIME);
            counts = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
        }
        counts.0 -= 1;
    }

    fn most(&self) -> usize {
        self.counts.lock().unwrap_or_else(PoisonError::into_inner).1
    }
}

/// [`MemFs`], reporting `serial` as its [`FsCapabilities::serial_syncs`] and
/// counting the file syncs in flight.
struct Watched {
    inner: MemFs,
    serial: bool,
    syncs: Arc<InFlight>,
}

impl Fs for Watched {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        Ok(Box::new(WatchedFile {
            inner: self.inner.open(path, opts)?,
            syncs: Arc::clone(&self.syncs),
        }))
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        self.inner.read_dir(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir_all(path)
    }

    fn remove_dir(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        self.inner.metadata(path)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        self.inner.sync_directory(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        self.inner.exists(path)
    }

    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()> {
        self.inner.hard_link(src, dst)
    }

    fn truncate_file(&self, path: &Path) -> io::Result<()> {
        self.inner.truncate_file(path)
    }

    fn capabilities(&self, path: &Path) -> FsCapabilities {
        let mut capabilities = self.inner.capabilities(path);
        capabilities.serial_syncs = self.serial;
        capabilities
    }
}

struct WatchedFile {
    inner: Box<dyn FsFile>,
    syncs: Arc<InFlight>,
}

impl std::io::Read for WatchedFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl std::io::Write for WatchedFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl std::io::Seek for WatchedFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl FsFile for WatchedFile {
    fn sync_all(&self) -> io::Result<()> {
        self.syncs.sync();
        self.inner.sync_all()
    }

    fn sync_data(&self) -> io::Result<()> {
        self.syncs.sync();
        self.inner.sync_data()
    }

    fn metadata(&self) -> io::Result<FsMetadata> {
        self.inner.metadata()
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        self.inner.set_len(size)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.inner.read_at(buf, offset)
    }

    fn lock_exclusive(&self) -> io::Result<()> {
        self.inner.lock_exclusive()
    }

    fn try_lock_exclusive(&self) -> io::Result<bool> {
        self.inner.try_lock_exclusive()
    }
}

/// The most file syncs in flight at once during one synced flush of a blob
/// tree (its blob file and its table) on a backend reporting `serial`.
fn most_syncs_in_one_flush(serial: bool) -> lsm_tree::Result<usize> {
    let syncs = Arc::new(InFlight {
        meet: !serial,
        ..InFlight::default()
    });
    let fs = Watched {
        inner: MemFs::new(),
        serial,
        syncs: Arc::clone(&syncs),
    };
    let tree = Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(fs)
    .table_file_pool(0)
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;
    for i in 0..100u64 {
        tree.insert(format!("key{i:03}"), "value", i);
    }
    syncs
        .counts
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .1 = 0;
    tree.flush_active_memtable(0)?;
    Ok(syncs.most())
}

/// On a backend whose concurrent syncs cost more than serial ones (NTFS), a
/// flush that synced its files side by side took about 20 ms where one after
/// another took under 1 ms.
#[test]
fn a_flush_syncs_one_file_at_a_time_where_the_backend_asks_for_it() -> lsm_tree::Result<()> {
    assert_eq!(most_syncs_in_one_flush(true)?, 1);
    Ok(())
}

/// The twin: elsewhere the table and the blob file are synced side by side,
/// which also shows the count above can see two syncs at once.
#[test]
fn a_flush_syncs_its_table_and_blob_file_side_by_side_elsewhere() -> lsm_tree::Result<()> {
    assert!(most_syncs_in_one_flush(false)? >= 2);
    Ok(())
}
