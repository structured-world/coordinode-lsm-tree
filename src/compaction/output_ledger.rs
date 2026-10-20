// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The files one compaction run creates, held until a version names them.

use crate::fs::Fs;
use crate::path::PathBuf;
use crate::tree::inner::TreeId;
use crate::{DescriptorTable, GlobalTableId};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
// no-std: spin mirrors parking_lot's Mutex API without an allocator.
#[cfg(feature = "std")]
use parking_lot::Mutex;
#[cfg(not(feature = "std"))]
use spin::Mutex;

/// What a recorded file is, which names the descriptor-table slot an open
/// handle on it may still occupy.
#[derive(Clone, Copy)]
enum Kind {
    Table,
    BlobFile,
}

struct Output {
    kind: Kind,
    id: u64,
    path: PathBuf,
    fs: Arc<dyn Fs>,
}

/// The table and blob files a compaction run has created that no installed
/// version names, and the room the run reserved for them.
///
/// Each writer the run opens records a file here once it has created it, and
/// tells it once it has finished one; each install of the run's outputs
/// empties the list. Whatever is left when the run returns was never
/// installed, finished or not, so [`Self::remove_uninstalled`] deletes it in
/// one place rather than every failure site rolling back the outputs it
/// happens to hold, and gives the run's room back only then: a file not yet
/// removed still takes the room it landed in. Clones share one ledger, so the
/// sub-compactions of a run record into the same list.
#[derive(Clone, Default)]
pub struct OutputLedger(Arc<Ledger>);

#[derive(Default)]
struct Ledger {
    files: Mutex<Vec<Output>>,
    room: Mutex<Option<Arc<super::capacity::Reservation>>>,
}

impl OutputLedger {
    /// Records a table file the run created at `path`.
    pub fn record_table(&self, id: crate::TableId, path: PathBuf, fs: Arc<dyn Fs>) {
        self.record(Kind::Table, id, path, fs);
    }

    /// Records a blob file the run created at `path`.
    pub fn record_blob_file(&self, id: crate::vlog::BlobFileId, path: PathBuf, fs: Arc<dyn Fs>) {
        self.record(Kind::BlobFile, id, path, fs);
    }

    fn record(&self, kind: Kind, id: u64, path: PathBuf, fs: Arc<dyn Fs>) {
        self.0.files.lock().push(Output { kind, id, path, fs });
    }

    /// Holds `room`, reserved for the run's outputs, until the run returns.
    pub fn reserve(&self, room: super::capacity::Reservation) {
        // The room it replaces, a finished slice's, goes back as it drops.
        let replaced = self.0.room.lock().replace(Arc::new(room));
        drop(replaced);
    }

    /// Gives the room of a finished slice back before the next one is checked,
    /// so the next is not checked against room its predecessor no longer
    /// needs.
    pub fn release_room(&self) {
        let released = self.0.room.lock().take();
        drop(released);
    }

    /// The run's room, if it holds one.
    fn room(&self) -> Option<Arc<super::capacity::Reservation>> {
        self.0.room.lock().clone()
    }

    /// Adds `demand` to the run's room when it fits under `check`, checked and
    /// taken in one step; a run holding no room only checks it against `pool`
    /// for `tree`.
    pub fn try_reserve_more(
        &self,
        pool: &super::capacity::CapacityPool,
        tree: crate::tree::inner::TreeId,
        demand: &super::capacity::Demand<'_>,
        check: super::capacity::Check,
    ) -> bool {
        match self.room() {
            Some(room) => room.try_add(demand, check),
            None => pool.would_fit(tree, core::slice::from_ref(demand), check),
        }
    }

    /// The file at `path` is finished: the bytes it holds land on its volume
    /// and are taken from the run's room.
    ///
    /// # Errors
    ///
    /// Any error reading the file's length, and
    /// [`crate::Error::StorageFull`] when the volume has no room for what the
    /// run wrote past its reservation.
    pub fn landed(&self, fs: &dyn Fs, path: &crate::path::Path) -> crate::Result<()> {
        let Some(room) = self.room() else {
            return Ok(());
        };
        // Charged to the folder, which the room was reserved for: a backend
        // may tell the folder's volume and not the file's.
        room.landed(fs, path.parent().unwrap_or(path), fs.metadata(path)?.len)
    }

    /// Whether the run's room stopped it: its outputs outgrew the room and
    /// the volume had none left for them.
    pub fn out_of_room(&self) -> bool {
        self.room().is_some_and(|room| room.refused())
    }

    /// A version now names every recorded file, so they are no longer the
    /// run's to remove.
    pub fn installed(&self) {
        self.0.files.lock().clear();
    }

    /// Leaves every recorded file in place, after an install whose outcome is
    /// unknown: a power loss may still make it the installed one, naming
    /// them. They and the quota bytes they take are handed back as
    /// [`KeptOutputs`], to go once an install proves no version names them.
    pub fn keep_uninstalled(&self) -> KeptOutputs {
        let files = core::mem::take(&mut *self.0.files.lock());
        let room = self.0.room.lock().take();
        let hold = room.as_ref().map(|room| {
            // `None` once a file's length cannot be read: the room keeps all
            // it granted.
            room.strand(files.iter().try_fold(0u64, |sum, output| {
                Some(sum + output.fs.metadata(&output.path).ok()?.len)
            }))
        });
        drop(room);
        KeptOutputs { files, hold }
    }

    /// Deletes every recorded file, then gives the run's room back. Called
    /// once the run has returned, when no handle it opened on these files is
    /// left: each one's cached descriptor is evicted first, since a backend
    /// can refuse to unlink an open file. A file already gone (a writer that
    /// removed its own empty output, a rolled-back handle) is skipped; one
    /// that cannot be removed stays for the orphan sweep of the next open, and
    /// keeps its bytes held in the tree's quota until then.
    pub fn remove_uninstalled(&self, tree_id: TreeId, descriptor_table: Option<&DescriptorTable>) {
        let outputs = core::mem::take(&mut *self.0.files.lock());
        // Given back once the files it covered are gone, below.
        let room = self.0.room.lock().take();
        let stranded = remove_files(outputs, tree_id, descriptor_table);
        if let Some(room) = &room {
            // Held until the tree leaves the pool: the orphan sweep of its
            // next open is what removes these.
            drop(room.strand(stranded));
        }
        drop(room);
    }
}

/// The outputs of a run whose install had an unknown outcome, left on disk
/// with the quota bytes they take.
///
/// The install that failed marks the next one to write a full snapshot, which
/// supersedes the edit whichever way it went: from then on no version names
/// these files, on disk or in memory, and [`Self::remove`] takes them.
pub struct KeptOutputs {
    files: Vec<Output>,
    hold: Option<super::capacity::QuotaHold>,
}

impl KeptOutputs {
    /// Deletes the files, as [`OutputLedger::remove_uninstalled`] deletes
    /// uninstalled ones, and gives back the quota of each one gone.
    pub fn remove(self, tree_id: TreeId, descriptor_table: Option<&DescriptorTable>) {
        let stranded = remove_files(self.files, tree_id, descriptor_table);
        if let Some(hold) = self.hold {
            hold.release_but(stranded);
        }
    }
}

/// Removes `outputs`, each one's cached descriptor evicted first, and returns
/// the bytes of those that could not be removed (`None` once a length cannot
/// be read).
fn remove_files(
    outputs: Vec<Output>,
    tree_id: TreeId,
    descriptor_table: Option<&DescriptorTable>,
) -> Option<u64> {
    let mut stranded = Some(0u64);
    for output in outputs {
        let global_id = GlobalTableId::from((tree_id, output.id));
        if let Some(descriptors) = descriptor_table {
            match output.kind {
                Kind::Table => descriptors.remove_for_table(&global_id),
                Kind::BlobFile => descriptors.remove_for_blob_file(&global_id),
            }
        }
        match output.fs.remove_file(&output.path) {
            Ok(()) => log::debug!(
                "Removed uninstalled compaction output {}",
                output.path.display(),
            ),
            Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {}
            Err(e) => {
                log::warn!(
                    "Could not remove uninstalled compaction output {} ({e}); it stays until \
                     the next open's orphan sweep",
                    output.path.display(),
                );
                // A length that cannot be read holds the whole grant: the
                // quota is a limit, and the file may take all of it. Bounded
                // by the bytes the run wrote.
                stranded = stranded
                    .zip(output.fs.metadata(&output.path).ok())
                    .map(|(sum, metadata)| sum + metadata.len);
            }
        }
    }
    stranded
}

#[cfg(test)]
mod tests;
