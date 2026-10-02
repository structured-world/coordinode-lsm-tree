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
/// version names.
///
/// Each writer the run opens records a file here once it has created it, and
/// each install of the run's outputs empties the ledger. Whatever is left when
/// the run returns was never installed, finished or not, so
/// [`Self::remove_uninstalled`] deletes it in one place rather than every
/// failure site rolling back the outputs it happens to hold. Clones share one
/// ledger, so the sub-compactions of a run record into the same list.
#[derive(Clone, Default)]
pub struct OutputLedger(Arc<Mutex<Vec<Output>>>);

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
        self.0.lock().push(Output { kind, id, path, fs });
    }

    /// A version now names every recorded file, so they are no longer the
    /// run's to remove.
    pub fn installed(&self) {
        self.0.lock().clear();
    }

    /// Deletes every recorded file. Called once the run has returned, when no
    /// handle it opened on these files is left: each one's cached descriptor is
    /// evicted first, since a backend can refuse to unlink an open file. A file
    /// already gone (a writer that removed its own empty output, a rolled-back
    /// handle) is skipped; one that cannot be removed stays for the orphan
    /// sweep of the next open.
    pub fn remove_uninstalled(&self, tree_id: TreeId, descriptor_table: Option<&DescriptorTable>) {
        let outputs = core::mem::take(&mut *self.0.lock());
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
                Err(e) => log::warn!(
                    "Could not remove uninstalled compaction output {} ({e}); it stays until \
                     the next open's orphan sweep",
                    output.path.display(),
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests;
