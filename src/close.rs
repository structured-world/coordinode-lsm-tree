// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Closing a tree, and learning when it no longer touches its directory.
//!
//! Everything of a tree that owns a file of its directory or works in it holds
//! the tree's [`Life`]: the tree handles, the table file pool (joined refills
//! included), the background deleter until it drained its queue, and, through
//! the deletion pause they all share, every table, blob file and pin. A tree
//! is closed once the last of them drops; [`TreeClose`] reports that moment.

use crate::{AnyTree, BlobTree, Tree, path::PathBuf, tree::inner::TreeInner};
use alloc::{boxed::Box, sync::Arc, sync::Weak, vec::Vec};
use core::sync::atomic::{AtomicBool, Ordering};
// no-std: spin::Mutex (parking_lot is std-only); waiting needs std.
#[cfg(feature = "std")]
use parking_lot::Mutex;
#[cfg(not(feature = "std"))]
use spin::Mutex;

/// A callback waiting for a tree to close.
type OnClosed = Box<dyn FnOnce() + Send>;

/// What a tree's closing signal shares with its [`TreeClose`] handles.
struct Signal {
    closed: AtomicBool,
    /// The callbacks to run once closed; emptied by the closing.
    waiting: Mutex<Vec<OnClosed>>,
    /// Woken once closed, for the callers that block.
    #[cfg(feature = "std")]
    settled: parking_lot::Condvar,
}

impl Signal {
    fn fire(&self) {
        let callbacks = {
            let mut waiting = self.waiting.lock();
            self.closed.store(true, Ordering::Release);
            #[cfg(feature = "std")]
            self.settled.notify_all();
            core::mem::take(&mut *waiting)
        };
        // Outside the lock: a callback may register another, or wait.
        for callback in callbacks {
            run(callback);
        }
    }
}

/// Runs a callback of a closing tree. It runs inside a drop of the tree's own
/// parts, possibly while a panic unwinds, or at registration on the caller's
/// thread; either way its panic is contained, so it aborts no process and
/// keeps no other callback from running.
fn run(callback: impl FnOnce()) {
    #[cfg(feature = "std")]
    if std::panic::catch_unwind(core::panic::AssertUnwindSafe(callback)).is_err() {
        log::error!("a TreeClose callback panicked; the others still run");
    }
    #[cfg(not(feature = "std"))]
    callback();
}

/// A tree's liveness: held, as an `Arc`, by everything of the tree that owns
/// a file of its directory. Its last drop closes the tree.
pub struct Life {
    signal: Arc<Signal>,
}

impl Life {
    /// The liveness of a tree being opened.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            signal: Arc::new(Signal {
                closed: AtomicBool::new(false),
                waiting: Mutex::new(Vec::new()),
                #[cfg(feature = "std")]
                settled: parking_lot::Condvar::new(),
            }),
        })
    }

    /// A handle on the signal, holding no liveness itself.
    pub fn signal(&self) -> TreeClose {
        TreeClose {
            signal: Arc::clone(&self.signal),
        }
    }
}

impl Drop for Life {
    fn drop(&mut self) {
        self.signal.fire();
    }
}

/// The signal of a tree being closed, returned by
/// [`Tree::close`](crate::Tree::close) and its counterparts.
///
/// A tree is closed once nothing of it owns a file of its directory or works
/// in it any more: no tree handle, iterator, snapshot or pin holds it, its
/// table file pool made its last file and the background deleter removed
/// the last file it was handed. From then on the tree writes nothing into its
/// directory, and removing the directory is safe.
///
/// The handle keeps nothing open itself, nor do the callbacks registered on
/// it, so it can be held for as long as wanted.
#[derive(Clone)]
#[must_use = "the signal is the only way to learn when the tree is closed"]
pub struct TreeClose {
    signal: Arc<Signal>,
}

impl TreeClose {
    /// Runs `f` once the tree is closed, on the thread that finished closing
    /// it, or at once on this thread if it is closed already. Nothing waits or
    /// polls meanwhile: an async caller completes its own oneshot from it.
    ///
    /// `f` runs inside the drop that closed the tree, so it should be short.
    /// With `std` a panic in `f` is caught and logged, and the other
    /// callbacks still run.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use std::sync::mpsc;
    ///
    /// let tree = lsm_tree::Config::new(
    ///     folder.path(),
    ///     lsm_tree::SequenceNumberCounter::default(),
    ///     lsm_tree::SequenceNumberCounter::default(),
    /// )
    /// .open()?;
    ///
    /// let (closed, on_closed) = mpsc::channel();
    /// // The receiver may be gone by then: a failed send is nothing to report.
    /// tree.close().on_closed(move || {
    ///     let _ = closed.send(());
    /// });
    /// on_closed.recv()?;
    /// std::fs::remove_dir_all(folder.path())?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn on_closed(&self, f: impl FnOnce() + Send + 'static) {
        let mut waiting = self.signal.waiting.lock();
        if self.signal.closed.load(Ordering::Acquire) {
            drop(waiting);
            run(f);
        } else {
            waiting.push(Box::new(f));
        }
    }

    /// Whether the tree is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.signal.closed.load(Ordering::Acquire)
    }

    /// Blocks until the tree is closed.
    ///
    /// Blocks for good if the caller itself still holds a tree handle, an
    /// iterator, a snapshot or a pin of the tree: drop them first, or use
    /// [`on_closed`](Self::on_closed).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::AbstractTree;
    ///
    /// let tree = lsm_tree::Config::new(
    ///     folder.path(),
    ///     lsm_tree::SequenceNumberCounter::default(),
    ///     lsm_tree::SequenceNumberCounter::default(),
    /// )
    /// .open()?;
    /// tree.insert("a", "1", 0);
    /// tree.flush_active_memtable(0)?;
    ///
    /// tree.close().wait();
    /// std::fs::remove_dir_all(folder.path())?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[cfg(feature = "std")]
    pub fn wait(&self) {
        let mut waiting = self.signal.waiting.lock();
        while !self.signal.closed.load(Ordering::Acquire) {
            self.signal.settled.wait(&mut waiting);
        }
    }

    /// Blocks until the tree is closed or `timeout` passes; whether it is
    /// closed.
    ///
    /// Returns `false` at the timeout while anything of the tree is held, the
    /// caller's own handles, iterators, snapshots and pins included.
    #[cfg(feature = "std")]
    #[must_use]
    pub fn wait_timeout(&self, timeout: core::time::Duration) -> bool {
        let mut waiting = self.signal.waiting.lock();
        let _ = self.signal.settled.wait_while_for(
            &mut waiting,
            |_| !self.signal.closed.load(Ordering::Acquire),
            timeout,
        );
        self.signal.closed.load(Ordering::Acquire)
    }
}

impl core::fmt::Debug for TreeClose {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TreeClose")
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// A handle on a tree that does not keep it open, made by
/// [`Tree::downgrade`](crate::Tree::downgrade) and its counterparts.
///
/// For a holder that only acts on a live tree, such as a compaction monitor
/// or a queued flush request: it lets a dropped or closing tree close without
/// waiting for the holder.
#[derive(Clone)]
pub struct WeakTree {
    inner: Weak<TreeInner>,
    /// The blob folder of a key-value-separated tree.
    blobs_folder: Option<Arc<PathBuf>>,
}

impl WeakTree {
    pub(crate) fn of(tree: &Tree, blobs_folder: Option<Arc<PathBuf>>) -> Self {
        Self {
            inner: Arc::downgrade(&tree.0),
            blobs_folder,
        }
    }

    /// A strong handle on the tree, or `None` once every strong handle is
    /// gone or the tree is being closed.
    ///
    /// A tree being closed is not raised again, even while strong handles
    /// taken earlier still finish their work, so a holder woken in between
    /// starts no new work the closing would then wait for.
    #[must_use]
    pub fn upgrade(&self) -> Option<AnyTree> {
        let inner = self.inner.upgrade()?;
        #[cfg(test)]
        crate::tree::inner::TestHooks::fire(&inner.test_hooks.upgrade);
        // The one point `close` and `upgrade` meet: read after the strong
        // handle is taken, so a flag read clear means `close` comes after,
        // and the closing waits for this handle like any other.
        if inner.closing.load(Ordering::SeqCst) {
            return None;
        }
        let index = Tree(inner);
        Some(match &self.blobs_folder {
            None => AnyTree::Standard(index),
            Some(blobs_folder) => AnyTree::Blob(BlobTree {
                index,
                blobs_folder: Arc::clone(blobs_folder),
            }),
        })
    }
}

impl core::fmt::Debug for WeakTree {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WeakTree")
            .field("blob", &self.blobs_folder.is_some())
            .finish_non_exhaustive()
    }
}

/// Gives up `tree`'s handle and returns its closing signal: from here no weak
/// handle raises the tree again.
pub fn close(tree: Tree) -> TreeClose {
    let signal = tree.life.signal();
    tree.closing.store(true, Ordering::SeqCst);
    drop(tree);
    signal
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::expect_used, reason = "test code")]
mod tests;
