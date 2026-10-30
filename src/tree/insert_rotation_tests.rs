// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Inserts racing the rotations and flushes that seal the memtable they write
//! to: every insert lands in a memtable some flush or the active one still
//! holds, none in one a flush has already read.

use crate::{AbstractTree, Config, SeqNo, SequenceNumberCounter};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

const WRITERS: u64 = 4;
/// Keys each writer writes at least; it goes on past them until the flusher
/// has sealed `OVERLAPPED` memtables under the writes.
const PER_WRITER: u64 = 20_000;
/// Rotations that must seal written keys while writers are active.
const OVERLAPPED: u32 = 2;

fn key(writer: u64, i: u64) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&writer.to_be_bytes());
    key[8..].copy_from_slice(&i.to_be_bytes());
    key
}

/// Raises its flag as it drops, so a loop waiting on it stops on every way
/// out, a panic or an error included, and the test fails rather than hangs.
struct RaiseOnDrop<'a>(&'a AtomicBool);

impl Drop for RaiseOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Writers insert while another thread flushes over and over, until rotations
/// have sealed written keys while the writers were writing; afterwards every
/// key reads back. Guards against an insert taking the active memtable, the
/// rotation sealing it, and the flush reading it before the insert lands.
#[test]
fn inserts_racing_flushes_all_read_back() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let done = AtomicBool::new(false);
    let flusher_gone = AtomicBool::new(false);
    // Writers still writing, and the rotations that wrote a table while one
    // was writing both before and after it.
    let active = AtomicU64::new(WRITERS);
    let overlapped = AtomicU32::new(0);

    let written = std::thread::scope(|scope| -> crate::Result<Vec<u64>> {
        let stop = RaiseOnDrop(&done);
        let flusher = scope.spawn(|| -> crate::Result<()> {
            let _gone = RaiseOnDrop(&flusher_gone);
            while !done.load(Ordering::Acquire) {
                let writing = active.load(Ordering::Acquire) > 0;
                let tables = tree.table_count();
                tree.flush_active_memtable(0)?;
                // An empty rotation writes no table; one that ended after the
                // writers did proves nothing about racing them.
                if writing && tree.table_count() > tables && active.load(Ordering::Acquire) > 0 {
                    overlapped.fetch_add(1, Ordering::AcqRel);
                }
            }
            Ok(())
        });
        let writers: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let (tree, active, overlapped, flusher_gone) =
                    (&tree, &active, &overlapped, &flusher_gone);
                scope.spawn(move || {
                    let mut i = 0;
                    while i < PER_WRITER
                        || (overlapped.load(Ordering::Acquire) < OVERLAPPED
                            && !flusher_gone.load(Ordering::Acquire))
                    {
                        // Distinct per writer while `i < 2^32`, far past here.
                        let seqno: SeqNo = (writer << 32) + i;
                        tree.insert(key(writer, i), b"value", seqno);
                        i += 1;
                    }
                    active.fetch_sub(1, Ordering::AcqRel);
                    i
                })
            })
            .collect();
        let written = writers
            .into_iter()
            .map(|writer| writer.join().expect("writer"))
            .collect();
        drop(stop);
        flusher.join().expect("flusher")?;
        assert!(
            overlapped.load(Ordering::Acquire) >= OVERLAPPED,
            "rotations sealed written keys while the writers wrote"
        );
        Ok(written)
    })?;
    tree.flush_active_memtable(0)?;

    for (writer, &count) in (0..WRITERS).zip(&written) {
        for i in 0..count {
            assert_eq!(
                tree.get(key(writer, i), SeqNo::MAX)?.as_deref(),
                Some(b"value".as_slice()),
                "writer {writer} key {i}"
            );
        }
    }
    Ok(())
}
