// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Inserts racing the rotations and flushes that seal the memtable they write
//! to: every insert lands in a memtable some flush or the active one still
//! holds, none in one a flush has already read.

use crate::{AbstractTree, Config, SeqNo, SequenceNumberCounter};
use std::sync::atomic::{AtomicBool, Ordering};

const WRITERS: u64 = 4;
const PER_WRITER: u64 = 20_000;

fn key(writer: u64, i: u64) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&writer.to_be_bytes());
    key[8..].copy_from_slice(&i.to_be_bytes());
    key
}

/// Writers insert while another thread flushes over and over; afterwards every
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

    std::thread::scope(|scope| -> crate::Result<()> {
        let flusher = scope.spawn(|| -> crate::Result<u32> {
            let mut flushes = 0;
            while !done.load(Ordering::Acquire) {
                tree.flush_active_memtable(0)?;
                flushes += 1;
            }
            Ok(flushes)
        });
        let writers: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let tree = &tree;
                scope.spawn(move || {
                    for i in 0..PER_WRITER {
                        let seqno: SeqNo = writer * PER_WRITER + i;
                        tree.insert(key(writer, i), b"value", seqno);
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.join().expect("writer");
        }
        done.store(true, Ordering::Release);
        let flushes = flusher.join().expect("flusher")?;
        assert!(flushes > 1, "the flushes ran while the writers wrote");
        Ok(())
    })?;
    tree.flush_active_memtable(0)?;

    for writer in 0..WRITERS {
        for i in 0..PER_WRITER {
            assert_eq!(
                tree.get(key(writer, i), SeqNo::MAX)?.as_deref(),
                Some(b"value".as_slice()),
                "writer {writer} key {i}"
            );
        }
    }
    Ok(())
}
