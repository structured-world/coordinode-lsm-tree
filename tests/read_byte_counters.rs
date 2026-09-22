// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The three read-path byte counters, asserted against their definitions.
//!
//! `bytes_read`, `bytes_decoded` and `bytes_copied` are the instrument the
//! mixed-layout work states its acceptance in, so what they MEAN has to be
//! pinned by a test rather than by a doc comment alone. Each of these asserts
//! one clause of the definition:
//!
//! * read counts what was asked of the `Fs` trait, so a read served from the
//!   block cache adds nothing;
//! * decoded counts what the block transform produced, so it too is a
//!   property of the uncached path — and it exceeds read exactly when the
//!   bytes were compressed, which is what separates a physical projection
//!   from a cosmetic one;
//! * copied counts gathers, so a path that streams its input untouched
//!   reports zero and one that folds batches together reports more than it
//!   returns.
//!
//! A change that moves one of these without moving the behaviour it stands
//! for makes the whole instrument lie, which is why they are tested at all.

#![cfg(all(feature = "metrics", feature = "columnar"))]

use lsm_tree::{
    AbstractTree, AnyTree, CompressionType, Config, SeqNo, SequenceNumberCounter,
    config::CompressionPolicy, get_tmp_folder,
};
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

/// A tree of `n` rows with `value_len`-byte values, flushed, under the given
/// compression. Returns the tree so the caller can read it and inspect the
/// counters.
fn filled_tree(n: u32, value_len: usize, compression: CompressionType) -> AnyTree {
    let folder = get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(compression))
    .open()
    .expect("open");
    // Leak the tempdir with the tree: dropping it here would remove the files
    // under the open tree, and every assertion below is about reading them.
    core::mem::forget(folder);
    for i in 0..n {
        tree.insert(key(i), vec![b'v'; value_len], u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush");
    tree
}

#[test]
fn a_read_served_from_the_block_cache_counts_no_bytes() {
    // The clause: read and decoded are both properties of the UNCACHED path.
    // Read is what was asked of the filesystem, and a cache hit asks for
    // nothing; decoded is what the transform produced, and a cached block is
    // already decoded so no transform runs. A counter that ticked on a cache
    // hit would report a tree that never touches disk as doing I/O.
    let tree = filled_tree(2_000, 64, CompressionType::None);
    let m = tree.metrics();

    for i in 0..2_000 {
        let _ = tree.get(key(i), SeqNo::MAX).expect("get");
    }
    let (read_cold, decoded_cold) = (m.bytes_read(), m.bytes_decoded());
    assert!(read_cold > 0, "a cold pass must report bytes read");
    assert!(decoded_cold > 0, "a cold pass must report bytes decoded");

    // Same pass again, now entirely from cache.
    for i in 0..2_000 {
        let _ = tree.get(key(i), SeqNo::MAX).expect("get");
    }
    assert_eq!(
        m.bytes_read(),
        read_cold,
        "a cached read asked the filesystem for nothing, so read must not move",
    );
    assert_eq!(
        m.bytes_decoded(),
        decoded_cold,
        "a cached block is already decoded, so decoded must not move",
    );
}

#[test]
fn compression_makes_decoded_exceed_read() {
    // The clause that gives the pair its purpose. Read alone cannot tell a
    // 4 KiB block that holds 4 KiB from one that expands to 64 KiB, and it is
    // the second that a projection loading a whole wide block pays for. The
    // ratio between the two IS the compression the read paid for.
    let compressible = 4_096_usize;

    let plain = filled_tree(4_000, compressible, CompressionType::None);
    for i in 0..4_000 {
        let _ = plain.get(key(i), SeqNo::MAX).expect("get");
    }
    let plain_m = plain.metrics();
    let (plain_read, plain_decoded) = (plain_m.bytes_read(), plain_m.bytes_decoded());

    // Uncompressed: the transform is the identity, so decoded is the payload
    // inside what was read — never more than it.
    assert!(
        plain_decoded <= plain_read,
        "an identity transform cannot produce more than it was given: \
         decoded {plain_decoded} > read {plain_read}",
    );

    let lz4 = filled_tree(4_000, compressible, CompressionType::Lz4);
    for i in 0..4_000 {
        let _ = lz4.get(key(i), SeqNo::MAX).expect("get");
    }
    let lz4_m = lz4.metrics();
    let (lz4_read, lz4_decoded) = (lz4_m.bytes_read(), lz4_m.bytes_decoded());

    // Runs of one byte compress hard, so the expansion is unmistakable.
    assert!(
        lz4_decoded > lz4_read,
        "compressed blocks must decode to more than was read: \
         decoded {lz4_decoded} vs read {lz4_read}",
    );
    assert!(
        lz4_read < plain_read,
        "compression must reduce what is asked of the filesystem: \
         {lz4_read} vs {plain_read}",
    );
}

#[test]
fn streaming_a_single_segment_copies_nothing() {
    // The clause: copied counts GATHERS — building a new buffer from bytes
    // that already exist in another. A scan over one segment whose rows are
    // returned untouched builds nothing, so the counter must stay at zero.
    // If it moves here, the path is materialising something it does not need
    // to, which is precisely what the counter exists to expose.
    let tree = filled_tree(1_000, 32, CompressionType::None);
    let m = tree.metrics();
    let before = m.bytes_copied();

    let scanned = tree.range(key(0)..key(1_000_000), SeqNo::MAX, None).count();
    assert_eq!(scanned, 1_000, "the scan must see every row");

    assert_eq!(
        m.bytes_copied(),
        before,
        "a straight range scan gathers nothing and must not move the counter",
    );
}
