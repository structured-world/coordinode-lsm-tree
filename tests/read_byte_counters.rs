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

use lsm_tree::table::columnar::{COL_USER_KEY, COL_VALUE, ColumnBatch};
use lsm_tree::table::columnar_predicate::ColumnRangePredicate;
use lsm_tree::{
    AbstractTree, AnyTree, CompressionType, Config, Guard, SeqNo, SequenceNumberCounter, Tree,
    UserKey, config::CompressionPolicy, get_tmp_folder,
};
use tempfile::TempDir;
use test_log::test;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

/// A tree of `n` rows with `value_len`-byte values, flushed, under the given
/// compression. The directory comes back with the tree: the caller holds it
/// for as long as it reads, and dropping it afterwards removes the files.
fn filled_tree(n: u32, value_len: usize, compression: CompressionType) -> (TempDir, AnyTree) {
    let folder = get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(compression))
    .open()
    .expect("open");
    for i in 0..n {
        tree.insert(key(i), vec![b'v'; value_len], u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush");
    (folder, tree)
}

/// A columnar tree holding `n` unique keys in one flushed segment, so every
/// scan below takes the single-segment path.
fn columnar_segment(n: u32, value_len: usize) -> (TempDir, Tree) {
    let folder = get_tmp_folder();
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("open") else {
        panic!("expected a standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
    })
    .expect("enable columnar");
    for i in 0..n {
        tree.insert(key(i), vec![b'v'; value_len], u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush");
    (folder, tree)
}

/// Bytes a caller holds after a scan: every column's data plus its validity
/// bitmap, the same measure the gather counter charges per built batch.
fn batch_bytes(batch: &ColumnBatch) -> u64 {
    batch
        .columns
        .iter()
        .map(|c| (c.data.len() + c.validity.as_ref().map_or(0, Vec::len)) as u64)
        .sum()
}

#[test]
fn a_read_served_from_the_block_cache_counts_no_bytes() {
    // The clause: read and decoded are both properties of the UNCACHED path.
    // Read is what was asked of the filesystem, and a cache hit asks for
    // nothing; decoded is what the transform produced, and a cached block is
    // already decoded so no transform runs. A counter that ticked on a cache
    // hit would report a tree that never touches disk as doing I/O.
    let (_folder, tree) = filled_tree(2_000, 64, CompressionType::None);
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
#[cfg(feature = "lz4")]
fn compression_makes_decoded_exceed_read() {
    // The clause that gives the pair its purpose. Read alone cannot tell a
    // 4 KiB block that holds 4 KiB from one that expands to 64 KiB, and it is
    // the second that a projection loading a whole wide block pays for. The
    // ratio between the two IS the compression the read paid for.
    let compressible = 4_096_usize;

    let (_plain_folder, plain) = filled_tree(4_000, compressible, CompressionType::None);
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

    let (_lz4_folder, lz4) = filled_tree(4_000, compressible, CompressionType::Lz4);
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
fn resolving_a_separated_value_counts_the_blob_it_read() {
    // The clause that stops a key-value-separated tree from reading gigabytes
    // while reporting only its indirections. A separated value leaves the
    // filesystem through the blob path, not through a block, so if the blob
    // read went uncounted, a change that moved work into it would look like an
    // improvement.
    let folder = get_tmp_folder();
    let value_len = 8_192;
    let n = 200_u32;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .with_kv_separation(Some(Default::default()))
    .open()
    .expect("open");
    for i in 0..n {
        tree.insert(key(i), vec![b'v'; value_len], u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush");

    let m = tree.metrics();
    let before = (m.bytes_read(), m.blob_bytes_read(), m.bytes_decoded());

    for i in 0..n {
        let got = tree.get(key(i), SeqNo::MAX).expect("get").expect("present");
        assert_eq!(got.len(), value_len, "the whole value must come back");
    }

    let blob_read = m.blob_bytes_read() - before.1;
    let total_read = m.bytes_read() - before.0;
    let decoded = m.bytes_decoded() - before.2;
    let payload = u64::from(n) * value_len as u64;

    // Read is the ON-DISK span asked of the filesystem, decoded is what came
    // out of it. A blob file compresses, and a run of one byte compresses
    // hard, so the two must sit on opposite sides of the payload: anything
    // that reported them as equal would be measuring the same number twice
    // under two names.
    assert!(
        blob_read > 0,
        "resolving a separated value read no blob bytes"
    );
    assert!(
        blob_read < payload,
        "blob reads {blob_read} B for {payload} B of values; a compressed blob \
         file cannot be asked for more than it holds",
    );
    assert!(
        decoded >= payload,
        "decoded {decoded} B is less than the {payload} B of values returned",
    );
    assert!(
        total_read > blob_read,
        "the total must also carry the indirection blocks: {total_read} vs {blob_read}",
    );

    // Reading the same keys again is served from the blob cache, which asks
    // the filesystem for nothing — the same clause the block path is held to.
    let cached_from = m.blob_bytes_read();
    for i in 0..n {
        let _ = tree.get(key(i), SeqNo::MAX).expect("get");
    }
    assert_eq!(
        m.blob_bytes_read(),
        cached_from,
        "a cached blob read asked the filesystem for nothing, so read must not move",
    );
}

#[test]
fn a_blob_read_rejected_as_corrupt_still_counts_its_bytes() {
    // Read is what was asked of the filesystem, and a record that fails its
    // checksum was asked for all the same. Charging it only after the record
    // validates would hide exactly the reads a failing disk makes, and would
    // disagree with the prefetch path, which charges its span before parsing.
    let folder = get_tmp_folder();
    let value_len = 8_192;
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(Default::default()))
    .open()
    .expect("open");
    // Incompressible, so the record fills most of the blob file and the byte
    // flipped below lands in its payload rather than in the file's metadata.
    let mut state = 0x9E37_79B9_u32;
    let value: Vec<u8> = (0..value_len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state.to_le_bytes()[0]
        })
        .collect();
    tree.insert(key(0), value, 0);
    tree.flush_active_memtable(0).expect("flush");

    // Flip one byte inside the only blob record's payload.
    let blob_dir = folder.path().join("blobs");
    let blob_file = std::fs::read_dir(&blob_dir)
        .expect("blob dir")
        .map(|e| e.expect("entry").path())
        .find(|p| p.is_file())
        .expect("one blob file");
    let mut bytes = std::fs::read(&blob_file).expect("read blob file");
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xFF;
    std::fs::write(&blob_file, &bytes).expect("write blob file");

    let m = tree.metrics();
    let before = m.blob_bytes_read();
    assert!(
        tree.get(key(0), SeqNo::MAX).is_err(),
        "a corrupt blob record must fail the read",
    );
    assert!(
        m.blob_bytes_read() > before,
        "the corrupt record was read from the filesystem, so read must move",
    );
}

#[test]
fn streaming_a_single_segment_copies_nothing() {
    // The clause: copied counts GATHERS — building a new buffer from bytes
    // that already exist in another. A scan over one segment whose rows are
    // returned untouched builds nothing, so the counter must stay at zero.
    // If it moves here, the path is materialising something it does not need
    // to, which is precisely what the counter exists to expose.
    let (_folder, tree) = filled_tree(1_000, 32, CompressionType::None);
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

#[test]
fn a_range_bounded_columnar_scan_of_one_segment_counts_its_filter_gather() {
    // A bounded range over a single segment masks the rows outside it, which
    // builds a new batch from the decoded one. That is a gather whether or not
    // other segments overlap, so it must be charged exactly as the overlapping
    // merge's filter is; otherwise a singleton layout reports zero copies for
    // the same work and wins every comparison by not being instrumented.
    let (_folder, tree) = columnar_segment(1_000, 32);
    let m = tree.metrics();
    let before = m.bytes_copied();

    let mut returned = 0;
    let mut rows = 0;
    for batch in tree
        .columnar_scan(
            &[COL_USER_KEY, COL_VALUE],
            None,
            SeqNo::MAX,
            UserKey::from(key(100))..UserKey::from(key(200)),
        )
        .expect("scan")
    {
        let batch = batch.expect("batch");
        rows += batch.row_count;
        returned += batch_bytes(&batch);
    }
    assert_eq!(rows, 100, "the range holds exactly 100 rows");

    // The projection is exactly what the mask needs, so every returned batch
    // is one the mask built. A block wholly outside the range is masked too and
    // builds an empty batch (its offset arrays still exist), so the charge may
    // exceed what came back, never fall short of it.
    let copied = m.bytes_copied() - before;
    assert!(
        copied >= returned,
        "the range mask built {returned} B of returned batches but charged {copied} B",
    );
}

#[test]
fn reading_rows_from_a_columnar_segment_counts_their_reconstruction() {
    // A row read of a columnar segment rebuilds each row's key and value from
    // its sub-columns into new buffers. That reconstruction is the price a
    // row reader pays for the columnar layout, so leaving it uncharged would
    // report a columnar tree read row by row as copying nothing at all.
    let (_folder, tree) = columnar_segment(1_000, 32);
    let m = tree.metrics();

    let before = m.bytes_copied();
    let mut row_bytes = 0;
    for kv in tree.range(key(0)..key(1_000_000), SeqNo::MAX, None) {
        let (k, v) = kv.into_inner().expect("row");
        row_bytes += (k.len() + v.len()) as u64;
    }
    let scan_copied = m.bytes_copied() - before;
    assert!(
        scan_copied >= row_bytes,
        "rebuilding {row_bytes} B of rows was charged only {scan_copied} B",
    );

    let before = m.bytes_copied();
    let got = tree.get(key(7), SeqNo::MAX).expect("get").expect("present");
    assert_eq!(got.len(), 32, "the point read returns the whole value");
    assert!(
        m.bytes_copied() - before >= 32,
        "a point read rebuilt the value but charged no gather for it",
    );
}

#[test]
fn a_predicate_scan_of_one_segment_counts_its_filter_gather() {
    // The predicate is pushed into the table scan on the single-segment path,
    // and the table filters each block into a new batch there. The same
    // predicate over overlapping segments is charged in the merge; charging
    // it here too keeps the two layouts comparable.
    let (_folder, tree) = columnar_segment(1_000, 32);
    let m = tree.metrics();
    let before = m.bytes_copied();

    let pred = ColumnRangePredicate {
        column_id: COL_USER_KEY,
        lower: Some(key(100)),
        upper: Some(key(199)),
    };
    let mut returned = 0;
    let mut rows = 0;
    for batch in tree
        .columnar_scan(&[COL_USER_KEY, COL_VALUE], Some(&pred), SeqNo::MAX, ..)
        .expect("scan")
    {
        let batch = batch.expect("batch");
        rows += batch.row_count;
        returned += batch_bytes(&batch);
    }
    assert_eq!(rows, 100, "the predicate selects exactly 100 rows");

    assert_eq!(
        m.bytes_copied() - before,
        returned,
        "the predicate mask built the returned batch, so its bytes are the gather",
    );
}
