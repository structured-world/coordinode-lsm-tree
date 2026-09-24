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

use lsm_tree::table::columnar::{
    COL_SEQNO, COL_USER_KEY, COL_VALUE, Column, ColumnBatch, TypeTag, entries_to_column_batch,
};
use lsm_tree::table::columnar_predicate::ColumnRangePredicate;
use lsm_tree::{
    AbstractTree, AnyTree, CompressionType, Config, Guard, InternalValue, SeqNo,
    SequenceNumberCounter, Tree, UserKey, ValueType, config::CompressionPolicy, get_tmp_folder,
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
    // out of it. A blob file compresses when a compressor is compiled in, and
    // a run of one byte compresses hard, so the two must then sit on opposite
    // sides of the payload: anything that reported them as equal would be
    // measuring the same number twice under two names. Without one, a record
    // is its payload plus a header, and only the lower bounds apply.
    assert!(
        blob_read > 0,
        "resolving a separated value read no blob bytes"
    );
    #[cfg(feature = "lz4")]
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
fn a_block_read_rejected_as_corrupt_still_counts_its_bytes() {
    // The block-path twin of the blob clause below: a data block that fails
    // its checksum was still asked of the filesystem, so read must move even
    // though the get fails.
    let (folder, tree) = filled_tree(200, 64, CompressionType::None);
    let tables = folder.path().join("tables");
    let table = std::fs::read_dir(&tables)
        .expect("tables dir")
        .map(|e| e.expect("entry").path())
        .find(|p| p.is_file())
        .expect("one table file");
    let mut bytes = std::fs::read(&table).expect("read table");
    // The data blocks come first; a byte a little way in lands in one of them.
    bytes[64] ^= 0xFF;
    std::fs::write(&table, &bytes).expect("write table");

    let m = tree.metrics();
    let before = m.bytes_read();
    let failed = (0..200).any(|i| tree.get(key(i), SeqNo::MAX).is_err());
    assert!(failed, "a corrupt data block must fail some read");
    assert!(
        m.bytes_read() > before,
        "the corrupt block was read from the filesystem, so read must move",
    );
}

#[test]
fn a_block_read_rejected_as_corrupt_is_not_counted_as_a_load() {
    // Bytes are charged when the read is issued; a *load* is a block that came
    // back usable. Counting the rejected read as a load would inflate
    // `block_load_io_count` and pull every cache hit rate down for a block
    // that was never loaded.
    let (folder, tree) = filled_tree(200, 64, CompressionType::None);
    let tables = folder.path().join("tables");
    let table = std::fs::read_dir(&tables)
        .expect("tables dir")
        .map(|e| e.expect("entry").path())
        .find(|p| p.is_file())
        .expect("one table file");
    let mut bytes = std::fs::read(&table).expect("read table");
    bytes[64] ^= 0xFF;
    std::fs::write(&table, &bytes).expect("write table");

    let m = tree.metrics();
    let loaded = || m.data_block_load_count() - m.data_block_load_cached_count();
    let mut failed = 0;
    for i in 0..200 {
        let (loads, read) = (loaded(), m.bytes_read());
        if tree.get(key(i), SeqNo::MAX).is_err() {
            failed += 1;
            assert_eq!(
                loaded(),
                loads,
                "key {i}: the rejected block was counted as loaded"
            );
            assert!(
                m.bytes_read() > read,
                "key {i}: the rejected read must still count its bytes"
            );
        }
    }
    assert!(failed > 0, "a corrupt data block must fail some read");
}

#[test]
fn a_batched_multi_get_counts_the_blocks_it_prewarmed() {
    // multi_get reads a level's cold blocks in one batched request and decodes
    // them into the cache, outside the per-block load path. The later lookups
    // then hit the cache, so if the batched read went uncounted, a multi_get
    // would report reading almost nothing for the same bytes a loop of point
    // reads is charged for.
    let n = 2_000;
    let (_a_folder, point) = filled_tree(n, 64, CompressionType::None);
    for i in 0..n {
        let _ = point.get(key(i), SeqNo::MAX).expect("get");
    }
    let point_read = point.metrics().bytes_read();

    let (_b_folder, batched) = filled_tree(n, 64, CompressionType::None);
    let keys: Vec<Vec<u8>> = (0..n).map(key).collect();
    let got = batched.multi_get(&keys, SeqNo::MAX).expect("multi_get");
    assert!(got.iter().all(Option::is_some), "every key is present");
    assert_eq!(
        batched.metrics().bytes_read(),
        point_read,
        "the same blocks were asked of the filesystem either way",
    );
}

#[test]
fn a_multi_get_too_large_to_prewarm_counts_its_chunked_reads() {
    // When a level's cold blocks exceed half the cache, multi_get skips the
    // prewarm and reads them in chunks into scratch buffers it decodes
    // itself, bypassing both the cache and the per-block load path. Those
    // reads and decodes must be charged like any other.
    let n = 2_000;
    let small_cache = || std::sync::Arc::new(lsm_tree::Cache::with_capacity_bytes(32 * 1024));
    let open = |folder: &TempDir| {
        let tree = Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        .use_cache(small_cache())
        .open()
        .expect("open");
        for i in 0..n {
            tree.insert(key(i), vec![b'v'; 64], u64::from(i));
        }
        tree.flush_active_memtable(0).expect("flush");
        tree
    };

    let keys: Vec<Vec<u8>> = (0..n).map(key).collect();
    let folder = get_tmp_folder();
    let batched = open(&folder);
    let got = batched.multi_get(&keys, SeqNo::MAX).expect("multi_get");
    assert!(got.iter().all(Option::is_some), "every key is present");
    let m = batched.metrics();
    assert!(
        m.bytes_read() >= 100_000,
        "~140 KB of data blocks were read, but only {} B were charged",
        m.bytes_read(),
    );
    assert!(
        m.bytes_decoded() >= 100_000,
        "~140 KB of data blocks were decoded, but only {} B were charged",
        m.bytes_decoded(),
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

/// A key-value-separated tree of `n` 8 KiB values over a filesystem whose
/// faults the returned injector arms, flushed so every value is in a blob file.
fn faultable_blob_tree(
    n: u32,
) -> (
    TempDir,
    AnyTree,
    std::sync::Arc<lsm_tree::fs::FaultInjector>,
) {
    let folder = get_tmp_folder();
    let injector = std::sync::Arc::new(lsm_tree::fs::FaultInjector::new());
    let fs: std::sync::Arc<dyn lsm_tree::fs::Fs> = std::sync::Arc::new(
        lsm_tree::fs::FaultFs::with_injector(lsm_tree::fs::StdFs, std::sync::Arc::clone(&injector)),
    );
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs)
    .with_kv_separation(Some(Default::default()))
    .open()
    .expect("open");
    for i in 0..n {
        tree.insert(key(i), vec![b'v'; 8_192], u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush");
    (folder, tree, injector)
}

/// Makes every read of a blob file fail, leaving the tables readable, so a
/// lookup reaches the blob read and fails there.
fn refuse_blob_reads(injector: &lsm_tree::fs::FaultInjector) {
    injector.arm(
        lsm_tree::fs::FaultRule::new(
            lsm_tree::fs::FaultOp::ReadAt,
            lsm_tree::fs::Fault::Error(lsm_tree::io::ErrorKind::PermissionDenied),
        )
        .on_path("blobs"),
    );
}

/// The blob bytes one successful point read of `key(0)` asks for.
fn one_blob_record() -> u64 {
    let (_folder, tree, _) = faultable_blob_tree(1);
    let before = tree.metrics().blob_bytes_read();
    tree.get(key(0), SeqNo::MAX).expect("get").expect("present");
    tree.metrics().blob_bytes_read() - before
}

#[test]
fn a_blob_read_the_filesystem_refuses_still_counts_its_bytes() {
    // Read is what was asked of the filesystem, and a request the filesystem
    // then fails was asked all the same, exactly as a block read is charged
    // before its I/O. Charging after the read returns would drop every
    // failing request from the figure.
    let record = one_blob_record();
    assert!(record > 0);

    let (_folder, tree, injector) = faultable_blob_tree(1);
    refuse_blob_reads(&injector);
    let before = tree.metrics().blob_bytes_read();
    assert!(
        tree.get(key(0), SeqNo::MAX).is_err(),
        "a refused blob read must fail the lookup",
    );
    assert_eq!(
        tree.metrics().blob_bytes_read() - before,
        record,
        "the refused record was asked of the filesystem, so it is charged",
    );
}

#[test]
fn a_blob_prefetch_the_filesystem_refuses_still_counts_its_span() {
    // A scan asks for a run of neighbouring records in one coalesced read
    // before resolving the first of them. When the filesystem refuses that
    // read the scan falls back to the record alone, which fails too; both
    // requests were issued, so both are charged, and the figure exceeds the
    // one record the fallback asked for.
    let record = one_blob_record();

    let (_folder, tree, injector) = faultable_blob_tree(50);
    let mut scan = tree.iter(SeqNo::MAX, None);
    // The read-ahead arms on the first value a scan resolves, so the first
    // row is read alone and succeeds; the refusal starts after it.
    scan.next()
        .expect("the tree holds rows")
        .into_inner()
        .expect("the first row reads before any fault is armed");
    refuse_blob_reads(&injector);
    let before = tree.metrics().blob_bytes_read();
    assert!(
        scan.next()
            .expect("the tree holds more rows")
            .into_inner()
            .is_err(),
        "a refused blob read must fail the scan",
    );
    let charged = tree.metrics().blob_bytes_read() - before;
    assert!(
        charged > record,
        "the refused prefetch span went uncharged: {charged} B against the \
         {record} B record the fallback asked for",
    );
}

#[test]
fn an_uncompressed_blob_prefetch_counts_the_records_it_copies_out() {
    // A scan's read-ahead reads a run of neighbouring records in one buffer.
    // An uncompressed record would otherwise be handed out as a view that pins
    // the whole run in the cache, so each is copied into a buffer of its own:
    // a copy the scan performed, and one the gather counter has to show.
    let folder = get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .blob_compression(CompressionType::None)
    .with_kv_separation(Some(Default::default()))
    .open()
    .expect("open");
    let value_len = 8_192_usize;
    for i in 0..50 {
        tree.insert(key(i), vec![b'v'; value_len], u64::from(i));
    }
    tree.flush_active_memtable(0).expect("flush");

    let m = tree.metrics();
    let before = m.bytes_copied();
    let mut rows = 0_usize;
    for guard in tree.iter(SeqNo::MAX, None) {
        let (_, value) = guard.into_inner().expect("row");
        assert_eq!(value.len(), value_len);
        rows += 1;
    }
    assert_eq!(rows, 50);
    assert!(
        m.bytes_copied() - before >= value_len as u64,
        "the read-ahead copied records out of its span but charged {} B",
        m.bytes_copied() - before,
    );
}

#[test]
fn a_point_read_admitted_to_the_row_cache_counts_the_row_it_detaches() -> lsm_tree::Result<()> {
    // A point read that misses the row cache copies the key and value it
    // resolved out of the data block, so the cached row owns its bytes rather
    // than pinning the block. That copy is a gather like any other: both
    // lookup paths (value-only and full entry) charge it, and the later hit,
    // served from the cached row, copies nothing.
    let value_len = 200;
    let (_folder, tree) = filled_tree(100, value_len, CompressionType::None);
    let m = tree.metrics();
    let row = |i: u32| (key(i).len() + value_len) as u64;

    let before = m.bytes_copied();
    assert!(tree.get(key(5), SeqNo::MAX)?.is_some());
    assert_eq!(
        m.bytes_copied() - before,
        row(5),
        "the value-only lookup detached one row into the row cache",
    );

    let before = m.bytes_copied();
    assert!(tree.get(key(5), SeqNo::MAX)?.is_some());
    assert_eq!(m.bytes_copied(), before, "a row-cache hit copies nothing",);

    let before = m.bytes_copied();
    assert!(tree.get_internal_entry(&key(7), SeqNo::MAX)?.is_some());
    assert_eq!(
        m.bytes_copied() - before,
        row(7),
        "the full-entry lookup detached one row into the row cache",
    );
    Ok(())
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
fn a_narrow_projection_over_wide_rows_counts_the_columns_it_detaches() {
    // A key-only projection over rows carrying a large value covers a sliver
    // of each block, so the decoder copies the key column out rather than
    // keep the whole block alive for it. That copy is a gather like any
    // other: without a predicate, delete mask or range bound no later stage
    // charges anything, so if the decode-time copy went uncounted a
    // projection would look zero-copy exactly when it copies every byte it
    // returns.
    let (_folder, tree) = columnar_segment(1_000, 4_096);
    let m = tree.metrics();
    let before = m.bytes_copied();

    let mut returned = 0;
    for batch in tree
        .columnar_scan(&[COL_USER_KEY], None, SeqNo::MAX, ..)
        .expect("scan")
    {
        returned += batch_bytes(&batch.expect("batch"));
    }
    assert!(returned > 0, "the scan must return the keys");

    let copied = m.bytes_copied() - before;
    assert!(
        copied >= returned,
        "the projection detached {returned} B of keys from the blocks but charged {copied} B",
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

/// The bytes a scan batch of `keys` holds for the key, seqno and value
/// columns: two offset tables of `rows + 1` entries, the keys, one 8-byte
/// seqno per row and the values.
fn key_seqno_value_bytes(keys: core::ops::Range<u32>, value_len: usize) -> u64 {
    let rows = keys.len() as u64;
    let key_bytes: u64 = keys.map(|i| key(i).len() as u64).sum();
    2 * (rows + 1) * 4 + key_bytes + 8 * rows + rows * value_len as u64
}

#[test]
fn a_merged_columnar_scan_counts_the_seqno_column_it_rewrites() -> lsm_tree::Result<()> {
    // Overlapping segments carry different seqno offsets, so the merge gathers
    // the surviving rows and then writes each one's effective seqno into a new
    // column that replaces the gathered one. That second buffer is a gather of
    // its own: counting only the first charges every merged seqno once while
    // it was copied twice. Each segment is one block, so every gather of the
    // merge is known: each segment's visible rows, the accumulator rebuilt
    // once over both, the surviving rows, and the rewritten seqnos.
    let folder = get_tmp_folder();
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(lsm_tree::config::BlockSizePolicy::all(1 << 20))
    .open()?
    else {
        panic!("expected a standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)?;
    for i in 0..1_000 {
        tree.insert(key(i), vec![b'v'; 32], u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    for i in 500..1_500 {
        tree.insert(key(i), vec![b'w'; 32], 2_000 + u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    let m = tree.metrics();
    let before = m.bytes_copied();

    let mut returned = 0;
    let mut rows = 0_u64;
    for batch in tree.columnar_scan(&[COL_USER_KEY, COL_SEQNO, COL_VALUE], None, SeqNo::MAX, ..)? {
        let batch = batch?;
        rows += u64::from(batch.row_count);
        returned += batch_bytes(&batch);
    }
    assert_eq!(rows, 1_500, "the merge yields every key once");

    let first = key_seqno_value_bytes(0..1_000, 32);
    let second = key_seqno_value_bytes(500..1_500, 32);
    let both = first + second - 4 * 2;
    assert_eq!(
        m.bytes_copied() - before,
        first + second + both + returned + 8 * rows,
        "each segment's rows, the accumulator over both, the surviving rows and \
         the rewritten seqnos are one gather each",
    );
    Ok(())
}

#[test]
fn a_scan_of_one_ingested_segment_counts_the_seqno_column_it_globalizes() -> lsm_tree::Result<()> {
    // A bulk-ingested segment stores its rows at local seqno 0 and carries its
    // place in the tree as a per-segment offset. Returning its seqno column
    // therefore writes each row's effective seqno into a new column, even on
    // the path that otherwise streams the segment untouched: that rewrite is a
    // gather, 8 bytes per row, and the only one this scan performs.
    let folder = get_tmp_folder();
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::new(100),
        SequenceNumberCounter::default(),
    )
    .open()?
    else {
        panic!("expected a standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)?;
    let n = 1_000;
    let entries: Vec<InternalValue> = (0..n)
        .map(|i| InternalValue::from_components(key(i), b"vv", 0, ValueType::Value))
        .collect();
    let any = AnyTree::Standard(tree.clone());
    let mut ingest = any.ingestion()?;
    ingest.write_columnar_batch(&entries_to_column_batch(&entries)?)?;
    ingest.finish()?;
    let m = tree.metrics();
    let before = m.bytes_copied();

    let mut rows = 0_u64;
    for batch in tree.columnar_scan(&[COL_USER_KEY, COL_SEQNO, COL_VALUE], None, SeqNo::MAX, ..)? {
        let batch = batch?;
        let seqnos = batch
            .columns
            .iter()
            .find(|c| c.column_id == COL_SEQNO)
            .expect("seqno column");
        assert!(
            seqnos.data.chunks_exact(8).all(|s| s != [0; 8]),
            "the returned seqnos are the segment's effective ones, not its local zeros",
        );
        rows += u64::from(batch.row_count);
    }
    assert_eq!(rows, u64::from(n), "the scan yields every ingested row");
    assert_eq!(
        m.bytes_copied() - before,
        8 * rows,
        "the globalized seqno column is one gather of 8 bytes per row",
    );
    Ok(())
}

/// A columnar tree holding `n` rows ingested with the value split into one
/// fixed-width sub-column, so a row read has to rebuild each value.
fn subcolumn_segment(n: u32) -> (TempDir, Tree) {
    let (folder, tree) = columnar_segment(0, 0);
    let entries: Vec<InternalValue> = (0..n)
        .map(|i| InternalValue::from_components(key(i), b"ignored", 0, ValueType::Value))
        .collect();
    let mut batch = entries_to_column_batch(&entries).expect("transpose");
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: (0..n)
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<u8>>()
            .into(),
    });
    let any = AnyTree::Standard(tree.clone());
    let mut ingest = any.ingestion().expect("ingestion");
    ingest.write_columnar_batch(&batch).expect("write batch");
    ingest.finish().expect("finish");
    (folder, tree)
}

/// A columnar tree of `n` rows whose value is split into a 1000-byte bytes
/// sub-column and a fixed-width one, with the framed value a row read rebuilds
/// from them. The wide cell keeps the rebuilt value far larger than a row
/// block's own framing, so a copy of it is unmistakable in the counter.
fn wide_subcolumn_segment(n: u32) -> (TempDir, Tree, Vec<u8>) {
    let (folder, tree) = columnar_segment(0, 0);
    let cell = vec![b'w'; 1_000];
    let entries: Vec<InternalValue> = (0..n)
        .map(|i| InternalValue::from_components(key(i), cell.as_slice(), 0, ValueType::Value))
        .collect();
    let mut batch = entries_to_column_batch(&entries).expect("transpose");
    batch.columns.push(Column {
        column_id: COL_VALUE + 1,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: (0..n)
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<u8>>()
            .into(),
    });
    let any = AnyTree::Standard(tree.clone());
    let mut ingest = any.ingestion().expect("ingestion");
    ingest.write_columnar_batch(&batch).expect("write batch");
    ingest.finish().expect("finish");
    // Every row frames the same two cells, so every rebuilt value is this long.
    let framed = lsm_tree::table::columnar::frame_value_cells(&[
        (TypeTag::Bytes, &cell),
        (TypeTag::Fixed(4), &[0; 4]),
    ])
    .expect("frame");
    (folder, tree, framed)
}

#[test]
fn a_point_read_of_a_split_value_counts_the_rows_it_gathers_and_the_block() {
    // A columnar point read copies the matching key's rows out of the columns
    // into owned entries, then encodes those into a small row block. Two
    // gathers, each charged at the size of what it built: charging only the
    // block drops the first, which is as large as the value itself.
    let n = 200;
    let (_folder, tree, framed) = wide_subcolumn_segment(n);
    let m = tree.metrics();

    let before = m.bytes_copied();
    for i in 0..n {
        let got = tree.get(key(i), SeqNo::MAX).expect("get").expect("present");
        // Rows differ in their fixed-width cell, not in their framed length.
        assert_eq!(
            got.len(),
            framed.len(),
            "the read rebuilds the framed value"
        );
    }
    let rows = u64::from(n) * (key(0).len() + framed.len()) as u64;
    let copied = m.bytes_copied() - before;
    assert!(
        copied >= 2 * rows,
        "the rows were gathered ({rows} B) and then encoded into blocks at least \
         as large, but only {copied} B were charged",
    );
}

#[test]
fn a_multi_get_of_a_split_value_counts_the_values_it_rebuilds_and_the_blocks() {
    // The row path over a split-value block rebuilds every row's value from
    // its sub-columns and then encodes the rows into a row-major block. The
    // rebuild is a gather of its own, as large as the values; charging only
    // the encoded block drops it.
    let n = 200;
    let (_folder, tree, framed) = wide_subcolumn_segment(n);
    let m = tree.metrics();

    let keys: Vec<Vec<u8>> = (0..n).map(key).collect();
    let before = m.bytes_copied();
    let got = tree.multi_get(&keys, SeqNo::MAX).expect("multi_get");
    assert!(got.iter().all(Option::is_some), "every key is present");
    let values = u64::from(n) * framed.len() as u64;
    let copied = m.bytes_copied() - before;
    assert!(
        copied >= 2 * values,
        "every value was rebuilt ({values} B) and then encoded into blocks at \
         least as large, but only {copied} B were charged",
    );
}

#[test]
fn scanning_rows_of_a_single_value_column_copies_nothing() {
    // A columnar segment whose value is one plain bytes column hands every
    // row its key and value as views into the decoded column buffers. Nothing
    // is built, so a row scan over it must leave the counter where it was;
    // charging the row lengths would report a zero-copy scan as copying the
    // whole dataset.
    let (_folder, tree) = columnar_segment(1_000, 32);
    let m = tree.metrics();

    let before = m.bytes_copied();
    let scanned = tree.range(key(0)..key(1_000_000), SeqNo::MAX, None).count();
    assert_eq!(scanned, 1_000, "the scan must see every row");
    assert_eq!(
        m.bytes_copied(),
        before,
        "a scan of view-backed rows gathers nothing",
    );
}

#[test]
fn scanning_rows_of_a_split_value_counts_their_reconstruction() {
    // A value stored as sub-columns has no contiguous copy on disk: each row
    // read builds it into a fresh buffer. That rebuild is the gather a row
    // reader pays for the split layout, and it must be charged.
    let (_folder, tree) = subcolumn_segment(1_000);
    let m = tree.metrics();

    let before = m.bytes_copied();
    let mut value_bytes = 0;
    for kv in tree.range(key(0)..key(1_000_000), SeqNo::MAX, None) {
        let (_, v) = kv.into_inner().expect("row");
        value_bytes += v.len() as u64;
    }
    assert!(value_bytes > 0, "the scan must return values");
    assert_eq!(
        m.bytes_copied() - before,
        value_bytes,
        "every rebuilt value is one gather, sized as what it built",
    );
}

#[test]
fn a_point_read_of_a_columnar_segment_counts_the_block_it_rebuilds() {
    // A columnar point read rebuilds the looked-up key's rows into a small
    // row-major block, a new buffer made from the decoded columns.
    let (_folder, tree) = columnar_segment(1_000, 32);
    let m = tree.metrics();

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

#[test]
fn a_columnar_point_read_that_misses_counts_what_its_decode_copied() {
    // A point read decodes the block before it knows whether the key is there,
    // and decoding copies each nullable column's validity out of the block. A
    // miss still did that copy, so the counter moves on a miss as on a hit.
    // With no filter, a key absent from the segment but inside its key range
    // reaches the block.
    let folder = get_tmp_folder();
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .filter_policy(lsm_tree::config::FilterPolicy::disabled())
    .open()
    .expect("open") else {
        panic!("expected a standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)
        .expect("enable columnar");
    let n = 64_u32;
    let entries: Vec<InternalValue> = (0..n)
        .map(|i| InternalValue::from_components(key(2 * i), b"ignored", 0, ValueType::Value))
        .collect();
    let mut batch = entries_to_column_batch(&entries).expect("transpose");
    batch.columns.pop();
    let rows = n as usize;
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        validity: Some(vec![0b0101_0101; rows.div_ceil(8)]),
        data: vec![0; rows * 4].into(),
    });
    let any = AnyTree::Standard(tree.clone());
    let mut ingest = any.ingestion().expect("ingestion");
    ingest.write_columnar_batch(&batch).expect("write batch");
    ingest.finish().expect("finish");

    let m = tree.metrics();
    let before = m.bytes_copied();
    assert!(
        tree.get(key(3), SeqNo::MAX).expect("get").is_none(),
        "an odd key was never written",
    );
    assert!(
        m.bytes_copied() > before,
        "the miss decoded the block and copied its validity, but charged nothing",
    );
}

#[test]
fn a_compaction_counts_nothing_it_reads_or_opens() -> lsm_tree::Result<()> {
    // Compaction reads its inputs and then opens its outputs, and opening a
    // table walks its index to build the table's locator. Neither is a read a
    // caller made, so neither may move the read counters: a background
    // compaction running beside a measured read would otherwise inflate it.
    let folder = get_tmp_folder();
    let config = || {
        Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
    };
    {
        let tree = config().open()?;
        for i in 0..8 {
            tree.insert(key(i), vec![b'b'; 64 * 1024], u64::from(i));
        }
        tree.flush_active_memtable(0)?;
        tree.insert(key(100), b"x", 100);
        tree.flush_active_memtable(0)?;
    }

    let tree = config().open()?;
    let m = tree.metrics();
    let (read, decoded) = (m.bytes_read(), m.bytes_decoded());
    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(m.bytes_read(), read, "the compaction's reads were counted");
    assert_eq!(
        m.bytes_decoded(),
        decoded,
        "the compaction's decoding was counted"
    );
    Ok(())
}

/// A standard tree whose tables keep a partitioned index that is neither
/// pinned nor cached, so every walk of it reads index blocks from disk: the
/// shape in which a maintenance walk that forgot to detach from the tree's
/// metrics shows up in the read counters.
fn cold_index_tree(folder: &TempDir) -> lsm_tree::Result<Tree> {
    use lsm_tree::config::{BlockSizePolicy, PinningPolicy};
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .index_block_partitioning_policy(PinningPolicy::all(true))
    .index_block_pinning_policy(PinningPolicy::disabled())
    .use_cache(std::sync::Arc::new(lsm_tree::Cache::with_capacity_bytes(0)))
    .open()?
    else {
        panic!("expected a standard tree");
    };
    Ok(tree)
}

#[test]
fn a_patrol_scrub_over_a_cold_index_counts_nothing() -> lsm_tree::Result<()> {
    // A patrol scrub is maintenance: it walks each table's index to find the
    // blocks it verifies. With an index that is read from disk on every walk,
    // that walk must stay out of the read counters like the blocks it reads.
    let folder = get_tmp_folder();
    let tree = cold_index_tree(&folder)?;
    for i in 0..5_000 {
        tree.insert(key(i), vec![b'v'; 64], u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    let m = tree.metrics();
    let (read, decoded) = (m.bytes_read(), m.bytes_decoded());
    let report =
        lsm_tree::scrub::patrol_scrub(&tree, &lsm_tree::scrub::PatrolScrubOptions::default());
    assert!(
        report.is_ok(),
        "the scrub must find nothing wrong: {report:?}"
    );
    assert_eq!(m.bytes_read(), read, "the scrub's index walk was counted");
    assert_eq!(
        m.bytes_decoded(),
        decoded,
        "the scrub's index decoding was counted"
    );
    Ok(())
}

#[test]
fn a_compaction_over_a_cold_index_counts_nothing() -> lsm_tree::Result<()> {
    // The serial compaction scanner walks each input's index too; with a cold
    // index that walk reads from disk, and it is maintenance all the same.
    let folder = get_tmp_folder();
    let tree = cold_index_tree(&folder)?;
    for round in 0..2_u32 {
        for i in 0..2_500 {
            tree.insert(key(i), vec![b'v'; 64], u64::from(round * 2_500 + i));
        }
        tree.flush_active_memtable(0)?;
    }

    let m = tree.metrics();
    let (read, decoded) = (m.bytes_read(), m.bytes_decoded());
    tree.major_compact(u64::MAX, 5_000)?;
    assert_eq!(
        m.bytes_read(),
        read,
        "the compaction's index walk was counted"
    );
    assert_eq!(
        m.bytes_decoded(),
        decoded,
        "the compaction's index decoding was counted"
    );
    Ok(())
}

#[test]
fn a_parallel_sub_compaction_counts_nothing_it_reads() -> lsm_tree::Result<()> {
    // A compaction split across threads reads each input through a key-bounded
    // table iterator rather than the serial scanner. It is the same maintenance
    // either way, so the read counters must not move for it.
    let folder = get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(lsm_tree::config::BlockSizePolicy::all(512))
    .compaction_threads(4)
    .subcompaction_min_bytes(0)
    .open()?;

    let n = 2_000;
    // A bottom level of several tables, the boundaries the split follows.
    for i in 0..n {
        tree.insert(key(i), vec![b'a'; 64], u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(4_096, 0)?;
    let bottom_tables = tree.table_count();
    assert!(bottom_tables > 1, "the split needs several bottom tables");

    for i in 0..n {
        tree.insert(key(i), vec![b'b'; 64], u64::from(n + i));
    }
    tree.flush_active_memtable(0)?;

    let m = tree.metrics();
    let (read, decoded, copied) = (m.bytes_read(), m.bytes_decoded(), m.bytes_copied());
    tree.major_compact(u64::MAX, 0)?;
    assert!(
        tree.table_count() > 1,
        "the compaction must have split for the bounded path to run",
    );
    assert_eq!(
        m.bytes_read(),
        read,
        "the sub-compactions' reads were counted"
    );
    assert_eq!(
        m.bytes_decoded(),
        decoded,
        "the sub-compactions' decoding was counted"
    );
    assert_eq!(
        m.bytes_copied(),
        copied,
        "the sub-compactions' copies were counted"
    );
    Ok(())
}

#[test]
fn a_compaction_writing_a_delete_bitmap_counts_nothing_it_opens() -> lsm_tree::Result<()> {
    // Opening a columnar table whose rows carry a positional delete bitmap
    // walks its index a second time, to map each data block to its first row.
    // That walk is part of opening the table, like the locator's, so a
    // compaction that writes such a table must not move the read counters.
    let folder = get_tmp_folder();
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?
    else {
        panic!("expected a standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.zone_map = true;
        cfg.delete_strategy = lsm_tree::config::DeleteStrategyPolicy::all(
            lsm_tree::config::DeleteStrategy::MergeOnRead,
        );
    })?;
    for i in 0..200 {
        tree.insert(key(i), vec![b'v'; 64], u64::from(i));
    }
    tree.remove_range(
        UserKey::from(&key(0)[..]),
        UserKey::from(&key(50)[..]),
        1000,
    );
    tree.flush_active_memtable(0)?;

    let m = tree.metrics();
    let (read, decoded) = (m.bytes_read(), m.bytes_decoded());
    tree.major_compact(64 * 1024 * 1024, 5000)?;
    assert!(
        tree.current_version()
            .iter_tables()
            .any(|t| !t.delete_bitmap().is_empty()),
        "the compaction must write a delete bitmap for the walk to run",
    );
    assert_eq!(
        m.bytes_read(),
        read,
        "opening the output was counted as a read"
    );
    assert_eq!(
        m.bytes_decoded(),
        decoded,
        "opening the output was counted as a decode",
    );
    Ok(())
}

#[test]
fn a_compaction_filter_reading_a_blob_counts_nothing() -> lsm_tree::Result<()> {
    // Compaction is maintenance: its input reads and decoding stay outside the
    // read counters. A filter that resolves a separated value runs inside that
    // compaction, so its blob read is maintenance too, and counting it would
    // let a background compaction inflate what foreground reads report.
    use lsm_tree::KvSeparationOptions;
    use lsm_tree::compaction::filter::{CompactionFilter, Context, Factory, ItemAccessor, Verdict};
    use std::sync::Arc;

    struct ReadsValues;
    impl CompactionFilter for ReadsValues {
        fn filter_item(
            &mut self,
            item: ItemAccessor<'_>,
            _ctx: &Context,
        ) -> lsm_tree::Result<Verdict> {
            let _ = item.value()?;
            Ok(Verdict::Keep)
        }
    }
    struct ReadsValuesFactory;
    impl Factory for ReadsValuesFactory {
        fn name(&self) -> &str {
            "reads-values"
        }
        fn make_filter(&self, _ctx: &Context) -> Box<dyn CompactionFilter> {
            Box::new(ReadsValues)
        }
    }

    let folder = get_tmp_folder();
    let config = || {
        Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(KvSeparationOptions::default()))
        .with_compaction_filter_factory(Some(Arc::new(ReadsValuesFactory)))
    };
    {
        let tree = config().open()?;
        for i in 0..8 {
            tree.insert(key(i), vec![b'b'; 64 * 1024], u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }

    // Reopened, so no blob is cached and the filter's reads go to the files.
    let tree = config().open()?;
    let m = tree.metrics();
    let (read, blob, decoded) = (m.bytes_read(), m.blob_bytes_read(), m.bytes_decoded());
    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(
        m.blob_bytes_read(),
        blob,
        "the filter's blob reads were counted"
    );
    assert_eq!(m.bytes_read(), read, "the compaction's reads were counted");
    assert_eq!(
        m.bytes_decoded(),
        decoded,
        "the compaction's decoding was counted"
    );
    Ok(())
}
