use super::Cache;

/// A cached row must own its key and value, not view the data block they were
/// read out of.
///
/// The point-read path produces them as subslices of the decoded block, and
/// a subslice keeps the whole block allocation alive. The weigher charges the
/// row only its own key and value bytes, so a hundred-byte row viewing a four
/// kilobyte block is accounted as a hundred bytes while holding four thousand.
/// A workload touching one key per block would then exceed the capacity it
/// asked for by the ratio between the two, which is exactly the workload a
/// row cache is least able to help.
///
/// The observable is the address: a view's bytes live inside the block's
/// buffer, a copy's do not. That holds for either slice backend, where a
/// reference count does not.
#[test]
fn row_cache_when_given_block_subslices_stores_detached_copies() {
    let cache = Cache::with_capacity_bytes(1024 * 1024);
    let id = crate::table::GlobalTableId::from((0, 0));

    // Stand in for a decoded data block, large enough that the slice is
    // heap-backed rather than stored inline.
    let block = crate::Slice::from(vec![7_u8; 4096]);
    let block_range = {
        let start = block.as_ptr() as usize;
        start..start + block.len()
    };

    // Longer than the slice type's inline threshold (20 bytes on 64-bit, 16 on
    // 32-bit). A shorter subslice is copied into the handle itself, so it would
    // never point at the block and the test would prove nothing about the case
    // it exists for: a value large enough to be kept as a view.
    let value = block.slice(0..64);
    // A restart-head key is a view into the block too, with the same effect.
    let user_key = block.slice(64..96);
    assert!(
        block_range.contains(&(value.as_ptr() as usize))
            && block_range.contains(&(user_key.as_ptr() as usize)),
        "precondition: a subslice past the inline threshold points into the block's own buffer",
    );

    let copied = cache.insert_row(id, 1, &user_key, 1, crate::ValueType::Value, &value);
    assert_eq!(
        copied, 96,
        "the row owns a copy of its 32-byte key and 64-byte value"
    );

    let Some(got) = cache.get_row(id, 1, &user_key) else {
        panic!("the row was just inserted, so the lookup must hit");
    };

    assert!(
        !block_range.contains(&(got.value.as_ptr() as usize))
            && !block_range.contains(&(got.key.user_key.as_ptr() as usize)),
        "the cached row still points into the block it was read from, so it \
         keeps the whole block alive while being charged only its own bytes",
    );

    // And the copy has to be a faithful one.
    assert_eq!(&*got.value, &[7_u8; 64][..]);
    assert_eq!(&*got.key.user_key, &[7_u8; 32][..]);
}

/// A cache whose loads were never measured slow lets concurrent misses of one
/// block each load it, taking nothing in the registry; one set to share every
/// load coordinates them.
#[test]
fn misses_share_loads_only_once_loads_are_measured_slow() {
    use super::{BlockClaim, LoadIdentity};

    let id = crate::table::GlobalTableId::from((0, 1));
    let offset = crate::table::BlockOffset(0);
    let identity = LoadIdentity::Block {
        block_type: crate::table::block::BlockType::Data,
        size: 100,
        compression: crate::CompressionType::None,
        encrypted: false,
    };

    let quick = Cache::with_capacity_bytes(1024 * 1024);
    assert!(quick.load_coalescing_after() > core::time::Duration::ZERO);
    assert!(matches!(
        quick.claim_block(id, offset, identity),
        BlockClaim::Alone
    ));
    assert!(matches!(
        quick.claim_block(id, offset, identity),
        BlockClaim::Alone
    ));
    assert_eq!(quick.loads_in_flight(), 0, "the registry is not touched");

    let sharing = Cache::with_capacity_bytes(1024 * 1024)
        .with_load_coalescing_after(core::time::Duration::ZERO);
    let BlockClaim::Lead(lead) = sharing.claim_block(id, offset, identity) else {
        panic!("a sharing cache coordinates the load");
    };
    assert_eq!(sharing.loads_in_flight(), 1);
    drop(lead);
    assert_eq!(sharing.loads_in_flight(), 0);
}

/// Packed values read alone stay out of a cache that has not filled: caching
/// them there would grow it on the reads' path. 2,000 values of 100 bytes
/// read into 64 MiB leave the cache holding the table's blocks alone.
#[cfg(feature = "std")]
#[test]
fn packed_values_read_alone_stay_out_of_a_cache_still_filling() -> crate::Result<()> {
    use crate::AbstractTree;
    use std::sync::Arc;

    let folder = tempfile::tempdir()?;
    let cache = Arc::new(Cache::with_capacity_bytes(64 << 20).with_row_cache(false));
    let tree = crate::Config::new(
        folder.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .use_cache(Arc::clone(&cache))
    .with_kv_separation(Some(
        crate::KvSeparationOptions::default().separation_threshold(1),
    ))
    .blob_compression(crate::CompressionType::None)
    .open()?;
    for i in 0..2_000u32 {
        tree.insert(i.to_be_bytes(), [7u8; 100], u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    let pass = || -> crate::Result<u64> {
        for i in 0..2_000u32 {
            assert!(tree.get(i.to_be_bytes(), crate::SeqNo::MAX)?.is_some());
        }
        Ok(cache.size())
    };
    // The first pass loads the table's blocks; neither caches a value, which
    // a second miss into a filled shard would.
    let first = pass()?;
    assert!(
        first < 2_000 * 100,
        "{first} bytes cached by the first pass"
    );
    assert_eq!(pass()?, first, "the second pass cached its values");
    Ok(())
}

/// Packed values read alone go into a cache that has filled, as its
/// admission takes them: into a shard's room on a first miss, past it on a
/// second, so within three passes every value is read from the cache. The
/// cache is filled first with blocks of another table, which evict as they
/// go in.
#[cfg(all(feature = "std", feature = "metrics"))]
#[test]
fn packed_values_read_again_come_from_a_cache_that_has_filled() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::table::block::{BlockType, Header};
    use std::sync::Arc;

    let cache = Arc::new(Cache::with_capacity_bytes(1 << 20).with_row_cache(false));
    let filler = crate::GlobalTableId::from((u64::MAX, 0));
    for offset in 0..4_096u64 {
        cache.insert_block(
            filler,
            crate::table::BlockOffset(offset * 2_048),
            crate::table::Block {
                header: Header {
                    uncompressed_length: 1_024,
                    ..Header::test_dummy(BlockType::Data)
                },
                data: crate::Slice::from(&[0u8; 1_024][..]),
            },
        );
    }

    let folder = tempfile::tempdir()?;
    let tree = crate::Config::new(
        folder.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .use_cache(Arc::clone(&cache))
    .with_kv_separation(Some(
        crate::KvSeparationOptions::default().separation_threshold(1),
    ))
    .blob_compression(crate::CompressionType::None)
    .open()?;
    for i in 0..200u32 {
        tree.insert(i.to_be_bytes(), [7u8; 100], u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    let m = tree.metrics();
    let pass = || -> crate::Result<u64> {
        let before = m.blob_bytes_read();
        for i in 0..200u32 {
            assert_eq!(
                tree.get(i.to_be_bytes(), crate::SeqNo::MAX)?.as_deref(),
                Some(&[7u8; 100][..])
            );
        }
        Ok(m.blob_bytes_read() - before)
    };
    assert!(pass()? > 0, "the first pass reads the blob file");
    pass()?;
    assert_eq!(
        pass()?,
        0,
        "the third pass reads every value from the cache"
    );
    Ok(())
}

#[test]
fn metadata_priority_defaults_on_and_toggles() {
    // On by default.
    assert!(Cache::with_capacity_bytes(1024).metadata_priority());
    // Builder turns it off and back on.
    let off = Cache::with_capacity_bytes(1024).with_metadata_priority(false);
    assert!(!off.metadata_priority());
    assert!(off.with_metadata_priority(true).metadata_priority());
}

/// A blob cache entry is a frame, keyed by its place in a blob file, and a
/// corrupt index entry can point a key at a place that holds another key's
/// value. A cached frame still lists the keys of its values, so a value taken
/// out of it is checked exactly as one read from disk is: the key it was asked
/// for against the one at the handle's index. A conflicting key, or an index
/// the frame does not have, is refused rather than served the neighbour's
/// value.
#[test]
fn a_cached_frame_serves_a_value_only_to_the_key_it_lists() {
    use crate::vlog::blob_file::{frame::decoded_frame, reader::value_in};
    use std::sync::Arc;

    let cache = Cache::with_capacity_bytes(1024 * 1024);
    let at = super::BlobFrameAt {
        blob_file_id: 7,
        frame: 40,
    };
    cache.insert_blob_frame(
        0,
        at,
        Arc::new(decoded_frame(&[(b"first", b"one"), (b"second", b"two")])),
    );
    let frame = cache.get_blob_frame(0, at).expect("the frame is cached");
    assert_eq!(&*value_in(&frame, 1, b"second").expect("listed"), b"two");
    assert!(
        value_in(&frame, 1, b"first").is_err(),
        "another key at that index is refused"
    );
    assert!(
        value_in(&frame, 2, b"second").is_err(),
        "an index past the frame's list is refused"
    );
    assert!(
        cache.get_blob_frame(1, at).is_none(),
        "another tree's frame of that number is another entry"
    );
}

/// A thread that leads a batched read's load waits on no other load: with
/// another thread leading a block it needs, it is told to read the block
/// alone at once instead of blocking. Two batched reads each waiting on a
/// block the other leads would otherwise never finish.
#[test]
fn a_thread_leading_a_batched_load_never_waits() {
    use super::{BlockClaim, LoadIdentity};
    use std::sync::Arc;

    let cache = Arc::new(
        Cache::with_capacity_bytes(1024 * 1024)
            .with_load_coalescing_after(core::time::Duration::ZERO),
    );
    let id = crate::table::GlobalTableId::from((0, 1));
    let identity = LoadIdentity::Block {
        block_type: crate::table::block::BlockType::Data,
        size: 100,
        compression: crate::CompressionType::None,
        encrypted: false,
    };
    // Another reader leads the load of block 0 and keeps it in flight.
    let BlockClaim::Lead(held) = cache.claim_block(id, crate::table::BlockOffset(0), identity)
    else {
        panic!("the first claim leads");
    };

    let waited = std::thread::spawn({
        let cache = Arc::clone(&cache);
        move || {
            let Some(batched) =
                cache.claim_batched_block(id, crate::table::BlockOffset(1), identity, true)
            else {
                panic!("the batched read leads block 1");
            };
            let claim = cache.claim_block(id, crate::table::BlockOffset(0), identity);
            drop(batched);
            matches!(claim, BlockClaim::Alone)
        }
    })
    .join()
    .expect("the batched reader returns instead of waiting");
    assert!(waited, "told to read the block alone");

    // Without a batched lead the same claim waits, and takes the block once
    // it is published.
    let waiter = std::thread::spawn({
        let cache = Arc::clone(&cache);
        move || {
            matches!(
                cache.claim_block(id, crate::table::BlockOffset(0), identity),
                BlockClaim::Shared(_)
            )
        }
    });
    while cache.load_waiters() < 1 {
        std::thread::yield_now();
    }
    held.publish(&crate::table::Block {
        header: crate::table::block::Header::test_dummy(crate::table::block::BlockType::Data),
        data: crate::Slice::from(&b"block"[..]),
    });
    assert!(waiter.join().expect("waiter"), "took the published block");
}

/// Reads waiting on another read of the same blob frame take the frame it
/// published, whatever keys they read it for: the frame lists its keys, so a
/// read for a key it does not hold at its index is refused when it takes its
/// value out (see the test above), and never receives another key's value.
#[test]
fn reads_waiting_on_one_frame_share_it() {
    use super::BlobClaim;
    use crate::vlog::blob_file::{frame::decoded_frame, reader::value_in};
    use std::sync::Arc;

    let cache = Arc::new(
        Cache::with_capacity_bytes(1024 * 1024)
            .with_load_coalescing_after(core::time::Duration::ZERO),
    );
    let at = super::BlobFrameAt {
        blob_file_id: 3,
        frame: 0,
    };
    let BlobClaim::Lead(lead) = cache.claim_blob_frame(0, at) else {
        panic!("the first read leads");
    };
    let waiter = |key: &'static [u8]| {
        let cache = Arc::clone(&cache);
        std::thread::spawn(move || match cache.claim_blob_frame(0, at) {
            BlobClaim::Shared(frame) => Some(value_in(&frame, 0, key).ok()),
            _ => None,
        })
    };
    let owner = waiter(b"owner");
    let intruder = waiter(b"intruder");
    while cache.load_waiters() < 2 {
        std::thread::yield_now();
    }
    lead.publish(
        &cache,
        0,
        at,
        &Arc::new(decoded_frame(&[(b"owner", b"value")])),
        false,
    );
    let owner = owner.join().expect("reader").expect("shared");
    assert_eq!(
        owner.as_deref(),
        Some(&b"value"[..]),
        "the listed key takes its value"
    );
    let intruder = intruder.join().expect("reader").expect("shared");
    assert!(intruder.is_none(), "another key is refused its value");
    assert!(
        cache.get_blob_frame(0, at).is_none(),
        "a read that does not cache what it reads leaves the cache empty"
    );
}

/// Reads of one packed record behind another read of it take the record that
/// read hands over; a thread leading a batched load does not wait for it, and
/// reads the record alone.
#[test]
fn reads_waiting_on_one_record_share_it_unless_they_lead_a_batch() {
    use super::{LoadIdentity, RecordClaim};
    use std::sync::Arc;

    let cache = Arc::new(
        Cache::with_capacity_bytes(1024 * 1024)
            .with_load_coalescing_after(core::time::Duration::ZERO),
    );
    let at = super::BlobFrameAt {
        blob_file_id: 3,
        frame: 0,
    };
    let RecordClaim::Lead(lead) = cache.claim_blob_record(0, at, 5) else {
        panic!("the first read leads");
    };

    let alone = std::thread::spawn({
        let cache = Arc::clone(&cache);
        move || {
            let id = crate::table::GlobalTableId::from((0, 1));
            let identity = LoadIdentity::Block {
                block_type: crate::table::block::BlockType::Data,
                size: 100,
                compression: crate::CompressionType::None,
                encrypted: false,
            };
            let Some(batched) =
                cache.claim_batched_block(id, crate::table::BlockOffset(1), identity, true)
            else {
                panic!("the batched read leads its block");
            };
            let claim = cache.claim_blob_record(0, at, 5);
            drop(batched);
            matches!(claim, RecordClaim::Alone)
        }
    })
    .join()
    .expect("the batched reader returns instead of waiting");
    assert!(alone, "told to read the record alone");

    let waiter = std::thread::spawn({
        let cache = Arc::clone(&cache);
        move || match cache.claim_blob_record(0, at, 5) {
            RecordClaim::Shared(record) => Some(record),
            _ => None,
        }
    });
    while cache.load_waiters() < 1 {
        std::thread::yield_now();
    }
    lead.hand_over(crate::Slice::from(&b"record"[..]));
    let shared = waiter.join().expect("reader").expect("shared");
    assert_eq!(&*shared, b"record");
}

/// A read that caches what it reads publishes its frame into the cache as it
/// hands it to its waiters, so a miss after it finds the frame there.
#[test]
fn a_lead_that_fills_caches_the_frame_it_publishes() {
    use super::BlobClaim;
    use crate::vlog::blob_file::frame::decoded_frame;
    use std::sync::Arc;

    let cache = Cache::with_capacity_bytes(1024 * 1024)
        .with_load_coalescing_after(core::time::Duration::ZERO);
    let at = super::BlobFrameAt {
        blob_file_id: 3,
        frame: 0,
    };
    let BlobClaim::Lead(lead) = cache.claim_blob_frame(0, at) else {
        panic!("the first read leads");
    };
    lead.publish(
        &cache,
        0,
        at,
        &Arc::new(decoded_frame(&[(b"owner", b"value")])),
        true,
    );
    let cached = cache.get_blob_frame(0, at).expect("the frame is cached");
    assert_eq!(cached.key(0), Some(&b"owner"[..]));
}

/// A partial-tier entry whose prefix decoded `decoded` inner blocks and served
/// `hits` reads.
#[cfg(feature = "zstd")]
fn partial_entry(decoded: u32, hits: u32) -> super::PartialBlockEntry {
    super::PartialBlockEntry {
        resume: crate::table::lazy_block::PartialResume {
            window_prime: crate::Slice::from(vec![0_u8; decoded as usize * 16]),
            decoded_blocks: decoded,
            state: None,
            compressed_cursor: 0,
        },
        covered_upper: crate::UserKey::from(format!("upper-{decoded}")),
        total_blocks: 8,
        hits,
    }
}

/// Recording a partial prefix never shrinks the cached one, whichever of two
/// concurrent reads records last: a read that grew the prefix less, or served
/// from an older copy of it and bumps its hit count, keeps the longer prefix
/// and the higher hit count instead of overwriting them.
#[cfg(feature = "zstd")]
#[test]
fn partial_prefix_never_shrinks_whichever_read_records_last() {
    let id = crate::table::GlobalTableId::from((0, 1));
    let offset = crate::table::BlockOffset(0);
    let resident = |cache: &Cache| {
        let Some(entry) = cache.peek_partial_block(id, offset) else {
            panic!("a partial entry is cached");
        };
        (entry.resume.decoded_blocks, entry.hits, entry.covered_upper)
    };

    // The longer growth records first, then the shorter one.
    let cache = Cache::with_capacity_bytes(1024 * 1024);
    cache.insert_partial_block(id, offset, partial_entry(5, 1));
    cache.insert_partial_block(id, offset, partial_entry(2, 0));
    assert_eq!(resident(&cache), (5, 1, crate::UserKey::from("upper-5")));

    // The shorter growth records first, then the longer one.
    let cache = Cache::with_capacity_bytes(1024 * 1024);
    cache.insert_partial_block(id, offset, partial_entry(2, 0));
    cache.insert_partial_block(id, offset, partial_entry(5, 1));
    assert_eq!(resident(&cache), (5, 1, crate::UserKey::from("upper-5")));

    // A hit bump made on a copy taken before the prefix grew keeps the grown
    // prefix and carries the bump.
    let cache = Cache::with_capacity_bytes(1024 * 1024);
    let older = partial_entry(2, 2);
    cache.insert_partial_block(id, offset, partial_entry(5, 1));
    cache.insert_partial_block(
        id,
        offset,
        super::PartialBlockEntry {
            hits: older.hits + 1,
            ..older
        },
    );
    assert_eq!(resident(&cache), (5, 3, crate::UserKey::from("upper-5")));

    // An equally long prefix takes the newer hit count.
    let cache = Cache::with_capacity_bytes(1024 * 1024);
    cache.insert_partial_block(id, offset, partial_entry(5, 1));
    cache.insert_partial_block(id, offset, partial_entry(5, 2));
    assert_eq!(resident(&cache), (5, 2, crate::UserKey::from("upper-5")));
}
