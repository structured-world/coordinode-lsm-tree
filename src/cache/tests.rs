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

#[test]
fn metadata_priority_defaults_on_and_toggles() {
    // On by default.
    assert!(Cache::with_capacity_bytes(1024).metadata_priority());
    // Builder turns it off and back on.
    let off = Cache::with_capacity_bytes(1024).with_metadata_priority(false);
    assert!(!off.metadata_priority());
    assert!(off.with_metadata_priority(true).metadata_priority());
}

/// A blob cache entry is keyed by a POSITION in a blob file, and a corrupt
/// index entry can point a second key at a position that already holds another
/// key's value. A direct read catches that: the reader compares the key it was
/// asked for against the one stored in the record, and refuses the mismatch.
/// The cached path has to reach the same verdict, or a corrupt entry served
/// from cache would silently return the neighbouring key's value instead of
/// reporting the corruption.
#[test]
fn a_blob_lookup_under_a_conflicting_key_misses_rather_than_serving_the_other_value() {
    use crate::vlog::ValueHandle;

    let cache = Cache::with_capacity_bytes(1024 * 1024);
    let vhandle = ValueHandle {
        blob_file_id: 7,
        offset: 4096,
        on_disk_size: 5,
    };

    let copied = cache.insert_blob(
        0,
        &vhandle,
        b"real-key",
        crate::UserValue::from(&b"value"[..]),
    );
    assert_eq!(
        copied,
        b"real-key".len(),
        "the entry owns a copy of its key"
    );

    // The key it was stored under still finds it.
    assert_eq!(
        cache.get_blob(0, &vhandle, b"real-key").as_deref(),
        Some(&b"value"[..]),
        "the owning key must still hit",
    );

    // A different key pointed at the same position must NOT be served this
    // value; it reads as a miss so the caller does the real read and gets the
    // real error.
    assert!(
        cache.get_blob(0, &vhandle, b"other-key").is_none(),
        "a conflicting key must not be served the value at that offset",
    );
}

/// The key is not the only thing a direct read checks: `Reader::parse_record`
/// also rejects a handle whose declared `on_disk_size` disagrees with the size
/// in the record header. A corrupt handle can name the right key at the right
/// offset and still be wrong about the size, so the cached path has to compare
/// that too or it serves a value the reader would have refused.
#[test]
fn a_blob_lookup_with_a_conflicting_size_misses_rather_than_serving_the_value() {
    use crate::vlog::ValueHandle;

    let cache = Cache::with_capacity_bytes(1024 * 1024);
    let stored = ValueHandle {
        blob_file_id: 7,
        offset: 4096,
        on_disk_size: 5,
    };

    let copied = cache.insert_blob(0, &stored, b"key", crate::UserValue::from(&b"value"[..]));
    assert_eq!(copied, b"key".len(), "the entry owns a copy of its key");

    assert_eq!(
        cache.get_blob(0, &stored, b"key").as_deref(),
        Some(&b"value"[..]),
        "the handle it was stored under must still hit",
    );

    // Same file, same offset, same key, different declared size: the reader
    // would reject this handle, so the cache must not answer for it.
    let conflicting = ValueHandle {
        on_disk_size: 9,
        ..stored
    };
    assert!(
        cache.get_blob(0, &conflicting, b"key").is_none(),
        "a handle declaring a different size must not be served the cached value",
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

    let cache = Arc::new(Cache::with_capacity_bytes(1024 * 1024));
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
            let BlockClaim::Lead(batched) =
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
    held.publish(crate::table::Block {
        header: crate::table::block::Header::test_dummy(crate::table::block::BlockType::Data),
        data: crate::Slice::from(&b"block"[..]),
    });
    assert!(waiter.join().expect("waiter"), "took the published block");
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
