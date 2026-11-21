use super::*;

/// A memtable holds at least one block, taken by the skiplist's head node, and
/// a platform that commits heap allocations up front (Windows) charges the
/// whole block to the process then. Blocks are 4 MiB on every target, so an
/// idle memtable costs 4 MiB there; before, 64-bit targets took 64 MiB blocks.
#[test]
fn block_size_on_every_target_is_4_mib() {
    assert_eq!(BLOCK_SIZE, 4 * 1024 * 1024);
    assert_eq!(MAX_BLOCKS, 1024, "4 GiB of arena in 4 MiB blocks");
}

/// A lightly filled arena allocates only the block its data sits in.
#[test]
fn small_allocations_on_a_fresh_arena_take_only_block_0() {
    let arena = Arena::new();
    for _ in 0..1000 {
        let off = arena.alloc(64, 4).expect("alloc");
        assert_eq!(off >> BLOCK_SHIFT, 0);
    }
    assert!(!arena.blocks[0].load(Ordering::Relaxed).is_null());
    assert!(
        arena.blocks[1..]
            .iter()
            .all(|block| block.load(Ordering::Relaxed).is_null()),
        "no block past the first was allocated",
    );
}

/// A view that outlives the arena frees its block with the size the block was
/// allocated at.
#[cfg(not(feature = "bytes_1"))]
#[test]
fn a_view_into_a_block_outlives_the_arena() {
    let arena = Arena::new();
    let off = arena.alloc(40, 4).expect("alloc");
    // SAFETY: freshly allocated, exclusive access.
    unsafe {
        arena.get_bytes_mut(off, 40).copy_from_slice(&[7u8; 40]);
    }
    // SAFETY: the span was allocated and fully written above.
    let view = unsafe { arena.get_view(off, 40) };
    drop(arena);
    assert_eq!(&*view, &[7u8; 40][..]);
}

#[test]
fn basic_alloc_and_read() {
    let arena = Arena::new();

    let off = arena.alloc(4, 4).expect("should succeed");
    assert!(off >= 1);
    assert_eq!(off & 3, 0);

    // SAFETY: freshly allocated, exclusive access.
    unsafe {
        let bytes = arena.get_bytes_mut(off, 4);
        bytes.copy_from_slice(&[1, 2, 3, 4]);
    }

    let read = unsafe { arena.get_bytes(off, 4) };
    assert_eq!(read, &[1, 2, 3, 4]);
}

#[test]
fn alloc_respects_alignment() {
    let arena = Arena::new();
    let a = arena.alloc(1, 1).expect("ok");
    let b = arena.alloc(4, 4).expect("ok");
    assert_eq!(b & 3, 0);
    assert!(b > a);
}

#[test]
fn alloc_crosses_block_boundary() {
    let arena = Arena::new();
    let big = BLOCK_SIZE - 64;
    let off1 = arena.alloc(big, 1).expect("ok");
    assert_eq!(off1 >> BLOCK_SHIFT, 0);

    let off2 = arena.alloc(128, 4).expect("ok");
    assert_eq!(off2 >> BLOCK_SHIFT, 1);
}

/// Past the last block the arena is exhausted: an allocation that does not fit
/// the last block is refused rather than wrapping to an earlier block, and one
/// that fits still lands in it.
#[test]
fn an_allocation_past_the_last_block_is_refused() {
    let arena = Arena::new();
    let last = u32::try_from(MAX_BLOCKS - 1).expect("a block index fits u32");
    arena.cursor.store(last << BLOCK_SHIFT, Ordering::Relaxed);
    let big = BLOCK_SIZE - 64;
    let off = arena.alloc(big, 1).expect("fits the last block");
    assert_eq!(off >> BLOCK_SHIFT, last);
    assert!(
        arena.alloc(128, 4).is_none(),
        "no block follows the last one"
    );
    let tail = arena
        .alloc(8, 4)
        .expect("the last block still has room for this");
    assert_eq!(tail >> BLOCK_SHIFT, last);
}

#[test]
fn atomic_u32_round_trip() {
    let arena = Arena::new();
    let off = arena.alloc(4, 4).expect("ok");

    // SAFETY: freshly allocated, 4-byte aligned.
    unsafe {
        let atom = arena.get_atomic_u32(off);
        atom.store(42, Ordering::Relaxed);
        assert_eq!(atom.load(Ordering::Relaxed), 42);
    }
}

#[test]
fn concurrent_alloc() {
    use std::sync::Arc;

    let arena = Arc::new(Arena::new());
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let arena = Arc::clone(&arena);
            std::thread::spawn(move || {
                let mut offsets = Vec::new();
                for _ in 0..1000 {
                    if let Some(off) = arena.alloc(64, 4) {
                        offsets.push(off);
                    }
                }
                offsets
            })
        })
        .collect();

    let mut all_offsets: Vec<u32> = Vec::new();
    for h in handles {
        all_offsets.extend(h.join().expect("thread ok"));
    }

    all_offsets.sort();
    all_offsets.dedup();
    assert_eq!(all_offsets.len(), 8000);
}

#[test]
fn alloc_invalid_alignment_returns_none() {
    let arena = Arena::new();
    assert!(arena.alloc(100, 3).is_none()); // 3 is not a power of two
    assert!(arena.alloc(0, 4).is_none()); // zero size
    assert!(arena.alloc(BLOCK_SIZE, 1).is_none()); // size == BLOCK_SIZE
    assert!(arena.alloc(BLOCK_SIZE + 1, 1).is_none()); // size > BLOCK_SIZE
}

#[test]
fn default_impl() {
    let arena = Arena::default();
    let off = arena.alloc(8, 4).expect("should work");
    assert!(off > 0);
}

#[test]
fn drop_with_multiple_blocks() {
    let arena = Arena::new();
    // Allocate across 2 blocks to exercise Drop on both.
    let big = BLOCK_SIZE - 8;
    let _ = arena.alloc(big, 1).expect("block 0");
    let _ = arena.alloc(64, 4).expect("block 1");
    // Drop runs here — deallocates both blocks.
}

/// Regression test for #119: when an allocation fills a block exactly
/// to BLOCK_SIZE, the cursor OR produced `(block_idx << SHIFT) | BLOCK_SIZE`
/// which wrapped back to offset 0 of the *same* block, causing subsequent
/// allocations to overwrite existing data.
///
/// The bug only triggers when block_idx >= 1 because for block 0
/// `(0 << SHIFT) | BLOCK_SIZE` correctly decodes as block 1, offset 0.
/// For block_idx >= 1 the BLOCK_SHIFT bit is already set in the block
/// index, so the OR does not carry and the cursor wraps.
#[test]
fn exact_block_fill_does_not_corrupt() {
    let arena = Arena::new();

    // Jump the cursor directly to block 1, offset 0, without allocating
    // block 0 just to advance past it.
    arena.cursor.store(1 << BLOCK_SHIFT, Ordering::Relaxed);

    // Allocate (BLOCK_SIZE - 4) bytes to bring block 1's cursor to
    // offset BLOCK_SIZE - 4.
    let filler = BLOCK_SIZE - 4;
    let f = arena.alloc(filler, 1).expect("filler");
    assert_eq!(f >> BLOCK_SHIFT, 1, "filler should be in block 1");

    // Write a sentinel pattern into the last allocated byte.
    // SAFETY: `f` was just returned by alloc(filler, 1), so
    // [f, f+filler) is allocated and we have exclusive access.
    unsafe {
        let bytes = arena.get_bytes_mut(f, filler);
        bytes[filler as usize - 1] = 0xAB;
    }

    // Now cursor is at BLOCK_SIZE - 4 within block 1.  Allocate exactly
    // 4 bytes (align=4): new_end = BLOCK_SIZE exactly.  With the fix,
    // this allocation moves to block 2 (the tail bytes in block 1 are
    // sacrificed).
    let boundary = arena.alloc(4, 4).expect("boundary alloc");
    assert_eq!(
        boundary >> BLOCK_SHIFT,
        2,
        "exact-fill allocation must advance to the next block"
    );

    // A further allocation must also be in block 2 (not wrap to block 1).
    let next = arena.alloc(8, 4).expect("next alloc");
    assert_eq!(
        next >> BLOCK_SHIFT,
        2,
        "subsequent allocation must stay in the advanced block"
    );

    // Verify the sentinel byte in block 1 was NOT overwritten.
    // SAFETY: `f` is the offset returned by alloc(filler, 1) above,
    // guaranteeing [f, f+filler) is allocated and initialised.
    let read_sentinel = unsafe { arena.get_bytes(f, filler) };
    assert_eq!(
        read_sentinel[filler as usize - 1],
        0xAB,
        "block 1 data must not be corrupted by subsequent allocations"
    );
}

/// The block an arena allocated first, as the next arena on the same pool
/// sees it.
fn first_block(arena: &Arena) -> *mut u8 {
    arena.alloc(8, 4).expect("alloc");
    arena.blocks[0].load(Ordering::Acquire)
}

/// A pool of the test's own, apart from the one every memtable shares.
fn own_pool() -> &'static BlockPool {
    Box::leak(Box::new(BlockPool::new()))
}

/// A block nothing views any more goes to the pool when its arena drops and
/// is the next arena's first block, the next memtable's of any tree: it writes
/// into memory already mapped instead of faulting in fresh pages.
#[test]
fn a_dropped_arena_hands_its_unviewed_block_to_the_next() {
    let pool = own_pool();
    let first = Arena::with_pool(pool);
    let block = first_block(&first);
    drop(first);
    let second = Arena::with_pool(pool);
    assert_eq!(first_block(&second), block);
}

/// A block a view still reads stays with the view: the next arena gets
/// another, and writing into it leaves the viewed bytes as they were. The
/// `bytes` backend takes no views of the arena.
#[test]
#[cfg(not(feature = "bytes_1"))]
fn a_viewed_block_is_not_handed_on() {
    let pool = own_pool();
    let first = Arena::with_pool(pool);
    let at = first.alloc(64, 4).expect("alloc");
    // SAFETY: `at..at+64` was just allocated; written before it is viewed.
    unsafe { first.get_bytes_mut(at, 64).fill(0xAB) };
    let block = first.blocks[0].load(Ordering::Acquire);
    // SAFETY: the span was allocated and written above, and not written again.
    let view = unsafe { first.get_view(at, 64) };
    drop(first);

    let second = Arena::with_pool(pool);
    assert_ne!(first_block(&second), block, "a viewed block was handed on");
    let next = second.alloc(64, 4).expect("alloc");
    // SAFETY: `next..next+64` was just allocated.
    unsafe { second.get_bytes_mut(next, 64).fill(0x00) };
    assert_eq!(&*view, &[0xAB; 64][..]);
}

/// An arena of `blocks` blocks on `pool`, dropped.
fn drop_arena_of(pool: &'static BlockPool, blocks: usize) {
    let arena = Arena::with_pool(pool);
    for _ in 0..blocks {
        // Each nearly a block, so every one takes a block of its own.
        arena.alloc(BLOCK_SIZE - 8, 4).expect("alloc");
    }
    drop(arena);
}

/// The empty memtable a tree closes with, dropped right after a large one
/// was flushed, does not shrink the pool to its own single block: the next
/// tree finds the large one's blocks.
#[test]
fn a_small_arena_after_a_large_one_leaves_the_large_ones_blocks() {
    let pool = own_pool();
    drop_arena_of(pool, 6);
    drop_arena_of(pool, 1);
    assert_eq!(pool.pooled(), 6);
}

/// Once memtables shrink, the pool comes down to their size in a few drops
/// instead of holding the memory of a larger one for good.
#[test]
fn the_pool_comes_down_to_the_memtables_it_serves() {
    let pool = own_pool();
    drop_arena_of(pool, 8);
    assert_eq!(pool.pooled(), 8);
    for _ in 0..6 {
        drop_arena_of(pool, 2);
    }
    assert_eq!(pool.pooled(), 2);
}
