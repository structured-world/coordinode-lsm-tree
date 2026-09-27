use super::*;

/// A memtable's first block is small, so an empty or lightly filled memtable
/// holds kilobytes, not a whole full-size block: an allocation that does not
/// fit the 64 KiB first block moves to the next one. Before, block 0 was a
/// full `BLOCK_SIZE`, taken by the skiplist's head node alone, which on a
/// platform that commits heap allocations up front is that many bytes
/// committed per memtable.
#[test]
fn the_first_block_is_small_and_blocks_double_up_to_the_full_size() {
    const FIRST: u32 = 64 * 1024;
    let arena = Arena::new();
    let head = arena.alloc(64, 4).expect("head");
    assert_eq!(head >> BLOCK_SHIFT, 0, "the first allocation is in block 0");
    let past = arena
        .alloc(FIRST - 32, 1)
        .expect("an allocation past 64 KiB");
    assert_eq!(
        past >> BLOCK_SHIFT,
        1,
        "block 0 holds only its 64 KiB, so this moves to block 1"
    );
    assert_eq!(block_capacity(0), FIRST);
    assert_eq!(block_capacity(1), 2 * FIRST);
    let full = (0..MAX_BLOCKS)
        .find(|&idx| block_capacity(idx) == BLOCK_SIZE)
        .expect("capacities reach the full block size");
    assert!(
        (0..full).all(|idx| block_capacity(idx + 1) == 2 * block_capacity(idx)),
        "each block up to the full size doubles the one before it",
    );
    assert!(
        (full..MAX_BLOCKS).all(|idx| block_capacity(idx) == BLOCK_SIZE),
        "every block past that is full size",
    );
}

/// A view into a small block that outlives the arena frees the block with the
/// size it was allocated at: the view carries that size, and a mismatch would
/// free the region under the wrong layout.
#[cfg(not(feature = "bytes_1"))]
#[test]
fn a_view_into_a_small_block_outlives_the_arena() {
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

/// The first block that holds a full `BLOCK_SIZE`.
fn first_full_block() -> u32 {
    let idx = (0..MAX_BLOCKS)
        .find(|&idx| block_capacity(idx) == BLOCK_SIZE)
        .expect("capacities reach the full block size");
    u32::try_from(idx).expect("a block index fits u32")
}

#[test]
fn alloc_crosses_block_boundary() {
    let arena = Arena::new();
    let full = first_full_block();
    // Jump to the first full-size block rather than fill every smaller one.
    arena.cursor.store(full << BLOCK_SHIFT, Ordering::Relaxed);
    let big = BLOCK_SIZE - 64;
    let off1 = arena.alloc(big, 1).expect("ok");
    assert_eq!(off1 >> BLOCK_SHIFT, full);

    let off2 = arena.alloc(128, 4).expect("ok");
    assert_eq!(off2 >> BLOCK_SHIFT, full + 1);
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

/// An allocation larger than the small blocks can hold skips past them to the
/// first block that fits it, and reads back from there.
#[test]
fn an_allocation_larger_than_the_small_blocks_lands_in_one_that_fits() {
    let arena = Arena::new();
    let big = BLOCK_SIZE - 64;
    let off = arena.alloc(big, 1).expect("ok");
    assert_eq!(off >> BLOCK_SHIFT, first_full_block());
    // SAFETY: freshly allocated, exclusive access.
    unsafe {
        let bytes = arena.get_bytes_mut(off, big);
        bytes[0] = 1;
        bytes[big as usize - 1] = 2;
    }
    // SAFETY: allocated and both bytes read here were written above.
    let read = unsafe { arena.get_bytes(off, big) };
    assert_eq!((read[0], read[big as usize - 1]), (1, 2));
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
    // A small first block, then a full-size one, then the one after it:
    // Drop releases each at the size it was allocated at.
    let _ = arena.alloc(64, 4).expect("block 0");
    let big = BLOCK_SIZE - 8;
    let _ = arena.alloc(big, 1).expect("first full block");
    let _ = arena.alloc(64, 4).expect("the block after it");
    // Drop runs here — deallocates every block.
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

    // Jump the cursor directly to the first full-size block, at an index of
    // at least 1 — the case the bug needs — without allocating every block
    // before it.
    let full = first_full_block();
    assert!(full >= 1);
    arena.cursor.store(full << BLOCK_SHIFT, Ordering::Relaxed);

    // Allocate (BLOCK_SIZE - 4) bytes to bring that block's cursor to
    // offset BLOCK_SIZE - 4.
    let filler = BLOCK_SIZE - 4;
    let f = arena.alloc(filler, 1).expect("filler");
    assert_eq!(f >> BLOCK_SHIFT, full, "filler should be in the full block");

    // Write a sentinel pattern into the last allocated byte.
    // SAFETY: `f` was just returned by alloc(filler, 1), so
    // [f, f+filler) is allocated and we have exclusive access.
    unsafe {
        let bytes = arena.get_bytes_mut(f, filler);
        bytes[filler as usize - 1] = 0xAB;
    }

    // Now cursor is at BLOCK_SIZE - 4 within that block.  Allocate exactly
    // 4 bytes (align=4): new_end = BLOCK_SIZE exactly.  With the fix,
    // this allocation moves to the next block (the tail bytes of this one
    // are sacrificed).
    let boundary = arena.alloc(4, 4).expect("boundary alloc");
    assert_eq!(
        boundary >> BLOCK_SHIFT,
        full + 1,
        "exact-fill allocation must advance to the next block"
    );

    // A further allocation must also be in the next block (not wrap back).
    let next = arena.alloc(8, 4).expect("next alloc");
    assert_eq!(
        next >> BLOCK_SHIFT,
        full + 1,
        "subsequent allocation must stay in the advanced block"
    );

    // Verify the sentinel byte in the filled block was NOT overwritten.
    // SAFETY: `f` is the offset returned by alloc(filler, 1) above,
    // guaranteeing [f, f+filler) is allocated and initialised.
    let read_sentinel = unsafe { arena.get_bytes(f, filler) };
    assert_eq!(
        read_sentinel[filler as usize - 1],
        0xAB,
        "block 1 data must not be corrupted by subsequent allocations"
    );
}
