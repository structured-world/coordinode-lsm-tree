use super::*;

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

    // Jump the cursor directly to block 1, offset 0 — avoids allocating
    // an entire block 0 (64 MiB on 64-bit) just to advance past it.
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
