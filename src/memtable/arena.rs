// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

//! Multi-block bump-allocating arena for skiplist node storage.
//!
//! Blocks grow **geometrically**: the first chunk is small ([`FIRST_BLOCK_SIZE`])
//! and each subsequent chunk doubles (the largest is `2^31`). A small memtable
//! therefore allocates (and zeroes) only a small first chunk instead of one
//! fixed giant block; a large one grows on demand. The arena never
//! pre-allocates a contiguous buffer, so it works on 32-bit targets too.
//! Once a block cannot fit an allocation, a new (larger) one is allocated and
//! the remaining space is abandoned (negligible for typical < 100-byte nodes).
//!
//! # Address encoding
//!
//! [`alloc`](Arena::alloc) returns an opaque `u32` global byte offset into the
//! logical concatenation of all blocks. Offset `0` is the `UNSET` sentinel, so
//! offsets start at `1`. With block sizes `2^S, 2^S, 2^(S+1), 2^(S+2), …`
//! (`S` = [`FIRST_BLOCK_SHIFT`]), block `i >= 1` starts at `2^(S+i-1)`, which
//! makes the offset → `(block, within)` decode a constant-time bit operation
//! (a `leading_zeros`, no table, no search). Total addressable space is 4 GiB.

use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

/// Shift of the first (and second) arena chunk: `2^16 = 64 KiB`. Blocks 0 and 1
/// are this size; block `i >= 2` is `2^(FIRST_BLOCK_SHIFT + i - 1)` (doubling).
const FIRST_BLOCK_SHIFT: u32 = 16;

/// Size of the first two arena chunks in bytes (`64 KiB`).
const FIRST_BLOCK_SIZE: u32 = 1 << FIRST_BLOCK_SHIFT;

/// Maximum number of blocks: `33 - FIRST_BLOCK_SHIFT`. Block `i >= 1` starts at
/// `2.pow(FIRST_BLOCK_SHIFT + i - 1)`; the last start representable in a `u32`
/// is `2.pow(31)`, so the blocks span the full 4 GiB `u32` address space.
const MAX_BLOCKS: usize = (33 - FIRST_BLOCK_SHIFT) as usize;

/// Largest single allocation the arena can place in one (contiguous) block:
/// the size of the biggest block, `2^31`. Requests at or above this are
/// rejected (`alloc` returns `None`).
const MAX_ALLOC: u32 = 1 << 31;

/// Size in bytes of block `idx` (`2^16` for blocks 0 and 1, doubling after).
#[inline]
#[expect(
    clippy::cast_possible_truncation,
    reason = "idx < MAX_BLOCKS (<= 17), well within u32"
)]
const fn block_size(idx: usize) -> u32 {
    if idx == 0 {
        FIRST_BLOCK_SIZE
    } else {
        1u32 << (FIRST_BLOCK_SHIFT + idx as u32 - 1)
    }
}

/// Global byte offset at which block `idx` starts. `0` for block 0; for
/// `idx >= 1` it equals `block_size(idx)` (`2^(FIRST_BLOCK_SHIFT + idx - 1)`).
#[inline]
#[expect(
    clippy::cast_possible_truncation,
    reason = "idx < MAX_BLOCKS (<= 17), well within u32"
)]
const fn block_start(idx: usize) -> u32 {
    if idx == 0 {
        0
    } else {
        1u32 << (FIRST_BLOCK_SHIFT + idx as u32 - 1)
    }
}

/// Decodes a global byte offset into `(block_index, within_block_offset)` in
/// constant time. Offsets below [`FIRST_BLOCK_SIZE`] live in block 0; otherwise
/// the block index is derived from the position of the offset's highest set bit
/// (the buddy layout makes `block_start(i) == 2^(S+i-1)`).
#[inline]
fn locate(offset: u32) -> (usize, u32) {
    if offset < FIRST_BLOCK_SIZE {
        (0, offset)
    } else {
        // `offset >= FIRST_BLOCK_SIZE > 0`, so `ilog2()` is well-defined.
        let highest_bit = offset.ilog2();
        let block = (highest_bit - FIRST_BLOCK_SHIFT + 1) as usize;
        let within = offset - (1u32 << highest_bit);
        (block, within)
    }
}

/// A multi-block bump-allocating arena.
///
/// Thread-safe: concurrent allocations are serialised by a CAS loop on the
/// bump cursor.  Blocks are allocated lazily via CAS on `AtomicPtr`, so only
/// the blocks that are actually needed consume memory.
///
/// The `u32` offset returned by [`alloc`](Self::alloc) is a global byte offset
/// into the logical concatenation of the (geometrically growing) blocks; see
/// the module docs for the encoding. Decode it with [`locate`].
pub struct Arena {
    /// Block pointers.  Null means not yet allocated.  Once set to non-null,
    /// a block pointer is never modified — reads may use `Relaxed` ordering
    /// as long as the caller establishes happens-before via the skiplist CAS
    /// chain.
    blocks: Box<[AtomicPtr<u8>]>,

    /// Allocation cursor: the next free global byte offset (across all blocks).
    /// Starts at 1 (offset 0 is the UNSET sentinel).
    cursor: AtomicU32,
}

// Send+Sync derived automatically: all fields (Box<[AtomicPtr<_>]>, AtomicU32)
// are Send+Sync.

impl Arena {
    /// Creates a new empty arena.  No memory is allocated until the first
    /// [`alloc`](Self::alloc) call.
    pub fn new() -> Self {
        let mut blocks = Vec::with_capacity(MAX_BLOCKS);
        for _ in 0..MAX_BLOCKS {
            blocks.push(AtomicPtr::new(ptr::null_mut()));
        }

        Self {
            blocks: blocks.into_boxed_slice(),
            // Offset 0 is reserved as the UNSET sentinel.
            cursor: AtomicU32::new(1),
        }
    }

    /// Allocates `size` bytes with the given alignment.
    ///
    /// Returns the global byte offset, or `None` if `size` is zero,
    /// `size >= MAX_ALLOC` (too large for any single block), `align` is not a
    /// power of two, or the arena is exhausted (> 4 GiB total).
    pub fn alloc(&self, size: u32, align: u32) -> Option<u32> {
        if !align.is_power_of_two() || size == 0 || size >= MAX_ALLOC {
            return None;
        }

        loop {
            // Acquire pairs with the AcqRel CAS below: any thread that reads
            // a cursor value is guaranteed to see the corresponding
            // blocks[block_idx] pointer set by ensure_block, which runs before
            // the CAS that published this cursor value.
            let cur = self.cursor.load(Ordering::Acquire);
            let (block_idx, within) = locate(cur);
            let bsize = block_size(block_idx);
            // within < bsize <= 2^31 and align <= bsize, so no overflow.
            let aligned = (within + align - 1) & !(align - 1);

            // Fits the current block? (`aligned + size <= bsize`, overflow-safe.)
            if aligned.checked_add(size).is_some_and(|end| end <= bsize) {
                // Ensure the block exists BEFORE publishing the offset via CAS —
                // otherwise another thread could read the cursor, locate() the
                // same block, and decode() before the block pointer is set.
                self.ensure_block(block_idx);

                let alloc_offset = block_start(block_idx) + aligned;
                // Cannot overflow: alloc_offset + size <= block_start + bsize =
                // block_start(block_idx + 1) <= 2^32 (== for the last block,
                // where size keeps it strictly below since `size < MAX_ALLOC`).
                let new_cursor = alloc_offset.checked_add(size)?;
                if self
                    .cursor
                    .compare_exchange_weak(cur, new_cursor, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    return Some(alloc_offset);
                }
            } else {
                // Doesn't fit: advance to the first later block large enough to
                // hold `size` from offset 0 (skipping early small blocks for an
                // outsized allocation). The abandoned tail of the current block
                // is negligible for typical node sizes. Ensure the target block
                // exists BEFORE publishing the cursor so a concurrent reader
                // always finds a valid pointer.
                let mut next = block_idx + 1;
                while next < MAX_BLOCKS && block_size(next) < size {
                    next += 1;
                }
                if next >= MAX_BLOCKS {
                    return None;
                }
                self.ensure_block(next);
                let _ = self.cursor.compare_exchange_weak(
                    cur,
                    block_start(next),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );
            }
        }
    }

    /// Returns a shared reference to `len` bytes at the encoded `offset`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `offset..offset+len` was previously
    /// allocated by this arena and fully initialised.  The caller must also
    /// establish happens-before (typically via the skiplist CAS chain) so
    /// that the block pointer is visible.
    pub unsafe fn get_bytes(&self, offset: u32, len: u32) -> &[u8] {
        let (ptr, off) = unsafe { self.decode(offset) };
        debug_assert!(
            off + len as usize <= block_size(locate(offset).0) as usize,
            "get_bytes: off={off} + len={len} exceeds block size (offset={offset})",
        );
        // SAFETY: caller guarantees the range is allocated and initialised.
        unsafe { std::slice::from_raw_parts(ptr.add(off), len as usize) }
    }

    /// Returns an exclusive reference to `len` bytes at the encoded `offset`.
    ///
    /// # Safety
    ///
    /// The caller must ensure exclusive access to the given range.
    #[expect(
        clippy::mut_from_ref,
        reason = "interior mutability by design; caller guarantees exclusive access"
    )]
    pub unsafe fn get_bytes_mut(&self, offset: u32, len: u32) -> &mut [u8] {
        let (ptr, off) = unsafe { self.decode(offset) };
        // SAFETY: caller guarantees exclusive access (typically right after alloc,
        // before the node offset is published to other threads).
        unsafe { std::slice::from_raw_parts_mut(ptr.add(off), len as usize) }
    }

    /// Interprets 4 bytes at `offset` as an [`AtomicU32`] reference.
    ///
    /// # Safety
    ///
    /// - `offset` must be 4-byte aligned.
    /// - The region `[offset, offset+4)` must have been previously allocated.
    /// - No `&mut` reference to the same 4 bytes may exist concurrently.
    pub unsafe fn get_atomic_u32(&self, offset: u32) -> &AtomicU32 {
        let (ptr, off) = unsafe { self.decode(offset) };
        // SAFETY: caller guarantees alignment and prior allocation.
        // alloc(..., 4) ensures within-block alignment; the block base has
        // at least pointer-width alignment from the global allocator.
        #[expect(
            clippy::cast_ptr_alignment,
            reason = "caller guarantees 4-byte alignment via alloc(..., 4)"
        )]
        let atom_ptr = unsafe { ptr.add(off).cast::<u32>() };
        unsafe { AtomicU32::from_ptr(atom_ptr) }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Decodes an encoded offset into `(block_base_ptr, within_block_offset)`.
    ///
    /// Hot path: single `Acquire` load returns the cached block pointer.
    /// Cold path: spins until the block pointer becomes visible (another
    /// thread's `ensure_block` is in progress).
    #[inline]
    #[expect(
        clippy::indexing_slicing,
        reason = "block_idx < MAX_BLOCKS by construction (alloc enforces this)"
    )]
    unsafe fn decode(&self, offset: u32) -> (*mut u8, usize) {
        let (block_idx, within) = locate(offset);
        let off = within as usize;

        let mut ptr = self.blocks[block_idx].load(Ordering::Acquire);
        if ptr.is_null() {
            // The block is being allocated by another thread's ensure_block.
            // Spin briefly — ensure_block uses CAS with AcqRel, so the
            // pointer will become visible after a few iterations.
            for _ in 0..1000 {
                std::hint::spin_loop();
                ptr = self.blocks[block_idx].load(Ordering::Acquire);
                if !ptr.is_null() {
                    return (ptr, off);
                }
            }
            // If still null after spinning, allocate the block ourselves.
            self.ensure_block(block_idx);
            ptr = self.blocks[block_idx].load(Ordering::Acquire);
        }

        (ptr, off)
    }

    /// Ensures that the block at `idx` is allocated.  Uses CAS to avoid
    /// double-allocation when multiple threads race.
    #[expect(
        clippy::indexing_slicing,
        reason = "idx < MAX_BLOCKS enforced by alloc()"
    )]
    fn ensure_block(&self, idx: usize) {
        if self.blocks[idx].load(Ordering::Acquire).is_null() {
            // Allocate with explicit 4-byte alignment so that AtomicU32
            // accesses within the block are correctly aligned on all targets.
            let layout = Self::block_layout(idx);

            // SAFETY: layout is non-zero (block_size(idx) > 0).
            // alloc_zeroed guarantees zeroed memory (tower pointers = UNSET).
            // Visibility: the CAS below (AcqRel) makes the zeroed contents
            // visible to any thread that Acquire-loads the block pointer.
            let raw = unsafe { std::alloc::alloc_zeroed(layout) };
            if raw.is_null() {
                std::alloc::handle_alloc_error(layout);
            }

            // CAS null → raw.  If another thread won, free our block.
            if self.blocks[idx]
                .compare_exchange(ptr::null_mut(), raw, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                // SAFETY: raw was just allocated with `layout`.
                unsafe {
                    std::alloc::dealloc(raw, layout);
                }
            }
        }
    }

    /// Layout for block `idx`: `block_size(idx)` bytes with 4-byte alignment
    /// (required for `AtomicU32` tower pointers).
    fn block_layout(idx: usize) -> std::alloc::Layout {
        // SAFETY: block_size(idx) > 0 and align (4) is a power of two.
        unsafe { std::alloc::Layout::from_size_align_unchecked(block_size(idx) as usize, 4) }
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        for (idx, block) in self.blocks.iter().enumerate() {
            let ptr = block.load(Ordering::Relaxed);
            if !ptr.is_null() {
                // SAFETY: `ptr` was allocated for block `idx` using
                // `block_layout(idx)`, so deallocating with the same per-index
                // layout is valid.
                unsafe {
                    std::alloc::dealloc(ptr, Self::block_layout(idx));
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, reason = "tests use expect for brevity")]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    clippy::doc_markdown,
    clippy::stable_sort_primitive,
    reason = "test code"
)]
mod tests {
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
        // Nearly fill block 0; the next alloc must spill into block 1.
        let big = FIRST_BLOCK_SIZE - 64;
        let off1 = arena.alloc(big, 1).expect("ok");
        assert_eq!(locate(off1).0, 0, "first alloc in block 0");

        let off2 = arena.alloc(128, 4).expect("ok");
        assert_eq!(locate(off2).0, 1, "spill alloc in block 1");
    }

    #[test]
    fn block_sizes_grow_geometrically() {
        // Blocks 0 and 1 are the base size; each later block doubles.
        assert_eq!(block_size(0), FIRST_BLOCK_SIZE);
        assert_eq!(block_size(1), FIRST_BLOCK_SIZE);
        assert_eq!(block_size(2), FIRST_BLOCK_SIZE * 2);
        assert_eq!(block_size(3), FIRST_BLOCK_SIZE * 4);
        // `block_start(i) == block_size(i)` for i >= 1 (the buddy layout), and
        // each block ends exactly where the next begins.
        for i in 1..(MAX_BLOCKS - 1) {
            assert_eq!(block_start(i), block_size(i), "start==size for block {i}");
            assert_eq!(
                block_start(i) + block_size(i),
                block_start(i + 1),
                "block {i} ends where block {} begins",
                i + 1,
            );
        }
    }

    #[test]
    fn locate_round_trips_block_boundaries() {
        // Every block start decodes to (that block, offset 0), and the last
        // byte of each block decodes to (that block, size-1).
        for i in 0..(MAX_BLOCKS - 1) {
            let start = block_start(i);
            // Block 0 start is the UNSET sentinel (offset 0); probe offset 1.
            let probe = if i == 0 { 1 } else { start };
            let (b, w) = locate(probe);
            assert_eq!(b, i, "start of block {i} locates to block {i}");
            assert_eq!(w, probe - start, "within offset at block {i} start");

            let last = start + block_size(i) - 1;
            let (lb, lw) = locate(last);
            assert_eq!(lb, i, "last byte of block {i} locates to block {i}");
            assert_eq!(
                lw,
                block_size(i) - 1,
                "within offset of block {i} last byte"
            );
        }
    }

    #[test]
    fn small_first_alloc_touches_only_block_zero() {
        // A handful of tiny allocations must stay in block 0 — only that one
        // small chunk is materialized (the whole point of geometric growth:
        // a small memtable never zeroes a giant block).
        let arena = Arena::new();
        for _ in 0..100 {
            let off = arena.alloc(32, 4).expect("ok");
            assert_eq!(locate(off).0, 0, "tiny allocs stay in block 0");
        }
        assert!(
            !arena.blocks[0].load(Ordering::Acquire).is_null(),
            "block 0 allocated",
        );
        assert!(
            arena.blocks[1].load(Ordering::Acquire).is_null(),
            "block 1 must NOT be allocated for a small memtable",
        );
    }

    #[test]
    fn large_alloc_jumps_to_a_big_enough_block() {
        // An allocation larger than the early small blocks must skip ahead to
        // the first block big enough to hold it, and read/write correctly.
        let arena = Arena::new();
        let size = FIRST_BLOCK_SIZE * 3; // bigger than blocks 0,1 (64K) and 2 (128K)
        let off = arena.alloc(size, 1).expect("large alloc");
        let (block, within) = locate(off);
        assert!(block_size(block) >= size, "landed in a big-enough block");
        assert_eq!(within, 0, "outsized alloc starts at the block base");
        // Write the whole region and read it back to prove the mapping is sound.
        unsafe {
            let bytes = arena.get_bytes_mut(off, size);
            bytes[0] = 0x11;
            bytes[size as usize - 1] = 0x22;
        }
        let read = unsafe { arena.get_bytes(off, size) };
        assert_eq!(read[0], 0x11);
        assert_eq!(read[size as usize - 1], 0x22);
    }

    #[test]
    fn many_allocs_span_blocks_and_round_trip() {
        // Fill across several geometric blocks with distinct payloads, then
        // verify every allocation reads back its own bytes (no aliasing across
        // the variable-size block boundaries).
        let arena = Arena::new();
        let mut offs = Vec::new();
        for i in 0u32..5_000 {
            let off = arena.alloc(40, 4).expect("ok");
            unsafe {
                let b = arena.get_bytes_mut(off, 4);
                b.copy_from_slice(&i.to_le_bytes());
            }
            offs.push((off, i));
        }
        // Touched more than one block.
        let max_block = offs.iter().map(|&(o, _)| locate(o).0).max().unwrap();
        assert!(max_block >= 1, "5000 x 40B must span past block 0");
        for (off, i) in offs {
            let read = unsafe { arena.get_bytes(off, 4) };
            assert_eq!(read, &i.to_le_bytes(), "alloc must read back its payload");
        }
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
        assert!(arena.alloc(MAX_ALLOC, 1).is_none()); // size == MAX_ALLOC
        assert!(arena.alloc(MAX_ALLOC + 1, 1).is_none()); // size > MAX_ALLOC
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
        // Allocate across 2 blocks to exercise Drop on both (per-index layout).
        let big = FIRST_BLOCK_SIZE - 8;
        let _ = arena.alloc(big, 1).expect("block 0");
        let _ = arena.alloc(64, 4).expect("block 1");
        // Drop runs here — deallocates both blocks.
    }

    /// An allocation that fills a block EXACTLY to its size must not corrupt
    /// data: the exact-fill allocation succeeds in that block, the cursor lands
    /// precisely on the next block's start, the following allocation advances to
    /// it, and the prior block's bytes are preserved. (The global-offset
    /// encoding has no carry/wrap hazard at the boundary; this guards the
    /// `end <= bsize` boundary arithmetic.)
    #[test]
    fn exact_block_fill_does_not_corrupt() {
        let arena = Arena::new();

        // Jump the cursor directly to block 1, offset 0 — avoids materializing
        // block 0 just to advance past it.
        arena.cursor.store(block_start(1), Ordering::Relaxed);

        // Bring block 1's cursor to (size - 4).
        let filler = block_size(1) - 4;
        let f = arena.alloc(filler, 1).expect("filler");
        assert_eq!(locate(f).0, 1, "filler should be in block 1");

        // Sentinel in the last allocated byte.
        // SAFETY: `f` was just returned by alloc(filler, 1).
        unsafe {
            let bytes = arena.get_bytes_mut(f, filler);
            bytes[filler as usize - 1] = 0xAB;
        }

        // Cursor is now at (size - 4) in block 1. Allocate exactly 4 bytes
        // (align=4): end == block_size(1) exactly. It fits block 1 (`end <=
        // bsize`), and the cursor lands on block_start(2).
        let boundary = arena.alloc(4, 4).expect("boundary alloc");
        assert_eq!(locate(boundary).0, 1, "exact-fill alloc stays in block 1");
        assert_eq!(
            arena.cursor.load(Ordering::Relaxed),
            block_start(2),
            "cursor lands exactly on the next block's start",
        );

        // The next allocation advances to block 2 (no wrap back into block 1).
        let next = arena.alloc(8, 4).expect("next alloc");
        assert_eq!(
            locate(next).0,
            2,
            "subsequent allocation advances to block 2"
        );

        // The sentinel byte in block 1 must be intact.
        // SAFETY: `f` is a valid allocation from above.
        let read_sentinel = unsafe { arena.get_bytes(f, filler) };
        assert_eq!(
            read_sentinel[filler as usize - 1],
            0xAB,
            "block 1 data must not be corrupted by subsequent allocations"
        );
    }
}
