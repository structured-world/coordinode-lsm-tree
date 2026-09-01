// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

//! Lock-free segmented value storage for the memtable skiplist.
//!
//! Values are stored in fixed-size segments (64 K entries each), allocated
//! lazily via `AtomicPtr` CAS.  Reads are wait-free (one atomic load +
//! pointer dereference), writes are lock-free (atomic `fetch_add` on the
//! index counter + CAS for new segment allocation).
//!
//! This replaces `Mutex<Vec<UserValue>>` which serialised all value accesses
//! and caused 15-27% throughput regression under concurrent reads.

use crate::value::UserValue;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

use core::ptr;
use core::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

/// Number of entries per segment.  2^16 = 65 536.
const SEGMENT_SHIFT: u32 = 16;

/// Entries per segment.
const SEGMENT_SIZE: usize = 1 << SEGMENT_SHIFT;

/// Bitmask for within-segment offset.
#[expect(
    clippy::cast_possible_truncation,
    reason = "SEGMENT_SIZE = 65536, fits in u32"
)]
const SEGMENT_MASK: u32 = SEGMENT_SIZE as u32 - 1;

/// Maximum segments.  With 64 K entries/segment this supports ~4 billion entries.
const MAX_SEGMENTS: usize = 1 << (32 - SEGMENT_SHIFT); // 65 536

/// A lock-free append-only store for [`UserValue`] entries.
///
/// Entries are addressed by a u32 index returned from [`append`](Self::append).
/// Reads via [`get`](Self::get) are wait-free.  The store never shrinks —
/// it is dropped in bulk when the memtable is dropped.
pub struct ValueStore {
    /// Segment pointers.  Null = not yet allocated.  Once set, never modified.
    segments: Box<[AtomicPtr<UserValue>]>,

    /// Next index to allocate (monotonically increasing).
    next_idx: AtomicU32,
}

// Send+Sync derived automatically: all fields (Box<[AtomicPtr<_>]>, AtomicU32)
// are Send+Sync.

impl ValueStore {
    /// Creates a new empty store.
    ///
    /// Allocates a fixed-size segment-pointer array (~512 KiB on 64-bit).
    /// This is acceptable: one array per memtable, and memtables are few.
    pub fn new() -> Self {
        // Vec optimizes the repeated-null pattern into a single memset.
        // Using Box::new_zeroed_slice would be cleaner but requires nightly.
        let mut segments = Vec::with_capacity(MAX_SEGMENTS);
        for _ in 0..MAX_SEGMENTS {
            segments.push(AtomicPtr::new(ptr::null_mut()));
        }

        Self {
            segments: segments.into_boxed_slice(),
            next_idx: AtomicU32::new(0),
        }
    }

    /// Appends a value and returns its index.
    ///
    /// The value MOVES into its slot: the caller's handle becomes the stored
    /// one, so appending costs no refcount traffic.
    ///
    /// # Panics
    ///
    /// Panics when the index space is exhausted (`u32::MAX` reservations),
    /// rather than wrapping and re-issuing a slot that live nodes still
    /// reference. Unreachable in practice: the arena backing the nodes these
    /// values belong to addresses 2^32 bytes and a node costs at least 28 of
    /// them, so it reports exhaustion roughly 28x earlier.
    #[expect(
        clippy::indexing_slicing,
        reason = "seg_idx < MAX_SEGMENTS enforced by u32 index range"
    )]
    pub fn append(&self, value: UserValue) -> u32 {
        // One atomic RMW rather than a CAS loop guarding `u32::MAX`: the
        // counter cannot get there. Every value belongs to one skiplist node,
        // the node is allocated BEFORE this call and occupies at least 28
        // arena bytes, and arena offsets are `u32` (2^32 bytes total), so a
        // memtable holds fewer than 2^32 / 28 entries; the arena panics on
        // exhaustion roughly 28x before this index space could wrap.
        //
        // The assert is nonetheless a real one, not a `debug_assert`: the
        // bound above is an invariant spanning two modules, and if a future
        // change breaks it the failure mode is silent data corruption (a
        // wrapped index re-issues slot 0, `ptr::write` overwrites a value
        // live skiplist nodes still point at, and a concurrent reader of
        // that slot races the write). Failing loudly costs one compare
        // against a constant on a perfectly predicted branch, touches no
        // memory, and keeps the single-RMW reservation; a CAS loop would
        // charge the hot path for a path that cannot be taken. The last
        // index is spent on the guard rather than handed out, so the wrap
        // is refused before any slot can be re-issued.
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed);
        assert!(idx != u32::MAX, "ValueStore::append: index space exhausted");
        let seg_idx = (idx >> SEGMENT_SHIFT) as usize;
        let slot = (idx & SEGMENT_MASK) as usize;

        self.ensure_segment(seg_idx);

        // SAFETY: ensure_segment guarantees the segment is allocated.
        // The atomic fetch_add guarantees `slot` is unique: no two threads
        // write the same slot.  We write before publishing the node (via the
        // skiplist CAS), so readers see the value only after it's fully
        // written. The value MOVES into its slot: the caller's handle is the
        // stored one, so the insert path does no refcount traffic.
        unsafe {
            let seg_ptr = self.segments[seg_idx].load(Ordering::Acquire);
            debug_assert!(!seg_ptr.is_null());
            ptr::write(seg_ptr.add(slot), value);
        }

        idx
    }

    /// Test-only: positions the reservation counter so a test can reach the
    /// end of the index space without performing 4 billion appends.
    #[cfg(test)]
    pub(crate) fn set_next_idx_for_test(&self, idx: u32) {
        self.next_idx.store(idx, Ordering::Relaxed);
    }

    /// Reads a value by index (wait-free).
    ///
    /// # Safety
    ///
    /// `idx` must have been returned by a prior [`append`](Self::append) call,
    /// and the caller must establish happens-before (typically via the skiplist
    /// CAS chain) to ensure the value at `idx` has been fully written.
    #[expect(
        clippy::indexing_slicing,
        reason = "seg_idx < MAX_SEGMENTS enforced by u32 index range"
    )]
    pub unsafe fn get(&self, idx: u32) -> UserValue {
        let seg_idx = (idx >> SEGMENT_SHIFT) as usize;
        let slot = (idx & SEGMENT_MASK) as usize;

        // SAFETY: the caller guarantees happens-before via the skiplist CAS.
        // The value at `idx` was fully written during `append()`.  Acquire
        // pairs with the AcqRel CAS in ensure_segment.
        unsafe {
            let seg_ptr = self.segments[seg_idx].load(Ordering::Acquire);
            debug_assert!(!seg_ptr.is_null());
            (*seg_ptr.add(slot)).clone()
        }
    }

    /// Ensures the segment at `seg_idx` is allocated.
    #[expect(
        clippy::indexing_slicing,
        reason = "seg_idx < MAX_SEGMENTS enforced by caller"
    )]
    fn ensure_segment(&self, seg_idx: usize) {
        if self.segments[seg_idx].load(Ordering::Acquire).is_null() {
            // Allocate a segment of uninitialised UserValue slots.
            // We use alloc_zeroed for the raw memory — the slots will be
            // initialised one-by-one via ptr::write in append().
            #[expect(
                clippy::expect_used,
                reason = "Layout::array with compile-time-known size cannot fail"
            )]
            let layout =
                alloc::alloc::Layout::array::<UserValue>(SEGMENT_SIZE).expect("segment layout");

            // SAFETY: layout is non-zero (SEGMENT_SIZE > 0, UserValue is non-ZST).
            // The cast to *mut UserValue is safe because alloc_zeroed returns
            // memory with alignment >= align_of::<UserValue>() (Layout::array
            // sets alignment to align_of::<UserValue>()).
            #[expect(
                clippy::cast_ptr_alignment,
                reason = "Layout::array ensures correct alignment"
            )]
            let raw = unsafe { alloc::alloc::alloc_zeroed(layout) }.cast::<UserValue>();
            if raw.is_null() {
                alloc::alloc::handle_alloc_error(layout);
            }

            // CAS null → raw.  Loser frees its allocation.
            if self.segments[seg_idx]
                .compare_exchange(ptr::null_mut(), raw, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                // SAFETY: raw was just allocated with the same layout; no
                // slots were initialised (we lost the race before any append).
                unsafe {
                    alloc::alloc::dealloc(raw.cast::<u8>(), layout);
                }
            }
        }
    }
}

impl Default for ValueStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ValueStore {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "seg_idx < MAX_SEGMENTS (65536), fits in u32"
    )]
    fn drop(&mut self) {
        let total = self.next_idx.load(Ordering::Relaxed);
        if total == 0 {
            return;
        }

        // Only iterate segments that could contain initialised entries.
        let max_seg_idx = ((total - 1) >> SEGMENT_SHIFT) as usize + 1;

        for seg_idx in 0..max_seg_idx {
            #[expect(
                clippy::indexing_slicing,
                reason = "seg_idx < max_seg_idx <= MAX_SEGMENTS"
            )]
            let seg_ptr = self.segments[seg_idx].load(Ordering::Relaxed);

            if seg_ptr.is_null() {
                continue;
            }

            // Drop initialised slots in this segment.
            let seg_start = (seg_idx as u32) << SEGMENT_SHIFT;
            // `.min(total)` is the real bound (segment end clamped to the live
            // count); the saturating add just guards the intermediate u32 sum
            // before that clamp.
            let seg_end = seg_start.saturating_add(SEGMENT_SIZE as u32).min(total);

            if seg_start < total {
                let count = (seg_end - seg_start) as usize;
                for i in 0..count {
                    // SAFETY: slots 0..count were initialised via ptr::write
                    // in append().  We're the only thread running (Drop is &mut).
                    unsafe {
                        ptr::drop_in_place(seg_ptr.add(i));
                    }
                }
            }

            // Deallocate the segment.
            #[expect(
                clippy::expect_used,
                reason = "Layout::array with compile-time-known size cannot fail"
            )]
            let layout =
                alloc::alloc::Layout::array::<UserValue>(SEGMENT_SIZE).expect("segment layout");
            // SAFETY: `seg_ptr` came from `alloc_zeroed(layout)` in
            // `ensure_segment()`, all initialised entries were dropped above,
            // and `Drop` has exclusive access — so this frees that allocation
            // exactly once with the original layout.
            unsafe {
                alloc::alloc::dealloc(seg_ptr.cast::<u8>(), layout);
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    reason = "tests use expect/unwrap and narrow casts for brevity"
)]
mod tests;
