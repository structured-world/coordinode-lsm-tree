// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Heap-allocated byte buffer with caller-specified alignment.
//!
//! `AlignedBuf` exists for the `O_DIRECT` I/O path: Linux requires
//! both the file offset and the userspace buffer to be aligned to
//! the filesystem's logical block size (typically 512 B on legacy
//! disks, 4 KiB on Advanced Format SSDs). A `Vec<u8>` is aligned
//! to `align_of::<u8>() = 1`, so an unaligned write to an
//! `O_DIRECT` file errors with `EINVAL`.
//!
//! This wrapper exists exclusively for the `O_DIRECT` pairing
//! (#133 Phase 2). Normal cached I/O has no alignment requirement
//! and should keep using `Vec<u8>` / `BytesMut` — using
//! `AlignedBuf` there would waste the extra alignment slack with
//! no benefit.

use core::alloc::Layout;
use core::ptr::NonNull;
use core::slice;

/// A heap-allocated byte buffer aligned to a caller-specified
/// boundary.
///
/// Used for the `O_DIRECT` I/O path where kernel alignment
/// requirements (typically 4 KiB) exceed `Vec<u8>`'s default
/// `align_of::<u8>() = 1`.
///
/// # Invariants
///
/// - `ptr` is always non-null. When `capacity > 0`, it points to a
///   region of at least `capacity` bytes allocated via the global
///   allocator with `Layout::from_size_align(capacity, alignment)`.
///   When `capacity == 0`, it is a non-dereferenceable dangling
///   sentinel synthesised from the requested alignment (see
///   `new_zeroed` for the special-case path) — `len == 0` always
///   holds in that case, so the sentinel is never dereferenced.
/// - `len <= capacity`.
/// - `alignment` is a power of two ≥ 1 and ≤ `isize::MAX as usize`
///   (enforced at construction).
/// - `capacity` is an integer multiple of `alignment` (rounded up
///   at construction from the caller's requested size). The
///   multiplier itself is NOT required to be a power of two — e.g.
///   `new_zeroed(9000, 4096)` yields `capacity = 12288 = 3 × 4096`.
///
/// # `Send` + `Sync`
///
/// The raw pointer doesn't carry any cross-thread state; the
/// buffer's bytes are owned, immobile until `Drop`, and only
/// reachable via `&self` / `&mut self`. So `Send` + `Sync` are
/// both safe.
pub struct AlignedBuf {
    /// Non-null pointer to the start of the aligned allocation.
    ptr: NonNull<u8>,
    /// Number of bytes currently written (`<= capacity`).
    len: usize,
    /// Number of bytes allocated.
    capacity: usize,
    /// Alignment boundary the allocation satisfies (power of two).
    alignment: usize,
}

// SAFETY: AlignedBuf owns its allocation; the raw pointer doesn't
// alias anything else and is only reachable through &self / &mut
// self. Sending the buffer to another thread is sound; concurrent
// shared access through &self is sound (the bytes are immutable
// behind a shared reference).
#[expect(
    unsafe_code,
    reason = "raw-pointer wrapper; Send/Sync soundness justified"
)]
unsafe impl Send for AlignedBuf {}
#[expect(
    unsafe_code,
    reason = "raw-pointer wrapper; Send/Sync soundness justified"
)]
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    /// Allocates a zero-initialised buffer of `capacity` bytes
    /// aligned to `alignment`. `capacity` is rounded up to the
    /// next multiple of `alignment` so the trailing slack is
    /// large enough for aligned writes that consume the whole
    /// buffer.
    ///
    /// # Errors
    ///
    /// Returns `None` if:
    /// - `alignment` is not a power of two, OR
    /// - `alignment > isize::MAX as usize`, OR
    /// - the rounded-up capacity overflows `isize::MAX as usize`, OR
    /// - the global allocator fails (returns null).
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::fs::AlignedBuf;
    /// let buf = AlignedBuf::new_zeroed(8192, 4096).unwrap();
    /// assert_eq!(buf.capacity(), 8192);
    /// assert_eq!(buf.as_ptr().addr() % 4096, 0);
    /// ```
    #[must_use]
    pub fn new_zeroed(capacity: usize, alignment: usize) -> Option<Self> {
        if !alignment.is_power_of_two() {
            return None;
        }
        if alignment > (isize::MAX as usize) {
            return None;
        }
        // Round up so the trailing slack is large enough for an
        // aligned write that consumes the whole capacity.
        let rounded = capacity.checked_add(alignment - 1)? & !(alignment - 1);
        if rounded > (isize::MAX as usize) {
            return None;
        }
        // `alloc::alloc::alloc(layout)` requires `layout.size() > 0`
        // — calling it with a zero-size layout is UB per the trait
        // docs (Layout itself accepts size==0, but the allocator
        // call does not). Std handles this for `Vec<T>` etc. by
        // using `NonNull::dangling()` internally; we do the same
        // here but synthesise the sentinel from the caller's
        // requested alignment so `as_ptr().addr() % alignment == 0`
        // still holds for zero-capacity buffers. The sentinel is
        // never dereferenced — every read/write path is bounded by
        // `len`, which is 0 here.
        if rounded == 0 {
            // SAFETY: alignment is a power of two ≥ 1, so the
            // resulting pointer is non-null and properly aligned.
            // `without_provenance_mut` constructs an address-only
            // pointer (no provenance, no associated allocation) —
            // exactly right for a sentinel that must never be
            // dereferenced. We never deref past `len = 0`. Strict-
            // provenance-friendly: avoids the integer-to-pointer
            // cast lint by using the canonical exposed-address API.
            let dangling = {
                #[expect(unsafe_code, reason = "non-null dangling for 0-cap buffer")]
                unsafe {
                    NonNull::new_unchecked(core::ptr::without_provenance_mut::<u8>(alignment))
                }
            };
            return Some(Self {
                ptr: dangling,
                len: 0,
                capacity: 0,
                alignment,
            });
        }
        let layout = Layout::from_size_align(rounded, alignment).ok()?;
        // SAFETY: layout was just validated; alloc_zeroed is safe to
        // call for any valid non-zero layout. Returns null on OOM,
        // which we surface as None.
        #[expect(unsafe_code, reason = "global allocator call with validated layout")]
        let raw = unsafe { alloc::alloc::alloc_zeroed(layout) };
        let ptr = NonNull::new(raw)?;
        Some(Self {
            ptr,
            len: 0,
            capacity: rounded,
            alignment,
        })
    }

    /// Number of bytes currently written.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Buffer capacity in bytes (`>= len`, rounded up to a
    /// multiple of `alignment` at construction time).
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Alignment the allocation was constructed with (power of two).
    #[must_use]
    pub const fn alignment(&self) -> usize {
        self.alignment
    }

    /// `true` when `len == 0`.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Raw const pointer to the buffer's first byte. Stable across
    /// the lifetime of `self` (no reallocation). Valid for reads
    /// of `len` bytes.
    #[must_use]
    pub const fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr().cast_const()
    }

    /// Raw mut pointer to the buffer's first byte. Valid for
    /// writes of `capacity` bytes.
    #[must_use]
    pub const fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Shared slice over the currently-written `len` bytes.
    #[must_use]
    pub const fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` is valid for reads of `capacity >= len`
        // bytes by invariant; the lifetime is tied to `&self`.
        #[expect(unsafe_code, reason = "slice over owned aligned allocation")]
        unsafe {
            slice::from_raw_parts(self.ptr.as_ptr(), self.len)
        }
    }

    /// Mut slice over the FULL `capacity` — including bytes already
    /// in the `0..len` written region.
    ///
    /// Named `as_capacity_mut` (not `spare_capacity_mut`) because
    /// `spare_capacity` in `Vec` / `BytesMut` means the tail
    /// `len..capacity` only. This method intentionally exposes the
    /// entire allocation: `O_DIRECT` kernel reads need to overwrite
    /// already-buffered bytes when refilling a recycled buffer, so
    /// the right primitive is "full buffer", not "tail beyond len".
    ///
    /// Caller is responsible for updating `len` via
    /// [`Self::set_len`] after writing.
    #[must_use]
    pub const fn as_capacity_mut(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` is valid for writes of `capacity` bytes by
        // invariant; the lifetime is tied to `&mut self`.
        #[expect(unsafe_code, reason = "mut slice over owned aligned allocation")]
        unsafe {
            slice::from_raw_parts_mut(self.ptr.as_ptr(), self.capacity)
        }
    }

    /// Updates the written-bytes count.
    ///
    /// # Panics
    ///
    /// Panics if `new_len > capacity`.
    pub const fn set_len(&mut self, new_len: usize) {
        assert!(
            new_len <= self.capacity,
            "AlignedBuf::set_len exceeds capacity",
        );
        self.len = new_len;
    }

    /// Resets `len` to 0 without touching the allocation.
    pub const fn clear(&mut self) {
        self.len = 0;
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        if self.capacity == 0 {
            // Dangling sentinel from `new_zeroed(0, _)`; nothing to
            // free.
            return;
        }
        // SAFETY: layout reproduces the one used at allocation;
        // `ptr` was obtained from the global allocator with that
        // exact layout and hasn't been freed yet (Drop runs once).
        // The unwrap_or_else fast-paths the impossible case
        // (Layout was valid at construction; we never mutate
        // capacity / alignment after) without panicking — Drop
        // panics during unwinding would abort the process.
        let Ok(layout) = Layout::from_size_align(self.capacity, self.alignment) else {
            // Unreachable: invariants enforced at construction
            // guarantee Layout::from_size_align succeeds here.
            // Skipping dealloc leaks `capacity` bytes — preferable
            // to aborting if the invariant ever drifts.
            return;
        };
        #[expect(unsafe_code, reason = "matched dealloc for owned allocation")]
        unsafe {
            alloc::alloc::dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test assertions")]
mod tests;
