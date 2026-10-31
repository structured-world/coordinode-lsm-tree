// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Keys rebuilt from a prefix and a suffix, written side by side into shared
//! chunks, so a scan of a whole table takes one allocation per chunk rather
//! than one per key.

use super::Slice;

#[cfg(not(feature = "bytes_1"))]
mod chunks {
    use super::Slice;
    use crate::byteview::{ByteView, INLINE_SIZE};
    use core::ptr::NonNull;

    /// Bytes of one chunk: about a block's keys, so a key that outlives its
    /// neighbours keeps as much alive as a key sliced out of a block does.
    const CHUNK_BYTES: u32 = 4_096;

    /// The chunk keys are being written into.
    ///
    /// Holds one reference to its chunk; every view of it holds another, so a
    /// chunk lives until its last key is dropped. The bytes up to `used` are
    /// written once and never again, and only they are ever viewed.
    pub(crate) struct KeyChunks {
        chunk: Option<NonNull<u8>>,
        used: u32,
    }

    // SAFETY: the pointer is this value's own reference to its chunk. The only
    // writes go to bytes past `used`, which no view reaches, through `&mut
    // self`; the reference count is atomic.
    unsafe impl Send for KeyChunks {}

    impl KeyChunks {
        pub(crate) fn new() -> Self {
            Self {
                chunk: None,
                used: 0,
            }
        }

        /// The key made of `left` then `right`.
        pub(crate) fn fuse(&mut self, left: &[u8], right: &[u8]) -> Slice {
            let total = left.len() + right.len();
            if total <= INLINE_SIZE || total > CHUNK_BYTES as usize {
                return Slice::fused(left, right);
            }
            #[cfg(feature = "diagnostics")]
            {
                crate::diagnostics::add(&crate::diagnostics::CHUNK_KEYS, 1);
                crate::diagnostics::add(&crate::diagnostics::KEY_BYTES, total);
            }
            // Below `CHUNK_BYTES`, checked above.
            #[expect(clippy::cast_possible_truncation, reason = "total <= CHUNK_BYTES")]
            let len = total as u32;
            let base = self.chunk_with_room(len);
            let at = self.used;
            // SAFETY: `chunk_with_room` left `len` bytes of the chunk free past
            // `at`. No view reaches them, so the writes alias nothing, and
            // `left` and `right` are not part of them. They are written whole
            // and `used` moves past them, so they stay as written, which is
            // what `view_of_shared` asks. This value holds a reference.
            let view = unsafe {
                let target = base.as_ptr().add(at as usize);
                core::ptr::copy_nonoverlapping(left.as_ptr(), target, left.len());
                core::ptr::copy_nonoverlapping(right.as_ptr(), target.add(left.len()), right.len());
                ByteView::view_of_shared(base.as_ptr(), at, len, CHUNK_BYTES)
            };
            self.used = at + len;
            Slice::from_view(view)
        }

        /// The current chunk, replaced when `len` bytes no longer fit in it.
        fn chunk_with_room(&mut self, len: u32) -> NonNull<u8> {
            if let Some(chunk) = self.chunk
                && self.used + len <= CHUNK_BYTES
            {
                return chunk;
            }
            self.release();
            let raw = ByteView::alloc_shared_heap(CHUNK_BYTES);
            #[expect(clippy::expect_used, reason = "allocation failure aborts instead")]
            let chunk = NonNull::new(raw).expect("an allocated region");
            self.chunk = Some(chunk);
            self.used = 0;
            chunk
        }

        fn release(&mut self) {
            if let Some(chunk) = self.chunk.take() {
                // SAFETY: the chunk came from `alloc_shared_heap` of this size,
                // and this value's reference is the one given up.
                unsafe { ByteView::shared_heap_release(chunk.as_ptr(), CHUNK_BYTES) };
            }
        }
    }

    impl Drop for KeyChunks {
        fn drop(&mut self) {
            self.release();
        }
    }
}

#[cfg(not(feature = "bytes_1"))]
pub(crate) use chunks::KeyChunks;

/// The `bytes` slice backend cannot wrap a foreign view, so its keys take an
/// allocation each.
#[cfg(feature = "bytes_1")]
pub(crate) struct KeyChunks;

#[cfg(feature = "bytes_1")]
impl KeyChunks {
    pub(crate) fn new() -> Self {
        Self
    }

    /// The key made of `left` then `right`.
    #[expect(
        clippy::unused_self,
        clippy::needless_pass_by_ref_mut,
        reason = "the signature the chunk backend needs"
    )]
    pub(crate) fn fuse(&mut self, left: &[u8], right: &[u8]) -> Slice {
        Slice::fused(left, right)
    }
}

#[cfg(test)]
#[expect(clippy::indexing_slicing, reason = "test code")]
#[cfg_attr(
    feature = "bytes_1",
    expect(clippy::drop_non_drop, reason = "this backend's writer holds nothing")
)]
mod tests;
