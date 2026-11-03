// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

/// With the `mimalloc` feature, heap memory comes from mimalloc: it claims a
/// fresh allocation as its own, small and large alike.
#[test]
fn the_mimalloc_feature_makes_mimalloc_the_global_allocator() {
    for len in [64usize, 4 << 20] {
        let buf = alloc::vec![0u8; len];
        // SAFETY: mi_is_in_heap_region only compares the address with the
        // regions mimalloc manages; it does not read through the pointer.
        #[expect(unsafe_code, reason = "see safety")]
        let ours = unsafe { libmimalloc_sys::mi_is_in_heap_region(buf.as_ptr().cast()) };
        assert!(ours, "a {len}-byte allocation comes from mimalloc");
    }
}
