// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A value alone on its cache line.

use core::ops::Deref;

/// Holds a value every insert writes on a line of its own, so the fields every
/// insert only reads do not share it: a write invalidates the whole line in
/// every other core's cache, and a read-only neighbour would be fetched again
/// by each of them. 128 bytes is the line on Apple silicon and the pair of
/// lines x86 prefetches together.
#[repr(align(128))]
pub(crate) struct OwnLine<T>(pub(crate) T);

impl<T> Deref for OwnLine<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &T {
        &self.0
    }
}
