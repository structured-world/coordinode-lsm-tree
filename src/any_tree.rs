// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::{BlobTree, Tree};
use enum_dispatch::enum_dispatch;

/// May be a standard [`Tree`] or a [`BlobTree`]
#[derive(Clone)]
#[enum_dispatch(AbstractTree)]
pub enum AnyTree {
    /// Standard LSM-tree, see [`Tree`]
    Standard(Tree),

    /// Key-value separated LSM-tree, see [`BlobTree`]
    Blob(BlobTree),
}

impl crate::abstract_tree::sealed::Sealed for AnyTree {}

impl AnyTree {
    /// Starts a multi-get of `keys` at `seqno` that its caller drives: see
    /// [`Tree::start_multi_get`].
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::SnapshotBelowRetention`] when `seqno` is below
    /// what the tree retains.
    pub fn start_multi_get<K: Into<crate::UserKey>>(
        &self,
        keys: impl IntoIterator<Item = K>,
        seqno: crate::SeqNo,
    ) -> crate::Result<crate::resumable::Step> {
        match self {
            Self::Standard(tree) => tree.start_multi_get(keys, seqno),
            Self::Blob(tree) => tree.start_multi_get(keys, seqno),
        }
    }
}
