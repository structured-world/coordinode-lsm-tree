// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

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
