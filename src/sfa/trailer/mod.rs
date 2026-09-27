// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod reader;
pub mod writer;

/// Bytes the trailer takes: its magic, two flag bytes, the table-of-contents
/// checksum, position and length.
pub const TRAILER_LEN: usize = writer::TRAILER_MAGIC.len() + 1 + 1 + 16 + 8 + 8;
