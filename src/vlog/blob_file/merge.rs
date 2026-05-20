// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use super::scanner::Scanner as BlobFileScanner;
use crate::vlog::{BlobFileId, blob_file::scanner::ScanEntry};
use alloc::collections::BinaryHeap;
use alloc::vec::Vec;
use core::cmp::Reverse;

type IteratorIndex = usize;

#[derive(Debug)]
struct IteratorValue {
    index: IteratorIndex,
    scan_entry: ScanEntry,
    blob_file_id: BlobFileId,
}

// PartialEq / Eq are derived from Ord so they STAY consistent: the
// `Ord` contract requires `a == b` ⇔ `a.cmp(b) == Equal`. Defining
// Eq on just `key` (the previous impl) violated that, because two
// entries with the same key + different seqno would test equal but
// `cmp` would order them. BinaryHeap doesn't call Eq today, but
// breaking the contract is a latent footgun the moment any caller
// puts these in a HashSet, BTreeSet, dedup() pass, etc.
impl PartialEq for IteratorValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == core::cmp::Ordering::Equal
    }
}
impl Eq for IteratorValue {}

impl PartialOrd for IteratorValue {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IteratorValue {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        (&self.scan_entry.key, Reverse(&self.scan_entry.seqno))
            .cmp(&(&other.scan_entry.key, Reverse(&other.scan_entry.seqno)))
    }
}

/// Interleaves multiple blob file readers into a single, sorted stream.
///
/// Uses `BinaryHeap<Reverse<_>>` for a min-heap. The merger only needs
/// pop-min + push semantics — the previous `IntervalHeap` (double-ended)
/// was overkill, and its `compare` transitive dep doesn't declare
/// `#![no_std]`, blocking the crate's no-std-check job.
pub struct MergeScanner {
    readers: Vec<BlobFileScanner>,
    heap: BinaryHeap<Reverse<IteratorValue>>,
}

impl MergeScanner {
    /// Initializes a new merging reader
    pub fn new(readers: Vec<BlobFileScanner>) -> Self {
        let heap = BinaryHeap::with_capacity(readers.len());
        Self { readers, heap }
    }

    fn advance_reader(&mut self, idx: usize) -> crate::Result<()> {
        #[expect(clippy::indexing_slicing, reason = "we trust the caller")]
        let reader = &mut self.readers[idx];

        if let Some(value) = reader.next() {
            let scan_entry = value?;
            let blob_file_id = reader.blob_file_id;

            self.heap.push(Reverse(IteratorValue {
                index: idx,
                blob_file_id,
                scan_entry,
            }));
        }

        Ok(())
    }

    fn push_next(&mut self) -> crate::Result<()> {
        for idx in 0..self.readers.len() {
            self.advance_reader(idx)?;
        }

        Ok(())
    }
}

impl Iterator for MergeScanner {
    type Item = crate::Result<(ScanEntry, BlobFileId)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            fail_iter!(self.push_next());
        }

        if let Some(Reverse(head)) = self.heap.pop() {
            fail_iter!(self.advance_reader(head.index));
            return Some(Ok((head.scan_entry, head.blob_file_id)));
        }

        None
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::super::scanner::Scanner;
    use super::*;
    use crate::{Slice, fs::StdFs, vlog::blob_file::writer::Writer as BlobFileWriter};
    use tempfile::tempdir;
    use test_log::test;

    #[test]
    fn blob_file_merger_seqno() -> crate::Result<()> {
        let dir = tempdir()?;

        let blob_file_path = dir.path().join("0");
        {
            {
                let mut writer = BlobFileWriter::new(&blob_file_path, 0, 0, &StdFs)?;

                writer.write(b"a", 1, &b"1".repeat(100))?;
                writer.write(b"a", 0, &b"0".repeat(100))?;

                writer.finish()?;
            }
        }

        {
            let mut merger = MergeScanner::new(vec![Scanner::new(&blob_file_path, 0)?]);

            assert_eq!(
                (Slice::from(b"a"), Slice::from(b"1".repeat(100))),
                merger
                    .next()
                    .map(|result| result.map(|(entry, _)| (entry.key, entry.value)))
                    .unwrap()?,
            );
            assert_eq!(
                (Slice::from(b"a"), Slice::from(b"0".repeat(100))),
                merger
                    .next()
                    .map(|result| result.map(|(entry, _)| (entry.key, entry.value)))
                    .unwrap()?,
            );

            assert!(merger.next().is_none());
        }

        Ok(())
    }

    #[test]
    fn blob_file_merger() -> crate::Result<()> {
        let dir = tempdir()?;

        let blob_file_0_path = dir.path().join("0");
        let blob_file_1_path = dir.path().join("1");

        {
            let keys = [b"a", b"c", b"e"];

            {
                let mut writer = BlobFileWriter::new(&blob_file_0_path, 0, 0, &StdFs)?;

                for key in keys {
                    writer.write(key, 0, &key.repeat(100))?;
                }

                writer.finish()?;
            }
        }

        {
            let keys = [b"b", b"d"];

            {
                let mut writer = BlobFileWriter::new(&blob_file_1_path, 1, 0, &StdFs)?;

                for key in keys {
                    writer.write(key, 1, &key.repeat(100))?;
                }

                writer.finish()?;
            }
        }

        {
            let mut merger = MergeScanner::new(vec![
                Scanner::new(&blob_file_0_path, 0)?,
                Scanner::new(&blob_file_1_path, 1)?,
            ]);

            let merged_keys = [b"a", b"b", b"c", b"d", b"e"];

            for key in merged_keys {
                assert_eq!(
                    (Slice::from(key), Slice::from(key.repeat(100))),
                    merger
                        .next()
                        .map(|result| result.map(|(entry, _)| (entry.key, entry.value)))
                        .unwrap()?,
                );
            }

            assert!(merger.next().is_none());
        }

        Ok(())
    }
}
