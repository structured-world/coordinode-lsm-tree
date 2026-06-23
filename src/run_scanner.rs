// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use alloc::sync::Arc;

use crate::{InternalValue, Table, table::Scanner, version::Run};

/// Scans through a disjoint run
///
/// Optimized for compaction, by using a `TableScanner` instead of `TableReader`.
pub struct RunScanner {
    tables: Arc<Run<Table>>,
    lo: usize,
    hi: usize,
    lo_reader: Option<Scanner>,
}

impl RunScanner {
    pub fn culled(
        run: Arc<Run<Table>>,
        (lo, hi): (Option<usize>, Option<usize>),
    ) -> crate::Result<Self> {
        let lo = lo.unwrap_or_default();
        let hi = hi.unwrap_or(run.len() - 1);

        #[expect(
            clippy::expect_used,
            reason = "we trust the caller to pass valid indexes"
        )]
        let lo_table = run.get(lo).expect("should exist");

        let lo_reader = lo_table.scan()?;

        Ok(Self {
            tables: run,
            lo,
            hi,
            lo_reader: Some(lo_reader),
        })
    }
}

impl Iterator for RunScanner {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(lo_reader) = &mut self.lo_reader {
                if let Some(item) = lo_reader.next() {
                    return Some(item);
                }

                // NOTE: Lo reader is empty, get next one
                self.lo_reader = None;
                self.lo += 1;

                if self.lo <= self.hi {
                    #[expect(
                        clippy::expect_used,
                        reason = "hi is at most equal to the last slot; so because 0 <= lo <= hi, it must be a valid index"
                    )]
                    let scanner =
                        fail_iter!(self.tables.get(self.lo).expect("should exist").scan());

                    self.lo_reader = Some(scanner);
                }
            } else {
                return None;
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests;
