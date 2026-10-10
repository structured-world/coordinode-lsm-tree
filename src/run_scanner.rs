// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

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
    /// Told how long each table's reads take, for every table of the run.
    pace: Option<crate::table::util::Pacer>,
    /// Where each table's scanner records the row groups it reads, for a
    /// compaction that copies groups whole.
    #[cfg(feature = "columnar")]
    carry: Option<crate::table::group_carry::CarryTarget>,
}

impl RunScanner {
    /// Scans tables `lo..=hi` of `run`, telling `pace` how long the reads of
    /// every one of them take.
    pub fn culled(
        run: Arc<Run<Table>>,
        (lo, hi): (Option<usize>, Option<usize>),
        pace: Option<crate::table::util::Pacer>,
    ) -> crate::Result<Self> {
        let lo = lo.unwrap_or_default();
        let hi = hi.unwrap_or(run.len() - 1);

        #[expect(
            clippy::expect_used,
            reason = "we trust the caller to pass valid indexes"
        )]
        let lo_table = run.get(lo).expect("should exist");

        let lo_reader = lo_table.scan_paced(pace.as_ref())?;

        Ok(Self {
            tables: run,
            lo,
            hi,
            lo_reader: Some(lo_reader),
            pace,
            #[cfg(feature = "columnar")]
            carry: None,
        })
    }

    /// Has the scanner of every table of the run `target` takes record its
    /// groups there (see [`crate::table::group_carry::CarryTarget::takes`]).
    #[cfg(feature = "columnar")]
    #[must_use]
    pub(crate) fn with_carry(mut self, target: crate::table::group_carry::CarryTarget) -> Self {
        // Called on a scanner just built: its first reader is open and its
        // table is in the run.
        #[expect(
            clippy::expect_used,
            reason = "culled opened the reader of table lo, which the run holds"
        )]
        let table = self.tables.get(self.lo).expect("lo is within the run");
        self.lo_reader = self
            .lo_reader
            .take()
            .map(|reader| carrying(reader, table, &target));
        self.carry = Some(target);
        self
    }

    fn scan_table(&self, index: usize) -> crate::Result<Scanner> {
        // `index` is within `lo..=hi`, every slot of which names a table: a
        // missing one fails the scan rather than ending it early, which would
        // hand a merge the run without its tail.
        let Some(table) = self.tables.get(index) else {
            return Err(crate::Error::from(crate::io::Error::other(
                "run scanner: table index past the run",
            )));
        };
        let scanner = table.scan_paced(self.pace.as_ref())?;
        #[cfg(feature = "columnar")]
        if let Some(target) = &self.carry {
            return Ok(carrying(scanner, table, target));
        }
        Ok(scanner)
    }
}

/// `scanner` of `table`, recording its groups in `target` when it takes them.
#[cfg(feature = "columnar")]
fn carrying(
    scanner: Scanner,
    table: &Table,
    target: &crate::table::group_carry::CarryTarget,
) -> Scanner {
    if target.takes(table) {
        scanner.with_carry(target.carry_for(table))
    } else {
        scanner
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
                    self.lo_reader = Some(fail_iter!(self.scan_table(self.lo)));
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
