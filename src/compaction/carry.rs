// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Copies a compaction input's row groups into the output whole where the
//! merge left them as they were.
//!
//! The merge decides every row as it always does; this only watches what it
//! emits. A group's rows that come out exactly as its scanner read them, all
//! of them, with no other row among them and none of their keys continued on
//! either side, are what the output would encode for that key range, so the
//! group's bytes on disk are written in their place. Whether a group may be
//! carried is therefore proved by the merge's own output rather than
//! predicted from metadata: a row the merge drops, rewrites, re-seqnos or
//! interleaves sends the group's rows down the ordinary path.

use crate::table::group_carry::{CarryCandidate, CarryQueue};
use crate::{InternalValue, UserKey};

/// Which inputs of one compaction record their row groups, and where.
pub struct CarryInputs {
    /// Where the inputs' scanners record their groups.
    pub(crate) queue: CarryQueue,
    /// Per level, whether its tables' groups may be carried into the output:
    /// a copied group keeps the encodings and the row group and page sizes
    /// it was written with, so only a level whose groups the output's level
    /// would write the same way qualifies.
    levels: alloc::vec::Vec<bool>,
}

impl CarryInputs {
    /// The plan for a compaction of `inputs` from `version` into the level
    /// whose policies are those of `dest_level`, writing under `rc`; `None`
    /// when nothing it reads could be carried. Its output must be columnar
    /// and neither encrypted nor ECC-protected, since a copied group's blocks
    /// keep the plain transform of the tables that may be carried.
    pub(crate) fn plan(
        version: &crate::version::Version,
        config: &crate::Config,
        rc: &crate::runtime_config::RuntimeConfig,
        dest_level: usize,
        inputs: &[crate::Table],
    ) -> Option<Self> {
        if !rc.columnar
            || config.encryption.is_some()
            || config.page_ecc
            || !inputs.iter().any(crate::Table::carries_row_groups)
        {
            return None;
        }
        // A copied group keeps its row group and page sizes as well as its
        // encodings, so only a level that cuts them as the output's does
        // writes groups the output's level would have written.
        let shape = |level: usize| {
            (
                config.column_encoding_policy.get(level),
                config.columnar_row_group_size_policy.get(level),
                config.columnar_page_size_policy.get(level),
            )
        };
        let dest_shape = shape(dest_level);
        let levels = (0..version.iter_levels().count())
            .map(|level| shape(level) == dest_shape)
            .collect();
        Some(Self {
            queue: CarryQueue::default(),
            levels,
        })
    }

    /// The queue the tables of `level` record their groups in, or `None` when
    /// they may not be carried.
    pub(crate) fn queue_for(&self, level: usize) -> Option<&CarryQueue> {
        self.levels
            .get(level)
            .copied()
            .unwrap_or(false)
            .then_some(&self.queue)
    }
}

/// Where the rows the merge emitted go: written one by one, a whole group
/// copied in their place, or a group rebuilt around the pages it copies.
pub trait CarrySink {
    /// Writes one row.
    fn write(&mut self, row: InternalValue) -> crate::Result<()>;

    /// Copies `candidate`'s group in place of its rows, returning `false`
    /// when this output cannot take it as it is.
    fn carry(&mut self, candidate: &CarryCandidate) -> crate::Result<bool>;

    /// Writes `emitted`, what the merge made of `candidate`'s key range, as
    /// one group that copies the candidate's pages on the row pages it left
    /// as they were, returning the bytes of the pages copied, or `None`,
    /// having written nothing, when it cannot.
    fn carry_pages(
        &mut self,
        candidate: &CarryCandidate,
        emitted: &[InternalValue],
    ) -> crate::Result<Option<u64>>;
}

/// What a compaction copied instead of encoding.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Carried {
    /// Row groups copied whole.
    pub(crate) groups: u64,
    /// Rows those groups hold.
    pub(crate) rows: u64,
    /// Groups rebuilt around some of their pages, copied.
    pub(crate) partial_groups: u64,
    /// The bytes on disk copied, whole groups and pages together.
    pub(crate) bytes: u64,
}

/// A candidate whose key range the merge is emitting, and what it emitted.
struct Open {
    candidate: CarryCandidate,
    emitted: alloc::vec::Vec<InternalValue>,
}

/// Watches the rows a merge emits for the key ranges of the candidates.
pub struct CarryMatcher {
    queue: CarryQueue,
    comparator: crate::comparator::SharedComparator,
    /// The candidate whose key range the merge is emitting.
    open: Option<Open>,
    /// The key of the last row passed on, written or held.
    last_key: Option<UserKey>,
    carried: Carried,
}

impl CarryMatcher {
    /// A matcher taking its candidates from `queue`.
    pub(crate) fn new(queue: CarryQueue, comparator: crate::comparator::SharedComparator) -> Self {
        Self {
            queue,
            comparator,
            open: None,
            last_key: None,
            carried: Carried::default(),
        }
    }

    /// Passes on `row`, the next row the merge emitted.
    ///
    /// # Errors
    ///
    /// Any error of the sink.
    pub(crate) fn write(
        &mut self,
        row: InternalValue,
        sink: &mut dyn CarrySink,
    ) -> crate::Result<()> {
        if let Some(mut open) = self.open.take() {
            let within = self
                .comparator
                .compare(&row.key.user_key, open.candidate.last_key())
                != core::cmp::Ordering::Greater;
            if within {
                open.emitted.push(row);
                // A range the merge filled far past the group's rows is no
                // group of the source's shape any more; it goes out as rows,
                // which bounds what is held to about two groups.
                if open.emitted.len() > 2 * open.candidate.rows.len() {
                    self.last_key = open.emitted.last().map(|r| r.key.user_key.clone());
                    Self::write_rows(&open.emitted, sink)?;
                } else {
                    self.open = Some(open);
                }
                return Ok(());
            }
            self.resolve(open, sink)?;
        }

        // A group starts a key: one whose range opens on the key passed on
        // last would split that key's versions across the copy's edge.
        let continues_last = self
            .last_key
            .as_ref()
            .is_some_and(|last| crate::comparator::same_user_key(last, &row.key.user_key));
        if !continues_last && let Some(candidate) = self.take_covering(&row) {
            self.open = Some(Open {
                candidate,
                emitted: alloc::vec![row],
            });
            return Ok(());
        }

        self.last_key = Some(row.key.user_key.clone());
        sink.write(row)
    }

    /// Passes on what is still held once the merge has emitted its last row.
    ///
    /// # Errors
    ///
    /// Any error of the sink.
    pub(crate) fn finish(mut self, sink: &mut dyn CarrySink) -> crate::Result<Carried> {
        if let Some(open) = self.open.take() {
            self.resolve(open, sink)?;
        }
        Ok(self.carried)
    }

    /// Writes what the merge emitted over `open`'s key range: the group as it
    /// lies when that is exactly its rows, else a group copying the pages of
    /// the row pages it left as they were, else the rows.
    fn resolve(&mut self, open: Open, sink: &mut dyn CarrySink) -> crate::Result<()> {
        let Open { candidate, emitted } = open;
        self.last_key = emitted.last().map(|row| row.key.user_key.clone());
        let unchanged = emitted.len() == candidate.rows.len()
            && emitted
                .iter()
                .zip(candidate.rows.iter())
                .all(|(row, read)| same_row(read, row));
        if unchanged {
            if sink.carry(&candidate)? {
                self.carried.groups += 1;
                self.carried.rows += candidate.rows.len() as u64;
                self.carried.bytes += u64::from(candidate.group.size());
                return Ok(());
            }
        } else if let Some(bytes) = sink.carry_pages(&candidate, &emitted)? {
            self.carried.partial_groups += 1;
            self.carried.bytes += bytes;
            return Ok(());
        }
        Self::write_rows(&emitted, sink)
    }

    /// The candidate whose key range holds `row`'s key, taken from the queue,
    /// after dropping every candidate the merge has moved past: rows come out
    /// in key order, so one whose last key sorts below `row`'s can no longer
    /// be reached.
    fn take_covering(&self, row: &InternalValue) -> Option<CarryCandidate> {
        use core::cmp::Ordering::{Greater, Less};

        let key = &row.key.user_key;
        let mut queue = self.queue.lock();
        queue.retain(|candidate| self.comparator.compare(candidate.last_key(), key) != Less);
        let at = queue
            .iter()
            .position(|candidate| self.comparator.compare(candidate.first_key(), key) != Greater)?;
        queue.remove(at)
    }

    /// Writes `rows` one by one: rows the merge emitted that are not copied.
    fn write_rows(rows: &[InternalValue], sink: &mut dyn CarrySink) -> crate::Result<()> {
        rows.iter().try_for_each(|row| sink.write(row.clone()))
    }
}

/// How `emitted`, what the merge made of a group's key range, is cut into
/// row pages that keep the group's: the rows of each, one row page per source
/// row page, and which of them are the source's own, exactly its rows, so
/// their pages can be copied. `None` when no row page is the source's, or the
/// rows cannot be cut into as many non-empty row pages as the source has.
///
/// `source` are the group's rows and `source_pages` the rows of each of its
/// row pages. A row page whose rows come out unchanged and in place is kept;
/// the rows between two kept ones, or after the last, are spread over the row
/// pages between them, so every row page keeps its ordinal, which a copied
/// page's stamp names.
pub fn align_row_pages(
    source: &[InternalValue],
    source_pages: &[u32],
    emitted: &[InternalValue],
) -> Option<(alloc::vec::Vec<u32>, alloc::vec::Vec<bool>)> {
    // The source rows of each row page.
    let mut spans = alloc::vec::Vec::with_capacity(source_pages.len());
    let mut start = 0usize;
    for &rows in source_pages {
        let end = start + rows as usize;
        spans.push(source.get(start..end)?);
        start = end;
    }
    let matches_at = |span: &[InternalValue], at: usize| {
        emitted
            .get(at..at + span.len())
            .is_some_and(|rows| rows.iter().zip(span).all(|(row, read)| same_row(read, row)))
    };

    let mut rows = alloc::vec::Vec::with_capacity(spans.len());
    let mut copied = alloc::vec::Vec::with_capacity(spans.len());
    let mut at = 0usize;
    let mut page = 0usize;
    while let Some(span) = spans.get(page) {
        if matches_at(span, at) {
            rows.push(u32::try_from(span.len()).ok()?);
            copied.push(true);
            at += span.len();
            page += 1;
            continue;
        }
        // A run of changed row pages ends where a later one is found in
        // place, or at the end of what was emitted.
        let (next_page, next_at) = (page + 1..spans.len())
            .find_map(|later| {
                let later_span = spans.get(later)?;
                (at..=emitted.len().checked_sub(later_span.len())?)
                    .find(|&from| matches_at(later_span, from))
                    .map(|from| (later, from))
            })
            .unwrap_or((spans.len(), emitted.len()));
        let pages = next_page - page;
        let available = next_at - at;
        if available < pages {
            return None;
        }
        // Spread as evenly as the counts allow, every row page non-empty.
        for share in 0..pages {
            let take = available / pages + usize::from(share < available % pages);
            rows.push(u32::try_from(take).ok()?);
            copied.push(false);
        }
        at = next_at;
        page = next_page;
    }
    // Rows left past the last row page, later versions of its last key or
    // keys a kept last row page did not hold, join it.
    if at < emitted.len() {
        let extra = u32::try_from(emitted.len() - at).ok()?;
        let last = rows.last_mut()?;
        *last = last.checked_add(extra)?;
        *copied.last_mut()? = false;
    }
    copied.contains(&true).then_some((rows, copied))
}

#[cfg(test)]
#[expect(clippy::indexing_slicing, reason = "test code")]
mod tests;

/// Whether the merge emitted `row` exactly as `read`: the same key, version,
/// kind and value. A row the merge rewrote, re-seqnoed or replaced with
/// another input's version differs in one of them.
fn same_row(read: &InternalValue, row: &InternalValue) -> bool {
    read.key.seqno == row.key.seqno
        && read.key.value_type == row.key.value_type
        && crate::comparator::same_user_key(&read.key.user_key, &row.key.user_key)
        && *read.value == *row.value
}
