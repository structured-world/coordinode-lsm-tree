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
    /// a copied page keeps the encoding it was written with, so only a level
    /// whose pages the output's level would encode the same way qualifies.
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
        let dest_encoding = config.column_encoding_policy.get(dest_level);
        let levels = (0..version.iter_levels().count())
            .map(|level| config.column_encoding_policy.get(level) == dest_encoding)
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

/// Where the rows the merge emitted go: written one by one, or a whole group
/// copied in their place.
pub trait CarrySink {
    /// Writes one row.
    fn write(&mut self, row: InternalValue) -> crate::Result<()>;

    /// Copies `candidate`'s group in place of its rows, returning `false`
    /// when this output cannot take it as it is.
    fn carry(&mut self, candidate: &CarryCandidate) -> crate::Result<bool>;
}

/// What a compaction copied whole.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Carried {
    /// Row groups copied.
    pub(crate) groups: u64,
    /// Rows those groups hold.
    pub(crate) rows: u64,
    /// Their bytes on disk.
    pub(crate) bytes: u64,
}

/// Watches the rows a merge emits for runs that are a candidate's whole group.
pub struct CarryMatcher {
    queue: CarryQueue,
    comparator: crate::comparator::SharedComparator,
    /// The candidate whose rows the merge is emitting, with how many it has.
    open: Option<(CarryCandidate, usize)>,
    /// A candidate the merge emitted whole, held until a row of another key
    /// shows that its last key does not go on past it.
    complete: Option<CarryCandidate>,
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
            complete: None,
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
        if let Some(complete) = self.complete.take() {
            let continues = complete.rows.last().is_some_and(|last| {
                crate::comparator::same_user_key(&last.key.user_key, &row.key.user_key)
            });
            if continues {
                Self::write_rows(&complete.rows, sink)?;
            } else {
                self.carry(&complete, sink)?;
            }
        }

        if let Some((candidate, matched)) = self.open.take() {
            if candidate
                .rows
                .get(matched)
                .is_some_and(|next| same_row(next, &row))
            {
                self.pass(candidate, matched + 1, &row);
                return Ok(());
            }
            Self::write_rows(candidate.rows.get(..matched).unwrap_or_default(), sink)?;
        }

        // A group starts a key: one whose first key continues the key passed
        // on last would split that key's versions across the copy's edge.
        let continues_last = self
            .last_key
            .as_ref()
            .is_some_and(|last| crate::comparator::same_user_key(last, &row.key.user_key));
        if !continues_last && let Some(candidate) = self.take_starting_with(&row) {
            self.pass(candidate, 1, &row);
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
        if let Some(complete) = self.complete.take() {
            self.carry(&complete, sink)?;
        }
        if let Some((candidate, matched)) = self.open.take() {
            Self::write_rows(candidate.rows.get(..matched).unwrap_or_default(), sink)?;
        }
        Ok(self.carried)
    }

    /// Records that `candidate`'s first `matched` rows came out of the merge,
    /// `row` the last of them.
    fn pass(&mut self, candidate: CarryCandidate, matched: usize, row: &InternalValue) {
        self.last_key = Some(row.key.user_key.clone());
        if matched == candidate.rows.len() {
            self.complete = Some(candidate);
        } else {
            self.open = Some((candidate, matched));
        }
    }

    /// The candidate whose first row is `row`, taken from the queue, after
    /// dropping every candidate the merge has moved past: rows come out in
    /// key order, so one whose first key sorts below `row`'s can no longer
    /// start.
    fn take_starting_with(&self, row: &InternalValue) -> Option<CarryCandidate> {
        let mut queue = self.queue.lock();
        let mut found = None;
        queue.retain(|candidate| {
            let Some(first) = candidate.rows.first() else {
                return false;
            };
            if found.is_none() && same_row(first, row) {
                found = Some(CarryCandidate {
                    table: candidate.table.clone(),
                    group: candidate.group,
                    rows: alloc::sync::Arc::clone(&candidate.rows),
                });
                return false;
            }
            self.comparator
                .compare(&first.key.user_key, &row.key.user_key)
                != core::cmp::Ordering::Less
        });
        found
    }

    /// Copies `candidate`'s group, or writes its rows when the sink refuses it.
    fn carry(&mut self, candidate: &CarryCandidate, sink: &mut dyn CarrySink) -> crate::Result<()> {
        if sink.carry(candidate)? {
            self.carried.groups += 1;
            self.carried.rows += candidate.rows.len() as u64;
            self.carried.bytes += u64::from(candidate.group.size());
            Ok(())
        } else {
            Self::write_rows(&candidate.rows, sink)
        }
    }

    /// Writes `rows` one by one: rows the merge emitted that are not copied.
    fn write_rows(rows: &[InternalValue], sink: &mut dyn CarrySink) -> crate::Result<()> {
        rows.iter().try_for_each(|row| sink.write(row.clone()))
    }
}

/// Whether the merge emitted `row` exactly as `read`: the same key, version,
/// kind and value. A row the merge rewrote, re-seqnoed or replaced with
/// another input's version differs in one of them.
fn same_row(read: &InternalValue, row: &InternalValue) -> bool {
    read.key.seqno == row.key.seqno
        && read.key.value_type == row.key.value_type
        && crate::comparator::same_user_key(&read.key.user_key, &row.key.user_key)
        && *read.value == *row.value
}
