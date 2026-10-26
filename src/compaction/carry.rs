// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Copies a compaction input's row groups or data blocks into the output
//! whole where the merge left them as they were.
//!
//! The merge decides every row as it always does; this only watches what it
//! emits. A group's or block's rows that come out exactly as its scanner read
//! them, all of them, with no other row among them and none of their keys
//! continued on either side, are what the output would encode for that key
//! range, so its bytes on disk are written in their place. Whether it may be
//! carried is therefore proved by the merge's own output rather than
//! predicted from metadata: a row the merge drops, rewrites, re-seqnos or
//! interleaves sends its rows down the ordinary path.

use crate::table::group_carry::{CarryCandidate, CarryQueue, CarryShape, CarryTarget};
use crate::{InternalValue, UserKey};

/// Where the inputs of a compaction into the level whose policies are those
/// of canonical level `dest_canonical`, writing under `rc`, record their row
/// groups, when the output is columnar, or their data blocks, when it is
/// row-major; `None` when none of `inputs` could be carried. The output must
/// be neither encrypted nor ECC-protected, since a copy keeps the plain
/// transform of the tables that may be carried.
pub fn plan(
    config: &crate::Config,
    rc: &crate::runtime_config::RuntimeConfig,
    dest_canonical: usize,
    inputs: &[crate::Table],
) -> Option<CarryTarget> {
    let blocks = CarryShape::Blocks {
        restart_interval: config
            .data_block_restart_interval_policy
            .get(dest_canonical),
        hash_ratio_bits: config
            .data_block_hash_ratio_policy
            .get(dest_canonical)
            .to_bits(),
    };
    #[cfg(feature = "columnar")]
    let shape = if rc.columnar {
        CarryShape::Groups(crate::table::meta::GroupShape {
            encoding: config.column_encoding_policy.get(dest_canonical),
            row_group_size: config.columnar_row_group_size_policy.get(dest_canonical),
            page_size: config.columnar_page_size_policy.get(dest_canonical),
        })
    } else {
        blocks
    };
    #[cfg(not(feature = "columnar"))]
    let shape = blocks;
    let target = CarryTarget {
        queue: CarryQueue::default(),
        codec: rc.data_block_compression_policy.get(dest_canonical),
        shape,
    };
    (config.encryption.is_none()
        && !config.page_ecc
        && inputs.iter().any(|table| target.takes(table)))
    .then_some(target)
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

    /// The count of the compaction filter's transformations so far.
    fn transforms_seen(&self) -> u64;

    /// Takes `seen`, read by [`Self::transforms_seen`] when the row last put
    /// in the output was emitted, as the count the output's window closes at:
    /// a row written or copied after being held must not take the
    /// transformations of the keys the merge passed meanwhile, which belong
    /// to the window of the output holding the next row.
    fn settle_transforms(&mut self, seen: u64);
}

/// What a compaction copied instead of encoding.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Carried {
    /// Row groups copied whole.
    pub(crate) groups: u64,
    /// Data blocks copied whole.
    pub(crate) blocks: u64,
    /// Rows those groups and blocks hold.
    pub(crate) rows: u64,
    /// Groups rebuilt around some of their pages, copied.
    pub(crate) partial_groups: u64,
    /// The bytes on disk copied, whole groups, blocks and pages together.
    pub(crate) bytes: u64,
}

impl core::ops::AddAssign for Carried {
    fn add_assign(&mut self, other: Self) {
        // Each count is bounded by what one compaction's outputs hold.
        self.groups += other.groups;
        self.blocks += other.blocks;
        self.rows += other.rows;
        self.partial_groups += other.partial_groups;
        self.bytes += other.bytes;
    }
}

/// The filter's transformation count when each held row came out: one count
/// while they all agree, as they do unless a filter transforms rows among
/// them, so holding a run of rows records no list.
enum SeenCounts {
    /// `rows` rows, all at `count`.
    Same { count: u64, rows: usize },
    /// Each row's own.
    Each(alloc::vec::Vec<u64>),
}

impl SeenCounts {
    fn push(&mut self, count: u64) {
        match self {
            Self::Same { count: same, rows } if *same == count => *rows += 1,
            Self::Same { count: same, rows } => {
                let mut each = alloc::vec![*same; *rows];
                each.push(count);
                *self = Self::Each(each);
            }
            Self::Each(each) => each.push(count),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Same { rows, .. } => *rows,
            Self::Each(each) => each.len(),
        }
    }

    /// The count of the `at`th row, `at` below [`Self::len`].
    fn get(&self, at: usize) -> u64 {
        match self {
            Self::Same { count, .. } => *count,
            Self::Each(each) => each.get(at).copied().unwrap_or_default(),
        }
    }
}

/// A candidate whose key range the merge is emitting, and what it emitted:
/// never nothing, since it opens on the first row of the range.
struct Open {
    candidate: CarryCandidate,
    /// What a group's range emitted. A data block's stays empty: it is held
    /// only while its rows come out as read, so what was emitted is the first
    /// [`Self::held`] of its own rows, and nothing is copied to keep it.
    emitted: alloc::vec::Vec<InternalValue>,
    seen: SeenCounts,
    /// The count when the last row held came out.
    last_seen: u64,
    /// The bytes of `emitted`'s keys and values.
    held_bytes: u64,
}

impl Open {
    fn new(candidate: CarryCandidate, row: InternalValue, sink: &dyn CarrySink) -> Self {
        let last_seen = sink.transforms_seen();
        let mut open = Self {
            candidate,
            held_bytes: 0,
            emitted: alloc::vec::Vec::new(),
            seen: SeenCounts::Same {
                count: last_seen,
                rows: 0,
            },
            last_seen,
        };
        open.hold(row, sink);
        open
    }

    fn hold(&mut self, row: InternalValue, sink: &dyn CarrySink) {
        self.last_seen = sink.transforms_seen();
        self.seen.push(self.last_seen);
        if !self.candidate.is_block() {
            self.held_bytes += crate::table::group_carry::row_bytes(&row);
            self.emitted.push(row);
            #[cfg(all(test, feature = "columnar"))]
            tests::note_held(&self.emitted);
        }
    }

    /// How many rows are held.
    fn held(&self) -> usize {
        self.seen.len()
    }

    /// The key of the last row held.
    fn last_held_key(&self) -> Option<&UserKey> {
        if self.candidate.is_block() {
            self.held()
                .checked_sub(1)
                .and_then(|last| self.candidate.rows.get(last))
        } else {
            self.emitted.last()
        }
        .map(|row| &row.key.user_key)
    }

    /// Whether the merge has filled the range far past the group's rows, by
    /// their count or their bytes: it is no group of the source's shape any
    /// more, and holding it on would hold the merge. A data block never is,
    /// since it is held only while it matches.
    fn overfilled(&self) -> bool {
        self.emitted.len() > 2 * self.candidate.rows.len()
            || self.held_bytes > 2 * self.candidate.bytes()
    }
}

/// Watches the rows a merge emits for the key ranges of the candidates.
pub struct CarryMatcher {
    queue: CarryQueue,
    comparator: crate::comparator::SharedComparator,
    /// The candidate whose key range the merge is emitting.
    open: Option<Open>,
    /// The key of the last row passed on, written or held.
    last_key: Option<UserKey>,
    /// What the last search of the queue that found nothing left: the count of
    /// candidates recorded then, and the smallest first key among the
    /// candidates still queued. Until another is recorded, a row below that key
    /// has no candidate, and the queue is not searched for it.
    none_below: Option<(usize, Option<UserKey>)>,
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
            none_below: None,
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
                // A data block is copied whole or not at all, so the first row
                // that is not its next one as read sends the range out as rows
                // at once, rather than holding the rest of it to find out.
                if open.candidate.is_block()
                    && !open
                        .candidate
                        .rows
                        .get(open.held())
                        .is_some_and(|read| same_row(read, &row))
                {
                    Self::write_rows(open, sink)?;
                    self.last_key = Some(row.key.user_key.clone());
                    return sink.write(row);
                }
                open.hold(row, sink);
                // An overfilled range goes out as rows, which bounds what is
                // held to about two groups and one row.
                if open.overfilled() {
                    self.last_key = open.last_held_key().cloned();
                    Self::write_rows(open, sink)?;
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
        if !continues_last
            && let Some(candidate) = self.take_covering(&row)
            // A data block whose first row is not this one can no longer come
            // out whole, and is dropped.
            && (!candidate.is_block() || candidate.rows.first().is_some_and(|read| same_row(read, &row)))
        {
            self.open = Some(Open::new(candidate, row, sink));
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
        let candidate = &open.candidate;
        let emitted = &open.emitted;
        self.last_key = open.last_held_key().cloned();
        // A data block was held only while each row came out as read.
        let unchanged = if candidate.is_block() {
            open.held() == candidate.rows.len()
        } else {
            emitted.len() == candidate.rows.len()
                && emitted
                    .iter()
                    .zip(candidate.rows.iter())
                    .all(|(row, read)| same_row(read, row))
        };
        if unchanged {
            if sink.carry(candidate)? {
                match candidate.unit {
                    #[cfg(feature = "columnar")]
                    crate::table::group_carry::CarryUnit::RowGroup(_) => self.carried.groups += 1,
                    crate::table::group_carry::CarryUnit::DataBlock(_) => self.carried.blocks += 1,
                }
                self.carried.rows += candidate.rows.len() as u64;
                self.carried.bytes += u64::from(candidate.group.size());
                sink.settle_transforms(open.last_seen);
                return Ok(());
            }
        } else if !candidate.is_block()
            && shares_a_row(&candidate.rows, emitted, &self.comparator)
            && let Some(bytes) = sink.carry_pages(candidate, emitted)?
        {
            self.carried.partial_groups += 1;
            self.carried.bytes += bytes;
            sink.settle_transforms(open.last_seen);
            return Ok(());
        }
        Self::write_rows(open, sink)
    }

    /// The candidate whose key range holds `row`'s key, taken from the queue,
    /// after dropping every candidate the merge has moved past: rows come out
    /// in key order, so one whose last key sorts below `row`'s can no longer
    /// be reached.
    fn take_covering(&mut self, row: &InternalValue) -> Option<CarryCandidate> {
        use core::cmp::Ordering::{Greater, Less};

        let key = &row.key.user_key;
        let recorded = self.queue.recorded();
        if let Some((seen, first)) = &self.none_below
            && *seen == recorded
            && first
                .as_ref()
                .is_none_or(|first| self.comparator.compare(key, first) == Less)
        {
            return None;
        }
        let mut queue = self.queue.lock();
        queue.retain(|candidate| self.comparator.compare(candidate.last_key(), key) != Less);
        if let Some(at) = queue
            .iter()
            .position(|candidate| self.comparator.compare(candidate.first_key(), key) != Greater)
        {
            self.none_below = None;
            return queue.remove(at);
        }
        // Every candidate left starts past the key; the smallest start bounds
        // the rows that may find none until another is recorded.
        let first = queue
            .iter()
            .map(CarryCandidate::first_key)
            .min_by(|a, b| self.comparator.compare(a, b))
            .cloned();
        self.none_below = Some((recorded, first));
        None
    }

    /// Writes the rows `open` held one by one, each closing the output's
    /// window at the count seen when it came out: rows the merge emitted
    /// that are not copied.
    fn write_rows(open: Open, sink: &mut dyn CarrySink) -> crate::Result<()> {
        if open.candidate.is_block() {
            // The rows emitted are the block's own as read, byte for byte.
            for (at, row) in open.candidate.rows.iter().take(open.held()).enumerate() {
                sink.write(row.clone())?;
                sink.settle_transforms(open.seen.get(at));
            }
            return Ok(());
        }
        for (at, row) in open.emitted.into_iter().enumerate() {
            sink.write(row)?;
            sink.settle_transforms(open.seen.get(at));
        }
        Ok(())
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
#[cfg(feature = "columnar")]
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

/// Whether every row page of `emitted` cut as `row_pages` says, that is not
/// `copied`, holds a single row or rows of at most `limit` bytes: a page past
/// it is one the writer would have split, and a group rebuilt with it could
/// grow a page past what one block may hold.
#[cfg(feature = "columnar")]
pub fn changed_pages_fit(
    emitted: &[InternalValue],
    (row_pages, copied): (&[u32], &[bool]),
    limit: u64,
) -> bool {
    let mut rest = emitted;
    for (&rows, &copied) in row_pages.iter().zip(copied) {
        let Some((page, after)) = rest.split_at_checked(rows as usize) else {
            return false;
        };
        if !copied
            && page.len() > 1
            && page
                .iter()
                .map(crate::table::group_carry::row_bytes)
                .sum::<u64>()
                > limit
        {
            return false;
        }
        rest = after;
    }
    true
}

#[cfg(all(test, feature = "columnar"))]
#[expect(clippy::indexing_slicing, reason = "test code")]
mod tests;

#[cfg(all(test, feature = "metrics"))]
mod block_tests;

/// Whether some row of `source` came out in `emitted` exactly as it was read.
/// A copied page needs its rows so, and both lists are in the merge's order,
/// key ascending and version descending, so one pass over them decides it
/// before the group's bytes are read: a merge that changed every row, as one
/// writing seqnos as zero does, copies no page.
fn shares_a_row(
    source: &[InternalValue],
    emitted: &[InternalValue],
    comparator: &crate::comparator::SharedComparator,
) -> bool {
    use core::cmp::Ordering::{Equal, Greater, Less};

    let (mut source, mut emitted) = (source.iter().peekable(), emitted.iter().peekable());
    while let (Some(read), Some(row)) = (source.peek(), emitted.peek()) {
        match comparator
            .compare(&read.key.user_key, &row.key.user_key)
            .then_with(|| row.key.seqno.cmp(&read.key.seqno))
        {
            Less => {
                source.next();
            }
            Greater => {
                emitted.next();
            }
            Equal => {
                if same_row(read, row) {
                    return true;
                }
                source.next();
                emitted.next();
            }
        }
    }
    false
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
