// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Where a tight-space slice ends, found as its merge runs.
//!
//! The merge hands the rows it keeps and the versions it drops to a
//! [`SliceGovernor`], which holds back the rows of the key they belong to
//! until the key ends, and then hands the key on when what the slice writes
//! with it stays within the slice's room. The first key past the room ends
//! the slice, and the next slice starts with it: a key never splits across
//! slices. Only the key being merged is held, so the rows reach the ledger
//! and the writer as the merge emits them, a key late: a carry plan copies
//! the input groups whose rows come out as read.
//!
//! The bound is not taken at every key. Each bound taken also proves a quota:
//! what the slice may add on top of what it holds and still fit, as a cost to
//! its tables and one to its blob files. An entry costs its tables at least
//! its key and value bytes, and [`ENTRY_COST`] for itself, for each filter
//! hash and for each object it owns; it costs the blob files the bytes it
//! relocates. So a quota of `q` holds at most `q` bytes of keys, `q` of
//! values and `q / ENTRY_COST` entries, hashes and owned objects, which is
//! the slice the quota is proven for.
//! A key that leaves the slice within its quota, with no key, value or seqno
//! past the widths the quota was proven for and nothing the bound counts once
//! per kind (a blob file it links, a field column, a key domain, a cut key, a
//! source dictionary) new, is handed on with a sum and a comparison. Only a
//! key past the quota takes a bound of its own, as every key does once the
//! room is nearly used.
//!
//! A dropped version is handed on in the merge's order with the keys around
//! it: the ownership ledger settles a key's blob objects from its dropped
//! versions, and those of a key past the end are the next slice's to count.

use super::{
    BLOB_META_BYTES, DomainEdge, Relocated, SliceCaps, SliceLoad, SliceMeasure, SliceOutputs,
    enter_domain, take_entry,
};
use crate::table::writer::{RewriteLoad, varint_len};
use crate::{InternalValue, UserKey};
use alloc::vec::Vec;
use core::cell::RefCell;

/// What an entry, a filter hash and an owned object each cost a slice's
/// tables at the least: a quota of `q` holds at most `q / ENTRY_COST` of
/// each. An entry smaller than this is counted as this large.
const ENTRY_COST: u64 = 64;

/// The most bytes the varints of an entry's key and value lengths take: a
/// key of up to `u16::MAX` bytes and a value of up to `u32::MAX`.
const ENTRY_LENGTHS: u64 = 2 * 3 + 5;

/// A cost to a slice's tables and one to its blob files.
#[derive(Clone, Copy, Default)]
struct Cost {
    tables: u64,
    blobs: u64,
}

/// What a slice's merge hands on, in its order.
pub(super) enum Event {
    /// A row the merge keeps, and whether it is known to start a key.
    Kept(InternalValue, bool),
    /// A version the merge drops.
    Dropped(InternalValue),
}

impl Event {
    fn key(&self) -> &UserKey {
        match self {
            Self::Kept(item, _) | Self::Dropped(item) => &item.key.user_key,
        }
    }
}

/// Collects the versions a slice's merge drops, for the governor to place
/// among its rows.
pub(super) struct DropQueue<'q>(pub &'q RefCell<Vec<InternalValue>>);

impl crate::compaction::stream::DroppedKvCallback for DropQueue<'_> {
    fn on_dropped(&mut self, kv: &InternalValue) {
        self.0.borrow_mut().push(kv.clone());
    }
}

/// The widths a quota was proven for: the longest key and value (`None`
/// for a value of any length) and the highest seqno a key may bring and
/// still be handed on without a bound.
struct Widths {
    key: u64,
    value: Option<u64>,
    seqno: u64,
}

/// Decides how far a tight-space slice's merge writes (see the module docs).
pub(super) struct SliceGovernor<'a> {
    caps: SliceCaps,
    outputs: &'a SliceOutputs<'a>,
    relocated: Option<&'a Relocated>,
    comparator: &'a crate::comparator::SharedComparator,
    split_cells: bool,
    hashes_read_keys: bool,
    /// What the keys handed on write.
    committed: SliceLoad,
    /// The cost the slice may add past the last bound taken and still fit,
    /// what it added since, and the widths the quota holds for.
    quota: Cost,
    spent: Cost,
    widths: Widths,
    /// The longest key seen.
    longest_key: u64,
    /// The key being merged, what its rows write, the cut keys it passes,
    /// and whether it brings what the quota does not cover.
    key: Option<UserKey>,
    load: SliceLoad,
    key_cuts: usize,
    past_quota: bool,
    edges: (DomainEdge, DomainEdge),
    /// What the merge handed over of the key being merged, with the versions
    /// dropped since the key before.
    held: Vec<Event>,
    /// What may be handed on now.
    ready: Vec<Event>,
    /// The key the slice ends before, once it stopped.
    end: Option<UserKey>,
}

impl<'a> SliceGovernor<'a> {
    /// A governor keeping a slice from `from` on within `caps`, its tables
    /// written as `outputs` bounds them and its values relocated as
    /// `relocated` says. The cut keys below `from` were the slices' before.
    pub(super) fn new(
        caps: SliceCaps,
        outputs: &'a SliceOutputs<'a>,
        relocated: Option<&'a Relocated>,
        comparator: &'a crate::comparator::SharedComparator,
        from: Option<&UserKey>,
    ) -> Self {
        let cmp = comparator.as_ref();
        let cuts = from.map_or(0, |from| {
            outputs
                .cut_keys
                .partition_point(|cut| cmp.compare(cut, from) == core::cmp::Ordering::Less)
        });
        Self {
            caps,
            outputs,
            relocated,
            comparator,
            split_cells: outputs.probe.splits_cells(),
            hashes_read_keys: outputs.probe.hashes_read_keys(),
            committed: SliceLoad::default(),
            // No bound taken yet: the first key takes one.
            quota: Cost::default(),
            spent: Cost::default(),
            widths: Widths {
                key: 0,
                value: Some(0),
                seqno: 0,
            },
            longest_key: 0,
            key: None,
            load: SliceLoad::default(),
            key_cuts: cuts,
            past_quota: false,
            edges: (DomainEdge::Unseen, DomainEdge::Unseen),
            held: Vec::new(),
            ready: Vec::new(),
            end: None,
        }
    }

    /// The key the slice ends before, when it stopped short of the merge's
    /// end.
    pub(super) fn end(&self) -> Option<&UserKey> {
        self.end.as_ref()
    }

    /// Whether the slice took no key: its room holds not even its first.
    pub(super) fn took_nothing(&self) -> bool {
        !self.any_committed()
    }

    /// Whether a key was handed on: every key holds a kept entry.
    fn any_committed(&self) -> bool {
        self.committed.rows.entries > 0
    }

    /// What may be handed on, in order.
    pub(super) fn take_ready(&mut self) -> alloc::vec::Drain<'_, Event> {
        self.ready.drain(..)
    }

    /// Takes `item`, a row the merge keeps, after the versions it dropped
    /// before it (`dropped`). `false` once the slice ends: the merge stops.
    ///
    /// # Errors
    ///
    /// Returns an error when a row's value does not decode.
    pub(super) fn offer(
        &mut self,
        item: InternalValue,
        starts_key: bool,
        dropped: &RefCell<Vec<InternalValue>>,
    ) -> crate::Result<bool> {
        self.hold_dropped(dropped);
        let new_key = self
            .key
            .as_ref()
            .is_none_or(|key| !crate::comparator::same_user_key(key, &item.key.user_key));
        if new_key {
            if self.key.is_some() && !self.settle(Some(&item.key.user_key)) {
                return Ok(false);
            }
            self.key = Some(item.key.user_key.clone());
        }
        self.take(&item, new_key)?;
        self.held.push(Event::Kept(item, starts_key));
        Ok(true)
    }

    /// Ends the merge: the key being merged is handed on if it fits.
    pub(super) fn finish(&mut self, dropped: &RefCell<Vec<InternalValue>>) {
        if self.end.is_some() {
            return;
        }
        self.hold_dropped(dropped);
        if self.key.is_some() {
            self.settle(None);
        } else {
            self.ready.append(&mut self.held);
        }
    }

    /// Places the versions dropped since among the held events.
    fn hold_dropped(&mut self, dropped: &RefCell<Vec<InternalValue>>) {
        self.held
            .extend(dropped.borrow_mut().drain(..).map(Event::Dropped));
    }

    /// Takes the kept row `item` into the key being merged: one more entry,
    /// and the cut keys and key domains it passes when it starts the key
    /// (`new_key`).
    fn take(&mut self, item: &InternalValue, new_key: bool) -> crate::Result<()> {
        let cmp = self.comparator.as_ref();
        let key = &item.key.user_key;
        let key_len = key.len() as u64;
        let value_len = item.value.len() as u64;
        self.longest_key = self.longest_key.max(key_len);
        if new_key {
            while self
                .outputs
                .cut_keys
                .get(self.key_cuts)
                .is_some_and(|cut| cmp.compare(cut, key) != core::cmp::Ordering::Greater)
            {
                self.load.cut_keys += 1;
                self.key_cuts += 1;
                self.past_quota = true;
            }
        }
        let hashes = if self.hashes_read_keys {
            self.outputs.probe.filter_hashes_bound(key)
        } else {
            1
        };
        let owned = self.load.rows.owned_blob_objects;
        let mut dictionaries = Vec::new();
        let (blobs, grew) = take_entry(
            &mut self.load,
            &self.committed,
            (item.key.value_type, item.key.seqno, key.len(), &item.value),
            hashes,
            self.split_cells,
            self.relocated,
            &mut dictionaries,
        )?;
        self.load.blobs += blobs;
        for source in dictionaries {
            if !self.committed.dictionaries.contains(&source)
                && !self.load.dictionaries.contains(&source)
            {
                self.load.add_dictionary(source, self.relocated);
                self.past_quota = true;
            }
        }
        if let Some(domains) = self.outputs.domains {
            let (table_end, blob_end) = &mut self.edges;
            let seen = (self.load.table_domains.len(), self.load.blob_domains.len());
            enter_domain(domains, cmp, table_end, &mut self.load.table_domains, key);
            if blobs > 0 {
                enter_domain(domains, cmp, blob_end, &mut self.load.blob_domains, key);
            }
            if (self.load.table_domains.len(), self.load.blob_domains.len()) != seen {
                self.past_quota = true;
            }
        }
        // Each sum is bounded by the inputs' bytes and objects.
        let owned = self.load.rows.owned_blob_objects - owned;
        self.spent.tables += (key_len + value_len)
            .max(ENTRY_COST)
            .max(ENTRY_COST * hashes)
            .max(ENTRY_COST * owned);
        self.spent.blobs += blobs;
        if grew
            || key_len > self.widths.key
            || self.widths.value.is_some_and(|width| value_len > width)
            || item.key.seqno > self.widths.seqno
        {
            self.past_quota = true;
        }
        Ok(())
    }

    /// Hands on the key being merged, with the held events below `upto` (all
    /// of them for `None`), when the slice with it fits, `true`; otherwise
    /// ends the slice before the key, `false`.
    fn settle(&mut self, upto: Option<&UserKey>) -> bool {
        if !self.past_quota
            && self.spent.tables <= self.quota.tables
            && self.spent.blobs <= self.quota.blobs
        {
            // Within the quota the key opened no set and passed no cut key.
            self.committed.rows.absorb(&self.load.rows);
            self.committed.blobs += self.load.blobs;
            self.load.rows = RewriteLoad::default();
            self.load.blobs = 0;
            self.hand_on_below(upto);
            return true;
        }
        let with = self.committed.measure_with(&self.load, self.relocated);
        if !self.fits(&with) {
            let end = self.key.take();
            if self.any_committed() {
                // The versions dropped below the key are this slice's.
                self.hand_on_below(end.as_ref());
            }
            self.held.clear();
            self.end = end;
            return false;
        }
        self.prove_quota(&with);
        let load = core::mem::take(&mut self.load);
        self.committed.absorb(&load, self.relocated);
        self.past_quota = false;
        self.hand_on_below(upto);
        true
    }

    /// Takes `now`, a slice that fits, as the last bound: the quota is what a
    /// slice past `now` adds and still fits, in each cost the larger of the
    /// quota before and twice the cost spent since, halved as often as needed
    /// on the side past its cap. A quota used up doubles while the room
    /// lasts, so the bounds taken grow as the logarithm of the keys, and it
    /// stays within twice what the slice spent between two bounds.
    fn prove_quota(&mut self, now: &SliceMeasure) {
        let widths = Widths {
            key: 2 * now.rows.longest_key,
            value: match now.rows.longest_value {
                Some(longest) => Some(2 * longest),
                None if now.rows.entries == 0 => Some(0),
                None => None,
            },
            seqno: same_width_seqno(now.rows.highest_seqno),
        };
        // Bounded by twice the cost of the keys taken, far below `u64::MAX`.
        // No quota past a cap fits: a cost of `q` adds `q` bytes to the side
        // it is charged to.
        let mut quota = Cost {
            tables: self
                .quota
                .tables
                .max(2 * self.spent.tables)
                .min(self.caps.tables.min(self.caps.total)),
            blobs: self
                .quota
                .blobs
                .max(2 * self.spent.blobs)
                .min(self.caps.blobs.min(self.caps.total)),
        };
        loop {
            let (tables, blobs) = self.bound(&past(now, quota, &widths));
            let over_tables = tables > self.caps.tables;
            let over_blobs = blobs > self.caps.blobs;
            if !over_tables && !over_blobs && tables + blobs <= self.caps.total {
                break;
            }
            // Past the shared cap alone, both sides give way.
            let both = !over_tables && !over_blobs;
            let halved = Cost {
                tables: if over_tables || both {
                    quota.tables / 2
                } else {
                    quota.tables
                },
                blobs: if over_blobs || both {
                    quota.blobs / 2
                } else {
                    quota.blobs
                },
            };
            if (halved.tables, halved.blobs) == (quota.tables, quota.blobs) {
                // Only the widths are past the room: every key takes a bound.
                quota = Cost::default();
                break;
            }
            quota = halved;
        }
        self.quota = quota;
        self.spent = Cost::default();
        self.widths = widths;
    }

    /// Moves the held events below `upto` (all for `None`) to `ready`; they
    /// lie in key order.
    fn hand_on_below(&mut self, upto: Option<&UserKey>) {
        let cmp = self.comparator.as_ref();
        let at = upto.map_or(self.held.len(), |upto| {
            self.held
                .iter()
                .position(|event| cmp.compare(event.key(), upto) != core::cmp::Ordering::Less)
                .unwrap_or(self.held.len())
        });
        self.ready.extend(self.held.drain(..at));
    }

    /// Whether a slice writing `m` stays within the caps.
    fn fits(&self, m: &SliceMeasure) -> bool {
        let (tables, blobs) = self.bound(m);
        tables <= self.caps.tables && blobs <= self.caps.blobs && tables + blobs <= self.caps.total
    }

    /// What the keys handed on write: their tables and their relocated
    /// values, as [`Self::fits`] bounds them.
    #[cfg(test)]
    pub(super) fn committed_bound(&self) -> (u64, u64) {
        self.bound(
            &self
                .committed
                .measure_with(&SliceLoad::default(), self.relocated),
        )
    }

    /// The bytes a slice writing `m` writes to its tables and to the blob
    /// files: its tables as the probe bounds them, holding the input
    /// tombstones and linking the blob files the relocation writes, one more
    /// on each cut key it passes and each key domain past the first; and its
    /// relocated values, each output blob file closing with its metadata and
    /// its first and last keys, a file per target's worth of frames and, in
    /// each key domain the frames lie in, one more for each codec and lifetime
    /// class they may go to. A file of frames copied out of a source with a
    /// dictionary of its own holds it as well. Every sum is bounded by the
    /// inputs' bytes.
    fn bound(&self, m: &SliceMeasure) -> (u64, u64) {
        let (blobs, files) = match self.relocated {
            Some(relocated) if m.blobs > 0 => {
                let domains = m.blob_domains.max(1);
                let split = m.blobs.div_ceil(relocated.target_size.max(1));
                let files = split + relocated.file_groups * domains;
                let fresh = if m.dictionary_bytes > 0 {
                    relocated.fresh_dictionary
                } else {
                    0
                };
                // A quota proven for longer keys bounds them by its width.
                let longest_key = self.longest_key.max(m.rows.longest_key);
                let bytes = m.blobs
                    + files * (BLOB_META_BYTES + 2 * longest_key)
                    + (m.dictionary_bytes + fresh) * relocated.groups * domains
                    + split * relocated.largest_dictionary;
                (bytes, files)
            }
            _ => (m.blobs, 0),
        };
        let mut rows = m.rows;
        rows.absorb(&self.outputs.tombstones);
        rows.linked_blob_files += files;
        let tables = self
            .outputs
            .probe
            .rewrite_bound(&rows, m.cut_keys + m.table_domains.max(1) - 1);
        (tables, blobs)
    }
}

/// The most a slice writing `now` writes once it adds `quota` with keys,
/// values and seqnos within `widths`: the key and value bytes grow by the
/// table cost at most and the relocated bytes by the blob cost, the entries,
/// filter hashes and owned objects by the table cost's share of
/// [`ENTRY_COST`], the length varints by those of as many entries, and the
/// rest stays as it is.
fn past(now: &SliceMeasure, quota: Cost, widths: &Widths) -> SliceMeasure {
    let n = &now.rows;
    // Each sum is bounded by the caps, far below `u64::MAX`.
    let entries = quota.tables / ENTRY_COST;
    let rows = RewriteLoad {
        entries: n.entries + entries,
        key_bytes: n.key_bytes + quota.tables,
        value_bytes: n.value_bytes + quota.tables,
        filter_hashes: n.filter_hashes + entries,
        longest_key: n.longest_key.max(widths.key),
        longest_value: match (n.longest_value, widths.value) {
            (Some(longest), Some(width)) => Some(longest.max(width)),
            (None, Some(width)) if n.entries == 0 => Some(width),
            _ => None,
        },
        highest_seqno: n.highest_seqno.max(widths.seqno),
        entry_lengths: match n.entry_lengths {
            Some(lengths) => Some(lengths + entries * ENTRY_LENGTHS),
            None if n.entries == 0 => Some(entries * ENTRY_LENGTHS),
            None => None,
        },
        range_tombstones: n.range_tombstones,
        range_tombstone_bytes: n.range_tombstone_bytes,
        linked_blob_files: n.linked_blob_files,
        owned_blob_objects: n.owned_blob_objects + entries,
        cell_columns: n.cell_columns,
    };
    SliceMeasure {
        rows,
        cut_keys: now.cut_keys,
        blobs: now.blobs + quota.blobs,
        dictionary_bytes: now.dictionary_bytes,
        table_domains: now.table_domains,
        blob_domains: now.blob_domains,
    }
}

/// The highest seqno an entry stores in as many varint bytes as `seqno`: an
/// entry stores `2 * seqno + 1`, seven bits a byte.
fn same_width_seqno(seqno: u64) -> u64 {
    let Some(stored) = seqno
        .checked_mul(2)
        .and_then(|doubled| doubled.checked_add(1))
    else {
        return u64::MAX;
    };
    let width = varint_len(stored);
    match u32::try_from(7 * width)
        .ok()
        .and_then(|bits| 1u64.checked_shl(bits))
    {
        // `2 * s + 1 < 2^(7 * width)`, the largest such `s`.
        Some(limit) => (limit - 2) / 2,
        None => u64::MAX,
    }
}
