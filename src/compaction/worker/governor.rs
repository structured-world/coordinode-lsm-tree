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
//! What a slice writes only grows with the keys it takes, so the bound is
//! not taken at every key. At each bound taken the governor also checks a
//! point past the slice so far, grown as much again as it grew since the
//! bound before; a key that keeps the slice within that point, in every
//! measure the bound grows with, fits without a bound of its own.
//!
//! A dropped version is handed on in the merge's order with the keys around
//! it: the ownership ledger settles a key's blob objects from its dropped
//! versions, and those of a key past the end are the next slice's to count.

use super::{
    BLOB_META_BYTES, DomainEdge, Relocated, SliceCaps, SliceLoad, SliceMeasure, SliceOutputs,
    enter_domain, take_entry,
};
use crate::table::writer::RewriteLoad;
use crate::{InternalValue, UserKey};
use alloc::vec::Vec;
use core::cell::RefCell;

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
    /// Whether a key was handed on.
    any_committed: bool,
    /// The slice as it stood at the last bound taken, and the point past it
    /// known to fit.
    checked: SliceMeasure,
    room: SliceMeasure,
    /// The longest key seen.
    longest_key: u64,
    /// The key being merged, what its rows write, and the cut keys it passes.
    key: Option<UserKey>,
    load: SliceLoad,
    key_cuts: usize,
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
        let empty = SliceLoad::default().measure_with(&SliceLoad::default(), relocated);
        Self {
            caps,
            outputs,
            relocated,
            comparator,
            split_cells: outputs.probe.splits_cells(),
            hashes_read_keys: outputs.probe.hashes_read_keys(),
            committed: SliceLoad::default(),
            any_committed: false,
            checked: empty.clone(),
            room: empty,
            longest_key: 0,
            key: None,
            load: SliceLoad::default(),
            key_cuts: cuts,
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
        !self.any_committed
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
        self.longest_key = self.longest_key.max(key.len() as u64);
        if new_key {
            while self
                .outputs
                .cut_keys
                .get(self.key_cuts)
                .is_some_and(|cut| cmp.compare(cut, key) != core::cmp::Ordering::Greater)
            {
                self.load.cut_keys += 1;
                self.key_cuts += 1;
            }
        }
        let hashes = if self.hashes_read_keys {
            self.outputs.probe.filter_hashes_bound(key)
        } else {
            1
        };
        let mut dictionaries = Vec::new();
        let blobs = take_entry(
            &mut self.load,
            (item.key.value_type, item.key.seqno, key.len(), &item.value),
            hashes,
            self.split_cells,
            self.relocated,
            &mut dictionaries,
        )?;
        self.load.blobs += blobs;
        for source in dictionaries {
            self.load.add_dictionary(source, self.relocated);
        }
        if let Some(domains) = self.outputs.domains {
            let (table_end, blob_end) = &mut self.edges;
            enter_domain(domains, cmp, table_end, &mut self.load.table_domains, key);
            if blobs > 0 {
                enter_domain(domains, cmp, blob_end, &mut self.load.blob_domains, key);
            }
        }
        Ok(())
    }

    /// Hands on the key being merged, with the held events below `upto` (all
    /// of them for `None`), when the slice with it fits, `true`; otherwise
    /// ends the slice before the key, `false`.
    fn settle(&mut self, upto: Option<&UserKey>) -> bool {
        let with = self.committed.measure_with(&self.load, self.relocated);
        let fits = within(&with, &self.room) || {
            let fits = self.fits(&with);
            if fits {
                self.grow_room(with);
            }
            fits
        };
        if !fits {
            let end = self.key.take();
            if self.any_committed {
                // The versions dropped below the key are this slice's.
                self.hand_on_below(end.as_ref());
            }
            self.held.clear();
            self.end = end;
            return false;
        }
        let load = core::mem::take(&mut self.load);
        self.committed.absorb(&load, self.relocated);
        self.any_committed = true;
        self.hand_on_below(upto);
        true
    }

    /// Records `now`, a slice that fits, as the last bound taken, and the
    /// point past it grown as much again as it grew since the bound before,
    /// or as much of that as fits, as the room keys take without a bound.
    fn grow_room(&mut self, now: SliceMeasure) {
        let mut step = Growth::between(&self.checked, &now);
        let mut room = step.past(&now);
        // A few halvings find most of the room left; past them a key takes
        // a bound of its own, as every key does once the room is nearly used.
        for _ in 0..8 {
            if self.fits(&room) {
                break;
            }
            step = step.halved();
            room = step.past(&now);
        }
        if !self.fits(&room) {
            room = now.clone();
        }
        self.checked = now;
        self.room = room;
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
                let bytes = m.blobs
                    + files * (BLOB_META_BYTES + 2 * self.longest_key)
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

/// Whether a slice writing `m` writes no more than one writing `room`: every
/// measure the bound grows with is at or below `room`'s.
fn within(m: &SliceMeasure, room: &SliceMeasure) -> bool {
    let (a, b) = (&m.rows, &room.rows);
    // A missing bound is the widest: it is past any value it stands for.
    let at_most = |a: Option<u64>, b: Option<u64>| match (a, b) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(a), Some(b)) => a <= b,
    };
    a.entries <= b.entries
        && a.key_bytes <= b.key_bytes
        && a.value_bytes <= b.value_bytes
        && a.filter_hashes <= b.filter_hashes
        && a.longest_key <= b.longest_key
        && at_most(a.longest_value, b.longest_value)
        && a.highest_seqno <= b.highest_seqno
        && at_most(a.entry_lengths, b.entry_lengths)
        && a.range_tombstones <= b.range_tombstones
        && a.range_tombstone_bytes <= b.range_tombstone_bytes
        && a.linked_blob_files <= b.linked_blob_files
        && a.owned_blob_objects <= b.owned_blob_objects
        && match (a.cell_columns, b.cell_columns) {
            (None, None) => true,
            (Some(a), Some(b)) => a.count <= b.count && a.row_bytes <= b.row_bytes,
            _ => false,
        }
        && m.cut_keys <= room.cut_keys
        && m.blobs <= room.blobs
        && m.dictionary_bytes <= room.dictionary_bytes
        && m.table_domains <= room.table_domains
        && m.blob_domains <= room.blob_domains
}

/// How much a slice grew between two bounds taken, in the measures that add
/// up: what a point past the slice adds to it.
#[derive(Clone, Copy)]
struct Growth {
    entries: u64,
    key_bytes: u64,
    value_bytes: u64,
    filter_hashes: u64,
    entry_lengths: u64,
    linked_blob_files: u64,
    owned_blob_objects: u64,
    cut_keys: u64,
    blobs: u64,
    dictionary_bytes: u64,
    table_domains: u64,
    blob_domains: u64,
}

impl Growth {
    /// What the slice grew by from `before` to `now`, which holds it.
    fn between(before: &SliceMeasure, now: &SliceMeasure) -> Self {
        let (b, n) = (&before.rows, &now.rows);
        // Each measure of `now` is at least `before`'s: the slice only grew.
        Self {
            entries: n.entries - b.entries,
            key_bytes: n.key_bytes - b.key_bytes,
            value_bytes: n.value_bytes - b.value_bytes,
            filter_hashes: n.filter_hashes - b.filter_hashes,
            // A missing one is the widest already, and grows no further.
            entry_lengths: match (b.entry_lengths, n.entry_lengths) {
                (Some(b), Some(n)) => n - b,
                _ => 0,
            },
            linked_blob_files: n.linked_blob_files - b.linked_blob_files,
            owned_blob_objects: n.owned_blob_objects - b.owned_blob_objects,
            cut_keys: now.cut_keys - before.cut_keys,
            blobs: now.blobs - before.blobs,
            dictionary_bytes: now.dictionary_bytes - before.dictionary_bytes,
            table_domains: now.table_domains - before.table_domains,
            blob_domains: now.blob_domains - before.blob_domains,
        }
    }

    /// Half of it.
    fn halved(self) -> Self {
        Self {
            entries: self.entries / 2,
            key_bytes: self.key_bytes / 2,
            value_bytes: self.value_bytes / 2,
            filter_hashes: self.filter_hashes / 2,
            entry_lengths: self.entry_lengths / 2,
            linked_blob_files: self.linked_blob_files / 2,
            owned_blob_objects: self.owned_blob_objects / 2,
            cut_keys: self.cut_keys / 2,
            blobs: self.blobs / 2,
            dictionary_bytes: self.dictionary_bytes / 2,
            table_domains: self.table_domains / 2,
            blob_domains: self.blob_domains / 2,
        }
    }

    /// The point past `now` it reaches. The longest key and value may
    /// double, and the seqnos run to the last one whose varint is as long as
    /// the highest's, which leaves the entries as long.
    fn past(&self, now: &SliceMeasure) -> SliceMeasure {
        let n = &now.rows;
        // Each sum is bounded by the inputs' bytes and seqnos, far below
        // `u64::MAX`, so doubling one does not overflow.
        let rows = RewriteLoad {
            entries: n.entries + self.entries,
            key_bytes: n.key_bytes + self.key_bytes,
            value_bytes: n.value_bytes + self.value_bytes,
            filter_hashes: n.filter_hashes + self.filter_hashes,
            longest_key: 2 * n.longest_key,
            longest_value: n.longest_value.map(|longest| 2 * longest),
            highest_seqno: same_width_seqno(n.highest_seqno),
            entry_lengths: n.entry_lengths.map(|lengths| lengths + self.entry_lengths),
            range_tombstones: n.range_tombstones,
            range_tombstone_bytes: n.range_tombstone_bytes,
            linked_blob_files: n.linked_blob_files + self.linked_blob_files,
            owned_blob_objects: n.owned_blob_objects + self.owned_blob_objects,
            cell_columns: n.cell_columns,
        };
        SliceMeasure {
            rows,
            cut_keys: now.cut_keys + self.cut_keys,
            blobs: now.blobs + self.blobs,
            dictionary_bytes: now.dictionary_bytes + self.dictionary_bytes,
            table_domains: now.table_domains + self.table_domains,
            blob_domains: now.blob_domains + self.blob_domains,
        }
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
    let width = crate::table::writer::varint_len(stored);
    match u32::try_from(7 * width)
        .ok()
        .and_then(|bits| 1u64.checked_shl(bits))
    {
        // `2 * s + 1 < 2^(7 * width)`, the largest such `s`.
        Some(limit) => (limit - 2) / 2,
        None => u64::MAX,
    }
}
