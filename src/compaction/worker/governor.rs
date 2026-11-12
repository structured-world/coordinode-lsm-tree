// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Where a tight-space slice ends, found as its merge runs.
//!
//! The merge hands the rows it keeps and the versions it drops to a
//! [`SliceGovernor`], which holds them back and hands them on a run of keys
//! at a time while what the slice writes stays within its room. What a slice
//! writes only grows with the keys it takes, so a run past the room is cut at
//! the last of its keys that fits, the slice ends before the next key, and
//! the next slice starts there. A key never splits across slices.
//!
//! A dropped version is handed on with the keys around it, in the merge's
//! order: the ownership ledger settles a key's blob objects from its dropped
//! versions, and those of a key past the end are the next slice's to count.

use super::{
    BLOB_META_BYTES, DomainEdge, Relocated, SliceCaps, SliceLoad, SliceMeasure, SliceOutputs,
    enter_domain, take_entry,
};
use crate::{InternalValue, UserKey};
use alloc::vec::Vec;
use core::cell::RefCell;

/// The first run of held keys checked, by the bytes of their keys and
/// values: a slice of a few keys is decided at once.
const FIRST_CHECK: usize = 64 * 1_024;

/// The longest run of held keys: past it what is held is checked, so what
/// the governor holds in memory stays within it whatever the room.
const LONGEST_CHECK: usize = 1_024 * 1_024;

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
    /// The cut keys `committed` passed.
    committed_cuts: usize,
    /// The longest key seen.
    longest_key: u64,
    /// What the merge handed over since, held back.
    held: Vec<Event>,
    /// Each held key, and where its first kept row lies in `held`.
    keys: Vec<(usize, UserKey)>,
    /// What the held keys write, and the cut keys they pass.
    load: SliceLoad,
    load_cuts: usize,
    load_edges: (DomainEdge, DomainEdge),
    /// The bytes of the held keys and values, and the bytes at which they
    /// are checked.
    held_bytes: usize,
    check_at: usize,
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
        let passed = from.map_or(0, |from| {
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
            any_committed: false,
            committed_cuts: passed,
            longest_key: 0,
            held: Vec::new(),
            keys: Vec::new(),
            load: SliceLoad::default(),
            load_cuts: passed,
            load_edges: (DomainEdge::Unseen, DomainEdge::Unseen),
            held_bytes: 0,
            check_at: FIRST_CHECK,
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
            .keys
            .last()
            .is_none_or(|(_, key)| !crate::comparator::same_user_key(key, &item.key.user_key));
        if new_key
            && !self.keys.is_empty()
            && self.held_bytes >= self.check_at
            && !self.settle(Some(&item.key.user_key))?
        {
            return Ok(false);
        }
        if new_key {
            self.keys.push((self.held.len(), item.key.user_key.clone()));
        }
        self.held_bytes += item.key.user_key.len() + item.value.len();
        let (mut cuts, mut edges) = (
            self.load_cuts,
            core::mem::replace(
                &mut self.load_edges,
                (DomainEdge::Unseen, DomainEdge::Unseen),
            ),
        );
        let mut load = core::mem::take(&mut self.load);
        let taken = self.take(&mut load, &mut cuts, &mut edges, &item, new_key);
        (self.load, self.load_cuts, self.load_edges) = (load, cuts, edges);
        taken?;
        self.held.push(Event::Kept(item, starts_key));
        Ok(true)
    }

    /// Ends the merge: what is held is handed on as far as it fits.
    ///
    /// # Errors
    ///
    /// Returns an error when a row's value does not decode.
    pub(super) fn finish(&mut self, dropped: &RefCell<Vec<InternalValue>>) -> crate::Result<()> {
        if self.end.is_some() {
            return Ok(());
        }
        self.hold_dropped(dropped);
        self.settle(None)?;
        Ok(())
    }

    /// Places the versions dropped since among the held events.
    fn hold_dropped(&mut self, dropped: &RefCell<Vec<InternalValue>>) {
        self.held
            .extend(dropped.borrow_mut().drain(..).map(Event::Dropped));
    }

    /// Takes the kept row `item` into `load`: one more entry, and the cut
    /// keys and key domains it passes when it starts a key (`new_key`).
    fn take(
        &mut self,
        load: &mut SliceLoad,
        cuts: &mut usize,
        (table_end, blob_end): &mut (DomainEdge, DomainEdge),
        item: &InternalValue,
        new_key: bool,
    ) -> crate::Result<()> {
        let cmp = self.comparator.as_ref();
        let key = &item.key.user_key;
        self.longest_key = self.longest_key.max(key.len() as u64);
        if new_key {
            while self
                .outputs
                .cut_keys
                .get(*cuts)
                .is_some_and(|cut| cmp.compare(cut, key) != core::cmp::Ordering::Greater)
            {
                load.cut_keys += 1;
                *cuts += 1;
            }
        }
        let hashes = if self.hashes_read_keys {
            self.outputs.probe.filter_hashes_bound(key)
        } else {
            1
        };
        let mut dictionaries = Vec::new();
        let blobs = take_entry(
            load,
            (item.key.value_type, item.key.seqno, key.len(), &item.value),
            hashes,
            self.split_cells,
            self.relocated,
            &mut dictionaries,
        )?;
        load.blobs += blobs;
        for source in dictionaries {
            load.add_dictionary(source, self.relocated);
        }
        if let Some(domains) = self.outputs.domains {
            enter_domain(domains, cmp, table_end, &mut load.table_domains, key);
            if blobs > 0 {
                enter_domain(domains, cmp, blob_end, &mut load.blob_domains, key);
            }
        }
        Ok(())
    }

    /// Hands on the held keys below `upto` (all of them for `None`) when
    /// they fit, `true`; otherwise as many as fit, the slice ending before
    /// the next, `false`.
    fn settle(&mut self, upto: Option<&UserKey>) -> crate::Result<bool> {
        if self.fits(&self.committed.measure_with(&self.load, self.relocated)) {
            let load = core::mem::take(&mut self.load);
            self.committed.absorb(&load, self.relocated);
            self.committed_cuts = self.load_cuts;
            self.any_committed |= !self.keys.is_empty();
            self.keys.clear();
            self.load_edges = (DomainEdge::Unseen, DomainEdge::Unseen);
            self.held_bytes = 0;
            self.check_at = (self.check_at * 2).min(LONGEST_CHECK);
            self.hand_on_below(upto);
            return Ok(true);
        }
        // The most held keys that fit: none fits at the least, all do not.
        let (mut fitting, mut past) = (0, self.keys.len());
        while past - fitting > 1 {
            let mid = fitting + (past - fitting) / 2;
            if self.prefix_fits(mid)? {
                fitting = mid;
            } else {
                past = mid;
            }
        }
        let end = self.keys.get(fitting).map(|(_, key)| key.clone());
        if fitting > 0 {
            let load = self.prefix_load(fitting)?;
            self.committed.absorb(&load, self.relocated);
            self.any_committed = true;
        }
        if self.any_committed {
            self.hand_on_below(end.as_ref());
        }
        self.held.clear();
        self.end = end;
        Ok(false)
    }

    /// Whether the first `count` held keys fit beside what was handed on.
    fn prefix_fits(&mut self, count: usize) -> crate::Result<bool> {
        let load = self.prefix_load(count)?;
        Ok(self.fits(&self.committed.measure_with(&load, self.relocated)))
    }

    /// What the first `count` held keys write, all of them past the last.
    fn prefix_load(&mut self, count: usize) -> crate::Result<SliceLoad> {
        let stop = self
            .keys
            .get(count)
            .map_or(self.held.len(), |&(stop, _)| stop);
        let mut load = SliceLoad::default();
        let mut cuts = self.committed_cuts;
        let mut edges = (DomainEdge::Unseen, DomainEdge::Unseen);
        let mut previous: Option<UserKey> = None;
        let held = core::mem::take(&mut self.held);
        let mut taken = Ok(());
        for event in held.get(..stop).unwrap_or_default() {
            let Event::Kept(item, _) = event else {
                continue;
            };
            let new_key = previous
                .as_ref()
                .is_none_or(|key| !crate::comparator::same_user_key(key, &item.key.user_key));
            if new_key {
                previous = Some(item.key.user_key.clone());
            }
            taken = self.take(&mut load, &mut cuts, &mut edges, item, new_key);
            if taken.is_err() {
                break;
            }
        }
        self.held = held;
        taken?;
        Ok(load)
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

    /// Whether a slice writing `m` stays within the caps: its tables as the
    /// probe bounds them, holding the input tombstones and linking the blob
    /// files the relocation writes, one more on each cut key it passes and
    /// each key domain past the first; and its relocated values, each output
    /// blob file closing with its metadata and its first and last keys, a
    /// file per target's worth of frames and, in each key domain the frames
    /// lie in, one more for each codec and lifetime class they may go to. A
    /// file of frames copied out of a source with a dictionary of its own
    /// holds it as well. Every sum is bounded by the inputs' bytes.
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
    /// files (see [`Self::fits`]).
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
