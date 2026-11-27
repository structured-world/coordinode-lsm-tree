// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The table of one cache shard: a chained hash table whose readers take no
//! lock.
//!
//! Entries live in slots of a slab that only grows and is freed with the
//! table, so a reader may follow any link it ever saw: the memory is there,
//! whatever the slot holds by then. Chains link slots by number. Each slot
//! carries one word of state (`meta`): how many readers hold it, whether it is
//! free, visible or retired, its S3-FIFO frequency and queue, and a version
//! that changes every time the slot is freed.
//!
//! - **A reader** walks its bucket's chain reading only atomics, and checks
//!   the version around each step: a slot freed and reused under it sends it
//!   back to the bucket's head. On a slot of its hash it adds itself to the
//!   slot's readers with one compare-exchange, which succeeds only on a
//!   visible slot of the version it saw; from there the key and value cannot
//!   be dropped until it lets go.
//! - **Writers** of one table take turns behind its lock, which readers never
//!   touch. A writer links a new slot with one store, and takes an entry out
//!   by unlinking its slot and retiring it: a retired slot is freed by the
//!   writer when no reader holds it, and otherwise by the last reader to let
//!   go. A replaced entry's new slot takes the old one's place in the chain
//!   in one store, so a reader finds one of the two.
//! - **The buckets** double when the table holds an entry per two buckets. The
//!   rebuild relinks every slot under a counter that is odd while it runs; a
//!   reader that found nothing answers only if the counter stood still and
//!   even through its walk. Old bucket arrays are kept until the table drops.

use super::Evicted;
use alloc::boxed::Box;
use alloc::vec::Vec;
use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicPtr, AtomicU32, AtomicUsize, Ordering};
use portable_atomic::AtomicU64;

#[cfg(feature = "std")]
type Mutex<T> = parking_lot::Mutex<T>;
#[cfg(feature = "std")]
type MutexGuard<'a, T> = parking_lot::MutexGuard<'a, T>;
// no-std: the spin lock below is the no-std arm already.
#[cfg(not(feature = "std"))]
type Mutex<T> = spin::Mutex<T>;
#[cfg(not(feature = "std"))]
type MutexGuard<'a, T> = spin::MutexGuard<'a, T>;

/// No slot: the end of a chain or of the free list.
const NIL: u32 = u32::MAX;

/// One reader in a slot's `meta`, and the bits that count them.
const REF: u64 = 1;
const REFS: u64 = (1 << 24) - 1;

/// A slot's state in its `meta`.
const STATE: u64 = 0b11 << 24;
/// Holds nothing; on the free list, or about to be.
const FREE: u64 = 0;
/// Holds an entry, linked in its bucket's chain.
const VISIBLE: u64 = 1 << 24;
/// Holds an entry taken out of the table, until its last reader lets go.
const RETIRED: u64 = 2 << 24;

/// The S3-FIFO frequency in a slot's `meta`: two bits.
const FREQ_SHIFT: u32 = 26;
const FREQ: u64 = 0b11 << FREQ_SHIFT;
const FREQ_ONE: u64 = 1 << FREQ_SHIFT;

/// Set in `meta` while the entry is in the main queue, clear in the small one.
const MAIN: u64 = 1 << 28;

/// Set in `meta` of an entry too heavy for its shard's queues: it is in
/// neither, and whoever owns the table accounts for it apart.
const LARGE: u64 = 1 << 29;

/// Where an entry is queued.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Queue {
    Small,
    Main,
    Large,
}

impl Queue {
    const fn flags(self) -> u64 {
        match self {
            Self::Small => 0,
            Self::Main => MAIN,
            Self::Large => LARGE,
        }
    }
}

/// The version takes the upper half of `meta`.
const VERSION_SHIFT: u32 = 32;

/// Slots of the first segment of the slab; each next one holds twice the last.
const FIRST: u32 = 32;
/// Segments enough for every slot number below [`NIL`].
const SEGMENTS: usize = 27;

/// Buckets of a new table.
const FIRST_BUCKETS: usize = 16;

/// `meta` of the same slot freed: the next version, no readers, no flags.
const fn freed(meta: u64) -> u64 {
    ((meta >> VERSION_SHIFT).wrapping_add(1) << VERSION_SHIFT) | FREE
}

/// The version held in `meta`.
const fn version_of(meta: u64) -> u32 {
    (meta >> VERSION_SHIFT) as u32
}

/// The bucket of `hash` among `mask + 1`. The hash is mixed once more, so the
/// bits that picked the shard do not also pick the bucket.
const fn bucket_of(hash: u64, mask: usize) -> usize {
    (hash.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize & mask
}

/// The segment of slot `link` and its place there.
#[expect(
    clippy::cast_possible_truncation,
    reason = "a place in a segment is below the slot number, a u32"
)]
const fn place(link: u32) -> (usize, usize) {
    let n = link as u64 + FIRST as u64;
    let segment = n.ilog2() - FIRST.ilog2();
    let offset = n - ((FIRST as u64) << segment);
    (segment as usize, offset as usize)
}

/// Slots of segment `segment`.
const fn segment_len(segment: usize) -> usize {
    (FIRST as usize) << segment
}

struct Slot<K, V> {
    meta: AtomicU64,
    /// The entry's hash, written while the slot is free.
    hash: AtomicU64,
    /// The next slot of the chain, or of the free list while free.
    next: AtomicU32,
    /// The entry's weight, written while the slot is free.
    weight: AtomicU64,
    key: UnsafeCell<MaybeUninit<K>>,
    value: UnsafeCell<MaybeUninit<V>>,
}

impl<K, V> Slot<K, V> {
    const fn empty() -> Self {
        Self {
            meta: AtomicU64::new(FREE),
            hash: AtomicU64::new(0),
            next: AtomicU32::new(NIL),
            weight: AtomicU64::new(0),
            key: UnsafeCell::new(MaybeUninit::uninit()),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

/// The heads of the chains, a power of two of them.
#[derive(Clone, Copy)]
struct Buckets<'a> {
    heads: &'a [AtomicU32],
}

impl<'a> Buckets<'a> {
    /// A new array of `len` empty chains, to be freed by [`Self::free`].
    fn allocate(len: usize) -> *mut AtomicU32 {
        let heads: Box<[AtomicU32]> = (0..len).map(|_| AtomicU32::new(NIL)).collect();
        Box::into_raw(heads).cast()
    }

    /// The `len` heads at `at`.
    ///
    /// # Safety
    ///
    /// `at` came from [`Self::allocate`] with at least `len` heads and is not
    /// freed while `'a` lasts.
    #[expect(unsafe_code, reason = "see safety")]
    const unsafe fn at(at: *const AtomicU32, len: usize) -> Self {
        Self {
            // SAFETY: the caller's contract.
            heads: unsafe { core::slice::from_raw_parts(at, len) },
        }
    }

    /// Frees an array of `len` heads.
    ///
    /// # Safety
    ///
    /// `at` came from [`Self::allocate`] with `len` heads, is freed once, and
    /// nothing reads it afterwards.
    #[expect(unsafe_code, reason = "see safety")]
    unsafe fn free(at: *mut AtomicU32, len: usize) {
        // SAFETY: the caller's contract.
        unsafe {
            drop(Box::from_raw(core::ptr::slice_from_raw_parts_mut(at, len)));
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "a bucket number is masked to the count"
    )]
    fn head(self, hash: u64) -> &'a AtomicU32 {
        &self.heads[bucket_of(hash, self.heads.len() - 1)]
    }
}

/// A bucket array readers may still be walking, and its length: freed with
/// the table.
struct Retired(*mut AtomicU32, usize);

// SAFETY: the pointer is only dereferenced to free it, by the table's drop.
#[expect(unsafe_code, reason = "see safety")]
unsafe impl Send for Retired {}

/// What the table's writers hold behind its lock, with the state `P` of
/// whoever decides what the table keeps.
struct Locked<P> {
    /// Slots ever handed out: every slot number below it is in the slab.
    allocated: u32,
    /// Entries visible now.
    len: usize,
    retired: Vec<Retired>,
    policy: P,
}

/// A slot and the version it held an entry at: stale once the slot was freed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Node {
    pub(super) link: u32,
    version: u32,
}

/// See the module docs.
pub(super) struct Table<K, V, P> {
    segments: [AtomicPtr<Slot<K, V>>; SEGMENTS],
    /// The bucket array, and its length less one beside it: one load each,
    /// with no header to chase. A rebuild stores the array first, and a
    /// reader loads the mask first, so a reader's mask is never of a longer
    /// array than the one it then loads.
    buckets: AtomicPtr<AtomicU32>,
    mask: AtomicUsize,
    /// Odd while the buckets are rebuilt, and changed by every rebuild.
    rebuilds: AtomicUsize,
    writes: Writes<P>,
}

/// What writers store to on every change, on cache lines of its own: the
/// fields above it are read by every lookup and written by a rebuild alone.
#[repr(align(64))]
struct Writes<P> {
    /// The free slots, linked by `next`: pushed by whoever frees a slot,
    /// popped only behind the lock.
    free: AtomicU32,
    locked: Mutex<Locked<P>>,
}

// SAFETY: a reader on any thread reads keys and values in place (`Sync`) and
// may be the one to drop them (`Send`); the rest is atomics and a lock.
#[expect(unsafe_code, reason = "see safety")]
unsafe impl<K: Send + Sync, V: Send + Sync, P: Send> Sync for Table<K, V, P> {}
// SAFETY: the table owns its keys, values and policy.
#[expect(unsafe_code, reason = "see safety")]
unsafe impl<K: Send, V: Send, P: Send> Send for Table<K, V, P> {}

impl<K, V, P> Table<K, V, P> {
    pub(super) fn new(policy: P) -> Self {
        Self {
            segments: [const { AtomicPtr::new(core::ptr::null_mut()) }; SEGMENTS],
            buckets: AtomicPtr::new(Buckets::allocate(FIRST_BUCKETS)),
            mask: AtomicUsize::new(FIRST_BUCKETS - 1),
            rebuilds: AtomicUsize::new(0),
            writes: Writes {
                free: AtomicU32::new(NIL),
                locked: Mutex::new(Locked {
                    allocated: 0,
                    len: 0,
                    retired: Vec::new(),
                    policy,
                }),
            },
        }
    }

    /// The slot numbered `link`: a number read from this table's heads, links
    /// or free list, or handed out by its lock.
    #[expect(
        clippy::indexing_slicing,
        reason = "a slot number below NIL has a segment"
    )]
    fn slot(&self, link: u32) -> &Slot<K, V> {
        debug_assert!(link != NIL);
        let (segment, offset) = place(link);
        let base = self.segments[segment].load(Ordering::Acquire);
        debug_assert!(!base.is_null());
        // SAFETY: a slot number reaches a reader only through a store made
        // after its segment was stored, and segments live as long as the
        // table; `offset` is below the segment's length by `place`.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            &*base.add(offset)
        }
    }

    fn buckets(&self) -> Buckets<'_> {
        let mask = self.mask.load(Ordering::Acquire);
        let at = self.buckets.load(Ordering::Acquire);
        // SAFETY: every array ever stored lives as long as the table, and the
        // array loaded after a mask is the mask's own or a longer one.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            Buckets::at(at, mask + 1)
        }
    }

    /// Puts a freed slot, its entry already dropped, on the free list.
    fn push_free(&self, link: u32) {
        let slot = self.slot(link);
        let free = &self.writes.free;
        let mut head = free.load(Ordering::Relaxed);
        loop {
            slot.next.store(head, Ordering::Relaxed);
            match free.compare_exchange_weak(head, link, Ordering::Release, Ordering::Relaxed) {
                Ok(_) => return,
                Err(now) => head = now,
            }
        }
    }

    /// Lets go of a slot a reader held, freeing it when it was retired and
    /// this reader was its last.
    fn unpin(&self, link: u32) {
        let slot = self.slot(link);
        let before = slot.meta.fetch_sub(REF, Ordering::AcqRel);
        if before & (STATE | REFS) != RETIRED | REF {
            return;
        }
        // Retired with no reader left: no reader can join a retired slot and
        // its writer left it to its readers, so the word is this thread's.
        slot.meta.store(freed(before), Ordering::Release);
        // SAFETY: the slot held an entry until the store above, and nothing
        // reads the entry of a free slot.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            (*slot.key.get()).assume_init_drop();
            (*slot.value.get()).assume_init_drop();
        }
        self.push_free(link);
    }

    /// The table's lock: one writer at a time.
    pub(super) fn lock(&self) -> Write<'_, K, V, P> {
        Write {
            table: self,
            locked: self.writes.locked.lock(),
        }
    }

    /// [`Self::lock`] when no writer holds it now.
    pub(super) fn try_lock(&self) -> Option<Write<'_, K, V, P>> {
        Some(Write {
            table: self,
            locked: self.writes.locked.try_lock()?,
        })
    }
}

impl<K: Eq, V, P> Table<K, V, P> {
    /// Finds `key` and holds its slot, counting a hit on it when `hit`.
    pub(super) fn pin(&self, hash: u64, key: &K, hit: bool) -> Option<Pinned<'_, K, V, P>> {
        'walk: loop {
            let rebuilds = self.rebuilds.load(Ordering::Acquire);
            if rebuilds & 1 == 1 {
                // Wait for the rebuild behind the lock its writer holds: a
                // writer the scheduler took off its core mid-rebuild would
                // otherwise have every reader of the shard spin through its
                // whole time slice.
                drop(self.writes.locked.lock());
                continue;
            }
            let buckets = self.buckets();
            let mask = buckets.heads.len() - 1;
            let bucket = bucket_of(hash, mask);
            let mut link = buckets.head(hash).load(Ordering::Acquire);
            let mut steps = 0_u32;
            while link != NIL {
                let slot = self.slot(link);
                let meta = slot.meta.load(Ordering::Acquire);
                if meta & STATE == FREE {
                    continue 'walk;
                }
                let slot_hash = slot.hash.load(Ordering::Relaxed);
                let next = slot.next.load(Ordering::Acquire);
                // The hash and the link are this entry's only if the slot was
                // not freed between the two looks at its version.
                if version_of(slot.meta.load(Ordering::Acquire)) != version_of(meta)
                    || bucket_of(slot_hash, mask) != bucket
                {
                    continue 'walk;
                }
                if slot_hash == hash {
                    let mut seen = meta;
                    loop {
                        if seen & STATE != VISIBLE || version_of(seen) != version_of(meta) {
                            // Taken out under this reader: replaced, perhaps,
                            // by a slot now in its place.
                            continue 'walk;
                        }
                        let mut held = seen + REF;
                        if hit && held & FREQ != FREQ {
                            held += FREQ_ONE;
                        }
                        match slot.meta.compare_exchange_weak(
                            seen,
                            held,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => break,
                            Err(now) => seen = now,
                        }
                    }
                    let pinned = Pinned { table: self, link };
                    if pinned.key() == key {
                        return Some(pinned);
                    }
                }
                link = next;
                steps += 1;
                // A walk this long may be going round links a rebuild moved.
                if steps.is_multiple_of(64) && self.rebuilds.load(Ordering::Acquire) != rebuilds {
                    continue 'walk;
                }
            }
            if self.rebuilds.load(Ordering::Acquire) == rebuilds {
                return None;
            }
        }
    }
}

impl<K, V, P> Drop for Table<K, V, P> {
    fn drop(&mut self) {
        let locked = self.writes.locked.get_mut();
        for link in 0..locked.allocated {
            let (segment, offset) = place(link);
            let Some(base) = self.segments.get_mut(segment).map(|at| *at.get_mut()) else {
                continue;
            };
            // SAFETY: every slot below `allocated` is in a stored segment, and
            // a slot that is not free holds an entry no reader is left to
            // drop.
            #[expect(unsafe_code, reason = "see safety")]
            unsafe {
                let slot = &mut *base.add(offset);
                if *slot.meta.get_mut() & STATE != FREE {
                    slot.key.get_mut().assume_init_drop();
                    slot.value.get_mut().assume_init_drop();
                }
            }
        }
        for (segment, at) in self.segments.iter_mut().enumerate() {
            let base = *at.get_mut();
            if !base.is_null() {
                // SAFETY: stored once from a boxed slice of this length.
                #[expect(unsafe_code, reason = "see safety")]
                unsafe {
                    drop(Box::from_raw(core::ptr::slice_from_raw_parts_mut(
                        base,
                        segment_len(segment),
                    )));
                }
            }
        }
        let current = Retired(*self.buckets.get_mut(), *self.mask.get_mut() + 1);
        for Retired(at, len) in locked.retired.drain(..).chain([current]) {
            // SAFETY: each array was allocated with this length, is freed
            // once, and no reader is left.
            #[expect(unsafe_code, reason = "see safety")]
            unsafe {
                Buckets::free(at, len);
            }
        }
    }
}

/// A reader's hold on a slot: its key and value stay until this is dropped.
pub(super) struct Pinned<'a, K, V, P> {
    table: &'a Table<K, V, P>,
    link: u32,
}

impl<K, V, P> Pinned<'_, K, V, P> {
    fn key(&self) -> &K {
        // SAFETY: a held slot keeps its entry until the last reader lets go.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            (*self.table.slot(self.link).key.get()).assume_init_ref()
        }
    }

    pub(super) fn value(&self) -> &V {
        // SAFETY: as for the key.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            (*self.table.slot(self.link).value.get()).assume_init_ref()
        }
    }
}

impl<K, V, P> Drop for Pinned<'_, K, V, P> {
    fn drop(&mut self) {
        self.table.unpin(self.link);
    }
}

/// The table's lock, held: what only one writer at a time may do. Every slot
/// number it takes is one of a visible slot of this table.
pub(super) struct Write<'a, K, V, P> {
    table: &'a Table<K, V, P>,
    locked: MutexGuard<'a, Locked<P>>,
}

impl<K, V, P> Write<'_, K, V, P> {
    pub(super) fn policy(&self) -> &P {
        &self.locked.policy
    }

    pub(super) fn policy_mut(&mut self) -> &mut P {
        &mut self.locked.policy
    }

    /// Entries visible now.
    pub(super) fn len(&self) -> usize {
        self.locked.len
    }

    fn meta(&self, link: u32) -> u64 {
        self.table.slot(link).meta.load(Ordering::Relaxed)
    }

    /// Whether `node`'s slot still holds the entry the node was made for.
    pub(super) fn is_live(&self, node: Node) -> bool {
        let meta = self.meta(node.link);
        meta & STATE == VISIBLE && version_of(meta) == node.version
    }

    pub(super) fn hash(&self, link: u32) -> u64 {
        self.table.slot(link).hash.load(Ordering::Relaxed)
    }

    pub(super) fn weight(&self, link: u32) -> u64 {
        self.table.slot(link).weight.load(Ordering::Relaxed)
    }

    pub(super) fn freq(&self, link: u32) -> u8 {
        ((self.meta(link) & FREQ) >> FREQ_SHIFT) as u8
    }

    pub(super) fn queue(&self, link: u32) -> Queue {
        let meta = self.meta(link);
        if meta & LARGE != 0 {
            Queue::Large
        } else if meta & MAIN != 0 {
            Queue::Main
        } else {
            Queue::Small
        }
    }

    pub(super) fn clear_freq(&self, link: u32) {
        self.table
            .slot(link)
            .meta
            .fetch_and(!FREQ, Ordering::Relaxed);
    }

    /// One off the frequency of an entry whose frequency is above zero:
    /// readers only raise it, so it stays above zero until this.
    pub(super) fn lower_freq(&self, link: u32) {
        debug_assert!(self.freq(link) > 0);
        self.table
            .slot(link)
            .meta
            .fetch_sub(FREQ_ONE, Ordering::Relaxed);
    }

    pub(super) fn set_main(&self, link: u32) {
        self.table.slot(link).meta.fetch_or(MAIN, Ordering::Relaxed);
    }

    pub(super) fn key(&self, link: u32) -> &K {
        // SAFETY: a visible slot holds an entry, and only this lock's holder
        // takes one out.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            (*self.table.slot(link).key.get()).assume_init_ref()
        }
    }

    #[cfg(feature = "zstd")]
    pub(super) fn value(&self, link: u32) -> &V {
        // SAFETY: as for the key.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            (*self.table.slot(link).value.get()).assume_init_ref()
        }
    }

    /// The numbers of the visible slots.
    #[cfg(any(feature = "std", test))]
    pub(super) fn links(&self) -> impl Iterator<Item = u32> + '_ {
        (0..self.locked.allocated).filter(|&link| self.meta(link) & STATE == VISIBLE)
    }

    /// A free slot, from the free list or fresh from the slab.
    fn allocate(&mut self) -> u32 {
        // Only this lock's holder pops, so the head it read is still linked
        // to the `next` it read when the exchange finds the head unchanged.
        let mut head = self.table.writes.free.load(Ordering::Acquire);
        while head != NIL {
            let next = self.table.slot(head).next.load(Ordering::Relaxed);
            match self.table.writes.free.compare_exchange_weak(
                head,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return head,
                Err(now) => head = now,
            }
        }
        let link = self.locked.allocated;
        assert!(link != NIL, "a cache shard holds fewer than 2^32 entries");
        let (segment, offset) = place(link);
        if offset == 0 {
            let slots: Box<[Slot<K, V>]> =
                (0..segment_len(segment)).map(|_| Slot::empty()).collect();
            if let Some(at) = self.table.segments.get(segment) {
                at.store(Box::into_raw(slots).cast(), Ordering::Release);
            }
        }
        self.locked.allocated = link + 1;
        link
    }

    /// Writes an entry into a free slot whose chain link is `next`, and makes
    /// the slot visible: readers find it once a chain links to it.
    #[expect(
        clippy::too_many_arguments,
        reason = "an entry's parts, written together"
    )]
    fn fill(
        &mut self,
        hash: u64,
        key: K,
        value: V,
        weight: u64,
        freq: u8,
        queue: Queue,
        next: u32,
    ) -> Node {
        let link = self.allocate();
        let slot = self.table.slot(link);
        let meta = slot.meta.load(Ordering::Relaxed);
        slot.hash.store(hash, Ordering::Relaxed);
        slot.weight.store(weight, Ordering::Relaxed);
        slot.next.store(next, Ordering::Relaxed);
        // SAFETY: a free slot off the free list holds no entry and no reader
        // reads one in it.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            (*slot.key.get()).write(key);
            (*slot.value.get()).write(value);
        }
        let flags = (u64::from(freq) << FREQ_SHIFT) & FREQ | queue.flags();
        slot.meta.store(
            (meta & !(STATE | REFS | FREQ | MAIN | LARGE)) | VISIBLE | flags,
            Ordering::Release,
        );
        Node {
            link,
            version: version_of(meta),
        }
    }

    /// The link that points at visible slot `link`: its bucket's head, or the
    /// slot before it in the chain.
    fn link_to(&self, link: u32) -> &AtomicU32 {
        let head = self.table.buckets().head(self.hash(link));
        let mut at = head;
        loop {
            let next = at.load(Ordering::Relaxed);
            if next == link || next == NIL {
                debug_assert!(next == link, "a visible slot is in its chain");
                return at;
            }
            at = &self.table.slot(next).next;
        }
    }

    /// Takes an unlinked slot's entry out: freed now, its value handed to
    /// `evicted`, when no reader holds it; left to its last reader otherwise.
    fn retire(&self, link: u32, evicted: &mut Evicted<V>) {
        let slot = self.table.slot(link);
        let mut meta = slot.meta.load(Ordering::Relaxed);
        loop {
            let (to, free) = if meta & REFS == 0 {
                (freed(meta), true)
            } else {
                ((meta & !STATE) | RETIRED, false)
            };
            match slot
                .meta
                .compare_exchange_weak(meta, to, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) if free => break,
                Ok(_) => return,
                Err(now) => meta = now,
            }
        }
        // SAFETY: the slot held an entry and no reader when it was freed, and
        // no reader joins a free slot.
        #[expect(unsafe_code, reason = "see safety")]
        let value = unsafe {
            (*slot.key.get()).assume_init_drop();
            (*slot.value.get()).assume_init_read()
        };
        evicted.push(value);
        self.table.push_free(link);
    }

    /// Adds an entry no visible slot holds the key of.
    pub(super) fn insert(
        &mut self,
        hash: u64,
        key: K,
        value: V,
        weight: u64,
        freq: u8,
        queue: Queue,
    ) -> Node {
        // Two buckets an entry: most chains hold one slot, so a lookup reads
        // one slot's cache line, at four bytes a bucket.
        if self.locked.len * 2 >= self.table.buckets().heads.len() {
            self.grow();
        }
        let head = self.table.buckets().head(hash);
        let node = self.fill(
            hash,
            key,
            value,
            weight,
            freq,
            queue,
            head.load(Ordering::Relaxed),
        );
        head.store(node.link, Ordering::Release);
        self.locked.len += 1;
        node
    }

    /// Puts an entry of the same key in `old`'s place in its chain, in one
    /// store, and takes `old` out.
    #[expect(
        clippy::too_many_arguments,
        reason = "an entry's parts, written together"
    )]
    pub(super) fn replace(
        &mut self,
        old: u32,
        key: K,
        value: V,
        weight: u64,
        freq: u8,
        queue: Queue,
        evicted: &mut Evicted<V>,
    ) -> Node {
        let next = self.table.slot(old).next.load(Ordering::Relaxed);
        let node = self.fill(self.hash(old), key, value, weight, freq, queue, next);
        self.link_to(old).store(node.link, Ordering::Release);
        self.retire(old, evicted);
        node
    }

    /// Takes the entry of visible slot `link` out of the table.
    pub(super) fn remove(&mut self, link: u32, evicted: &mut Evicted<V>) {
        let next = self.table.slot(link).next.load(Ordering::Relaxed);
        self.link_to(link).store(next, Ordering::Release);
        self.locked.len -= 1;
        self.retire(link, evicted);
    }

    /// Doubles the buckets, relinking every visible slot.
    fn grow(&mut self) {
        let old_at = self.table.buckets.load(Ordering::Relaxed);
        let old = self.table.buckets();
        let len = old.heads.len() * 2;
        let new_at = Buckets::allocate(len);
        // SAFETY: just allocated with this length, and freed only with the
        // table.
        #[expect(unsafe_code, reason = "see safety")]
        let new = unsafe { Buckets::at(new_at, len) };
        self.table.rebuilds.fetch_add(1, Ordering::AcqRel);
        for head in old.heads {
            let mut link = head.load(Ordering::Relaxed);
            while link != NIL {
                let slot = self.table.slot(link);
                let next = slot.next.load(Ordering::Relaxed);
                let to = new.head(slot.hash.load(Ordering::Relaxed));
                slot.next
                    .store(to.load(Ordering::Relaxed), Ordering::Release);
                to.store(link, Ordering::Relaxed);
                link = next;
            }
        }
        self.table.buckets.store(new_at, Ordering::Release);
        self.table.mask.store(len - 1, Ordering::Release);
        self.table.rebuilds.fetch_add(1, Ordering::Release);
        self.locked.retired.push(Retired(old_at, old.heads.len()));
    }
}

impl<K: Eq, V, P> Write<'_, K, V, P> {
    /// The visible slot holding `key`.
    pub(super) fn find(&self, hash: u64, key: &K) -> Option<u32> {
        let mut link = self.table.buckets().head(hash).load(Ordering::Relaxed);
        while link != NIL {
            if self.hash(link) == hash && self.key(link) == key {
                return Some(link);
            }
            link = self.table.slot(link).next.load(Ordering::Relaxed);
        }
        None
    }
}
