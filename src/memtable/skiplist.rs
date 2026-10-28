// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Arena-based concurrent skiplist for memtable storage.
//!
//! Nodes are allocated from a contiguous [`Arena`] for cache locality and O(1)
//! bulk deallocation when the memtable is dropped.  Concurrent skiplist
//! traversal is lock-free (atomic loads on next-pointers); inserts use CAS with
//! retry on tower links.  Values are stored in a lock-free segmented
//! [`ValueStore`] — reads are wait-free.
//!
//! The design follows the arena-skiplist pattern used by Pebble/CockroachDB
//! and Badger, adapted for Rust's ownership model and the lsm-tree
//! `InternalKey` ordering (`user_key` ASC, seqno DESC).

use super::arena::Arena;
use super::value_store::ValueStore;
use crate::comparator::SharedComparator;
use crate::key::InternalKey;
use crate::runtime_config::ChecksumAlgorithm;
use crate::value::{InternalValue, SeqNo, UserValue};
use crate::{UserKey, ValueType};

use core::cmp::Ordering as CmpOrdering;
use core::ops::{Bound, RangeBounds};
use core::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum tower height.  With P = 1/4 this supports ~4^20 ≈ 10^12 entries.
const MAX_HEIGHT: usize = 20;

/// Sentinel offset meaning "no node".  Offset 0 is reserved in the arena.
const UNSET: u32 = 0;

/// Most nodes a backward step keeps batched ahead (a power of two).
const BACK_BATCH: usize = 64;

/// Level down to which a back-batch search descends before walking level 0.
const BACK_BATCH_LEVEL: usize = 2;

// ---------------------------------------------------------------------------
// Node layout (offsets within a node allocation)
// ---------------------------------------------------------------------------
// All multi-byte fields are stored in **native** byte order (LE on x86/ARM)
// because the arena is never persisted — it lives only in memory.
//
// +0   u32  key_offset    — offset of user_key bytes in the arena
// +4   u32  value_idx     — index into the SkipMap `ValueStore`
// +8   u16  key_len       — user_key length
// +10  u8   value_type    — ValueType discriminant
// +11  u8   height        — tower height (1..=MAX_HEIGHT)
// +12  u32  (reserved)    — padding for alignment
// +16  u64  seqno         — sequence number
// +24  [u32; height]      — tower: next-pointers per level (AtomicU32)
// +24+4×height  [u8; key_len] — user_key bytes (same allocation, so the key
//                              shares the header's cache line for the common
//                              height-1/2 node; `key_offset` still addresses
//                              it like any arena span)
//
// Values are stored in a separate heap-backed Vec so that large values
// don't bloat the arena and cause exhaustion.
//
// Total: 24 + 4 × height + key_len   (always 4-byte aligned)

// Layout offsets — only OFF_HEIGHT and OFF_TOWER are used by name in code;
// the rest are accessed via array slicing in the node_*() accessors.
const OFF_HEIGHT: u32 = 11;
const OFF_TOWER: u32 = 24;

// Per-KV insert-time digest (KvChecksumComputePoint::AtInsert): the 4-byte
// reserved slot at +12 (otherwise alignment padding) holds the entry's 4-byte
// digest, fixed at insert and re-checked at flush. The value_type byte (+10)
// packs three fields, since ValueType discriminants are only 0..=2:
//   bits 0..=2  ValueType discriminant
//   bits 5..=6  wire_tag of the 4-byte algorithm the digest was computed with
//   bit  7      "this node carries an insert digest"
// Storing the algorithm PER NODE (not once per memtable) is what makes the
// residence check immune to a mid-memtable `kv_checksum_algo` change: each
// node is verified under the algorithm it was stored with. A node without the
// presence bit has the slot zeroed and is never verified (covers the
// mixed-variant memtable after an Off -> AtInsert toggle).
const KV_DIGEST_PRESENT: u8 = 0x80;
const VALUE_TYPE_MASK: u8 = 0x07;
const KV_ALGO_SHIFT: u8 = 5;
const KV_ALGO_MASK: u8 = 0x60;

/// Byte size of a node with the given tower `height`.
#[expect(
    clippy::cast_possible_truncation,
    reason = "height <= MAX_HEIGHT (20), always fits in u32"
)]
const fn node_size(height: usize) -> u32 {
    OFF_TOWER + (height as u32) * 4
}

/// Arena bytes taken by the smallest node: a one-level tower and an empty key.
/// Every value in the store belongs to a node, so a memtable holds at most
/// `2^32 / MIN_NODE_SIZE` values.
pub(super) const MIN_NODE_SIZE: u32 = node_size(1);

/// Outcome of reading a node's insert-time per-KV digest slot
/// (`KvChecksumComputePoint::AtInsert`).
enum NodeKvDigest {
    /// No digest stored (the `KV_DIGEST_PRESENT` flag is clear).
    Absent,
    /// A valid 4-byte digest under the recovered algorithm.
    Present(u32, ChecksumAlgorithm),
    /// The presence bit is set but the algorithm tag is not a 4-byte algorithm
    /// `AtInsert` would store (corruption of the algorithm or presence bits).
    /// Carries the offending wire tag for diagnostics.
    CorruptAlgorithm(u8),
}

// ---------------------------------------------------------------------------
// SkipMap
// ---------------------------------------------------------------------------

/// A concurrent ordered map backed by an arena-allocated skiplist.
///
/// Provides lock-free traversal and CAS-based inserts with O(log n) expected
/// time.  Values are stored in a lock-free segmented [`ValueStore`] so large
/// blobs do not bloat the arena; value reads are wait-free.  Keys are
/// [`InternalKey`] (ordered by `user_key` ascending, then seqno descending).
pub(crate) struct SkipMap {
    arena: Arena,
    /// Lock-free segmented storage for values.  Keys live in the arena for
    /// cache locality during comparisons; values live here so large blobs
    /// don't exhaust the arena.  Indexed by `value_idx` stored in each node.
    values: ValueStore,
    /// User key comparator for ordering entries.
    comparator: SharedComparator,
    /// Cached `comparator.is_lexicographic()`, read once at construction.
    /// Lets the per-comparison hot path (`compare_key`) take a direct,
    /// inlinable `slice::cmp` for the default byte-ordering comparator instead
    /// of an indirect `dyn` vtable call on every node visited during a search.
    is_lexicographic: bool,
    /// Offset of the sentinel head node in the arena.
    head: u32,
    /// Current maximum height of any inserted node.
    height: AtomicUsize,
    /// Seed of the node heights, mixed with each node's value index: the
    /// index reservation is already one per insert, so the heights need no
    /// counter of their own for the writers to share.
    height_seed: u64,
    /// The most recently appended TAIL node (`UNSET` until one exists), a
    /// hint for the sequential-insert fast path in
    /// [`Self::insert_with_kv_digest`]. Purely advisory: the fast path
    /// re-validates it and the level-0 CAS is what makes the link correct,
    /// so a stale hint costs one comparison and falls back to the full
    /// splice search. Nodes are never freed while the map lives, so the
    /// offset stays dereferenceable.
    tail_hint: AtomicU32,
    /// Links an insert retried because another writer changed its predecessor
    /// between the splice search and the link: the contention concurrent
    /// writers meet. Counted on the failed path only.
    #[cfg(feature = "metrics")]
    cas_retries: AtomicUsize,
    /// Test-only: runs once inside a seek, between its level-0 comparison and
    /// its return, so a test can link a node into that window deterministically.
    #[cfg(test)]
    seek_hook: std::sync::Mutex<Option<SeekHook>>,
    /// Test-only: runs once inside an insert, between its splice search and
    /// its first link, so a test can race that link deterministically.
    #[cfg(all(test, feature = "metrics"))]
    link_hook: std::sync::Mutex<Option<SeekHook>>,
}

/// What [`SkipMap::seek_hook`] runs.
#[cfg(test)]
pub(crate) type SeekHook = alloc::boxed::Box<dyn FnOnce(&SkipMap) + Send>;

impl SkipMap {
    /// Creates a new empty skiplist with the given user key comparator.
    ///
    /// The arena grows lazily in 4 MiB blocks — no large upfront allocation.
    pub(crate) fn new(comparator: SharedComparator) -> Self {
        let arena = Arena::new();

        // Allocate the head sentinel with MAX_HEIGHT.
        let head_size = node_size(MAX_HEIGHT);
        #[expect(
            clippy::expect_used,
            reason = "arena capacity is a fixed configuration; exhaustion is fatal"
        )]
        let head = arena
            .alloc(head_size, 4)
            .expect("arena must fit at least the head sentinel");

        // The arena no longer zeroes memory, so explicitly initialize the head
        // sentinel: zero the whole node (header fields + all MAX_HEIGHT tower
        // slots = UNSET), then stamp the height byte. The head's tower is read
        // by every search start, so its slots MUST be UNSET before any reader;
        // regular nodes self-initialize their `[0, height)` tower in `insert`.
        // SAFETY: head was just allocated with size head_size; we have exclusive
        // access because no other thread can see this arena yet.
        unsafe {
            let bytes = arena.get_bytes_mut(head, head_size);
            bytes.fill(0);
            #[expect(
                clippy::indexing_slicing,
                reason = "OFF_HEIGHT (11) < head_size (104) by construction"
            )]
            {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "MAX_HEIGHT = 20, fits in u8"
                )]
                {
                    bytes[OFF_HEIGHT as usize] = MAX_HEIGHT as u8;
                }
            }
        }

        // Seed PRNG with an address-derived non-zero value.
        let seed = {
            let p = (&raw const arena) as u64;
            if p == 0 { 0xDEAD_BEEF } else { p }
        };

        let is_lexicographic = comparator.is_lexicographic();
        Self {
            arena,
            values: ValueStore::new(),
            comparator,
            is_lexicographic,
            head,
            height: AtomicUsize::new(1),
            height_seed: seed,
            tail_hint: AtomicU32::new(UNSET),
            #[cfg(feature = "metrics")]
            cas_retries: AtomicUsize::new(0),
            #[cfg(test)]
            seek_hook: std::sync::Mutex::new(None),
            #[cfg(all(test, feature = "metrics"))]
            link_hook: std::sync::Mutex::new(None),
        }
    }

    /// Inserts whose link was retried because another writer changed the
    /// predecessor first, counted once per retried level.
    #[cfg(feature = "metrics")]
    pub(crate) fn cas_retries(&self) -> usize {
        self.cas_retries.load(Ordering::Relaxed)
    }

    /// Test-only: arms `hook` to run once inside the next insert, between its
    /// splice search and its first link.
    #[cfg(all(test, feature = "metrics"))]
    pub(crate) fn set_link_hook(&self, hook: SeekHook) {
        *self
            .link_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(hook);
    }

    /// Test-only: runs the armed link hook, if any, once.
    #[cfg(all(test, feature = "metrics"))]
    fn run_link_hook(&self) {
        let hook = self
            .link_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(hook) = hook {
            hook(self);
        }
    }

    /// Test-only: arms `hook` to run once inside the next seek, between its
    /// level-0 comparison and its return.
    #[cfg(test)]
    pub(crate) fn set_seek_hook(&self, hook: SeekHook) {
        *self
            .seek_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(hook);
    }

    /// Test-only: runs the armed seek hook, if any, once.
    #[cfg(test)]
    fn run_seek_hook(&self) {
        let hook = self
            .seek_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(hook) = hook {
            hook(self);
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Inserts a key-value pair into the skiplist.
    ///
    /// Multiple entries with the same `user_key` but different `seqno` are
    /// expected (MVCC).  No deduplication is performed.
    pub(crate) fn insert(&self, key: &InternalKey, value: UserValue) {
        self.insert_with_kv_digest(key, value, None);
    }

    /// Inserts a key-value pair, optionally storing a 4-byte per-KV digest
    /// computed at insert (`KvChecksumComputePoint::AtInsert`).
    ///
    /// `kv_digest` is `Some((digest, algo))` where `digest` is the low 32 bits
    /// of the entry's logical-content digest under the 4-byte `algo` (the
    /// algorithm is stored per node so a later config change cannot misverify
    /// this entry), or `None` for a plain insert. When present the digest and
    /// algorithm tag are stored in the node and it is flagged for flush-time
    /// verification via [`Self::verify_kv_digests`]; when absent the node is
    /// byte-identical to a plain insert.
    #[expect(
        clippy::indexing_slicing,
        reason = "preds/succs are [u32; MAX_HEIGHT]; level < height <= MAX_HEIGHT"
    )]
    pub(crate) fn insert_with_kv_digest(
        &self,
        key: &InternalKey,
        value: UserValue,
        kv_digest: Option<(u32, ChecksumAlgorithm)>,
    ) {
        let value_idx = self.values.reserve();
        let height = node_height(self.height_seed, value_idx);
        let node = self.alloc_node(key, value, value_idx, height, kv_digest);

        // Raise the list height if needed.
        let mut list_h = self.height.load(Ordering::Relaxed);
        while height > list_h {
            match self.height.compare_exchange_weak(
                list_h,
                height,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(h) => list_h = h,
            }
        }

        // Find predecessors and link the node at each level.
        let mut preds = [self.head; MAX_HEIGHT];
        let mut succs = [UNSET; MAX_HEIGHT];

        // Sequential-insert fast path. A height-1 node links at level 0 only,
        // and a key that sorts after the current tail (the hinted node has no
        // successor and compares Less) belongs right behind it: `preds[0]` is
        // the hint, `succs[0]` is UNSET, and the full top-down splice search
        // (about one node walk per level from the head) is skipped. Both
        // facts are re-checked here and the level-0 CAS below still guards
        // against a concurrent append: if another writer took the tail slot
        // first, the CAS fails and the regular per-level re-search runs.
        // Taller nodes need predecessors on every level and take the full
        // search; they are one insert in four.
        let hint = self.tail_hint.load(Ordering::Relaxed);
        let appended_at_tail = height == 1
            && hint != UNSET
            && self.next_at(hint, 0) == UNSET
            && self.compare_key(hint, key) == CmpOrdering::Less;
        if appended_at_tail {
            preds[0] = hint;
        } else {
            self.find_splice(key, &mut preds, &mut succs);
        }
        #[cfg(all(test, feature = "metrics"))]
        self.run_link_hook();

        for level in 0..height {
            loop {
                // SAFETY: `node` was allocated with `height` levels and
                // `level < height`, so `tower_atomic(node, level)` is within
                // the node's arena allocation.
                // new_node.next[level] = succs[level]
                unsafe {
                    self.tower_atomic(node, level)
                        .store(succs[level], Ordering::Release);
                }

                // SAFETY: `preds[level]` is a valid node established by
                // `find_splice` — either the head sentinel (MAX_HEIGHT levels)
                // or a previously inserted node with height > level.
                // CAS pred.next[level] from succs[level] to new_node
                let pred_next = unsafe { self.tower_atomic(preds[level], level) };
                if pred_next
                    .compare_exchange_weak(succs[level], node, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
                #[cfg(feature = "metrics")]
                self.cas_retries.fetch_add(1, Ordering::Relaxed);
                // Predecessor changed — re-search at this level.
                self.find_splice_for_level(key, &mut preds, &mut succs, level);
            }
        }

        // `succs[0]` is the level-0 successor the node was actually linked
        // before (the CAS loop refreshes it on a re-search), so UNSET means
        // this node is the new tail: publish it as the next append's hint.
        if succs[0] == UNSET {
            self.tail_hint.store(node, Ordering::Relaxed);
        }
    }

    /// Returns the number of entries, an insert in progress included: each
    /// holds one value index from before its node is linked.
    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    /// Test-only: flips a bit in the first node's user-key bytes, simulating a
    /// RAM bit-flip during memtable residence. Used to exercise
    /// [`Self::verify_kv_digests`] end-to-end through the flush path.
    #[cfg(test)]
    pub(crate) fn test_flip_first_key_byte(&self) {
        let node = self.first_node();
        assert_ne!(node, UNSET, "skiplist must be non-empty to corrupt");
        let off = self.node_key_offset(node);
        // SAFETY: `off` is the first node's just-allocated key region (len >= 1).
        unsafe {
            if let Some(b) = self.arena.get_bytes_mut(off, 1).first_mut() {
                *b ^= 0xFF;
            }
        }
    }

    /// Test-only: corrupts the first node's `value_type` low bits to an invalid
    /// discriminant (0b111), keeping the presence + algorithm bits intact.
    /// Simulates a RAM bit-flip in the `value_type` byte so the residence
    /// verifier can be checked to fail closed instead of panicking.
    #[cfg(test)]
    #[expect(
        clippy::indexing_slicing,
        reason = "node metadata is exactly OFF_TOWER bytes; index 10 is the value_type byte"
    )]
    pub(crate) fn test_corrupt_first_node_value_type(&self) {
        let node = self.first_node();
        assert_ne!(node, UNSET, "skiplist must be non-empty to corrupt");
        // SAFETY: `node` is a valid allocated node; its metadata is OFF_TOWER
        // bytes, so index 10 (the value_type byte) is in bounds.
        unsafe {
            let m = self.arena.get_bytes_mut(node, OFF_TOWER);
            m[10] = (m[10] & !VALUE_TYPE_MASK) | VALUE_TYPE_MASK; // low bits = 0b111 (invalid)
        }
    }

    /// Returns `true` if the skiplist is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Verifies every node carrying an insert-time per-KV digest
    /// (`KvChecksumComputePoint::AtInsert`) against a recompute over its
    /// current bytes, each under the algorithm the node was stored with.
    ///
    /// A divergence means the entry's logical content changed while it sat in
    /// the memtable, i.e. a RAM bit-flip during residence; it is reported as
    /// [`crate::Error::MemtableKvChecksumMismatch`] with the diverging entry's
    /// seqno. Nodes without a stored digest (inserted under `Off` /
    /// `AtBlockCompile`, including those before an `Off -> AtInsert` toggle in
    /// the same memtable) are skipped. A skiplist with no insert digests at all
    /// walks the nodes and returns `Ok` immediately.
    ///
    /// # Errors
    ///
    /// - [`crate::Error::MemtableKvChecksumMismatch`] when a stored digest does
    ///   not match the recompute.
    /// - [`crate::Error::FeatureUnsupported`] when a node's algorithm is not
    ///   compiled into this build (cannot recompute). Config validation rejects
    ///   selecting an uncompiled algorithm, so this is a defensive guard.
    pub(crate) fn verify_kv_digests(&self) -> crate::Result<()> {
        let mut node = self.first_node();
        while node != UNSET {
            match self.node_kv_digest(node) {
                NodeKvDigest::Absent => {}
                NodeKvDigest::Present(stored, algo) => {
                    // Decode the key fallibly: a corrupt value_type on a
                    // digest-bearing node must fail closed here, not panic.
                    let item = InternalValue::new(
                        self.node_internal_key_checked(node)?,
                        self.node_value(node),
                    );
                    let recomputed = crate::table::block::kv_checksum::kv_digest(&item, algo)
                        .ok_or(crate::Error::FeatureUnsupported("kv-checksum-algorithm"))?;
                    // AtInsert stores the low 32 bits; truncate the recompute the
                    // same way so the comparison is width-consistent.
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "AtInsert uses a 4-byte algorithm; only the low 32 bits are stored"
                    )]
                    let got = recomputed as u32;
                    if got != stored {
                        return Err(crate::Error::MemtableKvChecksumMismatch {
                            seqno: item.key.seqno,
                            got: u64::from(got),
                            expected: u64::from(stored),
                        });
                    }
                }
                NodeKvDigest::CorruptAlgorithm(tag) => {
                    // A present digest under an algorithm AtInsert never stores:
                    // the algorithm metadata was corrupted in RAM. Refuse to
                    // verify under it instead of risking a flipped tag passing.
                    return Err(crate::Error::MemtableKvChecksumCorruptAlgorithm {
                        seqno: self.node_seqno(node),
                        tag,
                    });
                }
            }
            node = self.next_at(node, 0);
        }
        Ok(())
    }

    /// Returns an iterator over all entries in order.
    pub(crate) fn iter(&self) -> Iter<'_> {
        Iter {
            map: self,
            front: self.first_node(),
            back: UNSET,
            back_init: false,
            batch: None,
            done: false,
        }
    }

    /// Returns an iterator over entries within the given range.
    pub(crate) fn range<R: RangeBounds<InternalKey>>(&self, range: R) -> Range<'_> {
        let front = match range.start_bound() {
            Bound::Included(k) => self.seek_ge(k),
            Bound::Excluded(k) => self.seek_gt(k),
            Bound::Unbounded => self.first_node(),
        };

        let end_bound = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        Range {
            map: self,
            end_bound,
            front,
            back: UNSET,
            back_init: false,
            batch: None,
            done: false,
        }
    }

    // -----------------------------------------------------------------------
    // Internal: node allocation
    // -----------------------------------------------------------------------

    /// Allocates and initialises a node in the arena, returning its offset.
    ///
    /// Key data is stored in the arena for comparison locality.
    /// Value data is appended to the heap-backed `values` Vec.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "key_bytes.len() <= u16::MAX, value idx <= u32::MAX, height <= MAX_HEIGHT (20)"
    )]
    fn alloc_node(
        &self,
        key: &InternalKey,
        value: UserValue,
        value_idx: u32,
        height: usize,
        kv_digest: Option<(u32, ChecksumAlgorithm)>,
    ) -> u32 {
        let key_bytes: &[u8] = &key.user_key;

        // One arena allocation carries the node header, the tower AND the key
        // bytes (key right after the tower). Two separate allocations would
        // cost a second CAS loop on the bump cursor per insert, and would let
        // the key land a block away from the metadata the search reads right
        // before it — with a typical height of 1-2 the key starts at +28/+32,
        // on the same cache line as the header `compare_key` just loaded.
        let n_size = node_size(height);
        #[expect(
            clippy::expect_used,
            reason = "arena capacity is fixed; exhaustion is fatal"
        )]
        let node = self
            .arena
            .alloc(n_size + key_bytes.len() as u32, 4)
            .expect("arena exhausted (node)");
        let key_offset = node + n_size;
        if !key_bytes.is_empty() {
            // SAFETY: key_offset..+len is within the allocation made above;
            // exclusive access before publish.
            unsafe {
                self.arena
                    .get_bytes_mut(key_offset, key_bytes.len() as u32)
                    .copy_from_slice(key_bytes);
            }
        }

        // Store value in the lock-free segmented store.
        // SAFETY: `value_idx` was reserved by this insert and is written once.
        unsafe { self.values.write(value_idx, value) };

        // Write immutable metadata using direct byte offsets matching the
        // node layout comment above.  The arena guarantees 24+ bytes at `node`.
        //
        // SAFETY: node was just allocated with size >= OFF_TOWER (24 bytes);
        // exclusive access before publish.
        #[expect(
            clippy::indexing_slicing,
            reason = "meta is exactly OFF_TOWER (24) bytes by construction"
        )]
        unsafe {
            let meta = self.arena.get_bytes_mut(node, OFF_TOWER);
            meta[0..4].copy_from_slice(&key_offset.to_ne_bytes());
            meta[4..8].copy_from_slice(&value_idx.to_ne_bytes());
            // Cast is safe: InternalKey::new() asserts key.len() <= u16::MAX.
            meta[8..10].copy_from_slice(&(key_bytes.len() as u16).to_ne_bytes());
            // ValueType discriminant in the low bits; high bit (KV_DIGEST_PRESENT)
            // flags an insert-time per-KV digest in the reserved slot below.
            let vt_byte = u8::from(key.value_type);
            // Arena memory is uninitialized — the reserved padding at +12 is
            // either the digest (when present) or zeroed, so the whole header
            // is fully written before the node is published. The algorithm's
            // wire_tag is packed into bits 5..=6 of the value_type byte so the
            // digest is verified per node under its own algorithm.
            if let Some((d, algo)) = kv_digest {
                meta[10] = vt_byte | KV_DIGEST_PRESENT | (algo.wire_tag() << KV_ALGO_SHIFT);
                meta[12..16].copy_from_slice(&d.to_ne_bytes());
            } else {
                meta[10] = vt_byte;
                meta[12..16].copy_from_slice(&[0u8; 4]);
            }
            meta[11] = height as u8;
            meta[16..24].copy_from_slice(&key.seqno.to_ne_bytes());
            // Tower slots [0, height) are NOT initialized here: `insert` writes
            // every one of them (release store) before linking the node at that
            // level, so no reader observes an unwritten slot even though the
            // arena did not zero them.
        }

        node
    }

    // -----------------------------------------------------------------------
    // Internal: reading node fields
    // -----------------------------------------------------------------------

    /// Reads the immutable metadata header of a node (24 bytes at `node`).
    ///
    /// # Safety
    ///
    /// `node` must be a valid node offset previously returned by `alloc_node`.
    unsafe fn meta(&self, node: u32) -> &[u8] {
        unsafe { self.arena.get_bytes(node, OFF_TOWER) }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "metadata is exactly OFF_TOWER (24) bytes by construction"
    )]
    #[expect(
        clippy::expect_used,
        reason = "infallible: 4-byte slice always converts to [u8; 4]"
    )]
    fn node_key_offset(&self, node: u32) -> u32 {
        let m = unsafe { self.meta(node) };
        u32::from_ne_bytes(m[0..4].try_into().expect("4 bytes"))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "metadata is exactly OFF_TOWER (24) bytes by construction"
    )]
    #[expect(
        clippy::expect_used,
        reason = "infallible: 2-byte slice always converts to [u8; 2]"
    )]
    fn node_key_len(&self, node: u32) -> u16 {
        let m = unsafe { self.meta(node) };
        u16::from_ne_bytes(m[8..10].try_into().expect("2 bytes"))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "metadata is exactly OFF_TOWER (24) bytes by construction"
    )]
    #[expect(
        clippy::expect_used,
        reason = "ValueType discriminant written during alloc_node is always valid"
    )]
    fn node_value_type(&self, node: u32) -> ValueType {
        let m = unsafe { self.meta(node) };
        // Mask off the KV_DIGEST_PRESENT flag (high bit): the low bits hold the
        // ValueType discriminant, the high bit is the insert-digest presence
        // flag and is not part of the type.
        let byte = m[10] & VALUE_TYPE_MASK;
        debug_assert!(
            ValueType::try_from(byte).is_ok(),
            "invalid ValueType byte {byte} at node offset {node}, meta={m:?}",
        );
        ValueType::try_from(byte).expect("valid ValueType discriminant")
    }

    /// Reads a node's insert-time per-KV digest slot, distinguishing "no
    /// digest" from "digest present but its algorithm metadata is corrupt".
    ///
    /// The algorithm is recovered from the `wire_tag` packed in bits 5..=6 of
    /// the `value_type` byte, so each node verifies under its own algorithm
    /// regardless of later config changes. `AtInsert` only ever stores a 4-byte
    /// algorithm tag, so a present-digest node tagged with a non-4-byte or
    /// unknown algorithm is metadata corruption (e.g. a single-bit flip in the
    /// algorithm bits): reported as [`NodeKvDigest::CorruptAlgorithm`] rather
    /// than silently verified under the wrong algorithm, which could let a
    /// flipped tag pass the residence check.
    #[expect(
        clippy::indexing_slicing,
        reason = "metadata is exactly OFF_TOWER (24) bytes by construction"
    )]
    #[expect(
        clippy::expect_used,
        reason = "infallible: 4-byte slice always converts to [u8; 4]"
    )]
    fn node_kv_digest(&self, node: u32) -> NodeKvDigest {
        // SAFETY: `node` is a published skiplist node; its metadata header
        // [0..OFF_TOWER) was fully written in `alloc_node` before the node was
        // linked (CAS with Release), so the read is valid for the map's lifetime.
        let m = unsafe { self.meta(node) };
        if m[10] & KV_DIGEST_PRESENT == 0 {
            return NodeKvDigest::Absent;
        }
        let algo_tag = (m[10] & KV_ALGO_MASK) >> KV_ALGO_SHIFT;
        match ChecksumAlgorithm::from_wire_tag(algo_tag) {
            Some(algo) if algo.digest_size() == 4 => {
                let digest = u32::from_ne_bytes(m[12..16].try_into().expect("4 bytes"));
                NodeKvDigest::Present(digest, algo)
            }
            // Unknown tag, or a known-but-non-4-byte algorithm (Xxh3_64): a
            // present-digest node never legitimately carries one, so this is
            // corruption of the algorithm bits or the presence bit.
            _ => NodeKvDigest::CorruptAlgorithm(algo_tag),
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "metadata is exactly OFF_TOWER (24) bytes by construction"
    )]
    #[expect(
        clippy::expect_used,
        reason = "infallible: 4-byte slice always converts to [u8; 4]"
    )]
    fn node_value_idx(&self, node: u32) -> u32 {
        let m = unsafe { self.meta(node) };
        u32::from_ne_bytes(m[4..8].try_into().expect("4 bytes"))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "metadata is exactly OFF_TOWER (24) bytes by construction"
    )]
    #[expect(
        clippy::expect_used,
        reason = "infallible: 8-byte slice always converts to [u8; 8]"
    )]
    fn node_seqno(&self, node: u32) -> SeqNo {
        let m = unsafe { self.meta(node) };
        u64::from_ne_bytes(m[16..24].try_into().expect("8 bytes"))
    }

    /// Returns the raw `user_key` bytes stored in the arena for `node`.
    fn node_user_key_bytes(&self, node: u32) -> &[u8] {
        let off = self.node_key_offset(node);
        let len = u32::from(self.node_key_len(node));
        // SAFETY: `node` is reachable via skiplist links only after publication
        // (CAS with Release), so its metadata (key_offset, key_len) was fully
        // written during alloc_node.  The arena block backing off..off+len is
        // never freed while the SkipMap lives.
        unsafe { self.arena.get_bytes(off, len) }
    }

    /// Reconstructs the [`InternalKey`] for `node`.
    ///
    /// Zero-copy on the default `Slice` backend: the user key is a refcounted
    /// view into the arena block that stores it (inline for short keys), so no
    /// allocation or byte copy happens per reconstructed key. The `bytes`
    /// backend cannot view foreign allocations and copies, as before.
    fn node_internal_key(&self, node: u32) -> InternalKey {
        #[cfg(not(feature = "bytes_1"))]
        // SAFETY: key_offset/key_len were written during alloc_node and the
        // node is only reachable after its publishing CAS (happens-before);
        // published key bytes are never rewritten.
        let user_key: UserKey = UserKey::from_view(unsafe {
            self.arena.get_view(
                self.node_key_offset(node),
                u32::from(self.node_key_len(node)),
            )
        });
        #[cfg(feature = "bytes_1")]
        let user_key: UserKey = self.node_user_key_bytes(node).into();
        let seqno = self.node_seqno(node);
        let vt = self.node_value_type(node);
        InternalKey {
            user_key,
            seqno,
            value_type: vt,
        }
    }

    /// Like [`Self::node_internal_key`] but returns a typed error instead of
    /// panicking when the node's `value_type` bits are not a valid discriminant.
    ///
    /// Used on the residence-verify path ([`Self::verify_kv_digests`]) so a node
    /// whose metadata was corrupted in RAM fails closed with
    /// [`crate::Error::InvalidTag`] rather than aborting the process via the
    /// `expect` in [`Self::node_value_type`].
    #[expect(
        clippy::indexing_slicing,
        reason = "metadata is exactly OFF_TOWER (24) bytes by construction"
    )]
    fn node_internal_key_checked(&self, node: u32) -> crate::Result<InternalKey> {
        // SAFETY: `node` is reached via skiplist links only after publication,
        // so its metadata header is fully initialized; reading the value_type
        // byte (index 10, within [0..OFF_TOWER)) is valid.
        let vt_byte = unsafe { self.meta(node) }[10] & VALUE_TYPE_MASK;
        let value_type = ValueType::try_from(vt_byte)
            .map_err(|()| crate::Error::InvalidTag(("memtable-value-type", vt_byte)))?;
        Ok(InternalKey {
            user_key: self.node_user_key_bytes(node).into(),
            seqno: self.node_seqno(node),
            value_type,
        })
    }

    /// Reads the value for `node` from the lock-free value store (wait-free).
    fn node_value(&self, node: u32) -> UserValue {
        // SAFETY: node_value_idx was set during alloc_node→ValueStore::append,
        // and this node is only reachable after the skiplist CAS that published
        // it (establishing happens-before for the value write).
        unsafe { self.values.get(self.node_value_idx(node)) }
    }

    // -----------------------------------------------------------------------
    // Internal: tower access
    // -----------------------------------------------------------------------

    /// Returns a reference to the `AtomicU32` next-pointer at `level` for `node`.
    ///
    /// # Safety
    ///
    /// `level` must be < the node's height.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "level < MAX_HEIGHT (20), fits in u32"
    )]
    unsafe fn tower_atomic(&self, node: u32, level: usize) -> &core::sync::atomic::AtomicU32 {
        // SAFETY: caller guarantees level < node height; node + OFF_TOWER + level*4
        // is within the node's arena allocation and 4-byte aligned.
        unsafe {
            self.arena
                .get_atomic_u32(node + OFF_TOWER + (level as u32) * 4)
        }
    }

    /// Loads the next-pointer at `level` for `node`.
    /// Returns UNSET (0) if no next node.
    fn next_at(&self, node: u32, level: usize) -> u32 {
        // SAFETY: next_at is only called with levels within the node's height
        // or the head sentinel's MAX_HEIGHT.
        unsafe { self.tower_atomic(node, level).load(Ordering::Acquire) }
    }

    /// The first data node (`head.next[0]`), or UNSET if empty.
    fn first_node(&self) -> u32 {
        self.next_at(self.head, 0)
    }

    // -----------------------------------------------------------------------
    // Internal: key comparison
    // -----------------------------------------------------------------------

    /// Compares the key stored at `node` with `target` using the pluggable
    /// `UserComparator` for `user_key` ordering, then seqno DESC.
    #[inline]
    fn compare_key(&self, node: u32, target: &InternalKey) -> CmpOrdering {
        self.compare_key_parts(node, &target.user_key, target.seqno)
    }

    /// [`Self::compare_key`] against a borrowed `(user_key, seqno)` pair, so
    /// point lookups can search without materializing an [`InternalKey`]
    /// (which would heap-copy the probe key on the slice backend).
    #[expect(
        clippy::indexing_slicing,
        reason = "metadata is exactly OFF_TOWER (24) bytes by construction"
    )]
    #[expect(
        clippy::expect_used,
        reason = "infallible: fixed-width slices always convert to arrays"
    )]
    fn compare_key_parts(&self, node: u32, target_uk: &[u8], target_seqno: SeqNo) -> CmpOrdering {
        // One metadata read serves key offset, key length AND (on the equal
        // branch) the seqno: every field split into its own accessor would
        // re-resolve the arena block pointer (an extra Acquire load + branch)
        // per field, and this comparison runs O(log n) times per insert/lookup.
        // SAFETY: `node` is reachable via skiplist links only after its
        // publishing CAS, so the metadata header is fully written.
        let m = unsafe { self.meta(node) };
        let key_off = u32::from_ne_bytes(m[0..4].try_into().expect("4 bytes"));
        let key_len = u32::from(u16::from_ne_bytes(m[8..10].try_into().expect("2 bytes")));
        // SAFETY: key_offset/key_len were written during alloc_node; the key
        // bytes are immutable once the node is published.
        let node_uk = unsafe { self.arena.get_bytes(key_off, key_len) };

        // Hot path: for the default byte-ordering comparator, compare the user
        // keys with a direct `slice::cmp` (inlines to `memcmp`) instead of the
        // `dyn` vtable call. This search visits O(log n) nodes per insert/lookup
        // and is the dominant memtable-write cost, so removing the indirect call
        // per visit matters. Custom comparators still go through the trait.
        let uk_ord = if self.is_lexicographic {
            node_uk.cmp(target_uk)
        } else {
            self.comparator.compare(node_uk, target_uk)
        };

        match uk_ord {
            CmpOrdering::Equal => {
                // Reverse seqno: higher seqno sorts first.
                let node_seq = u64::from_ne_bytes(m[16..24].try_into().expect("8 bytes"));
                target_seqno.cmp(&node_seq)
            }
            other => other,
        }
    }

    /// Compares two nodes by key without allocating (reads raw arena bytes).
    ///
    /// Ordering: `(user_key via comparator, Reverse(seqno))`.  `value_type` is
    /// intentionally excluded — it is not part of `InternalKey::Ord` or
    /// [`InternalKey::compare_with`], and `(user_key, seqno)` is unique per entry.
    #[expect(
        clippy::indexing_slicing,
        reason = "metadata is exactly OFF_TOWER (24) bytes by construction"
    )]
    #[expect(
        clippy::expect_used,
        reason = "infallible: fixed-width slices always convert to arrays"
    )]
    fn compare_nodes(&self, a: u32, b: u32) -> CmpOrdering {
        // Single metadata read per node — see `compare_key` for why the fields
        // are not read through the per-field accessors here.
        // SAFETY: both nodes are published skiplist nodes; their metadata
        // headers were fully written before their publishing CAS.
        let (ma, mb) = unsafe { (self.meta(a), self.meta(b)) };
        let a_off = u32::from_ne_bytes(ma[0..4].try_into().expect("4 bytes"));
        let a_len = u32::from(u16::from_ne_bytes(ma[8..10].try_into().expect("2 bytes")));
        let b_off = u32::from_ne_bytes(mb[0..4].try_into().expect("4 bytes"));
        let b_len = u32::from(u16::from_ne_bytes(mb[8..10].try_into().expect("2 bytes")));
        // SAFETY: key spans were written during alloc_node and are immutable
        // once the nodes are published.
        let (a_uk, b_uk) = unsafe {
            (
                self.arena.get_bytes(a_off, a_len),
                self.arena.get_bytes(b_off, b_len),
            )
        };
        // Same direct-`slice::cmp` fast path as `compare_key` for the default
        // comparator (avoids the `dyn` vtable call per comparison).
        let uk_ord = if self.is_lexicographic {
            a_uk.cmp(b_uk)
        } else {
            self.comparator.compare(a_uk, b_uk)
        };
        match uk_ord {
            CmpOrdering::Equal => {
                let a_seq = u64::from_ne_bytes(ma[16..24].try_into().expect("8 bytes"));
                let b_seq = u64::from_ne_bytes(mb[16..24].try_into().expect("8 bytes"));
                b_seq.cmp(&a_seq) // reverse seqno
            }
            other => other,
        }
    }

    // -----------------------------------------------------------------------
    // Internal: search helpers
    // -----------------------------------------------------------------------

    /// Populates `preds` and `succs` arrays with the splice point for `key`.
    #[expect(clippy::indexing_slicing, reason = "level < list_h <= MAX_HEIGHT")]
    fn find_splice(
        &self,
        key: &InternalKey,
        preds: &mut [u32; MAX_HEIGHT],
        succs: &mut [u32; MAX_HEIGHT],
    ) {
        let list_h = self.height.load(Ordering::Acquire);
        let mut node = self.head;

        for level in (0..list_h).rev() {
            // Track the successor from the comparison loop — do NOT re-read
            // from the list, as a concurrent insert could return a node that
            // sorts before our key, leading to an out-of-order CAS.
            let mut next = self.next_at(node, level);
            while next != UNSET && self.compare_key(next, key) == CmpOrdering::Less {
                node = next;
                next = self.next_at(node, level);
            }
            preds[level] = node;
            succs[level] = next;
        }
    }

    /// Re-searches at a single `level` starting from the stored predecessor
    /// (or a higher-level predecessor as fallback).
    #[expect(
        clippy::indexing_slicing,
        reason = "level < MAX_HEIGHT; preds/succs are [u32; MAX_HEIGHT]"
    )]
    fn find_splice_for_level(
        &self,
        key: &InternalKey,
        preds: &mut [u32; MAX_HEIGHT],
        succs: &mut [u32; MAX_HEIGHT],
        level: usize,
    ) {
        // Re-search from the head sentinel (which has MAX_HEIGHT levels).
        // We cannot start from preds[level+1] because that node's tower
        // height may be only level+2, making higher-level reads OOB.
        // Starting from head is safe and still O(log n) via the walk-down.
        let mut node = self.head;
        let list_h = self.height.load(Ordering::Acquire);

        // Walk down from the list height, narrowing the search at each level.
        // Every node reached via next_at(node, lv) was linked at level lv,
        // so its height > lv — tower reads are always in-bounds.
        for lv in (level + 1..list_h).rev() {
            let mut next = self.next_at(node, lv);
            while next != UNSET && self.compare_key(next, key) == CmpOrdering::Less {
                node = next;
                next = self.next_at(node, lv);
            }
        }

        // Final search at the target level.
        let mut next = self.next_at(node, level);
        while next != UNSET && self.compare_key(next, key) == CmpOrdering::Less {
            node = next;
            next = self.next_at(node, level);
        }

        preds[level] = node;
        succs[level] = next;
    }

    /// Finds the first node whose key >= `target`, or UNSET.
    #[inline]
    fn seek_ge(&self, target: &InternalKey) -> u32 {
        self.seek_ge_parts(&target.user_key, target.seqno)
    }

    /// [`Self::seek_ge`] against a borrowed `(user_key, seqno)` pair.
    fn seek_ge_parts(&self, target_uk: &[u8], target_seqno: SeqNo) -> u32 {
        let mut node = self.head;
        let mut next = UNSET;
        let list_h = self.height.load(Ordering::Acquire);

        for level in (0..list_h).rev() {
            loop {
                next = self.next_at(node, level);
                if next == UNSET {
                    break;
                }
                if self.compare_key_parts(next, target_uk, target_seqno) == CmpOrdering::Less {
                    node = next;
                } else {
                    break;
                }
            }
        }

        #[cfg(test)]
        self.run_seek_hook();
        // The level-0 successor this search compared (UNSET at the end of the
        // list), as `find_splice` keeps it. Re-reading `node`'s successor could
        // return a node a concurrent insert linked after the comparison: one
        // below the target, or a newer version of it. That insert is concurrent
        // with the seek and not part of what it observed.
        next
    }

    /// Point lookup: the entry for `key` with the highest seqno `<= max_seqno`,
    /// or `None` if no version of `key` is visible.
    ///
    /// Searches with borrowed `(key, seqno)` parts — no [`InternalKey`] bound is
    /// materialized, so the probe key is never copied and nothing allocates.
    /// The found entry's key IS materialized, as a zero-copy view on the slice
    /// backend (see [`Self::node_internal_key`]).
    pub(crate) fn point_get(&self, key: &[u8], max_seqno: SeqNo) -> Option<InternalValue> {
        let node = self.seek_ge_parts(key, max_seqno);
        if node == UNSET {
            return None;
        }
        // Key identity is byte equality (see `comparator::same_user_key`), so
        // no comparator dispatch is needed to reject a different key.
        if !crate::comparator::same_user_key(self.node_user_key_bytes(node), key) {
            return None;
        }
        Some(InternalValue {
            key: self.node_internal_key(node),
            value: self.node_value(node),
        })
    }

    /// Finds the first node whose key > `target`, or UNSET.
    fn seek_gt(&self, target: &InternalKey) -> u32 {
        let mut node = self.head;
        let mut next = UNSET;
        let list_h = self.height.load(Ordering::Acquire);

        for level in (0..list_h).rev() {
            loop {
                next = self.next_at(node, level);
                if next == UNSET {
                    break;
                }
                if self.compare_key(next, target) == CmpOrdering::Greater {
                    break;
                }
                node = next;
            }
        }

        #[cfg(test)]
        self.run_seek_hook();
        // The compared level-0 successor, for the reason `seek_ge_parts` gives:
        // a reload could return a node equal to the exclusive bound.
        next
    }

    /// Finds the first node of a user key after `user_key`, or UNSET.
    ///
    /// Versions sort by descending seqno and seqno 0 is the lowest, so the
    /// first node after `(user_key, 0)` is past every version of it, one at
    /// seqno 0 included.
    fn seek_past_user_key(&self, user_key: &[u8]) -> u32 {
        let mut node = self.head;
        let mut next = UNSET;
        let list_h = self.height.load(Ordering::Acquire);

        for level in (0..list_h).rev() {
            loop {
                next = self.next_at(node, level);
                if next == UNSET {
                    break;
                }
                if self.compare_key_parts(next, user_key, 0) == CmpOrdering::Greater {
                    break;
                }
                node = next;
            }
        }

        // The compared level-0 successor, for the reason `seek_ge_parts` gives.
        next
    }

    /// Finds the last node whose key <= `target`, or UNSET if all nodes > target.
    fn seek_le(&self, target: &InternalKey) -> u32 {
        let mut node = self.head;
        let list_h = self.height.load(Ordering::Acquire);

        for level in (0..list_h).rev() {
            loop {
                let next = self.next_at(node, level);
                if next == UNSET {
                    break;
                }
                if self.compare_key(next, target) == CmpOrdering::Greater {
                    break;
                }
                node = next;
            }
        }

        if node == self.head { UNSET } else { node }
    }

    /// Finds the last node whose key < `target`, or UNSET.
    fn seek_lt(&self, target: &InternalKey) -> u32 {
        let mut node = self.head;
        let list_h = self.height.load(Ordering::Acquire);

        for level in (0..list_h).rev() {
            loop {
                let next = self.next_at(node, level);
                if next == UNSET {
                    break;
                }
                if self.compare_key(next, target) == CmpOrdering::Less {
                    node = next;
                } else {
                    break;
                }
            }
        }

        if node == self.head { UNSET } else { node }
    }

    /// Returns the last node in the skiplist, or UNSET if empty.
    fn last_node(&self) -> u32 {
        let mut node = self.head;
        let list_h = self.height.load(Ordering::Acquire);

        for level in (0..list_h).rev() {
            loop {
                let next = self.next_at(node, level);
                if next == UNSET {
                    break;
                }
                node = next;
            }
        }

        if node == self.head { UNSET } else { node }
    }

    /// Fills `batch` with the nodes right before `target_node`, the closest
    /// [`BACK_BATCH`] of them at most, so the next backward steps pop them
    /// instead of searching from the top once per node.
    ///
    /// The search stops at level [`BACK_BATCH_LEVEL`] on the last node before
    /// `target_node` and walks level 0 from there: a level-2 gap holds about
    /// 16 nodes, so one search pays for that many steps. An empty batch means
    /// `target_node` is the first node.
    fn fill_back_batch(&self, target_node: u32, batch: &mut BackBatch) {
        batch.clear();
        let mut node = self.head;
        let list_h = self.height.load(Ordering::Acquire);

        for level in (BACK_BATCH_LEVEL..list_h).rev() {
            loop {
                let next = self.next_at(node, level);
                if next == UNSET || next == target_node {
                    break;
                }
                if self.compare_nodes(next, target_node) == CmpOrdering::Less {
                    node = next;
                } else {
                    break;
                }
            }
        }

        // `node` sorts before `target_node` and is itself a predecessor unless
        // it is the head. `target_node` is linked at level 0, so the walk
        // reaches it; a node linked meanwhile in between is batched like any
        // other.
        if node != self.head {
            batch.push(node);
        }
        let mut next = self.next_at(node, 0);
        while next != UNSET && next != target_node {
            batch.push(next);
            next = self.next_at(next, 0);
        }
    }
}

/// The tower height of the node holding value `value_idx`: geometric with
/// P = 1/4, from the splitmix64 finaliser over `seed + value_idx`. The index
/// is unique per insert, so the heights are as independent as a shared counter
/// would make them, without one.
fn node_height(seed: u64, value_idx: u32) -> usize {
    let mut z = seed
        .wrapping_add(u64::from(value_idx))
        .wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;

    // Each pair of trailing zero bits adds one level.
    let tz = z.trailing_zeros() as usize;
    (1 + tz / 2).min(MAX_HEIGHT)
}

// ---------------------------------------------------------------------------
// Entry reference
// ---------------------------------------------------------------------------

/// A reference to a key-value pair stored in the skiplist arena.
pub(crate) struct Entry<'a> {
    map: &'a SkipMap,
    node: u32,
}

impl Entry<'_> {
    /// Reconstructs the [`InternalKey`] (allocates a new `Slice` for `user_key`).
    pub(crate) fn key(&self) -> InternalKey {
        self.map.node_internal_key(self.node)
    }

    /// Reconstructs the value (allocates a new `Slice`).
    pub(crate) fn value(&self) -> UserValue {
        self.map.node_value(self.node)
    }
}

// ---------------------------------------------------------------------------
// Back batch
// ---------------------------------------------------------------------------

/// The nodes just before an iterator's back end, in ascending order: a ring
/// that keeps the last [`BACK_BATCH`] pushed, popped from the high end.
struct BackBatch {
    nodes: [u32; BACK_BATCH],
    start: usize,
    len: usize,
}

impl BackBatch {
    const fn new() -> Self {
        Self {
            nodes: [UNSET; BACK_BATCH],
            start: 0,
            len: 0,
        }
    }

    fn clear(&mut self) {
        self.start = 0;
        self.len = 0;
    }

    /// Appends `node`, dropping the lowest one once the ring is full.
    #[expect(
        clippy::indexing_slicing,
        reason = "masked with BACK_BATCH - 1, a power of two"
    )]
    fn push(&mut self, node: u32) {
        self.nodes[(self.start + self.len) & (BACK_BATCH - 1)] = node;
        if self.len == BACK_BATCH {
            self.start = (self.start + 1) & (BACK_BATCH - 1);
        } else {
            self.len += 1;
        }
    }

    /// Takes the highest node, or `None` once the batch is used up.
    #[expect(
        clippy::indexing_slicing,
        reason = "masked with BACK_BATCH - 1, a power of two"
    )]
    fn pop(&mut self) -> Option<u32> {
        let len = self.len.checked_sub(1)?;
        self.len = len;
        Some(self.nodes[(self.start + len) & (BACK_BATCH - 1)])
    }
}

impl SkipMap {
    /// The node before `node`, taken from `batch` and refilled from the list
    /// when it runs out; UNSET when `node` is the first node. The batch is
    /// allocated on the first backward step, so a forward-only iterator
    /// carries no ring.
    fn step_back(&self, node: u32, batch: &mut Option<alloc::boxed::Box<BackBatch>>) -> u32 {
        let batch = batch.get_or_insert_with(|| alloc::boxed::Box::new(BackBatch::new()));
        if let Some(prev) = batch.pop() {
            return prev;
        }
        self.fill_back_batch(node, batch);
        batch.pop().unwrap_or(UNSET)
    }

    // The two ends of an iterator meet by key order, not only by reaching the
    // same node: a node linked meanwhile between nodes the back has batched is
    // not in the batch, so the front can step onto it, past the back's next
    // node, and the back can step below the front. Either end stops there
    // instead of handing out what the other already did.

    /// Whether the front's next `node` lies past the back's next node (or the
    /// back has handed out everything down to the start).
    fn front_passed_back(&self, node: u32, back: u32) -> bool {
        back == UNSET || self.compare_nodes(node, back) == CmpOrdering::Greater
    }

    /// Whether the back's next `node` lies before the front's next node.
    fn back_passed_front(&self, node: u32, front: u32) -> bool {
        front != UNSET && self.compare_nodes(node, front) == CmpOrdering::Less
    }
}

// ---------------------------------------------------------------------------
// Full iterator
// ---------------------------------------------------------------------------

/// Forward + backward iterator over all entries in a [`SkipMap`].
pub(crate) struct Iter<'a> {
    map: &'a SkipMap,
    front: u32,
    /// The node the next backward step hands out.
    back: u32,
    back_init: bool,
    /// The nodes below `back`, batched ahead of the backward steps.
    batch: Option<alloc::boxed::Box<BackBatch>>,
    done: bool,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.front == UNSET {
            return None;
        }

        let node = self.front;

        if self.back_init && self.map.front_passed_back(node, self.back) {
            self.front = UNSET;
            self.done = true;
            return None;
        }

        // If front and back have converged, this is the last element.
        if self.back_init && node == self.back {
            self.done = true;
        } else {
            self.front = self.map.next_at(node, 0);
            if self.front == UNSET {
                self.done = true;
            }
        }

        Some(Entry {
            map: self.map,
            node,
        })
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if !self.back_init {
            self.back = self.map.last_node();
            self.back_init = true;
        }

        if self.back == UNSET || self.map.back_passed_front(self.back, self.front) {
            self.done = true;
            return None;
        }

        let node = self.back;

        // If front and back have converged, this is the last element.
        if node == self.front {
            self.done = true;
        } else {
            self.back = self.map.step_back(node, &mut self.batch);
        }

        Some(Entry {
            map: self.map,
            node,
        })
    }
}

// ---------------------------------------------------------------------------
// Range iterator
// ---------------------------------------------------------------------------

/// Forward + backward iterator over a range of entries in a [`SkipMap`].
pub(crate) struct Range<'a> {
    map: &'a SkipMap,
    end_bound: Bound<InternalKey>,
    front: u32,
    /// The node the next backward step hands out.
    back: u32,
    back_init: bool,
    /// The nodes below `back`, batched ahead of the backward steps.
    batch: Option<alloc::boxed::Box<BackBatch>>,
    done: bool,
}

impl Range<'_> {
    /// Returns `true` if `node` is within the end bound.
    fn within_end(&self, node: u32) -> bool {
        match &self.end_bound {
            Bound::Unbounded => true,
            Bound::Included(k) => self.map.compare_key(node, k) != CmpOrdering::Greater,
            Bound::Excluded(k) => self.map.compare_key(node, k) == CmpOrdering::Less,
        }
    }

    /// Moves the front past every remaining version of `user_key` with one
    /// search from the top of the list, rather than node by node. The front
    /// holds no node of an earlier key (the caller resolved `user_key` from
    /// the merge), so the search lands at or after it, never back.
    pub(crate) fn skip_front_past(&mut self, user_key: &[u8]) {
        if self.done || self.front == UNSET {
            return;
        }
        let next = self.map.seek_past_user_key(user_key);
        // The back has not yet handed out what lies up to it, so a search that
        // lands past it leaves nothing for either end.
        let passes_back = self.back_init
            && (self.back == UNSET
                || next == UNSET
                || self.map.compare_nodes(next, self.back) == CmpOrdering::Greater);
        if next == UNSET || passes_back {
            self.front = UNSET;
            self.done = true;
        } else {
            self.front = next;
        }
    }

    /// Moves the back up to the newest version of `user_key` below `seqno`
    /// with one search from the top of the list; whether the next backward
    /// step yields it. Only a back standing on an older version moves, and
    /// never past the front.
    pub(crate) fn skip_back_to_newest(&mut self, user_key: &[u8], seqno: SeqNo) -> bool {
        if self.done || !self.back_init || self.back == UNSET {
            return false;
        }
        // Versions sort by descending seqno: the first node at or after
        // `(user_key, seqno - 1)` is the newest one below `seqno`.
        let Some(newest_seqno) = seqno.checked_sub(1) else {
            return false;
        };
        let node = self.map.seek_ge_parts(user_key, newest_seqno);
        if node == UNSET
            || !crate::comparator::same_user_key(self.map.node_user_key_bytes(node), user_key)
            || self.map.compare_nodes(node, self.back) == CmpOrdering::Greater
            || self.map.back_passed_front(node, self.front)
        {
            return false;
        }
        self.back = node;
        // The batch held the nodes below the old back; the new back's are
        // fetched on the next step.
        if let Some(batch) = &mut self.batch {
            batch.clear();
        }
        true
    }
}

impl<'a> Iterator for Range<'a> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.front == UNSET {
            return None;
        }

        let node = self.front;

        // Check end bound, and the back once it has started.
        if !self.within_end(node) || (self.back_init && self.map.front_passed_back(node, self.back))
        {
            self.front = UNSET;
            self.done = true;
            return None;
        }

        // If front and back have converged, this is the last element.
        if self.back_init && node == self.back {
            self.done = true;
        } else {
            self.front = self.map.next_at(node, 0);
            if self.front == UNSET {
                self.done = true;
            }
        }

        Some(Entry {
            map: self.map,
            node,
        })
    }
}

impl DoubleEndedIterator for Range<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if !self.back_init {
            self.back = match &self.end_bound {
                Bound::Unbounded => self.map.last_node(),
                Bound::Included(k) => self.map.seek_le(k),
                Bound::Excluded(k) => self.map.seek_lt(k),
            };
            self.back_init = true;
        }

        if self.back == UNSET || self.front == UNSET {
            self.done = true;
            return None;
        }

        // Back before front in key order: the range is empty (e.g. start bound
        // > end bound), or the back stepped below the front.
        if self.map.back_passed_front(self.back, self.front) {
            self.done = true;
            return None;
        }

        let node = self.back;

        // If front and back have converged, this is the last element.
        if node == self.front {
            self.done = true;
        } else {
            self.back = self.map.step_back(node, &mut self.batch);
        }

        Some(Entry {
            map: self.map,
            node,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::expect_used,
    clippy::doc_markdown,
    clippy::cast_sign_loss,
    reason = "tests use unwrap/indexing/expect for brevity"
)]
mod tests;
