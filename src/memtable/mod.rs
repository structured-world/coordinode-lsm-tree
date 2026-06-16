// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

pub mod arena;
pub mod interval_tree;
pub mod skiplist;
pub mod value_store;

use crate::comparator::SharedComparator;
use crate::key::InternalKey;
use crate::range_tombstone::RangeTombstone;
use crate::{
    UserKey, ValueType,
    value::{InternalValue, SeqNo},
};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::ops::RangeBounds;
use core::sync::atomic::{AtomicBool, AtomicU8};
use portable_atomic::AtomicU64;
// `parking_lot::RwLock` (std: small, userspace fast-path, no poisoning) /
// `spin::RwLock` (no_std). Neither poisons on a panicked holder, so the read/
// write guards are taken without a `LockResult` unwrap.
#[cfg(feature = "std")]
use parking_lot::RwLock;
#[cfg(not(feature = "std"))]
use spin::RwLock;

pub use crate::tree::inner::MemtableId;

/// The memtable serves as an intermediary, ephemeral, sorted storage for new items
///
/// When the Memtable exceeds some size, it should be flushed to a table.
pub struct Memtable {
    #[doc(hidden)]
    pub id: MemtableId,

    /// The user key comparator used for ordering entries.
    pub(crate) comparator: SharedComparator,

    /// The actual content, stored in an arena-based skiplist with lock-free traversal.
    ///
    /// Nodes are allocated from a contiguous byte arena for cache locality
    /// and O(1) bulk deallocation when the memtable is dropped.  Traversal of
    /// the skiplist index uses atomic loads and CAS for inserts.
    pub(crate) items: skiplist::SkipMap,

    /// Range tombstones stored in an interval tree.
    ///
    /// Protected by `RwLock` — read-heavy suppression queries (`query_suppression`,
    /// `range_tombstones_sorted`) take a shared read lock, while `insert_range_tombstone`
    /// takes an exclusive write lock. After a rotation has been requested via
    /// `requested_rotation`, the interval tree is treated as read-only by convention,
    /// and only readers are expected to access this field (the `RwLock` is still used
    /// for synchronization, but there should be no further writes).
    ///
    /// `std::sync::RwLock` may be reader-biased on some platforms, but writer
    /// starvation is not a concern here: range deletes are rare, the write-side
    /// critical section is O(log n) with n typically small, and the memtable
    /// rotates (becoming read-only) well before contention could accumulate.
    pub(crate) range_tombstones: RwLock<interval_tree::IntervalTree>,

    /// Approximate active memtable size.
    ///
    /// If this grows too large, a flush is triggered.
    pub(crate) approximate_size: AtomicU64,

    /// Highest encountered sequence number.
    ///
    /// This is used so that `get_highest_seqno` has O(1) complexity.
    pub(crate) highest_seqno: AtomicU64,

    pub(crate) requested_rotation: AtomicBool,

    /// Algorithm of the insert-time per-KV digests stored in this memtable's
    /// nodes (`KvChecksumComputePoint::AtInsert`), as `1 + wire_tag`, or `0`
    /// when no `AtInsert` digest has been inserted. Set (idempotently) on the
    /// first digest-bearing insert and read once at flush by
    /// [`Self::verify_kv_residence`]. `0` means there is nothing to verify, so
    /// the default (`Off` / `AtBlockCompile`) path never walks the nodes.
    kv_insert_algo: AtomicU8,
}

impl Memtable {
    /// Returns the memtable ID.
    pub fn id(&self) -> MemtableId {
        self.id
    }

    /// Returns `true` if the memtable was already flagged for rotation.
    pub fn is_flagged_for_rotation(&self) -> bool {
        self.requested_rotation
            .load(core::sync::atomic::Ordering::Relaxed)
    }

    /// Flags the memtable as requested for rotation.
    pub fn flag_rotated(&self) {
        self.requested_rotation
            .store(true, core::sync::atomic::Ordering::Relaxed);
    }

    // `pub` + `#[doc(hidden)]`: used by the host crate (fjall) to construct
    // ephemeral memtables. Not part of the semver-stable API.
    // Keep the comparator by-value for hidden-public API compatibility while
    // still requiring callers to pass the tree comparator explicitly.
    #[doc(hidden)]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "hidden-public constructor keeps the preexisting by-value signature for compatibility"
    )]
    #[must_use]
    pub fn new(id: MemtableId, comparator: SharedComparator) -> Self {
        Self {
            id,
            items: skiplist::SkipMap::new(comparator.clone()),
            comparator: comparator.clone(),
            range_tombstones: RwLock::new(interval_tree::IntervalTree::new_with_comparator(
                comparator.clone(),
            )),
            approximate_size: AtomicU64::default(),
            highest_seqno: AtomicU64::default(),
            requested_rotation: AtomicBool::default(),
            kv_insert_algo: AtomicU8::new(0),
        }
    }

    /// Creates an iterator over all items.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = InternalValue> + '_ {
        self.items.iter().map(|entry| InternalValue {
            key: entry.key(),
            value: entry.value(),
        })
    }

    /// Creates an iterator over a range of items.
    ///
    /// Accepts `InternalKey`-based bounds.
    pub(crate) fn range_internal<'a, R: RangeBounds<InternalKey> + 'a>(
        &'a self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = InternalValue> + 'a {
        self.items.range(range).map(|entry| InternalValue {
            key: entry.key(),
            value: entry.value(),
        })
    }

    /// Returns the item by key if it exists.
    ///
    /// Returns the version with the highest seqno that is strictly less than
    /// the given `seqno`.  Pass [`MAX_SEQNO`](crate::MAX_SEQNO) to retrieve the latest version.
    #[doc(hidden)]
    pub fn get(&self, key: &[u8], seqno: SeqNo) -> Option<InternalValue> {
        if seqno == 0 {
            return None;
        }

        // NOTE: This range start deserves some explanation...
        // InternalKeys are multi-sorted by 2 categories: user_key and Reverse(seqno). (tombstone doesn't really matter)
        // We search for the lowest entry that is greater or equal the user's prefix key
        // and has the seqno (or lower) we want (because the seqno is stored in reverse order)
        //
        // Example: We search for "abc"
        //
        // key -> seqno
        //
        // a   -> 7
        // abc -> 5 <<< This is the lowest key (highest seqno) that matches the key with seqno=MAX
        // abc -> 4
        // abc -> 3 <<< If searching for abc and seqno=4, we would get this
        // abcdef -> 6
        // abcdef -> 5
        //
        let lower_bound = InternalKey::new(key, seqno - 1, ValueType::Value);

        let cmp = self.comparator.as_ref();

        let mut iter = self.items.range(lower_bound..).take_while(|entry| {
            cmp.compare(entry.user_key_bytes(), key) == core::cmp::Ordering::Equal
        });

        iter.next().map(|entry| InternalValue {
            key: entry.key(),
            value: entry.value(),
        })
    }

    /// Gets approximate size of memtable in bytes.
    pub fn size(&self) -> u64 {
        self.approximate_size
            .load(core::sync::atomic::Ordering::Acquire)
    }

    /// Counts the number of items in the memtable.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns `true` if the memtable has no KV items and no range tombstones.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty() && self.range_tombstone_count() == 0
    }

    /// Inserts multiple items into the memtable in bulk.
    ///
    /// More efficient than calling [`Memtable::insert`] in a loop because it
    /// performs a single `fetch_add` for the total size and a single
    /// `fetch_max` for the highest seqno.
    ///
    /// Returns `(total_bytes_added, new_memtable_size)`.
    #[doc(hidden)]
    pub fn insert_batch(&self, items: Vec<InternalValue>) -> (u64, u64) {
        self.insert_batch_with_kv_algo(items, None)
    }

    /// Bulk insert, optionally computing an insert-time per-KV digest per item
    /// under `kv_algo` (`KvChecksumComputePoint::AtInsert`).
    ///
    /// `kv_algo` is `Some(algo)` (a 4-byte algorithm) to fix each entry's
    /// digest at insert for the flush-time residence check, or `None` for the
    /// plain bulk path. Same single-`fetch_add` / single-`fetch_max` accounting
    /// as [`Self::insert_batch`].
    #[doc(hidden)]
    pub fn insert_batch_with_kv_algo(
        &self,
        items: Vec<InternalValue>,
        kv_algo: Option<crate::runtime_config::ChecksumAlgorithm>,
    ) -> (u64, u64) {
        if items.is_empty() {
            let size = self
                .approximate_size
                .load(core::sync::atomic::Ordering::Acquire);
            return (0, size);
        }

        let mut total_size: u64 = 0;
        let mut max_seqno: u64 = 0;

        let overhead =
            core::mem::size_of::<InternalValue>() + core::mem::size_of::<SharedComparator>();

        for item in &items {
            #[expect(
                clippy::expect_used,
                reason = "keys are limited to 16-bit length + values are limited to 32-bit length"
            )]
            let item_size: u64 = (item.key.user_key.len() + item.value.len() + overhead)
                .try_into()
                .expect("should fit into u64");

            total_size = total_size.saturating_add(item_size);

            if item.key.seqno > max_seqno {
                max_seqno = item.key.seqno;
            }
        }

        let size_before = self
            .approximate_size
            .fetch_add(total_size, core::sync::atomic::Ordering::AcqRel);

        if let Some(algo) = kv_algo {
            // Record the algorithm once (idempotent) for the flush-time verify.
            self.kv_insert_algo
                .store(1 + algo.wire_tag(), core::sync::atomic::Ordering::Relaxed);
        }

        for item in items {
            let digest = kv_algo.and_then(|algo| {
                crate::table::block::kv_checksum::kv_digest(&item, algo).map(|d| {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "AtInsert is config-validated to a 4-byte algorithm; the digest fits u32"
                    )]
                    let lo = d as u32;
                    lo
                })
            });
            let key = InternalKey::new(item.key.user_key, item.key.seqno, item.key.value_type);
            self.items.insert_with_kv_digest(&key, &item.value, digest);
        }

        self.highest_seqno
            .fetch_max(max_seqno, core::sync::atomic::Ordering::AcqRel);

        // fetch_add returns value BEFORE the add, so size_before + total_size
        // = value AFTER add = new memtable size. Same pattern as Memtable::insert().
        (total_size, size_before + total_size)
    }

    /// Inserts an item into the memtable
    #[doc(hidden)]
    pub fn insert(&self, item: InternalValue) -> (u64, u64) {
        #[expect(
            clippy::expect_used,
            reason = "keys are limited to 16-bit length + values are limited to 32-bit length"
        )]
        // Account for MemtableKey overhead (InternalKey + Arc<dyn UserComparator>)
        let item_size = (item.key.user_key.len()
            + item.value.len()
            + core::mem::size_of::<InternalValue>()
            + core::mem::size_of::<SharedComparator>())
        .try_into()
        .expect("should fit into u64");

        let size_before = self
            .approximate_size
            .fetch_add(item_size, core::sync::atomic::Ordering::AcqRel);

        let key = InternalKey::new(item.key.user_key, item.key.seqno, item.key.value_type);
        self.items.insert(&key, &item.value);

        self.highest_seqno
            .fetch_max(item.key.seqno, core::sync::atomic::Ordering::AcqRel);

        (item_size, size_before + item_size)
    }

    /// Inserts an item, optionally carrying a precomputed insert-time per-KV
    /// digest (`KvChecksumComputePoint::AtInsert`).
    ///
    /// `kv_digest` is `Some((digest, algo))` when the caller computed the
    /// entry's 4-byte logical-content digest at insert (under `AtInsert` with a
    /// 4-byte algorithm), or `None` for the plain path. When present, the
    /// digest is stored in the skiplist node and the memtable records `algo`
    /// so [`Self::verify_kv_residence`] can re-check every digest-bearing node
    /// at flush. Mixed inserts (some with, some without a digest) are supported
    /// for the `Off` -> `AtInsert` live toggle.
    #[doc(hidden)]
    pub fn insert_with_kv_digest(
        &self,
        item: InternalValue,
        kv_digest: Option<(u32, crate::runtime_config::ChecksumAlgorithm)>,
    ) -> (u64, u64) {
        #[expect(
            clippy::expect_used,
            reason = "keys are limited to 16-bit length + values are limited to 32-bit length"
        )]
        let item_size = (item.key.user_key.len()
            + item.value.len()
            + core::mem::size_of::<InternalValue>()
            + core::mem::size_of::<SharedComparator>())
        .try_into()
        .expect("should fit into u64");

        let size_before = self
            .approximate_size
            .fetch_add(item_size, core::sync::atomic::Ordering::AcqRel);

        if let Some((_, algo)) = kv_digest {
            // Record the algorithm (idempotent) so the flush-time verify knows
            // how to recompute. `1 + wire_tag` keeps `0` meaning "no AtInsert
            // digest written" (so the default path never walks nodes).
            self.kv_insert_algo
                .store(1 + algo.wire_tag(), core::sync::atomic::Ordering::Relaxed);
        }

        let key = InternalKey::new(item.key.user_key, item.key.seqno, item.key.value_type);
        self.items
            .insert_with_kv_digest(&key, &item.value, kv_digest.map(|(d, _)| d));

        self.highest_seqno
            .fetch_max(item.key.seqno, core::sync::atomic::Ordering::AcqRel);

        (item_size, size_before + item_size)
    }

    /// Verifies every insert-time per-KV digest in this memtable against a
    /// recompute over the entry's current bytes (the
    /// [`KvChecksumComputePoint::AtInsert`](crate::runtime_config::KvChecksumComputePoint::AtInsert)
    /// residence check), called once at flush.
    ///
    /// Returns `Ok` immediately when no `AtInsert` digest was ever inserted (the
    /// recorded algorithm is `0`), so the default path pays nothing.
    ///
    /// # Errors
    ///
    /// - [`crate::Error::MemtableKvChecksumMismatch`] when an entry's stored
    ///   digest diverges from the recompute (a RAM bit-flip during residence).
    /// - [`crate::Error::FeatureUnsupported`] when the recorded algorithm is
    ///   not compiled into this build.
    pub fn verify_kv_residence(&self) -> crate::Result<()> {
        let tag = self
            .kv_insert_algo
            .load(core::sync::atomic::Ordering::Relaxed);
        if tag == 0 {
            return Ok(());
        }
        let algo = crate::runtime_config::ChecksumAlgorithm::from_wire_tag(tag - 1)
            .ok_or(crate::Error::FeatureUnsupported("kv-checksum-algorithm"))?;
        self.items.verify_kv_digests(algo)
    }

    /// Inserts a range tombstone covering `[start, end)` at the given seqno.
    ///
    /// Returns the approximate size added to the memtable.
    ///
    /// Returns 0 if `start >= end` or if either bound exceeds `u16::MAX` bytes.
    ///
    /// # Panics
    ///
    /// Panics if the internal `RwLock` is poisoned.
    #[must_use]
    pub fn insert_range_tombstone(&self, start: UserKey, end: UserKey, seqno: SeqNo) -> u64 {
        // flag_rotated() (which sets requested_rotation) is called by the host
        // crate (fjall) before rotation; this crate never sets it directly.
        // The assert catches misuse by callers
        // in debug builds — intentionally debug-only because post-rotation writes
        // are structurally prevented by the host (sealed memtables are behind Arc
        // with no write path exposed), and an atomic load here would add overhead
        // on the hot insert path in release builds for no practical benefit.
        debug_assert!(
            !self.is_flagged_for_rotation(),
            "insert_range_tombstone called after memtable was flagged for rotation"
        );

        // Reject invalid intervals in release builds (debug_assert is not enough)
        if self.comparator.compare(&start, &end) != core::cmp::Ordering::Less {
            return 0;
        }

        // On-disk RT format writes key lengths as u16, enforce at insertion time.
        // Emit a warning when rejecting an oversized bound so this failure is diagnosable.
        if u16::try_from(start.len()).is_err() || u16::try_from(end.len()).is_err() {
            log::warn!(
                "insert_range_tombstone: rejecting oversized range tombstone \
                 bounds (start_len = {}, end_len = {}, max = {})",
                start.len(),
                end.len(),
                u16::MAX,
            );
            return 0;
        }

        let size = (start.len() + end.len() + core::mem::size_of::<RangeTombstone>()) as u64;

        self.range_tombstones
            .write()
            .insert(RangeTombstone::new(start, end, seqno));

        self.approximate_size
            .fetch_add(size, core::sync::atomic::Ordering::AcqRel);

        self.highest_seqno
            .fetch_max(seqno, core::sync::atomic::Ordering::AcqRel);

        size
    }

    /// Returns `true` if the key at `key_seqno` is suppressed by a range tombstone
    /// visible at `read_seqno`.
    pub(crate) fn is_key_suppressed_by_range_tombstone(
        &self,
        key: &[u8],
        key_seqno: SeqNo,
        read_seqno: SeqNo,
    ) -> bool {
        self.range_tombstones
            .read()
            .query_suppression(key, key_seqno, read_seqno)
    }

    /// Returns all range tombstones in sorted order (for flush).
    pub(crate) fn range_tombstones_sorted(&self) -> Vec<RangeTombstone> {
        self.range_tombstones.read().iter_sorted()
    }

    /// Returns the number of range tombstones.
    #[must_use]
    pub fn range_tombstone_count(&self) -> usize {
        self.range_tombstones.read().len()
    }

    /// Returns the highest sequence number in the memtable.
    pub fn get_highest_seqno(&self) -> Option<SeqNo> {
        if self.is_empty() {
            None
        } else {
            Some(
                self.highest_seqno
                    .load(core::sync::atomic::Ordering::Acquire),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ValueType;
    use crate::comparator::default_comparator;
    use std::sync::{Arc, Barrier};
    use test_log::test;

    fn new_memtable(id: MemtableId) -> Memtable {
        Memtable::new(id, default_comparator())
    }

    /// Low-32 logical digest of an entry under `algo`.
    fn at_insert_digest(
        item: &InternalValue,
        algo: crate::runtime_config::ChecksumAlgorithm,
    ) -> u32 {
        #[expect(
            clippy::cast_possible_truncation,
            clippy::expect_used,
            reason = "4-byte algo fits u32; test helper"
        )]
        let d = crate::table::block::kv_checksum::kv_digest(item, algo)
            .expect("xxh3 always available") as u32;
        d
    }

    #[test]
    fn verify_kv_residence_ok_when_intact() {
        // AtInsert path: every entry carries the digest of its own bytes, so
        // the flush-time residence check passes.
        let algo = crate::runtime_config::ChecksumAlgorithm::Xxh3Low32;
        let mt = new_memtable(0);
        for i in 0..5u8 {
            let item = InternalValue::from_components(
                [b'k', i],
                [b'v', i],
                u64::from(i) + 1,
                ValueType::Value,
            );
            let d = at_insert_digest(&item, algo);
            mt.insert_with_kv_digest(item, Some((d, algo)));
        }
        assert!(mt.verify_kv_residence().is_ok());
    }

    #[test]
    fn verify_kv_residence_ok_without_any_digest() {
        // Default path (no AtInsert digest recorded): nothing to verify, so the
        // residence check returns immediately even with data present.
        let mt = new_memtable(0);
        for i in 0..5u8 {
            mt.insert(InternalValue::from_components(
                [b'k', i],
                [b'v', i],
                u64::from(i) + 1,
                ValueType::Value,
            ));
        }
        assert!(mt.verify_kv_residence().is_ok());
    }

    #[cfg(feature = "crc32c")]
    #[test]
    fn verify_kv_residence_uses_per_node_algorithm_no_drift() {
        // Algorithm drift regression: insert one entry under Xxh3Low32, then a
        // second under Crc32c (as if kv_checksum_algo changed mid-memtable).
        // Both entries are intact, so the residence check must pass. It only
        // passes if each node is verified under the algorithm it was stored
        // with; a single per-memtable algorithm would recompute the first
        // entry under Crc32c and falsely flag it.
        let mt = new_memtable(0);

        let a = InternalValue::from_components(b"aaa", b"va", 1, ValueType::Value);
        let da = at_insert_digest(&a, crate::runtime_config::ChecksumAlgorithm::Xxh3Low32);
        mt.insert_with_kv_digest(
            a,
            Some((da, crate::runtime_config::ChecksumAlgorithm::Xxh3Low32)),
        );

        let b = InternalValue::from_components(b"bbb", b"vb", 2, ValueType::Value);
        let db = at_insert_digest(&b, crate::runtime_config::ChecksumAlgorithm::Crc32c);
        mt.insert_with_kv_digest(
            b,
            Some((db, crate::runtime_config::ChecksumAlgorithm::Crc32c)),
        );

        assert!(
            mt.verify_kv_residence().is_ok(),
            "per-node algorithm must prevent drift across a mid-memtable algo change"
        );
    }

    #[test]
    #[expect(clippy::expect_used, reason = "test asserts the error via expect_err")]
    fn verify_kv_residence_detects_corruption_end_to_end() {
        // Insert under AtInsert, then simulate a RAM bit-flip on the resident
        // entry's key. The flush-time residence check recomputes and reports
        // a MemtableKvChecksumMismatch.
        let algo = crate::runtime_config::ChecksumAlgorithm::Xxh3Low32;
        let mt = new_memtable(0);
        let item = InternalValue::from_components(b"victim", b"payload", 7, ValueType::Value);
        let d = at_insert_digest(&item, algo);
        mt.insert_with_kv_digest(item, Some((d, algo)));

        mt.items.test_flip_first_key_byte();

        let err = mt
            .verify_kv_residence()
            .expect_err("residence corruption must be detected at flush");
        assert!(
            matches!(err, crate::Error::MemtableKvChecksumMismatch { .. }),
            "expected MemtableKvChecksumMismatch, got {err:?}"
        );
    }

    #[test]
    #[expect(
        clippy::expect_used,
        reason = "tests use expect for lock and thread join"
    )]
    fn rwlock_read_while_read_held_succeeds() {
        let mt = new_memtable(0);
        let _ = mt.insert_range_tombstone(b"a".to_vec().into(), b"z".to_vec().into(), 10);

        // Two one-way channels avoid Barrier entirely — if either side
        // panics, the sender drops and recv() returns Err, unblocking the
        // peer so thread::scope can join without hanging.
        let (held_tx, held_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let rt_ref = &mt.range_tombstones;
        std::thread::scope(|s| {
            s.spawn(move || {
                let _guard = rt_ref.read();
                let _ = held_tx.send(()); // signal: guard held
                let _ = release_rx.recv(); // wait: main thread done
            });

            held_rx
                .recv()
                .expect("spawned thread panicked before acquiring guard");
            let guard2 = mt.range_tombstones.try_read();
            assert!(
                guard2.is_some(),
                "second read lock must succeed while first is held"
            );
            drop(guard2);
            drop(release_tx); // signal: done
        });
    }

    #[test]
    #[expect(clippy::expect_used, reason = "tests use expect for thread join")]
    fn suppression_queries_concurrent_readers_no_panic() {
        let mt = Arc::new(new_memtable(0));

        let _ = mt.insert_range_tombstone(b"a".to_vec().into(), b"z".to_vec().into(), 10);
        for i in 0u8..100 {
            let key = vec![b'a' + (i % 25)];
            mt.insert(InternalValue::from_components(
                key,
                b"v".to_vec(),
                u64::from(i),
                ValueType::Value,
            ));
        }

        let handles: Vec<_> = (0..8)
            .map(|t| {
                let mt = Arc::clone(&mt);
                std::thread::spawn(move || {
                    for i in 0u8..200 {
                        let key = vec![b'a' + ((t + i) % 25)];
                        let _ = mt.is_key_suppressed_by_range_tombstone(&key, 5, SeqNo::MAX);
                        let _ = mt.range_tombstone_count();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("reader thread panicked");
        }
    }

    #[test]
    #[expect(clippy::expect_used, reason = "tests use expect for thread join")]
    fn range_tombstones_concurrent_read_write_writers_observable() {
        let mt = Arc::new(new_memtable(0));
        // Barrier ensures all 6 threads start simultaneously.
        let start = Arc::new(Barrier::new(6));

        let _ = mt.insert_range_tombstone(b"a".to_vec().into(), b"m".to_vec().into(), 10);

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let mt = Arc::clone(&mt);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    for _ in 0..500 {
                        let suppressed =
                            mt.is_key_suppressed_by_range_tombstone(b"f", 5, SeqNo::MAX);
                        assert!(
                            suppressed,
                            "key 'f' at seqno=5 must be suppressed by RT [a,m)@10"
                        );
                    }
                })
            })
            .collect();

        let writers: Vec<_> = (0..2)
            .map(|t| {
                let mt = Arc::clone(&mt);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    let start_key: UserKey = b"n".to_vec().into();
                    let end_key: UserKey = b"z".to_vec().into();
                    for i in 0u64..100 {
                        let seqno = 100 + t * 1000 + i;
                        let _ =
                            mt.insert_range_tombstone(start_key.clone(), end_key.clone(), seqno);
                    }
                })
            })
            .collect();

        for h in readers {
            h.join().expect("reader panicked");
        }
        for h in writers {
            h.join().expect("writer panicked");
        }

        // We intentionally do not assert that any reader observed a
        // writer-inserted tombstone mid-loop. `std::sync::RwLock` may be
        // reader-biased, so writers are allowed to be blocked until all
        // readers have finished, which would make such an assertion flaky.
        // Instead, validate post-join visibility: writers insert [n,z) at
        // seqnos starting from 100, so keys in this range must be suppressed.
        assert!(mt.is_key_suppressed_by_range_tombstone(b"n", 50, SeqNo::MAX));
        assert!(mt.is_key_suppressed_by_range_tombstone(b"y", 150, SeqNo::MAX));
    }

    #[test]
    #[expect(clippy::expect_used, reason = "tests use expect for thread join")]
    fn range_tombstones_populated_tree_concurrent_reads_succeed() {
        let mt = Arc::new(new_memtable(0));

        for i in 0u8..50 {
            let start = vec![b'a' + (i % 25)];
            let end = vec![b'a' + (i % 25) + 1];
            let _ = mt.insert_range_tombstone(start.into(), end.into(), u64::from(i));
        }

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let mt = Arc::clone(&mt);
                std::thread::spawn(move || {
                    for _ in 0..500 {
                        let _ = mt.is_key_suppressed_by_range_tombstone(b"c", 5, SeqNo::MAX);
                        let sorted = mt.range_tombstones_sorted();
                        assert!(!sorted.is_empty());
                        let count = mt.range_tombstone_count();
                        assert!(count > 0);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("reader thread panicked");
        }
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn memtable_mvcc_point_read() {
        let memtable = new_memtable(0);

        memtable.insert(InternalValue::from_components(
            *b"hello-key-999991",
            *b"hello-value-999991",
            0,
            ValueType::Value,
        ));

        let item = memtable.get(b"hello-key-99999", SeqNo::MAX);
        assert_eq!(None, item);

        let item = memtable.get(b"hello-key-999991", SeqNo::MAX);
        assert_eq!(*b"hello-value-999991", &*item.unwrap().value);

        memtable.insert(InternalValue::from_components(
            *b"hello-key-999991",
            *b"hello-value-999991-2",
            1,
            ValueType::Value,
        ));

        let item = memtable.get(b"hello-key-99999", SeqNo::MAX);
        assert_eq!(None, item);

        let item = memtable.get(b"hello-key-999991", SeqNo::MAX);
        assert_eq!((*b"hello-value-999991-2"), &*item.unwrap().value);

        let item = memtable.get(b"hello-key-99999", 1);
        assert_eq!(None, item);

        let item = memtable.get(b"hello-key-999991", 1);
        assert_eq!((*b"hello-value-999991"), &*item.unwrap().value);

        let item = memtable.get(b"hello-key-99999", 2);
        assert_eq!(None, item);

        let item = memtable.get(b"hello-key-999991", 2);
        assert_eq!((*b"hello-value-999991-2"), &*item.unwrap().value);
    }

    #[test]
    fn memtable_get() {
        let memtable = new_memtable(0);

        let value =
            InternalValue::from_components(b"abc".to_vec(), b"abc".to_vec(), 0, ValueType::Value);

        memtable.insert(value.clone());

        assert_eq!(Some(value), memtable.get(b"abc", SeqNo::MAX));
    }

    #[test]
    fn memtable_get_highest_seqno() {
        let memtable = new_memtable(0);

        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            0,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            1,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            2,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            3,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            4,
            ValueType::Value,
        ));

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                4,
                ValueType::Value,
            )),
            memtable.get(b"abc", SeqNo::MAX)
        );
    }

    #[test]
    fn memtable_get_prefix() {
        let memtable = new_memtable(0);

        memtable.insert(InternalValue::from_components(
            b"abc0".to_vec(),
            b"abc".to_vec(),
            0,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            255,
            ValueType::Value,
        ));

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                255,
                ValueType::Value,
            )),
            memtable.get(b"abc", SeqNo::MAX)
        );

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc0".to_vec(),
                b"abc".to_vec(),
                0,
                ValueType::Value,
            )),
            memtable.get(b"abc0", SeqNo::MAX)
        );
    }

    #[test]
    fn memtable_get_old_version() {
        let memtable = new_memtable(0);

        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            0,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            99,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            255,
            ValueType::Value,
        ));

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                255,
                ValueType::Value,
            )),
            memtable.get(b"abc", SeqNo::MAX)
        );

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                99,
                ValueType::Value,
            )),
            memtable.get(b"abc", 100)
        );

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                0,
                ValueType::Value,
            )),
            memtable.get(b"abc", 50)
        );
    }
}
