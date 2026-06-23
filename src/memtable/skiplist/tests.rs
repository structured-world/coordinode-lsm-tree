use super::*;
use crate::ValueType;

fn new_map() -> SkipMap {
    SkipMap::new(crate::comparator::default_comparator())
}

fn make_key(user_key: &[u8], seqno: SeqNo) -> InternalKey {
    InternalKey::new(user_key.to_vec(), seqno, ValueType::Value)
}

fn make_value(data: &[u8]) -> UserValue {
    UserValue::from(data)
}

use crate::runtime_config::ChecksumAlgorithm;

/// Low-32 logical digest of an entry under `algo` (the value stored at
/// insert under `KvChecksumComputePoint::AtInsert`).
fn digest4(key: &InternalKey, val: &UserValue, algo: ChecksumAlgorithm) -> u32 {
    let item = InternalValue::new(key.clone(), val.clone());
    #[expect(
        clippy::cast_possible_truncation,
        clippy::expect_used,
        reason = "4-byte algo fits u32; test helper"
    )]
    let d = crate::table::block::kv_checksum::kv_digest(&item, algo).expect("xxh3 always available")
        as u32;
    d
}

#[test]
fn verify_kv_digests_passes_when_uncorrupted() {
    // Every node carries the digest computed over its own bytes, so a
    // recompute at flush matches and verify succeeds.
    let algo = ChecksumAlgorithm::Xxh3Low32;
    let map = new_map();
    for i in 0..8u8 {
        let key = make_key(&[b'k', i], u64::from(i) + 1);
        let val = make_value(&[b'v', i, i, i]);
        map.insert_with_kv_digest(&key, &val, Some((digest4(&key, &val, algo), algo)));
    }
    assert!(map.verify_kv_digests().is_ok());
}

#[test]
#[expect(clippy::expect_used, reason = "test asserts the error via expect_err")]
fn verify_kv_digests_detects_residence_corruption() {
    // Simulate a RAM bit-flip during residence: store the correct digest at
    // insert, then corrupt the entry's key bytes in the arena. The recompute
    // over the corrupted bytes diverges from the stored digest, so verify
    // reports a MemtableKvChecksumMismatch.
    let algo = ChecksumAlgorithm::Xxh3Low32;
    let map = new_map();
    let key = make_key(b"victim", 42);
    let val = make_value(b"payload");
    map.insert_with_kv_digest(&key, &val, Some((digest4(&key, &val, algo), algo)));

    // Flip a bit in the (only) node's user-key bytes, simulating residence
    // corruption.
    map.test_flip_first_key_byte();

    let err = map
        .verify_kv_digests()
        .expect_err("corruption must be detected");
    assert!(
        matches!(err, crate::Error::MemtableKvChecksumMismatch { .. }),
        "expected MemtableKvChecksumMismatch, got {err:?}"
    );
}

#[test]
fn verify_kv_digests_skips_nodes_without_a_digest() {
    // Mixed-variant memtable (Off -> AtInsert toggle): nodes inserted
    // without a digest are never verified, even if their bytes are
    // corrupted. Only digest-bearing nodes are checked.
    let algo = ChecksumAlgorithm::Xxh3Low32;
    let map = new_map();

    // No-digest node (pre-toggle): corrupt it later; must be skipped.
    let k0 = make_key(b"aaa", 1);
    let v0 = make_value(b"no-digest");
    map.insert_with_kv_digest(&k0, &v0, None);

    // Digest-bearing node (post-toggle): left intact.
    let k1 = make_key(b"bbb", 2);
    let v1 = make_value(b"has-digest");
    map.insert_with_kv_digest(&k1, &v1, Some((digest4(&k1, &v1, algo), algo)));

    // Corrupt the FIRST node's key (the no-digest "aaa" entry sorts first).
    map.test_flip_first_key_byte();

    // The corrupted node has no digest, so verify skips it and passes.
    assert!(
        map.verify_kv_digests().is_ok(),
        "nodes without a stored digest must not be verified"
    );
}

#[test]
fn verify_kv_digests_no_digests_is_ok() {
    // A memtable with only plain inserts has nothing to verify.
    let map = new_map();
    for i in 0..4u8 {
        let key = make_key(&[b'x', i], u64::from(i) + 1);
        map.insert(&key, &make_value(b"plain"));
    }
    assert!(map.verify_kv_digests().is_ok());
}

#[test]
#[expect(clippy::expect_used, reason = "test helper computes a known digest")]
fn verify_kv_digests_rejects_non_4_byte_algorithm_tag() {
    // AtInsert only ever stores a 4-byte algorithm. A present-digest node
    // whose algorithm bits decode to a non-4-byte algorithm (e.g. Xxh3_64
    // after a single-bit flip from Xxh3Low32's tag 1 to tag 0) is metadata
    // corruption. Store the Xxh3_64 digest truncated to u32 under the
    // 8-byte Xxh3_64 tag: the worst case where comparing the digest alone
    // would pass. Verification must reject it on the algorithm tag instead.
    let map = new_map();
    let key = make_key(b"k", 1);
    let val = make_value(b"v");
    let item = InternalValue::new(key.clone(), val.clone());
    #[expect(clippy::cast_possible_truncation, reason = "low 32 bits on purpose")]
    let d64 = crate::table::block::kv_checksum::kv_digest(&item, ChecksumAlgorithm::Xxh3_64)
        .expect("xxh3 always available") as u32;
    map.insert_with_kv_digest(&key, &val, Some((d64, ChecksumAlgorithm::Xxh3_64)));

    assert!(
        map.verify_kv_digests().is_err(),
        "a present digest under a non-4-byte algorithm must be rejected, not verified"
    );
}

#[test]
fn verify_kv_digests_fails_closed_on_corrupt_value_type() {
    // A digest-bearing node whose value_type bits were corrupted to an
    // invalid discriminant must surface a typed error from the residence
    // verifier, not panic (the verifier reconstructs the key, which decodes
    // value_type). Fail closed.
    let algo = ChecksumAlgorithm::Xxh3Low32;
    let map = new_map();
    let key = make_key(b"k", 1);
    let val = make_value(b"v");
    map.insert_with_kv_digest(&key, &val, Some((digest4(&key, &val, algo), algo)));

    map.test_corrupt_first_node_value_type();

    assert!(
        map.verify_kv_digests().is_err(),
        "corrupt value_type on a digest-bearing node must fail closed, not panic"
    );
}

#[test]
fn insert_and_get_single() {
    let map = new_map();
    let key = make_key(b"hello", 1);
    let val = make_value(b"world");
    map.insert(&key, &val);

    assert_eq!(map.len(), 1);
    assert!(!map.is_empty());

    let mut iter = map.iter();
    let entry = iter.next().expect("one entry");
    assert_eq!(&*entry.key().user_key, b"hello");
    assert_eq!(entry.key().seqno, 1);
    assert_eq!(&*entry.value(), b"world");
    assert!(iter.next().is_none());
}

#[test]
fn custom_comparator_orders_and_reverse_iterates() {
    // A non-lexicographic comparator (orders user keys in REVERSE byte
    // order) exercises the trait-dispatch (`else`) branch of compare_key
    // (via insert / find_splice) and compare_nodes (via reverse iteration /
    // find_predecessor) — the paths the default lexicographic fast path
    // skips.
    #[derive(Debug)]
    struct ReverseComparator;
    impl crate::comparator::UserComparator for ReverseComparator {
        fn name(&self) -> &'static str {
            "reverse-test"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
            b.cmp(a)
        }
        fn is_lexicographic(&self) -> bool {
            false
        }
    }

    let map = SkipMap::new(alloc::sync::Arc::new(ReverseComparator));
    assert!(
        !map.is_lexicographic,
        "a custom comparator must take the trait-dispatch path"
    );

    map.insert(&make_key(b"aaa", 1), &make_value(b"a"));
    map.insert(&make_key(b"ccc", 1), &make_value(b"c"));
    map.insert(&make_key(b"bbb", 1), &make_value(b"b"));

    // Forward iteration follows the custom (reverse) order.
    let fwd: Vec<Vec<u8>> = map.iter().map(|e| e.key().user_key.to_vec()).collect();
    assert_eq!(
        fwd,
        vec![b"ccc".to_vec(), b"bbb".to_vec(), b"aaa".to_vec()],
        "custom comparator orders entries in reverse byte order"
    );

    // Reverse iteration (drives compare_nodes via find_predecessor) mirrors it.
    let rev: Vec<Vec<u8>> = map
        .iter()
        .rev()
        .map(|e| e.key().user_key.to_vec())
        .collect();
    assert_eq!(
        rev,
        vec![b"aaa".to_vec(), b"bbb".to_vec(), b"ccc".to_vec()],
        "reverse iteration mirrors the custom order"
    );
}

#[test]
fn ordering_user_key_asc_seqno_desc() {
    let map = new_map();

    // Same user_key, different seqnos → should iterate highest seqno first.
    map.insert(&make_key(b"abc", 1), &make_value(b"v1"));
    map.insert(&make_key(b"abc", 3), &make_value(b"v3"));
    map.insert(&make_key(b"abc", 2), &make_value(b"v2"));

    let seqnos: Vec<SeqNo> = map.iter().map(|e| e.key().seqno).collect();
    assert_eq!(seqnos, vec![3, 2, 1]);

    // Different user_keys → ascending.
    map.insert(&make_key(b"zzz", 10), &make_value(b"z"));
    map.insert(&make_key(b"aaa", 10), &make_value(b"a"));

    let keys: Vec<Vec<u8>> = map.iter().map(|e| e.key().user_key.to_vec()).collect();
    assert_eq!(
        keys,
        vec![
            b"aaa".to_vec(),
            b"abc".to_vec(),
            b"abc".to_vec(),
            b"abc".to_vec(),
            b"zzz".to_vec(),
        ]
    );
}

#[test]
fn range_lower_bound() {
    let map = new_map();
    for i in 0u8..10 {
        let key = vec![b'a' + i];
        map.insert(&make_key(&key, 0), &make_value(&[i]));
    }

    // Range from 'e' onwards → e, f, g, h, i, j
    let bound = make_key(b"e", crate::MAX_SEQNO);
    let keys: Vec<u8> = map.range(bound..).map(|e| e.key().user_key[0]).collect();
    assert_eq!(keys, vec![b'e', b'f', b'g', b'h', b'i', b'j']);
}

#[test]
fn range_bounded() {
    let map = new_map();
    for i in 0u8..10 {
        let key = vec![b'a' + i];
        map.insert(&make_key(&key, 0), &make_value(&[i]));
    }

    let lo = make_key(b"c", crate::MAX_SEQNO);
    let hi = make_key(b"f", 0);
    let keys: Vec<u8> = map.range(lo..=hi).map(|e| e.key().user_key[0]).collect();
    assert_eq!(keys, vec![b'c', b'd', b'e', b'f']);
}

#[test]
fn double_ended_iter() {
    let map = new_map();
    for i in 0u8..5 {
        let key = vec![b'a' + i];
        map.insert(&make_key(&key, 0), &make_value(&[i]));
    }

    let mut iter = map.iter();
    assert_eq!(iter.next().unwrap().key().user_key[0], b'a');
    assert_eq!(iter.next_back().unwrap().key().user_key[0], b'e');
    assert_eq!(iter.next().unwrap().key().user_key[0], b'b');
    assert_eq!(iter.next_back().unwrap().key().user_key[0], b'd');
    assert_eq!(iter.next().unwrap().key().user_key[0], b'c');
    assert!(iter.next().is_none());
    assert!(iter.next_back().is_none());
}

#[test]
fn double_ended_range() {
    let map = new_map();
    for i in 0u8..10 {
        let key = vec![b'a' + i];
        map.insert(&make_key(&key, 0), &make_value(&[i]));
    }

    let lo = make_key(b"c", crate::MAX_SEQNO);
    let hi = make_key(b"g", 0);
    let rev: Vec<u8> = map
        .range(lo..=hi)
        .rev()
        .map(|e| e.key().user_key[0])
        .collect();
    assert_eq!(rev, vec![b'g', b'f', b'e', b'd', b'c']);
}

#[test]
fn empty_value() {
    let map = new_map();
    map.insert(&make_key(b"k", 0), &make_value(b""));
    let entry = map.iter().next().unwrap();
    assert!(entry.value().is_empty());
}

#[test]
fn concurrent_inserts() {
    use std::sync::Arc;

    let map = Arc::new(new_map());
    let n_threads = 8;
    let n_per_thread = 1000;

    let handles: Vec<_> = (0..n_threads)
        .map(|t| {
            let map = Arc::clone(&map);
            std::thread::spawn(move || {
                for i in 0..n_per_thread {
                    let key = format!("t{t:02}_k{i:05}");
                    map.insert(&make_key(key.as_bytes(), i as u64), &make_value(b"v"));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    assert_eq!(map.len(), n_threads * n_per_thread);

    // Verify sorted order.
    let entries: Vec<_> = map.iter().collect();
    for pair in entries.windows(2) {
        let a = pair[0].key();
        let b = pair[1].key();
        assert!(a <= b, "out of order: {a:?} > {b:?}");
    }
}

#[test]
fn mvcc_point_lookup_via_range() {
    let map = new_map();

    // Insert 3 versions of "key" at seqnos 1, 2, 3.
    map.insert(&make_key(b"key", 1), &make_value(b"v1"));
    map.insert(&make_key(b"key", 2), &make_value(b"v2"));
    map.insert(&make_key(b"key", 3), &make_value(b"v3"));

    // Memtable MVCC read at read_seqno=3 (visible: seqno <= 2).
    // The memtable uses lower_bound = InternalKey("key", read_seqno - 1).
    // With InternalKey ordering (user_key ASC, seqno DESC), range(("key", 2)..)
    // yields entries starting from seqno=2 downward.
    let lower = InternalKey::new(b"key".to_vec(), 2, ValueType::Value);
    let mut iter = map.range(lower..);
    let entry = iter
        .next()
        .filter(|e| &*e.key().user_key == b"key")
        .expect("should find key");
    assert_eq!(entry.key().seqno, 2);
    assert_eq!(&*entry.value(), b"v2");

    // At read_seqno=2, lower_bound = ("key", 1), yields seqno=1.
    let lower2 = InternalKey::new(b"key".to_vec(), 1, ValueType::Value);
    let entry2 = map
        .range(lower2..)
        .next()
        .filter(|e| &*e.key().user_key == b"key")
        .expect("should find key");
    assert_eq!(entry2.key().seqno, 1);
    assert_eq!(&*entry2.value(), b"v1");

    // At read_seqno=crate::MAX_SEQNO, lower_bound = ("key", MAX-1), yields seqno=3 (latest).
    let lower3 = InternalKey::new(b"key".to_vec(), crate::MAX_SEQNO - 1, ValueType::Value);
    let entry3 = map
        .range(lower3..)
        .next()
        .filter(|e| &*e.key().user_key == b"key")
        .expect("should find key");
    assert_eq!(entry3.key().seqno, 3);
    assert_eq!(&*entry3.value(), b"v3");
}

#[test]
fn empty_iter_next_back() {
    let map = new_map();
    let mut iter = map.iter();
    assert!(iter.next().is_none());
    assert!(iter.next_back().is_none());
}

#[test]
fn empty_range_next_back() {
    let map = new_map();
    let lo = make_key(b"a", crate::MAX_SEQNO);
    let hi = make_key(b"z", 0);
    let mut range = map.range(lo..=hi);
    assert!(range.next().is_none());
    assert!(range.next_back().is_none());
}

#[test]
fn range_excluded_end_next_back() {
    let map = new_map();
    for i in 0u8..5 {
        map.insert(&make_key(&[b'a' + i], 0), &make_value(&[i]));
    }

    // Excluded end "d" → range is [a, d) = a, b, c
    let lo = make_key(b"a", crate::MAX_SEQNO);
    let hi = make_key(b"d", crate::MAX_SEQNO);
    let rev: Vec<u8> = map
        .range(lo..hi)
        .rev()
        .map(|e| e.key().user_key[0])
        .collect();
    assert_eq!(rev, vec![b'c', b'b', b'a']);
}

#[test]
fn seek_le_all_greater_returns_none() {
    let map = new_map();
    map.insert(&make_key(b"m", 0), &make_value(b"v"));

    // All keys > "a", so seek_le("a") returns UNSET → next_back = None
    let hi = make_key(b"a", 0);
    let mut range = map.range(..=hi);
    assert!(range.next_back().is_none());
}

#[test]
fn next_back_on_first_element() {
    let map = new_map();
    map.insert(&make_key(b"only", 0), &make_value(b"v"));

    let mut iter = map.iter();
    // next_back on single-element list
    let entry = iter.next_back().expect("one entry");
    assert_eq!(&*entry.key().user_key, b"only");
    assert!(iter.next().is_none());
    assert!(iter.next_back().is_none());
}

/// Regression test for SIGBUS on aarch64: concurrent inserts + reads
/// caused misaligned AtomicU32 access when a skiplist next-pointer
/// contained a key-data offset (align=1) instead of a node offset
/// (align=4).
///
/// The test stresses concurrent insert + iteration to surface the
/// race.  Prior to the fix, this would SIGBUS on Apple Silicon.
#[test]
fn concurrent_insert_and_iter_no_sigbus() {
    use std::sync::{Arc, Barrier};

    let map = Arc::new(new_map());
    let barrier = Arc::new(Barrier::new(9)); // 8 writers + 1 reader

    // 8 writer threads
    let writers: Vec<_> = (0..8)
        .map(|t| {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..500 {
                    let key = format!("t{t:02}_k{i:04}");
                    map.insert(&make_key(key.as_bytes(), i as u64), &make_value(b"val"));
                }
            })
        })
        .collect();

    // 1 reader thread doing concurrent iteration
    let reader = {
        let map = Arc::clone(&map);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            let mut count = 0u64;
            for _ in 0..100 {
                // Iterate the skiplist while writers are active.
                // This exercises tower_atomic / next_at on every node.
                for entry in map.iter() {
                    let _ = entry.key();
                    let _ = entry.value();
                    count += 1;
                }
            }
            count
        })
    };

    for w in writers {
        w.join().expect("writer panicked");
    }
    let reads = reader.join().expect("reader panicked");

    // Sanity: all entries inserted
    assert_eq!(map.len(), 4000);
    // Reader count may be 0 if writers finished before reader iterated.
    // The key assertion is that no SIGBUS/panic occurred during iteration.
    let _ = reads;
}
