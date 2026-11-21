use super::*;
use crate::ValueType;

fn new_map() -> SkipMap {
    SkipMap::new(crate::comparator::default_comparator())
}

/// Longest key the exhaustive pairs below reach: past two 32-byte runs, so a
/// difference falls in a skipped run, in a word after the runs and in the
/// byte tail.
const LONGEST_EXHAUSTIVE_KEY: usize = 76;

/// The prefix-skipping comparison orders every pair of keys as `<[u8]>::cmp`
/// does and reports their exact common prefix, from every starting point a
/// search can pass (any length up to that prefix): every length up to past
/// two 32-byte runs, a difference at each position on either side of a run
/// and word boundary, a key that is a prefix of the other, a short key
/// against a long one, and pseudo-random pairs behind long shared runs.
/// Guards against a compare that reads the bytes in the wrong order, lets the
/// length decide too early, skips a run that differs, or misreports where two
/// keys part, which would make a later comparison skip bytes it must read.
#[test]
fn the_prefix_skipping_comparison_orders_keys_and_finds_their_common_prefix() {
    let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for len in 0..=LONGEST_EXHAUSTIVE_KEY {
        let base: Vec<u8> = (0..len)
            .map(|i| u8::try_from((i * 37 + 11) % 256).expect("below 256"))
            .collect();
        pairs.push((base.clone(), base.clone()));
        for at in 0..len {
            for delta in [1u8, 0x80, 0xFF] {
                let mut other = base.clone();
                other[at] = other[at].wrapping_add(delta);
                pairs.push((base.clone(), other.clone()));
                pairs.push((other, base.clone()));
            }
        }
        for shorter in 0..len {
            pairs.push((base[..shorter].to_vec(), base.clone()));
            pairs.push((base.clone(), base[..shorter].to_vec()));
        }
    }
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for round in 0..10_000 {
        let mut below = |n: u64| next() % n;
        let (la, lb) = (below(160), below(160));
        // A small alphabet makes long common prefixes likely; every other
        // pair also shares a prefix of two runs and a word.
        let mut a: Vec<u8> = (0..la)
            .map(|_| u8::try_from(below(3)).expect("below 3"))
            .collect();
        let mut b: Vec<u8> = (0..lb)
            .map(|_| u8::try_from(below(3)).expect("below 3"))
            .collect();
        if round % 2 == 0 {
            let shared = [7u8; 72];
            a = [&shared[..], &a].concat();
            b = [&shared[..], &b].concat();
        }
        pairs.push((a, b));
    }
    for (a, b) in pairs {
        let common = a.iter().zip(&b).take_while(|(x, y)| x == y).count();
        for from in 0..=common {
            assert_eq!(
                lexicographic_from(&a, &b, from),
                (a.cmp(&b), common),
                "{a:?} vs {b:?} from {from}"
            );
        }
    }
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
        map.insert_with_kv_digest(
            &key,
            InsertBytes::Borrowed(&val),
            Some((digest4(&key, &val, algo), algo)),
        );
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
    map.insert_with_kv_digest(
        &key,
        InsertBytes::Borrowed(&val),
        Some((digest4(&key, &val, algo), algo)),
    );

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
    // A kept buffer, the other in the arena: both placements are verified.
    map.insert_with_kv_digest(&k0, InsertBytes::Shared(v0), None);

    // Digest-bearing node (post-toggle): left intact.
    let k1 = make_key(b"bbb", 2);
    let v1 = make_value(b"has-digest");
    map.insert_with_kv_digest(
        &k1,
        InsertBytes::Borrowed(&v1),
        Some((digest4(&k1, &v1, algo), algo)),
    );

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
        map.insert(&key, make_value(b"plain"));
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
    map.insert_with_kv_digest(
        &key,
        InsertBytes::Borrowed(&val),
        Some((d64, ChecksumAlgorithm::Xxh3_64)),
    );

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
    map.insert_with_kv_digest(
        &key,
        InsertBytes::Borrowed(&val),
        Some((digest4(&key, &val, algo), algo)),
    );

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
    map.insert(&key, val);

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

    map.insert(&make_key(b"aaa", 1), make_value(b"a"));
    map.insert(&make_key(b"ccc", 1), make_value(b"c"));
    map.insert(&make_key(b"bbb", 1), make_value(b"b"));

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
    map.insert(&make_key(b"abc", 1), make_value(b"v1"));
    map.insert(&make_key(b"abc", 3), make_value(b"v3"));
    map.insert(&make_key(b"abc", 2), make_value(b"v2"));

    let seqnos: Vec<SeqNo> = map.iter().map(|e| e.key().seqno).collect();
    assert_eq!(seqnos, vec![3, 2, 1]);

    // Different user_keys → ascending.
    map.insert(&make_key(b"zzz", 10), make_value(b"z"));
    map.insert(&make_key(b"aaa", 10), make_value(b"a"));

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
        map.insert(&make_key(&key, 0), make_value(&[i]));
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
        map.insert(&make_key(&key, 0), make_value(&[i]));
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
        map.insert(&make_key(&key, 0), make_value(&[i]));
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
        map.insert(&make_key(&key, 0), make_value(&[i]));
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

/// Backward steps over a list far larger than one back batch hand out every
/// node once, in reverse order, for the whole list and for a bounded range.
#[test]
fn backward_steps_over_many_batches_reverse_the_forward_order() {
    let map = new_map();
    for i in 0u32..5_000 {
        map.insert(
            &make_key(&i.to_be_bytes(), u64::from(i % 3)),
            make_value(b"v"),
        );
    }

    let forward: Vec<_> = map.iter().map(|e| e.key()).collect();
    let mut backward: Vec<_> = map.iter().rev().map(|e| e.key()).collect();
    backward.reverse();
    assert_eq!(backward, forward);

    let lo = make_key(&700u32.to_be_bytes(), crate::MAX_SEQNO);
    let hi = make_key(&4_321u32.to_be_bytes(), 0);
    let forward: Vec<_> = map
        .range(lo.clone()..=hi.clone())
        .map(|e| e.key())
        .collect();
    let mut backward: Vec<_> = map.range(lo..=hi).rev().map(|e| e.key()).collect();
    backward.reverse();
    assert_eq!(backward, forward);
    assert_eq!(forward.len(), 4_321 - 700 + 1);
}

/// Both ends taken in turn meet once over a long list: nothing repeated,
/// nothing skipped, wherever the meeting point falls in a back batch.
#[test]
fn alternating_ends_over_many_batches_meet_once() {
    let map = new_map();
    for i in 0u32..3_001 {
        map.insert(&make_key(&i.to_be_bytes(), 0), make_value(b"v"));
    }
    for front_steps in [0usize, 1, 7, 100, 1_499, 1_500, 2_999] {
        let mut iter = map.iter();
        let mut seen = Vec::new();
        for _ in 0..front_steps {
            seen.push(iter.next().unwrap().key());
        }
        while let Some(e) = iter.next_back() {
            seen.push(e.key());
            let Some(e) = iter.next() else { break };
            seen.push(e.key());
        }
        seen.sort();
        let all: Vec<_> = map.iter().map(|e| e.key()).collect();
        assert_eq!(seen, all, "front_steps={front_steps}");
    }
}

/// A node linked between nodes the back end has already batched, while the
/// front is still before them, must not make either end hand a node out
/// twice: the ends meet by key order, not by meeting the same node.
#[test]
fn an_insert_inside_a_back_batch_never_repeats_a_key() {
    for (range, alternate) in [(false, true), (true, true), (false, false), (true, false)] {
        let map = new_map();
        for k in [b"a", b"c", b"e", b"g"] {
            map.insert(&make_key(k, 0), make_value(b"v"));
        }
        let lo = make_key(b"a", crate::MAX_SEQNO);
        let hi = make_key(b"z", 0);
        let mut iter: Box<dyn DoubleEndedIterator<Item = Entry<'_>>> = if range {
            Box::new(map.range(lo..=hi))
        } else {
            Box::new(map.iter())
        };

        let mut seen = vec![iter.next().unwrap().key()];
        seen.push(iter.next_back().unwrap().key());
        map.insert(&make_key(b"d", 0), make_value(b"v"));
        // Front takes "c", back takes "e" and pops "c" as its next node,
        // which the front has already handed out.
        seen.push(iter.next().unwrap().key());
        seen.push(iter.next_back().unwrap().key());
        if alternate {
            while let Some(e) = iter.next() {
                seen.push(e.key());
                let Some(e) = iter.next_back() else { break };
                seen.push(e.key());
            }
        } else {
            // The front alone first: it reaches the inserted "d", then "e",
            // which the back has already handed out.
            seen.extend(iter.by_ref().map(|e| e.key()));
            seen.extend(iter.rev().map(|e| e.key()));
        }

        let mut keys: Vec<_> = seen.iter().map(|k| k.user_key.to_vec()).collect();
        keys.sort();
        let len = keys.len();
        keys.dedup();
        assert_eq!(keys.len(), len, "a key came out twice (range={range})");
        for k in [b"a", b"c", b"e", b"g"] {
            assert!(keys.contains(&k.to_vec()), "{k:?} missing (range={range})");
        }
    }
}

#[test]
fn empty_value() {
    let map = new_map();
    map.insert(&make_key(b"k", 0), make_value(b""));
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
                    map.insert(&make_key(key.as_bytes(), i as u64), make_value(b"v"));
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
    map.insert(&make_key(b"key", 1), make_value(b"v1"));
    map.insert(&make_key(b"key", 2), make_value(b"v2"));
    map.insert(&make_key(b"key", 3), make_value(b"v3"));

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
        map.insert(&make_key(&[b'a' + i], 0), make_value(&[i]));
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
    map.insert(&make_key(b"m", 0), make_value(b"v"));

    // All keys > "a", so seek_le("a") returns UNSET → next_back = None
    let hi = make_key(b"a", 0);
    let mut range = map.range(..=hi);
    assert!(range.next_back().is_none());
}

#[test]
fn next_back_on_first_element() {
    let map = new_map();
    map.insert(&make_key(b"only", 0), make_value(b"v"));

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
                    map.insert(&make_key(key.as_bytes(), i as u64), make_value(b"val"));
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

/// A reconstructed key is a zero-copy view into the arena block; the view's
/// reference must keep the block's bytes alive even after the whole map (and
/// its arena) is dropped. Uses a key longer than the inline threshold so the
/// view actually shares the block, and a second map to give a use-after-free
/// a chance to scribble over recycled memory before the assertion.
#[test]
fn key_view_after_map_drop_remains_valid() {
    let long_key = vec![0xA5u8; 64];
    let map = new_map();
    map.insert(&make_key(&long_key, 1), make_value(b"payload"));

    #[expect(
        clippy::expect_used,
        reason = "test inserts one entry before reading it"
    )]
    let key = map.iter().next().expect("one entry").key();
    drop(map);

    // Recycle allocator pages with a fresh map full of different bytes.
    let other = new_map();
    for i in 0..100u64 {
        other.insert(&make_key(&[0x5Au8; 64], i), make_value(&[0xFFu8; 128]));
    }

    assert_eq!(&*key.user_key, long_key.as_slice());
}

/// The sequential-insert fast path (tail hint) must never change WHERE a
/// node lands: appends take it, everything else (a key below the tail, a
/// newer version of the tail's own key, which sorts BEFORE the older one)
/// must fall back to the full splice search. Interleaves all three and
/// checks the final order is the exact internal-key order with nothing
/// lost.
#[test]
fn tail_hint_keeps_internal_key_order_under_mixed_inserts() {
    let map = new_map();
    let mut expected: Vec<(Vec<u8>, SeqNo)> = Vec::new();

    for i in 0..2_000u64 {
        // Append: strictly increasing key (the hint path for height-1 nodes).
        let key = format!("k{i:06}").into_bytes();
        map.insert(&make_key(&key, 1), make_value(b"v"));
        expected.push((key.clone(), 1));

        if i % 7 == 3 {
            // Newer version of the tail key: same user key, higher seqno.
            // Sorts BEFORE the version just appended, so it is NOT a tail append.
            map.insert(&make_key(&key, 2), make_value(b"v2"));
            expected.push((key, 2));
        }
        if i % 11 == 5 {
            // A key below the current tail (out of order): full search path.
            let below = format!("k{:06}", i / 2).into_bytes();
            map.insert(&make_key(&below, 3), make_value(b"v3"));
            expected.push((below, 3));
        }
    }

    // Internal-key order: user key ascending, seqno DESCENDING within a key.
    expected.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));

    let got: Vec<(Vec<u8>, SeqNo)> = map
        .iter()
        .map(|e| {
            let k = e.key();
            (k.user_key.to_vec(), k.seqno)
        })
        .collect();
    assert_eq!(got.len(), expected.len(), "an insert was lost");
    assert_eq!(
        got, expected,
        "iteration order must be the internal-key order"
    );
    assert_eq!(map.len(), expected.len());
}

/// Arms the seek hook to insert `(user_key, seqno)` into the window between a
/// seek's level-0 comparison and its return, the way a concurrent writer can.
fn insert_during_next_seek(map: &SkipMap, user_key: &'static [u8], seqno: SeqNo) {
    map.set_seek_hook(alloc::boxed::Box::new(move |map: &SkipMap| {
        map.insert(&make_key(user_key, seqno), make_value(b"concurrent"));
    }));
}

/// An insert whose predecessor gains another node between the splice search
/// and the link re-searches and retries, and the retry is counted; both keys
/// end up in order. Guards against a counter that misses the contended path,
/// which would report a skewed writer load as free of contention.
#[cfg(feature = "metrics")]
#[test]
fn an_insert_raced_at_its_splice_counts_a_cas_retry() {
    let map = new_map();
    map.insert(&make_key(b"a", 1), make_value(b"a"));
    map.insert(&make_key(b"c", 1), make_value(b"c"));
    assert_eq!(map.cas_retries(), 0, "no contention yet");
    map.set_link_hook(alloc::boxed::Box::new(|map: &SkipMap| {
        map.insert(&make_key(b"b", 1), make_value(b"concurrent"));
    }));
    map.insert(&make_key(b"bb", 1), make_value(b"bb"));
    assert!(map.cas_retries() >= 1, "the raced link was retried");
    let keys: Vec<Vec<u8>> = map.iter().map(|e| e.key().user_key.to_vec()).collect();
    assert_eq!(keys, [&b"a"[..], b"b", b"bb", b"c"]);
}

/// Inserts no other writer races leave the retry count at zero, whatever the
/// order: the sequential tail append and the full splice search alike.
#[cfg(feature = "metrics")]
#[test]
fn uncontended_inserts_count_no_cas_retry() {
    let map = new_map();
    for i in 0..64u8 {
        map.insert(&make_key(&[i], 1), make_value(b"v"));
    }
    for i in (0..64u8).rev() {
        map.insert(&make_key(&[i], 2), make_value(b"v"));
    }
    assert_eq!(map.cas_retries(), 0);
}

/// `(user key, seqno)` of every entry a range yields.
fn yielded(range: Range<'_>) -> Vec<(Vec<u8>, SeqNo)> {
    range
        .map(|e| {
            let k = e.key();
            (k.user_key.to_vec(), k.seqno)
        })
        .collect()
}

/// A key linked below the lower bound after the seek compared its successor
/// must not be yielded: the seek is the range's only lower-bound check.
#[test]
fn a_range_never_yields_a_key_inserted_below_its_bound_during_the_seek() {
    let map = new_map();
    map.insert(&make_key(b"a", 1), make_value(b"a"));
    map.insert(&make_key(b"d", 1), make_value(b"d"));
    insert_during_next_seek(&map, b"b", 1);
    let got = yielded(map.range(make_key(b"c", SeqNo::MAX)..));
    assert_eq!(got, vec![(b"d".to_vec(), 1)]);
}

/// The same when the compared successor was the end of the list: a key
/// appended below the bound afterwards is not yielded either.
#[test]
fn a_range_past_the_last_key_never_yields_a_key_appended_below_its_bound() {
    let map = new_map();
    map.insert(&make_key(b"a", 1), make_value(b"a"));
    insert_during_next_seek(&map, b"b", 1);
    let got = yielded(map.range(make_key(b"c", SeqNo::MAX)..));
    assert!(got.is_empty(), "yielded {got:?}");
}

/// An exclusive lower bound excludes a key equal to it, even one linked
/// after the seek compared the successor.
#[test]
fn an_exclusive_bound_never_yields_an_equal_key_inserted_during_the_seek() {
    let map = new_map();
    map.insert(&make_key(b"a", 1), make_value(b"a"));
    map.insert(&make_key(b"d", 1), make_value(b"d"));
    insert_during_next_seek(&map, b"c", 1);
    let got = yielded(map.range((Bound::Excluded(make_key(b"c", 1)), Bound::Unbounded)));
    assert_eq!(got, vec![(b"d".to_vec(), 1)]);
}

/// Another key linked before the target during the seek must not hide the
/// target: `point_get` finds the version it compared.
#[test]
fn point_get_finds_the_key_when_another_is_inserted_before_it_during_the_seek() {
    let map = new_map();
    map.insert(&make_key(b"a", 1), make_value(b"a"));
    map.insert(&make_key(b"k", 3), make_value(b"k3"));
    insert_during_next_seek(&map, b"b", 1);
    let got = map.point_get(b"k", 5).map(|v| v.key.seqno);
    assert_eq!(got, Some(3));
}

/// A newer version of the key sorts before the visible one; linked during the
/// seek it must stay invisible to a read below its seqno, including one right
/// at `max_seqno + 1`.
#[test]
fn point_get_never_returns_a_version_above_its_snapshot_inserted_during_the_seek() {
    let got: Vec<(SeqNo, Option<SeqNo>)> = [9, 6]
        .into_iter()
        .map(|newer| {
            let map = new_map();
            map.insert(&make_key(b"a", 1), make_value(b"a"));
            map.insert(&make_key(b"k", 3), make_value(b"k3"));
            insert_during_next_seek(&map, b"k", newer);
            (newer, map.point_get(b"k", 5).map(|v| v.key.seqno))
        })
        .collect();
    assert_eq!(got, vec![(9, Some(3)), (6, Some(3))]);
}

/// A value of every size around the arena's limits reads back exactly as
/// written, by iteration and by point lookup, whether its bytes were copied
/// into the arena or its buffer kept: empty, inline-sized, just past the
/// inline size, and on both sides of the largest value the arena holds.
/// Guards the length word, the offset past the key and the placement choice.
#[test]
fn values_of_every_placement_read_back_as_written() {
    let sizes = [
        0,
        1,
        20,
        21,
        255,
        4096,
        ARENA_VALUE_MAX,
        ARENA_VALUE_MAX + 1,
    ];
    let map = new_map();
    let mut expected = Vec::new();
    for (i, &size) in sizes.iter().enumerate() {
        let value: Vec<u8> = (0..size)
            .map(|b| u8::try_from((b * 31 + i) % 251).expect("below 251"))
            .collect();
        // Keys of odd lengths put the length word at every alignment.
        let borrowed = vec![b'b'; i + 1];
        let shared = vec![b's'; i + 1];
        map.insert_with_kv_digest(&make_key(&borrowed, 1), InsertBytes::Borrowed(&value), None);
        map.insert_with_kv_digest(
            &make_key(&shared, 1),
            InsertBytes::Shared(make_value(&value)),
            None,
        );
        expected.push((borrowed, value.clone()));
        expected.push((shared, value));
    }
    for (key, value) in &expected {
        let got = map.point_get(key, 2).map(|v| v.value.to_vec());
        assert_eq!(
            got.as_deref(),
            Some(value.as_slice()),
            "{} bytes",
            value.len()
        );
    }
    let mut iterated: Vec<(Vec<u8>, Vec<u8>)> = map
        .iter()
        .map(|e| (e.key().user_key.to_vec(), e.value().to_vec()))
        .collect();
    iterated.sort();
    expected.sort();
    assert_eq!(iterated, expected);
}

/// Borrowed bytes past the inline size and up to the limit go into the arena,
/// and nothing past it: bytes a value holds inline stay in its slot, which
/// costs them no allocation, and a kept buffer never goes there. With the
/// `bytes` backend nothing goes there, since a `Bytes` value cannot view an
/// arena block.
#[test]
fn only_borrowed_bytes_up_to_the_limit_go_into_the_arena() {
    let placed = |value: InsertBytes<'_>| {
        let map = new_map();
        map.insert_with_kv_digest(&make_key(b"k", 1), value, None);
        map.value_in_arena(map.first_node())
    };
    // The `bytes` backend places nothing in the arena, at any size.
    #[cfg(not(feature = "bytes_1"))]
    let inline_size = crate::byteview::INLINE_SIZE;
    #[cfg(feature = "bytes_1")]
    let inline_size = 20;
    let inline = vec![7u8; inline_size];
    let past_inline = vec![7u8; inline_size + 1];
    let at_limit = vec![7u8; ARENA_VALUE_MAX];
    let past_limit = vec![7u8; ARENA_VALUE_MAX + 1];
    let arena_values = cfg!(not(feature = "bytes_1"));
    assert!(!placed(InsertBytes::Borrowed(b"")));
    assert!(!placed(InsertBytes::Borrowed(&inline)));
    assert_eq!(placed(InsertBytes::Borrowed(&past_inline)), arena_values);
    assert_eq!(placed(InsertBytes::Borrowed(&at_limit)), arena_values);
    assert!(!placed(InsertBytes::Borrowed(&past_limit)));
    assert!(!placed(InsertBytes::Shared(make_value(b"small"))));
}

/// An insert that unwinds between reserving its value slot and writing it
/// (a failed allocation of its node or of its value's buffer, which unwinds
/// without `std`) leaves the slot an empty value: the store drops every
/// reserved slot, and the slot lies in a segment an earlier insert allocated,
/// filled with a pattern that reads as a value with a wild pointer. Covered
/// for a value bound for the arena and for one bound for the store.
#[test]
fn an_insert_unwinding_before_its_value_write_leaves_a_droppable_slot() {
    for value in [
        InsertBytes::Borrowed(b"small"),
        InsertBytes::Shared(make_value(b"a value too long to be held inline")),
    ] {
        let map = new_map();
        map.values_for_test().scribble_segments_for_test();
        map.insert(
            &make_key(b"a", 1),
            make_value(b"first, allocating the segment"),
        );

        map.unwind_before_value_write_for_test();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            map.insert_with_kv_digest(&make_key(b"b", 2), value, None);
        }));
        assert!(
            unwound.is_err(),
            "the injected unwind must reach the caller"
        );

        // The map stays usable and drops every slot it reserved.
        map.insert(&make_key(b"c", 3), make_value(b"after the unwind"));
        assert_eq!(
            map.point_get(b"c", 4).map(|v| v.value).as_deref(),
            Some(&b"after the unwind"[..])
        );
        assert!(map.point_get(b"b", 4).is_none());
        drop(map);
    }
}

/// A value read from the arena stays valid after the map that held it is
/// dropped and its memory reused, as a key read from it does: the read is a
/// view that keeps its arena block alive.
#[test]
fn an_arena_value_outlives_the_map() {
    let map = new_map();
    let value = vec![0xA5u8; 300];
    map.insert_with_kv_digest(&make_key(b"k", 1), InsertBytes::Borrowed(&value), None);
    let read = map.point_get(b"k", 2).map(|v| v.value);
    drop(map);

    let other = new_map();
    for i in 0..100u64 {
        other.insert_with_kv_digest(
            &make_key(&i.to_be_bytes(), i),
            InsertBytes::Borrowed(&[0x5Au8; 300]),
            None,
        );
    }
    assert_eq!(read.as_deref(), Some(value.as_slice()));
}

/// A bit flip in a value held in the arena is caught by the residence check,
/// as one in its key is: the digest covers the bytes where they lie. The
/// `bytes` backend keeps no values in the arena.
#[test]
#[cfg(not(feature = "bytes_1"))]
#[expect(clippy::expect_used, reason = "test asserts the error via expect_err")]
fn verify_kv_digests_detects_corruption_of_an_arena_value() {
    let algo = ChecksumAlgorithm::Xxh3Low32;
    let map = new_map();
    let key = make_key(b"victim", 42);
    // Past the inline size, which stays out of the arena.
    let val = make_value(b"a payload too long to be held inline");
    map.insert_with_kv_digest(
        &key,
        InsertBytes::Borrowed(&val),
        Some((digest4(&key, &val, algo), algo)),
    );
    map.test_flip_first_arena_value_byte();
    let err = map
        .verify_kv_digests()
        .expect_err("corruption must be detected");
    assert!(
        matches!(err, crate::Error::MemtableKvChecksumMismatch { .. }),
        "expected MemtableKvChecksumMismatch, got {err:?}"
    );
}

/// Every search compares each node at most once, and still stops where a
/// linear walk does. The successor a level stops at sorts on the far side of
/// the target, so a level below that reaches it again ends there without a
/// second comparison; with a long shared key prefix each repeat costs a walk
/// over the prefix. Probes fall between, on and around stored keys, versions
/// above and below the stored seqno included.
#[test]
fn a_search_compares_no_node_twice_and_stops_where_a_walk_does() {
    let map = new_map();
    let key = |i: u32| {
        let mut k = [0x5A_u8; 36];
        k[32..].copy_from_slice(&i.wrapping_mul(0x9E37_79B9).to_be_bytes());
        k
    };
    for i in (0..8192).step_by(2) {
        map.insert(&make_key(&key(i), 5), make_value(b"v"));
    }
    // The first node at or after `target` in a walk of level 0, or UNSET.
    let walk_ge = |target: &InternalKey| {
        let mut node = map.first_node();
        while node != UNSET && map.compare_key(node, target) == CmpOrdering::Less {
            node = map.next_at(node, 0);
        }
        node
    };
    let once = |what: &str, compared: Vec<u32>| {
        let mut sorted = compared.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            compared.len(),
            "{what}: a node compared twice in {compared:?}"
        );
    };
    for i in 0..600 {
        for seqno in [1, 5, 9] {
            let target = make_key(&key(i), seqno);
            let expected = walk_ge(&target);

            let (mut preds, mut succs) = ([map.head; MAX_HEIGHT], [UNSET; MAX_HEIGHT]);
            map.record_compared();
            map.find_splice(&target, &mut preds, &mut succs);
            once("find_splice", map.take_compared());
            assert_eq!(succs[0], expected, "find_splice at {i}/{seqno}");

            let (mut preds, mut succs) = ([map.head; MAX_HEIGHT], [UNSET; MAX_HEIGHT]);
            map.record_compared();
            map.find_splice_for_level(&target, &mut preds, &mut succs, 0);
            once("find_splice_for_level", map.take_compared());
            assert_eq!(succs[0], expected, "find_splice_for_level at {i}/{seqno}");

            map.record_compared();
            let ge = map.seek_ge(&target);
            once("seek_ge", map.take_compared());
            assert_eq!(ge, expected, "seek_ge at {i}/{seqno}");

            map.record_compared();
            let lt = map.seek_lt(&target);
            once("seek_lt", map.take_compared());
            let walked_lt = (expected != map.first_node()).then(|| {
                let mut node = map.first_node();
                while map.next_at(node, 0) != expected {
                    node = map.next_at(node, 0);
                }
                node
            });
            assert_eq!(
                Some(lt).filter(|&n| n != UNSET),
                walked_lt,
                "seek_lt at {i}/{seqno}"
            );

            map.record_compared();
            let gt = map.seek_gt(&target);
            once("seek_gt", map.take_compared());
            map.record_compared();
            let le = map.seek_le(&target);
            once("seek_le", map.take_compared());
            // A stored version equal to the target sits between the two.
            let equal =
                expected != UNSET && map.compare_key(expected, &target) == CmpOrdering::Equal;
            let after = if equal {
                map.next_at(expected, 0)
            } else {
                expected
            };
            assert_eq!(gt, after, "seek_gt at {i}/{seqno}");
            let at_or_below = if equal { Some(expected) } else { walked_lt };
            assert_eq!(
                Some(le).filter(|&n| n != UNSET),
                at_or_below,
                "seek_le at {i}/{seqno}"
            );

            map.record_compared();
            map.seek_past_user_key(&key(i));
            once("seek_past_user_key", map.take_compared());
        }
    }
}

/// A search that skips the prefix its bounds share with the target still
/// stops where a walk of each level does. Keys share prefixes of 0, 40 and
/// 100 bytes and then differ in a short tail over a three-letter alphabet, so
/// the prefix the bounds share changes at every step of a descent: a skip
/// taken from one bound alone, or kept after the bound it came from moved,
/// would pass a node that differs inside the skipped bytes and splice or seek
/// at the wrong place. Each level is checked, those at and above the list's
/// height included, for versions above, at and below the stored seqno.
#[test]
fn a_prefix_skipping_search_stops_where_a_walk_of_each_level_does() {
    let map = new_map();
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut key = || {
        let shared = [0usize, 40, 100][usize::try_from(next() % 3).expect("below 3")];
        let tail = usize::try_from(next() % 12).expect("below 12");
        let mut k = vec![0xA5_u8; shared];
        k.extend((0..tail).map(|_| u8::try_from(next() % 3).expect("below 3")));
        k
    };
    let stored: Vec<Vec<u8>> = (0..3000).map(|_| key()).collect();
    let probes: Vec<Vec<u8>> = (0..600)
        .map(|_| key())
        .chain(stored[..600].to_vec())
        .collect();
    for k in &stored {
        map.insert(&make_key(k, 5), make_value(b"v"));
    }
    // The first node at or after `target` in a walk of `level`, or UNSET.
    let walk_ge = |target: &InternalKey, level: usize| {
        let mut node = map.next_at(map.head, level);
        while node != UNSET && map.compare_key(node, target) == CmpOrdering::Less {
            node = map.next_at(node, level);
        }
        node
    };
    for probe in &probes {
        for seqno in [1, 5, 9] {
            let target = make_key(probe, seqno);

            let (mut preds, mut succs) = ([map.head; MAX_HEIGHT], [UNSET; MAX_HEIGHT]);
            map.find_splice(&target, &mut preds, &mut succs);
            let height = map.height.load(Ordering::Acquire);
            for (level, &succ) in succs.iter().enumerate().take(height) {
                assert_eq!(
                    succ,
                    walk_ge(&target, level),
                    "find_splice level {level} for {probe:?}/{seqno}"
                );
            }

            for level in 0..MAX_HEIGHT {
                let (mut preds, mut succs) = ([map.head; MAX_HEIGHT], [UNSET; MAX_HEIGHT]);
                map.find_splice_for_level(&target, &mut preds, &mut succs, level);
                assert_eq!(
                    succs[level],
                    walk_ge(&target, level),
                    "find_splice_for_level {level} for {probe:?}/{seqno}"
                );
            }

            assert_eq!(
                map.seek_ge(&target),
                walk_ge(&target, 0),
                "seek_ge for {probe:?}/{seqno}"
            );
        }
    }
}
