use super::*;
use crate::ValueType;
use crate::comparator::default_comparator;
use std::sync::{Arc, Barrier};
use test_log::test;

fn new_memtable(id: MemtableId) -> Memtable {
    Memtable::new(id, default_comparator())
}

/// Low-32 logical digest of an entry under `algo`.
fn at_insert_digest(item: &InternalValue, algo: crate::runtime_config::ChecksumAlgorithm) -> u32 {
    #[expect(
        clippy::cast_possible_truncation,
        clippy::expect_used,
        reason = "4-byte algo fits u32; test helper"
    )]
    let d = crate::table::block::kv_checksum::kv_digest(item, algo).expect("xxh3 always available")
        as u32;
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
                    let suppressed = mt.is_key_suppressed_by_range_tombstone(b"f", 5, SeqNo::MAX);
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
                    let _ = mt.insert_range_tombstone(start_key.clone(), end_key.clone(), seqno);
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
