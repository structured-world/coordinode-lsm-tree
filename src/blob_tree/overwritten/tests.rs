use super::{MarkOverwritten, OverwrittenKeys};
use crate::{InternalValue, ValueType, comparator::default_comparator};

/// The merge order: by key, newest version first.
fn merged() -> Vec<crate::Result<InternalValue>> {
    [
        (b"a", 1),
        (b"b", 7),
        (b"b", 4),
        (b"b", 2),
        (b"c", 3),
        (b"d", 9),
        (b"d", 5),
    ]
    .into_iter()
    .map(|(key, seqno)| {
        Ok(InternalValue::from_components(
            key.as_slice(),
            b"v".as_slice(),
            seqno,
            ValueType::Value,
        ))
    })
    .collect()
}

/// A key with several versions is queued once, a key with one is not, and
/// every item passes through unchanged and in order.
#[test]
#[expect(clippy::unwrap_used)]
fn mark_overwritten_queues_each_multi_version_key_once() {
    let keys = OverwrittenKeys::default();
    let passed: Vec<_> = MarkOverwritten::new(merged().into_iter(), Some(&keys))
        .map(|item| {
            let item = item.unwrap();
            (item.key.user_key.to_vec(), item.key.seqno)
        })
        .collect();
    assert_eq!(
        passed,
        [
            (b"a".to_vec(), 1),
            (b"b".to_vec(), 7),
            (b"b".to_vec(), 4),
            (b"b".to_vec(), 2),
            (b"c".to_vec(), 3),
            (b"d".to_vec(), 9),
            (b"d".to_vec(), 5),
        ]
    );
    let queued: Vec<_> = keys.0.borrow().iter().map(|key| key.to_vec()).collect();
    assert_eq!(queued, [b"b".to_vec(), b"d".to_vec()]);
}

/// The key is queued by the time its newest version leaves the iterator,
/// which is what lets the writer, further down, find it.
#[test]
#[expect(clippy::unwrap_used)]
fn mark_overwritten_queues_before_the_newest_version_leaves() {
    let keys = OverwrittenKeys::default();
    let comparator = default_comparator();
    let mut iter = MarkOverwritten::new(merged().into_iter(), Some(&keys));

    let a = iter.next().unwrap().unwrap();
    assert!(!keys.take_before_and_check(&a.key.user_key, &comparator));

    let b = iter.next().unwrap().unwrap();
    assert_eq!(b.key.seqno, 7);
    assert!(keys.take_before_and_check(&b.key.user_key, &comparator));
}

/// The writer's check forgets keys it has passed and answers for the key it
/// is at, including every older version of that key it is handed.
#[test]
fn overwritten_keys_check_moves_forward_in_key_order() {
    let keys = OverwrittenKeys::default();
    let comparator = default_comparator();
    for item in MarkOverwritten::new(merged().into_iter(), Some(&keys)) {
        drop(item);
    }

    assert!(!keys.take_before_and_check(b"a", &comparator));
    assert!(keys.take_before_and_check(b"b", &comparator));
    assert!(keys.take_before_and_check(b"b", &comparator));
    assert!(!keys.take_before_and_check(b"c", &comparator));
    assert_eq!(keys.0.borrow().len(), 1, "`b` is forgotten once passed");
    assert!(keys.take_before_and_check(b"d", &comparator));
    assert!(!keys.take_before_and_check(b"e", &comparator));
    assert!(keys.0.borrow().is_empty());
}

/// Without a queue the merge passes through and nothing is recorded.
#[test]
fn mark_overwritten_without_a_queue_only_passes_through() {
    assert_eq!(MarkOverwritten::new(merged().into_iter(), None).count(), 7);
}
