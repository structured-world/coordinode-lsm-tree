use super::MAX_SEQNO;
use test_log::test;

#[test]
fn next_below_max_returns_valid_seqno() {
    let counter = super::SequenceNumberCounter::default();
    counter.set(MAX_SEQNO - 1);
    let _ = counter.next();
}

#[test]
#[should_panic(expected = "Ran out of sequence numbers")]
fn next_at_max_panics() {
    let counter = super::SequenceNumberCounter::default();
    counter.set(MAX_SEQNO);
    let _ = counter.next();
}

#[test]
#[should_panic(expected = "Sequence number must not use the reserved MSB")]
fn set_reserved_range_panics() {
    let counter = super::SequenceNumberCounter::default();
    counter.set(MAX_SEQNO + 1);
}

#[test]
fn fetch_max_clamps_reserved_to_max() {
    let counter = super::SequenceNumberCounter::default();
    counter.set(100);
    counter.fetch_max(MAX_SEQNO + 1);
    assert_eq!(counter.get(), MAX_SEQNO);
}
