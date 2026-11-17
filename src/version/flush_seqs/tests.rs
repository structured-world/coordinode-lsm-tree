use super::FlushSeqs;

/// The next number, which these tests never run out of.
fn take(seqs: &FlushSeqs) -> u64 {
    match seqs.begin() {
        Some(seq) => seq,
        None => panic!("the numbers ran out"),
    }
}

/// Numbering resumes above the watermark a manifest recorded, so a flush
/// after the open is never taken for one the manifest decided.
#[test]
fn numbers_start_above_the_recovered_watermark() {
    let seqs = FlushSeqs::after(41);
    assert_eq!(seqs.decided(), 41);
    assert_eq!(seqs.begin(), Some(42));
}

/// The last numbers are not handed out: a flush past them is written to the
/// manifest by name. Guards against the counter overflowing on a watermark a
/// manifest records at the top of the range.
#[test]
fn numbers_run_out_without_overflowing() {
    let seqs = FlushSeqs::after(u64::MAX - 1);
    assert_eq!(seqs.begin(), None);
    assert_eq!(seqs.decided(), u64::MAX - 1);
}

/// A manifest written while a flush is in flight leaves that flush
/// undecided, as its tables may still be installed; once it ends, the next
/// manifest decides it.
#[test]
fn the_flush_in_flight_is_left_undecided_until_it_ends() {
    let seqs = FlushSeqs::default();
    let first = take(&seqs);
    seqs.end(first);
    let second = take(&seqs);
    assert_eq!(seqs.decided(), first);
    seqs.end(second);
    assert_eq!(seqs.decided(), second);
}

/// Ending a flush that already ended leaves the next one in flight. Guards
/// against the caller's end, after the install's, deciding a later flush.
#[test]
fn ending_a_flush_twice_leaves_a_later_one_undecided() {
    let seqs = FlushSeqs::default();
    let first = take(&seqs);
    seqs.end(first);
    let second = take(&seqs);
    seqs.end(first);
    assert_eq!(seqs.decided(), first, "the later flush stays undecided");
    seqs.end(second);
    assert_eq!(seqs.decided(), second);
}

/// A clear decides the flush in flight, whose memtables it dropped, and no
/// manifest written later leaves that flush undecided again. Guards against
/// the cleared flush's self-described tables being installed by an open after
/// a power loss.
#[test]
fn a_clear_decides_the_flush_in_flight_for_good() {
    let seqs = FlushSeqs::default();
    let cleared = take(&seqs);
    seqs.decide_taken();
    assert_eq!(seqs.decided(), cleared, "the clear's manifest decides it");
    assert!(seqs.cleared(cleared));
    seqs.end(cleared);
    let next = take(&seqs);
    assert_eq!(
        seqs.decided(),
        cleared,
        "a later flush in flight does not lower it"
    );
    assert!(next > cleared);
    assert!(!seqs.cleared(next));
}
