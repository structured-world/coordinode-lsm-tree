use super::FlushSeqs;

/// Numbering resumes above the watermark a manifest recorded, so a flush
/// after the open is never taken for one the manifest decided.
#[test]
fn numbers_start_above_the_recovered_watermark() {
    let seqs = FlushSeqs::after(41);
    assert_eq!(seqs.decided(), 41);
    assert_eq!(seqs.begin(), 42);
}

/// A manifest written while a flush is in flight leaves that flush
/// undecided, as its tables may still be installed; once it ends, the next
/// manifest decides it.
#[test]
fn the_flush_in_flight_is_left_undecided_until_it_ends() {
    let seqs = FlushSeqs::default();
    let first = seqs.begin();
    seqs.end();
    let second = seqs.begin();
    assert_eq!(seqs.decided(), first);
    seqs.end();
    assert_eq!(seqs.decided(), second);
}

/// A clear decides the flush in flight, whose memtables it dropped, and no
/// manifest written later leaves that flush undecided again. Guards against
/// the cleared flush's self-described tables being installed by an open after
/// a power loss.
#[test]
fn a_clear_decides_the_flush_in_flight_for_good() {
    let seqs = FlushSeqs::default();
    let cleared = seqs.begin();
    seqs.decide_taken();
    assert_eq!(seqs.decided(), cleared, "the clear's manifest decides it");
    seqs.end();
    let next = seqs.begin();
    assert_eq!(
        seqs.decided(),
        cleared,
        "a later flush in flight does not lower it"
    );
    assert!(next > cleared);
}
