use super::BlockBuf;
use test_log::test;

/// An oversized `advance` clamps at the capacity instead of overflowing:
/// `filled + n` must not be computed first, or a huge `n` panics in debug
/// builds and wraps `filled` BACKWARDS in release builds.
#[test]
fn advance_with_oversized_n_clamps_at_capacity() {
    let mut mem = [0u8; 4];
    let mut buf = BlockBuf::new(&mut mem);
    buf.append(&[1, 2]);
    buf.advance(usize::MAX);
    assert_eq!(buf.filled(), 4, "clamped at capacity, not wrapped");
    assert!(buf.is_full());
}

/// The count only moves when bytes are written, which is what tells the
/// caller the request was actually served.
#[test]
fn a_fresh_buffer_is_empty_and_not_full() {
    let mut mem = [0u8; 4];
    let buf = BlockBuf::new(&mut mem);
    assert_eq!(buf.capacity(), 4);
    assert_eq!(buf.filled(), 0);
    assert!(!buf.is_full(), "nothing has been written yet");
}

/// `append` fills and counts in one step.
#[test]
fn appending_counts_only_what_it_wrote() {
    let mut mem = [0u8; 4];
    let mut buf = BlockBuf::new(&mut mem);

    assert_eq!(buf.append(&[1, 2]), 2);
    assert_eq!(buf.filled(), 2);
    assert!(!buf.is_full());

    assert_eq!(buf.append(&[3, 4]), 2);
    assert!(buf.is_full(), "the request is filled end to end");
    assert_eq!(mem, [1, 2, 3, 4], "and the bytes landed in order");
}

/// A write past the end takes only what fits, so the count can never claim
/// more than the buffer holds.
#[test]
fn appending_past_the_end_takes_only_what_fits() {
    let mut mem = [0u8; 3];
    let mut buf = BlockBuf::new(&mut mem);

    assert_eq!(buf.append(&[1, 2, 3, 4, 5]), 3, "only three bytes fit");
    assert!(buf.is_full());
    assert_eq!(buf.append(&[6]), 0, "a full buffer takes nothing more");
    assert_eq!(buf.filled(), 3);
}

/// The unfilled region shrinks as it is filled, so a reader that takes both
/// its pointer and its length from here cannot run past the end. Taking the
/// length from `capacity` instead is the mistake this guards against.
#[test]
fn the_unfilled_region_shrinks_as_it_fills() {
    let mut mem = [0u8; 8];
    let mut buf = BlockBuf::new(&mut mem);

    assert_eq!(buf.unfilled_mut().len(), 8);
    buf.append(&[1, 2, 3]);
    assert_eq!(buf.unfilled_mut().len(), 5, "three bytes are spoken for");

    buf.unfilled_mut().fill(9);
    buf.advance(5);
    assert!(buf.is_full());
    assert_eq!(mem, [1, 2, 3, 9, 9, 9, 9, 9]);
}

/// The point of the type: an `Fs` implementation is safe code, and safe code
/// that reports success without writing anything leaves the request short.
/// The caller sees that and refuses the request, rather than decoding a
/// block out of whatever the allocation happened to hold.
#[test]
fn a_request_a_lazy_implementation_ignored_is_not_full() {
    let mut mem = [0u8; 8];
    let buf = BlockBuf::new(&mut mem);

    // Everything a safe implementation can do without writing.
    let _ = buf.capacity();
    let _ = buf.filled();

    assert!(
        !buf.is_full(),
        "a buffer nobody wrote to must never report itself filled",
    );
}
