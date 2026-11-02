use super::QueuedBuf;
use crate::fs::BlockBuf;
use test_log::test;

/// A buffer filled end to end becomes the block, its bytes in order.
#[test]
fn a_filled_buffer_becomes_its_bytes() {
    let mut buf = QueuedBuf::new(8);
    assert_eq!(buf.capacity(), 8);
    assert_eq!(buf.fill_with(|block| block.append(b"abcd")), 4);
    assert_eq!(buf.fill_with(|block| block.append(b"efgh")), 4);
    assert!(buf.is_full());
    assert_eq!(buf.filled_bytes(), b"abcdefgh");
    let slice = buf.into_slice().expect("filled");
    assert_eq!(&*slice, b"abcdefgh");
}

/// A buffer left short is no block: a read reported done that never filled
/// it must not hand out whatever the allocation held.
#[test]
fn a_short_buffer_is_no_block() {
    let mut buf = QueuedBuf::new(8);
    buf.fill_with(|block| block.append(b"abc"));
    assert_eq!(buf.filled(), 3);
    assert!(!buf.is_full());
    assert!(buf.into_slice().is_none());
}

/// A filler that swaps in a destination of its own, filled, counts nothing
/// here: the bytes it claims are not this buffer's, which nothing wrote.
#[test]
fn a_destination_swapped_in_counts_nothing() {
    // Only memory that outlives every borrow can be swapped in.
    let own: &'static mut [u8] = Box::leak(vec![7u8; 8].into_boxed_slice());
    let mut buf = QueuedBuf::new(8);
    buf.fill_with(|block| {
        let mut other = BlockBuf::new(own);
        other.append(&[1; 8]);
        core::mem::swap(block, &mut other);
    });
    assert_eq!(buf.filled(), 0, "the swapped-in destination is not counted");
    assert!(buf.into_slice().is_none());
}

/// The part not filled yet is zeroed the first time safe code asks for it,
/// so a read that looks at its destination before writing it sees zeroes,
/// never what the allocation held before.
#[test]
fn the_unfilled_part_reads_as_zeroes() {
    let mut buf = QueuedBuf::new(16);
    buf.fill_with(|block| {
        block.append(b"ab");
        assert!(block.unfilled_mut().iter().all(|&b| b == 0));
        block.advance(14);
    });
    assert!(buf.is_full());
    let (head, rest) = buf.filled_bytes().split_at(2);
    assert_eq!(head, b"ab");
    assert!(rest.iter().all(|&b| b == 0));
}

/// `advance` counts only bytes known initialized: past them, a destination
/// never written stays unfilled.
#[test]
fn advance_stops_at_the_bytes_known_written() {
    let mut buf = QueuedBuf::new(8);
    buf.fill_with(|block| {
        block.append(b"ab");
        block.advance(6);
    });
    assert_eq!(buf.filled(), 2, "nothing past the two written bytes counts");
}

/// A short read's bytes stay put while the buffer moves between a queue's
/// lists: a read in flight writes through a pointer taken when it was
/// submitted.
#[test]
fn a_short_buffer_keeps_its_bytes_in_place_when_moved() {
    let buf = QueuedBuf::new(4);
    let at = buf.addr();
    let mut held = vec![buf];
    let moved = held.pop().expect("held");
    assert_eq!(moved.addr(), at);
}

/// The block shares the read's allocation: turning the buffer into the
/// block copies nothing.
#[test]
fn the_block_is_the_read_allocation() {
    let mut buf = QueuedBuf::new(4096);
    let at = buf.addr();
    buf.fill_with(|block| block.append(&[9; 4096]));
    let slice = buf.into_slice().expect("filled");
    assert_eq!(
        slice.as_ptr() as usize,
        at,
        "no copy between read and block"
    );
}
