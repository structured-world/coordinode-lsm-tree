use super::QueuedBuf;
use crate::fs::{BlockBuf, Fs, FsFile, FsOpenOptions, MemFs};
use std::io::Write;
use std::path::Path;
use test_log::test;

/// A file holding `bytes`, opened for reading.
fn file_of(bytes: &[u8]) -> Box<dyn FsFile> {
    let fs = MemFs::new();
    let path = Path::new("/f");
    let mut file = fs
        .open(path, &FsOpenOptions::new().write(true).create(true))
        .expect("create");
    file.write_all(bytes).expect("write");
    fs.open(path, &FsOpenOptions::new().read(true))
        .expect("open")
}

/// A read that resumes past the last offset a file can have fails as an
/// invalid input, before anything is read, instead of wrapping around to
/// the start of the file.
#[test]
fn read_from_past_the_last_offset_fails_as_invalid_input() {
    let file = file_of(b"abcd");
    let mut buf = QueuedBuf::new(4);
    buf.fill_with(|block| block.append(b"a"));
    let err = buf
        .read_from(file.as_ref(), u64::MAX)
        .expect_err("offset + filled overflows");
    assert_eq!(err.kind(), crate::io::ErrorKind::InvalidInput);
    assert_eq!(buf.filled(), 1, "nothing was read");
}

/// A file ending before the block does fails as an unexpected end of file,
/// keeping the bytes it did hold.
#[test]
fn read_from_a_file_shorter_than_the_block_fails_as_eof() {
    let file = file_of(b"abc");
    let mut buf = QueuedBuf::new(8);
    let err = buf
        .read_from(file.as_ref(), 0)
        .expect_err("the file ends at 3 bytes");
    assert_eq!(err.kind(), crate::io::ErrorKind::UnexpectedEof);
    assert_eq!(buf.filled_bytes(), b"abc");
    assert!(buf.into_slice().is_none());
}

/// A read resumed past the bytes already filled reads on from there.
#[test]
fn read_from_resumes_past_the_filled_part() {
    let file = file_of(b"hello, world");
    let mut buf = QueuedBuf::new(5);
    buf.fill_with(|block| block.append(b"w"));
    buf.read_from(file.as_ref(), 7).expect("read");
    assert_eq!(buf.filled_bytes(), b"world");
}

/// The debug form names the buffer's sizes, never its bytes.
#[test]
fn the_debug_form_shows_capacity_init_and_filled() {
    let mut buf = QueuedBuf::new(8);
    buf.fill_with(|block| block.append(b"abc"));
    let shown = format!("{buf:?}");
    assert_eq!(shown, "QueuedBuf { capacity: 8, init: 3, filled: 3, .. }");
}

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
