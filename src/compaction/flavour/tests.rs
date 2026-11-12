use super::*;
use crate::{UserKey, UserValue, vlog::ValueHandle};

#[expect(clippy::unnecessary_wraps)]
fn entry(
    blob_file_id: BlobFileId,
    key: &[u8],
    frame: u32,
) -> crate::Result<(ScanEntry, BlobFileId)> {
    Ok((
        ScanEntry {
            key: UserKey::from(key),
            frame,
            index: 0,
            offset: u64::from(frame),
            seqno: 0,
            uncompressed_len: 0,
            value: Some(UserValue::empty()),
            // These fixtures use ordinal offsets (0, 1, 2, ...), not real byte
            // positions, so the frame ends one ordinal past its start, never
            // at the start itself, which would mislead any future test that
            // inspects the consumed-frontier argument.
            frame_end: u64::from(frame) + 1,
            resynced: false,
            frames: None,
            block: None,
        },
        blob_file_id,
    ))
}

/// The indirection naming `frame`, `index` of blob file `blob_file_id`.
fn naming(blob_file_id: BlobFileId, frame: u32, index: u16) -> BlobIndirection {
    BlobIndirection {
        size: 0,
        stored: crate::blob_tree::handle::Stored::Frames(0),
        vhandle: ValueHandle {
            blob_file_id,
            frame,
            index,
        },
    }
}

/// A held row costs its place in the queue as well as its bytes: a run of
/// tombstones, whose keys and values are short, still counts toward the
/// bound on what a relocation holds.
#[test]
fn a_held_row_counts_its_place_in_the_queue() {
    let tombstone = InternalValue::from_components(
        UserKey::from(b"k".as_slice()),
        UserValue::empty(),
        1,
        crate::ValueType::Tombstone,
    );
    // Its place, plus the one byte of its key.
    assert!(held_len(&Held::Row(tombstone)) > core::mem::size_of::<Held>());
}

/// A held pointer to `value`, claimed out of a block.
fn held_in_block(value: UserValue, index: u16) -> Held {
    Held::Pointer {
        item: InternalValue::from_components(
            UserKey::from(b"k".as_slice()),
            UserValue::empty(),
            1,
            crate::ValueType::Indirection,
        ),
        indirection: naming(7, 0, index),
        claimed: Claimed {
            blob_file_id: 7,
            frame: 0,
            index,
            seqno: 1,
            #[expect(clippy::cast_possible_truncation, reason = "test values are short")]
            uncompressed_len: value.len() as u32,
            value: Some(value),
            frames: None,
            in_block: true,
        },
    }
}

/// A held value decoded out of a block is a view of it and keeps the whole
/// block alive: the bound on held bytes counts the block, once for all its
/// values held one after another, until the last of them is let go, even
/// when the one held first goes first. Without that, one live value would pin
/// a block of dead ones past the bound.
#[test]
#[cfg(not(feature = "bytes_1"))]
fn held_values_of_a_block_count_it_once_until_the_last_goes() {
    let block = UserValue::from(alloc::vec![b'x'; 8 * 1024]);
    let crate::slice::Allocation::Shared { len: block_len, .. } = block.allocation() else {
        panic!("a block on the heap");
    };
    let mut held = HeldRows::default();
    let first = held_in_block(block.slice(100..164), 0);
    let first_len = held_len(&first);
    held.push(first);
    assert_eq!(held.bytes, first_len + block_len);
    let second = held_in_block(block.slice(200..264), 1);
    let second_len = held_len(&second);
    held.push(second);
    // The block once, with the views' own bytes.
    assert_eq!(held.bytes, first_len + second_len + block_len);
    drop(block);
    // The first view goes; the second one still keeps the block.
    assert!(held.pop_front().is_some());
    assert_eq!(held.bytes, second_len + block_len);
    assert!(held.pop_front().is_some());
    assert_eq!(held.bytes, 0);
    assert!(held.is_empty());
}

/// Where the slice cannot tell the allocation it views, a held block value is
/// copied out of it, so the block it would keep alive does not pass the
/// bound uncounted.
#[test]
#[cfg(feature = "bytes_1")]
fn a_held_block_value_of_an_unknown_allocation_lets_its_block_go() {
    let block = UserValue::from(alloc::vec![b'x'; 8 * 1024]);
    let value = block.slice(100..164);
    let mut held = HeldRows::default();
    held.push(held_in_block(value.clone(), 0));
    let Some(Held::Pointer { claimed, .. }) = held.front() else {
        panic!("a pointer");
    };
    let Some(kept) = &claimed.value else {
        panic!("the value is kept");
    };
    assert_eq!(&**kept, &*value);
    assert!(
        !block.as_ptr_range().contains(&kept.as_ptr()),
        "the held value no longer lies in its block"
    );
}

/// A block whose values are held again past another block's is counted
/// again: counted too often the bound is only reached sooner, never passed.
#[test]
#[cfg(not(feature = "bytes_1"))]
fn a_block_held_again_past_another_is_counted_again() {
    let a = UserValue::from(alloc::vec![b'a'; 8 * 1024]);
    let b = UserValue::from(alloc::vec![b'b'; 4 * 1024]);
    let size = |block: &UserValue| match block.allocation() {
        crate::slice::Allocation::Shared { len, .. } => len,
        _ => panic!("a block on the heap"),
    };
    let (a_len, b_len) = (size(&a), size(&b));
    let mut held = HeldRows::default();
    let rows = [
        held_in_block(a.slice(100..164), 0),
        held_in_block(b.slice(100..164), 0),
        held_in_block(a.slice(200..264), 1),
    ];
    let own = rows.iter().map(held_len).sum::<usize>();
    for row in rows {
        held.push(row);
    }
    assert_eq!(held.bytes, own + 2 * a_len + b_len);
    while held.pop_front().is_some() {}
    assert_eq!(held.bytes, 0);
}

/// Values of one block share its frame: a drain toward one of them passes
/// the block's earlier values and stops at its own index.
#[test]
fn drain_blobs_stops_at_the_index_within_a_block() -> crate::Result<()> {
    let in_block = |index: u16| -> crate::Result<(ScanEntry, BlobFileId)> {
        let (mut scanned, file) = entry(0, b"a", 3)?;
        scanned.index = index;
        Ok((scanned, file))
    };
    let mut iter = [in_block(0), in_block(1), in_block(2)]
        .into_iter()
        .peekable();

    drain_blobs(
        &mut iter,
        b"a",
        &naming(0, 3, 2),
        crate::comparator::default_comparator().as_ref(),
        &mut |_, _| {},
        &mut |_, _| {},
    )?;

    assert_eq!(in_block(2)?, iter.next().unwrap()?);
    Ok(())
}

#[test]
fn drain_blobs_simple() -> crate::Result<()> {
    let mut iter = [
        entry(0, b"a", 0),
        entry(0, b"a", 1),
        entry(0, b"a", 2),
        entry(0, b"a", 3),
        entry(0, b"a", 4),
    ]
    .into_iter()
    .peekable();

    drain_blobs(
        &mut iter,
        b"a",
        &naming(0, 4, 0),
        crate::comparator::default_comparator().as_ref(),
        &mut |_, _| {},
        &mut |_, _| {},
    )?;

    assert_eq!(entry(0, b"a", 4)?, iter.next().unwrap()?);

    Ok(())
}

#[test]
fn drain_blobs_multiple_keys() -> crate::Result<()> {
    let mut iter = [
        entry(0, b"a", 0),
        entry(0, b"b", 0),
        entry(0, b"c", 0),
        entry(0, b"d", 0),
        entry(0, b"e", 0),
    ]
    .into_iter()
    .peekable();

    drain_blobs(
        &mut iter,
        b"e",
        &naming(0, 0, 0),
        crate::comparator::default_comparator().as_ref(),
        &mut |_, _| {},
        &mut |_, _| {},
    )?;

    assert_eq!(entry(0, b"e", 0)?, iter.next().unwrap()?);

    Ok(())
}

/// Under a comparator that reverses byte order the scan runs from high keys
/// to low: draining towards a lower key passes higher ones, which sort before
/// it in the tree's order.
#[test]
fn drain_blobs_follows_the_tree_comparator() -> crate::Result<()> {
    struct Reverse;
    impl crate::comparator::UserComparator for Reverse {
        fn name(&self) -> &'static str {
            "reverse"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
            b.cmp(a)
        }
    }

    let mut iter = [entry(0, b"e", 0), entry(0, b"c", 1), entry(0, b"a", 2)]
        .into_iter()
        .peekable();
    drain_blobs(
        &mut iter,
        b"a",
        &naming(0, 2, 0),
        &Reverse,
        &mut |_, _| {},
        &mut |_, _| {},
    )?;

    assert_eq!(entry(0, b"a", 2)?, iter.next().unwrap()?);
    Ok(())
}

/// Draining a slice's unclaimed frames takes every frame below the bound,
/// records the proven ones and leaves the bound key's frames in place.
#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn drain_unclaimed_below_takes_the_frames_below_the_bound() -> crate::Result<()> {
    let mut resync = entry(1, b"c", 7)?;
    resync.0.resynced = true;
    let mut iter = [
        entry(0, b"a", 0),
        entry(1, b"b", 4),
        Ok(resync),
        entry(0, b"d", 1),
    ]
    .into_iter()
    .peekable();

    let mut recorded = Vec::new();
    drain_unclaimed_below(
        &mut iter,
        b"d",
        crate::comparator::default_comparator().as_ref(),
        &mut |id, frame_end| recorded.push((id, frame_end)),
        &mut |_, _| {},
        &|| false,
    )?;

    assert_eq!(
        recorded,
        [(0, 1), (1, 5)],
        "the resynced frame moves nothing"
    );
    assert_eq!(entry(0, b"d", 1)?, iter.next().unwrap()?);
    Ok(())
}

/// A scan error met while draining ends the drain and is returned, not
/// skipped: the frontier never moves past a frame that could not be read.
#[test]
fn drain_unclaimed_below_returns_a_scan_error() {
    let mut iter = [
        entry(0, b"a", 0),
        Err(crate::Error::Io(crate::io::Error::other(
            "unreadable frame",
        ))),
        entry(0, b"b", 1),
    ]
    .into_iter()
    .peekable();

    let mut recorded = Vec::new();
    let result = drain_unclaimed_below(
        &mut iter,
        b"z",
        crate::comparator::default_comparator().as_ref(),
        &mut |id, frame_end| recorded.push((id, frame_end)),
        &mut |_, _| {},
        &|| false,
    );

    assert!(
        matches!(result, Err(crate::Error::Io(_))),
        "the scan error is returned: {result:?}"
    );
    assert_eq!(recorded, [(0, 1)], "only the frame before the error counts");
}

/// A stop requested while a slice drains its unclaimed frames ends the drain
/// with the compaction's cancellation error instead of scanning on.
#[test]
fn drain_unclaimed_below_stops_on_request() {
    let mut iter = [entry(0, b"a", 0), entry(0, b"b", 1)]
        .into_iter()
        .peekable();

    let mut recorded = 0usize;
    let result = drain_unclaimed_below(
        &mut iter,
        b"z",
        crate::comparator::default_comparator().as_ref(),
        &mut |_, _| recorded += 1,
        &mut |_, _| {},
        &|| true,
    );

    assert!(result.is_err(), "a stop must end the drain with an error");
    assert_eq!(recorded, 0, "nothing is drained after the stop");
    assert!(iter.next().is_some(), "the frames stay unread");
}

#[test]
fn drain_blobs_does_not_advance_the_frontier_past_a_resynced_frame() -> crate::Result<()> {
    // A RESYNCED entry's `frame_end` boundary is unproven (see
    // `ScanEntry::resynced`). Draining it must NOT advance the reclaim frontier,
    // or the tight-space loop could punch past that unanchored boundary and then
    // skip a later frame that is actually valid when the scan resumes there.
    let mut resync = entry(0, b"a", 3)?;
    resync.0.resynced = true;
    let mut iter = [entry(0, b"a", 0), entry(0, b"a", 1), Ok(resync)]
        .into_iter()
        .peekable();

    let mut max_recorded = 0u64;
    drain_blobs(
        &mut iter,
        b"a",
        &naming(0, 9, 0),
        crate::comparator::default_comparator().as_ref(),
        &mut |_, frame_end| max_recorded = max_recorded.max(frame_end),
        &mut |_, _| {},
    )?;

    // The last PROVEN frame (offset 1) ends at ordinal 2; the resynced frame
    // (frame_end 4) must not have advanced the frontier past it.
    assert_eq!(
        max_recorded, 2,
        "a resynced frame must not advance the reclaim frontier past the last proven boundary",
    );
    Ok(())
}
