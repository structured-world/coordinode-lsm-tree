use super::span_extent;
use crate::vlog::ValueHandle;
use test_log::test;

/// `(key, handle, record length)` triples at the given offsets, all in one blob
/// file, each record `len` bytes long. Sorted by offset, as `prefetch` sorts
/// them before walking.
fn items(offsets: &[u64], len: usize) -> Vec<(&'static [u8], ValueHandle, usize)> {
    offsets
        .iter()
        .map(|&offset| {
            (
                b"k".as_ref(),
                ValueHandle {
                    blob_file_id: 1,
                    offset,
                    #[expect(clippy::cast_possible_truncation, reason = "test lengths are small")]
                    on_disk_size: len as u32,
                },
                len,
            )
        })
        .collect()
}

/// Adjacent records merge into one span.
#[test]
fn adjacent_records_merge_into_one_span() {
    let items = items(&[0, 100, 200], 100);
    let (end, span_end) = span_extent(&items, 0, 4096, 1_000_000, u64::MAX).expect("extent");
    assert_eq!(end, 3, "all three records belong to the span");
    assert_eq!(span_end, 300);
}

/// A gap wider than `max_gap` ends the span rather than reading through it.
#[test]
fn a_gap_wider_than_the_limit_ends_the_span() {
    // 0..100, then a 5 KiB hole, then the next record.
    let items = items(&[0, 100, 5_200], 100);
    let (end, span_end) = span_extent(&items, 0, 4096, 1_000_000, u64::MAX).expect("extent");
    assert_eq!(end, 2, "the far record starts a new span");
    assert_eq!(span_end, 200);
}

/// The extent bound covers the GAPS, not just the record bytes: this is the
/// number a caller reads, and small records spread across wide gaps must not
/// slip past a budget expressed in bytes read.
#[test]
fn the_extent_bound_counts_gap_bytes_not_only_records() {
    // Ten 100-byte records, each 4 KiB from the last: 1 000 bytes of records
    // spread over ~37 KiB.
    let offsets: Vec<u64> = (0..10).map(|i| i * 4_096).collect();
    let items = items(&offsets, 100);

    // A budget of 10 KiB is far more than the 1 000 bytes of records, and far
    // less than the extent they span.
    let (end, span_end) = span_extent(&items, 0, 4096, 10_240, u64::MAX).expect("extent");

    assert!(
        span_end <= 10_240,
        "extent {span_end} must stay inside the 10 KiB bound",
    );
    assert!(
        end < items.len(),
        "the span must stop short of the full run ({end} of {})",
        items.len(),
    );
}

/// The remaining read budget bounds a span just as the per-read cap does.
///
/// The per-read cap is a constant, and bounding by it alone let one prefetch
/// issue a whole series of capped reads: the budget is what limits the sum. A
/// run of small records with wide gaps is where the two diverge most, since the
/// records themselves stay tiny while each merge adds a gap to the read.
#[test]
fn a_span_never_exceeds_the_remaining_read_budget() {
    // Ten 100-byte records, each 4 KiB apart: 1 000 bytes of records spread
    // over ~37 KiB, so the extent is what matters.
    let offsets: Vec<u64> = (0..10).map(|i| i * 4_096).collect();
    let items = items(&offsets, 100);

    // Per-read cap wide open, budget nearly spent.
    let budget = 8_192;
    let (end, span_end) = span_extent(&items, 0, 4096, u64::MAX, budget).expect("extent");

    assert!(
        span_end <= budget,
        "extent {span_end} must stay inside the {budget}-byte budget left",
    );
    assert!(
        end < items.len(),
        "the span must stop short of the full run ({end} of {})",
        items.len(),
    );
}

/// The record that anchors a span is bounded too.
///
/// Its length comes from a handle, so a corrupt one can declare a record wider
/// than any read is allowed to be. Checking only the records merged AFTER it
/// leaves that width unexamined, and a second handle pointing inside the
/// declared span turns it into a multi-record span that gets read in full.
#[test]
fn an_anchor_record_wider_than_the_bound_does_not_carry_a_span() {
    // First record claims 1 MiB; the second sits inside that claim, so it
    // passes a check made only on `next_end - first.offset`.
    let mut items = items(&[0], 1_048_576);
    items.push((
        b"k".as_ref(),
        ValueHandle {
            blob_file_id: 1,
            offset: 100,
            on_disk_size: 100,
        },
        100,
    ));

    let (end, _) = span_extent(&items, 0, 4096, 4_096, u64::MAX).expect("extent");
    assert_eq!(
        end, 1,
        "an anchor wider than the read cap must not gather a span",
    );
}

/// A budget of zero admits nothing beyond the record that anchors the span, so
/// the caller sees a one-record span and skips it (a single record is what the
/// direct read already does well).
#[test]
fn a_zero_extent_bound_yields_a_single_record_span() {
    let items = items(&[0, 100, 200], 100);
    let (end, span_end) = span_extent(&items, 0, 4096, 0, u64::MAX).expect("extent");
    assert_eq!(end, 1, "nothing can be merged into a zero-byte budget");
    assert_eq!(span_end, 100, "the anchor record's own end");
}

/// A record whose end does not fit a `u64` is a handle no writer produced: it
/// is rejected, not clamped. Clamped, its span would appear to reach the top of
/// the address space and swallow every later record regardless of distance.
#[test]
fn a_record_end_past_u64_is_rejected() {
    let items = items(&[u64::MAX - 10], 100);
    assert!(
        span_extent(&items, 0, 4096, 1_000_000, u64::MAX).is_none(),
        "an unrepresentable record end must be refused",
    );
}

/// The same overflow one record into the run ends the span instead of taking
/// the bad record with it.
#[test]
fn an_overflowing_later_record_ends_the_span() {
    let mut items = items(&[0, 100], 100);
    items.push((
        b"k".as_ref(),
        ValueHandle {
            blob_file_id: 1,
            offset: u64::MAX - 10,
            on_disk_size: 100,
        },
        100,
    ));

    // The third record is far past the gap limit anyway, so the span ends at
    // two either way; the point is that it ends rather than panicking or
    // wrapping.
    let (end, span_end) = span_extent(&items, 0, 4096, u64::MAX, u64::MAX).expect("extent");
    assert_eq!(end, 2);
    assert_eq!(span_end, 200);
}

/// A span never crosses into another blob file, however close the offsets are.
#[test]
fn a_span_stops_at_the_blob_file_boundary() {
    let mut items = items(&[0, 100], 100);
    items.push((
        b"k".as_ref(),
        ValueHandle {
            blob_file_id: 2,
            offset: 200,
            on_disk_size: 100,
        },
        100,
    ));

    let (end, span_end) = span_extent(&items, 0, 4096, 1_000_000, u64::MAX).expect("extent");
    assert_eq!(end, 2, "the other file's record is not part of this span");
    assert_eq!(span_end, 200);
}
