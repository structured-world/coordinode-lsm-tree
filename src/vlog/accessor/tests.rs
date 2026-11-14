use super::span_extent;
use crate::vlog::ValueHandle;
use test_log::test;

/// A frame index: frame `n` lies over `spans[n]`.
struct Index(Vec<(u64, u64)>);

impl Index {
    /// Frames of `len` bytes starting at `starts`, in blob file 1.
    fn at(starts: &[u64], len: u64) -> Self {
        Self(starts.iter().map(|&start| (start, start + len)).collect())
    }

    fn span(&self, vhandle: &ValueHandle) -> Option<(u64, u64)> {
        self.0.get(vhandle.frame as usize).copied()
    }

    /// One `(key, handle, _)` per frame, in blob file 1, in frame order, as
    /// `prefetch` sorts them before walking.
    fn items(&self) -> Vec<(&'static [u8], ValueHandle, usize)> {
        (0..self.0.len())
            .map(|frame| (b"k".as_ref(), handle(1, frame), 0))
            .collect()
    }
}

fn handle(blob_file_id: u64, frame: usize) -> ValueHandle {
    ValueHandle {
        blob_file_id,
        frame: u32::try_from(frame).expect("few frames"),
        index: 0,
    }
}

/// Adjacent frames merge into one span.
#[test]
fn adjacent_frames_merge_into_one_span() {
    let index = Index::at(&[0, 100, 200], 100);
    let span = span_extent(&index.items(), 0, 4096, 1_000_000, u64::MAX, |h| {
        index.span(h)
    })
    .expect("extent");
    assert_eq!(span.frames, 0..3, "all three frames belong to the span");
    assert_eq!((span.start, span.end), (0, 300));
}

/// A gap wider than `max_gap` ends the span rather than reading through it.
#[test]
fn a_gap_wider_than_the_limit_ends_the_span() {
    // 0..100, then a 5 KiB hole, then the next frame.
    let index = Index::at(&[0, 100, 5_200], 100);
    let span = span_extent(&index.items(), 0, 4096, 1_000_000, u64::MAX, |h| {
        index.span(h)
    })
    .expect("extent");
    assert_eq!(span.frames, 0..2, "the far frame starts a new span");
    assert_eq!(span.end, 200);
}

/// The extent bound covers the GAPS, not just the frame bytes: this is the
/// number a caller reads, and small frames spread across wide gaps must not
/// slip past a budget expressed in bytes read.
#[test]
fn the_extent_bound_counts_gap_bytes_not_only_frames() {
    // Ten 100-byte frames, each 4 KiB from the last: 1 000 bytes of frames
    // spread over ~37 KiB.
    let starts: Vec<u64> = (0..10).map(|i| i * 4_096).collect();
    let index = Index::at(&starts, 100);
    let items = index.items();

    // A budget of 10 KiB is far more than the 1 000 bytes of frames, and far
    // less than the extent they span.
    let span = span_extent(&items, 0, 4096, 10_240, u64::MAX, |h| index.span(h)).expect("extent");

    assert!(
        span.end <= 10_240,
        "extent {} must stay inside the 10 KiB bound",
        span.end
    );
    assert!(
        span.frames.end < items.len(),
        "the span must stop short of the full run ({:?} of {})",
        span.frames,
        items.len(),
    );
}

/// The remaining read budget bounds a span just as the per-read cap does.
///
/// The per-read cap is a constant, and bounding by it alone let one prefetch
/// issue a whole series of capped reads: the budget is what limits the sum.
#[test]
fn a_span_never_exceeds_the_remaining_read_budget() {
    let starts: Vec<u64> = (0..10).map(|i| i * 4_096).collect();
    let index = Index::at(&starts, 100);
    let items = index.items();

    // Per-read cap wide open, budget nearly spent.
    let budget = 8_192;
    let span = span_extent(&items, 0, 4096, u64::MAX, budget, |h| index.span(h)).expect("extent");

    assert!(
        span.end <= budget,
        "extent {} must stay inside the {budget}-byte budget left",
        span.end
    );
    assert!(span.frames.end < items.len());
}

/// A frame wider than the read cap is left to the direct read alone: it yields
/// no span, neither one the next frames join nor one the caller would charge
/// to its budget.
#[test]
fn an_anchor_frame_wider_than_the_bound_yields_no_span() {
    let index = Index(vec![(0, 1_048_576), (1_048_576, 1_048_676)]);
    assert!(
        span_extent(&index.items(), 0, 4096, 4_096, u64::MAX, |h| index.span(h)).is_none(),
        "an anchor wider than the read cap must not make a span",
    );
}

/// A frame wider than the read budget left is left to the direct read too,
/// even under a wide read cap: a span charged to the budget never exceeds it.
#[test]
fn an_anchor_frame_wider_than_the_budget_left_yields_no_span() {
    let index = Index::at(&[0, 100, 200], 100);
    assert!(
        span_extent(&index.items(), 0, 4096, u64::MAX, 99, |h| index.span(h)).is_none(),
        "a frame wider than the budget left must not make a span",
    );
}

/// A budget of zero admits no frame at all: every frame is the direct read's.
#[test]
fn a_zero_extent_bound_yields_no_span() {
    let index = Index::at(&[0, 100, 200], 100);
    assert!(
        span_extent(&index.items(), 0, 4096, 0, u64::MAX, |h| index.span(h)).is_none(),
        "nothing fits a zero-byte bound"
    );
}

/// A handle naming a frame its file's index does not have yields no span:
/// the direct read reports it.
#[test]
fn a_frame_the_index_lacks_yields_no_span() {
    let index = Index::at(&[0], 100);
    let items = vec![(b"k".as_ref(), handle(1, 5), 0)];
    assert!(span_extent(&items, 0, 4096, 1_000_000, u64::MAX, |h| index.span(h)).is_none());
}

/// A gap whose reach would run past `u64::MAX` ends the span instead of
/// wrapping into a merge across any distance.
#[test]
fn a_gap_past_u64_ends_the_span() {
    let index = Index(vec![(0, 100), (u64::MAX - 10, u64::MAX)]);
    for max_gap in [u64::MAX, u64::MAX - 50, 4096] {
        let span = span_extent(&index.items(), 0, max_gap, u64::MAX, u64::MAX, |h| {
            index.span(h)
        })
        .expect("extent");
        assert_eq!(span.frames, 0..1, "max_gap {max_gap}");
    }
}

/// A span never crosses into another blob file, however close the frames are.
#[test]
fn a_span_stops_at_the_blob_file_boundary() {
    let index = Index::at(&[0, 100, 200], 100);
    let mut items = index.items();
    if let Some(last) = items.last_mut() {
        last.1.blob_file_id = 2;
    }
    let span =
        span_extent(&items, 0, 4096, 1_000_000, u64::MAX, |h| index.span(h)).expect("extent");
    assert_eq!(
        span.frames,
        0..2,
        "the other file's frame is not part of this span"
    );
    assert_eq!(span.end, 200);
}

/// Frames read ahead into a read's own store out of one coalesced read hold
/// none of it: blocks stored as they are (values a codec does not shrink)
/// decode into views of the read, and a view kept in the store would keep the
/// whole read, the other frames and the gaps between them, past the payloads
/// the store counts.
#[cfg(feature = "lz4")]
#[test]
fn a_local_read_ahead_keeps_no_part_of_the_coalesced_read() -> crate::Result<()> {
    use crate::{
        AbstractTree, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter,
        blob_tree::handle::BlobIndirection, iter_guard::IterGuard,
    };

    let dir = tempfile::tempdir()?;
    let tree = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .blob_compression(crate::CompressionType::Lz4)
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(16)
            .block_size(1_024),
    ))
    .open()?
    {
        crate::AnyTree::Blob(tree) => tree,
        crate::AnyTree::Standard(_) => panic!("expected a blob tree"),
    };
    // Values lz4 does not shrink, a few to a block.
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for i in 0..64u64 {
        let value: Vec<u8> = (0..200)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[0]
            })
            .collect();
        tree.insert(format!("k{i:04}"), value, i);
    }
    tree.flush_active_memtable(0)?;

    let rows: Vec<(Vec<u8>, ValueHandle)> = tree
        .index
        .iter(SeqNo::MAX, None)
        .map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.to_vec(), BlobIndirection::decode_slice(&value)?.vhandle))
        })
        .collect::<crate::Result<_>>()?;
    let mut items: Vec<(&[u8], ValueHandle, usize)> = rows
        .iter()
        .enumerate()
        .map(|(at, (key, vhandle))| (key.as_slice(), *vhandle, at))
        .collect();
    let version = tree.current_version();
    let accessor = super::Accessor::new(
        &version.blob_files,
        crate::config::BlobCacheFill::default(),
        #[cfg(feature = "metrics")]
        None,
    );
    let mut store = super::Prefetched::default();
    accessor.prefetch(
        tree.index.id(),
        &mut items,
        &tree.index.config.cache,
        4_096,
        1 << 20,
        super::PrefetchInto::Local(&mut store),
    );
    assert!(
        store.frames.len() > 1,
        "{} frames read ahead",
        store.frames.len()
    );
    // A frame that was a read by itself stays a view of it, entries and
    // payload alike; one copied out of a larger read holds its own payload,
    // and its entries must not keep that read either.
    let mut copied = 0;
    for (at, frame) in &store.frames {
        if frame.payload.is_empty() || !frame.payload.is_unique() {
            continue;
        }
        copied += 1;
        assert!(
            frame.entries.is_empty() || frame.entries.is_unique(),
            "the entries of {at:?} keep the read its payload was copied out of"
        );
    }
    assert!(copied > 0, "some frames came out of a larger read");
    Ok(())
}
