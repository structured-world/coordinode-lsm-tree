use super::*;
use crate::{
    comparator::default_comparator,
    table::BlockHandle,
    table::block::{BlockOffset, BlockType, Decoder, Header},
};

/// Builds an IndexBlock containing entries with the given keys (seqno=0 for all).
fn make_index_block(keys: &[&[u8]], restart_interval: u8) -> IndexBlock {
    let items: Vec<KeyedBlockHandle> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            KeyedBlockHandle::new(
                (*k).into(),
                0,
                BlockHandle::new(BlockOffset(i as u64 * 100), 100),
            )
        })
        .collect();

    let bytes =
        IndexBlock::encode_into_vec_with_restart_interval(&items, restart_interval).unwrap();
    let data_len = bytes.len() as u32;
    IndexBlock::new(crate::table::block::Block {
        data: bytes.into(),
        header: Header {
            data_length: data_len,
            uncompressed_length: data_len,
            ..Header::test_dummy(BlockType::Index)
        },
    })
}

#[test]
fn from_block_iterates_all_entries() {
    let block = make_index_block(&[b"a", b"b", b"c"], 1);
    let mut iter = OwnedIndexBlockIter::from_block(block, default_comparator()).unwrap();

    let keys: Vec<_> = iter.by_ref().map(|h| h.end_key().to_vec()).collect();
    assert_eq!(keys, vec![b"a", b"b", b"c"]);
}

#[test]
fn from_validated_block_after_prevalidation_iterates_all_entries() {
    let block = make_index_block(&[b"a", b"b", b"c"], 1);

    // Pre-validate: mirrors what FullBlockIndex::new does.
    Decoder::<KeyedBlockHandle, crate::table::index_block::IndexBlockParsedItem>::try_new(
        &block.inner,
    )
    .unwrap();

    let iter = OwnedIndexBlockIter::from_validated_block(block, default_comparator());

    let keys: Vec<_> = iter.map(|h| h.end_key().to_vec()).collect();
    assert_eq!(keys, vec![b"a", b"b", b"c"]);
}

#[test]
fn from_block_with_bounds_no_bounds_returns_all() {
    let block = make_index_block(&[b"a", b"b", b"c"], 1);
    let iter = OwnedIndexBlockIter::from_block_with_bounds(block, default_comparator(), None, None)
        .unwrap();

    assert!(iter.is_some());
    let keys: Vec<_> = iter.unwrap().map(|h| h.end_key().to_vec()).collect();
    assert_eq!(keys, vec![b"a", b"b", b"c"]);
}

#[test]
fn from_block_with_bounds_lo_bound_seeks_forward() {
    let block = make_index_block(&[b"a", b"b", b"c"], 1);
    let iter = OwnedIndexBlockIter::from_block_with_bounds(
        block,
        default_comparator(),
        Some((b"b", SeqNo::MAX)),
        None,
    )
    .unwrap();

    assert!(iter.is_some());
    let keys: Vec<_> = iter.unwrap().map(|h| h.end_key().to_vec()).collect();
    assert_eq!(keys, vec![b"b", b"c"]);
}

#[test]
fn from_block_with_bounds_hi_bound_sets_back_cursor() {
    // For restart_interval=1, seek_upper primarily positions the
    // decoder's back-end cursor.
    let block = make_index_block(&[b"a", b"b", b"c", b"d"], 1);
    let mut iter = OwnedIndexBlockIter::from_block_with_bounds(
        block,
        default_comparator(),
        None,
        Some((b"c", 0)),
    )
    .unwrap()
    .unwrap();

    // Forward iteration still starts from the beginning
    assert_eq!(iter.next().unwrap().end_key().as_ref(), b"a");

    // seek_upper("c", 0) positions the back cursor at the first block
    // with end_key > "c", which is "d". next_back yields from there downward.
    assert_eq!(iter.next_back().unwrap().end_key().as_ref(), b"d");
    assert_eq!(iter.next_back().unwrap().end_key().as_ref(), b"c");
    assert_eq!(iter.next_back().unwrap().end_key().as_ref(), b"b");
    assert!(iter.next_back().is_none());
}

#[test]
fn from_block_with_bounds_both_bounds() {
    // Include trailing key "e" so the hi bound actually clips the sequence;
    // with [a,b,c,d] and hi="c", "d" is already the tail and a broken
    // upper-bound path would still pass.
    let block = make_index_block(&[b"a", b"b", b"c", b"d", b"e"], 1);
    let iter = OwnedIndexBlockIter::from_block_with_bounds(
        block,
        default_comparator(),
        Some((b"b", SeqNo::MAX)),
        Some((b"c", 0)),
    )
    .unwrap()
    .unwrap();

    let keys: Vec<_> = iter.map(|h| h.end_key().to_vec()).collect();
    assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
}

#[test]
fn from_block_with_bounds_compressed_both_bounds() {
    // Exercise the seek_lower_bound_cursor → seek_upper_bound_cursor
    // sequence with restart_interval > 1 to cover the compressed-interval
    // trim_back_to_upper_bound + advance_upper_restart_interval path.
    let block = make_index_block(&[b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"], 4);
    let iter = OwnedIndexBlockIter::from_block_with_bounds(
        block,
        default_comparator(),
        Some((b"c", SeqNo::MAX)),
        Some((b"f", 0)),
    )
    .unwrap()
    .unwrap();

    let keys: Vec<_> = iter.map(|h| h.end_key().to_vec()).collect();
    assert_eq!(
        keys,
        vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec(), b"f".to_vec(),]
    );
}

#[test]
fn from_block_with_bounds_lo_past_end_returns_none() {
    let block = make_index_block(&[b"a", b"b"], 1);
    let iter = OwnedIndexBlockIter::from_block_with_bounds(
        block,
        default_comparator(),
        Some((b"z", SeqNo::MAX)),
        None,
    )
    .unwrap();

    assert!(iter.is_none());
}

#[test]
fn from_block_with_bounds_inverted_bounds_returns_none() {
    let block = make_index_block(&[b"a", b"b", b"c", b"d", b"e"], 1);
    let iter = OwnedIndexBlockIter::from_block_with_bounds(
        block,
        default_comparator(),
        Some((b"d", SeqNo::MAX)),
        Some((b"b", 0)),
    )
    .unwrap();

    assert!(iter.is_none(), "inverted lo > hi must return None");
}

#[test]
fn from_block_with_bounds_restart_interval_gt_one() {
    let block = make_index_block(
        &[
            b"adj:out:vertex-0001:edge-0000",
            b"adj:out:vertex-0001:edge-0001",
            b"adj:out:vertex-0001:edge-0002",
            b"adj:out:vertex-0001:edge-0003",
            b"adj:out:vertex-0001:edge-0004",
            b"adj:out:vertex-0001:edge-0005",
        ],
        4,
    );

    let iter = OwnedIndexBlockIter::from_block_with_bounds(
        block,
        default_comparator(),
        Some((b"adj:out:vertex-0001:edge-0002", SeqNo::MAX)),
        None,
    )
    .unwrap();

    assert!(iter.is_some());
    let keys: Vec<_> = iter.unwrap().map(|h| h.end_key().to_vec()).collect();
    assert_eq!(
        keys,
        vec![
            b"adj:out:vertex-0001:edge-0002".to_vec(),
            b"adj:out:vertex-0001:edge-0003".to_vec(),
            b"adj:out:vertex-0001:edge-0004".to_vec(),
            b"adj:out:vertex-0001:edge-0005".to_vec(),
        ]
    );
}

#[test]
fn from_block_with_upper_bound_restart_interval_gt_one() {
    let block = make_index_block(
        &[
            b"adj:out:vertex-0001:edge-0000",
            b"adj:out:vertex-0001:edge-0001",
            b"adj:out:vertex-0001:edge-0002",
            b"adj:out:vertex-0001:edge-0003",
            b"adj:out:vertex-0001:edge-0004",
            b"adj:out:vertex-0001:edge-0005",
        ],
        4,
    );

    let mut iter = OwnedIndexBlockIter::from_block_with_bounds(
        block,
        default_comparator(),
        None,
        Some((b"adj:out:vertex-0001:edge-0002", 0)),
    )
    .unwrap()
    .unwrap();

    let keys: Vec<_> =
        std::iter::from_fn(|| iter.next_back().map(|h| h.end_key().to_vec())).collect();
    assert_eq!(
        keys,
        vec![
            b"adj:out:vertex-0001:edge-0002".to_vec(),
            b"adj:out:vertex-0001:edge-0001".to_vec(),
            b"adj:out:vertex-0001:edge-0000".to_vec(),
        ]
    );
}

#[test]
fn seek_upper_with_equal_end_keys_keeps_full_forward_limit_span() {
    let block = make_index_block(&[b"k", b"k", b"k", b"k"], 1);
    let mut iter = OwnedIndexBlockIter::from_block(block, default_comparator()).unwrap();

    assert!(iter.seek_upper(b"k", SeqNo::MAX));

    let keys: Vec<_> = iter.map(|h| h.end_key().to_vec()).collect();
    assert_eq!(
        keys,
        vec![b"k".to_vec(), b"k".to_vec(), b"k".to_vec(), b"k".to_vec()]
    );
}
