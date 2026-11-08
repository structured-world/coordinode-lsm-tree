use super::*;
use crate::HashMap;
use crate::blob_tree::handle::Stored;
use crate::{
    ValueType,
    coding::{Decode, Encode},
    compaction::stream::CompactionStream,
    value::InternalValue,
    vlog::ValueHandle,
};
use test_log::test;

#[test]
fn frag_map_merge_into() {
    let mut map = FragmentationMap(HashMap::default());
    map.0.insert(
        0,
        FragmentationEntry::new(1, 1_000, 500).with_block_bytes(10),
    );
    map.0.insert(1, FragmentationEntry::new(2, 2_000, 1_000));

    // test merge_into
    let mut diff = FragmentationMap(HashMap::default());
    diff.0.insert(
        0,
        FragmentationEntry::new(3, 3_000, 1_500).with_block_bytes(30),
    );
    diff.0.insert(3, FragmentationEntry::new(4, 4_000, 2_000));

    diff.merge_into(&mut map);

    assert_eq!(map.0.len(), 3);
    assert_eq!(
        map.0[&0],
        FragmentationEntry::new(4, 4_000, 2_000).with_block_bytes(40)
    );
    assert_eq!(map.0[&1], FragmentationEntry::new(2, 2_000, 1_000));
    assert_eq!(map.0[&3], FragmentationEntry::new(4, 4_000, 2_000));
}

/// Both halves of an entry, frames of their own and block entries, survive a
/// round trip through the manifest's encoding.
#[test]
fn frag_map_roundtrip() {
    let map = FragmentationMap({
        let mut map = HashMap::default();
        map.insert(0, FragmentationEntry::new(1, 1_000, 500));
        map.insert(
            1,
            FragmentationEntry::new(2, 2_000, 1_000).with_block_bytes(77),
        );
        map
    });

    let encoded = map.encode_into_vec();
    let decoded = FragmentationMap::decode_from(&mut &encoded[..]).expect("should decode map");
    assert_eq!(map, decoded);
}

#[test]
#[expect(clippy::unwrap_used)]
fn compaction_stream_gc_count_drops() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = &[
        InternalValue::from_components("a", b"abc", 1, ValueType::Value),

        InternalValue::from_components("a", BlobIndirection {
          size: 1000,
          stored: Stored::Frames(500),
          vhandle: ValueHandle {
            blob_file_id: 0,
            frame: 0,
            index: 0,
          }
        }.encode_into_vec(), 0, ValueType::Indirection),
    ];

    let mut my_watcher = FragmentationMap::default();

    let iter = vec.iter().cloned().map(Ok);
    let mut iter = CompactionStream::new(iter, 1_000).with_drop_callback(&mut my_watcher);

    assert_eq!(
        // TODO: Seqno is normally reset to 0
        InternalValue::from_components(*b"a", b"abc", 1, ValueType::Value),
        iter.next().unwrap()?,
    );

    assert_eq!(
        {
            let mut map = HashMap::default();
            map.insert(0, FragmentationEntry::new(1, 1_000, 500));
            map
        },
        my_watcher.0,
    );

    Ok(())
}

/// A dropped value that shares a block is charged its uncompressed entry as
/// block bytes, apart from the exact bytes of values with frames of their
/// own, so its share of the compressed block can be told later.
#[test]
fn a_dropped_value_in_a_block_charges_its_entry_as_block_bytes() {
    let at = |frame, stored| BlobIndirection {
        vhandle: ValueHandle {
            blob_file_id: 4,
            frame,
            index: 0,
        },
        size: 10,
        stored,
    };
    let mut map = FragmentationMap::default();
    map.charge(&at(0, Stored::InBlock(25)));
    map.charge(&at(1, Stored::Frames(60)));
    map.charge(&at(2, Stored::InBlock(30)));
    assert_eq!(
        map.0.get(&4),
        Some(&FragmentationEntry::new(3, 30, 60).with_block_bytes(55))
    );
}

/// A dropped cell row charges only the objects it owns: a borrowed object is
/// charged through its owner, once, or a file holding it would be charged
/// twice its share and could reach its size while other objects in it live.
#[test]
fn a_dropped_cell_row_charges_only_its_owned_objects() -> crate::Result<()> {
    use crate::blob_tree::field_row::{RowCell, RowField, encode_row};

    let at = |frame, size| BlobIndirection {
        vhandle: ValueHandle {
            blob_file_id: 3,
            frame,
            index: 0,
        },
        size,
        stored: Stored::Frames(size),
    };
    let row = encode_row(&[
        RowField::bytes(
            3,
            RowCell::Ref {
                indirection: at(0, 100),
                owner: true,
            },
        ),
        RowField::bytes(4, RowCell::Value(b"status")),
        RowField::bytes(
            5,
            RowCell::Ref {
                indirection: at(1, 40),
                owner: false,
            },
        ),
    ])?;

    let mut map = FragmentationMap::default();
    map.on_dropped(&InternalValue::from_components(
        "k",
        row,
        5,
        ValueType::CellRow,
    ));
    assert_eq!(map.0.get(&3), Some(&FragmentationEntry::new(1, 100, 100)));
    Ok(())
}
