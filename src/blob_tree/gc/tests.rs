use super::*;
use crate::HashMap;
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
        FragmentationEntry {
            len: 1,
            bytes: 1_000,
            on_disk_bytes: 500,
        },
    );
    map.0.insert(
        1,
        FragmentationEntry {
            len: 2,
            bytes: 2_000,
            on_disk_bytes: 1_000,
        },
    );

    // test merge_into
    let mut diff = FragmentationMap(HashMap::default());
    diff.0.insert(
        0,
        FragmentationEntry {
            len: 3,
            bytes: 3_000,
            on_disk_bytes: 1_500,
        },
    );
    diff.0.insert(
        3,
        FragmentationEntry {
            len: 4,
            bytes: 4_000,
            on_disk_bytes: 2_000,
        },
    );

    diff.merge_into(&mut map);

    assert_eq!(map.0.len(), 3);
    assert_eq!(map.0[&0].len, 4);
    assert_eq!(map.0[&0].bytes, 4_000);
    assert_eq!(map.0[&0].on_disk_bytes, 2_000);
    assert_eq!(map.0[&1].len, 2);
    assert_eq!(map.0[&1].bytes, 2_000);
    assert_eq!(map.0[&1].on_disk_bytes, 1_000);
    assert_eq!(map.0[&3].len, 4);
    assert_eq!(map.0[&3].bytes, 4_000);
    assert_eq!(map.0[&3].on_disk_bytes, 2_000);
}

#[test]
fn frag_map_roundtrip() {
    let map = FragmentationMap({
        let mut map = HashMap::default();
        map.insert(
            0,
            FragmentationEntry {
                len: 1,
                bytes: 1_000,
                on_disk_bytes: 500,
            },
        );
        map.insert(
            1,
            FragmentationEntry {
                len: 2,
                bytes: 2_000,
                on_disk_bytes: 1_000,
            },
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
          vhandle: ValueHandle {
            blob_file_id: 0,
            on_disk_size: 500,
            offset: 0,
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
            map.insert(
                0,
                FragmentationEntry {
                    len: 1,
                    bytes: 1_000,
                    on_disk_bytes: 500,
                },
            );
            map
        },
        my_watcher.0,
    );

    Ok(())
}
