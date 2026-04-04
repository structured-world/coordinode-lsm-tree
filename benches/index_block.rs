#![expect(
    clippy::expect_used,
    reason = "benchmark setup favors concise panic messages"
)]

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use lsm_tree::{
    Cache, Checksum, DefaultUserComparator, DescriptorTable, InternalValue, SeqNo,
    SharedComparator, TableId, ValueType,
    fs::StdFs,
    table::{
        BlockHandle, BlockOffset, IndexBlock, KeyedBlockHandle, Table, Writer,
        filter::standard_bloom::Builder as BloomBuilder,
    },
};
use std::sync::Arc;
use tempfile::TempDir;

fn default_cmp() -> SharedComparator {
    Arc::new(DefaultUserComparator)
}

fn make_index_handles(count: usize) -> Vec<KeyedBlockHandle> {
    (0..count)
        .map(|i| {
            let key = format!("adj:out:vertex-0001:edge-{i:04}:target-0001");
            KeyedBlockHandle::new(
                key.into(),
                i as u64,
                BlockHandle::new(BlockOffset((i as u64) * 4096), 4096),
            )
        })
        .collect()
}

fn bench_index_block_seek(c: &mut Criterion) {
    let handles = make_index_handles(4096);
    let needle = b"adj:out:vertex-0001:edge-2048:target-0001";

    let mut group = c.benchmark_group("index_block_seek");

    for restart_interval in [1u8, 4, 16] {
        let cmp = default_cmp();
        let bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, restart_interval)
            .expect("index block should encode");
        #[expect(
            clippy::cast_possible_truncation,
            reason = "4096 handles produce ~200KB, well under u32::MAX"
        )]
        let data_len = bytes.len() as u32;
        let block = IndexBlock::new(lsm_tree::table::Block {
            data: bytes.into(),
            header: lsm_tree::table::block::Header {
                block_type: lsm_tree::table::block::BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: data_len,
                uncompressed_length: data_len,
            },
        });

        group.bench_with_input(
            BenchmarkId::from_parameter(restart_interval),
            &restart_interval,
            |b, &_restart_interval| {
                b.iter(|| {
                    let mut iter = block.iter(cmp.clone());
                    assert!(iter.seek(needle, SeqNo::MAX));
                    assert!(iter.next().is_some());
                });
            },
        );
    }

    group.finish();
}

struct BenchTable {
    _dir: TempDir,
    key: Vec<u8>,
    key_hash: u64,
    table: Table,
}

fn build_table_for_point_read(restart_interval: u8) -> BenchTable {
    let dir = tempfile::tempdir().expect("tempdir should be created");
    let path = dir.path().join("table.sst");

    let mut writer = Writer::new(path.clone(), TableId::default(), 0, Arc::new(StdFs))
        .expect("writer should be created")
        .use_data_block_size(256)
        .use_index_block_restart_interval(restart_interval);

    for i in 0..4096u32 {
        let key = format!("adj:out:vertex-0001:edge-{i:04}:target-0001");
        let value = format!("payload-{i:04}");
        writer
            .write(InternalValue::from_components(
                key.as_bytes(),
                value.as_bytes(),
                i as u64,
                ValueType::Value,
            ))
            .expect("item should be written");
    }

    let (_, checksum) = writer
        .finish()
        .expect("finish should succeed")
        .expect("table should be non-empty");

    let key = b"adj:out:vertex-0001:edge-2048:target-0001".to_vec();
    let table = Table::recover(
        path,
        checksum,
        0,
        0,
        Arc::new(Cache::with_capacity_bytes(1_000_000)),
        Some(Arc::new(DescriptorTable::new(8))),
        Arc::new(lsm_tree::fs::StdFs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        default_cmp(),
        #[cfg(feature = "metrics")]
        Arc::new(lsm_tree::Metrics::default()),
    )
    .expect("table should recover");

    BenchTable {
        _dir: dir,
        key_hash: BloomBuilder::get_hash(&key),
        key,
        table,
    }
}

fn bench_table_point_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_point_read_index_restart_interval");

    for restart_interval in [1u8, 4, 16] {
        let bench_table = build_table_for_point_read(restart_interval);

        group.bench_with_input(
            BenchmarkId::from_parameter(restart_interval),
            &bench_table,
            |b, bench_table| {
                b.iter(|| {
                    let value = bench_table
                        .table
                        .get(&bench_table.key, SeqNo::MAX, bench_table.key_hash)
                        .expect("point read should succeed");
                    assert!(value.is_some());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_index_block_seek, bench_table_point_read);
criterion_main!(benches);
