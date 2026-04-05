use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use lsm_tree::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter, WriteBatch};

fn setup_tree_with_disk_data(n: u64) -> (AnyTree, tempfile::TempDir) {
    let folder = tempfile::tempdir().unwrap();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .unwrap();

    for i in 0..n {
        tree.insert(format!("key_{i:06}"), format!("value_{i}"), i);
    }
    tree.flush_active_memtable(0).unwrap();

    (tree, folder)
}

fn bench_multi_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_get");

    for count in [10, 50, 100, 500] {
        let (tree, _folder) = setup_tree_with_disk_data(1000);

        let keys: Vec<String> = (0..count).map(|i| format!("key_{i:06}")).collect();

        group.bench_with_input(BenchmarkId::new("disk", count), &count, |b, _| {
            b.iter(|| {
                tree.multi_get(&keys, SeqNo::MAX).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_get_pinned(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_pinned");

    let (tree, _folder) = setup_tree_with_disk_data(1000);

    group.bench_function("disk_hit", |b| {
        b.iter(|| {
            tree.get_pinned("key_000500", SeqNo::MAX).unwrap();
        });
    });

    // Compare with regular get
    group.bench_function("get_regular", |b| {
        b.iter(|| {
            tree.get("key_000500", SeqNo::MAX).unwrap();
        });
    });

    group.finish();
}

fn bench_write_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_batch");

    for batch_size in [10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("insert", batch_size),
            &batch_size,
            |b, &size| {
                let folder = tempfile::tempdir().unwrap();
                let tree = Config::new(
                    &folder,
                    SequenceNumberCounter::default(),
                    SequenceNumberCounter::default(),
                )
                .open()
                .unwrap();

                let mut seqno = 0u64;

                b.iter(|| {
                    let mut batch = WriteBatch::with_capacity(size);
                    for i in 0..size {
                        batch.insert(format!("key_{seqno}_{i:04}"), format!("value_{i}"));
                    }
                    tree.apply_batch(batch, seqno).unwrap();
                    seqno += 1;
                });
            },
        );
    }

    // Compare with individual inserts
    for batch_size in [10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("individual_inserts", batch_size),
            &batch_size,
            |b, &size| {
                let folder = tempfile::tempdir().unwrap();
                let tree = Config::new(
                    &folder,
                    SequenceNumberCounter::default(),
                    SequenceNumberCounter::default(),
                )
                .open()
                .unwrap();

                let mut seqno = 0u64;

                b.iter(|| {
                    for i in 0..size {
                        tree.insert(format!("key_{seqno}_{i:04}"), format!("value_{i}"), seqno);
                    }
                    seqno += 1;
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_multi_get,
    bench_get_pinned,
    bench_write_batch
);
criterion_main!(benches);
