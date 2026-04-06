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
                std::hint::black_box(tree.multi_get(&keys, SeqNo::MAX).unwrap());
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
            std::hint::black_box(tree.get_pinned("key_000500", SeqNo::MAX).unwrap());
        });
    });

    // Compare with regular get
    group.bench_function("get_regular", |b| {
        b.iter(|| {
            std::hint::black_box(tree.get("key_000500", SeqNo::MAX).unwrap());
        });
    });

    group.finish();
}

fn setup_empty_tree() -> (AnyTree, tempfile::TempDir) {
    let folder = tempfile::tempdir().unwrap();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .unwrap();
    (tree, folder)
}

fn bench_write_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_batch");

    for batch_size in [10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("insert", batch_size),
            &batch_size,
            |b, &size| {
                let keys: Vec<String> = (0..size).map(|i| format!("key_{i:04}")).collect();

                b.iter_batched(
                    setup_empty_tree,
                    |(tree, _folder)| {
                        let mut batch = WriteBatch::with_capacity(size);
                        for k in &keys {
                            batch.insert(k.as_str(), "value");
                        }
                        tree.apply_batch(batch, 0).unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    // Compare with individual inserts
    for batch_size in [10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("individual_inserts", batch_size),
            &batch_size,
            |b, &size| {
                let keys: Vec<String> = (0..size).map(|i| format!("key_{i:04}")).collect();

                b.iter_batched(
                    setup_empty_tree,
                    |(tree, _folder)| {
                        for k in &keys {
                            tree.insert(k.as_str(), "value", 0);
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
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
