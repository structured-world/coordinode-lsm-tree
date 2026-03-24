// TODO: The binary_search module was removed from public API.
// partition_point is now an internal utility. Rewrite needed if
// benchmarking custom binary search is still desired.

use criterion::{criterion_group, criterion_main, Criterion};

fn placeholder(_c: &mut Criterion) {}

criterion_group!(benches, placeholder);
criterion_main!(benches);
