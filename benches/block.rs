// TODO: These benchmarks used ValueBlock (now DataBlock) and other internal
// types that were restructured. DataBlock no longer exposes items/header
// directly — the block format changed to raw bytes with binary index.
// Needs rewrite against the current DataBlock API.

use criterion::{criterion_group, criterion_main, Criterion};

fn placeholder(_c: &mut Criterion) {}

criterion_group!(benches, placeholder);
criterion_main!(benches);
