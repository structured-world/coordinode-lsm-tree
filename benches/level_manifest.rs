// TODO: These benchmarks accessed `tree.levels` field directly which is no
// longer public. Level iteration moved to `Version::iter_levels()` which is
// internal. Needs rewrite to benchmark level manifest performance through
// the public API.

use criterion::{criterion_group, criterion_main, Criterion};

fn placeholder(_c: &mut Criterion) {}

criterion_group!(benches, placeholder);
criterion_main!(benches);
