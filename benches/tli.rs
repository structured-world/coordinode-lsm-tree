// TODO: This benchmark used TopLevelIndex and KeyedBlockIndex which were
// removed during the block_index → index_block restructuring. The index_block
// module is pub(crate) and does not expose TopLevelIndex externally.
// Needs rewrite against the current IndexBlock/KeyedBlockHandle API.

use criterion::{criterion_group, criterion_main, Criterion};

fn placeholder(_c: &mut Criterion) {}

criterion_group!(benches, placeholder);
criterion_main!(benches);
