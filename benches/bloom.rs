//! BuRR filter microbenches (construction + probe).
//!
//! The bench file is still named `bloom.rs` for git-history continuity —
//! the standard bloom filter it used to benchmark has been replaced by
//! BuRR (Bumped Ribbon Retrieval). Compare absolute numbers against the
//! pre-BuRR runs to track migration deltas.

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::hash::hash64;
use lsm_tree::table::filter::ribbon::burr::{
    BurrBuilder, BurrFilter, BurrFilterReader, BurrParams,
};
use rand::RngExt;
use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;

type Hasher = BuildHasherDefault<DefaultHasher>;

fn fast_block_index(c: &mut Criterion) {
    pub fn fast_impl(h: u64, num_blocks: usize) -> usize {
        // https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
        (((h >> 32).wrapping_mul(num_blocks as u64)) >> 32) as usize
    }

    let mut rng = rand::rng();
    let num_blocks = 100_000;

    c.bench_function("block index - mod", |b| {
        b.iter(|| {
            let h: u64 = rng.random();
            std::hint::black_box(h % (num_blocks as u64))
        });
    });

    c.bench_function("block index - fast", |b| {
        b.iter(|| {
            let h: u64 = rng.random();
            std::hint::black_box(fast_impl(h, num_blocks))
        });
    });
}

fn burr_filter_construction(c: &mut Criterion) {
    let mut rng = rand::rng();

    for n in [100_000_usize, 1_000_000] {
        let label = format!("burr filter build, {n} keys @ FPR=1%");
        c.bench_function(&label, |b| {
            // Pre-hash a key universe so the bench measures BuRR build cost,
            // not RNG.
            let mut keys = Vec::with_capacity(n);
            for _ in 0..n {
                let mut key = [0_u8; 16];
                rng.fill(&mut key[..]);
                keys.push(hash64(&key));
            }

            b.iter(|| {
                let params = BurrParams::with_fp_rate(n, 0.01).expect("params");
                let builder = BurrBuilder::new(params, Hasher::default()).expect("builder");
                let filter: BurrFilter<Hasher> = builder.build_from_hashes(&keys).expect("build");
                std::hint::black_box(filter.layer_count());
            });
        });
    }
}

fn burr_filter_contains(c: &mut Criterion) {
    let keys: Vec<Vec<u8>> = (0..100_000_u128)
        .map(|x| x.to_be_bytes().to_vec())
        .collect();

    for fpr in [0.01_f32, 0.001, 0.0001] {
        let n = 1_000_000_usize;

        let hashes: Vec<u64> = keys.iter().map(|k| hash64(k)).collect();
        // Pad to n with random hashes so the filter is sized realistically.
        let mut rng = rand::rng();
        let mut padded = hashes.clone();
        while padded.len() < n {
            padded.push(rng.random::<u64>());
        }

        let params = BurrParams::with_fp_rate(n, fpr).expect("params");
        let builder = BurrBuilder::new(params, Hasher::default()).expect("builder");
        let filter = builder.build_from_hashes(&padded).expect("build");
        let filter_bytes = filter.to_wire_bytes();

        // Long-lived reader matches the table read path (FilterBlock
        // pins the parsed view); construct ONCE outside b.iter to
        // measure steady-state probe latency, not parse+probe.
        let reader = BurrFilterReader::new(&filter_bytes).unwrap();
        c.bench_function(
            &format!(
                "burr filter contains (probe-only), true positive (FPR={}%)",
                fpr * 100.0
            ),
            |b| {
                b.iter(|| {
                    use rand::seq::IndexedRandom;
                    let sample = keys.choose(&mut rng).unwrap();
                    let hash = hash64(sample);
                    assert!(reader.contains_hash(hash));
                });
            },
        );

        // Separate decode+probe bench so the cost of parsing the wire
        // header is also visible — e.g. for callers that don't pin a
        // long-lived reader.
        c.bench_function(
            &format!(
                "burr filter contains (decode+probe), true positive (FPR={}%)",
                fpr * 100.0
            ),
            |b| {
                b.iter(|| {
                    use rand::seq::IndexedRandom;
                    let reader = BurrFilterReader::new(&filter_bytes).unwrap();
                    let sample = keys.choose(&mut rng).unwrap();
                    let hash = hash64(sample);
                    assert!(reader.contains_hash(hash));
                });
            },
        );
    }
}

/// Standard (single-layer) Ribbon contains_in bench — apples-to-apples
/// against BuRR's contains_hash so the BuRR multi-layer overhead vs
/// pure Ribbon stays visible.
fn ribbon_filter_contains(c: &mut Criterion) {
    use lsm_tree::table::filter::ribbon::{Mode, Params, RibbonBuilder};

    let keys: Vec<u64> = (0..100_000_u64).collect();

    for fpr in [0.01_f32, 0.001, 0.0001] {
        let n = 1_000_000_usize;
        // r = ceil(-log2(fpr)) — matches what BuRR picks internally.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "r is derived from bounded FPR inputs for this benchmark"
        )]
        #[expect(
            clippy::cast_sign_loss,
            reason = "(-log2(fpr)).ceil() is non-negative for fpr in (0, 1)"
        )]
        let r = (-fpr.log2()).ceil() as usize;
        let params = Params::new(n, 64, r, Mode::Standard)
            .expect("ribbon params")
            .with_seed(0);
        let builder = RibbonBuilder::new(params, Hasher::default()).expect("builder");
        // Pad the key set to n with random fillers so the Ribbon load
        // factor matches the BuRR bench above. Without padding the
        // Ribbon body would be ~10% loaded vs BuRR's ~70-90%, skewing
        // probe-latency conclusions.
        let mut rng = rand::rng();
        let mut padded = keys.clone();
        while padded.len() < n {
            padded.push(rng.random::<u64>());
        }
        let filter = builder.build(&padded).expect("build");
        let mut scratch = filter.new_scratch();

        c.bench_function(
            &format!(
                "standard ribbon contains, true positive (FPR={}%)",
                fpr * 100.0
            ),
            |b| {
                b.iter(|| {
                    use rand::seq::IndexedRandom;
                    let sample = keys.choose(&mut rng).unwrap();
                    assert!(filter.contains_in(sample, &mut scratch));
                });
            },
        );
    }
}

criterion_group!(
    benches,
    fast_block_index,
    burr_filter_construction,
    burr_filter_contains,
    ribbon_filter_contains,
);
criterion_main!(benches);
