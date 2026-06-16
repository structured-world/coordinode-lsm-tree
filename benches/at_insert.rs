// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Write-throughput cost of the per-KV insert-time digest
//! (`KvChecksumComputePoint::AtInsert`).
//!
//! Three head-to-head insert + flush arms over the same workload:
//!   - `off`: no per-KV checksums (the default, zero-overhead path). This is
//!     also the pre-feature baseline: with the digest path disabled the insert
//!     does only a cheap runtime-config load and two enum checks.
//!   - `at_block_compile`: `kv_checksums = AllLevels` with the default
//!     `AtBlockCompile`: per-KV footer on disk, no memtable-insert cost.
//!   - `at_insert`: `AllLevels` + `AtInsert` + a 4-byte algorithm: the digest
//!     is computed at insert and verified at flush (the residence check).
//!
//! The residence check is a deliberate extra hash pass over the memtable at
//! flush: it verifies the AS-INSERTED entries (pre-merge), which is a distinct
//! scope from the on-disk footer digest (computed over post-merge / post-filter
//! bytes by the writer). The two cannot be fused without breaking correctness
//! under a merge operator, where the flushed value differs from any single
//! inserted value. So `at_insert` carries the per-insert digest cost plus this
//! one verify pass; `off` is indistinguishable from a no-per-KV baseline (the
//! digest path never runs).

#![expect(
    clippy::expect_used,
    reason = "benchmark setup favors concise panic messages"
)]

use criterion::{Criterion, criterion_group, criterion_main};
use lsm_tree::runtime_config::{
    ChecksumAlgorithm, KvChecksumComputePoint, KvChecksumPolicy, RuntimeConfig,
};
use lsm_tree::{AbstractTree, AnyTree, Config, SequenceNumberCounter, Tree};
use tempfile::TempDir;

/// Opens a fresh tree configured with the given per-KV checksum knobs.
fn open_tree(
    policy: KvChecksumPolicy,
    algo: ChecksumAlgorithm,
    compute_point: KvChecksumComputePoint,
) -> (TempDir, Tree) {
    let dir = TempDir::new().expect("tempdir");
    let mut rc = RuntimeConfig::default();
    rc.kv_checksums = policy;
    rc.kv_checksum_algo = algo;
    rc.kv_checksum_compute_point = compute_point;
    let any = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_runtime_config(rc)
    .open()
    .expect("open");
    let tree = match any {
        AnyTree::Standard(t) => t,
        AnyTree::Blob(_) => panic!("expected Standard tree"),
    };
    (dir, tree)
}

/// Insert `n` entries and flush once (the flush runs the AtInsert residence
/// verify when that arm is active).
fn insert_and_flush(tree: &Tree, n: u64) {
    for i in 0..n {
        tree.insert(format!("key{i:08}").as_bytes(), b"value-payload", i);
    }
    tree.flush_active_memtable(0).expect("flush");
}

fn bench_at_insert_write(c: &mut Criterion) {
    let n = 20_000u64;
    let arms: &[(
        &str,
        KvChecksumPolicy,
        ChecksumAlgorithm,
        KvChecksumComputePoint,
    )] = &[
        (
            "off",
            KvChecksumPolicy::Off,
            ChecksumAlgorithm::Xxh3_64,
            KvChecksumComputePoint::AtBlockCompile,
        ),
        (
            "at_block_compile",
            KvChecksumPolicy::AllLevels,
            ChecksumAlgorithm::Xxh3Low32,
            KvChecksumComputePoint::AtBlockCompile,
        ),
        (
            "at_insert",
            KvChecksumPolicy::AllLevels,
            ChecksumAlgorithm::Xxh3Low32,
            KvChecksumComputePoint::AtInsert,
        ),
    ];

    let mut group = c.benchmark_group("at_insert/write_throughput");
    for &(label, policy, algo, cp) in arms {
        group.bench_function(label, |b| {
            b.iter_custom(|iters| {
                use std::time::Instant;
                let started = Instant::now();
                for _ in 0..iters {
                    let (dir, tree) = open_tree(policy, algo, cp);
                    insert_and_flush(&tree, n);
                    std::hint::black_box(&dir);
                }
                started.elapsed()
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_at_insert_write);
criterion_main!(benches);
