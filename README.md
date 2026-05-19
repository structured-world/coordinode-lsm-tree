# coordinode-lsm-tree

[![CI](https://github.com/structured-world/coordinode-lsm-tree/actions/workflows/coordinode-ci.yml/badge.svg)](https://github.com/structured-world/coordinode-lsm-tree/actions/workflows/coordinode-ci.yml)
[![codecov](https://codecov.io/gh/structured-world/coordinode-lsm-tree/graph/badge.svg)](https://codecov.io/gh/structured-world/coordinode-lsm-tree)
[![Benchmarks](https://img.shields.io/badge/benchmarks-dashboard-orange)](https://structured-world.github.io/coordinode-lsm-tree/dev/bench/)
[![Crates.io](https://img.shields.io/crates/v/coordinode-lsm-tree?color=blue)](https://crates.io/crates/coordinode-lsm-tree)
[![docs.rs](https://img.shields.io/docsrs/coordinode-lsm-tree?color=green)](https://docs.rs/coordinode-lsm-tree)
![MSRV](https://img.shields.io/badge/MSRV-1.92.0-blue)
[![dependency status](https://deps.rs/repo/github/structured-world/coordinode-lsm-tree/status.svg)](https://deps.rs/repo/github/structured-world/coordinode-lsm-tree)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](#license)

LSM-tree storage engine in Rust. Embedded library; provides keyed point reads, prefix and range scans, MVCC snapshots, compaction, and a block cache. No write-ahead log — durability is the caller's responsibility. Built for [CoordiNode](https://github.com/structured-world/coordinode); usable standalone.

## Status

On-disk format version **V5**. V5 introduces a wire-format break for filter blocks (BuRR replaces Bloom); V3 and V4 databases are not readable by this version and vice versa. Versioning is single-monotonic — every breaking format change bumps to the next version with explicit migration notes.

## Features

### Read path

- Point reads via `get` / `multi_get` (batch-optimized).
- `PinnableSlice` for zero-copy reads.
- `BurrFilter` AMQ filter (Bumped Ribbon Retrieval, Walzer & Dillinger 2022): ~1% memory overhead vs the information-theoretic minimum — ~30% smaller filter blocks than a same-FPR Bloom filter, or ~10× tighter FPR at the same memory budget. Used for both per-key and per-prefix membership checks.
- Forward and reverse range / prefix iteration.
- Block cache with size cap.
- File-descriptor cache to bound `fopen` syscalls.

### Write path

- `WriteBatch` with seqno-grouped batch writes — caller-controlled atomic visibility.
- Single deletion tombstones (`remove_weak`).
- Range tombstones (`delete_range` / `delete_prefix`).
- Merge operators for commutative LSM operations.
- Optional key-value separation (BlobTree) for large-value workloads with automatic garbage collection.

### Compaction

- Leveled, size-tiered, dynamic-leveled, and FIFO strategies.
- Intra-L0 compaction for overlapping runs.
- Major compaction (full force flush + merge).
- Optional compaction filters for custom logic during compactions.
- Merge-aware compaction resolves operands lazily.

### Storage & encoding

- Block-based tables with optional compression (none / LZ4 / Zstd) and prefix truncation.
- Per-table data block size policy and per-table compression policy.
- Optional **zstd dictionary compression** — trained per-table or per-column for small (4-64 KiB) blocks and blob files.
- Optional **block-level encryption at rest** — AES-256-GCM, key supplied by caller.
- Optional per-table block hash indexes for faster point lookups [[3]](#footnotes).
- Optional partitioned block index & filters for better cache efficiency [[1]](#footnotes).
- Per-level filter/index block pinning configuration.

### Concurrency & API

- Thread-safe `BTreeMap`-like API.
- `SequenceNumberGenerator` trait — pluggable seqno source.
- Custom `UserComparator` for non-lexicographic ordering.
- MVCC: snapshot reads at a chosen `SeqNo`.

### Internals

- 100% stable Rust, MSRV 1.92.
- No FFI: zstd via [`structured-zstd`](https://github.com/structured-world/structured-zstd) (pure-Rust), LZ4 via `lz4_flex`, AES via `aes-gcm`.
- Pluggable `Fs` trait — back the engine on the standard filesystem, on `io_uring`, on an in-memory `MemFs`, or on a custom implementation.
- Pluggable `CompressionProvider` for third-party codecs.

## Limits

- Keys: up to 65 536 bytes.
- Values: up to 2³² bytes.
- Larger keys and values carry a proportional performance cost.

## Feature flags

| Flag | Default | Effect |
|---|---|---|
| `lz4` | off | LZ4 block compression via [`lz4_flex`](https://github.com/PSeitz/lz4_flex). |
| `zstd` | off | Zstd block compression via [`structured-zstd`](https://github.com/structured-world/structured-zstd). Supports `CompressionType::Zstd` and dictionary-mode `CompressionType::ZstdDict`. Decompression throughput is currently ~2–3.5× slower than the C reference implementation. |
| `encryption` | off | AES-256-GCM block encryption (`aes-gcm`). |
| `io-uring` | off (linux only) | `io_uring`-backed `Fs` implementation. |
| `bytes_1` | off | Use [`bytes`](https://github.com/tokio-rs/bytes) as the underlying `Slice` type. |
| `metrics` | off | Counters and timers exposed via the `Metrics` accessor. |
| `ribbon-serde` | off | Serde derives on the internal Ribbon filter representation. Not used by the on-disk format. |

## Benchmarks

CI runs [`db_bench`](tools/db_bench) on every push to `main` and on pull requests. Results from `main` are published to the [benchmark dashboard](https://structured-world.github.io/coordinode-lsm-tree/dev/bench/). PRs regressing performance by more than 15% trigger an alert; more than 25% fails CI.

Flamegraphs are generated on every merge to `main` from instrumented `db_bench` runs and published under `flamegraphs/<commit-sha>/flamegraph.svg` on [gh-pages](https://structured-world.github.io/coordinode-lsm-tree/).

Local Criterion microbenchmarks:

```bash
cargo bench --features lz4
```

Local flamegraphs:

```bash
cd tools/db_bench
cargo run --release --features flamegraph -- \
  --benchmark all --num 100000 --flamegraph --skip-calibration
# Folded stacks: target/flamegraphs/all.folded
# Render: cargo install inferno && inferno-flamegraph target/flamegraphs/all.folded > flame.svg
```

## Support the project

<div align="center">

![USDT TRC-20 Donation QR Code](assets/usdt-qr.svg)

USDT (TRC-20): `TFDsezHa1cBkoeZT5q2T49Wp66K8t2DmdA`

</div>

## License

All source code is licensed under Apache-2.0. Contributions are accepted under the same license.

Maintained by [Structured World Foundation](https://sw.foundation).

## Footnotes

[1] https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html

[2] https://github.com/facebook/rocksdb/wiki/BlobDB

[3] https://rocksdb.org/blog/2018/08/23/data-block-hash-index.html
