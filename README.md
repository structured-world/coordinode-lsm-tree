# coordinode-lsm-tree

[![CI](https://github.com/structured-world/coordinode-lsm-tree/actions/workflows/coordinode-ci.yml/badge.svg)](https://github.com/structured-world/coordinode-lsm-tree/actions/workflows/coordinode-ci.yml)
[![codecov](https://codecov.io/gh/structured-world/coordinode-lsm-tree/graph/badge.svg)](https://codecov.io/gh/structured-world/coordinode-lsm-tree)
[![Benchmarks](https://img.shields.io/badge/benchmarks-dashboard-orange)](https://structured-world.github.io/coordinode-lsm-tree/dev/bench/)
[![Crates.io](https://img.shields.io/crates/v/coordinode-lsm-tree?color=blue)](https://crates.io/crates/coordinode-lsm-tree)
[![docs.rs](https://img.shields.io/docsrs/coordinode-lsm-tree?color=green)](https://docs.rs/coordinode-lsm-tree)
![MSRV](https://img.shields.io/badge/MSRV-1.92.0-blue)
[![dependency status](https://deps.rs/repo/github/structured-world/coordinode-lsm-tree/status.svg)](https://deps.rs/repo/github/structured-world/coordinode-lsm-tree)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](#license)

LSM-tree storage engine in Rust. Embedded library; provides keyed point reads, prefix and range scans, MVCC snapshots, compaction, and a block cache. No write-ahead log: durability is the caller's responsibility. Built for [CoordiNode](https://github.com/structured-world/coordinode); usable standalone.

## Status

On-disk format version **V5**. V5 introduces a wire-format break for filter blocks (BuRR replaces Bloom); V3 and V4 databases are not readable by this version and vice versa. Versioning is single-monotonic: every breaking format change bumps to the next version with explicit migration notes.

## Features

### Read path

- Point reads via `get` / `multi_get` (batch-optimized).
- `PinnableSlice` for zero-copy reads.
- `BurrFilter` AMQ filter (Bumped Ribbon Retrieval, Walzer & Dillinger 2022): ~1% memory overhead vs the information-theoretic minimum: ~30% smaller filter blocks than a same-FPR Bloom filter, or ~10× tighter FPR at the same memory budget. Used for both per-key and per-prefix membership checks.
- Forward and reverse range / prefix iteration.
- Block cache with size cap.
- File-descriptor cache to bound `fopen` syscalls.

### Write path

- `WriteBatch` with seqno-grouped batch writes: caller-controlled atomic visibility.
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
- Optional **zstd dictionary compression**, trained per-table or per-column for small (4-64 KiB) blocks and blob files.
- Optional **columnar PAX block layout** (`columnar` feature): per-row-group column-major storage of key / seqno / value-type / value sub-columns, for column-projection pushdown, vectorized predicate scans, per-column dictionaries, and positional delete-bitmap MVCC.
- Optional **block-level encryption at rest**: AES-256-GCM, key supplied by caller.
- Optional **per-block error-correcting parity** (`page_ecc` feature): Reed-Solomon shards or per-word SEC-DED after each block, with on-read self-healing, three-state verification, and a patrol scrub for latent bit-rot.
- Optional per-table block hash indexes for faster point lookups [[3]](#footnotes).
- Optional partitioned block index & filters for better cache efficiency [[1]](#footnotes).
- Per-level filter/index block pinning configuration.

### Concurrency & API

- Thread-safe `BTreeMap`-like API.
- `SequenceNumberGenerator` trait: pluggable seqno source.
- Custom `UserComparator` for non-lexicographic ordering.
- MVCC: snapshot reads at a chosen `SeqNo`.
- Point-in-time recovery snapshots via `Tree::create_checkpoint`: hard-link
  every live SST + blob file into a fresh directory in O(1) per file, zero
  extra disk until the source files compact away. Compaction continues
  during the checkpoint (deletions are deferred), and the resulting
  directory opens as an independent tree.

### Introspection

- Storage footprint and entry shape (`storage_stats`): used / capacity / available bytes, item & table counts, average on-disk entry size, reclaimable-bytes estimate, and a coarse storage status.
- Per-level and per-segment size / entry / access stats (`level_segment_stats`) for tiering and placement decisions.
- Approximate range size (`approximate_range_stats`) and cardinality + selectivity (`approximate_range_cardinality`) for query planning, estimated from block-index offsets and per-block zone maps without reading data blocks.
- Compaction-debt estimate (`compaction_debt`): pending-compaction bytes above each level's target, a scheduler / tiering signal.

### Internals

- 100% stable Rust, MSRV 1.92.
- `no_std` + `alloc` support: the core engine (read / write / compaction / recovery over the injected `Fs`) compiles without `std`; std-only conveniences (threaded fan-out, system clock, the std filesystem backend) stay behind the `std` feature.
- No FFI: zstd via [`structured-zstd`](https://github.com/structured-world/structured-zstd) (pure-Rust), LZ4 via `lz4_flex`, AES via `aes-gcm`.
- Pluggable `Fs` trait: back the engine on the standard filesystem, on `io_uring`, on an in-memory `MemFs`, or on a custom implementation.
- Pluggable `CompressionProvider` for third-party codecs.

## Incremental scan / CDC

`Tree::scan_since_seqno(target)` streams every change committed at or after a
sequence number as `ScanSinceEvent`s (`Insert`, `MergeOperand`,
`PointTombstone`, `RangeTombstone`), in increasing seqno order. It is a
change-data-capture primitive: a downstream consumer (replica, Kafka connector,
Debezium-style pipeline) replays the events in order to reconstruct the source's
history. Superseded versions are preserved (no MVCC collapse) and tombstones are
exposed, so deletes replay faithfully.

With the runtime-toggleable `seqno_in_index` policy, each SST index entry
carries its data block's `(seqno_min, seqno_max)`, and the scan **skips any data
block whose `seqno_max` is below the target without reading it**. On a
sparse-change workload (e.g. 1% of data changed since the last poll) this turns
an O(data) scan into O(changed blocks). Trees with a mix of seqno-bounded and
legacy SSTs are scanned transparently (legacy blocks fall back to a per-entry
filter), so the policy can be enabled on a live tree and takes effect as
compaction rewrites SSTs: no migration step, no format-version bump.

| Engine | CDC granularity | Survives compaction | Embeddable | Block-skip |
|--------|-----------------|---------------------|------------|------------|
| RocksDB `GetUpdatesSince` | WAL events (lost after compaction) | no | yes | n/a |
| Pebble | SST file (64–128 MB) | yes | yes | no |
| CockroachDB changefeed | SST file | yes | no (distributed) | no |
| FoundationDB | per-event | yes | no (distributed) | n/a |
| **coordinode-lsm-tree** | **data block (4–32 KB)** | **yes** | **yes** | **yes** |

The trade-off: there is no write-ahead log, so we do not offer WAL-style
millisecond tailing of in-flight updates. For arbitrary historical-seqno queries
(not just "since X"), pair with `Tree::create_checkpoint`. To layer your own
durability on top of the engine, see the external-WAL integration contract in
[docs/external-wal.md](docs/external-wal.md).

## Limits

- Keys: up to 65,535 bytes (the on-disk encoding caps the key-length field at `u16`).
- Values: up to 4,294,967,295 bytes (`2³² − 1`; the encoding caps the value-length field at `u32`).
- Larger keys and values carry a proportional performance cost.

## Feature flags

All optional. The default build (`std` + `parallel`) is the minimal core: no compression, no encryption, std filesystem, with the built-in parallel-compression executor. Every capability flag below is gated because it pulls in extra dependencies or runtime overhead. Turning `std` off (with `alloc`) selects the `no_std` build (see the platform note in `Cargo.toml`).

| Flag | Pulls in | Enable when |
|---|---|---|
| `lz4` | [`lz4_flex`](https://github.com/PSeitz/lz4_flex) | Block compression wanted, decompression latency matters more than ratio. |
| `zstd` | [`structured-zstd`](https://github.com/structured-world/structured-zstd) (pure-Rust, no FFI) | Block compression wanted, ratio matters more than absolute decompression speed. Supports `CompressionType::Zstd` and dictionary-mode `CompressionType::ZstdDict`. Decompression is ~2-3.5× slower than C reference. |
| `columnar` | (pure code, `alloc` only) | Columnar PAX SST block layout: each row-group stores its key / seqno / value-type / value sub-columns contiguously, for column-projection pushdown, vectorized predicate scans, per-column zstd dictionaries, and positional delete-bitmap MVCC. Enable for analytical or wide-row scans where reading a subset of columns dominates. |
| `encryption` | `aes-gcm`, `rand_chacha` | AES-256-GCM block encryption at rest. Keys are caller-managed. |
| `page_ecc` | [`reed-solomon-simd`](https://github.com/AndersTrier/reed-solomon-simd) | Per-block error-correcting parity (Reed-Solomon shards or per-word SEC-DED) after each on-disk block, with on-read self-healing, three-state verification, and a patrol scrub. Enable for latent-bit-rot protection on long-lived cold data. |
| `crc32c` | [`crc32c`](https://github.com/zowens/crc32c) | Selects the hardware-accelerated CRC32C block checksum (`ChecksumType::Crc32c`) over the default XXH3. Enable when interop or CPU profile favours CRC32C. |
| `io-uring` (linux only) | [`io-uring`](https://github.com/tokio-rs/io-uring) | I/O-bound workload on a modern Linux kernel: adds an `io_uring` `Fs` backend. |
| `io-uring-raw` (linux only) | [`syscalls`](https://github.com/jasonwhite/syscalls) | Pure-Rust `no_std` io_uring `Fs` backend on raw Linux syscalls (no libc), for embedded or sandboxed Linux targets without `std`. |
| `bytes_1` | [`bytes`](https://github.com/tokio-rs/bytes) | Consumer already speaks `bytes::Bytes` (tokio/hyper/tonic stack) and wants zero-copy interop with engine slices. |
| `parallel` (default-on) | [`rayon`](https://github.com/rayon-rs/rayon) | Built-in rayon-backed parallel block-compression executor. `std`-only; the `CompactionSpawner` trait lets a caller inject a custom executor without this flag. |
| `metrics` | (none) | Production observability or profiling. Compiles in atomic counters around block I/O, filter probes, compaction, and cache hit rates (`tree.metrics()`). Small but non-zero hot-path cost. |
| `ribbon-serde` | `serde` | Snapshotting the internal `RibbonFilterRepr` for debugging or out-of-band transport. Not used by the on-disk format. |

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
  --benchmark all --num 100000 --flamegraph
# Folded stacks: target/flamegraphs/all.folded
# Render: cargo install inferno && inferno-flamegraph target/flamegraphs/all.folded > flame.svg
```

## Operational tools

| Tool | Use |
|------|-----|
| [`tools/db_bench`](tools/db_bench) | RocksDB-compatible benchmark suite, also drives the CI perf dashboard. |
| [`tools/sst-dump`](tools/sst-dump) | Inspect / verify a single SST file out-of-band. Subcommands: `verify` (walk every block, check per-block XXH3, exit non-zero on corruption), `properties` (print the SST's stored metadata: id, key range, KV / tombstone counts, block counts, compression, timestamp), `hex <offset>` (raw hex dump of a region with optional `Header` decode; useful for inspecting a specific offset flagged by `verify --verbose`), `index-dump` (print TLI entries: end_key + offset + size + seqno per pointed-at block; useful for diagnosing range-read fan-out), `dump` (stream every KV entry to stdout with optional `--from` / `--to` / `--max=N` / `--keys-only` filters; full-index SSTs only), `filter-stats` (print BuRR filter sizing: section bytes, layer count, item count, approximate bits-per-key; single-block filters only, partitioned filters not yet supported). |

## Data integrity & durability

Every record is checksummed end to end, from the in-memory memtable, through
each on-disk block, to the manifest, with optional Reed-Solomon ECC,
self-healing scrub, and AAD-bound encryption at rest. Off by default,
byte-identical when disabled; turned on, detection becomes recovery.

This is the tamper-evident, silent-corruption protection that clinical and
regulated systems require: the integrity controls behind HIPAA §164.312(c),
FDA 21 CFR Part 11, and ALCOA+.

See **[docs/data-integrity.md](docs/data-integrity.md)** for the full integrity
stack: block + per-KV checksums (including memtable-residence RAM bit-flip
detection), the Page ECC spectrum, self-healing scrub, tamper-evident
encryption, the five-layer manifest hardening surface, and the manifest
recovery modes.

See **[docs/tight-space-compaction.md](docs/tight-space-compaction.md)** for the
opt-in tight-space compaction: how a gated merge on a near-full disk is rewritten
in key-range slices and reclaimed in place with hole punching, so a compaction
completes on a disk far smaller than the data it rewrites.

See **[docs/INVARIANTS.md](docs/INVARIANTS.md)** for the engine's load-bearing
invariants grouped by subsystem (block layout, manifest, compaction,
snapshot / seqno, range tombstones, recovery / ECC, columnar) — the rule, why it
holds, and where it is enforced.

## Support the project

<div align="center">

![USDT TRC-20 Donation QR Code](assets/usdt-qr.svg)

USDT (TRC-20): `TFDsezHa1cBkoeZT5q2T49Wp66K8t2DmdA`

</div>

## Credits

Originally created by Marvin Blum as part of [fjall-rs/lsm-tree](https://github.com/fjall-rs/lsm-tree); this codebase carries the original copyright (`Copyright (c) 2024-present, fjall-rs`). The vendored Ribbon filter (`src/table/filter/ribbon/`) is by [William Rågstad](https://github.com/WilliamRagstad); see [`src/table/filter/ribbon/_vendored/`](src/table/filter/ribbon/_vendored/) for the upstream license texts.

## License

All source code is licensed under [Apache-2.0](LICENSE-APACHE). Each first-party `.rs` file carries an `SPDX-License-Identifier: Apache-2.0` header alongside the original-author copyright and the maintainer copyright (Structured World Foundation). Contributions are accepted under the same license.

The vendored Ribbon filter (`src/table/filter/ribbon/`) keeps its upstream layout: it carries William Rågstad's per-module licensing commentary rather than per-file SPDX headers, plus the original `LICENSE-APACHE` and `LICENSE-MIT` preserved verbatim in `src/table/filter/ribbon/_vendored/`. The upstream crate is dual-licensed (`MIT OR Apache-2.0`); we redistribute the vendored copy only under the Apache-2.0 arm per Apache-2.0 §4.

Maintained by [Structured World Foundation](https://sw.foundation).

## Footnotes

[1] https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html

[2] https://github.com/facebook/rocksdb/wiki/BlobDB

[3] https://rocksdb.org/blog/2018/08/23/data-block-hash-index.html
