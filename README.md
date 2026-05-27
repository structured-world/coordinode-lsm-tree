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
- Point-in-time recovery snapshots via `Tree::create_checkpoint` — hard-link
  every live SST + blob file into a fresh directory in O(1) per file, zero
  extra disk until the source files compact away. Compaction continues
  during the checkpoint (deletions are deferred), and the resulting
  directory opens as an independent tree.

### Internals

- 100% stable Rust, MSRV 1.92.
- No FFI: zstd via [`structured-zstd`](https://github.com/structured-world/structured-zstd) (pure-Rust), LZ4 via `lz4_flex`, AES via `aes-gcm`.
- Pluggable `Fs` trait — back the engine on the standard filesystem, on `io_uring`, on an in-memory `MemFs`, or on a custom implementation.
- Pluggable `CompressionProvider` for third-party codecs.

## Limits

- Keys: up to 65,535 bytes (the on-disk encoding caps the key-length field at `u16`).
- Values: up to 4,294,967,295 bytes (`2³² − 1`; the encoding caps the value-length field at `u32`).
- Larger keys and values carry a proportional performance cost.

## Feature flags

All optional, all off by default. The default build is the minimal core (no compression, no encryption, std filesystem). Every flag below is gated because it pulls in extra dependencies or runtime overhead.

| Flag | Pulls in | Enable when |
|---|---|---|
| `lz4` | [`lz4_flex`](https://github.com/PSeitz/lz4_flex) | Block compression wanted, decompression latency matters more than ratio. |
| `zstd` | [`structured-zstd`](https://github.com/structured-world/structured-zstd) (pure-Rust, no FFI) | Block compression wanted, ratio matters more than absolute decompression speed. Supports `CompressionType::Zstd` and dictionary-mode `CompressionType::ZstdDict`. Decompression is ~2-3.5× slower than C reference. |
| `encryption` | `aes-gcm`, `rand_chacha` | AES-256-GCM block encryption at rest. Keys are caller-managed. |
| `io-uring` (linux only) | [`io-uring`](https://github.com/tokio-rs/io-uring) | I/O-bound workload on a modern Linux kernel — adds an `io_uring` `Fs` backend. |
| `bytes_1` | [`bytes`](https://github.com/tokio-rs/bytes) | Consumer already speaks `bytes::Bytes` (tokio/hyper/tonic stack) and wants zero-copy interop with engine slices. |
| `metrics` | — | Production observability or profiling. Compiles in atomic counters around block I/O, filter probes, compaction, and cache hit rates (`tree.metrics()`). Small but non-zero hot-path cost. |
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

## Manifest hardening

The per-version manifest file (`v{N}`) is stored as a sequence of standard lsm-tree `Block`s — one `BlockType::Manifest` Block per section, plus a `BlockType::ManifestFooter` Block at the tail carrying the table of contents and the manifest layout version. Every Block goes through the same XXH3-64 / optional ECC / optional AEAD pipeline data Blocks use, so every protection that applies to a data Block automatically applies to the manifest.

Five layers compose the manifest's integrity surface; each is independently togglable:

| Layer | Defends against | Config knob | Default |
|-------|-----------------|-------------|---------|
| L1 — Block XXH3-64 | Bit-rot detection per section / footer Block | Always on (`Block` invariant) | always on |
| L2 — Page ECC (Reed-Solomon (4, 2)) | Bit-rot recovery per Block | `RuntimeConfig::page_ecc` for **manifest Blocks** (current release) + `Config::page_ecc` for **SST data Blocks** (compile-time `page_ecc` feature gates both) | off (opt-in) |
| L3 — AEAD encryption | Tampering detection + confidentiality | `Config::with_encryption(provider)` | off |
| L4 — Footer Block tail hint | Reader locates footer without scanning | Always on (trailing `u32` size hint) | always on |
| L5 — Head footer mirror | Partial-write / tail-bit-rot recovery via mirrored copy at offset 0 | `RuntimeConfig::manifest_footer_mirror` | on |

Manifest-side ECC and the head mirror are reachable through `Tree::update_runtime_config` (for L2 *on manifest Blocks* and L5); SST data-block ECC is tree-static via `Config::page_ecc` at open time and not affected by runtime updates. AEAD is supplied at open via `Config::with_encryption` (L3). Runtime toggles take effect on the next manifest write, and existing on-disk manifests stay readable in their original format because each Block self-describes via its header.

Failure-mode coverage with the defaults (mirror on, ECC off, AEAD off):

- **Bit-flip inside one section Block** → XXH3 surfaces it, other sections + the footer Block still read correctly (per-section isolation; see [`manifest_blocks::reader::tests::reader_isolates_corruption_to_one_section_other_sections_readable`](src/manifest_blocks/reader.rs)).
- **Tail footer Block corruption** → reader falls back to the head mirror at offset 0 and continues.
- **Partial write mid-update** → reader falls back to the head mirror, which still holds the prior version's TOC, so the on-disk state rolls back to the last fully-committed version rather than refusing to open.
- **Accidental file substitution / mislinking** → the `CURRENT` pointer carries an XXH3-128 of the referenced `v{N}` section bytes; `Tree::open` re-hashes on read and refuses to follow a mismatched pointer. This catches restore/copy mishaps and half-applied snapshots, but XXH3-128 is **not** a cryptographic MAC: an adversary with write access can craft matching content. Enable `Config::with_encryption(...)` (AEAD per Block) for adversarial tamper resistance.

Turning ECC on (`page_ecc = true`) upgrades the first three rows from "detect and refuse" to "detect and recover" without any change to call sites — the Block layer transparently emits / consumes the parity trailer per Block.

## Manifest recovery modes

`Config::manifest_recovery_mode` controls how the engine reacts to a malformed MANIFEST record at `Tree::open` time. Each mode trades a different point on the **strictness ↔ availability** axis; pick the one whose contract matches the deployment.

| Mode | Behaviour on corruption | When to use |
|------|-------------------------|-------------|
| `AbsoluteConsistency` (default) | Any per-record decode mismatch (bad XXH3, invalid tag, truncated TOC entry, declared-count overrun) aborts the open with the original error. No data is silently dropped. | **Production default.** Surfaces every byte of corruption before the tree comes back online; matches what most workloads actually want. |
| `TolerateCorruptedTailRecords` | If the iteration over `tables` / `blob_files` runs out of bytes before the declared count is reached (truncated tail), keep everything that decoded cleanly before the cut and emit a `warn!` listing the dropped count. Any mid-record error that is NOT a clean tail truncation (bad checksum, etc.) still aborts. | **Power-loss-at-write-tail salvage.** Use when a crash mid-fsync left the MANIFEST tail incomplete and you'd rather come up with the last consistent prefix than refuse to open. Not a general bit-rot tolerance, only "the writer never finished". |
| `PointInTimeRecovery` | On the first record-decode mismatch inside the `tables` section, keeps the consistent prefix collected so far (records that decoded cleanly BEFORE the corrupt one in the current run, plus complete earlier runs in the same level, plus complete earlier levels) and drops everything after. "Record-decode mismatch" covers all three failure shapes the per-record loop produces: (a) framing-layer XXH3 mismatch, (b) framing-header structural failure (`len > MAX_FRAME_PAYLOAD`), and (c) payload-decode failure inside an otherwise-framed-OK record (e.g. `InvalidTag` from a corrupt `checksum_type` byte — the framing XXH3 happens to cover the corrupt byte, so the bytes pass the framing layer; the corruption only surfaces at per-entry decode). Analogous treatment on `blob_files`. Tail-truncation is still tolerated like `TolerateCorruptedTailRecords`. | **Post-corruption salvage with LSM invariants intact.** Use when a manifest has acquired real bit-rot (not just a truncated write) and you want the largest internally-consistent prefix the engine can still vouch for; matches RocksDB's `kPointInTimeRecovery` accept-the-prefix rule adapted to the level/run/table nesting. |
| `SkipAnyCorruptedRecords` | On any per-record decode mismatch (framing-layer XXH3 mismatch, payload-decode failure inside an otherwise-framed-OK record, or framing-header `BadHeader`), logs the skip and advances past the bad record using the framing-supplied length field. If the framing header itself is corrupt (length field outside the legal range, so the next-record boundary cannot be located), the rest of that section is dropped — there's no safe way to find the next record boundary in that case. Symmetric on `tables` and `blob_files`. | **Maximum-availability forensic mode.** Use when you'd rather open the tree with whatever survives than refuse to open at all; pairs with the `repair_db` tooling tracked in [#303](https://github.com/structured-world/coordinode-lsm-tree/issues/303) for the cases where even the surviving records aren't enough. |

When a non-default mode drops records, the recovery path logs `warn!` lines describing what was tolerated. Individual table-IDs / blob-file-IDs are NOT enumerated because they were never decoded. Warnings fall into two categories:

**Per-condition warns** (one warn for each malformed shape encountered, at the point of detection):

- `tables` section truncated before the `level_count` byte → tail-tolerant mode produces 0 levels.
- `tables` declared `table_count` exceeds remaining section payload (count header forged or entries truncated) → loop walks bytes-actually-present and stops at the first EOF.
- `blob_files` section truncated before its count header → 0 blob files.
- `blob_files` declared count exceeds remaining section capacity → same forged-or-truncated shape, same walk-and-stop fallback.
- `blob_gc_stats` payload truncated (power-loss between the `blob_files` commit and the `blob_gc_stats` payload landing) → tail-tolerant mode produces an empty `FragmentationMap`. GC stats are advisory (fragmentation re-accrues on the next compaction pass), so this is a "rebuild on next pass" outcome, not data loss. This is a single in-place warn with no later summary.

**Per-section summary warns** (at end of section processing, only if the section actually lost records):

- `tables` section, emitted only when `tables_dropped_to_tail > 0` OR `tables_truncated_headers > 0`. Reports two counters in one line: declared-but-missing table records (count header said N, only K < N records read before EOF → N-K dropped) and the separate counter for level / run / `table_count` headers cut mid-byte (no records were supposed to be present yet for those levels / runs, so the headers contribute zero to record loss but the levels / runs themselves are absent).
- `blob_files` section, emitted only when `blob_dropped_to_tail > 0`. Reports the declared-but-missing blob-file records count, analogous to the tables-section record-drop counter. A `blob_files` section whose only damage was a missing count header surfaces the per-condition warn above but does NOT add a summary line.

Operators wanting a per-record audit trail should pair a tail-tolerant open with an out-of-band integrity scan (see `verify::verify_integrity` / `tools/sst-dump verify`).

For workflows where the MANIFEST is unrecoverable even under the lossy modes, the planned `repair_db` tool ([#303](https://github.com/structured-world/coordinode-lsm-tree/issues/303)) will rebuild the MANIFEST from the SST files themselves.

## Support the project

<div align="center">

![USDT TRC-20 Donation QR Code](assets/usdt-qr.svg)

USDT (TRC-20): `TFDsezHa1cBkoeZT5q2T49Wp66K8t2DmdA`

</div>

## Credits

Originally created by Marvin Blum as part of [fjall-rs/lsm-tree](https://github.com/fjall-rs/lsm-tree); this codebase carries the original copyright (`Copyright (c) 2024-present, fjall-rs`). The vendored Ribbon filter (`src/table/filter/ribbon/`) is by [William Rågstad](https://github.com/WilliamRagstad) — see [`src/table/filter/ribbon/_vendored/`](src/table/filter/ribbon/_vendored/) for the upstream license texts.

## License

All source code is licensed under [Apache-2.0](LICENSE-APACHE). Each first-party `.rs` file carries an `SPDX-License-Identifier: Apache-2.0` header alongside the original-author copyright and the maintainer copyright (Structured World Foundation). Contributions are accepted under the same license.

The vendored Ribbon filter (`src/table/filter/ribbon/`) keeps its upstream layout — it carries William Rågstad's per-module licensing commentary rather than per-file SPDX headers, plus the original `LICENSE-APACHE` and `LICENSE-MIT` preserved verbatim in `src/table/filter/ribbon/_vendored/`. The upstream crate is dual-licensed (`MIT OR Apache-2.0`); we redistribute the vendored copy only under the Apache-2.0 arm per Apache-2.0 §4.

Maintained by [Structured World Foundation](https://sw.foundation).

## Footnotes

[1] https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html

[2] https://github.com/facebook/rocksdb/wiki/BlobDB

[3] https://rocksdb.org/blog/2018/08/23/data-block-hash-index.html
