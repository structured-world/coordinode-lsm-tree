# Benchmarking

How to run the head-to-head benchmarks and, more importantly, how to keep
their results honest.

## The Benchmark Symmetry Invariant

`coordinode-lsm-tree` ships on-disk integrity features (manifest hardening,
per-KV checksums, Page ECC, seqno-in-index) that RocksDB and most other LSM
engines have no equivalent for. If those are active while a competitor's are
not, every published comparison is unfair in one of two directions:

- we pay for protection the competitor lacks, losing a comparison we should
  win, or
- we win unfairly on a workload where the competitor simply has no equivalent
  feature enabled.

To prevent both, every new on-disk format feature MUST satisfy at least one of:

1. **OFF by default** (the user opts in explicitly), OR
2. have a **baseline equivalent in RocksDB** with a matching durability
   profile, OR
3. provide an **explicit OFF mode** that produces wire-output identical to
   "feature absent".

The `compare-rocksdb` harness encodes this as a set of presets. Public
comparisons use the `RocksDbParity` preset (or a documented equivalent).

## Presets

Only OUR engine's configuration moves across presets; RocksDB is the fixed
baseline. Select a preset with the `LSM_BENCH_PRESET` environment variable
(default: `rocksdb-parity`):

| `LSM_BENCH_PRESET` | Preset | Purpose |
|--------------------|--------|---------|
| `rocksdb-parity` (default) | `RocksDbParity` | Every lsm-tree-only opt-in OFF, matching RocksDB's durability defaults. The honest apples-to-apples number, used by the dashboard CI run. |
| `lsm-default` | `LsmTreeDefault` | Production defaults (manifest hardening + FS-aware optimizations ON). What a real deployment runs. |
| `lsm-paranoid` | `LsmTreeParanoid` | Every opt-in ON. Worst-case protection-overhead measurement. |

`RocksDbParity` disables, in one place (`apply_preset` in
`tools/compare-rocksdb/benches/compare.rs`):

| Feature | Parity setting | Rationale |
|---------|----------------|-----------|
| `manifest_footer_mirror` | off | lsm-tree-only manifest hardening |
| `kv_checksums` | `Off` | RocksDB has no per-KV checksum |
| `seqno_in_index` | off | lsm-tree-only index extension |
| `page_ecc` | off | RocksDB has no Page ECC |
| `disable_cow_on_sst_files` | off | RocksDB has no FS-aware CoW control |
| `use_reflink_for_checkpoint` | off | RocksDB has no reflink path |
| `locator_policy` | disabled | RocksDB has no retrieval-ribbon locator |
| index / filter partitioning | off at every level | matches RocksDB's default single index (`kBinarySearch`) and full filter per table |
| index / filter pinning | pinned at every level | matches RocksDB's `cache_index_and_filter_blocks = false`: the table reader holds them, not the block cache |
| `manifest_kv_checksums` | **on** | parity: matches RocksDB's per-record MANIFEST CRC32 |
| block-level XXH3 checksum | **on** | parity: matches RocksDB's per-block checksum |

The preset disables each opt-in explicitly even when it is already off by
default, so the comparison stays honest if a default ever flips and so the full
parity surface is documented in one place.

## Same workload for every engine

Beyond the preset, the harness gives every engine the same work:

- **Values.** 256-byte values cut from one pool of half-compressible bytes,
  the shape of RocksDB `db_bench`'s default value generator
  (`--compression_ratio=0.5`), so a codec does the work it does on real data
  rather than collapsing a constant run. The compaction scenario that splits
  the output across threads uses incompressible values instead, so its output
  spans enough tables to split.
- **Reads without copies.** Point reads use RocksDB's `get_pinned` and scans
  and seeks its `raw_iterator`, both zero-copy, the way our engine hands out
  values.
- **Matched block options.** Block size, a 10-bit bloom filter and a 16 MiB
  LRU block cache on both sides, compaction scenarios included; no WAL.
- **The same cores.** Both engines compress a table's blocks on four threads
  wherever they compress (ours through `compaction_threads`, RocksDB through
  `compression_options_parallel_threads`). At their defaults ours would use
  half the host's cores and RocksDB one, and a write group would compare
  unequal parallelism.
- **One untimed setup.** A read or overwrite scenario writes its starting
  state once per engine, codec and size, and every timed iteration opens a
  copy of it, so only the measured operation is timed.

## Paired measurement

The bench hosts are shared machines, and a burst of other work on them lasts
seconds. Measured one engine after another, such a burst fell on whichever
engine was running, and two runs of one commit on one host disagreed on the
ratio of our engine to RocksDB by a median 15-20%, up to 3x on single arms.

So the engines of a group are measured in the same **rounds**: one sample each
per round, taking turns at going first. A burst slows every engine of the
rounds it overlaps and divides out of their ratio. Each group reports, per
engine, the median over the rounds of its time over RocksDB's in the same
round and its median time per operation, each with a distribution-free 95%
confidence interval. An arm runs as many iterations per sample as fill 20 ms,
and a group as many rounds as fit a 4-second budget, between ten and forty,
rounded to a multiple of the group's engine count so every engine takes every
position equally often.
Two runs of one commit on one quiet host agreed on the ratio to within 2% on
the median arm and 7% on nine arms in ten, so a ratio within a few percent of
1 is parity. `cargo bench --bench compare -- --help` lists the flags that
change these settings, and a positional filter selects groups.

The run writes `target/head-to-head/summary.json`, which the published
head-to-head page draws from.

## Running

The harness links RocksDB through `librocksdb-sys`, whose `bindgen` build step
needs `libclang`.

```sh
# Linux: the distro libclang.so is found automatically.
cd tools/compare-rocksdb && cargo bench

# macOS (Homebrew LLVM):
export LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib
export DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib
cd tools/compare-rocksdb && cargo bench

# Worst-case overhead run:
LSM_BENCH_PRESET=lsm-paranoid cargo bench
```

The active preset is printed once to stderr at the start of the run, so it is
recorded in the dashboard provenance.

## Dashboard series

The `db_bench` dashboard (`dev/bench` on the project's GitHub Pages) keeps one
rate suite per **major version line** and bench host,
`lsm-tree db_bench <N>.x · <os> · <runner>`, so a commit is only ever compared
with points of its own line measured on the same machine.

`<N>` is the major of the next version release-plz computes for the measured
commit, the version it will ship as, not the version already in `Cargo.toml`:
that one moves only when the release PR merges. A manual dispatch from this
branch is therefore compared against the `5.x` suites and writes nothing; only
a push to the default branch extends a suite. The RocksDB head-to-head page
(`dev/compare/`) is a snapshot replaced on every run and names the line, branch
and commit it measured.

## Checklist for format-changing PRs

A PR that adds or changes an on-disk format feature MUST:

- [ ] Make the feature satisfy the invariant (off by default, RocksDB-equivalent,
      or wire-identical OFF mode).
- [ ] Document the feature's default in the `Config` / `RuntimeConfig` docstring
      and the README "what works" surface.
- [ ] If it adds a new opt-in field, disable it in the `RocksDbParity` preset
      (`apply_preset`) so the parity comparison stays apples-to-apples.
- [ ] Include bench data showing the ON-vs-OFF impact (`lsm-paranoid` vs
      `rocksdb-parity`).
