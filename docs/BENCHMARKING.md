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
| `manifest_kv_checksums` | **on** | parity: matches RocksDB's per-record MANIFEST CRC32 |
| block-level XXH3 checksum | **on** | parity: matches RocksDB's per-block checksum |

The preset disables each opt-in explicitly even when it is already off by
default, so the comparison stays honest if a default ever flips and so the full
parity surface is documented in one place.

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

## Read-path byte counters

Three counters describe what a read costs, and they are reported together
because each one alone is misleading. They are behind the `metrics` feature
and read through `AbstractTree::metrics()`.

| Counter | Counts | Does not count |
|---|---|---|
| `bytes_read` | Bytes requested from the `Fs` trait: a block's on-disk size, summed over the block roles (data, index, filter, range tombstone). | Device I/O. The OS page cache, readahead and request coalescing sit below this line. A block served from the block cache asks for nothing and adds nothing. |
| `bytes_decoded` | Payload bytes the block transform produced — what decompression, decryption and Page-ECC verification turned the bytes read into. | Anything on the cached path: a cached block is already decoded, so no transform runs for it. |
| `bytes_copied` | Bytes moved by a **gather**: column-batch accumulation, batch filtering, row gathering by index, and row-value reconstruction from sub-columns. The figure is the size of the RESULT. | Transform output (that is `bytes_decoded`), write-path serialisation, and moves that transfer ownership without duplicating bytes. |

**Why the definitions are written down rather than inferred.** "Bytes read"
can plausibly mean either bytes asked of the filesystem or bytes the device
actually moved, and the two differ by the whole page cache. "Bytes copied"
means nothing at all until the set of operations it counts is named — without
that, a new code path wins simply by not being instrumented. A figure whose
definition is implicit is not a measurement.

**How to read them.**

- **Read and decoded together** tell a physical projection from a cosmetic
  one. A projection that returns two columns of a wide record but still loads
  and decompresses the whole block leaves decoded unchanged while the
  returned batch shrinks; one that reads only the pages it needs moves it.
  Read alone cannot show this — a 4 KiB compressed block is 4 KiB read
  however much it expands to.
- **Their ratio** is the compression the read actually paid for.
- **Copied per input byte** should be a small constant. A path that
  materialises its working set once sits there; one that folds batches
  together pairwise records the whole accumulated size on every fold, so the
  counter grows with the square of the fold count rather than with the data.
  That growth is visible here and nowhere else.

`tests/read_byte_counters.rs` pins one clause of each definition, so a change
that moves a counter without moving the behaviour it stands for fails rather
than quietly rebasing the instrument.

**May not regress:** `bytes_read` and `bytes_decoded` per emitted row on the
projection scenarios, and `bytes_copied` per input byte on every scenario. A
change that improves compressed size while raising decoded per row has not
paid for itself.

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
