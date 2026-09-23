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
| `bytes_read` | Bytes requested from the `Fs` trait: a block's on-disk size summed over the block roles (data, index, filter, range tombstone), plus the on-disk span of every blob record a key-value-separated tree resolved (a coalesced prefetch charges its whole extent, gaps included, because that is what it read). Charged when the read is issued, so a read that then fails its checksum is counted, and on every path that reads outside the block cache: point and range reads, `multi_get`'s batched and chunked reads, and the partial decode of large zstd blocks. | Device I/O. The OS page cache, readahead and request coalescing sit below this line. Anything served from a cache, block or blob, asks for nothing and adds nothing. |
| `blob_bytes_read` | The blob-only share of `bytes_read`, so a scan can be asked whether it paid for the blobs of rows it then discarded. | Everything the block roles cover. |
| `bytes_decoded` | Payload bytes produced after the transform — what decompression, decryption and Page-ECC verification turned the bytes read into, for blocks and for blob records alike. | Anything on a cached path: a cached block or blob is already decoded, so no transform runs for it. |
| `bytes_copied` | Bytes moved by a **gather**: column-batch accumulation, batch filtering, row gathering by index, row-value reconstruction from sub-columns, a point read's copy of the matching rows out of the columns, the row-major block a columnar block is re-encoded into, and what decoding a columnar block copies out of it (validity bitmaps, and columns a narrow projection detaches rather than keep the whole block alive for), on every read that performs one (single-segment and merged columnar scans, row iteration and point reads of a columnar segment) and in salvage. The figure is the size of the RESULT. A row read whose value is a single bytes column hands out views into the decoded column and charges nothing. | Transform output (that is `bytes_decoded`), write-path serialisation, the input decoding of compaction and repair (maintenance, not reads), and moves that transfer ownership without duplicating bytes. |

**Why the definitions are written down rather than inferred.** "Bytes read"
can plausibly mean either bytes asked of the filesystem or bytes the device
actually moved, and the two differ by the whole page cache. "Bytes copied"
means nothing at all until the set of operations it counts is named — without
that, a new code path wins simply by not being instrumented. A figure whose
definition is implicit is not a measurement.

That last hazard is not hypothetical: a key-value-separated tree keeps most of
its bytes in blob files, so a counter that covered only blocks would report a
tree reading gigabytes as reading a few bytes of indirection per row, and any
change that moved work into the blob path would read as a win. Blob reads are
counted for that reason.

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

### The `mixed-layout` workload

`db_bench --benchmark mixed-layout` is where those counters are read. It walks
a set of record shapes rather than repeating one operation, and reports the
byte counters per emitted row instead of a rate:

```sh
cd tools/db_bench && cargo run --release --features counters -- --benchmark mixed-layout --num 70000
```

It is built only with the `counters` feature, which turns on the engine's
`metrics`: the counters are atomics on every read path, so a binary carrying
them measures a slower engine than the one that ships. The rate workloads
therefore run from the default build, and the dashboard runs this workload as
a second pass from a `counters` build.

| Scenario | Shape |
|---|---|
| `narrow-records` | A few small fields per row. The control: no projection can cost more per row than reading a narrow row whole. |
| `wide-records-full-read` | Small fields plus a 4 KiB payload, read whole. The baseline a projection is compared against. |
| `wide-records-projected` | **Unsupported.** Needs a projection that returns the header fields without the payload. |
| `mixed-value-sizes` | Short and long values in one key space, so no single block geometry fits both. |
| `row-updates-over-columnar-base` | A columnar base flushed first, then the layout switched off and a third of the keys rewritten, so the newest version of those lives in a row-major run above a columnar one. |
| `versions-deletes-tombstones` | Several versions per key, a fifth point-deleted, a contiguous slice covered by a range tombstone, read at `SeqNo::MAX`. |
| `selective-scan-sparse` | A predicate matching ~1% of rows, handed to the columnar scan over a field stored in a sub-column of its own, with zone maps on. Where materializing before the predicate runs wastes nearly all the work. |
| `selective-scan-near-full` | A predicate matching ~90%, over the same fixture. Deferring materialization buys almost nothing here and its bookkeeping can cost more than it saves, so the two are read together. |
| `blobs-well-placed` | Values far above the separation threshold, written once in key order, so neighbours' blobs are adjacent. Read by a full scan, the pass where adjacent blobs are fetched ahead and merged into one read. |
| `blobs-scattered` | The same blobs written in a strided order and rewritten in several flushed rounds, so a key's live blob sits in whichever file its last round landed in. Read by the same full scan, so the gap to the well-placed figure is what placement costs. Both placement scenarios report **unsupported** under `--cache-mb 0`: the prefetch holds what it fetches in the cache, so without one placement cannot show. |
| `blobs-filtered-before-fetch` | **Unsupported.** Needs materialization deferred past the filter, so discarded rows' blobs are never fetched. |

The selective scans hand the predicate to the engine rather than filtering
returned rows: a harness-side filter would make every selectivity cost the same
engine work and change only the row count the figures divide by.

**Every scenario checks what it read.** A pass that only counted rows would
report a flattering figure for a build that stopped resolving versions, since
skipping work is fast. Each read pass compares every value against the oracle
its fixture derived from the **write history** — not from a second read, since
two paths sharing one faulty version-resolution routine agree with each other
while both are wrong.

A scenario whose native path does not exist reports `UNSUPPORTED` with the
capability it waits for, and contributes no figure. It is never quietly run
through a fallback path under the same name: a series that stays continuous
across the change that was supposed to move it is worse than a gap. Its
fixture exists regardless and is exercised by the workload's tests
(`cd tools/db_bench && cargo nextest run --features counters`), so enabling the scenario later is
one line rather than a fresh argument about what the expected result is.

**On the dashboard** this workload publishes one series per scenario per
counter, named `mixed-layout / <scenario> rows per KiB read` (and `… decoded`),
in place of the ops/sec every other workload reports — for a scenario sweep the
rate counts scenarios per second, which describes the harness rather than the
engine. The `--json` report and the plain summary carry the same series in
place of the rate. The fixtures open their trees with the run's cache, metadata
and compression flags (`--compression` reaches blob files too), so
`--cache-mb 0` measures cold reads here as everywhere else.

Read and decoded are yields in the bigger-is-better suite: more rows out of the
same kibibyte is the improvement. Copied is published as a cost,
`mixed-layout / <scenario> bytes copied per byte decoded` (the copy
amplification of the read), in a separate smaller-is-better suite,
`lsm-tree db_bench costs`, because the dashboard fixes one direction per suite.
Zero is the best value there, and a scenario whose copies go from zero to
anything alerts. `db_bench --github-json` writes the yields to stdout, or
appends them to the array in a file with `--github-json-append <PATH>` (how the
second pass joins the first pass's suite), and `--github-json-costs <PATH>`
writes the costs.

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
