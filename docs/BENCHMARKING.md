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

## Same work for every engine

Beyond the preset, every engine gets the same work:

- **Values.** 256-byte values cut from one pool of half-compressible bytes,
  the shape of RocksDB `db_bench`'s default value generator
  (`--compression_ratio=0.5`), so a codec does the work it does on real data
  rather than collapsing a constant run. The compaction scenario that splits
  its output across threads uses incompressible values instead, so its output
  spans enough tables to split.
- **Reads without copies.** Point reads use RocksDB's `get_pinned`, scans and
  seeks its `raw_iterator`, both zero-copy, the way our engine hands out values.
- **Matched block options.** Block size, a 10-bit bloom filter and a 16 MiB LRU
  block cache on both sides, compaction scenarios included; no WAL.
- **The same cores.** Both engines compress a table's blocks on four threads
  wherever they compress (ours through `compaction_threads`, RocksDB through
  `compression_options_parallel_threads`). At their defaults ours would use
  half the host's cores and RocksDB one, and a write group would compare
  unequal parallelism.
- **One untimed setup.** A read or overwrite group writes its starting state
  once per engine, codec and size, and every timed iteration opens a copy of
  it, so only the measured operation is timed.

## Paired measurement

The bench hosts are shared machines, and a burst of other work on them lasts
seconds. Measured one engine after another, such a burst fell on whichever
engine was running, and two runs of one commit on one host disagreed on the
ratio of our engine to RocksDB by a median 15-20%, up to 3x on single arms,
with some arms trading the verdict of which engine is faster.

So the engines of a group are measured in the same **rounds**: one sample each
per round, taking turns at going first. A burst slows every engine of the
rounds it overlaps and divides out of their ratio. Each group reports, per
engine:

- **the ratio to RocksDB**: the median over the rounds of this engine's time
  over RocksDB's in the same round, with a distribution-free 95% confidence
  interval (the order statistics a Binomial(rounds, 1/2) count bounds);
- **the time per operation**: the median over the rounds, with the same kind
  of interval.

An arm runs as many iterations per sample as fill 20 ms, and a group runs as
many rounds as fit a 4-second budget, between ten and forty (ten is the fewest
for which a 95% interval of the median exists at ranks 2 and 9), rounded to a
multiple of the group's engine count so every engine takes every position
equally often. Two runs of
one commit on one quiet host then agreed on the ratio to within 2% on the
median arm and 7% on nine arms in ten; the interval covers the noise within a
run, so a ratio within a few percent of 1 is parity. `cargo bench --bench compare
-- --help` lists the flags that change these settings, and a positional filter
selects groups. The run writes `target/head-to-head/summary.json`, which the
published page is drawn from.

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
| `bytes_read` | Bytes requested from the `Fs` trait: a block's on-disk size summed over the block roles (data, index, filter, range tombstone), plus the on-disk span of every blob record a key-value-separated tree resolved (a coalesced prefetch charges its whole extent, gaps included, because that is what it read). Charged when the read is issued, so a read that then fails its checksum is counted, and on every path that reads outside the block cache: point and range reads, `multi_get`'s staged filter, index and data reads, and the partial decode of large zstd blocks. | Device I/O. The OS page cache, readahead and request coalescing sit below this line. Anything served from a cache, block or blob, asks for nothing and adds nothing. Maintenance: a compaction's input, the values its filter resolves, the index walks that open a table, verify it or locate its live region, a patrol scrub's reads, including the re-read that confirms an ECC correction, and salvage. Monitoring: the storage statistics report's reads, which also neither fill the block cache nor promote what they find there, so polling it during a run leaves the run's figures unchanged. A query planner's range estimates are part of the query they plan and are counted. |
| `blob_bytes_read` | The blob-only share of `bytes_read`, so a scan can be asked whether it paid for the blobs of rows it then discarded. | Everything the block roles cover. |
| `blob_read_count` | Read requests behind `blob_bytes_read`: one per value read on its own, one per coalesced read-ahead span. The figure blob locality moves: values that sit next to each other in one file coalesce into few requests, the same values interleaved across files need one per file per stretch. | The same as `blob_bytes_read`. |
| `bytes_decoded` | Payload bytes produced after the transform — what decompression, decryption and Page-ECC verification turned the bytes read into, for blocks and for blob records alike. | Anything on a cached path: a cached block or blob is already decoded, so no transform runs for it. |
| `bytes_copied` | Bytes moved by a **gather**: column-batch accumulation, batch filtering, row gathering by index, row-value reconstruction from sub-columns, a point read's copy of the matching rows out of the columns, the row-major block a columnar row group is re-encoded into, the copy of each uncompressed blob record out of a scan's read-ahead span (so a cached value does not pin the whole span), the partial decode of a large zstd block (the decoded prefix each time it grows, a resumed prefix moved back into the decoder's window, and the row block synthesized from the prefix, including one served from the partial cache), the validity bitmaps decoding a columnar page copies out of it (a `Plain` column's data is a view of its own page, which pins nothing else, so a narrow projection copies no data), the effective seqnos written over a bulk-ingested segment's local ones, the key and value a point read detaches into the row cache (so the cached row does not pin its block), and the key each resolved blob is cached under, on every read that performs one (single-segment and merged columnar scans, row iteration and point reads). The figure sums the bytes each of those operations produced, whether or not the read then returns a row: a point read of a missing key still decoded its block, a block refused after a copy still made it, and an intermediate gather that is copied again counts both times. A row read whose value is a single bytes column hands out views into the decoded column and charges nothing. A view is a view whatever its representation: a short key or value stored inline in its handle is not charged, because that inline copy costs no more than building the handle. | Transform output (that is `bytes_decoded`), write-path serialisation, the input decoding of compaction, repair and salvage (maintenance, not reads), the storage statistics report (monitoring), and moves that transfer ownership without duplicating bytes. |

Four more figures describe what a projected scan's late reads spare. They are
reported beside the three above, never in place of them.

| Counter | Counts | Does not count |
|---|---|---|
| `bytes_materialized` | The bytes of the batches a projected columnar scan hands to its caller: what it assembled for the rows it returned, and nothing for a row it dropped. | Anything a scan did not return, and every read but a projected scan's. |
| `payload_bytes_useful` | Of the payload pages a projected scan read for its chosen rows only (the pages of a field read after the rows were decided), the decoded bytes of those rows' cells. | Pages read with the rest, eagerly; blob objects. |
| `payload_bytes_incidental` | Of the same pages, the decoded bytes of the rows not chosen: what a page holding one chosen row brings along. It shows where the chosen rows sit, together or one per page, apart from how many there are. | As `payload_bytes_useful`. |
| `blob_bytes_prefetched` | The share of `blob_bytes_read` read ahead: the coalesced spans a scan reads before asking for each value in them, the gaps it reads to merge two records into one request included. | Values read on their own. |

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
- **Copied per row** should be a small constant. A path that
  materialises its working set once sits there; one that folds batches
  together pairwise records the whole accumulated size on every fold, so the
  counter grows with the square of the fold count rather than with the data.
  That growth is visible here and nowhere else.

`tests/read_byte_counters.rs` pins one clause of each definition, so a change
that moves a counter without moving the behaviour it stands for fails rather
than quietly rebasing the instrument.

**May not regress:** `bytes_read` and `bytes_decoded` per emitted row on the
projection scenarios, and `bytes_copied` per emitted row on every scenario. A
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
a second pass from a `counters` build. It runs on one thread, since its figures
are bytes per row and concurrency does not change them, and each scenario fixes
its own key format, value sizes and tree kind. `--threads` other than 1,
`--key-size`, `--value-size` and `--use-blob-tree` are therefore refused rather
than recorded against a run that did not use them. `--num` is an upper bound:
each fixture caps its key count at a size that keeps the sweep's build time
bounded, and every series names in its annotation (`keys: N`) the size it was
measured on.

| Scenario | Shape |
|---|---|
| `narrow-records` | A few small fields per row. The control: no projection can cost more per row than reading a narrow row whole. |
| `wide-records-full-read` | Small fields plus a 4 KiB payload, read whole. The baseline a projection is compared against. |
| `wide-records-projected` | **Unsupported.** Needs a projection that returns the header fields without the payload. |
| `mixed-value-sizes` | Short and long values in one key space, so no single block geometry fits both. |
| `row-updates-over-columnar-base` | A columnar base flushed first, then the layout switched off and a third of the keys rewritten, so the newest version of those lives in a row-major run above a columnar one. Read by point reads. |
| `row-updates-over-columnar-base-scan` | The same fixture read by the projected columnar scan of key and value, which merges the row-major run with the columnar base. Same extra figures as the columnar scans below. |
| `versions-deletes-tombstones` | Several versions per key, a fifth point-deleted, a contiguous slice covered by a range tombstone, read at `SeqNo::MAX`. |
| `selective-scan-sparse` | A predicate matching ~1% of rows, handed to the columnar scan over a field stored in a sub-column of its own, with zone maps on. Where materializing before the predicate runs wastes nearly all the work. |
| `selective-scan-near-full` | A predicate matching ~90%, over the same fixture. Deferring materialization buys almost nothing here and its bookkeeping can cost more than it saves, so the two are read together. |
| `columnar-scan-one-segment` | 256-byte values in one flushed columnar segment, read whole by the projected columnar scan of key and value. Also reports the time and bytes read from the scan's creation to its first batch, and the most page bytes the scan held at once (`retained payload`), which stays within `columnar_scan_budget` except for reads its counter reports as past a share. The time goes to the host's timings suite, the bytes to the costs. |
| `columnar-scan-overlap-8` | The same rows written round-robin into eight flushed segments, so all eight form one overlapping group the scan merges. Same extra figures: the first batch comes after a bounded prefix of each segment, and the eight share one budget. |
| `blobs-well-placed` | Values far above the separation threshold, written once in key order, so neighbours' blobs are adjacent. Read by a full scan, the pass where adjacent blobs are fetched ahead and merged into one read. |
| `blobs-scattered` | The same blobs written in a strided order and rewritten in several flushed rounds, so a key's live blob sits in whichever file its last round landed in. Read by the same full scan, so the gap to the well-placed figure is what placement costs. Both placement scenarios report **unsupported** under `--cache-mb 0`: the prefetch holds what it fetches in the cache, so without one placement cannot show. |
| `blobs-well-placed-churn`, `blobs-scattered-churn` | The two placement fixtures, then eight rounds of writes, each flushed and compacted: one key in ten rewritten three times a round, one in ten once every other round, the rest never, and as many new keys as hot ones appended each round and never rewritten, so a flush mixes values that die by the next round with values that live on. Publishes `relocated bytes per reclaimed byte` (blob bytes the relocating compactions copied over the blob bytes the compactions removed) and the full scan's per-row figures afterwards, so what collection costs and what placement then costs a scan come from one run. Unsupported under `--cache-mb 0`, like the placement scans. |
| `blobs-well-placed-churn-one-group`, `blobs-scattered-churn-one-group` | The same, in trees that keep every blob value in one lifetime group (`KvSeparationOptions::lifetime_groups(LifetimeGroups::ONE)`) instead of the default four: the comparison the grouping is judged by. |
| `blobs-filtered-before-fetch` | Rows written as cells into a blob tree with columnar tables, an 8 KiB payload in blob files scattered by rewrite rounds, read by the projected scan with a ~1% predicate on a field of its own: the payload of a row the predicate drops is never fetched. |
| `wide-cells-projected` | Rows written as cells with a 4 KiB payload in blob files, projected to a header field alone: no blob is read at all. The cell-row counterpart of `wide-records-projected`. |
| `cells-scan-sparse-clustered` | Rows written as cells with a 64-byte payload kept in its column, a ~1% predicate keeping runs of neighbouring keys: the payload is read only from the few pages holding them (`payload_bytes_incidental` stays small). |
| `cells-scan-sparse-one-per-page` | The same rows, a predicate keeping about one row in each row page: every payload page holds a kept row, so a late read spares no I/O and must cost no more than an eager one; what it spares is materialising the rows dropped (`bytes_materialized`). |
| `cells-scan-sparse-one-per-page-eager` | The `cells-scan-sparse-one-per-page` rows read whole, the predicate applied by the caller: the time the late read is held against. |
| `cells-scan-near-full` | The same rows, a ~90% predicate: the scan reads the payload with the rest once its choices are dense. |
| `cells-scan-near-full-eager` | The `cells-scan-near-full` rows read whole, the predicate applied by the caller: the sequential pass the density decision must not lose to. |
| `cells-scan-ref-filtered` | The `cells-scan-sparse-clustered` rows with the cluster field kept in a blob file however small, so the ~1% predicate on it judges a row only once its object is read: the rows it drops cost their objects, and the inline payload of their pages is what the scan still spares. |
| `cells-scan-ref-filtered-eager` | The `cells-scan-ref-filtered` rows read whole, the predicate applied by the caller. |
| `cells-scan-under-compaction` | The `blobs-filtered-before-fetch` scan repeated forty times while a second thread writes other rows, flushes and compacts, so blob files are relocated and dropped under the scans. Each repetition is verified; it publishes the scans' `scan P50` and `scan P99` in microseconds, to the host's timings suite, and no byte series, since the compacting thread reads and copies through the same counters. |

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
counter, named `mixed-layout / <scenario> bytes read per row` (and
`… bytes decoded per row`, `… bytes copied per row`, and, for a projected
scan, `… bytes materialized per row` and `… payload bytes useful per row` /
`… payload bytes incidental per row` where it read a payload late), in place of the ops/sec
every other workload reports — for a scenario sweep the
rate counts scenarios per second, which describes the harness rather than the
engine. The `--json` report and the plain summary carry the same series in
place of the rate. The fixtures open their trees with the run's cache, metadata
and compression flags (`--compression` reaches blob files too), so
`--cache-mb 0` measures cold reads here as everywhere else.

All three are costs with one denominator, the rows the scenario emitted, so
they read the same way and compare directly: a projection that stops loading a
payload moves read and decoded per row down together. They live in a
smaller-is-better suite of their own, `lsm-tree db_bench costs <N>.x`, because
the dashboard fixes one direction per suite and the rates are bigger-is-better.
Zero is the best value there (a read that gathers nothing copies nothing), and
a scenario whose copies go from zero to anything alerts. A scenario that
emitted no row publishes nothing, since its cost per row does not exist. The
plain summary also prints decoded over read and copied over decoded as
diagnostics, `n/a` where the denominator is zero; they are not series.
`db_bench --github-json` writes the rates to stdout, or appends them to the
array in a file with `--github-json-append <PATH>`; `--github-json-costs <PATH>`
writes the costs and `--github-json-timings <PATH>` the timings.

## Dashboard series

The `db_bench` dashboard (`dev/bench` on the project's GitHub Pages) keeps one
suite per **major version line**, so a commit is only ever compared with
points of its own line and the regression alert never judges a 6.0 commit
against a 5.x baseline:

| Suite | Holds |
|---|---|
| `lsm-tree db_bench <N>.x · <os> · <runner>` | The rates of one line measured on one bench host. A rate measured on one machine is no baseline for another, so each host has its own suite. |
| `lsm-tree db_bench costs <N>.x` | The bytes-per-row costs of one line. They are counted, not timed, so one suite serves every host. |
| `lsm-tree db_bench timings <N>.x · <os> · <runner>` | Times of one line measured on one host that improve by shrinking, such as a scan's time to its first batch: smaller is better like the costs, one suite per host like the rates. |

**Which suite a series goes to** follows from two facts. The store action fixes
one direction per suite, so a cost cannot share a suite with a rate. And a
figure the engine counts is the same on every host, while a figure timed on one
host means nothing as a baseline for another and, in a suite shared across
hosts, would alert on whichever host ran last.

**A series that changes meaning gets a new name.** The old points measured
something else, so they are not the start of the new series: they are dropped
from the data file on the next push to `main`, by a rule in
`.github/bench/retired-series.json` that names the suite, the series and the
reason. A series that moves to another suite leaves the old one the same way.

**Where the line comes from.** `<N>` is the major of the next version
release-plz computes for the measured commit, the version it will ship as: the
bench job runs `release-plz update` on its checkout and reads the version it
writes. It is not the version already in `Cargo.toml`, because that one moves
only when the release PR merges, so it names the previous line from the first
breaking commit until the release (`main` carried `5.11.1` while it was already
the 6.0 line). release-plz reads the same conventional-commit markers the
release does (`!`, `BREAKING CHANGE`), so the dashboard and the release agree
on every commit's line. A maintained branch (`5.x.x`) gets its own line the
same way.

**What writes where.** Only a push to `main` appends to its line's suites. A
manual dispatch from any branch is compared against the suites of the line it
measures and writes nothing.

**Where to look.** The dashboard (`dev/bench/`) picks a version line and a
runner, defaulting to whatever was measured last, and shows the rates, the
costs or the timings, grouped by what they measure; for the rates and timings
it can also draw every runner of the line on one chart. The RocksDB
head-to-head page (`dev/compare/`) is a snapshot replaced on every run; it
names the line, branch, commit and runner it measured, and shows each engine's
time relative to RocksDB or its time per operation.

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
