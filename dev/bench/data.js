window.BENCHMARK_DATA = {
  "lastUpdate": 1779113931815,
  "repoUrl": "https://github.com/structured-world/coordinode-lsm-tree",
  "entries": {
    "lsm-tree db_bench": [
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b9c68970f2f45ee3c1df1d2a6bf8d17d21616a5d",
          "message": "feat(testing): db_bench suite + property-based model tests (#45)\n\n## Summary\n\n- Add `tools/db_bench/` standalone crate with 9 RocksDB\ndb_bench-compatible benchmark workloads\n- Add proptest-based property tests with BTreeMap MVCC oracle\n- Property tests found 2 MVCC bugs — both fixed in PR #56 (issues #52,\n#53)\n\n## db_bench Workloads\n\n| Benchmark | Description | M1 Mac result |\n|-----------|-------------|---------------|\n| `fillseq` | Sequential inserts | 2,738K ops/s |\n| `fillrandom` | Random inserts | 514K ops/s |\n| `readrandom` | Random point reads | 375K ops/s |\n| `readseq` | Full forward scan | 467 MB/s |\n| `seekrandom` | Random seek + next | 270K ops/s |\n| `prefixscan` | Prefix scans | 244K ops/s |\n| `overwrite` | Random overwrites | 299K ops/s |\n| `mergerandom` | Hot key compaction stress | 74K ops/s |\n| `readwhilewriting` | Concurrent read+write (4T) | 665K ops/s |\n\nRun: `cd tools/db_bench && cargo run --release -- --benchmark fillseq\n--num 1000000`\n\n## Property Tests\n\n- `prop_btreemap_oracle.rs` — Insert/Remove/Flush/Compact vs BTreeMap\noracle\n- `prop_range_tombstone.rs` — Range tombstone focused\n- `prop_mvcc.rs` — Snapshot isolation at historical seqnos\n- `prop_regression_rt_tombstone.rs` — 7 regression tests (all passing)\n\n## Bugs Found & Fixed\n\n1. **L0 stale reads** (#52): 3+ L0 SSTs + non-empty active memtable →\npoint reads return stale values — **fixed in PR #56**\n2. **RT + tombstone** (#53): Point tombstone invisible when range\ntombstone exists in prior SST — **fixed in PR #56**\n\nAll regression tests and proptests now run without `#[ignore]`.\n\n## Test Plan\n\n- [x] `cargo test --all-features` — all suites pass, 0 failures\n- [x] `cargo clippy --all-features -- -D warnings` — clean\n- [x] All 9 db_bench workloads produce correct output\n- [x] JSON output mode works (`--json`)\n- [x] CI: `PROPTEST_CASES=32` for bounded CI runtime\n\nCloses #42 (partial: db_bench + property tests)\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **New Features**\n* Added `db_bench` benchmarking tool with multiple workload types\n(sequential fill, random fill, read operations, merge operations, and\nrange scans).\n\n* **Tests**\n* Added property-based tests for MVCC snapshot consistency, range\ntombstone behavior validation, and oracle-based verification.\n\n* **Chores**\n* Enhanced test infrastructure with improved timeout configuration for\nproperty-based tests.\n* Updated CI/CD pipeline with automated benchmark execution and GitHub\nPages reporting.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-03-22T15:01:23+02:00",
          "tree_id": "013e544b13688410ea97f35f9f3751378a99f845",
          "url": "https://github.com/structured-world/lsm-tree/commit/b9c68970f2f45ee3c1df1d2a6bf8d17d21616a5d"
        },
        "date": 1774184552180,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1863915.4752355856,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.11s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1078840.3879110864,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 1.6us | P99.9: 3.8us\nthreads: 1 | elapsed: 0.19s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 655440.3305655628,
            "unit": "ops/sec",
            "extra": "P50: 1.3us | P99: 5.4us | P99.9: 11.2us\nthreads: 1 | elapsed: 0.31s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2412298.3602015926,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.1us | P99.9: 8.0us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 394519.8797906606,
            "unit": "ops/sec",
            "extra": "P50: 2.2us | P99: 6.3us | P99.9: 12.4us\nthreads: 1 | elapsed: 0.51s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 191850.88557212878,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 7.5us | P99.9: 16.1us\nthreads: 1 | elapsed: 1.04s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 929551.8754039077,
            "unit": "ops/sec",
            "extra": "P50: 0.9us | P99: 3.1us | P99.9: 9.9us\nthreads: 1 | elapsed: 0.22s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 697430.4566199599,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 0.6us | P99.9: 0.9us\nthreads: 1 | elapsed: 0.29s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 523817.1499636354,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 7.9us | P99.9: 13.7us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "9c4b06595c7b13dcb1a584792cf1f810769cbc16",
          "message": "refactor: unify merge resolution via bloom-filtered iterator pipeline (#69)\n\n## Summary\n\n- Replace hand-rolled `resolve_merge_get()` with\n`resolve_merge_via_pipeline()` that reuses `Merger → MvccStream` on a\n`key..=key` range\n- Add standard bloom pre-filtering\n(`Table::bloom_may_contain_key_hash()`) to skip many disk tables for\npoint reads\n- Eliminate duplicated operand collection / RT suppression / Indirection\nlogic between point reads and range scans\n\nNet **-143 lines** — merge resolution now lives in one place\n(`MvccStream`).\n\n## Changes\n\n| File | What |\n|------|------|\n| `table/mod.rs` | Extract `bloom_may_contain_hash()` base, add\n`bloom_may_contain_key_hash()` |\n| `range.rs` | Add `key_hash` to `IterState`, `bloom_passes()` helper\nfor unified prefix+key bloom |\n| `tree/mod.rs` | `resolve_merge_via_pipeline()` replaces ~150-line\n`resolve_merge_get()` |\n| `memtable/mod.rs` | Remove unused `get_all_for_key()` and its tests |\n| `tests/merge_operator.rs` | Update comments referencing old function\nname |\n\n## Test plan\n\n- [x] `cargo check` — 0 warnings, 0 errors\n- [x] `cargo test` — 757 passed, 0 failed\n- [x] All 44 merge operator tests pass unchanged\n- [ ] Benchmark point-read latency on 100-table tree within 5% of\nbaseline\n\nCloses #46\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **Improvements**\n* Enhanced bloom-filter pre-filtering for single- and multi-table scans\nwith optional key-hash checks and consolidated pass/fail logic;\nprefix-based skip metrics adjusted.\n\n* **Refactor**\n* Merge resolution unified into a pipeline-based point-read path;\nobsolete per-key retrieval pathway removed.\n\n* **Tests**\n* Added and updated tests validating prefix/bloom behavior and merge\nresolution with overlapping/non-matching tables.\n\n* **Chores**\n  * Added a benchmark for merge point-read performance.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-03-22T17:37:29+02:00",
          "tree_id": "96ae1889c1cb52cea1404ed15987ea348fbe6967",
          "url": "https://github.com/structured-world/lsm-tree/commit/9c4b06595c7b13dcb1a584792cf1f810769cbc16"
        },
        "date": 1774193921253,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1961848.116249624,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 1.8us | P99.9: 3.8us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 792196.3745489408,
            "unit": "ops/sec",
            "extra": "P50: 1.1us | P99: 2.4us | P99.9: 9.5us\nthreads: 1 | elapsed: 0.25s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 529814.6491663025,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 4.5us | P99.9: 10.3us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 3118700.8577534496,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 3.3us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.06s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 358026.9349713834,
            "unit": "ops/sec",
            "extra": "P50: 2.4us | P99: 5.6us | P99.9: 10.6us\nthreads: 1 | elapsed: 0.56s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 232849.66026990075,
            "unit": "ops/sec",
            "extra": "P50: 4.0us | P99: 5.1us | P99.9: 11.4us\nthreads: 1 | elapsed: 0.86s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 729768.2800349746,
            "unit": "ops/sec",
            "extra": "P50: 1.2us | P99: 3.3us | P99.9: 9.1us\nthreads: 1 | elapsed: 0.27s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 735288.3921293583,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 0.9us | P99.9: 1.3us\nthreads: 1 | elapsed: 0.27s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 433143.6017810614,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.3us | P99.9: 12.8us\nthreads: 1 | elapsed: 0.46s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "9f3ee1eb92efa6bd5cb14068147b3a4c35f1c2cd",
          "message": "fix(testing): prevent proptest oracle timeouts in CI (#95)\n\n## Summary\n\n- Increase nextest slow-timeout for prop tests from 120s to 240s\n- Set `PROPTEST_CASES=32` in `codecov` and `cross` CI jobs (were\ndefaulting to 256)\n- Reduce op sequence ranges: btreemap `200→100`, range_tombstone\n`300→150`\n- Add `fork: false` to all proptest configs to skip subprocess overhead\n\n## Root Cause\n\nThree prop tests (`prop_btreemap_oracle`, `prop_mvcc`,\n`prop_range_tombstone`) were hitting the 120s nextest terminate\nthreshold. Contributing factors:\n1. `codecov` and `cross` jobs didn't set `PROPTEST_CASES` — ran 256\ncases instead of 32\n2. Large op sequence ranges (up to 300 ops/case) with expensive\nflush+compact I/O\n3. Tight nextest budget (`30s × 4 = 120s`) left no headroom for slower\nCI runners\n\n## Test Plan\n\n- [x] All prop tests pass locally with `PROPTEST_CASES=32` (13s + 8s +\n29s)\n- [x] Full test suite passes (`cargo test --all-features`)\n- [x] `cargo clippy --all-features -- -D warnings` clean\n- [x] `cargo fmt --check` clean\n\nCloses #93",
          "timestamp": "2026-03-22T18:56:52+02:00",
          "tree_id": "f84a1baf516c88b0da3926cbb29a3f5d227a2ee1",
          "url": "https://github.com/structured-world/lsm-tree/commit/9f3ee1eb92efa6bd5cb14068147b3a4c35f1c2cd"
        },
        "date": 1774198822101,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1850147.1981736235,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.11s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1047616.0838632599,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 1.7us | P99.9: 3.9us\nthreads: 1 | elapsed: 0.19s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 645505.836350702,
            "unit": "ops/sec",
            "extra": "P50: 1.3us | P99: 5.3us | P99.9: 11.5us\nthreads: 1 | elapsed: 0.31s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2378416.168197215,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 4.3us | P99.9: 8.8us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 373530.3660194361,
            "unit": "ops/sec",
            "extra": "P50: 2.3us | P99: 6.4us | P99.9: 12.9us\nthreads: 1 | elapsed: 0.54s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 195786.71109249876,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 6.9us | P99.9: 15.0us\nthreads: 1 | elapsed: 1.02s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 869153.7675556025,
            "unit": "ops/sec",
            "extra": "P50: 0.9us | P99: 3.2us | P99.9: 10.8us\nthreads: 1 | elapsed: 0.23s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 616324.3716153931,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 0.6us | P99.9: 0.9us\nthreads: 1 | elapsed: 0.32s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 484385.11089586595,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 10.3us | P99.9: 17.5us\nthreads: 1 | elapsed: 0.41s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "610e11332a673fef9776b1024c4bf5c770e8b62a",
          "message": "feat: custom key comparison / comparator (#67)\n\n## Summary\n\n- Add pluggable `UserComparator` trait for custom key ordering instead\nof hardcoded lexicographic byte comparison\n- Thread comparator through memtable, block index search, merge\niterator, point read, and RT suppression paths\n- Enable CoordiNode to define natural ordering for composite keys\nwithout manual byte encoding tricks\n\n## Technical Details\n\n**New public API:**\n- `UserComparator` trait — `compare(&self, a: &[u8], b: &[u8]) ->\nOrdering` + `is_lexicographic()` for fast-path detection\n- `DefaultUserComparator` — lexicographic bytes (backward compatible\ndefault)\n- `Config::comparator(Arc<dyn UserComparator>)` — builder method (field\nis `pub(crate)`)\n- Bytewise equality invariant: `compare(a, b) == Equal` must imply `a ==\nb` (bloom/hash rely on this)\n- Comparator identity is not persisted — caller ensures same comparator\nacross open/close\n\n**Threading strategy:**\n- Memtable: `MemtableKey` wrapper carries `SharedComparator` for\n`SkipMap` ordering\n- Block search: `ParsedItem::compare_key` accepts `&dyn UserComparator`;\n`compare_prefixed_slice` has zero-alloc fast path for lexicographic\ncomparators\n- Merge iterator: `HeapItem` uses `InternalKey::compare_with`;\n`Merger::new` requires explicit comparator\n- Point reads: `Run::get_for_key_cmp` for correct table selection\n- RT suppression: `is_suppressed_by_range_tombstones` uses comparator\nfor key-range filter and containment\n- Data/index block iterators: store `SharedComparator`, use in seek\npredicates\n- Static `default_comparator()` via `LazyLock` avoids repeated Arc\nallocations\n\n**Known limitations:**\n- Memtable interval tree for range tombstones still uses lexicographic\n`Ord` — RT suppression in memtable may be incorrect with\nnon-lexicographic comparators (tracked as follow-up issue)\n- `KeyRange` comparisons in some compaction paths still use\nlexicographic ordering\n- Comparator identity is not persisted to disk (same approach as\nRocksDB)\n\n## Test Plan\n\n- [x] All existing lib + integration tests pass\n- [x] 6 new integration tests: reverse comparator, u64 big-endian\ncomparator\n- [x] Tests cover in-memory and after-flush point reads + range scans\n- [x] `cargo clippy` clean\n\nCloses #17\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **New Features**\n* Add support for pluggable/custom key comparators to control iteration\nand lookup ordering (e.g., reverse or numeric ordering).\n* Iteration, point-reads, and range behavior now respect configured\ncomparator semantics.\n\n* **API Changes**\n* Configuration builder accepts a comparator; components that perform\nkey ordering now require or accept a comparator to ensure consistent\nbehavior.\n\n* **Tests**\n* New and updated tests verify custom comparator behaviors and ordering\nacross operations.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-03-22T19:13:12+02:00",
          "tree_id": "5a0e02e881dc29fb82aa03d8a5e082f14f712ce8",
          "url": "https://github.com/structured-world/lsm-tree/commit/610e11332a673fef9776b1024c4bf5c770e8b62a"
        },
        "date": 1774199682975,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2091444.4122675722,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 843399.1439793878,
            "unit": "ops/sec",
            "extra": "P50: 1.0us | P99: 2.4us | P99.9: 10.7us\nthreads: 1 | elapsed: 0.24s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 563101.6653309426,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 5.5us | P99.9: 12.0us\nthreads: 1 | elapsed: 0.36s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2379434.386793406,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.2us | P99.9: 8.6us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 349538.1246912595,
            "unit": "ops/sec",
            "extra": "P50: 2.5us | P99: 6.4us | P99.9: 13.3us\nthreads: 1 | elapsed: 0.57s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 188148.19965197568,
            "unit": "ops/sec",
            "extra": "P50: 5.0us | P99: 6.4us | P99.9: 15.7us\nthreads: 1 | elapsed: 1.06s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 833798.175816351,
            "unit": "ops/sec",
            "extra": "P50: 0.9us | P99: 3.3us | P99.9: 10.3us\nthreads: 1 | elapsed: 0.24s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 734186.947534876,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.5us | P99.9: 0.8us\nthreads: 1 | elapsed: 0.27s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 459641.8157036677,
            "unit": "ops/sec",
            "extra": "P50: 1.9us | P99: 9.0us | P99.9: 18.4us\nthreads: 1 | elapsed: 0.44s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "56e3f1c58841b1c55239712f722174c530bd87bd",
          "message": "feat: block-level encryption at rest (#71)\n\n## Summary\n\n- Add pluggable `EncryptionProvider` trait for block-level encryption at\nrest\n- Ship AES-256-GCM implementation behind `encryption` feature flag\n(`aes-gcm` crate)\n- Encrypt all block types (data, index, filter, meta, range tombstone)\nafter compression, before checksumming\n- Thread encryption through Config → Writer → sub-writers → recovery →\nread path\n\n## Upstream Reference\n\nfjall-rs/lsm-tree#224\n\n## Design\n\n**Pipeline:** `raw data → compress → encrypt → checksum → disk` (reverse\non read)\n\nChecksums protect encrypted bytes on disk, so corruption is detected\ncheaply before any decryption attempt. Per-block overhead: **28 bytes**\n(12-byte random nonce + 16-byte GCM auth tag).\n\n**API:**\n\n```rust\nuse lsm_tree::{Config, Aes256GcmProvider};\n\nlet encryption = Arc::new(Aes256GcmProvider::new(&key));\nlet tree = Config::new(path, seqno, visible_seqno)\n    .with_encryption(Some(encryption))\n    .open()?;\n```\n\nThe `EncryptionProvider` trait is always available (no feature gate);\nonly the built-in `Aes256GcmProvider` requires `encryption` feature.\nCustom providers (hardware KMS, envelope encryption) can implement the\ntrait directly.\n\n## Test Plan\n\n- [x] 9 unit tests for `EncryptionProvider` / `Aes256GcmProvider`\n(roundtrip, wrong key, tamper, truncation)\n- [x] 3 integration tests: encrypted write→flush→read roundtrip,\nroundtrip with LZ4 compression, on-disk confidentiality verification\n(plaintext absent from encrypted SST)\n- [x] 427 existing unit tests pass (0 regressions)\n- [x] 727 total tests across all test binaries pass\n- [x] Clippy clean (0 new warnings)\n- [x] Builds with and without `encryption` feature\n\nCloses #20\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **New Features**\n* Optional block-level encryption-at-rest (feature-gated) with a\npluggable provider and config API; AES-256-GCM provider provided.\nWriters and table I/O now propagate encryption so on-disk blocks can be\nencrypted.\n\n* **Error Handling**\n* New encrypt/decrypt error variants surface encryption/decryption\nfailures.\n\n* **Tests**\n* Integration and unit tests for encryption roundtrips, ciphertext vs\nplaintext on-disk checks, and tamper-detection.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-03-22T20:02:00+02:00",
          "tree_id": "a6acd1aa1f6b3d80427d0599b1f669dbdd1e385a",
          "url": "https://github.com/structured-world/lsm-tree/commit/56e3f1c58841b1c55239712f722174c530bd87bd"
        },
        "date": 1774202585275,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2058917.2343684842,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.2us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1004076.0618341566,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 1.9us | P99.9: 4.1us\nthreads: 1 | elapsed: 0.20s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 583311.7075378695,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 5.4us | P99.9: 11.4us\nthreads: 1 | elapsed: 0.34s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2539211.6166900094,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.0us | P99.9: 7.3us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 382693.30674630264,
            "unit": "ops/sec",
            "extra": "P50: 2.2us | P99: 6.3us | P99.9: 12.1us\nthreads: 1 | elapsed: 0.52s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 192869.4042333247,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 7.0us | P99.9: 14.7us\nthreads: 1 | elapsed: 1.04s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 919068.9946886497,
            "unit": "ops/sec",
            "extra": "P50: 0.9us | P99: 3.1us | P99.9: 9.7us\nthreads: 1 | elapsed: 0.22s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 728852.4845537117,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.5us | P99.9: 0.8us\nthreads: 1 | elapsed: 0.27s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 473694.7959844712,
            "unit": "ops/sec",
            "extra": "P50: 1.8us | P99: 8.0us | P99.9: 17.5us\nthreads: 1 | elapsed: 0.42s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b29c5b603f8a4a6599bf0134fe9f88c4ed6df34f",
          "message": "test: add end-to-end corruption test for seqno#kv_max metadata (#96)\n\n## Summary\n- Add `meta_seqno_kv_max_corruption_returns_invalid_data` test that\nexercises the on-disk validation path for `seqno#kv_max` in\n`ParsedMeta::load_with_handle`\n- Writes a valid table, tampers the persisted `seqno#kv_max` to exceed\n`seqno#max`, recomputes the block checksum so corruption reaches the\nmetadata validation layer, and asserts `InvalidData`\n\n## Test Plan\n- `cargo test meta_seqno_kv_max_corruption_returns_invalid_data` passes\n- Full lib test suite (424 tests) passes\n\nCloses #82\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n\n## Summary by CodeRabbit\n\n* **Tests**\n* Added end-to-end corruption detection test to validate data integrity\nchecks when metadata is corrupted and system responses appropriately\nwith error handling.\n\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-03-22T20:22:27+02:00",
          "tree_id": "ee64b11a477bdcd3fe013752f4da03c326b079a3",
          "url": "https://github.com/structured-world/lsm-tree/commit/b29c5b603f8a4a6599bf0134fe9f88c4ed6df34f"
        },
        "date": 1774203824027,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2119263.2788109765,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 1.7us | P99.9: 3.7us\nthreads: 1 | elapsed: 0.09s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 826193.8582838846,
            "unit": "ops/sec",
            "extra": "P50: 1.0us | P99: 2.3us | P99.9: 9.5us\nthreads: 1 | elapsed: 0.24s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 520058.6112295559,
            "unit": "ops/sec",
            "extra": "P50: 1.8us | P99: 4.7us | P99.9: 10.7us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 3108823.721518784,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 3.1us | P99.9: 5.5us\nthreads: 1 | elapsed: 0.06s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 350573.92685438384,
            "unit": "ops/sec",
            "extra": "P50: 2.5us | P99: 5.4us | P99.9: 11.3us\nthreads: 1 | elapsed: 0.57s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 216654.8964741605,
            "unit": "ops/sec",
            "extra": "P50: 4.3us | P99: 5.6us | P99.9: 11.9us\nthreads: 1 | elapsed: 0.92s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 821464.6174426749,
            "unit": "ops/sec",
            "extra": "P50: 1.0us | P99: 2.9us | P99.9: 8.7us\nthreads: 1 | elapsed: 0.24s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 772354.7153184126,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 0.6us | P99.9: 1.1us\nthreads: 1 | elapsed: 0.26s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 433600.5739414701,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 7.6us | P99.9: 12.7us\nthreads: 1 | elapsed: 0.46s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "15353e6a2f353b36024df7509c8d48918ab11caf",
          "message": "feat(fs): add Fs trait for pluggable filesystem backends (#80)\n\n## Summary\n\n- Define `Fs` and `FsFile` traits with full filesystem operation\ncoverage (open, create_dir_all, read_dir, remove_file, remove_dir_all,\nrename, metadata, sync_directory, exists)\n- `FsFile::read_at` for pread-style concurrent reads — matches the hot\nread path (`crate::file::read_exact`) that uses `FileExt::read_at` with\nshared `&self` reference\n- `Fs::exists` returns `io::Result<bool>` (uses `try_exists`) to\ndistinguish \"not found\" from I/O errors\n- Implement `StdFs` — zero-sized default backend delegating to `std::fs`\n- Cross-platform `lock_exclusive` (unix: flock with EINTR retry,\nwindows: LockFileEx) without new dependencies\n- Make `Config` generic over `F: Fs` with default `StdFs` — existing API\nunchanged\n- Object-safe: `Arc<dyn Fs<File=.., ReadDir=..>>` compiles\n\n## Technical Details\n\n**Hybrid approach:** Generic `F: Fs` for main filesystem (zero-cost\nmonomorphized), `Arc<dyn Fs>` for per-level overrides (dynamic dispatch\nonly when tiered storage configured).\n\n**`read_at` design choice:** The `FsFile` trait includes both `Read +\nWrite + Seek` supertraits (for cold-path sequential I/O during recovery)\nand `read_at(&self, buf, offset)` (for hot-path concurrent block reads).\n`read_at` takes `&self` (not `&mut self`), enabling lock-free concurrent\nreads from multiple threads — matching lsm-tree's existing `pread`\npattern.\n\nBuilder methods moved to `impl<F: Fs> Config<F>` so they work with any\nfilesystem backend. StdFs-specific constructors (`new`,\n`new_with_generators`, `open`) remain on `impl Config`.\n\nThis is T1 (trait definition only) — call-site refactoring is tracked in\nseparate issues.\n\n**Scope note on `Config.fs` field visibility:** All `Config` fields are\n`#[doc(hidden)] pub` by convention — callers use builder methods or\n`..Default::default()`, not struct literals directly. The new `fs` field\nfollows this existing pattern. A `with_fs()` builder will be added when\ncall-site refactoring lands.\n\n## Known Limitations\n\n- Call sites still use `std::fs` directly — migration is tracked in\nseparate issues\n- `Config.fs` field is present but unused until call-site refactoring\n- `lock_exclusive` uses raw FFI (extern flock/LockFileEx) to avoid\nadding dependencies\n- Platform-specific tests (read_at, lock_exclusive) gated with\n`#[cfg(any(unix, windows))]`\n\n## Test Plan\n\n- 15 unit tests for `StdFs` (create/read/write, directory ops, rename,\nsync, metadata, set_len, lock with EINTR, object safety, read_at,\ntruncate, append, sync_data, FsOpenOptions builders, FsDirEntry fields)\n- All existing tests pass unchanged\n- Doc-test verifies `Arc<dyn Fs<..>>` object safety\n\nCloses #75",
          "timestamp": "2026-03-22T20:39:41+02:00",
          "tree_id": "6fbef071bc8f818805c2c29c41ce4e7728e2b1e3",
          "url": "https://github.com/structured-world/lsm-tree/commit/15353e6a2f353b36024df7509c8d48918ab11caf"
        },
        "date": 1774204839862,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2085988.797072006,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.2us | P99.9: 5.1us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 914613.892384124,
            "unit": "ops/sec",
            "extra": "P50: 0.9us | P99: 2.1us | P99.9: 5.1us\nthreads: 1 | elapsed: 0.22s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 537789.3916982177,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 5.7us | P99.9: 12.3us\nthreads: 1 | elapsed: 0.37s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2372576.0576706803,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.3us | P99.9: 8.8us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 361564.93315347744,
            "unit": "ops/sec",
            "extra": "P50: 2.4us | P99: 6.5us | P99.9: 13.5us\nthreads: 1 | elapsed: 0.55s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 191052.75037605033,
            "unit": "ops/sec",
            "extra": "P50: 4.9us | P99: 6.7us | P99.9: 16.8us\nthreads: 1 | elapsed: 1.05s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 921753.123929643,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 3.1us | P99.9: 10.2us\nthreads: 1 | elapsed: 0.22s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 751122.7595228659,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.5us | P99.9: 0.9us\nthreads: 1 | elapsed: 0.27s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 437527.7069889547,
            "unit": "ops/sec",
            "extra": "P50: 2.0us | P99: 10.7us | P99.9: 19.2us\nthreads: 1 | elapsed: 0.46s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c0e2b30fcc4cf69346ebb58f0024153e512a0c55",
          "message": "refactor: return CompactionResult from Tree::compact (#103)\n\n## Summary\n\n- Add `CompactionResult` and `CompactionAction` types exposing which\ncompaction path was taken (Merged/Moved/Dropped/Nothing), destination\nlevel, and input/output table counts\n- Thread `CompactionResult` through `do_compaction()` →\n`inner_compact()` → `AbstractTree::compact()` / `major_compact()`\n- Change `CompactionFlavour::finish()` to return the output table count\n- Update leveled compaction tests to assert on `CompactionResult` fields\ninstead of relying on indirect side-effect checks\n\n## Breaking change\n\n`AbstractTree::compact()` and `major_compact()` now return\n`Result<CompactionResult>` instead of `Result<()>`. Callers that discard\nthe result with `?` are unaffected; callers that pattern-match or bind\nthe `Ok(())` variant need to update. This is an intentional API change\nrequested in #73.\n\n## Test plan\n\n- [x] `cargo check --all-features` — compiles cleanly\n- [x] `cargo check --tests` — all test targets compile\n- [x] 414 lib unit tests pass (including all compaction/leveled tests)\n- [x] Integration tests (`tree_major_compaction`, `compaction_filter`)\npass\n- [x] Leveled tests now assert `CompactionAction::Merged` and\n`dest_level >= 2` for multi-level skip path\n\nCloses #73",
          "timestamp": "2026-03-22T21:30:03+02:00",
          "tree_id": "b845623dde40609f0ecf0cad4d0faef1dd50083d",
          "url": "https://github.com/structured-world/lsm-tree/commit/c0e2b30fcc4cf69346ebb58f0024153e512a0c55"
        },
        "date": 1774207880228,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2085303.858592042,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.2us | P99.9: 5.1us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1030487.5088013938,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 1.8us | P99.9: 4.3us\nthreads: 1 | elapsed: 0.19s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 602188.794882802,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 5.6us | P99.9: 11.5us\nthreads: 1 | elapsed: 0.33s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2402026.705228483,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.1us | P99.9: 8.6us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 365724.96438168,
            "unit": "ops/sec",
            "extra": "P50: 2.3us | P99: 6.4us | P99.9: 12.8us\nthreads: 1 | elapsed: 0.55s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 186949.05261240076,
            "unit": "ops/sec",
            "extra": "P50: 5.0us | P99: 7.9us | P99.9: 16.1us\nthreads: 1 | elapsed: 1.07s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 912140.505758651,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 3.1us | P99.9: 8.1us\nthreads: 1 | elapsed: 0.22s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 652781.5824517662,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 0.5us | P99.9: 0.9us\nthreads: 1 | elapsed: 0.31s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 440066.027066635,
            "unit": "ops/sec",
            "extra": "P50: 2.0us | P99: 8.0us | P99.9: 16.1us\nthreads: 1 | elapsed: 0.45s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ec762361df54422c5bb7b75b3e387f56ffecd5ff",
          "message": "refactor(fs): migrate crate::file::read_exact to FsFile::read_at (#111)\n\n## Summary\n\n- Change `file::read_exact()` to accept `&impl FsFile` instead of\n`&std::fs::File`, delegating to `FsFile::read_at()` and removing\nplatform-specific `#[cfg(unix)]`/`#[cfg(windows)]` code from the\nfunction\n- Propagate the `FsFile` trait bound to `Block::from_file`,\n`Table::read_tli`, and `ParsedMeta::load_with_handle`\n- Explicit deref `Arc<File>` at call sites where generic type inference\nrequires it\n\n## Technical Details\n\n`read_exact()` previously duplicated the platform-specific pread logic\nthat already exists in the `FsFile` trait impl for `std::fs::File`. This\nremoves that duplication and makes `read_exact()` work with any `FsFile`\nimplementation, enabling pluggable filesystem backends for the read\npath.\n\nNo behavioral changes — all existing callers pass `std::fs::File` which\nimplements `FsFile`.\n\n## Test Plan\n\n- All 431 unit tests pass\n- All integration tests pass\n- All proptest tests pass\n- `cargo clippy --lib` clean\n\nCloses #89",
          "timestamp": "2026-03-22T22:10:10+02:00",
          "tree_id": "bbbd96fc7c374efbda7bd19513ee30591f74145a",
          "url": "https://github.com/structured-world/lsm-tree/commit/ec762361df54422c5bb7b75b3e387f56ffecd5ff"
        },
        "date": 1774210542617,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2060406.9491221549,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.2us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 971130.9664016557,
            "unit": "ops/sec",
            "extra": "P50: 0.9us | P99: 1.9us | P99.9: 4.2us\nthreads: 1 | elapsed: 0.21s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 570777.7575830265,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 5.5us | P99.9: 11.7us\nthreads: 1 | elapsed: 0.35s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2389207.6337429755,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.3us | P99.9: 8.8us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 359190.0737966782,
            "unit": "ops/sec",
            "extra": "P50: 2.4us | P99: 6.5us | P99.9: 13.4us\nthreads: 1 | elapsed: 0.56s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 189374.67841102267,
            "unit": "ops/sec",
            "extra": "P50: 4.9us | P99: 6.7us | P99.9: 16.2us\nthreads: 1 | elapsed: 1.06s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 930709.6788808693,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 3.0us | P99.9: 9.7us\nthreads: 1 | elapsed: 0.21s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 742389.502588677,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 0.5us | P99.9: 0.8us\nthreads: 1 | elapsed: 0.27s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 459307.8084348388,
            "unit": "ops/sec",
            "extra": "P50: 1.9us | P99: 7.9us | P99.9: 16.9us\nthreads: 1 | elapsed: 0.44s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "2c7d5dd5a0a76f4761598ee8b83f46914f5675fb",
          "message": "refactor: centralize OwnedIndexBlockIter adapter pattern (#99)\n\n## Summary\n\n- Add `from_block` and `from_block_with_bounds` constructors to\n`OwnedIndexBlockIter`, replacing duplicated closure-based construction\nand seek-bound application across all three block index types\n- 6 duplicated call-sites across `full.rs`, `two_level.rs`, and\n`volatile.rs` now delegate to 2 centralized methods in `iter.rs`\n\n## Technical Details\n\n- `from_block(block, comparator)` — eliminates the repeated\n`::new(block, |b| b.iter(cmp))` closure pattern\n- `from_block_with_bounds(block, comparator, lo, hi) -> Option<Self>` —\nadditionally centralizes the optional `seek_lower`/`seek_upper` bound\napplication, returning `None` when bounds exclude all items\n\nNo behavioral changes — pure mechanical refactor.\n\n## Test Plan\n\n- `cargo test` — all unit and integration tests pass\n- `cargo build` — clean compilation\n\nCloses #85\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n\n## Summary by CodeRabbit\n\n* **Refactor**\n* Optimized internal iterator construction patterns across table block\nindexing operations for improved efficiency and maintainability.\nConsolidated bound-checking logic into dedicated constructors, reducing\ncode complexity without affecting existing functionality or performance\ncharacteristics.\n\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-03-22T23:42:43+02:00",
          "tree_id": "894990030acb2a6d4954f43886030cb4ce195797",
          "url": "https://github.com/structured-world/lsm-tree/commit/2c7d5dd5a0a76f4761598ee8b83f46914f5675fb"
        },
        "date": 1774216169085,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2073532.0712220527,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1012070.7707286807,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 1.8us | P99.9: 4.2us\nthreads: 1 | elapsed: 0.20s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 575665.4725963979,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 5.4us | P99.9: 11.8us\nthreads: 1 | elapsed: 0.35s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2378351.93624723,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 4.3us | P99.9: 8.1us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 377488.32518263685,
            "unit": "ops/sec",
            "extra": "P50: 2.3us | P99: 6.5us | P99.9: 13.0us\nthreads: 1 | elapsed: 0.53s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 190757.78894096598,
            "unit": "ops/sec",
            "extra": "P50: 4.9us | P99: 6.9us | P99.9: 15.0us\nthreads: 1 | elapsed: 1.05s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 922941.9009365479,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 3.0us | P99.9: 10.4us\nthreads: 1 | elapsed: 0.22s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 693381.2502304625,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.5us | P99.9: 0.7us\nthreads: 1 | elapsed: 0.29s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 475480.5226369691,
            "unit": "ops/sec",
            "extra": "P50: 1.8us | P99: 7.9us | P99.9: 15.7us\nthreads: 1 | elapsed: 0.42s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7c3fa377b163eb19dfcf15b1de8582a229d24c1c",
          "message": "perf: lazy iterator pipeline initialization for point-read merge path (#110)\n\n## Summary\n\n- Add `TreeIter::create_range_point()` fast path for single-key merge\nresolution that skips RT sort+dedup, table-skip computation, and\n`RangeTombstoneFilter` wrapping\n- Defer `RangeTombstoneFilter` forward/reverse sorting to first\n`next()`/`next_back()` call (benefits all range scans)\n- Defer `RunReader` lo/hi `table.range()` seeks to first iteration\n(resolves existing TODO comments)\n- Wire `resolve_merge_via_pipeline()` to use the new fast path\n\n## Technical Details\n\n**P0 — Dedicated point-read fast path (`create_range_point`):**\nFor point reads with merge operators, the previous\n`create_range(key..=key)` eagerly collected range tombstones from ALL\ntables, sorted them twice, deduped, computed table-skip coverage, and\nwrapped the result in `RangeTombstoneFilter`. The new fast path:\n- Collects RTs from all key-range-overlapping tables (correctness\nrequirement — an RT in a bloom-negative table can suppress the target\nkey), skipping tables whose key range cannot overlap\n- Builds iterators only from bloom-passing tables (typically 1-3)\n- Uses a simple linear post-merge RT check instead of the O(n log n)\n`RangeTombstoneFilter`\n- `MvccStream::is_rt_suppressed` handles merge-internal RT suppression\n\n**P1 — Lazy `RangeTombstoneFilter` sorting:**\nConstruction is now O(1). Forward sort deferred to first `next()`,\nreverse clone+sort deferred to first `next_back()`. Most iterators are\nforward-only, so reverse init is often never triggered.\n\n**P2 — Lazy `RunReader` init:**\n`table.range()` calls (which perform index seeks) are now deferred to\nfirst `next()`/`next_back()`. The range is stored as owned\n`(Bound<UserKey>, Bound<UserKey>)` for deferred initialization.\n\n## Known Limitations\n\n- `create_range_point` does not perform table-skip optimization\n(RT-covered table elision) since bloom filtering already eliminates most\ntables\n- `Merger` heap initialization remains eager on first `next()` — this is\nO(N) and inherent to the merge algorithm\n\n## Test Plan\n\n- [x] All 429 lib tests pass unchanged\n- [x] 7 integration tests for point-read merge fast path (RT\nsuppression, bloom filtering, sealed memtable, multi-operand, etc.)\n- [x] Clippy clean (lib)\n- [ ] Benchmark: `cargo bench --bench merge_point_read` (100-table case)\n\nCloses #84",
          "timestamp": "2026-03-23T00:23:56+02:00",
          "tree_id": "66f12010c66350fcdfc89d25f9d7fd06736239ad",
          "url": "https://github.com/structured-world/lsm-tree/commit/7c3fa377b163eb19dfcf15b1de8582a229d24c1c"
        },
        "date": 1774218605818,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2066493.67014907,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.2us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 906558.5120604375,
            "unit": "ops/sec",
            "extra": "P50: 0.9us | P99: 2.2us | P99.9: 11.4us\nthreads: 1 | elapsed: 0.22s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 514573.17809230014,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 5.7us | P99.9: 12.4us\nthreads: 1 | elapsed: 0.39s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2327035.4678135975,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 4.1us | P99.9: 8.1us\nthreads: 1 | elapsed: 0.09s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 398093.08396806434,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.3us | P99.9: 12.1us\nthreads: 1 | elapsed: 0.50s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 189309.66934196316,
            "unit": "ops/sec",
            "extra": "P50: 5.0us | P99: 6.7us | P99.9: 15.0us\nthreads: 1 | elapsed: 1.06s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 939059.24772236,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 3.0us | P99.9: 7.8us\nthreads: 1 | elapsed: 0.21s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 669714.0508857509,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.5us | P99.9: 0.8us\nthreads: 1 | elapsed: 0.30s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 504920.836024593,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 7.8us | P99.9: 13.4us\nthreads: 1 | elapsed: 0.40s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "56e2695b49fb05e80629368d3ec56727cf6278cd",
          "message": "feat(memtable): arena-based skiplist for memtable (#79)\n\n## Summary\n\n- Replace `crossbeam-skiplist` with a custom arena-based concurrent\nskiplist\n- Key data stored in multi-block arena (lazy 64 MiB / 4 MiB blocks) for\ncache locality\n- Values stored in lock-free segmented `ValueStore` (wait-free reads)\n- CAS-based lock-free inserts, lock-free traversal,\n`DoubleEndedIterator` support\n- Pluggable `SharedComparator` threaded through skiplist for custom key\nordering\n- Remove `crossbeam-skiplist` dependency entirely\n- Fix `benches/memtable.rs` and `benches/merge.rs` for current API\n\n## Technical Details\n\n**Multi-block arena** (`src/memtable/arena.rs`): Lazily-allocated blocks\n(64 MiB on 64-bit, 4 MiB on 32-bit) with 4-byte alignment. u32 offset\nencodes block index + within-block offset. Lock-free allocation via CAS\non atomic cursor. Blocks zeroed via `alloc + write_bytes`.\n\n**Skiplist** (`src/memtable/skiplist.rs`): Nodes encode key_offset,\nkey_len, seqno, value_type, and a variable-height tower of `AtomicU32`\nnext-pointers. Height generation uses splitmix64 with geometric\ndistribution (P=1/4, max 20 levels). Backward iteration uses O(log n)\npredecessor search. User key comparison delegates to `SharedComparator`.\nCAS retry re-searches from head (O(log n) walk-down) to avoid OOB tower\nreads on short nodes.\n\n**Lock-free ValueStore** (`src/memtable/value_store.rs`): Segmented\narray with 64K entries per segment, allocated lazily via AtomicPtr CAS.\nReads are wait-free (one atomic load + dereference).\n\n**Concurrent insert correctness**: Successor tracked from the comparison\nloop itself (never re-read from the list). CAS retry re-searches from\nhead sentinel to avoid reading tower levels above a node's allocated\nheight.\n\n## Test Plan\n\n- [x] All lib unit tests pass (including custom comparator tests)\n- [x] All integration tests pass (including `a_lot_of_ranges` with 1M\nentries)\n- [x] Concurrent insert + read regression test (8 writers + 1 reader, no\nSIGBUS)\n- [x] `DoubleEndedIterator` convergence tested with interleaved\n`next`/`next_back`\n- [x] `cargo clippy --lib -- -D warnings` passes\n- [x] `cargo fmt --all -- --check` clean\n\nCloses #19",
          "timestamp": "2026-03-23T02:33:23+02:00",
          "tree_id": "eea3d2c500341c2214b75fd9f85fd97b34650247",
          "url": "https://github.com/structured-world/lsm-tree/commit/56e2695b49fb05e80629368d3ec56727cf6278cd"
        },
        "date": 1774226074139,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2016912.4766982319,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1256999.4126795945,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.6us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 530188.2790831785,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 5.6us | P99.9: 15.2us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2371628.8866580296,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 4.3us | P99.9: 8.7us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 362941.28554191685,
            "unit": "ops/sec",
            "extra": "P50: 2.4us | P99: 6.4us | P99.9: 12.7us\nthreads: 1 | elapsed: 0.55s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 185897.23286429144,
            "unit": "ops/sec",
            "extra": "P50: 5.0us | P99: 7.8us | P99.9: 16.4us\nthreads: 1 | elapsed: 1.08s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1211359.5312726668,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 698870.5200595704,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.6us | P99.9: 3.2us\nthreads: 1 | elapsed: 0.29s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 468741.65237033926,
            "unit": "ops/sec",
            "extra": "P50: 1.8us | P99: 10.0us | P99.9: 17.1us\nthreads: 1 | elapsed: 0.43s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ff827175b7e3bb7b1e6460ef3280056857e79e7f",
          "message": "feat: add UserComparator::name() for stable identity persistence (#101)\n\n## Summary\n\n- Add `name() -> &'static str` method to `UserComparator` trait for\nstable comparator identity\n- Persist comparator name in tree manifest; check on reopen — mismatch\nreturns `Error::ComparatorMismatch`\n- Backward compatible: trees created before this change default to\n`\"default\"` (matching `DefaultUserComparator`)\n\n## Technical Details\n\n- Comparator name written as `comparator_name` section in sfa archive\nduring `persist_version`\n- `SuperVersions` stores `comparator_name: Arc<str>` so flush/compaction\nversion upgrades include it without extra plumbing\n- Check runs in `Tree::recover` after manifest decode, before any data\naccess\n- Follows RocksDB `Comparator::Name()` pattern (requested in #67 review)\n\n## Test Plan\n\n- [x] Reopen with same comparator succeeds\n- [x] Reopen with different custom comparator fails with\n`ComparatorMismatch`\n- [x] Reopen custom-comparator tree with default comparator fails\n- [x] Reopen default-comparator tree with default comparator succeeds\n- [x] All existing tests pass (429 unit + integration)\n\nCloses #74\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n\n## Summary by CodeRabbit\n\n* **New Features**\n- Tree comparators are now persisted and automatically validated when\nreopening a tree.\n\n* **Bug Fixes**\n- Attempting to reopen a tree with an incompatible comparator now fails\nwith a clear error message.\n\n* **Tests**\n- Added comprehensive tests for comparator persistence and validation\nbehavior.\n\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-03-23T03:31:52+02:00",
          "tree_id": "541ff6b0f306ac98605d2e56fb0ad0260bcb2e3a",
          "url": "https://github.com/structured-world/lsm-tree/commit/ff827175b7e3bb7b1e6460ef3280056857e79e7f"
        },
        "date": 1774229570142,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2015009.2802259906,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1277896.8965820211,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 599904.1731071004,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 5.6us | P99.9: 11.6us\nthreads: 1 | elapsed: 0.33s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2388765.8064036216,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 4.2us | P99.9: 7.9us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 413780.97110483685,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.3us | P99.9: 12.2us\nthreads: 1 | elapsed: 0.48s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 201322.20228648128,
            "unit": "ops/sec",
            "extra": "P50: 4.7us | P99: 6.5us | P99.9: 14.8us\nthreads: 1 | elapsed: 0.99s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1058086.997542392,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.8us | P99.9: 50.8us\nthreads: 1 | elapsed: 0.19s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 651522.796740309,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.6us | P99.9: 4.4us\nthreads: 1 | elapsed: 0.31s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 510809.0450336789,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 9.0us | P99.9: 15.7us\nthreads: 1 | elapsed: 0.39s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1ee0db2851cf53c5e391ae11727fdffdb76ce378",
          "message": "fix(test): use shared seqno counter in proptest oracle (#97)\n\n## Summary\n\n- Fix proptest oracle to use shared `SequenceNumberCounter` per API\ncontract (was using independent counter)\n- Add regression test for stale point-read after compact cycles (derived\nfrom proptest seed)\n- Fix clippy `never_loop` lint in oracle's `get()` method\n\n## Technical Details\n\nThe proptest used an independent seqno counter (`let mut seqno = 1`)\nthat did not advance on flush/compact, violating the API contract\nrequiring data seqnos from the shared `SequenceNumberCounter` passed to\n`Config::new`. With independent counters, internal SuperVersion seqnos\nadvance faster than data seqnos, causing `get_version_for_snapshot` to\nreturn a stale SuperVersion whose memtable misses recent inserts.\n\nRoot cause: `get_version_for_snapshot(S)` finds the latest SV with\n`seqno < S`. When the internal counter (advanced by flush/compact)\noutpaces user data seqnos, the returned SV references an old memtable\nthat was rotated away.\n\nFix: use `seqno_counter.next()` from the shared counter for all data\noperations in the proptest, keeping SV seqnos and data seqnos properly\ninterleaved.\n\n**Note:** The bloom skipping feature (src/ changes) was merged via PR\n#64. This PR now contains only test improvements.\n\n## Test Plan\n\n- [x] Regression test\n`point_read_after_compact_flush_returns_latest_value` passes\n- [x] Proptest `prop_btreemap_oracle_correctness` passes (256 cases)\n- [x] All 468+ library and integration tests pass\n- [x] `cargo clippy --tests` clean\n\nCloses #58",
          "timestamp": "2026-03-23T10:38:52+02:00",
          "tree_id": "3a1f961e12d9371e96d4c79edbb24f1641200132",
          "url": "https://github.com/structured-world/lsm-tree/commit/1ee0db2851cf53c5e391ae11727fdffdb76ce378"
        },
        "date": 1774255215618,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1943588.3396710998,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 1.8us | P99.9: 3.8us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1113468.3607464232,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 1.7us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.18s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 660932.6048542975,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 4.3us | P99.9: 10.0us\nthreads: 1 | elapsed: 0.30s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 3147534.081735102,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 3.1us | P99.9: 5.6us\nthreads: 1 | elapsed: 0.06s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 441976.9519450217,
            "unit": "ops/sec",
            "extra": "P50: 1.9us | P99: 5.0us | P99.9: 9.5us\nthreads: 1 | elapsed: 0.45s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 226256.853796655,
            "unit": "ops/sec",
            "extra": "P50: 4.1us | P99: 5.0us | P99.9: 10.0us\nthreads: 1 | elapsed: 0.88s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1130393.2843967993,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.3us | P99.9: 5.0us\nthreads: 1 | elapsed: 0.18s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 736422.9592833329,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 1.6us | P99.9: 3.8us\nthreads: 1 | elapsed: 0.27s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 517239.433865819,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 5.8us | P99.9: 10.6us\nthreads: 1 | elapsed: 0.39s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "74143c6a79ab5bc490b35169dd566d064657e60d",
          "message": "feat(compaction): compute L2 overlaps per-range in multi-level path (#108)\n\n## Summary\n\n- Query L2 overlaps per individual input table key range instead of one\ncoarse aggregate range during multi-level compaction\n- On sparse keyspaces where L1 tables are disjoint (e.g. `[a,d]` and\n`[x,z]`), the old aggregate range `[a,z]` pulled in gap-filling L2\ntables that had zero actual overlap with input data\n- Add regression test verifying multi-level compaction data integrity\nand `CompactionResult` assertions\n\n## Technical Details\n\nThe multi-level compaction path (L0+L1→L2) previously computed a single\nmerged `KeyRange` from all L0 and L1 inputs, then queried L2 for any\ntable overlapping that combined span. On sparse keyspaces this\nover-selects L2 tables occupying gaps between disjoint input ranges,\ncausing unnecessary I/O and write amplification.\n\nThe fix iterates each L0 and L1 table individually, queries L2 for\noverlaps against that table's key range, and deduplicates via the\nexisting `HashSet<TableId>`.\n\n## Test Plan\n\n- [x] All leveled compaction tests pass (including new\n`multi_level_sparse_keyspace_data_integrity`)\n- [x] Test asserts `CompactionResult.action == Merged` and `dest_level\n>= 2`\n- [x] Existing multi-level tests unchanged and passing\n\n**Known coverage gap:** The per-range L2 overlap inner loop requires L2\nto be non-empty, but the leveled strategy's force-trivial-move scoring\n(99.99) cascades all intermediate levels to Lmax with small test data,\nmaking it impossible to populate both L1 and L2 simultaneously in unit\ntests.\n\nCloses #72",
          "timestamp": "2026-03-23T12:17:50+02:00",
          "tree_id": "56918f3c36b88909897a86888e05b4765090d59f",
          "url": "https://github.com/structured-world/lsm-tree/commit/74143c6a79ab5bc490b35169dd566d064657e60d"
        },
        "date": 1774261144786,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1958268.802256709,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.4us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 967658.3469708246,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 2.7us | P99.9: 10.0us\nthreads: 1 | elapsed: 0.21s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 478792.04406102933,
            "unit": "ops/sec",
            "extra": "P50: 1.9us | P99: 5.8us | P99.9: 17.2us\nthreads: 1 | elapsed: 0.42s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2405412.794573466,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.3us | P99.9: 8.8us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 339426.45743598416,
            "unit": "ops/sec",
            "extra": "P50: 2.6us | P99: 6.6us | P99.9: 14.2us\nthreads: 1 | elapsed: 0.59s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 193367.67458796798,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 7.0us | P99.9: 16.9us\nthreads: 1 | elapsed: 1.03s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 931932.5374595292,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 3.1us | P99.9: 9.8us\nthreads: 1 | elapsed: 0.21s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 717197.8680533142,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 4.1us\nthreads: 1 | elapsed: 0.28s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 454123.3025554974,
            "unit": "ops/sec",
            "extra": "P50: 1.9us | P99: 7.9us | P99.9: 15.9us\nthreads: 1 | elapsed: 0.44s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "befb45007bdbbd0ec23ce38b3bd7cc9935e18693",
          "message": "test+fix: integration tests for compaction/merge with custom comparator (#100)\n\n## Summary\n\n- Add 19 integration tests exercising compaction and merge operator\npaths with custom `UserComparator` implementations (`ReverseComparator`,\n`U64BigEndianComparator`)\n- Fix bug where `Run::push()` sorted tables lexicographically instead of\nby the configured comparator, breaking inter-SST ordering for\nnon-lexicographic comparators (#98)\n- Add unit tests for all new comparator-aware `Run` methods (`push_cmp`,\n`get_for_key_cmp`, `get_overlapping_cmp`, `range_overlap_indexes_cmp`)\n\n## What changed\n\n**Tests** (`tests/custom_comparator_compaction.rs`):\n- Compaction with Leveled, SizeTiered, and major_compact strategies\n- Merge operator resolution through compaction stream with custom\ncomparator\n- Tombstone handling and cross-flush merge operands\n- Update and delete scenarios after compaction\n- Range scans after compaction (2 ignored — RunReader comparator\nplumbing tracked in #116)\n\n**Bug fix** (discovered during test development):\n- `Run::push()` used lexicographic `.cmp()` to sort tables instead of\nthe custom comparator\n- Added `push_cmp()`, `range_overlap_indexes_cmp()`,\n`get_overlapping_cmp()` to `Run`\n- Added `overlaps_with_key_range_cmp()` to `KeyRange`\n- Threaded comparator through `optimize_runs()`,\n`Version::with_new_l0_run()`, `with_merge()`, `with_moved()`,\n`with_dropped()` and all callers\n- Added doc comments clarifying lexicographic assumptions on existing\nmethods (`push`, `get_overlapping`, `extend`, `contains_key`)\n\n**Unit tests** (`src/version/run.rs`):\n- `push_cmp_sorts_by_comparator` — verifies comparator-aware sorting\n- `get_for_key_cmp_reverse` — point lookup with reverse comparator\n- `get_overlapping_cmp_reverse` — overlap detection with reverse\ncomparator\n- `range_overlap_indexes_cmp_reverse` — inclusive, exclusive, and\nsemi-open bounds\n\n## Test plan\n\n- [x] 17/19 new integration tests pass (2 range scan tests ignored —\n#116)\n- [x] All library unit tests pass\n- [x] All existing integration tests pass (custom_comparator,\nmerge_operator, compaction_filter, etc.)\n- [x] Clippy clean (`cargo clippy --lib --tests`)\n\nCloses #86\nFixes #98\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **Refactor**\n* Propagated comparator context through versioning and compaction flows\nso run transformations (merge/move/drop/new-L0) accept a comparator.\n\n* **New Features**\n* Comparator-aware run APIs and range operations enabling custom\nordering for insertion, sorting, and overlap queries.\n\n* **Documentation**\n* Clarified key-range behavior: default is lexicographic and pointed to\ncomparator-based overlap API.\n\n* **Tests**\n* Added integration tests validating custom comparators across\ncompaction, merge, tombstone, and iteration.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-03-23T13:51:49+02:00",
          "tree_id": "e6e9fb0334af23e65171d0bb7622fc8da299ec22",
          "url": "https://github.com/structured-world/lsm-tree/commit/befb45007bdbbd0ec23ce38b3bd7cc9935e18693"
        },
        "date": 1774266782580,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1984197.2971502524,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1253442.4700678252,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.4us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 541667.3110250721,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 5.6us | P99.9: 11.6us\nthreads: 1 | elapsed: 0.37s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2425439.2682740777,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.2us | P99.9: 8.4us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 381970.60837008833,
            "unit": "ops/sec",
            "extra": "P50: 2.3us | P99: 6.4us | P99.9: 12.3us\nthreads: 1 | elapsed: 0.52s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 195600.6839889902,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 6.8us | P99.9: 18.8us\nthreads: 1 | elapsed: 1.02s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1145685.1717754707,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.8us | P99.9: 6.0us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 718056.7379855699,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.5us | P99.9: 4.5us\nthreads: 1 | elapsed: 0.28s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 477980.910742533,
            "unit": "ops/sec",
            "extra": "P50: 1.8us | P99: 7.8us | P99.9: 16.7us\nthreads: 1 | elapsed: 0.42s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5b32b1cf8d0815dad2493c3bcaf7598c6e4168aa",
          "message": "perf(encryption): replace OsRng with thread-local seeded CSPRNG (#104)\n\n## Summary\n\n- Replace per-block `OsRng` (`getrandom` syscall) with a thread-local\n`ChaCha20Rng` seeded once from `OsRng` per thread\n- Eliminates 1-10 µs syscall overhead per block encryption under\ncontention\n- Fork-aware: tracks process ID via `ForkAwareRng` and reseeds after\n`fork()` to prevent nonce reuse across PIDs\n- No security reduction — `ChaCha20Rng` is a CSPRNG with identical\nguarantees\n\n## Technical Details\n\n- `rand_chacha 0.3` added as optional dep gated behind `encryption`\nfeature (already in transitive dep tree via `aes-gcm` — zero new\ndownloads)\n- `rand_core` types (`OsRng`, `SeedableRng`) accessed via\n`aes_gcm::aead::rand_core` re-export to avoid version-skew with a direct\ndependency\n- Module-scope `thread_local!` with `ForkAwareRng` wrapper — compares\n`std::process::id()` on each call and reseeds if PID changed\n- Single `borrow_mut()` per call — reseed and use share the same\n`RefMut` guard\n- `EncryptionProvider` trait API unchanged; change is internal to\n`Aes256GcmProvider::encrypt()`\n\n## Known Limitations\n\n- Estimated 5-15% improvement for write-heavy encrypted workloads; no\nbenchmark added yet\n\n## Test Plan\n\n- [x] All 11 encryption unit tests pass (including fork-aware reseed +\nnonce uniqueness)\n- [x] All 3 encryption integration tests pass (`encryption_roundtrip`)\n- [x] `cargo clippy --features encryption -- -D warnings` clean\n\nCloses #87\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n\n## Summary by CodeRabbit\n\n* **New Features**\n* Enhanced encryption feature with improved random number generation\ninfrastructure.\n* Optimized nonce generation with thread-local caching for better\nperformance.\n* Added fork-aware random number generation to ensure security across\nprocess forks.\n\n* **Tests**\n  * Added tests validating nonce uniqueness and fork-aware behavior.\n\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-03-23T14:36:46+02:00",
          "tree_id": "e37545fb1fe8bcc041192af0ebc4ddbe7c4cfae7",
          "url": "https://github.com/structured-world/lsm-tree/commit/5b32b1cf8d0815dad2493c3bcaf7598c6e4168aa"
        },
        "date": 1774269470367,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2067500.2441717787,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1293912.160107552,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.4us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.15s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 545845.9194599161,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 5.8us | P99.9: 12.0us\nthreads: 1 | elapsed: 0.37s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2393064.134358107,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.3us | P99.9: 8.5us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 412615.2559225907,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.2us | P99.9: 12.0us\nthreads: 1 | elapsed: 0.48s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 200823.26936972968,
            "unit": "ops/sec",
            "extra": "P50: 4.7us | P99: 6.6us | P99.9: 14.7us\nthreads: 1 | elapsed: 1.00s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1202598.0423014704,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 6.0us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 675868.4717239138,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.6us | P99.9: 4.3us\nthreads: 1 | elapsed: 0.30s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 494393.16742709896,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 8.8us | P99.9: 15.4us\nthreads: 1 | elapsed: 0.40s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "61cf608c7d682025fb2a426d0ffebc45199b31bf",
          "message": "perf: partition-aware bloom filtering for point-read pipeline (#102)\n\n## Summary\n\n- Add `Table::bloom_may_contain_key(key, key_hash)` — seeks the\npartitioned filter TLI by user key and checks only the matching\npartition's bloom filter, replacing the conservative `Ok(true)` fallback\n- Add `bloom_key` field to `IterState`, populated by\n`resolve_merge_via_pipeline` for single-key point-read pipelines\n- `bloom_passes()` dispatches to the key-aware method when `bloom_key`\nis available, falls back to hash-only path otherwise\n- `debug_assert` ensures `bloom_key` is never set without `key_hash`\n\n## Technical Details\n\nPreviously, `bloom_may_contain_key_hash` returned `Ok(true)` for\npartitioned/TLI filter configurations because the partition index is\nkeyed by user key boundaries, not by raw hash — checking by hash alone\nwould require scanning all partitions. The new `bloom_may_contain_key`\nmethod accepts the actual user key, seeks the TLI to the correct\npartition in O(log P), and queries only that partition's bloom filter.\nKeys beyond all partition boundaries return `Ok(false)` (definite miss).\n\nThe existing `bloom_may_contain_key_hash` (hash-only) path is preserved\nunchanged for callers that don't have the key available (e.g. prefix\nscans).\n\n`pinned_filter_block` and `pinned_filter_index` are mutually exclusive\n(set at construction time), so the branch order in\n`bloom_may_contain_key` is safe.\n\n`Slice::from(key)` in the merge pipeline copies the key once per\nresolution (not zero-copy), but the cost is negligible compared to I/O\nsavings.\n\n## Known Limitations\n\n- Only `resolve_merge_via_pipeline` sets `bloom_key` — general range\nscans still use hash-only bloom pre-filtering (which is correct but less\neffective for partitioned filters)\n- Unpinned filter TLI path falls through to hash-only (consistent with\nexisting `unimplemented!` for unpinned TLI in `Table::get`)\n\n## Test Plan\n\n- [x] `partitioned_bloom_skip_for_point_reads` — verifies bloom filter\nis queried for non-matching key with partitioned filters (metrics:\n`filter_queries >= 1`)\n- [x] `partitioned_bloom_skip_beyond_partitions` — verifies key beyond\nall partition boundaries is correctly rejected\n- [x] `partitioned_bloom_skip_merge_pipeline` — exercises\n`bloom_may_contain_key` through the merge pipeline with bracketing\ndistractor keys\n- [x] `full_filter_bloom_skip_merge_pipeline` — covers the full-filter\ndelegation path through the merge pipeline\n- [x] `bloom_may_contain_key_full_filter` — unit test: both methods\nagree for full filters\n- [x] `bloom_may_contain_key_partitioned_filter` — unit test: contrast\nassertion proving key-based rejects while hash-only returns conservative\n`Ok(true)`\n- [x] All existing tests pass unchanged\n\nCloses #83\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **Performance Improvements**\n* Partition-aware bloom checks reduce unnecessary reads by skipping keys\noutside targeted partitions.\n\n* **New Features**\n* Key-aware bloom query path added; iterators now include the bloom key\nwhen available to enable more precise partitioned filtering while\npreserving conservative behavior when partition info is absent.\n\n* **Tests**\n* Added unit and integration tests validating full and partitioned bloom\nbehavior across point reads and merge-pipeline scenarios.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-03-23T15:09:41+02:00",
          "tree_id": "c4df5f5eca798a06ec6ada85a6e94d80a093f25d",
          "url": "https://github.com/structured-world/lsm-tree/commit/61cf608c7d682025fb2a426d0ffebc45199b31bf"
        },
        "date": 1774271460942,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1994172.3702572263,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1296803.561421995,
            "unit": "ops/sec",
            "extra": "P50: 0.6us | P99: 1.4us | P99.9: 5.5us\nthreads: 1 | elapsed: 0.15s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 611814.0759672897,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 5.5us | P99.9: 11.5us\nthreads: 1 | elapsed: 0.33s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2440687.4508613343,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.2us | P99.9: 8.1us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 397995.90198345575,
            "unit": "ops/sec",
            "extra": "P50: 2.2us | P99: 6.2us | P99.9: 12.2us\nthreads: 1 | elapsed: 0.50s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 197978.4760315185,
            "unit": "ops/sec",
            "extra": "P50: 4.7us | P99: 6.4us | P99.9: 15.0us\nthreads: 1 | elapsed: 1.01s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1186803.626653511,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.8us | P99.9: 6.0us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 661774.6672941922,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 3.3us\nthreads: 1 | elapsed: 0.30s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 505507.3918222775,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 10.0us | P99.9: 16.2us\nthreads: 1 | elapsed: 0.40s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c1f55111f136221cec2430031be24c55bc7b6f8a",
          "message": "refactor(fs): thread Fs through FileAccessor and DescriptorTable (#112)\n\n## Summary\n\n- Replace hardcoded `Arc<std::fs::File>` with `Arc<dyn FsFile>` in\n`DescriptorTable` and `FileAccessor` (Option B — dynamic dispatch)\n- Thread `&dyn FsFile` through `Block::from_file`,\n`ParsedMeta::load_with_handle`, and blob `Reader`\n- Strengthen `FsFile::read_at` contract to fill-or-EOF with EINTR retry\nin `StdFs`\n\n## Technical Details\n\nThe FD cache (`DescriptorTable`) and its access wrapper (`FileAccessor`)\nwere hardcoded to `std::fs::File`. This blocked pluggable filesystem\nbackends introduced by the `Fs` trait in #80.\n\n**Approach:** Option B from the issue — `Arc<dyn FsFile>` for\nsimplicity. Vtable overhead (~5ns) is negligible vs I/O latency. Call\nsites use type-annotated bindings (`let fd: Arc<dyn FsFile> =\nArc::new(...)`) for unsizing coercion at the file-open boundary. Future\ncall-site refactoring will replace `std::fs::File::open` with\n`Fs::open`, eliminating the coercions.\n\n**`FsFile::read_at` contract:** Strengthened to fill-or-EOF semantics —\nimplementations must either fill the buffer completely or return a short\nread only at EOF. `StdFs::read_at` now includes a retry loop that\nhandles EINTR and OS-level short reads, matching the documented\ncontract. `file::read_exact` relies on this single-call guarantee.\n\n## Test Plan\n\n- [x] `cargo check` — zero errors, zero warnings\n- [x] `cargo clippy --lib` — clean\n- [x] `cargo test --lib` — all tests pass\n- [x] `cargo test` — all integration + doc tests pass\n- [x] `codecov/patch` — passing\n- [x] All CI checks green (lint, test matrix, cross-compilation)\n\nCloses #90",
          "timestamp": "2026-03-23T16:09:28+02:00",
          "tree_id": "d218ca68edde10a1a977c258cf906c0263be90cd",
          "url": "https://github.com/structured-world/lsm-tree/commit/c1f55111f136221cec2430031be24c55bc7b6f8a"
        },
        "date": 1774275039501,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1993465.4600911306,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 1.8us | P99.9: 3.7us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 964781.9145455514,
            "unit": "ops/sec",
            "extra": "P50: 0.9us | P99: 2.1us | P99.9: 7.6us\nthreads: 1 | elapsed: 0.21s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 552449.1996926714,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 4.6us | P99.9: 10.1us\nthreads: 1 | elapsed: 0.36s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 3035687.9728046074,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 3.2us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.07s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 372414.6336047507,
            "unit": "ops/sec",
            "extra": "P50: 2.3us | P99: 5.4us | P99.9: 11.1us\nthreads: 1 | elapsed: 0.54s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 220381.37949769635,
            "unit": "ops/sec",
            "extra": "P50: 4.2us | P99: 5.3us | P99.9: 11.9us\nthreads: 1 | elapsed: 0.91s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 968575.0428092008,
            "unit": "ops/sec",
            "extra": "P50: 0.9us | P99: 2.6us | P99.9: 7.7us\nthreads: 1 | elapsed: 0.21s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 767656.2531102074,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 0.7us | P99.9: 4.1us\nthreads: 1 | elapsed: 0.26s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 452584.4841493192,
            "unit": "ops/sec",
            "extra": "P50: 2.0us | P99: 6.1us | P99.9: 11.0us\nthreads: 1 | elapsed: 0.44s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d7279d395919ec7024dbc70fbdf426fb9faf53ab",
          "message": "feat(fs): simplify dyn Fs object safety for per-level routing (#109)\n\n## Summary\n\nRemove associated types (`File`, `ReadDir`) from the `Fs` trait so that\n`Arc<dyn Fs>` works without specifying type parameters — enabling\nergonomic per-level filesystem routing.\n\n- `Fs::open()` now returns `Box<dyn FsFile>` (allocation overhead is\nnegligible for syscall-backed implementations like `StdFs`)\n- `Fs::read_dir()` now returns `Vec<FsDirEntry>` (cold-path only:\nrecovery, compaction file listing)\n- Remove `StdReadDir` public type (logic inlined into `StdFs::read_dir`)\n\n**Before:** `Arc<dyn Fs<File = std::fs::File, ReadDir = StdReadDir>>`\n**After:** `Arc<dyn Fs>`\n\n## Changes\n\n- `src/fs/mod.rs` — remove `type File` and `type ReadDir` associated\ntypes, update method signatures and object-safety doc\n- `src/fs/std_fs.rs` — update `StdFs` impl, remove `StdReadDir`, update\ntests\n\n## Testing\n\nAll 429 unit tests + integration tests pass. Object-safety test updated\nto assert simple `Arc<dyn Fs>`.\n\nCloses #92",
          "timestamp": "2026-03-23T17:37:59+02:00",
          "tree_id": "848803e4baa780cbd79b4e3ccc4a3aebc246ac67",
          "url": "https://github.com/structured-world/lsm-tree/commit/d7279d395919ec7024dbc70fbdf426fb9faf53ab"
        },
        "date": 1774280349982,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1974496.7873999383,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 1.8us | P99.9: 3.9us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1048053.5925977568,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 1.8us | P99.9: 6.9us\nthreads: 1 | elapsed: 0.19s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 605989.6520176838,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 4.5us | P99.9: 9.9us\nthreads: 1 | elapsed: 0.33s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 3054133.461936785,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 3.1us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.07s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 403838.6234207462,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 5.2us | P99.9: 10.0us\nthreads: 1 | elapsed: 0.50s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 219523.5260964623,
            "unit": "ops/sec",
            "extra": "P50: 4.2us | P99: 6.1us | P99.9: 34.8us\nthreads: 1 | elapsed: 0.91s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1103902.1456098924,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 2.4us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.18s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 784591.1446116187,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 1.6us | P99.9: 3.2us\nthreads: 1 | elapsed: 0.25s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 510888.07605290384,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 6.9us | P99.9: 11.2us\nthreads: 1 | elapsed: 0.39s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e99ede98b1cb553be5096decb22cc2b9a8db8d1c",
          "message": "perf(encryption): reduce allocations in encrypt/decrypt block pipeline (#105)\n\n## Summary\n\n- Add `encrypt_vec`/`decrypt_vec` buffer-reusing methods to\n`EncryptionProvider` trait\n- `Aes256GcmProvider` overrides both for in-place encrypt/decrypt\n(memmove instead of alloc)\n- Write path: reuses owned compression buffer via `encrypt_vec` (saves 1\nalloc per block)\n- Read `from_reader`: reads into Vec when encrypted, `decrypt_vec`\nreuses buffer in-place\n- Read `from_file`: encrypted path reads into Vec via single `read_at`,\nstrips header via `copy_within`, then `decrypt_vec` in-place — single\nI/O, single allocation, no Slice\n\n## Technical Details\n\n**Trait extension** — `encrypt_vec(Vec<u8>)` and `decrypt_vec(Vec<u8>)`\nwith default impls delegating to `encrypt`/`decrypt`.\nBackwards-compatible: existing implementors automatically get the\ndefault.\n\n**AES-256-GCM in-place strategy:**\n- `encrypt_vec`: `reserve(28)` → `resize` + `copy_within` (shift right)\n→ `copy_from_slice` (nonce) → encrypt in-place → `extend(tag)`\n- `decrypt_vec`: `copy_within` (shift left, strip nonce) → `truncate`\n(strip tag) → decrypt in-place → return\n\n**Block pipeline savings (per block with encryption enabled):**\n| Path | Before | After |\n|------|--------|-------|\n| Write (compress+encrypt) | 2 allocs | 1 alloc |\n| Read `from_reader` | 3 allocs, peak 2× block | 2 allocs, peak 1× block\n|\n| Read `from_file` | Slice + Vec copy overlap | single Vec via\n`read_at`, no Slice |\n\n## Test Plan\n\n- [x] 7 unit tests for `encrypt_vec`/`decrypt_vec` (roundtrip,\ncross-interop, empty, tampered, truncated)\n- [x] 2 tests for default trait method delegation (XorProvider stub)\n- [x] 14 encrypted block tests (roundtrip × compression ×\nfrom_reader/from_file + error paths)\n- [x] All lib tests pass\n- [x] Clippy clean (0 warnings)\n- [x] Codecov patch coverage passing\n\nCloses #88\n\n## Related\n\n- #127 — extract tempfile helper for `from_file` tests (out of scope for\nthis PR)\n- #128 — mixed-load encryption stress test (out of scope for this PR)",
          "timestamp": "2026-03-23T19:11:23+02:00",
          "tree_id": "a8888455876b4fe0461f96cce4b025620996636e",
          "url": "https://github.com/structured-world/lsm-tree/commit/e99ede98b1cb553be5096decb22cc2b9a8db8d1c"
        },
        "date": 1774286008855,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1964341.5629717764,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.4us | P99.9: 5.4us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1272315.2808462312,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.3us | P99.9: 5.6us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 575062.7279861344,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 5.7us | P99.9: 11.9us\nthreads: 1 | elapsed: 0.35s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2397160.180220899,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.3us | P99.9: 8.1us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 383063.8111680508,
            "unit": "ops/sec",
            "extra": "P50: 2.2us | P99: 6.4us | P99.9: 13.0us\nthreads: 1 | elapsed: 0.52s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 192365.2365185311,
            "unit": "ops/sec",
            "extra": "P50: 4.9us | P99: 6.9us | P99.9: 15.6us\nthreads: 1 | elapsed: 1.04s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1233004.5504094583,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 622018.6045702425,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.5us | P99.9: 3.9us\nthreads: 1 | elapsed: 0.32s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 457674.2121957102,
            "unit": "ops/sec",
            "extra": "P50: 1.9us | P99: 8.3us | P99.9: 14.4us\nthreads: 1 | elapsed: 0.44s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c2fe71e91cd440529e8bd119034fd2d4ae78364b",
          "message": "refactor(fs): thread Fs through table::Writer and BlobFile creation (#107)\n\n## Summary\n\n- Generalize `BlockIndexWriter`/`FilterWriter` traits to use generic `W`\ninstead of hardcoded `std::fs::File` in `finish()` methods\n- Make `table::Writer`, `table::MultiWriter`, `vlog::blob_file::Writer`,\n`vlog::blob_file::MultiWriter` use `Arc<dyn Fs>` / `Box<dyn FsFile>` for\npluggable filesystem backends\n- Thread `Fs` through `rewrite_atomic()`, `fsync_directory()`,\n`persist_version()`, and `upgrade_version()`\n- Replace `std::fs::create_dir_all` / `Path::try_exists` with\n`Fs::create_dir_all` / `Fs::exists` in tree creation and recovery\n- Update all call sites (flush, compaction, ingestion, recovery) to pass\n`config.fs` through\n\nThis eliminates the last direct `std::fs` dependency from the write\npath, enabling:\n- **io_uring**: batch SQE submissions for sequential writes during\ncompaction\n- **Per-level Fs**: new tables written to the appropriate device for\ntheir target level\n\n## Test plan\n\n- [x] `cargo test --lib --all-features` — 519 passed, 0 failed\n- [x] Clean build with zero warnings\n\nCloses #91",
          "timestamp": "2026-03-23T20:06:58+02:00",
          "tree_id": "e224e66c71828767b0ac608abce7a9eb681e3c0b",
          "url": "https://github.com/structured-world/lsm-tree/commit/c2fe71e91cd440529e8bd119034fd2d4ae78364b"
        },
        "date": 1774289275454,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1949928.6784336932,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1278350.776142045,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.3us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 633419.296460744,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 5.4us | P99.9: 11.7us\nthreads: 1 | elapsed: 0.32s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2417463.0081925765,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 4.0us | P99.9: 7.5us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 414434.76622302615,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.3us | P99.9: 12.2us\nthreads: 1 | elapsed: 0.48s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 197854.03536378327,
            "unit": "ops/sec",
            "extra": "P50: 4.7us | P99: 5.8us | P99.9: 15.1us\nthreads: 1 | elapsed: 1.01s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1224482.6014583479,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 703103.1919911118,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.5us | P99.9: 2.8us\nthreads: 1 | elapsed: 0.28s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 524839.9354317768,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 7.8us | P99.9: 12.7us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5e1eb1b4c488fd8755e4d785534626e2ec0cdf1b",
          "message": "fix(memtable): cursor wrap on exact block fill corrupts arena (#130)\n\n## Summary\n\n- Fix arena cursor corruption when an allocation fills a block exactly\nto `BLOCK_SIZE`\n- The bitwise OR in `(block_idx << BLOCK_SHIFT) | new_end` wraps the\ncursor back to offset 0 of the current block instead of advancing to the\nnext one, causing subsequent allocations to overwrite existing node data\n- Only manifests on i686 (4 MiB blocks, ~10 block boundaries for 1M\nentries); on x86_64 (64 MiB blocks) a single memtable rarely fills even\none block\n\n## Technical Details\n\n**Root cause:** `new_end == BLOCK_SIZE` means `new_end = 1 <<\nBLOCK_SHIFT`. The OR with `block_idx << BLOCK_SHIFT` doesn't carry — the\ncursor stays in the same block. Corrupted arena nodes produce invalid\n`ValueType` discriminants, panicking at `node_value_type()`.\n\n**Fix:** Change `new_end <= BLOCK_SIZE` to strict `<` so the exact-fill\ncase falls through to the next-block path. Any remaining bytes in the\ncurrent block (at most `BLOCK_SIZE - offset`, including the\nwould-have-fit allocation) are abandoned — acceptable waste for typical\nnode sizes.\n\nAdditionally, reject `size >= BLOCK_SIZE` upfront to prevent an infinite\nloop of block advances (since `new_end` can never be `< BLOCK_SIZE` when\n`size >= BLOCK_SIZE`).\n\n## Test Plan\n\n- [x] Regression unit test `exact_block_fill_does_not_corrupt` targeting\nblock_idx >= 1 (where the OR collision actually triggers)\n- [x] All 477 lib tests pass\n- [x] `a_lot_of_ranges` integration test passes in both debug and\nrelease\n- [x] Full test suite green\n\nCloses #119",
          "timestamp": "2026-03-23T20:24:19+02:00",
          "tree_id": "3ba58180284305181564a5a9de3a67947ed07758",
          "url": "https://github.com/structured-world/lsm-tree/commit/5e1eb1b4c488fd8755e4d785534626e2ec0cdf1b"
        },
        "date": 1774290318893,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2057881.4554918106,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1277073.2749844939,
            "unit": "ops/sec",
            "extra": "P50: 0.6us | P99: 1.5us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 597842.3945699482,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 5.4us | P99.9: 11.6us\nthreads: 1 | elapsed: 0.33s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2391055.95869556,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.3us | P99.9: 8.7us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 399459.6261971463,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.3us | P99.9: 12.4us\nthreads: 1 | elapsed: 0.50s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 195268.59164514157,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 6.7us | P99.9: 15.4us\nthreads: 1 | elapsed: 1.02s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1220796.9573735609,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 678697.9727780216,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.5us | P99.9: 4.2us\nthreads: 1 | elapsed: 0.29s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 526994.6306730458,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 7.8us | P99.9: 13.4us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3b54ecbb951165dca34a97bb3a5610dd13e71fb7",
          "message": "fix(test): enlarge bloom filter false-positive-rate sample to 100K (#135)\n\n## Summary\n- Decouple filter construction size (1K items) from FPR measurement\nsample (100K probes) in `filter_bloom_standard_bpk` test\n- Eliminates flaky CI failures caused by high statistical variance with\nsmall sample\n\n## Technical Details\nWith only 1K probe keys, measured FPR fluctuates enough (~10% ± 3%) to\noccasionally exceed the 13% assertion threshold. Increasing to 100K\nprobes reduces variance to ±0.3%, making the test stable while keeping\nthe same filter size and assertion.\n\n## Test Plan\n- [x] `cargo test --lib -- filter_bloom_standard_bpk` passes\nconsistently\n\nCloses #121",
          "timestamp": "2026-03-23T20:33:17+02:00",
          "tree_id": "2570281494d50009f6fe01b3cfcd28f28fa90e75",
          "url": "https://github.com/structured-world/lsm-tree/commit/3b54ecbb951165dca34a97bb3a5610dd13e71fb7"
        },
        "date": 1774290860323,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2041266.3216082861,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.2us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1205121.0074657367,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.6us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 614429.2119898967,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 5.5us | P99.9: 11.5us\nthreads: 1 | elapsed: 0.33s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2332242.959217968,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 4.1us | P99.9: 8.8us\nthreads: 1 | elapsed: 0.09s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 402284.5910887458,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.4us | P99.9: 12.4us\nthreads: 1 | elapsed: 0.50s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 189545.94536994142,
            "unit": "ops/sec",
            "extra": "P50: 4.9us | P99: 6.8us | P99.9: 15.2us\nthreads: 1 | elapsed: 1.06s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1146265.159478525,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.8us | P99.9: 9.2us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 703282.6858680844,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 4.4us\nthreads: 1 | elapsed: 0.28s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 525608.465712753,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 7.7us | P99.9: 15.8us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "75867395eeb9bf6c99cf176f067a853b03dc8a72",
          "message": "fix: thread UserComparator through Run, KeyRange, and Version methods (#117)\n\n## Summary\n\nExtends comparator-aware coverage (#98 core fix landed in #100) to\nremaining code paths, plus fixes #122.\n\n- **Leveled compaction `choose()`** — all overlap detection, key range\naggregation, trivial move decisions now use comparator\n- **`pick_minimal_compaction` multi-run aware (#122)** — accepts\n`&Level` instead of `&Run`, scans all runs for overlap/containment.\nEliminates missed tables in transient multi-run levels from multi-level\ncompaction (#108)\n- **`RunReader::new_cmp`** — comparator-aware table selection for range\nscans (`create_range` + `create_range_point`)\n- **`OwnedBounds::contains`** — comparator-aware containment for\n`drop_range` strategy\n- **`get_contained_cmp`** — comparator-aware table containment in runs\n- **`Level::aggregate_key_range_cmp`** + **`KeyRange::aggregate_cmp`** +\n**`KeyRange::contains_range_cmp`** — cross-run aggregation with\ncomparator\n\n## What #100 covered vs what this PR adds\n\n| Area | #100 | This PR |\n|------|------|---------|\n| `Run::push_cmp`, `get_overlapping_cmp`, `range_overlap_indexes_cmp` |\nDone | — |\n| `optimize_runs` + `Version::with_*` comparator threading | Done | — |\n| Leveled `choose()` comparator threading | — | Done |\n| `pick_minimal_compaction` multi-run aware (#122) | — | Done |\n| `RunReader::new_cmp` for range scans | — | Done |\n| `OwnedBounds::contains` with comparator | — | Done |\n| `get_contained_cmp`, `contains_range_cmp`, `aggregate_cmp` | — | Done\n|\n| `Level::aggregate_key_range_cmp` | — | Done |\n| `RunReader::new` public API preservation | — | Done |\n| `trim_slice` deduplication | — | Done |\n\n## Test Plan\n\n- [x] 4 regression tests with `ReverseComparator` (compaction, leveled,\nmerge operator, tombstone)\n- [x] Unit test for `get_contained_cmp` with reverse comparator\n- [x] All 17 custom_comparator tests pass + 17\ncustom_comparator_compaction (2 ignored — #116)\n- [x] `cargo check` + `cargo clippy --lib` clean\n\nCloses #122\n\n## Related\n\n- #116 — range bounds interpretation for reverse comparator (blocks\nrange scan tests)",
          "timestamp": "2026-03-23T21:04:44+02:00",
          "tree_id": "393a896098ac68d7fb00f3b56e133fbe7a072a15",
          "url": "https://github.com/structured-world/lsm-tree/commit/75867395eeb9bf6c99cf176f067a853b03dc8a72"
        },
        "date": 1774292748338,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1962507.5492759154,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1256481.9390270528,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.4us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 597586.1874430607,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 5.5us | P99.9: 11.6us\nthreads: 1 | elapsed: 0.33s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2337271.1756008896,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 4.2us | P99.9: 8.3us\nthreads: 1 | elapsed: 0.09s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 395624.3527227055,
            "unit": "ops/sec",
            "extra": "P50: 2.2us | P99: 6.3us | P99.9: 12.3us\nthreads: 1 | elapsed: 0.51s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 195585.34901092164,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 6.6us | P99.9: 15.1us\nthreads: 1 | elapsed: 1.02s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1184983.7274183354,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.8us | P99.9: 6.1us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 664018.7348105093,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 4.1us\nthreads: 1 | elapsed: 0.30s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 506384.7418904307,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 9.0us | P99.9: 16.0us\nthreads: 1 | elapsed: 0.39s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "cdba6eeef94adf80d3550dcd5feb0f995fa0a1de",
          "message": "fix(compaction): preserve range tombstones covering gaps between output tables (#137)\n\n## Summary\n\n- Fix RT clipping during compaction table rotation: clip to `[first_key,\nnext_table_first_key)` instead of `[first_key, upper_bound(last_key))`,\npreserving RTs that span the gap between output tables\n- Widen table `key_range` metadata to include gap-covering RTs so point\nreads consult the correct table — guarded to avoid disjoint-run overlap\nwhen `clipped.end == clip_upper`\n- Add regression tests: gap-covering RT preservation + key_range\ndisjointness when RT spans past next table\n\n## Technical Details\n\nWhen `MultiWriter` rotates during compaction, `write_rts_to_writer`\nclipped each range tombstone to the output table's KV key range\n`[first_key, upper_bound(last_key))`. If compaction produced tables\n`[a,l]` and `[q,z]`, an RT `[m,p)` fell entirely in the gap and was\ndropped by both tables — silently losing delete semantics for keys in\nlower levels.\n\nThe fix passes `self.current_key` (the first key of the **next** table)\nas the clip upper bound during rotation. This extends the\n\"responsibility range\" of the finishing table to cover the gap.\n\nThe table's `key_range.last_key` is widened to include the clipped RT's\nend **only when strictly less than `clip_upper`** — setting it to\nexactly `clip_upper` would make adjacent tables' key_ranges overlap and\nbreak `Run::get_for_key_cmp` for the boundary key.\n\n## Known Limitations\n\n- With the current compaction architecture (major_compact merges all\ntables, leveled pulls in overlapping tables recursively), the gap\nscenario is unlikely in practice. The fix is defensive for future\npartial/incremental compaction strategies.\n- When an RT spans past the next table's first key (`clipped.end ==\nclip_upper`), `last_key` is NOT widened to avoid disjoint-run overlap.\nGap keys in this edge case may not be found for RT suppression via the\nkey_range filter.\n\n## Test Plan\n\n- [x] `clip_preserves_rt_covering_gap_between_output_tables` —\nMultiWriter with forced rotation, RT in gap preserved\n- [x] `clip_rt_spanning_next_table_does_not_overlap_key_ranges` — RT\nspans past next table, key_ranges stay disjoint\n- [x] All lib tests pass (484)\n- [x] All range_tombstone integration tests pass (41)\n- [x] `cargo clippy --all-features -- -D warnings` clean\n\nCloses #32",
          "timestamp": "2026-03-23T22:00:41+02:00",
          "tree_id": "ed98314ca27b46dbc133ac318f74fa4c11029b69",
          "url": "https://github.com/structured-world/lsm-tree/commit/cdba6eeef94adf80d3550dcd5feb0f995fa0a1de"
        },
        "date": 1774296107581,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1908919.4404021748,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1315314.0395791938,
            "unit": "ops/sec",
            "extra": "P50: 0.6us | P99: 1.7us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.15s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 631627.9005253261,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 5.5us | P99.9: 11.2us\nthreads: 1 | elapsed: 0.32s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2415957.4764425424,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.1us | P99.9: 8.1us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 414327.30564444716,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.2us | P99.9: 11.9us\nthreads: 1 | elapsed: 0.48s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 196352.26578242698,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 6.8us | P99.9: 14.9us\nthreads: 1 | elapsed: 1.02s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1202874.4899225761,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 6.0us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 700498.8266154305,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 3.7us\nthreads: 1 | elapsed: 0.29s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 498609.23673511326,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 7.9us | P99.9: 14.2us\nthreads: 1 | elapsed: 0.40s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "distinct": true,
          "id": "600baee8fe452a1e183895536ae07c92d9b72030",
          "message": "chore: enable crates.io publish + fix CHANGELOG URL + benchmark series name\n\n- .release-plz.toml: remove publish = false (enable crates.io publishing)\n- CHANGELOG.md: update fork URL to coordinode-lsm-tree\n- benchmark.yml: keep name \"lsm-tree db_bench\" to preserve gh-pages time series",
          "timestamp": "2026-03-23T23:13:40+02:00",
          "tree_id": "2e9206075a17b3610cd2f5236315c618a293b6af",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/600baee8fe452a1e183895536ae07c92d9b72030"
        },
        "date": 1774300534417,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2001710.4615894281,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1318608.4851242898,
            "unit": "ops/sec",
            "extra": "P50: 0.6us | P99: 1.3us | P99.9: 4.9us\nthreads: 1 | elapsed: 0.15s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 575328.627208449,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 5.5us | P99.9: 11.6us\nthreads: 1 | elapsed: 0.35s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2349829.973940033,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 4.3us | P99.9: 8.6us\nthreads: 1 | elapsed: 0.09s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 384584.4344434373,
            "unit": "ops/sec",
            "extra": "P50: 2.2us | P99: 6.4us | P99.9: 12.8us\nthreads: 1 | elapsed: 0.52s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 195889.0072786547,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 6.8us | P99.9: 15.2us\nthreads: 1 | elapsed: 1.02s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1230549.337282117,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.6us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 709567.4027601825,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.6us | P99.9: 3.4us\nthreads: 1 | elapsed: 0.28s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 528414.6399083164,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 7.7us | P99.9: 13.7us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8c09303c5316dc33e8c1d613947b121497365bfc",
          "message": "fix: thread UserComparator through ingestion guards and range overlap (#139)\n\n## Summary\n\n- Replace lexicographic `key > *prev` in ingestion write guards with\ncomparator-aware ordering via `config.comparator.compare()`\n- Assertion messages updated to say \"ordered after ... by configured\ncomparator\"\n- Add `KeyRange::overlaps_with_bounds_cmp()` for comparator-aware bounds\noverlap detection\n- Replace `check_key_range_overlap` with `check_key_range_overlap_cmp`\nin all range scan paths (`create_range` + `create_range_point`)\n- Un-ignore reverse comparator range scan tests (now passing)\n\n## Files changed\n\n| File | Change |\n|------|--------|\n| `src/tree/ingest.rs` | 4 write guards → comparator-aware ordering +\nupdated messages |\n| `src/blob_tree/ingest.rs` | 3 write guards → comparator-aware ordering\n+ updated messages |\n| `src/key_range.rs` | Add `overlaps_with_bounds_cmp()` + unit tests |\n| `src/table/mod.rs` | Replace `check_key_range_overlap` with\n`check_key_range_overlap_cmp` |\n| `src/range.rs` | Use `check_key_range_overlap_cmp` at all 5 call sites\n|\n| `tests/custom_comparator_compaction.rs` | Un-ignore 2 range scan\ntests, add 2 ingestion guard tests |\n| `tests/ingestion_api.rs` | Update `should_panic` expected message |\n\n## Test plan\n\n- [x] All 4 previously-failing tests now pass (2 range scan + 2\ningestion)\n- [x] 8 new unit tests for `overlaps_with_bounds_cmp` with reverse\ncomparator\n- [x] 488+ unit tests pass\n- [x] All integration tests pass (including prop tests)\n- [x] No regressions in default (lexicographic) comparator paths\n\nCloses #116",
          "timestamp": "2026-03-24T00:50:23+02:00",
          "tree_id": "9cfd2c5f9626858c7b490a93115b83c0c2a51dfb",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/8c09303c5316dc33e8c1d613947b121497365bfc"
        },
        "date": 1774306282582,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2004860.7849731673,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1314487.8798926661,
            "unit": "ops/sec",
            "extra": "P50: 0.6us | P99: 1.3us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.15s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 627865.6514300929,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 5.4us | P99.9: 11.7us\nthreads: 1 | elapsed: 0.32s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2412632.1074407804,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.2us | P99.9: 8.1us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 397108.30339921016,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.3us | P99.9: 13.1us\nthreads: 1 | elapsed: 0.50s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 193535.49114026068,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 6.6us | P99.9: 15.4us\nthreads: 1 | elapsed: 1.03s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1209573.6301336724,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 721648.2506663674,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 4.3us\nthreads: 1 | elapsed: 0.28s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 530406.7891220357,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 7.7us | P99.9: 13.0us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ef6f4b34806f515e6b51408dc86b886a26423728",
          "message": "feat(compression): zstd dictionary compression support (#131)\n\n## Summary\n- Add `CompressionType::ZstdDict { level, dict_id }` variant for zstd\ndictionary-based block compression\n- Add `ZstdDictionary` struct (raw bytes + xxh3-based dict_id\nfingerprint)\n- Thread dictionary through Config → flush/compaction/ingestion/recovery\n→ Block write/read\n- Add `Error::ZstdDictMismatch { expected: u32, got: Option<u32> }` for\ndict_id validation\n\n## Technical Details\n- On-disk format: tag 4 (1B tag + 1B level + 4B dict_id = 6 bytes),\nbackward compatible — old readers get `InvalidTag`\n- Dictionary parameter uses `#[cfg(feature = \"zstd\")]` gating to avoid\nany overhead when the feature is disabled\n- Compression uses `zstd::bulk::Compressor::with_dictionary()`,\ndecompression uses `zstd::bulk::Decompressor::with_dictionary()`\n- **Config::open() validation (fail-fast):**\n- All `ZstdDict` entries in data block compression policies must match\nthe provided dictionary's `dict_id`\n- `KvSeparationOptions::compression` set to `ZstdDict` is rejected\n(`ErrorKind::Unsupported`)\n- `Table::recover()` validates the persisted `data_block_compression`\ndict_id against the provided dictionary\n- `Writer::use_index_block_compression()` silently downgrades `ZstdDict`\nto plain `Zstd` — dictionaries are trained on data block content, not\nindex/filter structures\n- Blob files return `ErrorKind::Unsupported` for `ZstdDict` at both\nconfig and runtime levels\n\n## Known Limitations\n- Blob file (KV-separated large values) dictionary compression not yet\nsupported\n- No built-in dictionary training API — users provide pre-trained\ndictionaries\n- Compressor/decompressor contexts created per-call (pre-built context\ncaching is future optimization)\n\n## Test Plan\n- [x] Unit tests: serialization roundtrip, level validation, dict_id\ncomputation, mismatch detection\n- [x] Block-level roundtrip: from_reader, from_file, large data,\nencrypted+dict (both branches)\n- [x] Block error paths: missing dict, wrong dict, write-side missing\ndict\n- [x] Integration: full tree write→flush→read, range scan with value\nverification, per-level policy (ZstdDict at L0)\n- [x] Validation: config open with mismatch, config open with missing\ndict, reopen with wrong dict fails at recovery\n- [x] Blob writer: ZstdDict returns ErrorKind::Unsupported\n- [x] Full test suite passes with `--all-features` (800+ tests, 0\nfailures)\n- [x] Compiles clean with `--no-default-features`, `--features lz4`,\n`--features zstd`, `--all-features`\n\nCloses #129",
          "timestamp": "2026-03-24T01:24:55+02:00",
          "tree_id": "a76137c1b5b572db78b160a1453f67916c7f872d",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/ef6f4b34806f515e6b51408dc86b886a26423728"
        },
        "date": 1774308372811,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1955001.2666453207,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 1.8us | P99.9: 3.8us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1047847.9490055575,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 1.8us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.19s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 590435.1687276249,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 4.5us | P99.9: 9.9us\nthreads: 1 | elapsed: 0.34s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 3025614.107760847,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 3.3us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.07s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 395848.281471594,
            "unit": "ops/sec",
            "extra": "P50: 2.2us | P99: 5.3us | P99.9: 9.7us\nthreads: 1 | elapsed: 0.51s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 220566.70709150782,
            "unit": "ops/sec",
            "extra": "P50: 4.2us | P99: 5.1us | P99.9: 11.0us\nthreads: 1 | elapsed: 0.91s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1110788.9514599391,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 2.3us | P99.9: 5.5us\nthreads: 1 | elapsed: 0.18s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 785891.379319728,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 1.6us | P99.9: 4.2us\nthreads: 1 | elapsed: 0.25s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 508756.2845423736,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 5.7us | P99.9: 10.2us\nthreads: 1 | elapsed: 0.39s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "distinct": true,
          "id": "8c4a2425a325c07c1b08178d2547b43b9afcfa9b",
          "message": "refactor: add #[non_exhaustive] to CompressionType enum\n\nPrevents cargo-semver-checks from triggering major version bump\nwhen new compression variants are added (e.g. ZstdDict).",
          "timestamp": "2026-03-24T01:57:38+02:00",
          "tree_id": "352ac12d0ea102e200d5a865726d04e72e1ed2df",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/8c4a2425a325c07c1b08178d2547b43b9afcfa9b"
        },
        "date": 1774310322069,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1947328.5597494685,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1252820.3099310822,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.3us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 598380.1197922892,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 5.5us | P99.9: 11.6us\nthreads: 1 | elapsed: 0.33s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2407892.5710061034,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.2us | P99.9: 8.2us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 403705.5551571879,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.4us | P99.9: 12.4us\nthreads: 1 | elapsed: 0.50s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 191149.60123693902,
            "unit": "ops/sec",
            "extra": "P50: 4.9us | P99: 6.8us | P99.9: 15.5us\nthreads: 1 | elapsed: 1.05s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1221323.1790895793,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 678447.5004433654,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 3.7us\nthreads: 1 | elapsed: 0.29s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 520058.0419098603,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 8.9us | P99.9: 15.8us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "distinct": true,
          "id": "471fffd7ba80cacd4cd69a413588941b4ebbeee8",
          "message": "ci: disable cargo-semver-checks in release-plz\n\nFork controls versioning manually — semver-checks was triggering\nv5.0.0 bumps for intentional API extensions (new enum variants,\n#[non_exhaustive]).",
          "timestamp": "2026-03-24T02:02:36+02:00",
          "tree_id": "4f78d20bf5f8a95c132f4eaf6a33013daebf3f0b",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/471fffd7ba80cacd4cd69a413588941b4ebbeee8"
        },
        "date": 1774310653521,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2027716.1628058986,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.4us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1299279.7988132793,
            "unit": "ops/sec",
            "extra": "P50: 0.6us | P99: 1.2us | P99.9: 4.9us\nthreads: 1 | elapsed: 0.15s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 624353.0044436295,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 5.4us | P99.9: 11.2us\nthreads: 1 | elapsed: 0.32s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2407070.23927241,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.2us | P99.9: 8.1us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 416320.2526557688,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.2us | P99.9: 12.0us\nthreads: 1 | elapsed: 0.48s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 193751.90353374052,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 6.3us | P99.9: 15.1us\nthreads: 1 | elapsed: 1.03s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1238960.6668705791,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 618694.7886051071,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.7us | P99.9: 4.2us\nthreads: 1 | elapsed: 0.32s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 526228.9063116263,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 7.8us | P99.9: 13.5us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "distinct": true,
          "id": "0307b5935fe45651b85a53bd9ec4d809dbd8ce1b",
          "message": "chore: expand changelog skip rules for release-plz\n\nSkip chore, ci, style, build, Merge commits from changelog.\nOnly feat/fix/perf/refactor/test/docs appear in release notes.",
          "timestamp": "2026-03-24T02:13:19+02:00",
          "tree_id": "528efb6eeed4e224c7a742585780b10a56e06cb0",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/0307b5935fe45651b85a53bd9ec4d809dbd8ce1b"
        },
        "date": 1774311340540,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1972890.355789863,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1302485.1958067347,
            "unit": "ops/sec",
            "extra": "P50: 0.6us | P99: 1.4us | P99.9: 5.1us\nthreads: 1 | elapsed: 0.15s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 622230.0244336353,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 5.4us | P99.9: 11.3us\nthreads: 1 | elapsed: 0.32s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2389767.464991191,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.2us | P99.9: 8.7us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 415964.1058915914,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.2us | P99.9: 12.2us\nthreads: 1 | elapsed: 0.48s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 194143.4883051404,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 6.6us | P99.9: 14.8us\nthreads: 1 | elapsed: 1.03s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1228382.6080897925,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 738100.8546912657,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 3.6us\nthreads: 1 | elapsed: 0.27s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 532982.0157504021,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 7.7us | P99.9: 13.2us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "distinct": true,
          "id": "3d10df62b95419caa4b401c4f1b6938cce0c7d7b",
          "message": "docs: add v4.0.0 fork epoch changelog (all changes since upstream v3.1.1)\n\nFull changelog for the fork's first release: 28 features, 100+ fixes,\n12 perf improvements, 38 refactors, 43 test suites.",
          "timestamp": "2026-03-24T02:22:19+02:00",
          "tree_id": "a21ba97a49c45809acb83aea4c340085bb667b28",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/3d10df62b95419caa4b401c4f1b6938cce0c7d7b"
        },
        "date": 1774311814933,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2010870.9897664867,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1150042.1593955213,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.7us | P99.9: 6.0us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 567664.6885520174,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 5.6us | P99.9: 11.5us\nthreads: 1 | elapsed: 0.35s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2354393.5603098217,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 4.3us | P99.9: 8.3us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 372118.22760222154,
            "unit": "ops/sec",
            "extra": "P50: 2.3us | P99: 6.5us | P99.9: 12.8us\nthreads: 1 | elapsed: 0.54s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 194576.85242509507,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 6.8us | P99.9: 15.0us\nthreads: 1 | elapsed: 1.03s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1177808.9047806456,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 6.2us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 710731.8317540825,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 4.7us\nthreads: 1 | elapsed: 0.28s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 453364.87695025525,
            "unit": "ops/sec",
            "extra": "P50: 1.9us | P99: 7.9us | P99.9: 14.6us\nthreads: 1 | elapsed: 0.44s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "19a4ebbff1917fa6e6b107d2342670e741dbd9f7",
          "message": "perf(compaction): merge input ranges before L2 overlap query (#146)\n\n## Summary\n\n- Add `KeyRange::merge_sorted_cmp()` to coalesce sorted key ranges into\ndisjoint intervals using a custom comparator\n- Replace per-table L2 overlap queries in multi-level compaction with\nmerged-interval queries, reducing redundant binary searches when L0\ntables overlap\n- Parts 1 and 3 of #122 were already completed in #117; this PR\nimplements Part 2 (merge input ranges optimization)\n\n## Technical Details\n\nPreviously, multi-level compaction queried L2 once per input table —\nO(L2_runs × input_tables × log L2_run_size). With overlapping L0 tables,\nmany queries hit the same L2 regions redundantly.\n\nNow, input key ranges from L0+L1 are sorted and merged into disjoint\nintervals first, then L2 is queried with the (typically much smaller)\nset of merged intervals.\n\n## Test Plan\n\n- 8 unit tests for `merge_sorted_cmp` (empty, single, disjoint,\noverlapping, adjacent, contained, mixed, reverse comparator)\n- All 21 existing leveled compaction tests pass (including multi-level\ndata integrity tests)\n- Full suite: 490 lib + 33 doc tests pass, zero clippy warnings\n\nCloses #122",
          "timestamp": "2026-03-24T03:03:09+02:00",
          "tree_id": "5f6da4558b268559a66cb74fa60b662cfe4e3d63",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/19a4ebbff1917fa6e6b107d2342670e741dbd9f7"
        },
        "date": 1774314247659,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2025642.8765072797,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1297364.8444226026,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.3us | P99.9: 4.8us\nthreads: 1 | elapsed: 0.15s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 605814.2984890486,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 5.6us | P99.9: 11.1us\nthreads: 1 | elapsed: 0.33s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2370357.5879975995,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 4.2us | P99.9: 7.4us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 403972.86059421947,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.2us | P99.9: 11.9us\nthreads: 1 | elapsed: 0.50s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 183162.04272471875,
            "unit": "ops/sec",
            "extra": "P50: 5.1us | P99: 7.5us | P99.9: 15.2us\nthreads: 1 | elapsed: 1.09s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1218391.6240010795,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 7.3us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 675854.192514147,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.5us | P99.9: 4.1us\nthreads: 1 | elapsed: 0.30s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 520530.1272453073,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 7.8us | P99.9: 12.8us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "distinct": true,
          "id": "18e6cb5a1136e828984fc80dfbb5c58863c2a4c6",
          "message": "chore: switch to Apache-2.0 license + fix post-rename references\n\n- License: MIT OR Apache-2.0 → Apache-2.0 (patent grant protection)\n- Remove LICENSE-MIT, add copyright appendix to LICENSE-APACHE\n- src/lib.rs: doc logo/favicon URLs → coordinode-lsm-tree repo\n- CONTRIBUTING.md: issues link → coordinode-lsm-tree\n- FUNDING.yml: fjall-rs → structured-world\n- Cargo.toml: update license + include fields",
          "timestamp": "2026-03-24T03:51:54+02:00",
          "tree_id": "b2b4dac1ba95ae4c992ebe9ea1a3798590e8e352",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/18e6cb5a1136e828984fc80dfbb5c58863c2a4c6"
        },
        "date": 1774317175200,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1831024.7254804138,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.8us | P99.9: 6.1us\nthreads: 1 | elapsed: 0.11s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1180392.7695560274,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.8us | P99.9: 6.8us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 547993.9604270436,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 6.6us | P99.9: 13.4us\nthreads: 1 | elapsed: 0.36s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2172430.5249386174,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 5.1us | P99.9: 9.0us\nthreads: 1 | elapsed: 0.09s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 373457.67651487817,
            "unit": "ops/sec",
            "extra": "P50: 2.3us | P99: 7.2us | P99.9: 14.8us\nthreads: 1 | elapsed: 0.54s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 184424.68778583236,
            "unit": "ops/sec",
            "extra": "P50: 5.1us | P99: 6.7us | P99.9: 17.1us\nthreads: 1 | elapsed: 1.08s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1100695.5410697053,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 3.3us | P99.9: 7.5us\nthreads: 1 | elapsed: 0.18s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 724749.5630602656,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.5us | P99.9: 5.1us\nthreads: 1 | elapsed: 0.28s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 480323.90469596564,
            "unit": "ops/sec",
            "extra": "P50: 1.8us | P99: 10.7us | P99.9: 16.9us\nthreads: 1 | elapsed: 0.42s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1e2c02f60092d0041a3c6d606e7b6ac9bf20956e",
          "message": "perf(merge): replace IntervalHeap with sorted-vec heap + replace_min/replace_max (#148)\n\n## Summary\n\n- Replace `IntervalHeap` with a custom `MergeHeap` backed by a sorted\nvector supporting both min and max extraction on a single data structure\n- Add `replace_min`/`replace_max` — replaces the extremum in-place and\nslides into sorted position. Common case (same source keeps winning in\nsequential scans) completes in **1 comparison** vs 2×O(log n) for the\nold pop+push pattern\n- Store comparator once in the heap instead of cloning the `Arc` into\nevery `HeapItem`, eliminating per-item atomic ref-count traffic\n- Add source-index tiebreaker to entry comparison for deterministic MVCC\nordering when key+seqno tie\n\n## Technical Details\n\nThe sorted-vector approach is competitive with a binary heap for the\ntypical merge fan-in (n=2–30) due to cache-friendly sequential layout\nand negligible `memmove` cost. A single heap (not two separate min/max\nheaps) preserves `DoubleEndedIterator` mixed forward/reverse correctness\nrequired by prefix ping-pong iteration.\n\nDuring implementation, discovered that the original `IntervalHeap`'s\npop+push pattern implicitly preserved source ordering for equal entries.\nThe new replace-in-place pattern broke this, causing MVCC bugs when\nkey+seqno tie across levels. Fixed by adding source index as a\ncomparison tiebreaker — an improvement over the original's accidental\nstability.\n\n## Test Plan\n\n- [x] All 496 existing tests pass (0 failures)\n- [x] Clippy clean (`-D warnings`)\n- [x] New unit tests: heap ordering (min/max), replace_min/replace_max\n(stays/slides), seqno tiebreak, source-index tiebreak, mixed min/max,\nempty/single element\n- [x] New merge tests: interleaved, many sources, seqno ordering\n- [x] Verified mixed forward/reverse iteration (`tree_disjoint_prefix`\nping-pong test)\n- [x] Verified compaction filter correctness with overlapping seqnos\n\nCloses #142",
          "timestamp": "2026-03-24T03:53:11+02:00",
          "tree_id": "beb255829461f3b12ab951f487fa1c025f3f3021",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/1e2c02f60092d0041a3c6d606e7b6ac9bf20956e"
        },
        "date": 1774317262390,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1977935.6139629832,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.4us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1273441.8356714998,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.6us | P99.9: 5.6us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 543008.0542592761,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 5.7us | P99.9: 16.2us\nthreads: 1 | elapsed: 0.37s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2549054.872267944,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.2us | P99.9: 7.4us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 367074.90578063566,
            "unit": "ops/sec",
            "extra": "P50: 2.4us | P99: 6.4us | P99.9: 13.7us\nthreads: 1 | elapsed: 0.54s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 204024.67312809272,
            "unit": "ops/sec",
            "extra": "P50: 4.6us | P99: 6.1us | P99.9: 15.5us\nthreads: 1 | elapsed: 0.98s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1147015.1119642456,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.9us | P99.9: 6.4us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 689579.0737962114,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.1us | P99.9: 4.3us\nthreads: 1 | elapsed: 0.29s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 462709.81035558484,
            "unit": "ops/sec",
            "extra": "P50: 1.9us | P99: 8.0us | P99.9: 13.9us\nthreads: 1 | elapsed: 0.43s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "2feddbe8a188bb5d990f90612b2082e8c0fb6a2f",
          "message": "chore: rename crate to coordinode-lsm-tree (#147)\n\n## Summary\n\n- Rename crate `lsm-tree` → `coordinode-lsm-tree` for crates.io\npublication\n- Bump version to `4.0.0` (fork epoch)\n- Keep `[lib] name = \"lsm_tree\"` — all downstream code (`use\nlsm_tree::`) works unchanged via `package` alias\n\n## Changes\n\n- `Cargo.toml`: name, version, repository, homepage, keywords\n- `tools/db_bench/Cargo.toml`: use `package = \"coordinode-lsm-tree\"`\nalias\n- `README.md`: badge URLs → coordinode-lsm-tree\n- `.github/workflows/benchmark.yml`: dashboard name\n- `.github/copilot-instructions.md`: project name\n\n## What stays the same\n\n- `[lib] name = \"lsm_tree\"` — Rust lib name unchanged\n- All `use lsm_tree::` in source code — zero changes needed\n- Consumers use: `lsm-tree = { package = \"coordinode-lsm-tree\", ... }`\n- `cargo publish --dry-run` passes\n\n## Test plan\n\n- [x] `cargo check` passes\n- [x] `cargo check --manifest-path tools/db_bench/Cargo.toml` passes\n- [x] `cargo test --lib` — 482 passed, 0 failed\n- [x] `cargo publish --dry-run --allow-dirty` — uploads\n`coordinode-lsm-tree v4.0.0`\n\nCloses #125 (Phases 1-2)",
          "timestamp": "2026-03-23T23:06:57+02:00",
          "tree_id": "7016ea1a4b98c0dd5da0a32f49c6e4b076315eb1",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/2feddbe8a188bb5d990f90612b2082e8c0fb6a2f"
        },
        "date": 1774300090291,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1899794.3947516282,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 1.8us | P99.9: 3.9us\nthreads: 1 | elapsed: 0.11s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 994035.1973648684,
            "unit": "ops/sec",
            "extra": "P50: 0.9us | P99: 2.0us | P99.9: 6.6us\nthreads: 1 | elapsed: 0.20s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 582614.0504072491,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 4.6us | P99.9: 9.9us\nthreads: 1 | elapsed: 0.34s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 3064834.61478155,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 3.3us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.07s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 393328.9444378617,
            "unit": "ops/sec",
            "extra": "P50: 2.2us | P99: 5.3us | P99.9: 9.9us\nthreads: 1 | elapsed: 0.51s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 223393.29804423658,
            "unit": "ops/sec",
            "extra": "P50: 4.2us | P99: 5.1us | P99.9: 10.8us\nthreads: 1 | elapsed: 0.90s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1080898.660443371,
            "unit": "ops/sec",
            "extra": "P50: 0.8us | P99: 2.4us | P99.9: 5.4us\nthreads: 1 | elapsed: 0.19s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 788208.5265560852,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 0.6us | P99.9: 4.0us\nthreads: 1 | elapsed: 0.25s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 502978.1664169355,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 5.9us | P99.9: 10.8us\nthreads: 1 | elapsed: 0.40s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "distinct": true,
          "id": "eb41b1493d9e2a9e2a2b664fca39d05b5908495a",
          "message": "style: format doc attribute URLs for rustfmt compliance",
          "timestamp": "2026-03-24T04:50:36+02:00",
          "tree_id": "85dc3f2eb703f61deb7adef54dda5f6284e2e772",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/eb41b1493d9e2a9e2a2b664fca39d05b5908495a"
        },
        "date": 1774320706637,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1858331.924098291,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.8us | P99.9: 6.2us\nthreads: 1 | elapsed: 0.11s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1154171.5725210262,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.0us | P99.9: 7.3us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 526898.1714091168,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 6.6us | P99.9: 13.5us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2272861.3715481944,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 5.1us | P99.9: 9.8us\nthreads: 1 | elapsed: 0.09s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 294087.87931068684,
            "unit": "ops/sec",
            "extra": "P50: 2.7us | P99: 12.5us | P99.9: 49.6us\nthreads: 1 | elapsed: 0.68s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 193383.24360823008,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 6.8us | P99.9: 17.2us\nthreads: 1 | elapsed: 1.03s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1072636.8719643406,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 3.3us | P99.9: 7.4us\nthreads: 1 | elapsed: 0.19s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 671395.4638152438,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.5us | P99.9: 4.6us\nthreads: 1 | elapsed: 0.30s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 469996.632286131,
            "unit": "ops/sec",
            "extra": "P50: 1.8us | P99: 9.7us | P99.9: 16.4us\nthreads: 1 | elapsed: 0.43s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "863d14341ed4ae5f8bb8b0684cdbe4fc09c3962a",
          "message": "feat(fs): io_uring Fs implementation for high-throughput I/O (#106)\n\n## Summary\n\n- `IoUringFs` implementing the object-safe `dyn Fs` trait with dedicated\nI/O thread and opportunistic SQE batching\n- `IoUringFile` implementing `dyn FsFile` — routes read/write/fsync\nthrough the ring; cold-path ops (mkdir, stat, rename) delegate to\n`std::fs`\n- `read_at` provides fill-or-EOF semantics with internal EINTR retry,\nmatching the `FsFile` trait contract\n- Runtime probe `is_io_uring_available()` for graceful fallback\n- Feature-gated: `io-uring = [\"dep:io-uring\"]`, Linux-only target\ndependency\n- 21 tests including concurrent `read_at` from 10 threads and edge-case\ncoverage\n\n## Design Decisions\n\n- **No libc dependency for errno constants** — values like `EINTR (4)`\nand `EIO (5)` are inlined with comments, consistent with `StdFs` which\nuses raw FFI for `flock` without importing libc\n- **Oversized buffers rejected with `InvalidInput`** — SQE length is\n`u32` but CQE result is `i32`, so buffers exceeding `i32::MAX` are\nrejected via `i32::try_from(buf.len())?.unsigned_abs()`. In practice LSM\nblock I/O is 4-64 KB\n- **Fatal ring error aborts the process** — if `submit_and_wait` fails\n(non-EINTR), previously submitted SQEs may still reference caller\nbuffers. `std::process::abort()` is the only sound option\n- **Ring thread panic aborts via `catch_unwind`** — if `event_loop`\npanics after submitting SQEs, those SQEs still reference caller buffers.\n`pending` map is wrapped in `ManuallyDrop` so SyncSenders survive stack\nunwinding, keeping callers blocked. `catch_unwind` + `abort` then kills\nthe process before any buffer can be freed\n- **Append mode uses `is_append` flag** — writes always query\n`file.metadata()?.len()` for the current EOF, ignoring the seek cursor.\nThis matches O_APPEND semantics since io_uring uses explicit offsets\n- **SQ full uses backpressure, not error** — when the submission queue\nis full, `enqueue` calls `submit_and_wait(1)` to drain a completion and\nretries the push. Since the Fs API is synchronous, callers are already\nblocking; backpressure is natural\n- **`AtomicU64` for cursor** — could be plain `u64` (already `Sync`),\nkept for interior-mutability pattern consistency and potential future\nshared cursor access\n- **Mutex on send_and_wait hot path** — guards `Option<SyncSender>` for\nclean shutdown. Lock held only for `send()` duration (~ns), negligible\nvs I/O latency (~µs) Submission channel is bounded to ring capacity\n(sync_channel) for natural backpressure\n- **FxHash for pending map** — uses `crate::HashMap` (FxBuildHasher) for\nreduced hashing overhead on the I/O thread hot path\n- **Seek positions may exceed `i64::MAX`** — matches\n`std::fs::File::seek` behavior; kernel rejects out-of-range offsets at\nthe actual I/O syscall\n- **Ring-thread error paths excluded from coverage** — `event_loop`,\n`enqueue`, and `Drop` contain error recovery (EINTR, SQ overflow, fatal\nring failure, mutex poisoning) that requires kernel fault injection to\nexercise\n\n## Test Plan\n\n- [x] `cargo check` — clean build without `io-uring` feature\n(macOS/Windows)\n- [x] `cargo test --lib` — all existing tests pass (no regressions)\n- [x] `cargo test --lib --features io-uring` — 21 io_uring tests\n(requires Linux 5.6+)\n- [x] Edge cases: empty buffers, seek overflow/underflow, sync_directory\nvalidation, Debug impl\n- [ ] Benchmark: compaction throughput StdFs vs IoUringFs on NVMe\n\nCloses #77",
          "timestamp": "2026-03-24T04:55:49+02:00",
          "tree_id": "6c104e08fd3ef2eb5962674f65f4bdc5dae7483d",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/863d14341ed4ae5f8bb8b0684cdbe4fc09c3962a"
        },
        "date": 1774321015763,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1954712.019999206,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1270048.4772423569,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.7us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 627668.8774890621,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 5.4us | P99.9: 11.3us\nthreads: 1 | elapsed: 0.32s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2510193.1097834837,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.2us | P99.9: 8.2us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 412113.073062855,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.2us | P99.9: 12.5us\nthreads: 1 | elapsed: 0.49s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 204120.06387880247,
            "unit": "ops/sec",
            "extra": "P50: 4.6us | P99: 6.3us | P99.9: 14.9us\nthreads: 1 | elapsed: 0.98s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1205322.477459837,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 682782.9922047656,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 3.2us\nthreads: 1 | elapsed: 0.29s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 519882.7183058823,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 7.8us | P99.9: 13.3us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "62ae0b5ab8445587fd64a35591cd85aa9ae3d8d8",
          "message": "docs: add benchmark dashboard link and update badges (#151)\n\n## Summary\n\n- Add codecov, benchmarks dashboard, deps.rs, and license badges; remove\nUpstream CI badge\n- Expand benchmarks section with link to CI dashboard and regression\nthresholds\n- Reframe project identity as independent derivative work (remove\nupstream contribution claims)\n- Update license references to Apache-2.0 in README and CONTRIBUTING.md\n\n## Test plan\n\n- [ ] Verify badge URLs resolve correctly\n- [ ] Verify benchmark dashboard link works\n\nCloses #124\n\n---------\n\nCo-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>",
          "timestamp": "2026-03-24T05:34:45+02:00",
          "tree_id": "74896e85e6692e8c2c4e6cfe5c4ef38410c08656",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/62ae0b5ab8445587fd64a35591cd85aa9ae3d8d8"
        },
        "date": 1774323377929,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1969189.509560356,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1258805.248301475,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.5us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 625214.5286889519,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 5.4us | P99.9: 11.1us\nthreads: 1 | elapsed: 0.32s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2486690.7956905747,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.3us | P99.9: 8.3us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 402016.7354461354,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.2us | P99.9: 12.5us\nthreads: 1 | elapsed: 0.50s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 202712.8008134816,
            "unit": "ops/sec",
            "extra": "P50: 4.6us | P99: 7.0us | P99.9: 14.9us\nthreads: 1 | elapsed: 0.99s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1191785.3027856592,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.8us | P99.9: 6.1us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 758589.0141169319,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 3.6us\nthreads: 1 | elapsed: 0.26s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 516507.2501903193,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 7.9us | P99.9: 13.3us\nthreads: 1 | elapsed: 0.39s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "75a85541e49377562357d7c47f464c6a188d13c5",
          "message": "fix(version): fsync version file before rewriting CURRENT pointer (#152)\n\n## Summary\n\n- Flush `BufWriter` and fsync the version file (`v{id}`) before\natomically rewriting the `CURRENT` pointer in `persist_version`\n- Prevents recovery from following `CURRENT` to a truncated or missing\nversion file after power loss\n\n## Technical Details\n\n`persist_version` writes the version file content via\n`ChecksummedWriter<BufWriter<FsFile>>`, then calls `rewrite_atomic` to\nupdate `CURRENT`. Previously, neither the `BufWriter` was flushed nor\nthe underlying file was fsynced before publishing the pointer.\n\nNow the sequence is: write → flush `BufWriter` → `FsFile::sync_all()` →\nfsync directory → rewrite `CURRENT`.\n\n## Test Plan\n\n- All existing tests pass (517 unit + integration + doc-tests)\n- No public API changes\n\nCloses #123",
          "timestamp": "2026-03-24T05:36:27+02:00",
          "tree_id": "0f709fe4a2f2ac47d0aae88ae7b13b05e2ce0734",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/75a85541e49377562357d7c47f464c6a188d13c5"
        },
        "date": 1774323470781,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1977177.284431623,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.3us | P99.9: 5.4us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1301478.5440492278,
            "unit": "ops/sec",
            "extra": "P50: 0.6us | P99: 1.5us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.15s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 618792.5879551088,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 5.5us | P99.9: 11.4us\nthreads: 1 | elapsed: 0.32s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2529190.302108114,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.1us | P99.9: 7.9us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 414072.1269505059,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.3us | P99.9: 12.1us\nthreads: 1 | elapsed: 0.48s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 206199.98676670346,
            "unit": "ops/sec",
            "extra": "P50: 4.5us | P99: 6.2us | P99.9: 14.8us\nthreads: 1 | elapsed: 0.97s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1141317.700933047,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.8us | P99.9: 22.8us\nthreads: 1 | elapsed: 0.18s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 679825.6021628353,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.6us | P99.9: 4.4us\nthreads: 1 | elapsed: 0.29s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 537122.139912223,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 7.8us | P99.9: 13.1us\nthreads: 1 | elapsed: 0.37s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "distinct": true,
          "id": "e45fcc938ec1d372b8c7313513b4ed27759a4ca5",
          "message": "ci: add dependabot auto-merge for minor/patch updates",
          "timestamp": "2026-03-24T05:46:49+02:00",
          "tree_id": "ae271225c90a4255868be1f078fe51a96a1e178d",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/e45fcc938ec1d372b8c7313513b4ed27759a4ca5"
        },
        "date": 1774324080542,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2007525.5709573363,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1298938.5574200107,
            "unit": "ops/sec",
            "extra": "P50: 0.6us | P99: 1.5us | P99.9: 5.1us\nthreads: 1 | elapsed: 0.15s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 647640.7977408787,
            "unit": "ops/sec",
            "extra": "P50: 1.3us | P99: 5.4us | P99.9: 11.1us\nthreads: 1 | elapsed: 0.31s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2342430.6625298094,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.4us | P99.9: 9.4us\nthreads: 1 | elapsed: 0.09s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 418949.7025221453,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.1us | P99.9: 12.0us\nthreads: 1 | elapsed: 0.48s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 199979.3703281363,
            "unit": "ops/sec",
            "extra": "P50: 4.7us | P99: 6.7us | P99.9: 14.7us\nthreads: 1 | elapsed: 1.00s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1217297.5375062914,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 6.0us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 687690.152344412,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 4.5us\nthreads: 1 | elapsed: 0.29s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 548624.2188866674,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 7.7us | P99.9: 13.4us\nthreads: 1 | elapsed: 0.36s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3066d12f597cc4e818f41d15073f626fd4cf21c4",
          "message": "refactor(version): comparator API cleanup — TransformContext + rename Run::push() (#153)\n\n## Summary\n\n- Introduce `TransformContext` struct bundling the comparator reference\nthreaded through `Version` transformation methods (`with_new_l0_run`,\n`with_merge`, `with_moved`, `with_dropped`)\n- Rename `Run::push()` → `Run::push_lexicographic()` to make the\nbyte-ordering precondition explicit at call sites\n\n## Technical Details\n\n`TransformContext<'a>` currently holds `&'a dyn UserComparator`. All\nfour `Version` mutators now accept `&TransformContext` instead of a bare\n`&dyn UserComparator`, giving a single extension point for future\ncontext parameters without further signature churn.\n\n`Run::push()` was renamed because the old name gave no indication that\nit assumes lexicographic key ordering — `push_cmp` exists for custom\ncomparators, and the naming asymmetry was misleading.\n\n## Test Plan\n\n- [x] `cargo test --workspace` — all tests pass\n- [x] `cargo clippy --workspace` — clean\n- [x] `cargo build` — clean\n\nCloses #113",
          "timestamp": "2026-03-24T06:28:43+02:00",
          "tree_id": "ac79a3190f4b86f1b863e6f6cfa2e14fba6bd996",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/3066d12f597cc4e818f41d15073f626fd4cf21c4"
        },
        "date": 1774326598345,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1994508.5395830513,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1315595.4752830067,
            "unit": "ops/sec",
            "extra": "P50: 0.6us | P99: 1.3us | P99.9: 4.8us\nthreads: 1 | elapsed: 0.15s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 639319.191550195,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 5.4us | P99.9: 11.2us\nthreads: 1 | elapsed: 0.31s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2448579.9735908406,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.2us | P99.9: 8.8us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 414655.0746910308,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.3us | P99.9: 12.4us\nthreads: 1 | elapsed: 0.48s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 196719.9077982448,
            "unit": "ops/sec",
            "extra": "P50: 4.8us | P99: 7.1us | P99.9: 14.8us\nthreads: 1 | elapsed: 1.02s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1208841.3982919895,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 6.0us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 691117.3813733816,
            "unit": "ops/sec",
            "extra": "P50: 0.4us | P99: 0.6us | P99.9: 4.3us\nthreads: 1 | elapsed: 0.29s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 529140.8067947987,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 8.8us | P99.9: 15.3us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "64bcf6849ae53f53c0ff1e336918d940b0715227",
          "message": "perf(bench): add multi-threaded support to all db_bench workloads (#155)\n\n## Summary\n- Extract `run_threaded` helper + `distribute_ops` into `mod.rs` —\nshared threading boilerplate for all workloads\n- Add `--threads N` support to all 8 single-threaded workloads:\n`fillseq`, `fillrandom`, `readrandom`, `readseq`, `seekrandom`,\n`prefixscan`, `overwrite`, `mergerandom`\n- Previously only `readwhilewriting` honored `--threads`; all others\nsilently ignored it\n\n## Design decisions\n| Workload | Multi-thread strategy |\n|----------|----------------------|\n| `fillseq`, `readseq` | Partitioned key ranges (thread t owns `[start,\nstart+ops)`) |\n| `fillrandom`, `overwrite`, `readrandom`, `seekrandom`, `prefixscan` |\nShared data, random access (contention intentional) |\n| `mergerandom` | Global op range partitioned to preserve key\ndistribution; flush + compact timed after thread join |\n\n## Test plan\n- [x] `cargo clippy -- -D warnings` — clean\n- [x] `cargo test --lib` — 515 passed, 0 failed\n- [x] All 9 workloads tested with `--threads 1` and `--threads 4`\n- [x] `mergerandom` counter verification passes with 4 threads\n- [x] `--benchmark all --github-json` works with both thread counts\n\nCloses #136",
          "timestamp": "2026-03-24T06:40:24+02:00",
          "tree_id": "b192d9b6e48f3acd062cd601d8ac7445da082f94",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/64bcf6849ae53f53c0ff1e336918d940b0715227"
        },
        "date": 1774327285777,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1995586.9191751976,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1248275.7177424661,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.6us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 578085.5335586688,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 5.6us | P99.9: 11.6us\nthreads: 1 | elapsed: 0.35s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2498525.1518344367,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.2us | P99.9: 8.6us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 372416.9622690615,
            "unit": "ops/sec",
            "extra": "P50: 2.4us | P99: 6.4us | P99.9: 12.7us\nthreads: 1 | elapsed: 0.54s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 200574.07207752633,
            "unit": "ops/sec",
            "extra": "P50: 4.6us | P99: 7.1us | P99.9: 15.9us\nthreads: 1 | elapsed: 1.00s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1129677.5754471268,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.8us | P99.9: 6.6us\nthreads: 1 | elapsed: 0.18s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 679503.592591553,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 3.2us\nthreads: 1 | elapsed: 0.29s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 494835.12253301527,
            "unit": "ops/sec",
            "extra": "P50: 1.7us | P99: 8.0us | P99.9: 15.6us\nthreads: 1 | elapsed: 0.40s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "255865126+sw-release-bot[bot]@users.noreply.github.com",
            "name": "sw-release-bot[bot]",
            "username": "sw-release-bot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "50bee97bbb00a56d3a611bc8868b3057a6ffe237",
          "message": "chore: release v4.1.0 (#150)\n\n## 🤖 New release\n\n* `coordinode-lsm-tree`: 4.0.0 -> 4.1.0\n\n<details><summary><i><b>Changelog</b></i></summary><p>\n\n<blockquote>\n\n##\n[4.1.0](https://github.com/structured-world/coordinode-lsm-tree/compare/v4.0.0...v4.1.0)\n- 2026-03-24\n\n### Added\n\n- *(fs)* io_uring Fs implementation for high-throughput I/O\n([#106](https://github.com/structured-world/coordinode-lsm-tree/pull/106))\n- *(compression)* zstd dictionary compression support\n([#131](https://github.com/structured-world/coordinode-lsm-tree/pull/131))\n\n### Documentation\n\n- add benchmark dashboard link and update badges\n([#151](https://github.com/structured-world/coordinode-lsm-tree/pull/151))\n- add v4.0.0 fork epoch changelog (all changes since upstream v3.1.1)\n\n### Fixed\n\n- *(version)* fsync version file before rewriting CURRENT pointer\n([#152](https://github.com/structured-world/coordinode-lsm-tree/pull/152))\n- thread UserComparator through ingestion guards and range overlap\n([#139](https://github.com/structured-world/coordinode-lsm-tree/pull/139))\n\n### Performance\n\n- *(bench)* add multi-threaded support to all db_bench workloads\n([#155](https://github.com/structured-world/coordinode-lsm-tree/pull/155))\n- *(merge)* replace IntervalHeap with sorted-vec heap +\nreplace_min/replace_max\n([#148](https://github.com/structured-world/coordinode-lsm-tree/pull/148))\n- *(compaction)* merge input ranges before L2 overlap query\n([#146](https://github.com/structured-world/coordinode-lsm-tree/pull/146))\n\n### Refactored\n\n- *(version)* comparator API cleanup — TransformContext + rename\nRun::push()\n([#153](https://github.com/structured-world/coordinode-lsm-tree/pull/153))\n- add #[non_exhaustive] to CompressionType enum\n</blockquote>\n\n\n</p></details>\n\n---\nThis PR was generated with\n[release-plz](https://github.com/release-plz/release-plz/).\n\nCo-authored-by: sw-release-bot[bot] <255865126+sw-release-bot[bot]@users.noreply.github.com>",
          "timestamp": "2026-03-24T14:49:07+02:00",
          "tree_id": "5f8b3f8de4139568eb715fed75ac391e4340a4ab",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/50bee97bbb00a56d3a611bc8868b3057a6ffe237"
        },
        "date": 1774356613725,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2063460.9789263166,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1256739.090571565,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.6us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 612246.5110413178,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 5.5us | P99.9: 11.4us\nthreads: 1 | elapsed: 0.33s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2484671.10116685,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.2us | P99.9: 8.4us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 413327.09311376745,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.3us | P99.9: 12.2us\nthreads: 1 | elapsed: 0.48s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 201621.4213243626,
            "unit": "ops/sec",
            "extra": "P50: 4.6us | P99: 6.7us | P99.9: 14.7us\nthreads: 1 | elapsed: 0.99s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1194288.0879162278,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.8us | P99.9: 6.5us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 742752.4389557581,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 2.9us\nthreads: 1 | elapsed: 0.27s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 528786.5851475518,
            "unit": "ops/sec",
            "extra": "P50: 1.6us | P99: 7.6us | P99.9: 12.9us\nthreads: 1 | elapsed: 0.38s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "433d169b54af11f51bc2d5a4fecd17bd502130bb",
          "message": "feat(compaction): expose seqno in CompactionFilter ItemAccessor (#160)\n\n## Summary\n\n- Add `ItemAccessor::seqno()` method to `CompactionFilter`, exposing the\nsequence number of items during compaction\n- Enables retention-aware MVCC GC patterns (e.g. keep versions within a\ntime window)\n\n## Technical Details\n\nSingle method addition to `ItemAccessor` in `src/compaction/filter.rs` —\ndelegates to `item.key.seqno`. Marked `#[must_use]` consistent with\nexisting `key()` method.\n\n## Test Plan\n\n- `compaction_filter_seqno_matches_insert_time_value` — verifies\n`seqno()` returns correct values matching insert-time seqnos\n- `compaction_filter_seqno_below_cutoff_removes_item` — end-to-end\nretention-based GC: items below seqno cutoff are removed, above are kept\n\nCloses #156",
          "timestamp": "2026-03-24T16:57:54+02:00",
          "tree_id": "283e540d5b24b7c8462073a5786564b330a0b720",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/433d169b54af11f51bc2d5a4fecd17bd502130bb"
        },
        "date": 1774364342579,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2008396.019222519,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1268347.3901526497,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 1.3us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 584047.0753623684,
            "unit": "ops/sec",
            "extra": "P50: 1.5us | P99: 5.6us | P99.9: 11.7us\nthreads: 1 | elapsed: 0.34s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2464701.682428783,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.3us | P99.9: 8.6us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 401244.78329619893,
            "unit": "ops/sec",
            "extra": "P50: 2.2us | P99: 6.4us | P99.9: 12.3us\nthreads: 1 | elapsed: 0.50s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 202950.43991659125,
            "unit": "ops/sec",
            "extra": "P50: 4.6us | P99: 6.1us | P99.9: 15.1us\nthreads: 1 | elapsed: 0.99s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1099039.4933357597,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.8us | P99.9: 6.6us\nthreads: 1 | elapsed: 0.18s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 673114.1307549508,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 0.5us | P99.9: 3.3us\nthreads: 1 | elapsed: 0.30s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 475673.37166813575,
            "unit": "ops/sec",
            "extra": "P50: 1.8us | P99: 8.1us | P99.9: 16.2us\nthreads: 1 | elapsed: 0.42s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "distinct": true,
          "id": "121006dc0908b86cde49ad295e7f8462b5830e12",
          "message": "ci: add release-plz 'release' step for auto-tagging and GitHub Release\n\nPreviously only 'release-pr' ran — created PR but never created\nGitHub Release + tag after merge. Added 'release' step that checks\nif Cargo.toml version > latest tag → creates tag + release →\ntriggers release.yml → cargo publish via OIDC.\n\nFlow: push main → release-pr (creates/updates PR) → release\n(creates tag + GitHub Release if version bumped) → release.yml\n(cargo publish)",
          "timestamp": "2026-03-24T17:26:24+02:00",
          "tree_id": "0143b459dd08b4769eda7075a9d236ac14de6fdd",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/121006dc0908b86cde49ad295e7f8462b5830e12"
        },
        "date": 1774366056267,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2031915.520592656,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1150838.4971888268,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.3us | P99.9: 6.7us\nthreads: 1 | elapsed: 0.17s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 487975.99456748035,
            "unit": "ops/sec",
            "extra": "P50: 1.9us | P99: 5.9us | P99.9: 12.7us\nthreads: 1 | elapsed: 0.41s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2470583.564437896,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.4us | P99.9: 8.8us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 349781.25406167086,
            "unit": "ops/sec",
            "extra": "P50: 2.5us | P99: 6.7us | P99.9: 13.7us\nthreads: 1 | elapsed: 0.57s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 199042.95965097658,
            "unit": "ops/sec",
            "extra": "P50: 4.7us | P99: 7.0us | P99.9: 16.2us\nthreads: 1 | elapsed: 1.00s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1092135.7619112586,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.9us | P99.9: 6.7us\nthreads: 1 | elapsed: 0.18s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 675587.6803803061,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.1us | P99.9: 2.8us\nthreads: 1 | elapsed: 0.30s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 447665.7997887053,
            "unit": "ops/sec",
            "extra": "P50: 2.0us | P99: 8.0us | P99.9: 13.7us\nthreads: 1 | elapsed: 0.45s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "distinct": true,
          "id": "5add112c416fab0a76ef123cf43bf93e0f8427c0",
          "message": "ci: add CodeRabbit config with auto-labeling for PRs\n\nCodeRabbit was only auto-labeling issues but not pull requests.\nEnable auto_label via repo-level config file.",
          "timestamp": "2026-03-24T18:14:26+02:00",
          "tree_id": "6dcafcc52eaa0035241effad18a9358c3019f7d2",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/5add112c416fab0a76ef123cf43bf93e0f8427c0"
        },
        "date": 1774368995025,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2024218.6015204028,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.2us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.10s | num: 200000"
          },
          {
            "name": "fillrandom",
            "value": 1307603.574216686,
            "unit": "ops/sec",
            "extra": "P50: 0.6us | P99: 2.0us | P99.9: 5.5us\nthreads: 1 | elapsed: 0.15s | num: 200000"
          },
          {
            "name": "readrandom",
            "value": 601827.4882869702,
            "unit": "ops/sec",
            "extra": "P50: 1.4us | P99: 5.4us | P99.9: 11.1us\nthreads: 1 | elapsed: 0.33s | num: 200000"
          },
          {
            "name": "readseq",
            "value": 2480753.569581119,
            "unit": "ops/sec",
            "extra": "P50: 0.2us | P99: 4.3us | P99.9: 8.3us\nthreads: 1 | elapsed: 0.08s | num: 200000"
          },
          {
            "name": "seekrandom",
            "value": 416573.3611766524,
            "unit": "ops/sec",
            "extra": "P50: 2.1us | P99: 6.3us | P99.9: 12.3us\nthreads: 1 | elapsed: 0.48s | num: 200000"
          },
          {
            "name": "prefixscan",
            "value": 199235.52928940597,
            "unit": "ops/sec",
            "extra": "P50: 4.7us | P99: 7.1us | P99.9: 15.3us\nthreads: 1 | elapsed: 1.00s | num: 200000"
          },
          {
            "name": "overwrite",
            "value": 1222312.1321073484,
            "unit": "ops/sec",
            "extra": "P50: 0.7us | P99: 2.7us | P99.9: 6.0us\nthreads: 1 | elapsed: 0.16s | num: 200000"
          },
          {
            "name": "mergerandom",
            "value": 728633.751530732,
            "unit": "ops/sec",
            "extra": "P50: 0.3us | P99: 2.0us | P99.9: 3.5us\nthreads: 1 | elapsed: 0.27s | num: 200000"
          },
          {
            "name": "readwhilewriting",
            "value": 487870.1134225077,
            "unit": "ops/sec",
            "extra": "P50: 1.8us | P99: 8.0us | P99.9: 15.1us\nthreads: 1 | elapsed: 0.41s | num: 200000"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "25550c6ccac384e2b7f8cf4333e19fd2ddf8b5be",
          "message": "perf(bench): normalize results against runner calibration (#162)\n\n## Summary\n\n- Add runner calibration workload (sequential write, random read, CPU\nCRC32) that measures hardware capabilities before benchmarks run,\nnormalizing ops/sec so results are comparable across different CI\nrunners\n- Add `--iterations N` flag (default 3 for `--github-json`) with median\nselection to reduce within-runner variance\n- Tighten CI regression thresholds from 15%/25% to 10%/15%\n- Optimize criterion benchmarks: reduce bloom filter size 100M→1M, trim\nFPR levels 5→3, reduce tree/level_manifest segment counts\n\n## Technical Details\n\n**Calibration** (`tools/db_bench/src/calibrate.rs`):\n- Sequential 4K write IOPS (64 MiB file)\n- Random 4K read IOPS (10K reads from 64 MiB file, deterministic LCG\noffsets)\n- CPU throughput (bitwise CRC32 over 64 MiB, `black_box`-guarded)\n- Weighted geometric mean: `seq^0.3 * rand^0.4 * cpu^0.3`\n- `REFERENCE_COMPOSITE = 23_000` (factor ≈ 1.0 on ubuntu-latest)\n\n**Normalization**: `normalized = raw_ops * REFERENCE / composite`\n\n**New CLI flags**: `--iterations N`, `--skip-calibration`\n\n**Criterion optimizations** (estimated ~60% runtime reduction):\n- `bloom.rs`: filter n=100M→1M, FPR levels [0.1..0.00001]→[0.01, 0.001,\n0.0001]\n- `tree.rs`: segments [1..512]→[1,4,16,64,128], drop 1M-item scans\n- `level_manifest.rs`: segments [0..4000]→[0..1000]\n\n## Test plan\n\n- [x] `cargo test --manifest-path tools/db_bench/Cargo.toml` — 6/6\npassed\n- [x] `cargo clippy` — clean\n- [x] `cargo test --lib` — 516 passed\n- [x] Manual test: `--github-json`, `--skip-calibration`, `--iterations\n2`\n- [ ] CI benchmark workflow runs successfully with calibration\n\nCloses #161\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **New Features**\n* Optional multi-iteration benchmark runs with median selection and a\nflag to skip calibration.\n* Hardware calibration to normalize throughput reporting; outputs show\ncalibrated and raw metrics.\n\n* **Chores**\n  * Tightened CI benchmark regression thresholds.\n  * Reduced benchmark input sizes to shorten test execution time.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-03-24T19:19:25+02:00",
          "tree_id": "48f9339c9c099e9af76d2b173faa663f4ff4e83a",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/25550c6ccac384e2b7f8cf4333e19fd2ddf8b5be"
        },
        "date": 1774372841100,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1289592.9636621,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1919954 ops/sec | factor: 0.672 | P50: 0.4us | P99: 2.4us | P99.9: 5.4us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=214730 rand_rd=590179 cpu=123 composite=34242.5"
          },
          {
            "name": "fillrandom",
            "value": 712145.6470742999,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1060247 ops/sec | factor: 0.672 | P50: 0.7us | P99: 2.9us | P99.9: 7.1us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=214730 rand_rd=590179 cpu=123 composite=34242.5"
          },
          {
            "name": "readrandom",
            "value": 371690.7330306526,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 553375 ops/sec | factor: 0.672 | P50: 1.6us | P99: 5.7us | P99.9: 12.1us\nthreads: 1 | elapsed: 0.36s | num: 200000 | iterations: 3 | runner: seq_wr=214730 rand_rd=590179 cpu=123 composite=34242.5"
          },
          {
            "name": "readseq",
            "value": 1657060.5943896933,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2467042 ops/sec | factor: 0.672 | P50: 0.2us | P99: 4.3us | P99.9: 8.1us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=214730 rand_rd=590179 cpu=123 composite=34242.5"
          },
          {
            "name": "seekrandom",
            "value": 253811.60250966853,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 377876 ops/sec | factor: 0.672 | P50: 2.3us | P99: 6.4us | P99.9: 13.9us\nthreads: 1 | elapsed: 0.53s | num: 200000 | iterations: 3 | runner: seq_wr=214730 rand_rd=590179 cpu=123 composite=34242.5"
          },
          {
            "name": "prefixscan",
            "value": 135063.03666320688,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 201083 ops/sec | factor: 0.672 | P50: 4.6us | P99: 6.3us | P99.9: 15.6us\nthreads: 1 | elapsed: 0.99s | num: 200000 | iterations: 3 | runner: seq_wr=214730 rand_rd=590179 cpu=123 composite=34242.5"
          },
          {
            "name": "overwrite",
            "value": 769441.9689476031,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1145550 ops/sec | factor: 0.672 | P50: 0.7us | P99: 2.8us | P99.9: 6.1us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=214730 rand_rd=590179 cpu=123 composite=34242.5"
          },
          {
            "name": "mergerandom",
            "value": 422188.39966286067,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 628557 ops/sec | factor: 0.672 | P50: 0.3us | P99: 2.1us | P99.9: 3.6us\nthreads: 1 | elapsed: 0.32s | num: 200000 | iterations: 3 | runner: seq_wr=214730 rand_rd=590179 cpu=123 composite=34242.5"
          },
          {
            "name": "readwhilewriting",
            "value": 334017.95382902323,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 497288 ops/sec | factor: 0.672 | P50: 1.8us | P99: 5.5us | P99.9: 13.0us\nthreads: 1 | elapsed: 0.40s | num: 200000 | iterations: 3 | runner: seq_wr=214730 rand_rd=590179 cpu=123 composite=34242.5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1d048147105cef0e8cfdda3c6075192dd412d6cc",
          "message": "feat(config): per-level Fs routing for tiered storage (#163)\n\n## Summary\n\n- Add `LevelRoute` type and `level_routes` config field to route SST\nfiles to different storage devices based on LSM level (e.g., NVMe for\nL0–L1, SSD for L2–L4, HDD for L5–L6)\n- All write paths (flush, compaction, ingestion) respect level routing;\nrecovery scans all configured folders\n- Trivial moves across device boundaries auto-convert to merge (rewrite\nto correct tier)\n- Zero overhead when unconfigured — single `Option` branch check, no\nallocations\n\n## Technical Details\n\n**Config API:**\n- `LevelRoute { levels: Range<u8>, path: PathBuf, fs: Arc<dyn Fs> }` —\nmaps level ranges to storage tiers\n- `Config::tables_folder_for_level(level)` — resolves `(PathBuf, Arc<dyn\nFs>)` with fallback to primary\n- `Config::all_tables_folders()` — deduplicated list for recovery\nscanning\n- `Config::level_routes(vec![...])` — builder with overlap validation\n(panics on overlapping ranges)\n\n**Write paths updated:**\n- `flush_to_tables_with_rt()` — uses `tables_folder_for_level(0)` for L0\n- `prepare_table_writer()` — uses `tables_folder_for_level(dest_level)`\nfor compaction output\n- `Ingestion::new()` / `BlobIngestion` — route to level 0 tier\n- `do_compaction()` — detects cross-device `Choice::Move` and converts\nto `Merge`\n\n**Recovery:** `recover_levels()` scans all folders from\n`all_tables_folders()` instead of just the primary path. No manifest\nschema changes — path is computed from level at runtime.\n\n## Known Limitations\n\n- Blob files (value log) are not level-routed — they stay in the primary\npath\n- `rename()` across filesystems is not supported; cross-device moves are\nhandled by rewriting\n\n## Test Plan\n\n- [x] `flush_writes_to_hot_tier` — L0 flush goes to configured hot tier\ndirectory\n- [x] `compaction_writes_to_correct_tier` — major compaction moves\ntables to cold tier\n- [x] `recovery_discovers_tables_across_tiers` — reopen finds tables\nacross all paths\n- [x] `no_overhead_without_level_routes` — default config works\nunchanged\n- [x] `tables_folder_for_level_fallback` — routing logic for all level\nranges\n- [x] `all_tables_folders_deduplicates` — no duplicate paths in recovery\nscan\n- [x] `overlapping_routes_panic` — validation rejects overlapping level\nranges\n\nCloses #78\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **New Features**\n* Tiered storage routing: per-level storage locations and filesystems\nvia configurable level routes; new config options to target tables by\nlevel.\n\n* **Bug Fixes**\n* Compaction avoids invalid cross-tier moves by rewriting when tables\nspan different storage folders.\n* Recovery/reopen scan and clean tables across all routed tables/\ndirectories and create missing tier dirs.\n\n* **Tests**\n* Added integration tests covering routing, placement, compaction\nbehavior, recovery, and config invariants.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-03-24T20:00:51+02:00",
          "tree_id": "79fa59556a16d9f1d1b896c05efb76e67f6caf1b",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/1d048147105cef0e8cfdda3c6075192dd412d6cc"
        },
        "date": 1774375324354,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1320196.418818304,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1960340 ops/sec | factor: 0.673 | P50: 0.4us | P99: 2.4us | P99.9: 5.4us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=202498 rand_rd=613086 cpu=123 composite=34152.4"
          },
          {
            "name": "fillrandom",
            "value": 801243.1510153636,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1189754 ops/sec | factor: 0.673 | P50: 0.7us | P99: 2.8us | P99.9: 6.1us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=202498 rand_rd=613086 cpu=123 composite=34152.4"
          },
          {
            "name": "readrandom",
            "value": 392414.8550608265,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 582691 ops/sec | factor: 0.673 | P50: 1.5us | P99: 5.5us | P99.9: 11.4us\nthreads: 1 | elapsed: 0.34s | num: 200000 | iterations: 3 | runner: seq_wr=202498 rand_rd=613086 cpu=123 composite=34152.4"
          },
          {
            "name": "readseq",
            "value": 1653511.9613935435,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2455276 ops/sec | factor: 0.673 | P50: 0.2us | P99: 4.3us | P99.9: 8.1us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=202498 rand_rd=613086 cpu=123 composite=34152.4"
          },
          {
            "name": "seekrandom",
            "value": 270695.8622928822,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 401952 ops/sec | factor: 0.673 | P50: 2.2us | P99: 6.4us | P99.9: 12.4us\nthreads: 1 | elapsed: 0.50s | num: 200000 | iterations: 3 | runner: seq_wr=202498 rand_rd=613086 cpu=123 composite=34152.4"
          },
          {
            "name": "prefixscan",
            "value": 136887.7393676269,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 203263 ops/sec | factor: 0.673 | P50: 4.6us | P99: 6.8us | P99.9: 15.3us\nthreads: 1 | elapsed: 0.98s | num: 200000 | iterations: 3 | runner: seq_wr=202498 rand_rd=613086 cpu=123 composite=34152.4"
          },
          {
            "name": "overwrite",
            "value": 789356.8188063244,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1172104 ops/sec | factor: 0.673 | P50: 0.7us | P99: 2.9us | P99.9: 6.1us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=202498 rand_rd=613086 cpu=123 composite=34152.4"
          },
          {
            "name": "mergerandom",
            "value": 487762.84370307426,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 724272 ops/sec | factor: 0.673 | P50: 0.3us | P99: 2.1us | P99.9: 2.8us\nthreads: 1 | elapsed: 0.28s | num: 200000 | iterations: 3 | runner: seq_wr=202498 rand_rd=613086 cpu=123 composite=34152.4"
          },
          {
            "name": "readwhilewriting",
            "value": 331406.43222883326,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 492101 ops/sec | factor: 0.673 | P50: 1.9us | P99: 4.4us | P99.9: 12.4us\nthreads: 1 | elapsed: 0.41s | num: 200000 | iterations: 3 | runner: seq_wr=202498 rand_rd=613086 cpu=123 composite=34152.4"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "distinct": true,
          "id": "1cdc5809144cdc3c9b19b48ed1fc499ff3055fd9",
          "message": "ci: auto-label issues by conventional title prefix\n\nParses issue titles for conventional commit format (feat/fix/perf/bench/etc)\nand applies matching labels. Also maps scopes (compaction, crash, encrypt)\nto domain-specific labels.",
          "timestamp": "2026-03-24T22:05:05+02:00",
          "tree_id": "ac70c8286a8e75442a5a5078795a661178352a96",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/1cdc5809144cdc3c9b19b48ed1fc499ff3055fd9"
        },
        "date": 1774383315199,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1323716.6167250746,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1975169 ops/sec | factor: 0.670 | P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=206696 rand_rd=610536 cpu=123 composite=34319.2"
          },
          {
            "name": "fillrandom",
            "value": 774829.5346473391,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1156153 ops/sec | factor: 0.670 | P50: 0.7us | P99: 2.8us | P99.9: 6.4us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=206696 rand_rd=610536 cpu=123 composite=34319.2"
          },
          {
            "name": "readrandom",
            "value": 371818.14238035976,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 554804 ops/sec | factor: 0.670 | P50: 1.6us | P99: 5.6us | P99.9: 11.8us\nthreads: 1 | elapsed: 0.36s | num: 200000 | iterations: 3 | runner: seq_wr=206696 rand_rd=610536 cpu=123 composite=34319.2"
          },
          {
            "name": "readseq",
            "value": 1670923.4123340282,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2493250 ops/sec | factor: 0.670 | P50: 0.2us | P99: 4.2us | P99.9: 8.4us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=206696 rand_rd=610536 cpu=123 composite=34319.2"
          },
          {
            "name": "seekrandom",
            "value": 269509.2857719811,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 402145 ops/sec | factor: 0.670 | P50: 2.2us | P99: 6.3us | P99.9: 12.6us\nthreads: 1 | elapsed: 0.50s | num: 200000 | iterations: 3 | runner: seq_wr=206696 rand_rd=610536 cpu=123 composite=34319.2"
          },
          {
            "name": "prefixscan",
            "value": 134814.68759039504,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 201162 ops/sec | factor: 0.670 | P50: 4.6us | P99: 6.5us | P99.9: 14.6us\nthreads: 1 | elapsed: 0.99s | num: 200000 | iterations: 3 | runner: seq_wr=206696 rand_rd=610536 cpu=123 composite=34319.2"
          },
          {
            "name": "overwrite",
            "value": 797083.1828964297,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1189359 ops/sec | factor: 0.670 | P50: 0.7us | P99: 2.8us | P99.9: 6.1us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=206696 rand_rd=610536 cpu=123 composite=34319.2"
          },
          {
            "name": "mergerandom",
            "value": 483433.67522262805,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 721350 ops/sec | factor: 0.670 | P50: 0.3us | P99: 2.1us | P99.9: 3.6us\nthreads: 1 | elapsed: 0.28s | num: 200000 | iterations: 3 | runner: seq_wr=206696 rand_rd=610536 cpu=123 composite=34319.2"
          },
          {
            "name": "readwhilewriting",
            "value": 344268.30829649814,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 513696 ops/sec | factor: 0.670 | P50: 1.8us | P99: 4.3us | P99.9: 12.2us\nthreads: 1 | elapsed: 0.39s | num: 200000 | iterations: 3 | runner: seq_wr=206696 rand_rd=610536 cpu=123 composite=34319.2"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8a07c582aab3efad5bc8c4fad56f838caa3d3c29",
          "message": "feat(error): RouteMismatch error, blocked_bloom cleanup, bench/clippy fixes (#166)\n\n## Summary\n\n- Add `Error::RouteMismatch { expected, found }` with level-based\ndetection — only returned when ALL missing tables are on levels not\ncovered by any current route (prevents masking genuine SST corruption)\n- Remove unfinished `blocked_bloom` module entirely (upstream\nfjall-rs/lsm-tree#78 still open, never integrated into Segment loader);\npreserve `FilterType::BlockedBloom` enum variant for on-disk format\ncompatibility\n- Fix never-looping `for` loops in `prop_mvcc` and\n`prop_range_tombstone` oracle `get()` methods\n- Update/remove benchmarks for current public API (`Config` 3-arg\nconstructor, `Cache`, `use_cache`, `SeqNo` params,\n`IterGuardImpl`/`Guard` pattern); remove 4 dead bench targets; fix\nTempDir lifetime\n- Convert `#[allow]` → `#[expect]` with reason strings in 14 test\nmodules\n- Fix `map_or` → `is_none_or` and needless borrow warnings in test code\n- Update `level_routes` reopen contract doc to mention `RouteMismatch`\n\n## Test plan\n\n- [x] `cargo test --test level_routing` — 24 passed (4 new: route\nmismatch, unrecoverable without routes, unrecoverable with routes, mixed\ncovered+uncovered)\n- [x] `cargo test --test prop_mvcc` — 1 passed\n- [x] `cargo test --test prop_range_tombstone` — 1 passed\n- [x] `cargo clippy --all-targets --all-features` — 0 errors\n- [x] codecov patch coverage — 100%\n\nCloses #164",
          "timestamp": "2026-03-25T00:00:31+02:00",
          "tree_id": "24474276a4910e71a7686a4e9d3f3d6056ae8a45",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/8a07c582aab3efad5bc8c4fad56f838caa3d3c29"
        },
        "date": 1774389699660,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1523659.8930085925,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1880643 ops/sec | factor: 0.810 | P50: 0.4us | P99: 2.7us | P99.9: 6.2us\nthreads: 1 | elapsed: 0.11s | num: 200000 | iterations: 3 | runner: seq_wr=207542 rand_rd=415516 cpu=108 composite=28388.7"
          },
          {
            "name": "fillrandom",
            "value": 834252.3138888007,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1029712 ops/sec | factor: 0.810 | P50: 0.7us | P99: 3.4us | P99.9: 9.1us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=207542 rand_rd=415516 cpu=108 composite=28388.7"
          },
          {
            "name": "readrandom",
            "value": 398693.7800676338,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 492105 ops/sec | factor: 0.810 | P50: 1.8us | P99: 6.8us | P99.9: 13.8us\nthreads: 1 | elapsed: 0.41s | num: 200000 | iterations: 3 | runner: seq_wr=207542 rand_rd=415516 cpu=108 composite=28388.7"
          },
          {
            "name": "readseq",
            "value": 1807817.193045485,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2231376 ops/sec | factor: 0.810 | P50: 0.2us | P99: 5.4us | P99.9: 10.4us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=207542 rand_rd=415516 cpu=108 composite=28388.7"
          },
          {
            "name": "seekrandom",
            "value": 279663.62204352143,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 345187 ops/sec | factor: 0.810 | P50: 2.5us | P99: 7.5us | P99.9: 14.9us\nthreads: 1 | elapsed: 0.58s | num: 200000 | iterations: 3 | runner: seq_wr=207542 rand_rd=415516 cpu=108 composite=28388.7"
          },
          {
            "name": "prefixscan",
            "value": 159709.33519938338,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 197128 ops/sec | factor: 0.810 | P50: 4.8us | P99: 6.3us | P99.9: 16.4us\nthreads: 1 | elapsed: 1.01s | num: 200000 | iterations: 3 | runner: seq_wr=207542 rand_rd=415516 cpu=108 composite=28388.7"
          },
          {
            "name": "overwrite",
            "value": 844372.496038282,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1042203 ops/sec | factor: 0.810 | P50: 0.7us | P99: 3.4us | P99.9: 8.9us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=207542 rand_rd=415516 cpu=108 composite=28388.7"
          },
          {
            "name": "mergerandom",
            "value": 588041.4887649999,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 725815 ops/sec | factor: 0.810 | P50: 0.4us | P99: 2.5us | P99.9: 3.8us\nthreads: 1 | elapsed: 0.28s | num: 200000 | iterations: 3 | runner: seq_wr=207542 rand_rd=415516 cpu=108 composite=28388.7"
          },
          {
            "name": "readwhilewriting",
            "value": 358243.69268951827,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 442178 ops/sec | factor: 0.810 | P50: 2.1us | P99: 5.0us | P99.9: 13.9us\nthreads: 1 | elapsed: 0.45s | num: 200000 | iterations: 3 | runner: seq_wr=207542 rand_rd=415516 cpu=108 composite=28388.7"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "9f5e98eadbec62e6fa0104c416f0909b33e534e6",
          "message": "perf(bench): consolidate benchmarks + nextest + flamegraph pipeline (#175)\n\n## Summary\n\n- **Phase 1:** Delete 4 redundant Criterion bench files, keep 3 core\nmicrobenchmarks (bloom, memtable, merge)\n- **Phase 2:** Add nextest `ci` profile with retries and JUnit XML\ngeneration\n- **Phase 3:** Add flamegraph pipeline — `--flamegraph` flag in db_bench\n(feature-gated with `tracing-flame`), CI workflow generates combined SVG\non main merges and publishes to gh-pages\n- **Bonus:** Fix all compiler warnings, reduce full test suite from\n~580s to 39s, raise benchmark regression thresholds\n\n## Technical Details\n\n### Benchmark consolidation\nRemoved 4 bench files that duplicated db_bench workloads or measured\nnon-hot-path code: `tree.rs`, `merge_point_read.rs`, `prefix_bloom.rs`,\n`fd_table.rs`. Remaining 3 (bloom, memtable, merge) are needed for\nupcoming #169 and #170.\n\n### Nextest CI profile\n`.config/nextest.toml` now has a `ci` profile with `retries = 2`,\n`fail-fast = false`, and JUnit XML at `target/nextest/ci/junit.xml`.\n\n### Flamegraph pipeline\ndb_bench gains a `flamegraph` Cargo feature (`tracing` + `tracing-flame`\n+ `tracing-subscriber`) and `--flamegraph` CLI flag. When enabled,\ntracing spans at workload and thread level are collected into a single\n`all.folded` file with thread collapsing. New `flamegraph.yml` workflow\nruns on main merges, generates a combined SVG with `inferno-flamegraph`\n(`--locked`), and deploys to\n`gh-pages/flamegraphs/<sha>/flamegraph.svg`.\n\n### Test speedup\n| Test | Before | After |\n|------|--------|-------|\n| blob_tree_fifo_limit | 52s | 4s |\n| a_lot_of_ranges | 41s | 3s |\n| leveled_sequential_inserts | 38s | 5s |\n| prop_mvcc | 124s | 7s |\n| prop_btreemap_oracle | 252s | 10s |\n| prop_range_tombstone | 309s | 11s |\n| **Full suite** | **~580s** | **39s** |\n\nProptest cases set to 32 (hardcoded in ProptestConfig). Edit `cases`\nfield in test files for thorough local runs.\n\n### Benchmark thresholds\nRaised from 10%/15% to 15% alert / 25% fail — shared CI runners have too\nmuch variance for tight thresholds.\n\n## Test plan\n- [x] `cargo bench --features lz4 --no-run` — 3 benches compile\n- [x] `cargo clippy --all-features -- -D warnings` — zero warnings\n- [x] `cargo nextest run --all-features` — 1040 passed, 0 failed, 39s\n- [x] `cargo test --doc --features lz4` — 34 passed\n- [x] `cargo clippy --features flamegraph` on db_bench — clean\n- [x] `db_bench --flamegraph --benchmark fillseq` — produces valid\nall.folded\n\nCloses #174",
          "timestamp": "2026-03-25T03:12:32+02:00",
          "tree_id": "76f573c204c1f051c2533c1714a511f03267e9bb",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/9f5e98eadbec62e6fa0104c416f0909b33e534e6"
        },
        "date": 1774401235661,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1361076.7529940973,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2058343 ops/sec | factor: 0.661 | P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=209854 rand_rd=624387 cpu=123 composite=34782.7"
          },
          {
            "name": "fillrandom",
            "value": 793665.827207775,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1200253 ops/sec | factor: 0.661 | P50: 0.7us | P99: 2.7us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=209854 rand_rd=624387 cpu=123 composite=34782.7"
          },
          {
            "name": "readrandom",
            "value": 401239.04497771687,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 606790 ops/sec | factor: 0.661 | P50: 1.5us | P99: 5.5us | P99.9: 11.4us\nthreads: 1 | elapsed: 0.33s | num: 200000 | iterations: 3 | runner: seq_wr=209854 rand_rd=624387 cpu=123 composite=34782.7"
          },
          {
            "name": "readseq",
            "value": 1650423.3171530243,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2495920 ops/sec | factor: 0.661 | P50: 0.2us | P99: 4.3us | P99.9: 7.8us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=209854 rand_rd=624387 cpu=123 composite=34782.7"
          },
          {
            "name": "seekrandom",
            "value": 274444.40871492634,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 415040 ops/sec | factor: 0.661 | P50: 2.1us | P99: 6.2us | P99.9: 12.2us\nthreads: 1 | elapsed: 0.48s | num: 200000 | iterations: 3 | runner: seq_wr=209854 rand_rd=624387 cpu=123 composite=34782.7"
          },
          {
            "name": "prefixscan",
            "value": 129527.20823528396,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 195883 ops/sec | factor: 0.661 | P50: 4.8us | P99: 5.8us | P99.9: 14.8us\nthreads: 1 | elapsed: 1.02s | num: 200000 | iterations: 3 | runner: seq_wr=209854 rand_rd=624387 cpu=123 composite=34782.7"
          },
          {
            "name": "overwrite",
            "value": 750282.0169232421,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1134644 ops/sec | factor: 0.661 | P50: 0.7us | P99: 2.8us | P99.9: 7.8us\nthreads: 1 | elapsed: 0.18s | num: 200000 | iterations: 3 | runner: seq_wr=209854 rand_rd=624387 cpu=123 composite=34782.7"
          },
          {
            "name": "mergerandom",
            "value": 489031.26240185247,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 739557 ops/sec | factor: 0.661 | P50: 0.3us | P99: 2.1us | P99.9: 3.7us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=209854 rand_rd=624387 cpu=123 composite=34782.7"
          },
          {
            "name": "readwhilewriting",
            "value": 356934.8711726238,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 539789 ops/sec | factor: 0.661 | P50: 1.7us | P99: 4.2us | P99.9: 11.7us\nthreads: 1 | elapsed: 0.37s | num: 200000 | iterations: 3 | runner: seq_wr=209854 rand_rd=624387 cpu=123 composite=34782.7"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "890707a5917d45f1cad74a635ebf9a4fe7b10625",
          "message": "feat(compression): CompressionProvider trait + pure Rust zstd backend (#176)\n\n## Summary\n\n- Add `CompressionProvider` trait abstracting zstd compress/decompress\nbehind compile-time selected backends\n- Add `zstd-pure` feature flag using\n[`structured-zstd`](https://github.com/structured-world/structured-zstd)\n— zero C dependencies\n- Replace all direct `zstd::bulk::*` calls with `ZstdBackend::*`\ndispatch through the trait\n- Both backends produce RFC 8878 compliant zstd frames\n(cross-compatible)\n\n## Technical Details\n\n**New files:**\n- `build.rs` — sets `cfg(zstd_any)` when either `zstd` or `zstd-pure`\nfeature is active, with `cargo:rerun-if-env-changed` for correct\nincremental rebuilds\n- `src/compression/mod.rs` — `CompressionProvider` trait + `ZstdBackend`\ntype alias (was `src/compression.rs`)\n- `src/compression/zstd_ffi.rs` — C FFI backend wrapping `zstd::bulk::*`\n- `src/compression/zstd_pure.rs` — pure Rust backend wrapping\n`structured_zstd`\n\n**cfg migration:** ~150 `cfg(feature = \"zstd\")` → `cfg(zstd_any)` across\n27 files so that `CompressionType::Zstd`, `ZstdDict`, `ZstdDictionary`,\nand all related match arms/parameters compile with either backend.\n\n**Backend precedence:** When both `zstd` and `zstd-pure` are enabled,\nthe C FFI backend takes precedence.\n\n**Decompression safety:** The pure Rust backend enforces capacity limits\n_during_ decode via `StreamingDecoder`'s `Read` impl — reads at most\n`capacity` bytes into a fixed buffer, preventing unbounded allocation\nfrom crafted zstd frames. Dictionary decompression uses `FrameDecoder`\n(StreamingDecoder lacks dict API) with a post-decode size check; the\nblock layer's `uncompressed_length` validation (capped at 256 MiB)\nprovides the primary bound.\n\n## Known Limitations\n\n- `zstd-pure` compression uses the `Fastest` level regardless of\nrequested level (higher levels not yet implemented in structured-zstd)\n- Dictionary compression not yet supported by pure Rust backend\n(dictionary decompression works)\n- Pure Rust decompression throughput ~2–3.5× slower than C reference\n- Dictionary is re-parsed from raw bytes on every decompress call (same\nas C FFI backend; cached precompiled dictionaries are a Phase 2\noptimization)\n\n## Test Plan\n\n- [x] `cargo check` — no features, `zstd`, `zstd-pure`, both features\n- [x] `cargo clippy` — zero warnings on lib code for all feature combos\n- [x] `cargo nextest run --features zstd` — 976 passed, 6 skipped\n- [x] `cargo nextest run --features zstd-pure` — 964 passed, 6 skipped\n(12 dict tests correctly gated on `feature = \"zstd\"`)\n- [x] `cargo test --doc --features zstd` — 34 passed, 2 ignored\n- [x] `cargo tree --features zstd-pure` — zero C dependencies in tree\n\nCloses #157\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **New Features**\n* Added a `zstd-pure` feature providing a pure-Rust Zstd backend (no C\ncompiler or system libs required).\n* Build script enables a unified Zstd configuration; when both backends\nare enabled, the C FFI backend takes precedence.\n\n* **Documentation**\n* README expanded to describe both Zstd backend options,\ninteroperability, precedence, and current pure-Rust limitations (Fastest\nmode only, no dictionary compression, slower decompression).\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-03-25T06:35:37+02:00",
          "tree_id": "121379cc1b6d93dac2a6ddcf3bc81a65be837469",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/890707a5917d45f1cad74a635ebf9a4fe7b10625"
        },
        "date": 1774413415188,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1326543.2837851713,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2012589 ops/sec | factor: 0.659 | P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=216167 rand_rd=615816 cpu=123 composite=34894.9"
          },
          {
            "name": "fillrandom",
            "value": 735439.3795002841,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1115785 ops/sec | factor: 0.659 | P50: 0.7us | P99: 2.9us | P99.9: 6.2us\nthreads: 1 | elapsed: 0.18s | num: 200000 | iterations: 3 | runner: seq_wr=216167 rand_rd=615816 cpu=123 composite=34894.9"
          },
          {
            "name": "readrandom",
            "value": 394440.00386952795,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 598432 ops/sec | factor: 0.659 | P50: 1.5us | P99: 5.5us | P99.9: 11.3us\nthreads: 1 | elapsed: 0.33s | num: 200000 | iterations: 3 | runner: seq_wr=216167 rand_rd=615816 cpu=123 composite=34894.9"
          },
          {
            "name": "readseq",
            "value": 1631709.1067514895,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2475577 ops/sec | factor: 0.659 | P50: 0.2us | P99: 4.2us | P99.9: 8.4us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=216167 rand_rd=615816 cpu=123 composite=34894.9"
          },
          {
            "name": "seekrandom",
            "value": 259976.46811686477,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 394428 ops/sec | factor: 0.659 | P50: 2.2us | P99: 6.4us | P99.9: 12.3us\nthreads: 1 | elapsed: 0.51s | num: 200000 | iterations: 3 | runner: seq_wr=216167 rand_rd=615816 cpu=123 composite=34894.9"
          },
          {
            "name": "prefixscan",
            "value": 131800.71591743617,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 199964 ops/sec | factor: 0.659 | P50: 4.6us | P99: 7.2us | P99.9: 14.7us\nthreads: 1 | elapsed: 1.00s | num: 200000 | iterations: 3 | runner: seq_wr=216167 rand_rd=615816 cpu=123 composite=34894.9"
          },
          {
            "name": "overwrite",
            "value": 777723.1572778897,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1179937 ops/sec | factor: 0.659 | P50: 0.7us | P99: 2.8us | P99.9: 6.3us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=216167 rand_rd=615816 cpu=123 composite=34894.9"
          },
          {
            "name": "mergerandom",
            "value": 484320.4308057616,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 734795 ops/sec | factor: 0.659 | P50: 0.3us | P99: 2.1us | P99.9: 3.0us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=216167 rand_rd=615816 cpu=123 composite=34894.9"
          },
          {
            "name": "readwhilewriting",
            "value": 358555.8415192324,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 543989 ops/sec | factor: 0.659 | P50: 1.6us | P99: 5.4us | P99.9: 12.0us\nthreads: 1 | elapsed: 0.37s | num: 200000 | iterations: 3 | runner: seq_wr=216167 rand_rd=615816 cpu=123 composite=34894.9"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f4a611d7dc979f4eb83b8948ceed7bc4cdf21171",
          "message": "chore: bump MSRV to 1.92, ignore dtolnay/rust-toolchain in dependabot (#179)\n\n## Summary\n- Bump `rust-version` in Cargo.toml: 1.90 → 1.92\n- Exclude `dtolnay/rust-toolchain` from dependabot github-actions\nupdates\n\nCloses #178\n\n---------\n\nCo-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>",
          "timestamp": "2026-03-25T20:31:11+02:00",
          "tree_id": "f8ca423395940cb6e2973bb845e54f0402884ad2",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/f4a611d7dc979f4eb83b8948ceed7bc4cdf21171"
        },
        "date": 1774463548030,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1302394.9846475415,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1974284 ops/sec | factor: 0.660 | P50: 0.4us | P99: 2.3us | P99.9: 5.4us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=222931 rand_rd=600012 cpu=123 composite=34865.4"
          },
          {
            "name": "fillrandom",
            "value": 793194.5392358815,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1202394 ops/sec | factor: 0.660 | P50: 0.6us | P99: 2.8us | P99.9: 6.5us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=222931 rand_rd=600012 cpu=123 composite=34865.4"
          },
          {
            "name": "readrandom",
            "value": 389655.9513816944,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 590675 ops/sec | factor: 0.660 | P50: 1.5us | P99: 5.6us | P99.9: 12.0us\nthreads: 1 | elapsed: 0.34s | num: 200000 | iterations: 3 | runner: seq_wr=222931 rand_rd=600012 cpu=123 composite=34865.4"
          },
          {
            "name": "readseq",
            "value": 1658359.3346149712,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2513886 ops/sec | factor: 0.660 | P50: 0.2us | P99: 4.1us | P99.9: 8.4us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=222931 rand_rd=600012 cpu=123 composite=34865.4"
          },
          {
            "name": "seekrandom",
            "value": 257154.42581547357,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 389817 ops/sec | factor: 0.660 | P50: 2.2us | P99: 6.4us | P99.9: 13.3us\nthreads: 1 | elapsed: 0.51s | num: 200000 | iterations: 3 | runner: seq_wr=222931 rand_rd=600012 cpu=123 composite=34865.4"
          },
          {
            "name": "prefixscan",
            "value": 134119.17415857821,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 203310 ops/sec | factor: 0.660 | P50: 4.6us | P99: 6.5us | P99.9: 16.2us\nthreads: 1 | elapsed: 0.98s | num: 200000 | iterations: 3 | runner: seq_wr=222931 rand_rd=600012 cpu=123 composite=34865.4"
          },
          {
            "name": "overwrite",
            "value": 801411.5053822275,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1214850 ops/sec | factor: 0.660 | P50: 0.7us | P99: 2.8us | P99.9: 6.0us\nthreads: 1 | elapsed: 0.16s | num: 200000 | iterations: 3 | runner: seq_wr=222931 rand_rd=600012 cpu=123 composite=34865.4"
          },
          {
            "name": "mergerandom",
            "value": 422978.77961879486,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 641188 ops/sec | factor: 0.660 | P50: 0.3us | P99: 2.1us | P99.9: 4.5us\nthreads: 1 | elapsed: 0.31s | num: 200000 | iterations: 3 | runner: seq_wr=222931 rand_rd=600012 cpu=123 composite=34865.4"
          },
          {
            "name": "readwhilewriting",
            "value": 342197.82946662937,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 518733 ops/sec | factor: 0.660 | P50: 1.7us | P99: 4.3us | P99.9: 12.1us\nthreads: 1 | elapsed: 0.39s | num: 200000 | iterations: 3 | runner: seq_wr=222931 rand_rd=600012 cpu=123 composite=34865.4"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c10f1ac008665af7a0546a2bef99423c56d21028",
          "message": "feat: comparator-aware range tombstones (#180)\n\n## Summary\n- thread the user comparator through memtable range tombstones, RT scan\nfiltering, MVCC suppression, table-skip checks, and RT clipping\n- add reverse-comparator regression coverage for memtable point reads\nand post-flush range scans\n- fold the Rust baseline update into this delivery: pin\n`rust-toolchain.toml` to `1.94.0`, raise MSRV to `1.92`, and migrate to\nRust 2024\n\n## Testing\n- `cargo nextest run --all-features`\n- `cargo test --doc --all-features`\n- `cargo check --all-features` in `tools/db_bench`\n\nCloses #94",
          "timestamp": "2026-03-26T23:12:27+02:00",
          "tree_id": "0e04fc2d6fe4a599c3d687aa0a8d2b165f988490",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/c10f1ac008665af7a0546a2bef99423c56d21028"
        },
        "date": 1774559632920,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1285955.1272474488,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1967767 ops/sec | factor: 0.654 | P50: 0.4us | P99: 2.3us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=219554 rand_rd=681562 cpu=108 composite=35194.6"
          },
          {
            "name": "fillrandom",
            "value": 696202.672039784,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1065329 ops/sec | factor: 0.654 | P50: 0.7us | P99: 3.1us | P99.9: 6.9us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=219554 rand_rd=681562 cpu=108 composite=35194.6"
          },
          {
            "name": "readrandom",
            "value": 366025.70440617675,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 560092 ops/sec | factor: 0.654 | P50: 1.6us | P99: 6.0us | P99.9: 12.8us\nthreads: 1 | elapsed: 0.36s | num: 200000 | iterations: 3 | runner: seq_wr=219554 rand_rd=681562 cpu=108 composite=35194.6"
          },
          {
            "name": "readseq",
            "value": 1520593.7487406214,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2326811 ops/sec | factor: 0.654 | P50: 0.2us | P99: 4.5us | P99.9: 9.5us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=219554 rand_rd=681562 cpu=108 composite=35194.6"
          },
          {
            "name": "seekrandom",
            "value": 245007.34205135627,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 374910 ops/sec | factor: 0.654 | P50: 2.3us | P99: 6.9us | P99.9: 14.0us\nthreads: 1 | elapsed: 0.53s | num: 200000 | iterations: 3 | runner: seq_wr=219554 rand_rd=681562 cpu=108 composite=35194.6"
          },
          {
            "name": "prefixscan",
            "value": 129553.73757008258,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 198243 ops/sec | factor: 0.654 | P50: 4.7us | P99: 6.3us | P99.9: 15.6us\nthreads: 1 | elapsed: 1.01s | num: 200000 | iterations: 3 | runner: seq_wr=219554 rand_rd=681562 cpu=108 composite=35194.6"
          },
          {
            "name": "overwrite",
            "value": 710461.4141659187,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1087147 ops/sec | factor: 0.654 | P50: 0.7us | P99: 3.1us | P99.9: 8.3us\nthreads: 1 | elapsed: 0.18s | num: 200000 | iterations: 3 | runner: seq_wr=219554 rand_rd=681562 cpu=108 composite=35194.6"
          },
          {
            "name": "mergerandom",
            "value": 498877.3180610398,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 763382 ops/sec | factor: 0.654 | P50: 0.4us | P99: 0.6us | P99.9: 3.2us\nthreads: 1 | elapsed: 0.26s | num: 200000 | iterations: 3 | runner: seq_wr=219554 rand_rd=681562 cpu=108 composite=35194.6"
          },
          {
            "name": "readwhilewriting",
            "value": 320571.37304466893,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 490538 ops/sec | factor: 0.654 | P50: 1.8us | P99: 5.2us | P99.9: 13.1us\nthreads: 1 | elapsed: 0.41s | num: 200000 | iterations: 3 | runner: seq_wr=219554 rand_rd=681562 cpu=108 composite=35194.6"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "962795745894d71ea6f5c6ab79a54f8eca38a276",
          "message": "test(table): add zstd dict helper coverage (#181)\n\n## Summary\n- extend the shared test_with_table helper to optionally carry a zstd\ndictionary through writer and all table recovery matrix variants\n- add unit-level ZstdDict coverage for the helper using a focused table\npoint-read round-trip\n- fix the partitioned-index helper path so dictionary-compressed tables\nare reopened with the matching dictionary in every matrix variant\n\n## Testing\n- cargo fmt --all --check\n- cargo clippy --all-features --all-targets -- -D warnings\n- cargo nextest run --all-features\n- cargo test --doc --all-features\n- cargo check --all-features in tools/db_bench\n\nCloses #177",
          "timestamp": "2026-03-26T23:41:16+02:00",
          "tree_id": "5538136a0a2856b96a6e80f3461a113b447eb244",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/962795745894d71ea6f5c6ab79a54f8eca38a276"
        },
        "date": 1774561364283,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 880757.6295947902,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1923689 ops/sec | factor: 0.458 | P50: 0.4us | P99: 1.8us | P99.9: 3.7us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=335874 rand_rd=1140922 cpu=117 composite=50235.0"
          },
          {
            "name": "fillrandom",
            "value": 476890.2645146566,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1041590 ops/sec | factor: 0.458 | P50: 0.8us | P99: 2.5us | P99.9: 6.4us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=335874 rand_rd=1140922 cpu=117 composite=50235.0"
          },
          {
            "name": "readrandom",
            "value": 272129.16678280785,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 594365 ops/sec | factor: 0.458 | P50: 1.5us | P99: 4.6us | P99.9: 9.8us\nthreads: 1 | elapsed: 0.34s | num: 200000 | iterations: 3 | runner: seq_wr=335874 rand_rd=1140922 cpu=117 composite=50235.0"
          },
          {
            "name": "readseq",
            "value": 1482995.1190967935,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 3239053 ops/sec | factor: 0.458 | P50: 0.2us | P99: 3.2us | P99.9: 5.5us\nthreads: 1 | elapsed: 0.06s | num: 200000 | iterations: 3 | runner: seq_wr=335874 rand_rd=1140922 cpu=117 composite=50235.0"
          },
          {
            "name": "seekrandom",
            "value": 186130.44445028433,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 406533 ops/sec | factor: 0.458 | P50: 2.1us | P99: 5.2us | P99.9: 10.0us\nthreads: 1 | elapsed: 0.49s | num: 200000 | iterations: 3 | runner: seq_wr=335874 rand_rd=1140922 cpu=117 composite=50235.0"
          },
          {
            "name": "prefixscan",
            "value": 102036.57681014432,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 222861 ops/sec | factor: 0.458 | P50: 4.2us | P99: 5.4us | P99.9: 11.1us\nthreads: 1 | elapsed: 0.90s | num: 200000 | iterations: 3 | runner: seq_wr=335874 rand_rd=1140922 cpu=117 composite=50235.0"
          },
          {
            "name": "overwrite",
            "value": 488346.72707429365,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1066612 ops/sec | factor: 0.458 | P50: 0.8us | P99: 2.4us | P99.9: 6.0us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=335874 rand_rd=1140922 cpu=117 composite=50235.0"
          },
          {
            "name": "mergerandom",
            "value": 349149.1916657986,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 762587 ops/sec | factor: 0.458 | P50: 0.4us | P99: 0.8us | P99.9: 3.7us\nthreads: 1 | elapsed: 0.26s | num: 200000 | iterations: 3 | runner: seq_wr=335874 rand_rd=1140922 cpu=117 composite=50235.0"
          },
          {
            "name": "readwhilewriting",
            "value": 223446.67386403232,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 488036 ops/sec | factor: 0.458 | P50: 1.9us | P99: 4.0us | P99.9: 10.0us\nthreads: 1 | elapsed: 0.41s | num: 200000 | iterations: 3 | runner: seq_wr=335874 rand_rd=1140922 cpu=117 composite=50235.0"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "255865126+sw-release-bot[bot]@users.noreply.github.com",
            "name": "sw-release-bot[bot]",
            "username": "sw-release-bot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f7545315713b15c0006c936705549feda267ce51",
          "message": "chore: release v4.2.0 (#165)\n\n## 🤖 New release\n\n* `coordinode-lsm-tree`: 4.1.0 -> 4.2.0\n\n<details><summary><i><b>Changelog</b></i></summary><p>\n\n<blockquote>\n\n##\n[4.2.0](https://github.com/structured-world/coordinode-lsm-tree/compare/v4.1.0...v4.2.0)\n- 2026-03-26\n\n### Added\n\n- comparator-aware range tombstones\n([#180](https://github.com/structured-world/coordinode-lsm-tree/pull/180))\n- *(compression)* CompressionProvider trait + pure Rust zstd backend\n([#176](https://github.com/structured-world/coordinode-lsm-tree/pull/176))\n- *(error)* RouteMismatch error, blocked_bloom cleanup, bench/clippy\nfixes\n([#166](https://github.com/structured-world/coordinode-lsm-tree/pull/166))\n- *(config)* per-level Fs routing for tiered storage\n([#163](https://github.com/structured-world/coordinode-lsm-tree/pull/163))\n\n### Performance\n\n- *(bench)* consolidate benchmarks + nextest + flamegraph pipeline\n([#175](https://github.com/structured-world/coordinode-lsm-tree/pull/175))\n\n### Testing\n\n- *(table)* add zstd dict helper coverage\n([#181](https://github.com/structured-world/coordinode-lsm-tree/pull/181))\n</blockquote>\n\n\n</p></details>\n\n---\nThis PR was generated with\n[release-plz](https://github.com/release-plz/release-plz/).\n\nCo-authored-by: sw-release-bot[bot] <255865126+sw-release-bot[bot]@users.noreply.github.com>",
          "timestamp": "2026-03-27T08:30:45+02:00",
          "tree_id": "b97b9d97b4bfd9dbe5e4cb5908ee523df4a66c6c",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/f7545315713b15c0006c936705549feda267ce51"
        },
        "date": 1774593138874,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1374634.2374385325,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2063228 ops/sec | factor: 0.666 | P50: 0.3us | P99: 2.3us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=210508 rand_rd=611226 cpu=123 composite=34521.4"
          },
          {
            "name": "fillrandom",
            "value": 797241.7964721083,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1196603 ops/sec | factor: 0.666 | P50: 0.7us | P99: 2.7us | P99.9: 6.0us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=210508 rand_rd=611226 cpu=123 composite=34521.4"
          },
          {
            "name": "readrandom",
            "value": 389906.62261401233,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 585222 ops/sec | factor: 0.666 | P50: 1.5us | P99: 5.7us | P99.9: 11.5us\nthreads: 1 | elapsed: 0.34s | num: 200000 | iterations: 3 | runner: seq_wr=210508 rand_rd=611226 cpu=123 composite=34521.4"
          },
          {
            "name": "readseq",
            "value": 1657862.37900138,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2488333 ops/sec | factor: 0.666 | P50: 0.2us | P99: 4.3us | P99.9: 8.2us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=210508 rand_rd=611226 cpu=123 composite=34521.4"
          },
          {
            "name": "seekrandom",
            "value": 259075.30300721794,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 388854 ops/sec | factor: 0.666 | P50: 2.2us | P99: 6.4us | P99.9: 12.5us\nthreads: 1 | elapsed: 0.51s | num: 200000 | iterations: 3 | runner: seq_wr=210508 rand_rd=611226 cpu=123 composite=34521.4"
          },
          {
            "name": "prefixscan",
            "value": 132421.72489574543,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 198756 ops/sec | factor: 0.666 | P50: 4.7us | P99: 7.0us | P99.9: 15.0us\nthreads: 1 | elapsed: 1.01s | num: 200000 | iterations: 3 | runner: seq_wr=210508 rand_rd=611226 cpu=123 composite=34521.4"
          },
          {
            "name": "overwrite",
            "value": 815657.9197557986,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1224244 ops/sec | factor: 0.666 | P50: 0.7us | P99: 2.7us | P99.9: 6.0us\nthreads: 1 | elapsed: 0.16s | num: 200000 | iterations: 3 | runner: seq_wr=210508 rand_rd=611226 cpu=123 composite=34521.4"
          },
          {
            "name": "mergerandom",
            "value": 466130.77165969176,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 699629 ops/sec | factor: 0.666 | P50: 0.3us | P99: 0.6us | P99.9: 4.3us\nthreads: 1 | elapsed: 0.29s | num: 200000 | iterations: 3 | runner: seq_wr=210508 rand_rd=611226 cpu=123 composite=34521.4"
          },
          {
            "name": "readwhilewriting",
            "value": 355933.37232274393,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 534231 ops/sec | factor: 0.666 | P50: 1.7us | P99: 4.3us | P99.9: 11.8us\nthreads: 1 | elapsed: 0.37s | num: 200000 | iterations: 3 | runner: seq_wr=210508 rand_rd=611226 cpu=123 composite=34521.4"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "2a78e7eb0441c6d3af9a3267a88644154264795e",
          "message": "perf(table): support compressed index blocks (#182)\n\n## Summary\n- enable index block restart intervals > 1 for index block encoding,\nmetadata, and seek paths\n- fix seek_upper behavior across compressed restart intervals by\nadvancing to the next restart head when the trimmed tail is still below\nneedle\n- fix restart_interval=1 upper-seek limit for all-same-end-key blocks by\nsplitting seek_upper_impl behavior by cursor mode\n- harden data_block and index_block parsers: checked_add for all offset\narithmetic, reject key/value spans exceeding block boundaries\n- harden decoder: reject EOF offsets in reader_at, reject zero-length\nbinary indexes, fail-close forward iteration after back-cursor\ncorruption (poison_back_cursor delegates to clamp_upper_to_lo so next()\nalso stops), clear hi_scanner stack at fill_stack entry\n- fix dead is_none() guard in both seek_upper and\nadvance_upper_restart_interval post-fill_stack — use stack.is_empty() to\ndetect corruption after poison_back_cursor switched to clamp semantics\n- fail-close seek_upper_impl after clamped\nadvance_upper_restart_interval\n- narrow decoder module visibility to pub(crate), narrow peek reset\nhelpers to pub(crate)\n- thread entries_end into parse_restart_key for bounded binary-search\nprobes (avoids full-item decode, validates key spans against\nentries_end)\n- propagate meta_partition_size through use_partitioned_filter (was\nmissing, matching use_partitioned_index)\n- add pre-write guard to use_partitioned_filter preventing mid-stream\nfilter-writer swap\n- fix misleading \"compression policy\" panic message in\nRestartIntervalPolicy::new\n- add doc comments to encode_into_vec_with_restart_interval,\nBlockIndexWriter trait method, seek_upper semantics, base_key_end\nvalidation\n- add regression tests: EOF binary-index offsets, zero-length binary\nindex, poison-back-cursor fail-close, corrupted restart-head key,\nempty-policy rejection, post-write filter/index swap rejection\n- align release automation by disabling publish in release-plz config\n(git_only=true, publish=false)\n\n## Testing\n- cargo fmt --all -- --check\n- cargo nextest run --workspace (1017 tests, 0 failed)\n- cargo clippy --all-targets -- -D warnings\n- cargo test --doc --all-features\n- cargo check --all-features (tools/db_bench)\n- cargo bench --bench index_block --no-run\n\n## Related\n- #184 — make block decoder trailer validation fallible (tracked\nseparately)\n\nCloses #170\nCloses #189\nCloses #183\nCloses #190\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **New Features**\n* Index encoding now supports configurable restart intervals (rejects\nzero); writers expose a builder to set it.\n* New public APIs to encode index blocks with custom restart intervals.\n\n* **Bug Fixes**\n* Decoding/iteration now fail-closed on truncated/corrupted blocks,\npreventing panics and out-of-bounds reads.\n* Configuration validates non-zero/ non-empty restart policies and\nprevents changing intervals after writing starts.\n\n* **Tests**\n* Added benchmarks and many unit/integration tests for restart\nintervals, decoding robustness, and point reads.\n\n* **Chores**\n  * Release flow set to git-only; crates.io publishing disabled.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-03-31T16:26:04+03:00",
          "tree_id": "2a6f6db329f57e2d4fb91f31daa80b7996ebf8eb",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/2a78e7eb0441c6d3af9a3267a88644154264795e"
        },
        "date": 1774963657337,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1132555.1301038002,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2084974 ops/sec | factor: 0.543 | P50: 0.3us | P99: 2.0us | P99.9: 5.0us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=239464 rand_rd=924917 cpu=123 composite=42341.8"
          },
          {
            "name": "fillrandom",
            "value": 642398.8531958979,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1182622 ops/sec | factor: 0.543 | P50: 0.7us | P99: 2.6us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=239464 rand_rd=924917 cpu=123 composite=42341.8"
          },
          {
            "name": "readrandom",
            "value": 278112.4060998146,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 511990 ops/sec | factor: 0.543 | P50: 1.8us | P99: 5.1us | P99.9: 13.8us\nthreads: 1 | elapsed: 0.39s | num: 200000 | iterations: 3 | runner: seq_wr=239464 rand_rd=924917 cpu=123 composite=42341.8"
          },
          {
            "name": "readseq",
            "value": 1384599.5589743878,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2548975 ops/sec | factor: 0.543 | P50: 0.2us | P99: 3.7us | P99.9: 7.5us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=239464 rand_rd=924917 cpu=123 composite=42341.8"
          },
          {
            "name": "seekrandom",
            "value": 191005.68984187345,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 351631 ops/sec | factor: 0.543 | P50: 2.5us | P99: 6.4us | P99.9: 15.0us\nthreads: 1 | elapsed: 0.57s | num: 200000 | iterations: 3 | runner: seq_wr=239464 rand_rd=924917 cpu=123 composite=42341.8"
          },
          {
            "name": "prefixscan",
            "value": 103544.48815212664,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 190620 ops/sec | factor: 0.543 | P50: 4.9us | P99: 6.9us | P99.9: 18.0us\nthreads: 1 | elapsed: 1.05s | num: 200000 | iterations: 3 | runner: seq_wr=239464 rand_rd=924917 cpu=123 composite=42341.8"
          },
          {
            "name": "overwrite",
            "value": 639714.8419517766,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1177681 ops/sec | factor: 0.543 | P50: 0.7us | P99: 2.6us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=239464 rand_rd=924917 cpu=123 composite=42341.8"
          },
          {
            "name": "mergerandom",
            "value": 380039.86591234076,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 699633 ops/sec | factor: 0.543 | P50: 0.3us | P99: 1.9us | P99.9: 3.2us\nthreads: 1 | elapsed: 0.29s | num: 200000 | iterations: 3 | runner: seq_wr=239464 rand_rd=924917 cpu=123 composite=42341.8"
          },
          {
            "name": "readwhilewriting",
            "value": 254420.498847161,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 468375 ops/sec | factor: 0.543 | P50: 2.0us | P99: 4.1us | P99.9: 13.9us\nthreads: 1 | elapsed: 0.43s | num: 200000 | iterations: 3 | runner: seq_wr=239464 rand_rd=924917 cpu=123 composite=42341.8"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "617a2f49557232f72a455b411dc7ea275637bf3e",
          "message": "refactor(table): make block decoder trailer validation fallible (#199)\n\n## Summary\n\n- Introduce `Decoder::try_new` that returns `Error::InvalidTrailer` on\nundersized blocks, zero `restart_interval`, invalid\n`binary_index_step_size`, or corrupt binary/hash-index layout metadata\n- Add `Trailer::try_new` size guard to prevent underflow on truncated\nblocks; `Trailer::new` delegates to `try_new` (panic instead of UB)\n- Add `DataBlock::try_iter` / `IndexBlock::try_iter` fallible wrappers\nand wire all table-reader validation paths through them\n- `FullBlockIndex::new` validates block type and trailer at construction\ntime; the pinned filter index does the same at load time\n- `DataBlock::point_read` returns `Result<Option<InternalValue>>`\n(breaking change from `Option<InternalValue>`) so trailer corruption is\nsurfaced instead of silently skipped\n- `Scanner`, `table::Iter`, `TwoLevelBlockIndex::Iter`, and\n`VolatileBlockIndex::Iter` are poisoned on all error paths so callers\ncannot silently skip corrupt blocks\n- Add corruption regression tests that tamper trailer fields and assert\nstructured error (no panic)\n\n## Test plan\n\n- [x] 1026/1026 tests pass with `cargo nextest run`\n- [x] 8 corruption regression tests verify `Error::InvalidTrailer` for\ntampered blocks\n- [x] `cargo clippy --all-features -- -D warnings` passes\n- [x] `cargo fmt --all -- --check` passes\n\n## Related\n\n- #193 — make meta block field reads fallible (out of scope for this PR)\n- #194 — two-level index scan stops prematurely on empty child\npartitions (pre-existing, out of scope)\n- #195 — blob file metadata corruption regression test (out of scope)\n- #196 — make index block bound-cursor helpers fallible (pre-existing\nAPI, out of scope)\n- #197 — add infallible OwnedIndexBlockIter constructor for\npre-validated blocks (out of scope)\n- #198 — validate block type on cache-hit path (pre-existing, out of\nscope)\n\nSupersedes #191 (closed — too many review threads).\n\nCloses #184\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **Bug Fixes**\n* Stronger validation rejects malformed/corrupt block trailers, indexes,\nand metadata earlier, returning clear errors instead of silently\nfailing.\n* Iteration, scanning and readers now surface the first encountered\nerror, halt further reads, and avoid silent data loss.\n* Point-reads and recovery now propagate decoding errors rather than\ntreating corrupted data as “not found.”\n\n* **Tests**\n* Added regression tests to assert invalid trailers, corrupted layouts,\nand related error paths are detected and reported.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-04-02T22:11:20+03:00",
          "tree_id": "b735954b36fc565d76c7056d9887d5e2cb0c2d4f",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/617a2f49557232f72a455b411dc7ea275637bf3e"
        },
        "date": 1775157159788,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1177142.7219108867,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2087989 ops/sec | factor: 0.564 | P50: 0.3us | P99: 1.9us | P99.9: 4.9us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=217874 rand_rd=904919 cpu=123 composite=40796.9"
          },
          {
            "name": "fillrandom",
            "value": 659267.3531171128,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1169393 ops/sec | factor: 0.564 | P50: 0.7us | P99: 2.5us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=217874 rand_rd=904919 cpu=123 composite=40796.9"
          },
          {
            "name": "readrandom",
            "value": 299141.4929523825,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 530610 ops/sec | factor: 0.564 | P50: 1.7us | P99: 5.1us | P99.9: 13.2us\nthreads: 1 | elapsed: 0.38s | num: 200000 | iterations: 3 | runner: seq_wr=217874 rand_rd=904919 cpu=123 composite=40796.9"
          },
          {
            "name": "readseq",
            "value": 1462483.2581787338,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2594119 ops/sec | factor: 0.564 | P50: 0.2us | P99: 3.8us | P99.9: 7.8us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=217874 rand_rd=904919 cpu=123 composite=40796.9"
          },
          {
            "name": "seekrandom",
            "value": 202446.01625093337,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 359094 ops/sec | factor: 0.564 | P50: 2.4us | P99: 6.0us | P99.9: 14.0us\nthreads: 1 | elapsed: 0.56s | num: 200000 | iterations: 3 | runner: seq_wr=217874 rand_rd=904919 cpu=123 composite=40796.9"
          },
          {
            "name": "prefixscan",
            "value": 107022.82697559228,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 189835 ops/sec | factor: 0.564 | P50: 4.9us | P99: 7.1us | P99.9: 16.3us\nthreads: 1 | elapsed: 1.05s | num: 200000 | iterations: 3 | runner: seq_wr=217874 rand_rd=904919 cpu=123 composite=40796.9"
          },
          {
            "name": "overwrite",
            "value": 678599.6523813711,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1203684 ops/sec | factor: 0.564 | P50: 0.7us | P99: 2.5us | P99.9: 6.1us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=217874 rand_rd=904919 cpu=123 composite=40796.9"
          },
          {
            "name": "mergerandom",
            "value": 416274.22632071265,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 738378 ops/sec | factor: 0.564 | P50: 0.3us | P99: 1.9us | P99.9: 3.4us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=217874 rand_rd=904919 cpu=123 composite=40796.9"
          },
          {
            "name": "readwhilewriting",
            "value": 271257.4146247556,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 481150 ops/sec | factor: 0.564 | P50: 1.9us | P99: 5.0us | P99.9: 13.6us\nthreads: 1 | elapsed: 0.42s | num: 200000 | iterations: 3 | runner: seq_wr=217874 rand_rd=904919 cpu=123 composite=40796.9"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fda0fb2068e48ea1330c03d2525c79b18a74c16d",
          "message": "fix(table): two-level index scan stops prematurely on empty child partitions (#202)\n\n## Summary\n\n- Fix `TwoLevelBlockIndex::Iter` to skip empty child partitions instead\nof terminating the scan\n- When `from_block_with_bounds` returns `Ok(None)` (child partition\ntrimmed empty by lo/hi bounds), `next()` and `next_back()` now\n`continue` to the next TLI entry instead of `return None`\n- Add regression test exercising partitioned index iteration with\nforward, backward, and mixed bounds\n\n## Technical Details\n\nIn `next()` and `next_back()`, the TLI consumption block is wrapped in a\n`loop`. When a child index partition yields no entries after applying\nseek bounds, the iterator advances to the next TLI entry rather than\nstopping. The loop `break`s only when the TLI is exhausted, falling\nthrough to the opposite-side consumer (hi\\_consumer / lo\\_consumer) as\nbefore.\n\nThe empty-child scenario can occur when `seek_upper_bound_cursor`\nreturns false (e.g., restart\\_interval > 1 with coarse-grained trim) or\nwhen `seek_lower` overshoots all entries in a child block. While rare\nwith well-formed data and restart\\_interval=1, the fix is a necessary\ndefensive measure for correctness across all configurations.\n\n## Test Plan\n\n- [x] New test `two_level_index_scan_skips_empty_child_partition`\nvalidates forward, backward, and mixed iteration through a partitioned\nindex with bounds\n- [x] 1027/1027 tests pass (`cargo nextest run`)\n- [x] `cargo clippy --all-features -- -D warnings` clean\n- [x] `cargo fmt -- --check` clean\n\nCloses #194\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **Bug Fixes**\n* Iteration now skips empty child partitions and continues scanning\nacross partition boundaries instead of stopping prematurely, ensuring\ncomplete forward and reverse scans, correct exhaustion behavior, and\nreliable bounded seeks across two-level indexes.\n\n* **Tests**\n* Added a regression test validating full, bounded, and mixed\nforward/reverse iteration over partitioned two-level indexes to prevent\nregressions and ensure correctness.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-04-03T03:23:11+03:00",
          "tree_id": "a056a276cec97d902d161cbc365e8594777748d7",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/fda0fb2068e48ea1330c03d2525c79b18a74c16d"
        },
        "date": 1775175884316,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 882380.3069233776,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1954739 ops/sec | factor: 0.451 | P50: 0.4us | P99: 1.8us | P99.9: 3.7us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=363857 rand_rd=1118242 cpu=116 composite=50952.0"
          },
          {
            "name": "fillrandom",
            "value": 464996.6756392288,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1030108 ops/sec | factor: 0.451 | P50: 0.8us | P99: 2.5us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=363857 rand_rd=1118242 cpu=116 composite=50952.0"
          },
          {
            "name": "readrandom",
            "value": 233884.95183375443,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 518126 ops/sec | factor: 0.451 | P50: 1.7us | P99: 5.7us | P99.9: 12.8us\nthreads: 1 | elapsed: 0.39s | num: 200000 | iterations: 3 | runner: seq_wr=363857 rand_rd=1118242 cpu=116 composite=50952.0"
          },
          {
            "name": "readseq",
            "value": 1486068.0640643828,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 3292090 ops/sec | factor: 0.451 | P50: 0.2us | P99: 3.2us | P99.9: 5.6us\nthreads: 1 | elapsed: 0.06s | num: 200000 | iterations: 3 | runner: seq_wr=363857 rand_rd=1118242 cpu=116 composite=50952.0"
          },
          {
            "name": "seekrandom",
            "value": 168487.21458570092,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 373250 ops/sec | factor: 0.451 | P50: 2.3us | P99: 5.3us | P99.9: 9.9us\nthreads: 1 | elapsed: 0.54s | num: 200000 | iterations: 3 | runner: seq_wr=363857 rand_rd=1118242 cpu=116 composite=50952.0"
          },
          {
            "name": "prefixscan",
            "value": 98636.56806126462,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 218510 ops/sec | factor: 0.451 | P50: 4.2us | P99: 5.1us | P99.9: 11.5us\nthreads: 1 | elapsed: 0.92s | num: 200000 | iterations: 3 | runner: seq_wr=363857 rand_rd=1118242 cpu=116 composite=50952.0"
          },
          {
            "name": "overwrite",
            "value": 475083.6992067352,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1052454 ops/sec | factor: 0.451 | P50: 0.8us | P99: 2.4us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=363857 rand_rd=1118242 cpu=116 composite=50952.0"
          },
          {
            "name": "mergerandom",
            "value": 365883.19797661563,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 810542 ops/sec | factor: 0.451 | P50: 0.4us | P99: 1.6us | P99.9: 3.7us\nthreads: 1 | elapsed: 0.25s | num: 200000 | iterations: 3 | runner: seq_wr=363857 rand_rd=1118242 cpu=116 composite=50952.0"
          },
          {
            "name": "readwhilewriting",
            "value": 203663.77054191285,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 451177 ops/sec | factor: 0.451 | P50: 2.0us | P99: 6.1us | P99.9: 10.8us\nthreads: 1 | elapsed: 0.44s | num: 200000 | iterations: 3 | runner: seq_wr=363857 rand_rd=1118242 cpu=116 composite=50952.0"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f8fe8881b20391188e7bed059c27f35accc3bb9b",
          "message": "fix(table): validate block type on cache-hit path (#203)\n\n## Summary\n\n- Add block type validation on cache-hit path in `load_block()` —\npreviously only the I/O miss path checked `block.header.block_type`, so\na corrupted handle pointing at a cached block of the wrong type would\nslip through silently\n- Remove redundant `cache.get_block()` fast-path in `iter.rs`\nforward/reverse iterators — both now go through `load_block()` which\nalready checks the cache internally and validates block type on both\npaths\n\n## Test plan\n\n- [x] Regression test `load_block_cache_hit_rejects_wrong_block_type`:\ncaches an Index block, requests the same offset as Data, asserts\n`InvalidTag` is returned\n- [x] Full suite: 1028 tests passed\n- [x] Clippy clean\n\nCloses #198\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **Bug Fixes**\n* Consistently validate cached vs on-disk blocks during table reads to\nprevent wrong block types being returned.\n* Preserve existing error handling when validation fails to avoid silent\ndata-mismatch returns.\n\n* **Tests**\n* Added a regression test to ensure cached block type validation and\nprevent future regressions.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-04-03T04:15:52+03:00",
          "tree_id": "c1348477788500ea6114fcd730634fd455fc1d72",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/f8fe8881b20391188e7bed059c27f35accc3bb9b"
        },
        "date": 1775179033030,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1143518.3068946302,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2084042 ops/sec | factor: 0.549 | P50: 0.3us | P99: 2.0us | P99.9: 4.9us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=229580 rand_rd=930753 cpu=123 composite=41917.1"
          },
          {
            "name": "fillrandom",
            "value": 663408.9161535563,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1209051 ops/sec | factor: 0.549 | P50: 0.7us | P99: 2.4us | P99.9: 5.6us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=229580 rand_rd=930753 cpu=123 composite=41917.1"
          },
          {
            "name": "readrandom",
            "value": 294229.29866928805,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 536228 ops/sec | factor: 0.549 | P50: 1.7us | P99: 5.3us | P99.9: 12.8us\nthreads: 1 | elapsed: 0.37s | num: 200000 | iterations: 3 | runner: seq_wr=229580 rand_rd=930753 cpu=123 composite=41917.1"
          },
          {
            "name": "readseq",
            "value": 1384331.488837049,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2522920 ops/sec | factor: 0.549 | P50: 0.2us | P99: 3.8us | P99.9: 7.4us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=229580 rand_rd=930753 cpu=123 composite=41917.1"
          },
          {
            "name": "seekrandom",
            "value": 202718.94304183443,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 369452 ops/sec | factor: 0.549 | P50: 2.4us | P99: 5.9us | P99.9: 13.9us\nthreads: 1 | elapsed: 0.54s | num: 200000 | iterations: 3 | runner: seq_wr=229580 rand_rd=930753 cpu=123 composite=41917.1"
          },
          {
            "name": "prefixscan",
            "value": 104468.8904540609,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 190393 ops/sec | factor: 0.549 | P50: 4.9us | P99: 7.2us | P99.9: 16.4us\nthreads: 1 | elapsed: 1.05s | num: 200000 | iterations: 3 | runner: seq_wr=229580 rand_rd=930753 cpu=123 composite=41917.1"
          },
          {
            "name": "overwrite",
            "value": 660524.428555056,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1203794 ops/sec | factor: 0.549 | P50: 0.7us | P99: 2.5us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=229580 rand_rd=930753 cpu=123 composite=41917.1"
          },
          {
            "name": "mergerandom",
            "value": 399772.91349782486,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 728579 ops/sec | factor: 0.549 | P50: 0.3us | P99: 0.5us | P99.9: 2.4us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=229580 rand_rd=930753 cpu=123 composite=41917.1"
          },
          {
            "name": "readwhilewriting",
            "value": 259461.63956831733,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 472864 ops/sec | factor: 0.549 | P50: 1.9us | P99: 5.1us | P99.9: 13.4us\nthreads: 1 | elapsed: 0.42s | num: 200000 | iterations: 3 | runner: seq_wr=229580 rand_rd=930753 cpu=123 composite=41917.1"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d20c47488fc747baa6c047530fec50919bac5756",
          "message": "refactor(table): make all meta/trailer reads fallible for truncated blocks (#204)\n\n## Summary\n\n- Replace all `expect`/`assert_eq!`/`unwrap_or_else(panic!)` in\n`TableMeta::load_with_handle` with\n`.ok_or(Error::InvalidHeader(\"TableMeta\"))?` — missing or malformed meta\nfields now return structured errors instead of panicking\n- Replace `unwrap!` macro calls in `Decoder::try_new` trailer reads with\n`.map_err(|_| Error::InvalidTrailer)?` for defense-in-depth\n- Add corruption regression test for blob file metadata trailer (#195)\n\n## Test plan\n\n- [x] 5 new `TableMeta` tests: valid roundtrip + 4 corruption variants\n(missing `table_version`, wrong version, missing `key#min`, missing\n`compression#data`)\n- [x] 1 new `BlobFileMeta` test: corrupted trailer bytes → `Err`\n- [x] Full suite: 1034 tests passed, 34 doc-tests passed, clippy clean\n\nCloses #201\nSupersedes #192, #193, #195\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **Bug Fixes**\n* Strengthened parsing of table and block metadata so malformed or\ntruncated header/trailer data now return explicit errors instead of\ncausing panics.\n\n* **Tests**\n* Added regression tests that corrupt metadata/trailer bytes to verify\nerror returns and removal of panic paths.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-04-03T10:17:22+03:00",
          "tree_id": "e6a18b71944e3f6a2eb9aa5d6fe841ee6741cb7d",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/d20c47488fc747baa6c047530fec50919bac5756"
        },
        "date": 1775200728831,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1121926.6140176652,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2016701 ops/sec | factor: 0.556 | P50: 0.4us | P99: 2.0us | P99.9: 5.0us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=220417 rand_rd=927627 cpu=123 composite=41343.3"
          },
          {
            "name": "fillrandom",
            "value": 672978.173008736,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1209701 ops/sec | factor: 0.556 | P50: 0.7us | P99: 2.4us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=220417 rand_rd=927627 cpu=123 composite=41343.3"
          },
          {
            "name": "readrandom",
            "value": 312918.14218303247,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 562481 ops/sec | factor: 0.556 | P50: 1.6us | P99: 5.1us | P99.9: 13.0us\nthreads: 1 | elapsed: 0.36s | num: 200000 | iterations: 3 | runner: seq_wr=220417 rand_rd=927627 cpu=123 composite=41343.3"
          },
          {
            "name": "readseq",
            "value": 1421670.3243003474,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2555501 ops/sec | factor: 0.556 | P50: 0.2us | P99: 3.7us | P99.9: 7.6us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=220417 rand_rd=927627 cpu=123 composite=41343.3"
          },
          {
            "name": "seekrandom",
            "value": 207320.14876824451,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 372665 ops/sec | factor: 0.556 | P50: 2.3us | P99: 6.0us | P99.9: 13.7us\nthreads: 1 | elapsed: 0.54s | num: 200000 | iterations: 3 | runner: seq_wr=220417 rand_rd=927627 cpu=123 composite=41343.3"
          },
          {
            "name": "prefixscan",
            "value": 102770.32844409894,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 184733 ops/sec | factor: 0.556 | P50: 5.0us | P99: 7.4us | P99.9: 16.8us\nthreads: 1 | elapsed: 1.08s | num: 200000 | iterations: 3 | runner: seq_wr=220417 rand_rd=927627 cpu=123 composite=41343.3"
          },
          {
            "name": "overwrite",
            "value": 659725.2116320718,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1185879 ops/sec | factor: 0.556 | P50: 0.7us | P99: 2.5us | P99.9: 6.2us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=220417 rand_rd=927627 cpu=123 composite=41343.3"
          },
          {
            "name": "mergerandom",
            "value": 416907.74379717454,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 749406 ops/sec | factor: 0.556 | P50: 0.3us | P99: 1.9us | P99.9: 3.7us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=220417 rand_rd=927627 cpu=123 composite=41343.3"
          },
          {
            "name": "readwhilewriting",
            "value": 268170.82974465756,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 482046 ops/sec | factor: 0.556 | P50: 1.8us | P99: 7.2us | P99.9: 14.5us\nthreads: 1 | elapsed: 0.41s | num: 200000 | iterations: 3 | runner: seq_wr=220417 rand_rd=927627 cpu=123 composite=41343.3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "37f46dbc1811ed731784a2d633382a6b7ee5cfbe",
          "message": "refactor(table): make index block bound-cursor helpers fallible (#205)\n\n## Summary\n\n- `seek_upper_impl` now returns `Result<bool>`: the poisoned/clamped\ncursor case (empty stack after `advance_upper_restart_interval` hit\ncorrupt data in a compressed index block) returns `Err(InvalidTrailer)`\ninstead of `false`\n- `seek_upper_bound_cursor` propagates the error via `?` — corruption in\nbounded scans is surfaced to callers instead of silently treated as\n\"empty range\" (`Ok(None)`)\n- `seek_lower_bound_cursor` returns `Result<bool>` as API plumbing\n(inner `seek_with_cache_resets` doesn't yet have a distinguishable\ncorruption path)\n- `from_block_with_bounds` propagates both via `?`, removing the\n`TODO(#196)` comment\n- `seek_upper` (public, returns `bool`) uses `unwrap_or(false)` for\nbackward compatibility\n\n## Test plan\n\n- [x] New: `seek_upper_bound_cursor_returns_err_on_poisoned_cursor` —\ncorrupt second interval triggers `Err(InvalidTrailer)`\n- [x] `cargo clippy --all-targets --all-features -- -D warnings` — clean\n- [x] `cargo nextest run --all-features` — 1137 passed, 0 failed\n- [x] `cargo test --doc` — 34 passed\n\nCloses #196\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n\n## Summary by CodeRabbit\n\n* **Bug Fixes**\n* Improved error handling for corrupted data in table indexes; errors\nfrom cursor operations now properly propagate as failures instead of\nbeing silently collapsed into empty range interpretations.\n* Added validation for poisoned cursors and invalid data trailers to\nprevent silent failures during index traversal.\n\n* **Tests**\n* Added test for detecting corrupted index blocks with invalid trailers.\n\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-04-03T12:05:34+03:00",
          "tree_id": "211c4a642170838dded7e47f2d759df541d5a876",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/37f46dbc1811ed731784a2d633382a6b7ee5cfbe"
        },
        "date": 1775207224036,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1189743.4929203328,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2124777 ops/sec | factor: 0.560 | P50: 0.3us | P99: 2.0us | P99.9: 4.9us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=214253 rand_rd=931910 cpu=123 composite=41076.0"
          },
          {
            "name": "fillrandom",
            "value": 660573.280446794,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1179726 ops/sec | factor: 0.560 | P50: 0.7us | P99: 2.6us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=214253 rand_rd=931910 cpu=123 composite=41076.0"
          },
          {
            "name": "readrandom",
            "value": 307250.9795774671,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 548723 ops/sec | factor: 0.560 | P50: 1.6us | P99: 5.1us | P99.9: 13.0us\nthreads: 1 | elapsed: 0.36s | num: 200000 | iterations: 3 | runner: seq_wr=214253 rand_rd=931910 cpu=123 composite=41076.0"
          },
          {
            "name": "readseq",
            "value": 1412975.0506888425,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2523449 ops/sec | factor: 0.560 | P50: 0.2us | P99: 3.8us | P99.9: 7.7us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=214253 rand_rd=931910 cpu=123 composite=41076.0"
          },
          {
            "name": "seekrandom",
            "value": 208799.778474593,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 372898 ops/sec | factor: 0.560 | P50: 2.4us | P99: 5.9us | P99.9: 14.0us\nthreads: 1 | elapsed: 0.54s | num: 200000 | iterations: 3 | runner: seq_wr=214253 rand_rd=931910 cpu=123 composite=41076.0"
          },
          {
            "name": "prefixscan",
            "value": 102318.18733746017,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 182731 ops/sec | factor: 0.560 | P50: 5.1us | P99: 7.6us | P99.9: 17.6us\nthreads: 1 | elapsed: 1.09s | num: 200000 | iterations: 3 | runner: seq_wr=214253 rand_rd=931910 cpu=123 composite=41076.0"
          },
          {
            "name": "overwrite",
            "value": 664381.017897691,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1186526 ops/sec | factor: 0.560 | P50: 0.7us | P99: 2.4us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=214253 rand_rd=931910 cpu=123 composite=41076.0"
          },
          {
            "name": "mergerandom",
            "value": 392916.5303822096,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 701714 ops/sec | factor: 0.560 | P50: 0.3us | P99: 0.6us | P99.9: 2.9us\nthreads: 1 | elapsed: 0.29s | num: 200000 | iterations: 3 | runner: seq_wr=214253 rand_rd=931910 cpu=123 composite=41076.0"
          },
          {
            "name": "readwhilewriting",
            "value": 269368.7885515463,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 481069 ops/sec | factor: 0.560 | P50: 1.9us | P99: 4.1us | P99.9: 13.7us\nthreads: 1 | elapsed: 0.42s | num: 200000 | iterations: 3 | runner: seq_wr=214253 rand_rd=931910 cpu=123 composite=41076.0"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7d1349ea6b86af418d42eb25c064eed031f7a1f2",
          "message": "perf(table): add infallible OwnedIndexBlockIter constructor for pre-validated blocks (#206)\n\n## Summary\n\n- Add `OwnedIndexBlockIter::from_validated_block` — an infallible\nconstructor that skips trailer validation for callers that have already\nvalidated the block\n- Use it from `FullBlockIndex::iter()`, which validates the trailer at\nconstruction time, eliminating redundant work and the `expect` on the\nhot path\n\n## Test plan\n\n- [x] New test `from_validated_block_iterates_all_entries` verifies\ncorrectness\n- [x] All 1036 existing tests pass\n- [x] Clippy clean\n\nCloses #197\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **Refactor**\n* Improved block-index iteration by removing explicit panic paths and\nusing a validated construction path for safer, infallible iterator\ncreation.\n\n* **Tests**\n* Added a unit test that pre-validates blocks and verifies iteration\nyields all entries in order.\n\n* **Style**\n* Minor test/import reformatting and comment reflowing (no behavior\nchanges).\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-04-03T12:45:06+03:00",
          "tree_id": "c6e49c6a6257eb449edd5d05ea6318f2c3938652",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/7d1349ea6b86af418d42eb25c064eed031f7a1f2"
        },
        "date": 1775209586089,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1153151.9210478074,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2116620 ops/sec | factor: 0.545 | P50: 0.3us | P99: 1.9us | P99.9: 4.9us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=231769 rand_rd=943197 cpu=122 composite=42216.7"
          },
          {
            "name": "fillrandom",
            "value": 653001.1792087426,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1198589 ops/sec | factor: 0.545 | P50: 0.7us | P99: 2.5us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=231769 rand_rd=943197 cpu=122 composite=42216.7"
          },
          {
            "name": "readrandom",
            "value": 306750.82456667925,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 563044 ops/sec | factor: 0.545 | P50: 1.6us | P99: 5.0us | P99.9: 12.8us\nthreads: 1 | elapsed: 0.36s | num: 200000 | iterations: 3 | runner: seq_wr=231769 rand_rd=943197 cpu=122 composite=42216.7"
          },
          {
            "name": "readseq",
            "value": 1356019.0143763567,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2488985 ops/sec | factor: 0.545 | P50: 0.2us | P99: 3.7us | P99.9: 7.4us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=231769 rand_rd=943197 cpu=122 composite=42216.7"
          },
          {
            "name": "seekrandom",
            "value": 191994.42011819407,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 352407 ops/sec | factor: 0.545 | P50: 2.5us | P99: 6.1us | P99.9: 14.2us\nthreads: 1 | elapsed: 0.57s | num: 200000 | iterations: 3 | runner: seq_wr=231769 rand_rd=943197 cpu=122 composite=42216.7"
          },
          {
            "name": "prefixscan",
            "value": 98591.6388815067,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 180966 ops/sec | factor: 0.545 | P50: 5.2us | P99: 6.8us | P99.9: 17.6us\nthreads: 1 | elapsed: 1.11s | num: 200000 | iterations: 3 | runner: seq_wr=231769 rand_rd=943197 cpu=122 composite=42216.7"
          },
          {
            "name": "overwrite",
            "value": 667980.8595552227,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1226085 ops/sec | factor: 0.545 | P50: 0.7us | P99: 2.6us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.16s | num: 200000 | iterations: 3 | runner: seq_wr=231769 rand_rd=943197 cpu=122 composite=42216.7"
          },
          {
            "name": "mergerandom",
            "value": 318989.12280811724,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 585507 ops/sec | factor: 0.545 | P50: 0.3us | P99: 1.9us | P99.9: 4.3us\nthreads: 1 | elapsed: 0.34s | num: 200000 | iterations: 3 | runner: seq_wr=231769 rand_rd=943197 cpu=122 composite=42216.7"
          },
          {
            "name": "readwhilewriting",
            "value": 257154.28586125583,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 472009 ops/sec | factor: 0.545 | P50: 1.9us | P99: 4.1us | P99.9: 13.1us\nthreads: 1 | elapsed: 0.42s | num: 200000 | iterations: 3 | runner: seq_wr=231769 rand_rd=943197 cpu=122 composite=42216.7"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ea4c28f4ce1a2c19e77200d4ba2cfd207c58e025",
          "message": "feat(fs): MemFs — in-memory Fs implementation for testing and in-memory trees (#211)\n\n## Summary\n\n- Implement `MemFs` + `MemFile` — `HashMap<PathBuf,\nArc<Mutex<Vec<u8>>>>`-backed virtual filesystem implementing the `Fs`\ntrait\n- De-generify `Config<F: Fs>` → `Config` with `Arc<dyn Fs>` field; add\n`with_fs()` and `with_shared_fs()` builders\n- Rewrite `rewrite_atomic` to use `Fs` trait (PID+seq temp naming, retry\non `AlreadyExists`, best-effort cleanup)\n- Pipe `Arc<dyn Fs>` through `Table::recover`, `FileAccessor`,\n`load_block`, and vlog paths\n- Wire correct level-routed `Fs` to all `Table::recover` call sites\n- `FileAccessor::Closed` sentinel for safe handle release in\n`Inner::Drop` (Windows safety)\n- Restore `table_file_opened_cached`/`uncached` metrics via\n`Option<bool>` return from `FileAccessor`\n- Harden blob recovery: fail-fast on missing blobs folder when manifest\nreferences blob files; defer descriptor-table inserts until recovery\ncommits\n- Replace all unchecked `as usize` casts with `usize::try_from` in\n`MemFile`\n- Bump `rust-toolchain.toml` 1.94.0 → 1.94.1\n\n## Design Notes\n\n- **Blob recovery `NotFound` handling:** `recover_blob_files` returns\n`Ok([], [])` when the blobs folder doesn't exist **and** `ids` is empty\n(standard non-blob trees). When `ids` is non-empty (manifest references\nblob files), a missing folder is unrecoverable corruption and returns\n`Err(Unrecoverable)`.\n- **`lock_exclusive` no-op in MemFs:** In-memory files are not shared\nacross processes — cross-process exclusivity is not meaningful.\n- **`fs` field:** Default filesystem backend for levels without an\nexplicit route. Per-level routing is separate.\n\n## Known Limitations\n\n- **Tree reopen**: `Tree::open` uses `try_exists()` and `std::fs`-based\nrecovery, bypassing the `Fs` trait. New trees work; reopening in-memory\ntrees is not supported. Tracked in #209.\n- **Version GC**: `SuperVersions::gc` uses `std::fs` directly. Tracked\nin #209.\n- **Compaction**: Some finalization code paths still bypass the `Fs`\ntrait.\n\n## Test Plan\n\n- [x] 30 MemFs unit tests (all Fs/FsFile methods, wrong-type errors,\npermission guards, empty paths, rename-replace)\n- [x] 7 integration tests (tree open, flush+read, delete+range, multiple\nflushes, shared MemFs, vlog recovery)\n- [x] StdFs rename-replace contract test\n- [x] 1081 tests pass (0 regressions)\n- [x] Clippy clean, `cargo fmt -- --check` clean\n\nCloses #187\nCloses #188\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **New Features**\n  * In-memory filesystem backend for ephemeral trees and testing.\n  * Config builder APIs to supply a custom or shared filesystem backend.\n\n* **Improvements**\n* More reliable atomic file writes (temp-write + fsync + replace) and\nconsistent directory fsync behavior.\n* Unified filesystem handling across recovery, flush, ingest and read\npaths; safer table deletion ordering.\n  * Improved file-open/cache metrics and cache semantics.\n\n* **Bug Fixes**\n  * Prevent partial descriptor-cache population during recovery.\n  * Stricter handling of missing blob folders during recovery.\n\n* **Tests**\n  * Expanded MemFs, recovery, and end-to-end tests.\n\n* **Chores**\n  * Rust toolchain bumped to 1.94.1.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-04-05T03:03:19+03:00",
          "tree_id": "f0e43db0b1f424b824361bc47220bfb484b57790",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/ea4c28f4ce1a2c19e77200d4ba2cfd207c58e025"
        },
        "date": 1775347479050,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1104010.2287566632,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2039875 ops/sec | factor: 0.541 | P50: 0.4us | P99: 2.0us | P99.9: 5.0us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=237500 rand_rd=939197 cpu=123 composite=42497.0"
          },
          {
            "name": "fillrandom",
            "value": 660312.8617661444,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1220057 ops/sec | factor: 0.541 | P50: 0.7us | P99: 2.4us | P99.9: 5.6us\nthreads: 1 | elapsed: 0.16s | num: 200000 | iterations: 3 | runner: seq_wr=237500 rand_rd=939197 cpu=123 composite=42497.0"
          },
          {
            "name": "readrandom",
            "value": 299956.0730026213,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 554228 ops/sec | factor: 0.541 | P50: 1.6us | P99: 5.3us | P99.9: 13.2us\nthreads: 1 | elapsed: 0.36s | num: 200000 | iterations: 3 | runner: seq_wr=237500 rand_rd=939197 cpu=123 composite=42497.0"
          },
          {
            "name": "readseq",
            "value": 1329586.289860147,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2456671 ops/sec | factor: 0.541 | P50: 0.3us | P99: 3.8us | P99.9: 7.4us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=237500 rand_rd=939197 cpu=123 composite=42497.0"
          },
          {
            "name": "seekrandom",
            "value": 205072.39741126183,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 378911 ops/sec | factor: 0.541 | P50: 2.3us | P99: 6.1us | P99.9: 13.8us\nthreads: 1 | elapsed: 0.53s | num: 200000 | iterations: 3 | runner: seq_wr=237500 rand_rd=939197 cpu=123 composite=42497.0"
          },
          {
            "name": "prefixscan",
            "value": 98166.34774632522,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 181382 ops/sec | factor: 0.541 | P50: 5.1us | P99: 6.7us | P99.9: 16.8us\nthreads: 1 | elapsed: 1.10s | num: 200000 | iterations: 3 | runner: seq_wr=237500 rand_rd=939197 cpu=123 composite=42497.0"
          },
          {
            "name": "overwrite",
            "value": 674990.56487895,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1247177 ops/sec | factor: 0.541 | P50: 0.6us | P99: 2.4us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.16s | num: 200000 | iterations: 3 | runner: seq_wr=237500 rand_rd=939197 cpu=123 composite=42497.0"
          },
          {
            "name": "mergerandom",
            "value": 359729.11247182876,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 664670 ops/sec | factor: 0.541 | P50: 0.3us | P99: 1.8us | P99.9: 2.8us\nthreads: 1 | elapsed: 0.30s | num: 200000 | iterations: 3 | runner: seq_wr=237500 rand_rd=939197 cpu=123 composite=42497.0"
          },
          {
            "name": "readwhilewriting",
            "value": 272114.0208921818,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 502784 ops/sec | factor: 0.541 | P50: 1.8us | P99: 4.0us | P99.9: 13.0us\nthreads: 1 | elapsed: 0.40s | num: 200000 | iterations: 3 | runner: seq_wr=237500 rand_rd=939197 cpu=123 composite=42497.0"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5b6eed969c6e650b2966ab19b96aa9a968c995d7",
          "message": "refactor(fs): migrate Tree::open recovery path to Fs trait (#212)\n\n## Summary\n\n- Replace all `try_exists()` / `std::fs` calls in the recovery path with\npluggable `Fs` trait methods, enabling MemFs and future non-StdFs\nbackends to reopen trees\n- Add `open_section_reader()` helper to replace\n`sfa::TocEntry::buf_reader()` which used `std::fs` directly\n- Thread `&dyn Fs` through `recover()`, `get_current_version()`,\n`cleanup_orphaned_version()`, `SuperVersions::maintenance()`, and\n`Manifest::decode_from()`\n- Eliminate TOCTOU race in `Tree::open` — replace `exists()` probe with\natomic read attempt via `get_current_version()`\n- Eliminate TOCTOU race in version GC — replace `exists()` +\n`remove_file()` with idempotent `remove_file()` that treats `NotFound`\nas success (both `SuperVersions::maintenance` and\n`cleanup_orphaned_version`)\n- Validate `table_count` and `blob_file_count` against section length in\nrecovery to prevent allocation-based DoS from corrupt version files\n- Validate CURRENT file checksum type before trusting version_id —\ndetects torn/corrupt pointer files\n- Document `NotFound` contract on `Fs::remove_file`\n\n## Behavior change\n\n`cleanup_orphaned_version` now fails fast on non-UTF-8 filenames (via\n`Fs::read_dir` returning `InvalidData`) instead of silently skipping\nthem with `to_string_lossy()`. This is intentional — version files are\nalways `v{u64}`, so non-UTF-8 entries indicate filesystem corruption and\nshould surface as an error.\n\n## Test plan\n\n- [x] All 1087 tests pass (existing + new)\n- [x] 36 doc-tests pass\n- [x] Clippy clean (0 warnings)\n- [x] MemFs round-trip: create → write → flush → drop → reopen → read\n- [x] MemFs manifest decode\n- [x] GC tests with seeded version files and distinct IDs — verify\nactual file deletion\n- [x] Non-UTF-8 filename regression test (Unix-only, platform-specific\nerror handling)\n- [x] Corruption tests: corrupt `table_count` and `blob_file_count` →\n`Unrecoverable`\n- [x] Maintenance tests isolated from working directory (MemFs)\n\nCloses #209\nCloses #213\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **Bug Fixes**\n* Recovery now detects corrupted manifests and rejects extreme/corrupt\ncounts; non‑UTF‑8 filename errors during reopen are surfaced correctly.\n* Version/manifest GC deletes via the configured storage backend and\ntreats missing files as no‑ops.\n\n* **Refactor**\n* Core I/O, manifest decoding and version recovery consistently use the\nconfigured, pluggable filesystem backend.\n\n* **Tests**\n* Added in‑memory and Unix regression tests for reopen, recovery, GC,\nand corrupt manifest scenarios.\n\n* **Documentation**\n* Removed docs claiming reopening was limited to the default filesystem.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-04-05T16:17:29+03:00",
          "tree_id": "2214e4972656c27b8d1dcec2ee99838a462efce4",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/5b6eed969c6e650b2966ab19b96aa9a968c995d7"
        },
        "date": 1775395131691,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1092125.7943618628,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1996134 ops/sec | factor: 0.547 | P50: 0.4us | P99: 2.0us | P99.9: 4.9us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=227142 rand_rd=945580 cpu=123 composite=42038.3"
          },
          {
            "name": "fillrandom",
            "value": 662287.7427869305,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1210497 ops/sec | factor: 0.547 | P50: 0.7us | P99: 2.4us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=227142 rand_rd=945580 cpu=123 composite=42038.3"
          },
          {
            "name": "readrandom",
            "value": 301573.1739548904,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 551201 ops/sec | factor: 0.547 | P50: 1.6us | P99: 5.2us | P99.9: 12.9us\nthreads: 1 | elapsed: 0.36s | num: 200000 | iterations: 3 | runner: seq_wr=227142 rand_rd=945580 cpu=123 composite=42038.3"
          },
          {
            "name": "readseq",
            "value": 1390008.0909630626,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2540589 ops/sec | factor: 0.547 | P50: 0.2us | P99: 3.8us | P99.9: 7.4us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=227142 rand_rd=945580 cpu=123 composite=42038.3"
          },
          {
            "name": "seekrandom",
            "value": 207838.6852551498,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 379877 ops/sec | factor: 0.547 | P50: 2.3us | P99: 5.8us | P99.9: 13.7us\nthreads: 1 | elapsed: 0.53s | num: 200000 | iterations: 3 | runner: seq_wr=227142 rand_rd=945580 cpu=123 composite=42038.3"
          },
          {
            "name": "prefixscan",
            "value": 96882.33379206192,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 177077 ops/sec | factor: 0.547 | P50: 5.3us | P99: 7.4us | P99.9: 17.1us\nthreads: 1 | elapsed: 1.13s | num: 200000 | iterations: 3 | runner: seq_wr=227142 rand_rd=945580 cpu=123 composite=42038.3"
          },
          {
            "name": "overwrite",
            "value": 665022.107397812,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1215495 ops/sec | factor: 0.547 | P50: 0.7us | P99: 2.5us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.16s | num: 200000 | iterations: 3 | runner: seq_wr=227142 rand_rd=945580 cpu=123 composite=42038.3"
          },
          {
            "name": "mergerandom",
            "value": 405206.53738245915,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 740617 ops/sec | factor: 0.547 | P50: 0.3us | P99: 1.8us | P99.9: 3.3us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=227142 rand_rd=945580 cpu=123 composite=42038.3"
          },
          {
            "name": "readwhilewriting",
            "value": 261563.26267758454,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 478073 ops/sec | factor: 0.547 | P50: 1.9us | P99: 5.0us | P99.9: 14.0us\nthreads: 1 | elapsed: 0.42s | num: 200000 | iterations: 3 | runner: seq_wr=227142 rand_rd=945580 cpu=123 composite=42038.3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "255865126+sw-release-bot[bot]@users.noreply.github.com",
            "name": "sw-release-bot[bot]",
            "username": "sw-release-bot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "cf5d5bd429a166afbca85ce7580603ae5ddd80ce",
          "message": "chore: release v4.3.0 (#200)\n\n## 🤖 New release\n\n* `coordinode-lsm-tree`: 4.2.0 -> 4.3.0\n\n<details><summary><i><b>Changelog</b></i></summary><p>\n\n<blockquote>\n\n##\n[4.3.0](https://github.com/structured-world/coordinode-lsm-tree/compare/v4.2.0...v4.3.0)\n- 2026-04-05\n\n### Added\n\n- *(fs)* MemFs — in-memory Fs implementation for testing and in-memory\ntrees\n([#211](https://github.com/structured-world/coordinode-lsm-tree/pull/211))\n\n### Fixed\n\n- *(table)* validate block type on cache-hit path\n([#203](https://github.com/structured-world/coordinode-lsm-tree/pull/203))\n- *(table)* two-level index scan stops prematurely on empty child\npartitions\n([#202](https://github.com/structured-world/coordinode-lsm-tree/pull/202))\n\n### Performance\n\n- *(table)* add infallible OwnedIndexBlockIter constructor for\npre-validated blocks\n([#206](https://github.com/structured-world/coordinode-lsm-tree/pull/206))\n\n### Refactored\n\n- *(fs)* migrate Tree::open recovery path to Fs trait\n([#212](https://github.com/structured-world/coordinode-lsm-tree/pull/212))\n- *(table)* make index block bound-cursor helpers fallible\n([#205](https://github.com/structured-world/coordinode-lsm-tree/pull/205))\n- *(table)* make all meta/trailer reads fallible for truncated blocks\n([#204](https://github.com/structured-world/coordinode-lsm-tree/pull/204))\n- *(table)* make block decoder trailer validation fallible\n([#199](https://github.com/structured-world/coordinode-lsm-tree/pull/199))\n</blockquote>\n\n\n</p></details>\n\n---\nThis PR was generated with\n[release-plz](https://github.com/release-plz/release-plz/).\n\nCo-authored-by: sw-release-bot[bot] <255865126+sw-release-bot[bot]@users.noreply.github.com>",
          "timestamp": "2026-04-05T16:26:42+03:00",
          "tree_id": "6df4c9e005b45c62b38f505941003b0649d647ca",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/cf5d5bd429a166afbca85ce7580603ae5ddd80ce"
        },
        "date": 1775395680240,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1148537.5894635925,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2072805 ops/sec | factor: 0.554 | P50: 0.3us | P99: 2.0us | P99.9: 5.0us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=230615 rand_rd=905921 cpu=122 composite=41508.9"
          },
          {
            "name": "fillrandom",
            "value": 663695.9309610558,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1197795 ops/sec | factor: 0.554 | P50: 0.7us | P99: 2.5us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=230615 rand_rd=905921 cpu=122 composite=41508.9"
          },
          {
            "name": "readrandom",
            "value": 311914.1436192461,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 562922 ops/sec | factor: 0.554 | P50: 1.6us | P99: 5.1us | P99.9: 12.6us\nthreads: 1 | elapsed: 0.36s | num: 200000 | iterations: 3 | runner: seq_wr=230615 rand_rd=905921 cpu=122 composite=41508.9"
          },
          {
            "name": "readseq",
            "value": 1336633.087199844,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2412267 ops/sec | factor: 0.554 | P50: 0.3us | P99: 3.7us | P99.9: 7.9us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=230615 rand_rd=905921 cpu=122 composite=41508.9"
          },
          {
            "name": "seekrandom",
            "value": 212174.6412340675,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 382919 ops/sec | factor: 0.554 | P50: 2.3us | P99: 5.9us | P99.9: 13.7us\nthreads: 1 | elapsed: 0.52s | num: 200000 | iterations: 3 | runner: seq_wr=230615 rand_rd=905921 cpu=122 composite=41508.9"
          },
          {
            "name": "prefixscan",
            "value": 99465.51964255307,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 179509 ops/sec | factor: 0.554 | P50: 5.2us | P99: 7.7us | P99.9: 17.2us\nthreads: 1 | elapsed: 1.11s | num: 200000 | iterations: 3 | runner: seq_wr=230615 rand_rd=905921 cpu=122 composite=41508.9"
          },
          {
            "name": "overwrite",
            "value": 687074.7425792207,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1239987 ops/sec | factor: 0.554 | P50: 0.7us | P99: 2.4us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.16s | num: 200000 | iterations: 3 | runner: seq_wr=230615 rand_rd=905921 cpu=122 composite=41508.9"
          },
          {
            "name": "mergerandom",
            "value": 414073.5132640846,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 747293 ops/sec | factor: 0.554 | P50: 0.3us | P99: 0.5us | P99.9: 3.1us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=230615 rand_rd=905921 cpu=122 composite=41508.9"
          },
          {
            "name": "readwhilewriting",
            "value": 283433.1104358807,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 511521 ops/sec | factor: 0.554 | P50: 1.8us | P99: 4.9us | P99.9: 12.9us\nthreads: 1 | elapsed: 0.39s | num: 200000 | iterations: 3 | runner: seq_wr=230615 rand_rd=905921 cpu=122 composite=41508.9"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "6f86575fefc05f76bd7ac18b76656f54b76fc62e",
          "message": "perf: batch multi_get + PinnableSlice + WriteBatch (#214)\n\n## Summary\n\n- **Batch multi_get**: deferred sort+hash after memtable phase, sorted\nkey access for SSTs, L0 seqno ceiling skip, BlobTree batch path with RT\nsuppression\n- **PinnableSlice**: zero-copy `get_pinned()` API — `Pinned` variant\nkeeps decompressed block buffer alive via refcounted `Slice`/`ByteView`,\n`Owned` for memtable/merge/blob values\n- **WriteBatch**: `apply_batch(batch, seqno) -> Result<(u64, u64)>` —\nshared seqno, single lock, `Error::MixedOperationBatch` rejects mixed-op\nduplicates unconditionally\n- **Table::get seqno fix**: returns global seqnos (was table-local for\ningested tables)\n- **Deps**: update `structured-zstd 0.0.1 → 0.0.7\n\n## Technical Details\n\n### Batch multi_get\n- Phase 1 (memtable): unsorted scan — memtable lookup is O(log n)\nregardless of order\n- Phase 2 (SST): sort remaining keys + compute bloom hashes only if\nmemtable misses exist (memtable-only batches skip entirely)\n- L0: Vec<bool> bitmap tracks keys at seqno ceiling (entry_seqno + 1 ==\nread_seqno), skips them in subsequent runs\n- L1+: sorted key access for sequential I/O\n- BlobTree: batch path with sorted keys, bloom hashes, RT suppression\n(was naive per-key loop)\n- Small batches (≤2 keys) use simple per-key path\n- Monomorphized `find_in_tables<T: TablePointLookup>` — zero-cost\ngeneric for `get` (no Block overhead) and `get_pinned` (retains Block)\n\n### PinnableSlice\n- `Pinned { _block: Block, value: Slice }` — block buffer alive via\nrefcount, does NOT prevent cache eviction\n- `resolve_pinned_entry` helper — single source of truth for\ntombstone/RT/merge resolution, used by both `get_pinned` and `multi_get`\n- `Table::point_read_inner` — shared block-index walk for `point_read`\nand `point_read_with_block`\n- `BloomResult` enum — DRY filter-loading with cfg(metrics) gating\n\n### WriteBatch\n- `apply_batch` returns `Result` — `Error::MixedOperationBatch` if same\nuser key has differing op types (insert + remove)\n- Repeated `merge()` on same key is safe (same value_type)\n- `Memtable::insert_batch` — single `saturating_add` for total size\n- Version-history read guard held for entire batch to prevent\n`rotate_memtable` race\n\n### Table::get global seqno\n- `Table::get` and `get_with_block` now add `global_seqno` back to\nreturned `InternalValue` seqno\n- Fixes L0 best-selection and RT suppression for bulk-ingested tables\nwith non-zero `global_seqno`\n- L0 fast-path: `checked_add(1) == Some(seqno)` (was dead code with `==\nseqno`)\n\n## Test Plan\n\n- [x] 1229 tests pass (`cargo nextest run --all-features`)\n- [x] 41 doc-tests pass (`cargo test --doc`)\n- [x] Clippy clean (`cargo clippy --all-targets --all-features -- -D\nwarnings`)\n- [x] Criterion benchmarks: multi_get (10-500 keys), get_pinned vs get,\nwrite_batch vs individual inserts\n- [x] MixedOperationBatch rejection test + repeated merge acceptance\ntest\n\n## Related\n\n- #223 — per-SST batch point-read (Table::batch_get) — follow-up\noptimization\n\nCloses #143\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **New Features**\n* Zero-copy pinned reads and a pinned-read API; WriteBatch for grouped\natomic writes and a bulk memtable insert path.\n\n* **Performance Improvements**\n* Batched multi-get pipeline and optimized table point-lookup for large\nrequests.\n\n* **Bug Fixes**\n* Rejected batches that mix conflicting operations on the same key\n(MixedOperationBatch).\n\n* **Documentation**\n* README clarifies batch-optimized multi_get, pinning, and atomic batch\nvisibility.\n\n* **Tests / Benchmarks**\n* New integration tests and Criterion benchmarks for pinned reads,\nmulti_get, and write batches.\n\n* **Chores**\n  * Dependency version bumped and benchmark target added.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-04-06T15:45:38+03:00",
          "tree_id": "de7ae2d0edea8ef28a544c521e02ab7a74a74cc4",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/6f86575fefc05f76bd7ac18b76656f54b76fc62e"
        },
        "date": 1775479616823,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1216281.4162948378,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1893985 ops/sec | factor: 0.642 | P50: 0.4us | P99: 2.4us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.11s | num: 200000 | iterations: 3 | runner: seq_wr=221559 rand_rd=707347 cpu=108 composite=35815.4"
          },
          {
            "name": "fillrandom",
            "value": 662719.4645972243,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1031982 ops/sec | factor: 0.642 | P50: 0.7us | P99: 3.1us | P99.9: 9.5us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=221559 rand_rd=707347 cpu=108 composite=35815.4"
          },
          {
            "name": "readrandom",
            "value": 294935.4384856734,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 459271 ops/sec | factor: 0.642 | P50: 2.0us | P99: 6.3us | P99.9: 13.6us\nthreads: 1 | elapsed: 0.44s | num: 200000 | iterations: 3 | runner: seq_wr=221559 rand_rd=707347 cpu=108 composite=35815.4"
          },
          {
            "name": "readseq",
            "value": 1471061.0918393773,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2290726 ops/sec | factor: 0.642 | P50: 0.3us | P99: 4.5us | P99.9: 9.1us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=221559 rand_rd=707347 cpu=108 composite=35815.4"
          },
          {
            "name": "seekrandom",
            "value": 213509.50381264018,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 332475 ops/sec | factor: 0.642 | P50: 2.6us | P99: 7.2us | P99.9: 14.9us\nthreads: 1 | elapsed: 0.60s | num: 200000 | iterations: 3 | runner: seq_wr=221559 rand_rd=707347 cpu=108 composite=35815.4"
          },
          {
            "name": "prefixscan",
            "value": 110680.18859462412,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 172350 ops/sec | factor: 0.642 | P50: 5.4us | P99: 7.0us | P99.9: 16.9us\nthreads: 1 | elapsed: 1.16s | num: 200000 | iterations: 3 | runner: seq_wr=221559 rand_rd=707347 cpu=108 composite=35815.4"
          },
          {
            "name": "overwrite",
            "value": 718082.3005311881,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1118193 ops/sec | factor: 0.642 | P50: 0.7us | P99: 3.0us | P99.9: 6.9us\nthreads: 1 | elapsed: 0.18s | num: 200000 | iterations: 3 | runner: seq_wr=221559 rand_rd=707347 cpu=108 composite=35815.4"
          },
          {
            "name": "mergerandom",
            "value": 473897.38234757184,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 737950 ops/sec | factor: 0.642 | P50: 0.4us | P99: 0.6us | P99.9: 3.4us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=221559 rand_rd=707347 cpu=108 composite=35815.4"
          },
          {
            "name": "readwhilewriting",
            "value": 291324.2625736475,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 453648 ops/sec | factor: 0.642 | P50: 2.0us | P99: 5.9us | P99.9: 13.5us\nthreads: 1 | elapsed: 0.44s | num: 200000 | iterations: 3 | runner: seq_wr=221559 rand_rd=707347 cpu=108 composite=35815.4"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7ee6c7c84a2c6bc2ba61b1a6c959d4cb799648b6",
          "message": "perf(compression): use numeric zstd levels in pure Rust backend (#226)\n\n## Summary\n\n- Wire `level` parameter through to `CompressionLevel::from_level()` in\nthe `zstd-pure` backend instead of hardcoding `Fastest`\n- Remove outdated \"Fastest only\" limitation from module docs and README\n- Fix pre-existing `dead_code` warning on `BloomResult::has_filter`\n(metrics-only field) with `cfg_attr(not(feature = \"metrics\"),\nexpect(dead_code))`\n\n## Context\n\nstructured-zstd 0.0.7 (already pinned in Cargo.toml) supports full\ncompression levels 1–22 via `CompressionLevel::from_level(i32)`. The\npure Rust backend was still ignoring the caller's level and hardcoding\n`Fastest` from when numeric levels weren't available.\n\n## Test plan\n\n- [x] `cargo clippy --all-features --all-targets -- -D warnings` — clean\n- [x] `cargo nextest run --features zstd-pure` — 1155 passed, 0 failed\n\nCloses #216\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n\n## Summary by CodeRabbit\n\n* **Bug Fixes**\n* Fixed compression level handling in zstd-pure backend—requested\ncompression levels are now properly applied instead of always using the\nfastest level.\n\n* **Documentation**\n* Updated documentation to clarify that zstd-pure supports compression\nlevels 1–22 without C dependencies, and noted that dictionary\ncompression is not yet supported.\n\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-04-06T17:46:35+03:00",
          "tree_id": "3ccfeb2207c705d03c9cf92b7c88a00de2d20026",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/7ee6c7c84a2c6bc2ba61b1a6c959d4cb799648b6"
        },
        "date": 1775486880303,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 863996.8053267363,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1964016 ops/sec | factor: 0.440 | P50: 0.4us | P99: 1.7us | P99.9: 3.6us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=380158 rand_rd=1146132 cpu=117 composite=52283.0"
          },
          {
            "name": "fillrandom",
            "value": 473282.4000628233,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1075854 ops/sec | factor: 0.440 | P50: 0.8us | P99: 2.4us | P99.9: 5.5us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=380158 rand_rd=1146132 cpu=117 composite=52283.0"
          },
          {
            "name": "readrandom",
            "value": 236559.3180850862,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 537741 ops/sec | factor: 0.440 | P50: 1.7us | P99: 4.6us | P99.9: 9.6us\nthreads: 1 | elapsed: 0.37s | num: 200000 | iterations: 3 | runner: seq_wr=380158 rand_rd=1146132 cpu=117 composite=52283.0"
          },
          {
            "name": "readseq",
            "value": 1446035.9839884874,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 3287092 ops/sec | factor: 0.440 | P50: 0.2us | P99: 3.1us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.06s | num: 200000 | iterations: 3 | runner: seq_wr=380158 rand_rd=1146132 cpu=117 composite=52283.0"
          },
          {
            "name": "seekrandom",
            "value": 174784.9403220498,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 397317 ops/sec | factor: 0.440 | P50: 2.2us | P99: 5.2us | P99.9: 9.6us\nthreads: 1 | elapsed: 0.50s | num: 200000 | iterations: 3 | runner: seq_wr=380158 rand_rd=1146132 cpu=117 composite=52283.0"
          },
          {
            "name": "prefixscan",
            "value": 95286.8825707479,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 216604 ops/sec | factor: 0.440 | P50: 4.3us | P99: 5.3us | P99.9: 10.9us\nthreads: 1 | elapsed: 0.92s | num: 200000 | iterations: 3 | runner: seq_wr=380158 rand_rd=1146132 cpu=117 composite=52283.0"
          },
          {
            "name": "overwrite",
            "value": 491298.80878158857,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1116808 ops/sec | factor: 0.440 | P50: 0.7us | P99: 2.3us | P99.9: 5.0us\nthreads: 1 | elapsed: 0.18s | num: 200000 | iterations: 3 | runner: seq_wr=380158 rand_rd=1146132 cpu=117 composite=52283.0"
          },
          {
            "name": "mergerandom",
            "value": 334166.14787585416,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 759618 ops/sec | factor: 0.440 | P50: 0.4us | P99: 1.5us | P99.9: 2.5us\nthreads: 1 | elapsed: 0.26s | num: 200000 | iterations: 3 | runner: seq_wr=380158 rand_rd=1146132 cpu=117 composite=52283.0"
          },
          {
            "name": "readwhilewriting",
            "value": 209384.02516292565,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 475966 ops/sec | factor: 0.440 | P50: 1.9us | P99: 4.5us | P99.9: 9.9us\nthreads: 1 | elapsed: 0.42s | num: 200000 | iterations: 3 | runner: seq_wr=380158 rand_rd=1146132 cpu=117 composite=52283.0"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "255865126+sw-release-bot[bot]@users.noreply.github.com",
            "name": "sw-release-bot[bot]",
            "username": "sw-release-bot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e0d4113bfdaa724003aff0a22b6e923e19757cb3",
          "message": "chore: release v4.3.1 (#225)\n\n## 🤖 New release\n\n* `coordinode-lsm-tree`: 4.3.0 -> 4.3.1\n\n<details><summary><i><b>Changelog</b></i></summary><p>\n\n<blockquote>\n\n##\n[4.3.1](https://github.com/structured-world/coordinode-lsm-tree/compare/v4.3.0...v4.3.1)\n- 2026-04-06\n\n### Performance\n\n- *(compression)* use numeric zstd levels in pure Rust backend\n([#226](https://github.com/structured-world/coordinode-lsm-tree/pull/226))\n- batch multi_get + PinnableSlice + WriteBatch\n([#214](https://github.com/structured-world/coordinode-lsm-tree/pull/214))\n</blockquote>\n\n\n</p></details>\n\n---\nThis PR was generated with\n[release-plz](https://github.com/release-plz/release-plz/).\n\nCo-authored-by: sw-release-bot[bot] <255865126+sw-release-bot[bot]@users.noreply.github.com>",
          "timestamp": "2026-04-06T18:45:59+03:00",
          "tree_id": "0e64f04d488c9ad5eb97251a94d5182124bb365d",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/e0d4113bfdaa724003aff0a22b6e923e19757cb3"
        },
        "date": 1775490443421,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1167267.3815699755,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2073184 ops/sec | factor: 0.563 | P50: 0.3us | P99: 1.9us | P99.9: 4.9us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=227551 rand_rd=878991 cpu=123 composite=40850.3"
          },
          {
            "name": "fillrandom",
            "value": 625567.1933690886,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1111070 ops/sec | factor: 0.563 | P50: 0.7us | P99: 2.6us | P99.9: 6.3us\nthreads: 1 | elapsed: 0.18s | num: 200000 | iterations: 3 | runner: seq_wr=227551 rand_rd=878991 cpu=123 composite=40850.3"
          },
          {
            "name": "readrandom",
            "value": 269355.70040009846,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 478403 ops/sec | factor: 0.563 | P50: 1.9us | P99: 5.4us | P99.9: 13.9us\nthreads: 1 | elapsed: 0.42s | num: 200000 | iterations: 3 | runner: seq_wr=227551 rand_rd=878991 cpu=123 composite=40850.3"
          },
          {
            "name": "readseq",
            "value": 1374811.324325964,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2441803 ops/sec | factor: 0.563 | P50: 0.3us | P99: 3.8us | P99.9: 8.4us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=227551 rand_rd=878991 cpu=123 composite=40850.3"
          },
          {
            "name": "seekrandom",
            "value": 195446.29516946906,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 347132 ops/sec | factor: 0.563 | P50: 2.5us | P99: 6.1us | P99.9: 14.8us\nthreads: 1 | elapsed: 0.58s | num: 200000 | iterations: 3 | runner: seq_wr=227551 rand_rd=878991 cpu=123 composite=40850.3"
          },
          {
            "name": "prefixscan",
            "value": 102410.08907083406,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 181891 ops/sec | factor: 0.563 | P50: 5.1us | P99: 7.9us | P99.9: 18.5us\nthreads: 1 | elapsed: 1.10s | num: 200000 | iterations: 3 | runner: seq_wr=227551 rand_rd=878991 cpu=123 composite=40850.3"
          },
          {
            "name": "overwrite",
            "value": 662471.048279366,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1176615 ops/sec | factor: 0.563 | P50: 0.7us | P99: 2.6us | P99.9: 6.0us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=227551 rand_rd=878991 cpu=123 composite=40850.3"
          },
          {
            "name": "mergerandom",
            "value": 410763.02536520595,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 729556 ops/sec | factor: 0.563 | P50: 0.3us | P99: 0.5us | P99.9: 3.0us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=227551 rand_rd=878991 cpu=123 composite=40850.3"
          },
          {
            "name": "readwhilewriting",
            "value": 237371.20820738518,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 421595 ops/sec | factor: 0.563 | P50: 2.2us | P99: 4.5us | P99.9: 14.8us\nthreads: 1 | elapsed: 0.47s | num: 200000 | iterations: 3 | runner: seq_wr=227551 rand_rd=878991 cpu=123 composite=40850.3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ab61d33f93764e719ef398bd122aaae5f031cf84",
          "message": "perf(compression): cache pre-compiled Dictionary across block decompress calls (#227)\n\n## Summary\n\n- **C FFI backend**: `DecoderDictionary<'static>` (wraps `ZSTD_DDict`)\nis now cached in `ZstdDictionary` via `Arc<OnceLock<...>>` — parsed once\nper process, shared across all clones of the same dictionary handle,\nzero re-parsing on subsequent blocks\n- **Pure Rust backend**: `FrameDecoder` with dictionary pre-loaded is\ncached in thread-local storage keyed by `dict_id` — parsed once per\nthread, no mutex needed (`FrameDecoder` is `!Send`)\n- **Correctness fix**: latent bug in pure Rust `decompress_with_dict` —\nwas calling `init(data)` on a `Copy` slice (only read the frame header;\ndecode buffer stayed empty, always returning `Ok([])`); replaced with\n`decode_all_to_vec(&mut input)` which fully decodes the frame\n\n## Changes\n\n| File | Change |\n|------|--------|\n| `src/compression/mod.rs` | Add `prepared:\nArc<OnceLock<DecoderDictionary<'static>>>` to `ZstdDictionary`; add\n`decoder_dict()` accessor; change `decompress_with_dict` signature to\ntake `&ZstdDictionary` |\n| `src/compression/zstd_ffi.rs` | Use\n`Decompressor::with_prepared_dictionary(dict.decoder_dict())` — no more\nper-call `ZSTD_createDDict` |\n| `src/compression/zstd_pure.rs` | TLS-cached `FrameDecoder`; fix\ncorrectness bug; add unit tests with pre-generated test vectors |\n| `src/table/block/mod.rs` | Update 4 `decompress_with_dict` call sites\nto pass `&dict` instead of `dict.raw()` |\n| `benches/zstd_dict.rs` | New: warm/cold per-block latency benchmarks |\n\n## Test Plan\n\n- [x] `cargo clippy --features zstd --all-targets -- -D warnings` —\nclean\n- [x] `cargo clippy --features zstd-pure --all-targets -- -D warnings` —\nclean\n- [x] `cargo nextest run --features zstd --workspace` — 1168/1168 passed\n- [x] `cargo nextest run --features zstd-pure --workspace` — 1157/1157\npassed\n- [x] `cargo test --doc --workspace` — 41/41 passed\n- [x] `cargo build --bench zstd_dict --features zstd` — compiles\n- [x] `cargo build --bench zstd_dict --features zstd-pure` — compiles\n\nCloses #217\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **Tests**\n* Added a benchmark to measure decompression performance using zstd\ndictionaries.\n\n* **Refactor**\n* Improved compression API to use dictionary objects and enable internal\ndictionary caching for better decompression efficiency.\n* Compression module is now hidden from generated public documentation.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-04-07T19:23:06+03:00",
          "tree_id": "671c6d588a6a9d0b0d5a37cb33f0872f0b2403ad",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/ab61d33f93764e719ef398bd122aaae5f031cf84"
        },
        "date": 1775579070317,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 975651.9888312398,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1950308 ops/sec | factor: 0.500 | P50: 0.4us | P99: 1.9us | P99.9: 3.8us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=336033 rand_rd=912806 cpu=117 composite=45976.5"
          },
          {
            "name": "fillrandom",
            "value": 406973.1839685128,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 813531 ops/sec | factor: 0.500 | P50: 1.0us | P99: 3.2us | P99.9: 7.7us\nthreads: 1 | elapsed: 0.25s | num: 200000 | iterations: 3 | runner: seq_wr=336033 rand_rd=912806 cpu=117 composite=45976.5"
          },
          {
            "name": "readrandom",
            "value": 216828.27312077183,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 433435 ops/sec | factor: 0.500 | P50: 2.1us | P99: 5.3us | P99.9: 10.7us\nthreads: 1 | elapsed: 0.46s | num: 200000 | iterations: 3 | runner: seq_wr=336033 rand_rd=912806 cpu=117 composite=45976.5"
          },
          {
            "name": "readseq",
            "value": 1592085.0741692944,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 3182545 ops/sec | factor: 0.500 | P50: 0.2us | P99: 3.6us | P99.9: 6.4us\nthreads: 1 | elapsed: 0.06s | num: 200000 | iterations: 3 | runner: seq_wr=336033 rand_rd=912806 cpu=117 composite=45976.5"
          },
          {
            "name": "seekrandom",
            "value": 161998.50086642226,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 323832 ops/sec | factor: 0.500 | P50: 2.8us | P99: 6.0us | P99.9: 11.8us\nthreads: 1 | elapsed: 0.62s | num: 200000 | iterations: 3 | runner: seq_wr=336033 rand_rd=912806 cpu=117 composite=45976.5"
          },
          {
            "name": "prefixscan",
            "value": 110686.30460405447,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 221260 ops/sec | factor: 0.500 | P50: 4.2us | P99: 5.7us | P99.9: 12.3us\nthreads: 1 | elapsed: 0.90s | num: 200000 | iterations: 3 | runner: seq_wr=336033 rand_rd=912806 cpu=117 composite=45976.5"
          },
          {
            "name": "overwrite",
            "value": 386202.50549860875,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 772011 ops/sec | factor: 0.500 | P50: 1.0us | P99: 3.6us | P99.9: 8.5us\nthreads: 1 | elapsed: 0.26s | num: 200000 | iterations: 3 | runner: seq_wr=336033 rand_rd=912806 cpu=117 composite=45976.5"
          },
          {
            "name": "mergerandom",
            "value": 380260.6261186645,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 760133 ops/sec | factor: 0.500 | P50: 0.4us | P99: 0.7us | P99.9: 3.7us\nthreads: 1 | elapsed: 0.26s | num: 200000 | iterations: 3 | runner: seq_wr=336033 rand_rd=912806 cpu=117 composite=45976.5"
          },
          {
            "name": "readwhilewriting",
            "value": 185713.20211867147,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 371237 ops/sec | factor: 0.500 | P50: 2.5us | P99: 4.9us | P99.9: 11.1us\nthreads: 1 | elapsed: 0.54s | num: 200000 | iterations: 3 | runner: seq_wr=336033 rand_rd=912806 cpu=117 composite=45976.5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "abd0ee5443dcf9362937ff54baefbb4bc8d29239",
          "message": "feat(compression): enable dictionary compression in pure Rust backend (#229)\n\n## Summary\n\n- Implements \\`compress_with_dict()\\` in \\`ZstdPureProvider\\` using\n\\`FrameCompressor\\` from \\`structured-zstd\\` v0.0.11\n(structured-world/structured-zstd#25)\n- Removes the C FFI \\`zstd\\` crate entirely; \\`structured-zstd\\` is now\nthe sole backend under the \\`zstd\\` feature flag\n- \\`zstd-pure\\` becomes a deprecated alias (\\`zstd-pure = [\"zstd\"]\\`) —\nenabling it is equivalent to enabling \\`zstd\\`\n- Supports both finalized zstd dictionaries (magic bytes \\`37 A4 30 EC\\`\n+ entropy tables) and raw content dictionaries\n- TLS caching: single-entry \\`FrameCompressor\\` / \\`FrameDecoder\\` per\nthread, keyed by 64-bit xxh3 fingerprint + level\n- \\`strip_dict_id\\`: rewritten in-place (\\`get_mut\\` + \\`copy_within\\` +\n\\`truncate\\`) — eliminates O(frame_len) allocation per compressed block\n- Bug fix: \\`decode_raw_content_bounded\\` with \\`capacity=0\\` and an\nempty frame no longer incorrectly returns \\`DecompressedSizeTooLarge\\`;\nuses \\`remaining.max(1)\\` in \\`UptoBytes\\` so the decoder advances past\nthe empty Last_Block before capacity is checked\n- CI: \\`test-zstd-pure\\` → \\`test-zstd\\`, matrix extended to include\nMSRV 1.92.0\n\n## Technical Details\n\n**Format detection:** \\`compress_with_dict\\` and\n\\`decompress_with_dict\\` check for the zstd dictionary magic prefix\n(bytes \\`37 A4 30 EC\\`, little-endian \\`0xEC30A437\\`):\n- Finalized dict → \\`Dictionary::decode_dict\\` (entropy tables +\ncontent)\n- Raw content dict → \\`Dictionary::from_raw_content\\` with ID = lower 32\nbits of xxh3, clamped to ≥1 (id=0 is reserved in the zstd frame format)\n\n**Raw-content dict ID stripping:** \\`compress_with_dict\\` strips the\nsynthetic dictID from the frame header after compression. This matches\nthe zstd standard convention where \\`dictID=0\\` (absent) means\n\"raw-content dict, id unknown, accept any\", preventing decompressors\nfrom requiring the specific synthetic id.\n\n**Decompression-bomb guard:** The raw-content dict decompress path calls\n\\`decoder.content_size()\\` after \\`init()\\` and rejects frames whose\ndeclared size exceeds the caller's capacity limit before allocating the\noutput buffer. Frames without the FCS field fall back to the post-decode\ncheck.\n\n**\\`ZstdDictionary::id()\\`:** Returns the raw lower 32 bits of xxh3 (may\ntheoretically be 0). Config validation paths compare \\`dict.id()\\`\nagainst on-disk \\`dict_id\\` — both sides derive the same value, so\nvalidation is unaffected. The \\`.max(1)\\` clamp is only applied inside\nthe backend when embedding an id in a zstd frame header.\n\n**Blocker resolved:** structured-world/structured-zstd#25 (FastCOVER +\ndictionary finalization) merged; \\`structured-zstd\\` bumped to v0.0.11\n(encoding performance improvements: row-based match finder, HC positions\nrebase, streaming scratch buffer reuse, FSE decoder packing, and HC\ntable improvements).\n\n## Known Limitations\n\n- TLS compressor/decoder is a single-entry memoizer; switching\ndictionaries/levels on the same thread causes a re-parse — tracked in\n#231\n- Blob-file dictionary compression remains unsupported — tracked in #230\n- Decompression throughput ~2–3.5× slower than C reference\n(pre-existing)\n\n## Test Plan\n\n- [x] \\`src/compression/zstd_pure.rs\\` — roundtrip, magic detection, all\nlevels, empty input, raw content dict, capacity guard, in-place\nstrip_dict_id, empty-frame-at-capacity-0 regression\n- [x] \\`tests/zstd_dict_roundtrip.rs\\` — full Tree write/flush/read,\nrange scan, reopen, missing/wrong dict errors, finalized dict,\nencryption, per-level policy\n- [x] \\`tests/zstd_dict_roundtrip.rs\\` — compaction path: 3 L0 SSTs\nflushed → \\`major_compact\\` → 300 keys readable + range scan (exercises\nboth \\`compress_with_dict\\` and \\`decompress_with_dict\\` on the\ncompaction hot path)\n- [x] \\`tests/zstd_dict_roundtrip.rs\\` — reopen with wrong dict fails at\nrecovery (\\`ZstdDictMismatch\\` on first SST read)\n- [x] \\`cargo nextest run --no-default-features --features zstd,lz4\\`:\n1208 passed\n- [x] \\`cargo nextest run --all-features\\`: 1256 passed\n- [x] \\`cargo clippy --all-features -- -D warnings\\`: clean\n- [x] \\`cargo clippy --no-default-features --features zstd,lz4\n--all-targets -- -D warnings\\`: clean\n- [x] CI job \\`test-zstd\\` (renamed from \\`test-zstd-pure\\`) runs on\n\\`[stable, \"1.92.0\"]\\`\n\nCloses #218\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **New Features**\n* Added support for zstd dictionary compression and decompression\n(pure-Rust backend).\n\n* **Documentation**\n* Clarifies zstd now uses a pure‑Rust backend (no C toolchain),\ndocuments performance tradeoffs, current limitations, and marks the old\nalias as deprecated.\n\n* **Tests**\n* Substantially expanded unit and integration tests for dictionary\nbehaviors and round‑trip/compaction scenarios.\n\n* **Chores**\n* CI and benchmark workflow updates; dependency and build-script feature\nwiring adjusted.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-04-09T13:16:04+03:00",
          "tree_id": "041bf19ebc8549f6cd00a538204fe159852a0994",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/abd0ee5443dcf9362937ff54baefbb4bc8d29239"
        },
        "date": 1775729827275,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1156876.464750592,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2114039 ops/sec | factor: 0.547 | P50: 0.3us | P99: 2.0us | P99.9: 4.9us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=225405 rand_rd=950499 cpu=123 composite=42029.5"
          },
          {
            "name": "fillrandom",
            "value": 646297.6479424117,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1181024 ops/sec | factor: 0.547 | P50: 0.7us | P99: 2.6us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=225405 rand_rd=950499 cpu=123 composite=42029.5"
          },
          {
            "name": "readrandom",
            "value": 321578.6052394696,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 587643 ops/sec | factor: 0.547 | P50: 1.5us | P99: 4.9us | P99.9: 12.7us\nthreads: 1 | elapsed: 0.34s | num: 200000 | iterations: 3 | runner: seq_wr=225405 rand_rd=950499 cpu=123 composite=42029.5"
          },
          {
            "name": "readseq",
            "value": 1369693.9633325106,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2502935 ops/sec | factor: 0.547 | P50: 0.2us | P99: 3.7us | P99.9: 7.7us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=225405 rand_rd=950499 cpu=123 composite=42029.5"
          },
          {
            "name": "seekrandom",
            "value": 213675.06712768466,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 390463 ops/sec | factor: 0.547 | P50: 2.2us | P99: 5.7us | P99.9: 13.5us\nthreads: 1 | elapsed: 0.51s | num: 200000 | iterations: 3 | runner: seq_wr=225405 rand_rd=950499 cpu=123 composite=42029.5"
          },
          {
            "name": "prefixscan",
            "value": 102822.20747632984,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 187894 ops/sec | factor: 0.547 | P50: 5.0us | P99: 7.4us | P99.9: 16.9us\nthreads: 1 | elapsed: 1.06s | num: 200000 | iterations: 3 | runner: seq_wr=225405 rand_rd=950499 cpu=123 composite=42029.5"
          },
          {
            "name": "overwrite",
            "value": 656502.1417723434,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1199671 ops/sec | factor: 0.547 | P50: 0.7us | P99: 2.6us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=225405 rand_rd=950499 cpu=123 composite=42029.5"
          },
          {
            "name": "mergerandom",
            "value": 372923.68981854105,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 681469 ops/sec | factor: 0.547 | P50: 0.3us | P99: 1.8us | P99.9: 5.0us\nthreads: 1 | elapsed: 0.29s | num: 200000 | iterations: 3 | runner: seq_wr=225405 rand_rd=950499 cpu=123 composite=42029.5"
          },
          {
            "name": "readwhilewriting",
            "value": 274109.5212248919,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 500899 ops/sec | factor: 0.547 | P50: 1.7us | P99: 7.0us | P99.9: 13.3us\nthreads: 1 | elapsed: 0.40s | num: 200000 | iterations: 3 | runner: seq_wr=225405 rand_rd=950499 cpu=123 composite=42029.5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "255865126+sw-release-bot[bot]@users.noreply.github.com",
            "name": "sw-release-bot[bot]",
            "username": "sw-release-bot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f01594940a0dd84468ca8ae201ea5f4ac16de2db",
          "message": "chore: release v4.4.0 (#228)\n\n## 🤖 New release\n\n* `coordinode-lsm-tree`: 4.3.1 -> 4.4.0\n\n<details><summary><i><b>Changelog</b></i></summary><p>\n\n<blockquote>\n\n##\n[4.4.0](https://github.com/structured-world/coordinode-lsm-tree/compare/v4.3.1...v4.4.0)\n- 2026-04-09\n\n### Added\n\n- *(compression)* enable dictionary compression in pure Rust backend\n([#229](https://github.com/structured-world/coordinode-lsm-tree/pull/229))\n\n### Performance\n\n- *(compression)* cache pre-compiled Dictionary across block decompress\ncalls\n([#227](https://github.com/structured-world/coordinode-lsm-tree/pull/227))\n</blockquote>\n\n\n</p></details>\n\n---\nThis PR was generated with\n[release-plz](https://github.com/release-plz/release-plz/).\n\nCo-authored-by: sw-release-bot[bot] <255865126+sw-release-bot[bot]@users.noreply.github.com>",
          "timestamp": "2026-04-09T13:53:40+03:00",
          "tree_id": "e0e7e4feaf7ace527b8959e24c0c6a8054d3425b",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/f01594940a0dd84468ca8ae201ea5f4ac16de2db"
        },
        "date": 1775732083230,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1202584.8336014163,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1886962 ops/sec | factor: 0.637 | P50: 0.4us | P99: 2.5us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.11s | num: 200000 | iterations: 3 | runner: seq_wr=218359 rand_rd=728891 cpu=108 composite=36089.0"
          },
          {
            "name": "fillrandom",
            "value": 687286.4719257228,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1078413 ops/sec | factor: 0.637 | P50: 0.7us | P99: 3.1us | P99.9: 7.1us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=218359 rand_rd=728891 cpu=108 composite=36089.0"
          },
          {
            "name": "readrandom",
            "value": 311121.0616144704,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 488176 ops/sec | factor: 0.637 | P50: 1.8us | P99: 6.3us | P99.9: 13.3us\nthreads: 1 | elapsed: 0.41s | num: 200000 | iterations: 3 | runner: seq_wr=218359 rand_rd=728891 cpu=108 composite=36089.0"
          },
          {
            "name": "readseq",
            "value": 1491369.5928134394,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2340091 ops/sec | factor: 0.637 | P50: 0.2us | P99: 4.7us | P99.9: 9.1us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=218359 rand_rd=728891 cpu=108 composite=36089.0"
          },
          {
            "name": "seekrandom",
            "value": 216337.1497646523,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 339452 ops/sec | factor: 0.637 | P50: 2.6us | P99: 7.2us | P99.9: 16.7us\nthreads: 1 | elapsed: 0.59s | num: 200000 | iterations: 3 | runner: seq_wr=218359 rand_rd=728891 cpu=108 composite=36089.0"
          },
          {
            "name": "prefixscan",
            "value": 115792.84868462507,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 181689 ops/sec | factor: 0.637 | P50: 5.2us | P99: 6.8us | P99.9: 17.1us\nthreads: 1 | elapsed: 1.10s | num: 200000 | iterations: 3 | runner: seq_wr=218359 rand_rd=728891 cpu=108 composite=36089.0"
          },
          {
            "name": "overwrite",
            "value": 710505.0984284881,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1114845 ops/sec | factor: 0.637 | P50: 0.7us | P99: 3.1us | P99.9: 6.9us\nthreads: 1 | elapsed: 0.18s | num: 200000 | iterations: 3 | runner: seq_wr=218359 rand_rd=728891 cpu=108 composite=36089.0"
          },
          {
            "name": "mergerandom",
            "value": 480315.6583716827,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 753658 ops/sec | factor: 0.637 | P50: 0.4us | P99: 2.2us | P99.9: 3.2us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=218359 rand_rd=728891 cpu=108 composite=36089.0"
          },
          {
            "name": "readwhilewriting",
            "value": 265976.3203808889,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 417340 ops/sec | factor: 0.637 | P50: 2.2us | P99: 5.9us | P99.9: 13.8us\nthreads: 1 | elapsed: 0.48s | num: 200000 | iterations: 3 | runner: seq_wr=218359 rand_rd=728891 cpu=108 composite=36089.0"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0fc0539eead6e49894719eca978b52c086c4bac9",
          "message": "feat(vlog): dictionary compression for blob files (#233)\n\n## Summary\n\n- Wire `ZstdDict` compression through the full blob-file I/O path\n(write, read, compaction filter reads, GC relocation)\n- Remove the explicit \"blob-file dictionary compression not supported\"\nrejection guards\n- Implement actual `compress_with_dict` / `decompress_with_dict` usage\nvia the `ZstdBackend` provider\n- Add `zstd_dictionary` field + `.dict()` builder to\n`KvSeparationOptions` with config-level validation (missing dict or\nmismatched `dict_id` → `Error::ZstdDictMismatch` at `open()`)\n\n## Technical Details\n\n**Write path** (`BlobFileWriter` / `MultiWriter` / `Writer`):\n- Added `zstd_dictionary: Option<Arc<ZstdDictionary>>` field and\n`use_zstd_dictionary()` builder\n- `ZstdDict` arm in `write_raw` calls `ZstdBackend::compress_with_dict`;\nerrors if dict absent\n- `rotate()` in `MultiWriter` threads the dictionary into each new\n`Writer`\n\n**Read path** (`Reader` / `Accessor`):\n- Added `zstd_dictionary: Option<&ZstdDictionary>` field and\n`with_dict()` builder to `Reader`\n- `ZstdDict` arm validates `dict_id` matches, then calls\n`ZstdBackend::decompress_with_dict`\n- `Accessor` stores dict reference and passes it into `Reader` on every\n`get()`\n\n**Higher-level wiring** (`BlobTree`, compaction filter):\n- `resolve_value_handle` receives dict via `#[cfg(zstd_any)]` parameter;\nall three call sites updated\n- `BlobFileWriter` creation in flush/GC path calls\n`.use_zstd_dictionary(kv_opts.zstd_dictionary.clone())`\n- `AccessorShared::get_indirect_value` in compaction filter constructs\n`Accessor` with dict\n\n**Config validation** (`KvSeparationOptions`):\n- `validate_zstd_dictionary` now checks blob compression in addition to\nSST compression\n- Missing dict → `ZstdDictMismatch { expected, got: None }`\n- Mismatched id → `ZstdDictMismatch { expected, got: Some(actual) }`\n\n**Misc**:\n- `ZstdDictionary` gains `PartialEq`/`Eq` impls (compare by `id`) so\n`KvSeparationOptions` can derive `PartialEq`\n- README: removed \"blob-file dictionary compression not supported\"\nlimitation note\n\n## Test Plan\n\n- `cargo nextest run --workspace --all-features` — 1266/1266 passed\n- `cargo test --doc --all-features` — 41/41 passed\n- `cargo clippy --all-targets --all-features -- -D warnings` — clean\n\nNew integration tests in `tests/zstd_dict_roundtrip.rs`:\n- `blob_zstd_dict_roundtrip_write_flush_read` — write blobs, flush, read\nback\n- `blob_zstd_dict_roundtrip_survives_major_compact` — blobs survive\nGC/relocation compaction\n- `blob_zstd_dict_missing_at_open_is_rejected` — missing dict caught at\n`open()`\n- `blob_zstd_dict_id_mismatch_at_open_is_rejected` — mismatched dict_id\ncaught at `open()`\n\nCloses #230\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **New Features**\n* Zstd dictionary compression now supports blob-backed values; reads and\nwrites can use a supplied dictionary for compression/decompression.\n\n* **Validation**\n* Configuration now enforces a matching Zstd dictionary when\ndictionary-based compression is enabled and surfaces clear\nmismatch/missing-dictionary errors.\n\n* **Documentation**\n* README updated to state Zstd dictionary compression applies to small\ntable blocks and blob files.\n\n* **Tests**\n* Added end-to-end and edge-case tests covering blob round-trips,\ncompaction persistence, range/prefix/multi-get reads, and config\nvalidation.\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-04-09T17:42:39+03:00",
          "tree_id": "afcdfdaabba23f4f3d0aa31cc660ee801e625c61",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/0fc0539eead6e49894719eca978b52c086c4bac9"
        },
        "date": 1775745825910,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1204850.5525356098,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1940188 ops/sec | factor: 0.621 | P50: 0.4us | P99: 2.4us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=231775 rand_rd=743275 cpu=109 composite=37037.2"
          },
          {
            "name": "fillrandom",
            "value": 648275.2515067102,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1043927 ops/sec | factor: 0.621 | P50: 0.7us | P99: 3.1us | P99.9: 7.5us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=231775 rand_rd=743275 cpu=109 composite=37037.2"
          },
          {
            "name": "readrandom",
            "value": 289096.85236871266,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 465537 ops/sec | factor: 0.621 | P50: 1.9us | P99: 6.4us | P99.9: 13.3us\nthreads: 1 | elapsed: 0.43s | num: 200000 | iterations: 3 | runner: seq_wr=231775 rand_rd=743275 cpu=109 composite=37037.2"
          },
          {
            "name": "readseq",
            "value": 1405131.667896422,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2262704 ops/sec | factor: 0.621 | P50: 0.3us | P99: 4.7us | P99.9: 9.5us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=231775 rand_rd=743275 cpu=109 composite=37037.2"
          },
          {
            "name": "seekrandom",
            "value": 204082.04201244522,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 328636 ops/sec | factor: 0.621 | P50: 2.7us | P99: 7.0us | P99.9: 15.5us\nthreads: 1 | elapsed: 0.61s | num: 200000 | iterations: 3 | runner: seq_wr=231775 rand_rd=743275 cpu=109 composite=37037.2"
          },
          {
            "name": "prefixscan",
            "value": 113402.29583579481,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 182613 ops/sec | factor: 0.621 | P50: 5.1us | P99: 6.9us | P99.9: 17.1us\nthreads: 1 | elapsed: 1.10s | num: 200000 | iterations: 3 | runner: seq_wr=231775 rand_rd=743275 cpu=109 composite=37037.2"
          },
          {
            "name": "overwrite",
            "value": 672418.2434038012,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1082805 ops/sec | factor: 0.621 | P50: 0.7us | P99: 3.1us | P99.9: 6.9us\nthreads: 1 | elapsed: 0.18s | num: 200000 | iterations: 3 | runner: seq_wr=231775 rand_rd=743275 cpu=109 composite=37037.2"
          },
          {
            "name": "mergerandom",
            "value": 471755.1258042507,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 759674 ops/sec | factor: 0.621 | P50: 0.3us | P99: 0.5us | P99.9: 3.2us\nthreads: 1 | elapsed: 0.26s | num: 200000 | iterations: 3 | runner: seq_wr=231775 rand_rd=743275 cpu=109 composite=37037.2"
          },
          {
            "name": "readwhilewriting",
            "value": 283929.1652989099,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 457215 ops/sec | factor: 0.621 | P50: 2.0us | P99: 4.8us | P99.9: 13.5us\nthreads: 1 | elapsed: 0.44s | num: 200000 | iterations: 3 | runner: seq_wr=231775 rand_rd=743275 cpu=109 composite=37037.2"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e8500f094d3705601efc718a32c37fc94773f80d",
          "message": "build(deps): update all dependencies + adopt upstream blob-tree clear() fix (#236)\n\n## Summary\n\n- **Dependency refresh**: all crates in `Cargo.toml` / `Cargo.lock`\nbumped to latest, including major-version bumps that required code\nchanges.\n- **Adopted upstream bug fix** (fjall-rs/lsm-tree#286):\n`BlobTree::clear()` was corrupting the on-disk tree by skipping the\nversion-manifest update, causing recovery to fail on reopen with *\"Tried\nto open a BlobTree, but the existing tree is of type StandardTree\"*.\nCherry-picked upstream `db394880`, adapted to our diverged\n`Memtable::new(id, comparator)` signature and `&*config.fs` argument,\nplus rewrote the new default `tree_type()` impl in idiomatic `if/else`\nto satisfy our stricter `deny(clippy::all)` (`obfuscated_if_else`,\n`unnecessary_lazy_evaluations`).\n\n## Commits\n\n1. `build(deps)` — `cargo update` for ~30 semver-compatible bumps;\nmanifest bumps for `structured-zstd 0.0.12 → 0.0.21` (our maintained\nfork; each pre-1.0 bump is breaking) and `rand 0.9 → 0.10` (dev-dep\nmajor bump — adapted `benches/bloom.rs` to `RngExt::random()` and\n`Rng::fill()` after `RngCore` was removed from rand crate root).\n2. `test(blob_tree)` — regression test reproducing upstream #286: insert\ninto kv-separated tree → `clear()` → reopen fails. Written **before**\nthe fix per the test-first protocol; failed cleanly on stock code.\n3. `fix` — cherry-pick of upstream `db394880` adapted to our fork. After\nthe fix the regression test passes.\n\n## Upstream sync analysis\n\nPost-divergence commits in `fjall-rs/lsm-tree:main` evaluated for\nadoption:\n\n| Upstream | Adopted | Reason |\n|---|---|---|\n| `bad4fe0a` (seqno of point-read ingested items) | ❌ | Our refactor\nalready adjusts `global_seqno` at the `get` / `get_with_block` caller\nlevel, so the bug doesn't reproduce here. |\n| `db394880` + `557cd0db` (clear() blob tree corruption #286) | ✅ | Real\nbug, also present in our fork — adopted with regression test. |\n| `05c082ff` + `a8db0880` (`Slice::as_slice()` convenience) | ❌ | Pure\nAPI addition; `Slice` already implements `Deref<Target=[u8]>`. Can be\nadded separately if needed. |\n\nOpen upstream issues triaged: none are confirmed bugs (only #289 —\ningestion-pinning enhancement).\n\n## Test plan\n\n- [x] `cargo check --all-features --all-targets` clean\n- [x] `cargo clippy --all-features --all-targets -- -D warnings` clean\n- [x] `cargo nextest run --all-features` — 1271 passed, 6 skipped\n- [x] `cargo test --doc --all-features` — 41 passed, 2 ignored\n- [x] New regression test `blob_tree_clear_then_reopen_succeeds` fails\non stock fork, passes after the fix\n\nCloses #235\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n\n## Summary by CodeRabbit\n\n* **New Features**\n* AbstractTree trait now provides a default tree type selection based on\nconfiguration.\n\n* **Bug Fixes**\n* Fixed BlobTree clear operation to properly reset version history,\npreventing potential corruption when reopening the tree.\n\n* **Chores**\n  * Updated dependencies: structured-zstd and rand.\n\n* **Tests**\n  * Added regression test for blob tree clear and reopen operations.\n\n<!-- review_stack_entry_start -->\n\n[![Review Change\nStack](https://storage.googleapis.com/coderabbit_public_assets/review-stack-in-coderabbit-ui.svg)](https://app.coderabbit.ai/change-stack/structured-world/coordinode-lsm-tree/pull/236?utm_source=github_walkthrough&utm_medium=github&utm_campaign=change_stack)\n\n<!-- review_stack_entry_end -->\n\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->\n\n---------\n\nCo-authored-by: marvin-j97 <marvin.janke.97@gmail.com>",
          "timestamp": "2026-05-18T03:39:47+03:00",
          "tree_id": "8ddb290e2ecd78b55e98417a356951c4191743f7",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/e8500f094d3705601efc718a32c37fc94773f80d"
        },
        "date": 1779064859718,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1176777.328682438,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2111468 ops/sec | factor: 0.557 | P50: 0.3us | P99: 2.0us | P99.9: 5.0us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=225163 rand_rd=908770 cpu=123 composite=41268.4"
          },
          {
            "name": "fillrandom",
            "value": 671642.2475625958,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1205114 ops/sec | factor: 0.557 | P50: 0.7us | P99: 2.6us | P99.9: 6.1us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=225163 rand_rd=908770 cpu=123 composite=41268.4"
          },
          {
            "name": "readrandom",
            "value": 293045.2809561007,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 525805 ops/sec | factor: 0.557 | P50: 1.7us | P99: 5.4us | P99.9: 13.1us\nthreads: 1 | elapsed: 0.38s | num: 200000 | iterations: 3 | runner: seq_wr=225163 rand_rd=908770 cpu=123 composite=41268.4"
          },
          {
            "name": "readseq",
            "value": 1388504.1520238821,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2491366 ops/sec | factor: 0.557 | P50: 0.2us | P99: 3.8us | P99.9: 8.1us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=225163 rand_rd=908770 cpu=123 composite=41268.4"
          },
          {
            "name": "seekrandom",
            "value": 202005.40131769123,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 362454 ops/sec | factor: 0.557 | P50: 2.4us | P99: 6.1us | P99.9: 14.2us\nthreads: 1 | elapsed: 0.55s | num: 200000 | iterations: 3 | runner: seq_wr=225163 rand_rd=908770 cpu=123 composite=41268.4"
          },
          {
            "name": "prefixscan",
            "value": 101526.96933354416,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 182168 ops/sec | factor: 0.557 | P50: 5.1us | P99: 7.6us | P99.9: 17.3us\nthreads: 1 | elapsed: 1.10s | num: 200000 | iterations: 3 | runner: seq_wr=225163 rand_rd=908770 cpu=123 composite=41268.4"
          },
          {
            "name": "overwrite",
            "value": 695252.640025987,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1247478 ops/sec | factor: 0.557 | P50: 0.6us | P99: 2.5us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.16s | num: 200000 | iterations: 3 | runner: seq_wr=225163 rand_rd=908770 cpu=123 composite=41268.4"
          },
          {
            "name": "mergerandom",
            "value": 411953.13543014193,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 739159 ops/sec | factor: 0.557 | P50: 0.3us | P99: 1.8us | P99.9: 3.2us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=225163 rand_rd=908770 cpu=123 composite=41268.4"
          },
          {
            "name": "readwhilewriting",
            "value": 254393.7815790257,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 456454 ops/sec | factor: 0.557 | P50: 1.9us | P99: 7.5us | P99.9: 15.6us\nthreads: 1 | elapsed: 0.44s | num: 200000 | iterations: 3 | runner: seq_wr=225163 rand_rd=908770 cpu=123 composite=41268.4"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "49699333+dependabot[bot]@users.noreply.github.com",
            "name": "dependabot[bot]",
            "username": "dependabot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d8cb62fb7c38fb8ab42ff64884fabc58e4ecbec3",
          "message": "ci(deps): bump actions/github-script from 7 to 9 (#237)\n\nBumps [actions/github-script](https://github.com/actions/github-script)\nfrom 7 to 9.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/actions/github-script/releases\">actions/github-script's\nreleases</a>.</em></p>\n<blockquote>\n<h2>v9.0.0</h2>\n<p><strong>New features:</strong></p>\n<ul>\n<li><strong><code>getOctokit</code> factory function</strong> —\nAvailable directly in the script context. Create additional\nauthenticated Octokit clients with different tokens for multi-token\nworkflows, GitHub App tokens, and cross-org access. See <a\nhref=\"https://github.com/actions/github-script#creating-additional-clients-with-getoctokit\">Creating\nadditional clients with <code>getOctokit</code></a> for details and\nexamples.</li>\n<li><strong>Orchestration ID in user-agent</strong> — The\n<code>ACTIONS_ORCHESTRATION_ID</code> environment variable is\nautomatically appended to the user-agent string for request\ntracing.</li>\n</ul>\n<p><strong>Breaking changes:</strong></p>\n<ul>\n<li><strong><code>require('@actions/github')</code> no longer works in\nscripts.</strong> The upgrade to <code>@actions/github</code> v9\n(ESM-only) means <code>require('@actions/github')</code> will fail at\nruntime. If you previously used patterns like <code>const { getOctokit }\n= require('@actions/github')</code> to create secondary clients, use the\nnew injected <code>getOctokit</code> function instead — it's available\ndirectly in the script context with no imports needed.</li>\n<li><code>getOctokit</code> is now an injected function parameter.\nScripts that declare <code>const getOctokit = ...</code> or <code>let\ngetOctokit = ...</code> will get a <code>SyntaxError</code> because\nJavaScript does not allow <code>const</code>/<code>let</code>\nredeclaration of function parameters. Use the injected\n<code>getOctokit</code> directly, or use <code>var getOctokit =\n...</code> if you need to redeclare it.</li>\n<li>If your script accesses other <code>@actions/github</code> internals\nbeyond the standard <code>github</code>/<code>octokit</code> client, you\nmay need to update those references for v9 compatibility.</li>\n</ul>\n<h2>What's Changed</h2>\n<ul>\n<li>Add ACTIONS_ORCHESTRATION_ID to user-agent string by <a\nhref=\"https://github.com/Copilot\"><code>@​Copilot</code></a> in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/695\">actions/github-script#695</a></li>\n<li>ci: use deployment: false for integration test environments by <a\nhref=\"https://github.com/salmanmkc\"><code>@​salmanmkc</code></a> in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/712\">actions/github-script#712</a></li>\n<li>feat!: add getOctokit to script context, upgrade\n<code>@​actions/github</code> v9, <code>@​octokit/core</code> v7, and\nrelated packages by <a\nhref=\"https://github.com/salmanmkc\"><code>@​salmanmkc</code></a> in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/700\">actions/github-script#700</a></li>\n</ul>\n<h2>New Contributors</h2>\n<ul>\n<li><a href=\"https://github.com/Copilot\"><code>@​Copilot</code></a> made\ntheir first contribution in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/695\">actions/github-script#695</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a\nhref=\"https://github.com/actions/github-script/compare/v8.0.0...v9.0.0\">https://github.com/actions/github-script/compare/v8.0.0...v9.0.0</a></p>\n<h2>v8.0.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>Update Node.js version support to 24.x by <a\nhref=\"https://github.com/salmanmkc\"><code>@​salmanmkc</code></a> in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/637\">actions/github-script#637</a></li>\n<li>README for updating actions/github-script from v7 to v8 by <a\nhref=\"https://github.com/sneha-krip\"><code>@​sneha-krip</code></a> in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/653\">actions/github-script#653</a></li>\n</ul>\n<h2>⚠️ Minimum Compatible Runner Version</h2>\n<p><strong>v2.327.1</strong><br />\n<a\nhref=\"https://github.com/actions/runner/releases/tag/v2.327.1\">Release\nNotes</a></p>\n<p>Make sure your runner is updated to this version or newer to use this\nrelease.</p>\n<h2>New Contributors</h2>\n<ul>\n<li><a href=\"https://github.com/salmanmkc\"><code>@​salmanmkc</code></a>\nmade their first contribution in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/637\">actions/github-script#637</a></li>\n<li><a\nhref=\"https://github.com/sneha-krip\"><code>@​sneha-krip</code></a> made\ntheir first contribution in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/653\">actions/github-script#653</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a\nhref=\"https://github.com/actions/github-script/compare/v7.1.0...v8.0.0\">https://github.com/actions/github-script/compare/v7.1.0...v8.0.0</a></p>\n<h2>v7.1.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>Upgrade husky to v9 by <a\nhref=\"https://github.com/benelan\"><code>@​benelan</code></a> in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/482\">actions/github-script#482</a></li>\n<li>Add workflow file for publishing releases to immutable action\npackage by <a\nhref=\"https://github.com/Jcambass\"><code>@​Jcambass</code></a> in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/485\">actions/github-script#485</a></li>\n<li>Upgrade IA Publish by <a\nhref=\"https://github.com/Jcambass\"><code>@​Jcambass</code></a> in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/486\">actions/github-script#486</a></li>\n<li>Fix workflow status badges by <a\nhref=\"https://github.com/joshmgross\"><code>@​joshmgross</code></a> in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/497\">actions/github-script#497</a></li>\n<li>Update usage of <code>actions/upload-artifact</code> by <a\nhref=\"https://github.com/joshmgross\"><code>@​joshmgross</code></a> in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/512\">actions/github-script#512</a></li>\n<li>Clear up package name confusion by <a\nhref=\"https://github.com/joshmgross\"><code>@​joshmgross</code></a> in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/514\">actions/github-script#514</a></li>\n<li>Update dependencies with <code>npm audit fix</code> by <a\nhref=\"https://github.com/joshmgross\"><code>@​joshmgross</code></a> in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/515\">actions/github-script#515</a></li>\n<li>Specify that the used script is JavaScript by <a\nhref=\"https://github.com/timotk\"><code>@​timotk</code></a> in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/478\">actions/github-script#478</a></li>\n<li>chore: Add Dependabot for NPM and Actions by <a\nhref=\"https://github.com/nschonni\"><code>@​nschonni</code></a> in <a\nhref=\"https://redirect.github.com/actions/github-script/pull/472\">actions/github-script#472</a></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/actions/github-script/commit/3a2844b7e9c422d3c10d287c895573f7108da1b3\"><code>3a2844b</code></a>\nMerge pull request <a\nhref=\"https://redirect.github.com/actions/github-script/issues/700\">#700</a>\nfrom actions/salmanmkc/expose-getoctokit + prepare re...</li>\n<li><a\nhref=\"https://github.com/actions/github-script/commit/ca10bbdd1a7739de09e99a200c7a59f5d73a4079\"><code>ca10bbd</code></a>\nfix: use <code>@​octokit/core/</code>types import for v7\ncompatibility</li>\n<li><a\nhref=\"https://github.com/actions/github-script/commit/86e48e20ac85c970ed1f96e718fd068173948b7b\"><code>86e48e2</code></a>\nmerge: incorporate main branch changes</li>\n<li><a\nhref=\"https://github.com/actions/github-script/commit/c1084728b5b935ec4ddc1e4cee877b01797b3ff9\"><code>c108472</code></a>\nchore: rebuild dist for v9 upgrade and getOctokit factory</li>\n<li><a\nhref=\"https://github.com/actions/github-script/commit/afff112e4f8b57c718168af75b89ce00bc8d091d\"><code>afff112</code></a>\nMerge pull request <a\nhref=\"https://redirect.github.com/actions/github-script/issues/712\">#712</a>\nfrom actions/salmanmkc/deployment-false + fix user-ag...</li>\n<li><a\nhref=\"https://github.com/actions/github-script/commit/ff8117e5b78c415f814f39ad6998f424fee7b817\"><code>ff8117e</code></a>\nci: fix user-agent test to handle orchestration ID</li>\n<li><a\nhref=\"https://github.com/actions/github-script/commit/81c6b7876079abe10ff715951c9fc7b3e1ab389d\"><code>81c6b78</code></a>\nci: use deployment: false to suppress deployment noise from integration\ntests</li>\n<li><a\nhref=\"https://github.com/actions/github-script/commit/3953caf8858d318f37b6cc53a9f5708859b5a7b7\"><code>3953caf</code></a>\ndocs: update README examples from <a\nhref=\"https://github.com/v8\"><code>@​v8</code></a> to <a\nhref=\"https://github.com/v9\"><code>@​v9</code></a>, add getOctokit docs\nand v9 brea...</li>\n<li><a\nhref=\"https://github.com/actions/github-script/commit/c17d55b90dcdb3d554d0027a6c180a7adc2daf78\"><code>c17d55b</code></a>\nci: add getOctokit integration test job</li>\n<li><a\nhref=\"https://github.com/actions/github-script/commit/a047196d9a02fe92098771cafbb98c2f1814e408\"><code>a047196</code></a>\ntest: add getOctokit integration tests via callAsyncFunction</li>\n<li>Additional commits viewable in <a\nhref=\"https://github.com/actions/github-script/compare/v7...v9\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility\nscore](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=actions/github-script&package-manager=github_actions&previous-version=7&new-version=9)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop\nDependabot creating any more for this major version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop\nDependabot creating any more for this minor version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop\nDependabot creating any more for this dependency (unless you reopen the\nPR or upgrade to it yourself)\n\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-05-18T04:27:57+03:00",
          "tree_id": "9045ad652878b7b8dc1c761f98444594be003709",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/d8cb62fb7c38fb8ab42ff64884fabc58e4ecbec3"
        },
        "date": 1779067958884,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1199109.5017969378,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1909738 ops/sec | factor: 0.628 | P50: 0.4us | P99: 2.5us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=220965 rand_rd=751625 cpu=108 composite=36630.5"
          },
          {
            "name": "fillrandom",
            "value": 695323.284802194,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1107393 ops/sec | factor: 0.628 | P50: 0.7us | P99: 3.1us | P99.9: 7.6us\nthreads: 1 | elapsed: 0.18s | num: 200000 | iterations: 3 | runner: seq_wr=220965 rand_rd=751625 cpu=108 composite=36630.5"
          },
          {
            "name": "readrandom",
            "value": 298294.0492268953,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 475072 ops/sec | factor: 0.628 | P50: 1.9us | P99: 6.5us | P99.9: 13.4us\nthreads: 1 | elapsed: 0.42s | num: 200000 | iterations: 3 | runner: seq_wr=220965 rand_rd=751625 cpu=108 composite=36630.5"
          },
          {
            "name": "readseq",
            "value": 1359743.6777350865,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2165569 ops/sec | factor: 0.628 | P50: 0.3us | P99: 4.4us | P99.9: 9.3us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=220965 rand_rd=751625 cpu=108 composite=36630.5"
          },
          {
            "name": "seekrandom",
            "value": 217271.61782972762,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 346033 ops/sec | factor: 0.628 | P50: 2.5us | P99: 7.0us | P99.9: 14.4us\nthreads: 1 | elapsed: 0.58s | num: 200000 | iterations: 3 | runner: seq_wr=220965 rand_rd=751625 cpu=108 composite=36630.5"
          },
          {
            "name": "prefixscan",
            "value": 105786.36231661707,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 168479 ops/sec | factor: 0.628 | P50: 5.6us | P99: 7.3us | P99.9: 17.3us\nthreads: 1 | elapsed: 1.19s | num: 200000 | iterations: 3 | runner: seq_wr=220965 rand_rd=751625 cpu=108 composite=36630.5"
          },
          {
            "name": "overwrite",
            "value": 712313.6613421607,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1134452 ops/sec | factor: 0.628 | P50: 0.7us | P99: 2.9us | P99.9: 6.7us\nthreads: 1 | elapsed: 0.18s | num: 200000 | iterations: 3 | runner: seq_wr=220965 rand_rd=751625 cpu=108 composite=36630.5"
          },
          {
            "name": "mergerandom",
            "value": 484572.8773203379,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 771745 ops/sec | factor: 0.628 | P50: 0.4us | P99: 2.1us | P99.9: 3.2us\nthreads: 1 | elapsed: 0.26s | num: 200000 | iterations: 3 | runner: seq_wr=220965 rand_rd=751625 cpu=108 composite=36630.5"
          },
          {
            "name": "readwhilewriting",
            "value": 284647.4135803387,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 453338 ops/sec | factor: 0.628 | P50: 2.0us | P99: 4.9us | P99.9: 13.2us\nthreads: 1 | elapsed: 0.44s | num: 200000 | iterations: 3 | runner: seq_wr=220965 rand_rd=751625 cpu=108 composite=36630.5"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "49699333+dependabot[bot]@users.noreply.github.com",
            "name": "dependabot[bot]",
            "username": "dependabot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "851e583e3892f91b3a72ea1065bf7080d44c9dea",
          "message": "ci(deps): bump actions/create-github-app-token from 1 to 3 (#238)\n\nBumps\n[actions/create-github-app-token](https://github.com/actions/create-github-app-token)\nfrom 1 to 3.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/actions/create-github-app-token/releases\">actions/create-github-app-token's\nreleases</a>.</em></p>\n<blockquote>\n<h2>v3.0.0</h2>\n<h1><a\nhref=\"https://github.com/actions/create-github-app-token/compare/v2.2.2...v3.0.0\">3.0.0</a>\n(2026-03-14)</h1>\n<ul>\n<li>feat!: node 24 support (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/275\">#275</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/2e564a0bb8e7cc2b907b2401a2afe177882d4325\">2e564a0</a>)</li>\n<li>fix!: require <code>NODE_USE_ENV_PROXY</code> for proxy support (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/342\">#342</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/4451bcbc139f8124b0bf04f968ea2586b17df458\">4451bcb</a>)</li>\n</ul>\n<h3>Bug Fixes</h3>\n<ul>\n<li>remove custom proxy handling (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/143\">#143</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/dce0ab05f36f30b22fd14289fd36655c618e4e8e\">dce0ab0</a>)</li>\n</ul>\n<h3>BREAKING CHANGES</h3>\n<ul>\n<li>Custom proxy handling has been removed. If you use HTTP_PROXY or\nHTTPS_PROXY, you must now also set NODE_USE_ENV_PROXY=1 on the action\nstep.</li>\n<li>Requires <a\nhref=\"https://github.com/actions/runner/releases/tag/v2.327.1\">Actions\nRunner v2.327.1</a> or later if you are using a self-hosted runner.</li>\n</ul>\n<h2>v3.0.0-beta.6</h2>\n<h1><a\nhref=\"https://github.com/actions/create-github-app-token/compare/v3.0.0-beta.5...v3.0.0-beta.6\">3.0.0-beta.6</a>\n(2026-03-13)</h1>\n<h3>Bug Fixes</h3>\n<ul>\n<li><strong>deps:</strong> bump <code>@​actions/core</code> from 1.11.1\nto 3.0.0 (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/337\">#337</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/b04413352d4644ac2131b9a90c074f5e93ca18a1\">b044133</a>)</li>\n<li><strong>deps:</strong> bump minimatch from 9.0.5 to 9.0.9 (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/335\">#335</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/5cbc65624c9ddc4589492bda7c8b146223e8c3e4\">5cbc656</a>)</li>\n<li><strong>deps:</strong> bump the production-dependencies group with 4\nupdates (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/336\">#336</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/6bda5bc1410576b9a0879ce6076d53345485bba9\">6bda5bc</a>)</li>\n<li><strong>deps:</strong> bump undici from 7.16.0 to 7.18.2 (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/323\">#323</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/b4f638f48ee0dcdbb0bc646c48e4cb2a2de847fe\">b4f638f</a>)</li>\n</ul>\n<h2>v3.0.0-beta.5</h2>\n<h1><a\nhref=\"https://github.com/actions/create-github-app-token/compare/v3.0.0-beta.4...v3.0.0-beta.5\">3.0.0-beta.5</a>\n(2026-03-13)</h1>\n<ul>\n<li>fix!: require <code>NODE_USE_ENV_PROXY</code> for proxy support (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/342\">#342</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/d53a1cdfde844c958786293adcaf739ecb8b5eb9\">d53a1cd</a>)</li>\n</ul>\n<h3>BREAKING CHANGES</h3>\n<ul>\n<li>Custom proxy handling has been removed. If you use HTTP_PROXY or\nHTTPS_PROXY, you must now also set NODE_USE_ENV_PROXY=1 on the action\nstep.</li>\n</ul>\n<h2>v3.0.0-beta.4</h2>\n<h1><a\nhref=\"https://github.com/actions/create-github-app-token/compare/v3.0.0-beta.3...v3.0.0-beta.4\">3.0.0-beta.4</a>\n(2026-03-13)</h1>\n<h3>Bug Fixes</h3>\n<ul>\n<li><strong>deps:</strong> bump <code>@​octokit/auth-app</code> from\n7.2.1 to 8.0.1 (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/257\">#257</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/bef1eaf1c0ac2b148ee2a0a74c65fbe6db0631f1\">bef1eaf</a>)</li>\n<li><strong>deps:</strong> bump <code>@​octokit/request</code> from\n9.2.3 to 10.0.2 (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/256\">#256</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/5d7307be63501c0070c634b0ae8fec74e8208130\">5d7307b</a>)</li>\n<li><strong>deps:</strong> bump glob from 10.4.5 to 10.5.0 (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/305\">#305</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/5480f4325a18c025ee16d7e081413854624e9edc\">5480f43</a>)</li>\n<li><strong>deps:</strong> bump p-retry from 6.2.1 to 7.1.0 (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/294\">#294</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/dce3be8b284f45e65caed11a610e2bef738d15b4\">dce3be8</a>)</li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Changelog</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/actions/create-github-app-token/blob/main/CHANGELOG.md\">actions/create-github-app-token's\nchangelog</a>.</em></p>\n<blockquote>\n<h1>Changelog</h1>\n<h2><a\nhref=\"https://github.com/actions/create-github-app-token/compare/v3.1.1...v3.2.0\">3.2.0</a>\n(2026-05-12)</h2>\n<h3>Features</h3>\n<ul>\n<li>add support for enterprise-level GitHub Apps (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/263\">#263</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/952a2a7073df6bfa5f49bc469ec895b6ec1acea4\">952a2a7</a>)</li>\n<li>support full repository names in <code>repositories</code> input (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/372\">#372</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/85eb8dd41472213aed25d1a126460e0069138ab6\">85eb8dd</a>)</li>\n</ul>\n<h3>Bug Fixes</h3>\n<ul>\n<li><strong>deps:</strong> bump <code>@​actions/core</code> from 3.0.0\nto 3.0.1 in the production-dependencies group (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/364\">#364</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/43e5c345bfd4d4f3ecea019ad0042001a09dd857\">43e5c34</a>)</li>\n<li>validate private-key input (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/376\">#376</a>)\n(<a\nhref=\"https://github.com/actions/create-github-app-token/commit/f24bbd89643991c0de27ae823c01791b2c6bafdd\">f24bbd8</a>)</li>\n</ul>\n</blockquote>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/actions/create-github-app-token/commit/bcd2ba49218906704ab6c1aa796996da409d3eb1\"><code>bcd2ba4</code></a>\nchore(main): release 3.2.0 (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/370\">#370</a>)</li>\n<li><a\nhref=\"https://github.com/actions/create-github-app-token/commit/f24bbd89643991c0de27ae823c01791b2c6bafdd\"><code>f24bbd8</code></a>\nfix: validate private-key input (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/376\">#376</a>)</li>\n<li><a\nhref=\"https://github.com/actions/create-github-app-token/commit/363531b6d972a60a00b3f1e6bb139e5e6c764cd9\"><code>363531b</code></a>\ndocs: capitalize Git as a proper noun in README (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/374\">#374</a>)</li>\n<li><a\nhref=\"https://github.com/actions/create-github-app-token/commit/fd2801133e469d2950f2c5af5e591d6b2ad833c8\"><code>fd28011</code></a>\ndocs: update procedure to configure Git (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/287\">#287</a>)</li>\n<li><a\nhref=\"https://github.com/actions/create-github-app-token/commit/85eb8dd41472213aed25d1a126460e0069138ab6\"><code>85eb8dd</code></a>\nfeat: support full repository names in <code>repositories</code> input\n(<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/372\">#372</a>)</li>\n<li><a\nhref=\"https://github.com/actions/create-github-app-token/commit/c9aabb83728c3bd519212fa657ebc07e1f2a5dec\"><code>c9aabb8</code></a>\nbuild(deps-dev): bump yaml from 2.8.3 to 2.8.4 in the\ndevelopment-dependencie...</li>\n<li><a\nhref=\"https://github.com/actions/create-github-app-token/commit/e02e816e5591415258a53bf735aff57977dcd5e2\"><code>e02e816</code></a>\nbuild(deps-dev): bump undici from 7.24.6 to 8.2.0 (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/366\">#366</a>)</li>\n<li><a\nhref=\"https://github.com/actions/create-github-app-token/commit/8d835bfd37aa48fcb8e709925115857568d98bc4\"><code>8d835bf</code></a>\nbuild(deps-dev): bump esbuild from 0.27.4 to 0.28.0 in the\ndevelopment-depend...</li>\n<li><a\nhref=\"https://github.com/actions/create-github-app-token/commit/952a2a7073df6bfa5f49bc469ec895b6ec1acea4\"><code>952a2a7</code></a>\nfeat: add support for enterprise-level GitHub Apps (<a\nhref=\"https://redirect.github.com/actions/create-github-app-token/issues/263\">#263</a>)</li>\n<li><a\nhref=\"https://github.com/actions/create-github-app-token/commit/43e5c345bfd4d4f3ecea019ad0042001a09dd857\"><code>43e5c34</code></a>\nfix(deps): bump <code>@​actions/core</code> from 3.0.0 to 3.0.1 in the\nproduction-dependenc...</li>\n<li>Additional commits viewable in <a\nhref=\"https://github.com/actions/create-github-app-token/compare/v1...v3\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility\nscore](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=actions/create-github-app-token&package-manager=github_actions&previous-version=1&new-version=3)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop\nDependabot creating any more for this major version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop\nDependabot creating any more for this minor version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop\nDependabot creating any more for this dependency (unless you reopen the\nPR or upgrade to it yourself)\n\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-05-18T04:28:11+03:00",
          "tree_id": "cbde0e8f8b2e9d89c004e97eb2b647e9deec4840",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/851e583e3892f91b3a72ea1065bf7080d44c9dea"
        },
        "date": 1779068138073,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1250906.2007611105,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1968707 ops/sec | factor: 0.635 | P50: 0.3us | P99: 2.4us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=220694 rand_rd=728324 cpu=108 composite=36198.0"
          },
          {
            "name": "fillrandom",
            "value": 671721.063097547,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1057171 ops/sec | factor: 0.635 | P50: 0.7us | P99: 3.2us | P99.9: 7.2us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=220694 rand_rd=728324 cpu=108 composite=36198.0"
          },
          {
            "name": "readrandom",
            "value": 286636.6098312856,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 451116 ops/sec | factor: 0.635 | P50: 2.0us | P99: 6.6us | P99.9: 13.7us\nthreads: 1 | elapsed: 0.44s | num: 200000 | iterations: 3 | runner: seq_wr=220694 rand_rd=728324 cpu=108 composite=36198.0"
          },
          {
            "name": "readseq",
            "value": 1467978.1063982,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2310340 ops/sec | factor: 0.635 | P50: 0.2us | P99: 4.6us | P99.9: 9.4us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=220694 rand_rd=728324 cpu=108 composite=36198.0"
          },
          {
            "name": "seekrandom",
            "value": 221304.97444933915,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 348295 ops/sec | factor: 0.635 | P50: 2.5us | P99: 7.1us | P99.9: 14.3us\nthreads: 1 | elapsed: 0.57s | num: 200000 | iterations: 3 | runner: seq_wr=220694 rand_rd=728324 cpu=108 composite=36198.0"
          },
          {
            "name": "prefixscan",
            "value": 117655.3899176707,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 185169 ops/sec | factor: 0.635 | P50: 5.1us | P99: 6.6us | P99.9: 16.8us\nthreads: 1 | elapsed: 1.08s | num: 200000 | iterations: 3 | runner: seq_wr=220694 rand_rd=728324 cpu=108 composite=36198.0"
          },
          {
            "name": "overwrite",
            "value": 709292.166768447,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1116301 ops/sec | factor: 0.635 | P50: 0.7us | P99: 3.0us | P99.9: 7.1us\nthreads: 1 | elapsed: 0.18s | num: 200000 | iterations: 3 | runner: seq_wr=220694 rand_rd=728324 cpu=108 composite=36198.0"
          },
          {
            "name": "mergerandom",
            "value": 489724.6912560415,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 770741 ops/sec | factor: 0.635 | P50: 0.4us | P99: 0.5us | P99.9: 3.5us\nthreads: 1 | elapsed: 0.26s | num: 200000 | iterations: 3 | runner: seq_wr=220694 rand_rd=728324 cpu=108 composite=36198.0"
          },
          {
            "name": "readwhilewriting",
            "value": 268454.28139758686,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 422500 ops/sec | factor: 0.635 | P50: 2.2us | P99: 5.6us | P99.9: 14.4us\nthreads: 1 | elapsed: 0.47s | num: 200000 | iterations: 3 | runner: seq_wr=220694 rand_rd=728324 cpu=108 composite=36198.0"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "49699333+dependabot[bot]@users.noreply.github.com",
            "name": "dependabot[bot]",
            "username": "dependabot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1f69645faec9738039d9457d1e9a00535d992612",
          "message": "chore(deps): update strum requirement from 0.27.2 to 0.28.0 (#242)\n\nUpdates the requirements on\n[strum](https://github.com/Peternator7/strum) to permit the latest\nversion.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/Peternator7/strum/releases\">strum's\nreleases</a>.</em></p>\n<blockquote>\n<h2>v0.28.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>Updated the CHANGELOG for the new release by <a\nhref=\"https://github.com/Peternator7\"><code>@​Peternator7</code></a> in\n<a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/452\">Peternator7/strum#452</a></li>\n<li>Add missing <code>#[automatically_derived]</code> by <a\nhref=\"https://github.com/clechasseur\"><code>@​clechasseur</code></a> in\n<a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/462\">Peternator7/strum#462</a></li>\n<li>Bump MSRV to v1.71 by <a\nhref=\"https://github.com/paolobarbolini\"><code>@​paolobarbolini</code></a>\nin <a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/466\">Peternator7/strum#466</a></li>\n<li>Use absolute paths in proc macro by <a\nhref=\"https://github.com/Snarpix\"><code>@​Snarpix</code></a> in <a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/469\">Peternator7/strum#469</a></li>\n<li>Upgrade <code>phf</code> to v0.13 by <a\nhref=\"https://github.com/paolobarbolini\"><code>@​paolobarbolini</code></a>\nin <a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/465\">Peternator7/strum#465</a></li>\n<li>docs: Fix typo by <a\nhref=\"https://github.com/j-g00da\"><code>@​j-g00da</code></a> in <a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/463\">Peternator7/strum#463</a></li>\n<li>Allow any kind of passthrough attributes on\n<code>EnumDiscriminants</code> by <a\nhref=\"https://github.com/clechasseur\"><code>@​clechasseur</code></a> in\n<a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/461\">Peternator7/strum#461</a></li>\n<li>Fix existing cargo fmt + clippy issues and add GH actions by <a\nhref=\"https://github.com/Peternator7\"><code>@​Peternator7</code></a> in\n<a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/473\">Peternator7/strum#473</a></li>\n<li>[EnumDiscriminant] Automatically add Default by <a\nhref=\"https://github.com/Peternator7\"><code>@​Peternator7</code></a> in\n<a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/474\">Peternator7/strum#474</a></li>\n<li>Honor parse_err_ty attribute when the enum has a default variant by\n<a href=\"https://github.com/scovich\"><code>@​scovich</code></a> in <a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/431\">Peternator7/strum#431</a></li>\n<li>Make TryFrom and FromStr infallible if there's a default by <a\nhref=\"https://github.com/Peternator7\"><code>@​Peternator7</code></a> in\n<a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/476\">Peternator7/strum#476</a></li>\n<li>Implement core::fmt::Display for ParseError by <a\nhref=\"https://github.com/Peternator7\"><code>@​Peternator7</code></a> in\n<a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/477\">Peternator7/strum#477</a></li>\n<li>Peternator7/0.28 by <a\nhref=\"https://github.com/Peternator7\"><code>@​Peternator7</code></a> in\n<a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/475\">Peternator7/strum#475</a></li>\n</ul>\n<h2>New Contributors</h2>\n<ul>\n<li><a\nhref=\"https://github.com/clechasseur\"><code>@​clechasseur</code></a>\nmade their first contribution in <a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/462\">Peternator7/strum#462</a></li>\n<li><a href=\"https://github.com/Snarpix\"><code>@​Snarpix</code></a> made\ntheir first contribution in <a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/469\">Peternator7/strum#469</a></li>\n<li><a href=\"https://github.com/j-g00da\"><code>@​j-g00da</code></a> made\ntheir first contribution in <a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/463\">Peternator7/strum#463</a></li>\n<li><a href=\"https://github.com/scovich\"><code>@​scovich</code></a> made\ntheir first contribution in <a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/431\">Peternator7/strum#431</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a\nhref=\"https://github.com/Peternator7/strum/compare/v0.27.2...v0.28.0\">https://github.com/Peternator7/strum/compare/v0.27.2...v0.28.0</a></p>\n</blockquote>\n</details>\n<details>\n<summary>Changelog</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/Peternator7/strum/blob/master/CHANGELOG.md\">strum's\nchangelog</a>.</em></p>\n<blockquote>\n<h2>0.28.0</h2>\n<ul>\n<li>\n<p><a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/461\">#461</a>:\nAllow any kind of passthrough attributes on\n<code>EnumDiscriminants</code>.</p>\n<ul>\n<li>Previously only list-style attributes (e.g.\n<code>#[strum_discriminants(derive(...))]</code>) were supported. Now\npath-only\n(e.g. <code>#[strum_discriminants(non_exhaustive)]</code>) and\nname/value (e.g. <code>#[strum_discriminants(doc =\n&quot;foo&quot;)]</code>)\nattributes are also supported.</li>\n</ul>\n</li>\n<li>\n<p><a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/462\">#462</a>:\nAdd missing <code>#[automatically_derived]</code> to generated impls not\ncovered by <a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/444\">#444</a>.</p>\n</li>\n<li>\n<p><a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/466\">#466</a>:\nBump MSRV to 1.71, required to keep up with updated <code>syn</code> and\n<code>windows-sys</code> dependencies. This is a breaking change if\nyou're on an old version of rust.</p>\n</li>\n<li>\n<p><a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/469\">#469</a>:\nUse absolute paths in generated proc macro code to avoid\npotential name conflicts.</p>\n</li>\n<li>\n<p><a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/465\">#465</a>:\nUpgrade <code>phf</code> dependency to v0.13.</p>\n</li>\n<li>\n<p><a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/473\">#473</a>:\nFix <code>cargo fmt</code> / <code>clippy</code> issues and add GitHub\nActions CI.</p>\n</li>\n<li>\n<p><a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/477\">#477</a>:\n<code>strum::ParseError</code> now implements\n<code>core::fmt::Display</code> instead\n<code>std::fmt::Display</code> to make it <code>#[no_std]</code>\ncompatible. Note the <code>Error</code> trait wasn't available in core\nuntil <code>1.81</code>\nso <code>strum::ParseError</code> still only implements that in std.</p>\n</li>\n<li>\n<p><a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/476\">#476</a>:\n<strong>Breaking Change</strong> - <code>EnumString</code> now\nimplements <code>From&lt;&amp;str&gt;</code>\n(infallible) instead of <code>TryFrom&lt;&amp;str&gt;</code> when the\nenum has a <code>#[strum(default)]</code> variant. This more accurately\nreflects that parsing cannot fail in that case. If you need the old\n<code>TryFrom</code> behavior, you can opt back in using\n<code>parse_error_ty</code> and <code>parse_error_fn</code>:</p>\n<pre lang=\"rust\"><code>#[derive(EnumString)]\n#[strum(parse_error_ty = strum::ParseError, parse_error_fn =\nmake_error)]\npub enum Color {\n    Red,\n    #[strum(default)]\n    Other(String),\n}\n<p>fn make_error(x: &amp;str) -&gt; strum::ParseError {\nstrum::ParseError::VariantNotFound\n}\n</code></pre></p>\n</li>\n<li>\n<p><a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/431\">#431</a>:\nFix bug where <code>EnumString</code> ignored the\n<code>parse_err_ty</code>\nattribute when the enum had a <code>#[strum(default)]</code>\nvariant.</p>\n</li>\n<li>\n<p><a\nhref=\"https://redirect.github.com/Peternator7/strum/pull/474\">#474</a>:\nEnumDiscriminants will now copy <code>default</code> over from the\noriginal enum to the Discriminant enum.</p>\n<pre lang=\"rust\"><code>#[derive(Debug, Default, EnumDiscriminants)]\n#[strum_discriminants(derive(Default))] // &lt;- Remove this in 0.28.\nenum MyEnum {\n    #[default] // &lt;- Will be the #[default] on the MyEnumDiscriminant\n    #[strum_discriminants(default)] // &lt;- Remove this in 0.28\n    Variant0,\n    Variant1 { a: NonDefault },\n}\n</code></pre>\n</li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/Peternator7/strum/commit/7376771128834d28bb9beba5c39846cba62e71ec\"><code>7376771</code></a>\nPeternator7/0.28 (<a\nhref=\"https://redirect.github.com/Peternator7/strum/issues/475\">#475</a>)</li>\n<li><a\nhref=\"https://github.com/Peternator7/strum/commit/26e63cd964a2e364331a5dd977d589bb9f649d8c\"><code>26e63cd</code></a>\nDisplay exists in core (<a\nhref=\"https://redirect.github.com/Peternator7/strum/issues/477\">#477</a>)</li>\n<li><a\nhref=\"https://github.com/Peternator7/strum/commit/9334c728eedaa8a992d1388a8f4564bbccad1934\"><code>9334c72</code></a>\nMake TryFrom and FromStr infallible if there's a default (<a\nhref=\"https://redirect.github.com/Peternator7/strum/issues/476\">#476</a>)</li>\n<li><a\nhref=\"https://github.com/Peternator7/strum/commit/0ccbbf823c16e827afc263182cd55e99e3b2a52e\"><code>0ccbbf8</code></a>\nHonor parse_err_ty attribute when the enum has a default variant (<a\nhref=\"https://redirect.github.com/Peternator7/strum/issues/431\">#431</a>)</li>\n<li><a\nhref=\"https://github.com/Peternator7/strum/commit/2c9e5a9259189ce8397f2f4967060240c6bafd74\"><code>2c9e5a9</code></a>\nAutomatically add Default implementation to EnumDiscriminant if it\nexists on ...</li>\n<li><a\nhref=\"https://github.com/Peternator7/strum/commit/e241243e48359b8b811b8eaccdcfa1ae87138e0d\"><code>e241243</code></a>\nFix existing cargo fmt + clippy issues and add GH actions (<a\nhref=\"https://redirect.github.com/Peternator7/strum/issues/473\">#473</a>)</li>\n<li><a\nhref=\"https://github.com/Peternator7/strum/commit/639b67fefd20eaead1c5d2ea794e9afe70a00312\"><code>639b67f</code></a>\nfeat: allow any kind of passthrough attributes on\n<code>EnumDiscriminants</code> (<a\nhref=\"https://redirect.github.com/Peternator7/strum/issues/461\">#461</a>)</li>\n<li><a\nhref=\"https://github.com/Peternator7/strum/commit/0ea1e2d0fd1460e7492ea32e6b460394d9199ff8\"><code>0ea1e2d</code></a>\ndocs: Fix typo (<a\nhref=\"https://redirect.github.com/Peternator7/strum/issues/463\">#463</a>)</li>\n<li><a\nhref=\"https://github.com/Peternator7/strum/commit/36c051b91086b37d531c63ccf5a49266832a846d\"><code>36c051b</code></a>\nUpgrade <code>phf</code> to v0.13 (<a\nhref=\"https://redirect.github.com/Peternator7/strum/issues/465\">#465</a>)</li>\n<li><a\nhref=\"https://github.com/Peternator7/strum/commit/9328b38617dc6f4a3bc5fdac03883d3fc766cf34\"><code>9328b38</code></a>\nUse absolute paths in proc macro (<a\nhref=\"https://redirect.github.com/Peternator7/strum/issues/469\">#469</a>)</li>\n<li>Additional commits viewable in <a\nhref=\"https://github.com/Peternator7/strum/compare/v0.27.2...v0.28.0\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop\nDependabot creating any more for this major version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop\nDependabot creating any more for this minor version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop\nDependabot creating any more for this dependency (unless you reopen the\nPR or upgrade to it yourself)\n\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-05-18T04:28:47+03:00",
          "tree_id": "49e1104acf18ed7b4b9baeff7a006c7e4599585e",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/1f69645faec9738039d9457d1e9a00535d992612"
        },
        "date": 1779068275490,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1211744.9985402026,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2143370 ops/sec | factor: 0.565 | P50: 0.3us | P99: 2.0us | P99.9: 5.0us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=214887 rand_rd=908322 cpu=123 composite=40683.1"
          },
          {
            "name": "fillrandom",
            "value": 684673.292002701,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1211070 ops/sec | factor: 0.565 | P50: 0.7us | P99: 2.5us | P99.9: 6.0us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=214887 rand_rd=908322 cpu=123 composite=40683.1"
          },
          {
            "name": "readrandom",
            "value": 278057.330635246,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 491836 ops/sec | factor: 0.565 | P50: 1.8us | P99: 5.5us | P99.9: 13.7us\nthreads: 1 | elapsed: 0.41s | num: 200000 | iterations: 3 | runner: seq_wr=214887 rand_rd=908322 cpu=123 composite=40683.1"
          },
          {
            "name": "readseq",
            "value": 1440675.8173293823,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2548310 ops/sec | factor: 0.565 | P50: 0.2us | P99: 3.8us | P99.9: 7.6us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=214887 rand_rd=908322 cpu=123 composite=40683.1"
          },
          {
            "name": "seekrandom",
            "value": 212096.93300714018,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 375163 ops/sec | factor: 0.565 | P50: 2.3us | P99: 6.0us | P99.9: 13.9us\nthreads: 1 | elapsed: 0.53s | num: 200000 | iterations: 3 | runner: seq_wr=214887 rand_rd=908322 cpu=123 composite=40683.1"
          },
          {
            "name": "prefixscan",
            "value": 104691.83675063252,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 185182 ops/sec | factor: 0.565 | P50: 5.0us | P99: 7.1us | P99.9: 16.9us\nthreads: 1 | elapsed: 1.08s | num: 200000 | iterations: 3 | runner: seq_wr=214887 rand_rd=908322 cpu=123 composite=40683.1"
          },
          {
            "name": "overwrite",
            "value": 689488.4920558097,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1219588 ops/sec | factor: 0.565 | P50: 0.7us | P99: 2.6us | P99.9: 6.1us\nthreads: 1 | elapsed: 0.16s | num: 200000 | iterations: 3 | runner: seq_wr=214887 rand_rd=908322 cpu=123 composite=40683.1"
          },
          {
            "name": "mergerandom",
            "value": 414642.7485519042,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 733432 ops/sec | factor: 0.565 | P50: 0.3us | P99: 0.6us | P99.9: 3.5us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=214887 rand_rd=908322 cpu=123 composite=40683.1"
          },
          {
            "name": "readwhilewriting",
            "value": 257359.83518161677,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 455226 ops/sec | factor: 0.565 | P50: 2.0us | P99: 4.5us | P99.9: 15.4us\nthreads: 1 | elapsed: 0.44s | num: 200000 | iterations: 3 | runner: seq_wr=214887 rand_rd=908322 cpu=123 composite=40683.1"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "49699333+dependabot[bot]@users.noreply.github.com",
            "name": "dependabot[bot]",
            "username": "dependabot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bf604f5cd118f740be47ddf351f96b8d74bbd54d",
          "message": "ci(deps): bump codecov/codecov-action from 5 to 6 (#240)\n\nBumps\n[codecov/codecov-action](https://github.com/codecov/codecov-action) from\n5 to 6.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/codecov/codecov-action/releases\">codecov/codecov-action's\nreleases</a>.</em></p>\n<blockquote>\n<h2>v6.0.0</h2>\n<h2>⚠️ This version introduces support for node24 which make cause\nbreaking changes for systems that do not currently support node24.\n⚠️</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>Revert &quot;Revert &quot;build(deps): bump actions/github-script\nfrom 7.0.1 to 8.0.0&quot;&quot; by <a\nhref=\"https://github.com/thomasrockhu-codecov\"><code>@​thomasrockhu-codecov</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1929\">codecov/codecov-action#1929</a></li>\n<li>Th/6.0.0 by <a\nhref=\"https://github.com/thomasrockhu-codecov\"><code>@​thomasrockhu-codecov</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1928\">codecov/codecov-action#1928</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a\nhref=\"https://github.com/codecov/codecov-action/compare/v5.5.4...v6.0.0\">https://github.com/codecov/codecov-action/compare/v5.5.4...v6.0.0</a></p>\n<h2>v5.5.4</h2>\n<p>This is a mirror of <code>v5.5.2</code>. <code>v6</code> will be\nreleased which requires <code>node24</code></p>\n<h2>What's Changed</h2>\n<ul>\n<li>Revert &quot;build(deps): bump actions/github-script from 7.0.1 to\n8.0.0&quot; by <a\nhref=\"https://github.com/thomasrockhu-codecov\"><code>@​thomasrockhu-codecov</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1926\">codecov/codecov-action#1926</a></li>\n<li>chore(release): 5.5.4 by <a\nhref=\"https://github.com/thomasrockhu-codecov\"><code>@​thomasrockhu-codecov</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1927\">codecov/codecov-action#1927</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a\nhref=\"https://github.com/codecov/codecov-action/compare/v5.5.3...v5.5.4\">https://github.com/codecov/codecov-action/compare/v5.5.3...v5.5.4</a></p>\n<h2>v5.5.3</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>build(deps): bump actions/github-script from 7.0.1 to 8.0.0 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1874\">codecov/codecov-action#1874</a></li>\n<li>chore(release): bump to 5.5.3 by <a\nhref=\"https://github.com/thomasrockhu-codecov\"><code>@​thomasrockhu-codecov</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1922\">codecov/codecov-action#1922</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a\nhref=\"https://github.com/codecov/codecov-action/compare/v5.5.2...v5.5.3\">https://github.com/codecov/codecov-action/compare/v5.5.2...v5.5.3</a></p>\n<h2>v5.5.2</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>check gpg only when skip-validation = false by <a\nhref=\"https://github.com/maxweng-sentry\"><code>@​maxweng-sentry</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1894\">codecov/codecov-action#1894</a></li>\n<li>chore: <code>disable_search</code> alignment by <a\nhref=\"https://github.com/freemanzMrojo\"><code>@​freemanzMrojo</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1881\">codecov/codecov-action#1881</a></li>\n<li>chore(release): 5.5.2 by <a\nhref=\"https://github.com/thomasrockhu-codecov\"><code>@​thomasrockhu-codecov</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1902\">codecov/codecov-action#1902</a></li>\n</ul>\n<h2>New Contributors</h2>\n<ul>\n<li><a\nhref=\"https://github.com/maxweng-sentry\"><code>@​maxweng-sentry</code></a>\nmade their first contribution in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1894\">codecov/codecov-action#1894</a></li>\n<li><a\nhref=\"https://github.com/freemanzMrojo\"><code>@​freemanzMrojo</code></a>\nmade their first contribution in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1881\">codecov/codecov-action#1881</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a\nhref=\"https://github.com/codecov/codecov-action/compare/v5.5.1...v5.5.2\">https://github.com/codecov/codecov-action/compare/v5.5.1...v5.5.2</a></p>\n<h2>v5.5.1</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>build(deps): bump ossf/scorecard-action from 2.4.1 to 2.4.2 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1833\">codecov/codecov-action#1833</a></li>\n<li>build(deps): bump github/codeql-action from 3.28.18 to 3.29.9 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1861\">codecov/codecov-action#1861</a></li>\n<li>Document a <code>codecov-cli</code> version reference example by <a\nhref=\"https://github.com/webknjaz\"><code>@​webknjaz</code></a> in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1774\">codecov/codecov-action#1774</a></li>\n<li>docs: fix typo in README by <a\nhref=\"https://github.com/datalater\"><code>@​datalater</code></a> in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1866\">codecov/codecov-action#1866</a></li>\n<li>fix: update to use local app/ dir by <a\nhref=\"https://github.com/thomasrockhu-codecov\"><code>@​thomasrockhu-codecov</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1872\">codecov/codecov-action#1872</a></li>\n<li>build(deps): bump github/codeql-action from 3.29.9 to 3.29.11 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1867\">codecov/codecov-action#1867</a></li>\n<li>build(deps): bump actions/checkout from 4.2.2 to 5.0.0 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1868\">codecov/codecov-action#1868</a></li>\n<li>fix: overwrite pr number on fork by <a\nhref=\"https://github.com/thomasrockhu-codecov\"><code>@​thomasrockhu-codecov</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1871\">codecov/codecov-action#1871</a></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Changelog</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/codecov/codecov-action/blob/main/CHANGELOG.md\">codecov/codecov-action's\nchangelog</a>.</em></p>\n<blockquote>\n<h2>v5.5.2</h2>\n<h3>What's Changed</h3>\n<p><strong>Full Changelog</strong>: <a\nhref=\"https://github.com/codecov/codecov-action/compare/v5.5.1..v5.5.2\">https://github.com/codecov/codecov-action/compare/v5.5.1..v5.5.2</a></p>\n<h2>v5.5.1</h2>\n<h3>What's Changed</h3>\n<ul>\n<li>fix: overwrite pr number on fork by <a\nhref=\"https://github.com/thomasrockhu-codecov\"><code>@​thomasrockhu-codecov</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1871\">codecov/codecov-action#1871</a></li>\n<li>build(deps): bump actions/checkout from 4.2.2 to 5.0.0 by\n<code>@​app/dependabot</code> in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1868\">codecov/codecov-action#1868</a></li>\n<li>build(deps): bump github/codeql-action from 3.29.9 to 3.29.11 by\n<code>@​app/dependabot</code> in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1867\">codecov/codecov-action#1867</a></li>\n<li>fix: update to use local app/ dir by <a\nhref=\"https://github.com/thomasrockhu-codecov\"><code>@​thomasrockhu-codecov</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1872\">codecov/codecov-action#1872</a></li>\n<li>docs: fix typo in README by <a\nhref=\"https://github.com/datalater\"><code>@​datalater</code></a> in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1866\">codecov/codecov-action#1866</a></li>\n<li>Document a <code>codecov-cli</code> version reference example by <a\nhref=\"https://github.com/webknjaz\"><code>@​webknjaz</code></a> in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1774\">codecov/codecov-action#1774</a></li>\n<li>build(deps): bump github/codeql-action from 3.28.18 to 3.29.9 by\n<code>@​app/dependabot</code> in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1861\">codecov/codecov-action#1861</a></li>\n<li>build(deps): bump ossf/scorecard-action from 2.4.1 to 2.4.2 by\n<code>@​app/dependabot</code> in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1833\">codecov/codecov-action#1833</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a\nhref=\"https://github.com/codecov/codecov-action/compare/v5.5.0..v5.5.1\">https://github.com/codecov/codecov-action/compare/v5.5.0..v5.5.1</a></p>\n<h2>v5.5.0</h2>\n<h3>What's Changed</h3>\n<ul>\n<li>feat: upgrade wrapper to 0.2.4 by <a\nhref=\"https://github.com/jviall\"><code>@​jviall</code></a> in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1864\">codecov/codecov-action#1864</a></li>\n<li>Pin actions/github-script by Git SHA by <a\nhref=\"https://github.com/martincostello\"><code>@​martincostello</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1859\">codecov/codecov-action#1859</a></li>\n<li>fix: check reqs exist by <a\nhref=\"https://github.com/joseph-sentry\"><code>@​joseph-sentry</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1835\">codecov/codecov-action#1835</a></li>\n<li>fix: Typo in README by <a\nhref=\"https://github.com/spalmurray\"><code>@​spalmurray</code></a> in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1838\">codecov/codecov-action#1838</a></li>\n<li>docs: Refine OIDC docs by <a\nhref=\"https://github.com/spalmurray\"><code>@​spalmurray</code></a> in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1837\">codecov/codecov-action#1837</a></li>\n<li>build(deps): bump github/codeql-action from 3.28.17 to 3.28.18 by\n<code>@​app/dependabot</code> in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1829\">codecov/codecov-action#1829</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a\nhref=\"https://github.com/codecov/codecov-action/compare/v5.4.3..v5.5.0\">https://github.com/codecov/codecov-action/compare/v5.4.3..v5.5.0</a></p>\n<h2>v5.4.3</h2>\n<h3>What's Changed</h3>\n<ul>\n<li>build(deps): bump github/codeql-action from 3.28.13 to 3.28.17 by\n<code>@​app/dependabot</code> in <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1822\">codecov/codecov-action#1822</a></li>\n<li>fix: OIDC on forks by <a\nhref=\"https://github.com/joseph-sentry\"><code>@​joseph-sentry</code></a>\nin <a\nhref=\"https://redirect.github.com/codecov/codecov-action/pull/1823\">codecov/codecov-action#1823</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a\nhref=\"https://github.com/codecov/codecov-action/compare/v5.4.2..v5.4.3\">https://github.com/codecov/codecov-action/compare/v5.4.2..v5.4.3</a></p>\n<h2>v5.4.2</h2>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/codecov/codecov-action/commit/57e3a136b779b570ffcdbf80b3bdc90e7fab3de2\"><code>57e3a13</code></a>\nTh/6.0.0 (<a\nhref=\"https://redirect.github.com/codecov/codecov-action/issues/1928\">#1928</a>)</li>\n<li><a\nhref=\"https://github.com/codecov/codecov-action/commit/f67d33dda8a42b51c42a8318a1f66468119e898b\"><code>f67d33d</code></a>\nRevert &quot;Revert &quot;build(deps): bump actions/github-script from\n7.0.1 to 8.0.0&quot;&quot;...</li>\n<li>See full diff in <a\nhref=\"https://github.com/codecov/codecov-action/compare/v5...v6\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility\nscore](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=codecov/codecov-action&package-manager=github_actions&previous-version=5&new-version=6)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop\nDependabot creating any more for this major version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop\nDependabot creating any more for this minor version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop\nDependabot creating any more for this dependency (unless you reopen the\nPR or upgrade to it yourself)\n\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-05-18T04:28:33+03:00",
          "tree_id": "906fb0f59b3e6f7b2ad2b23471eb1b4c7a69d730",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/bf604f5cd118f740be47ddf351f96b8d74bbd54d"
        },
        "date": 1779068277335,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1155952.507337531,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2110179 ops/sec | factor: 0.548 | P50: 0.3us | P99: 2.0us | P99.9: 5.0us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=228831 rand_rd=937317 cpu=123 composite=41986.3"
          },
          {
            "name": "fillrandom",
            "value": 657740.698617894,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1200699 ops/sec | factor: 0.548 | P50: 0.7us | P99: 2.6us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=228831 rand_rd=937317 cpu=123 composite=41986.3"
          },
          {
            "name": "readrandom",
            "value": 273774.5253200835,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 499773 ops/sec | factor: 0.548 | P50: 1.8us | P99: 5.3us | P99.9: 13.3us\nthreads: 1 | elapsed: 0.40s | num: 200000 | iterations: 3 | runner: seq_wr=228831 rand_rd=937317 cpu=123 composite=41986.3"
          },
          {
            "name": "readseq",
            "value": 1395651.104139596,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2547747 ops/sec | factor: 0.548 | P50: 0.2us | P99: 3.8us | P99.9: 7.7us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=228831 rand_rd=937317 cpu=123 composite=41986.3"
          },
          {
            "name": "seekrandom",
            "value": 197085.4916460746,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 359778 ops/sec | factor: 0.548 | P50: 2.4us | P99: 6.0us | P99.9: 14.3us\nthreads: 1 | elapsed: 0.56s | num: 200000 | iterations: 3 | runner: seq_wr=228831 rand_rd=937317 cpu=123 composite=41986.3"
          },
          {
            "name": "prefixscan",
            "value": 98644.85397199818,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 180075 ops/sec | factor: 0.548 | P50: 5.1us | P99: 7.6us | P99.9: 17.1us\nthreads: 1 | elapsed: 1.11s | num: 200000 | iterations: 3 | runner: seq_wr=228831 rand_rd=937317 cpu=123 composite=41986.3"
          },
          {
            "name": "overwrite",
            "value": 676745.5119889842,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1235392 ops/sec | factor: 0.548 | P50: 0.7us | P99: 2.5us | P99.9: 5.6us\nthreads: 1 | elapsed: 0.16s | num: 200000 | iterations: 3 | runner: seq_wr=228831 rand_rd=937317 cpu=123 composite=41986.3"
          },
          {
            "name": "mergerandom",
            "value": 405877.5416539711,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 740925 ops/sec | factor: 0.548 | P50: 0.4us | P99: 1.9us | P99.9: 3.9us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=228831 rand_rd=937317 cpu=123 composite=41986.3"
          },
          {
            "name": "readwhilewriting",
            "value": 254712.60374911118,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 464975 ops/sec | factor: 0.548 | P50: 2.0us | P99: 5.3us | P99.9: 13.7us\nthreads: 1 | elapsed: 0.43s | num: 200000 | iterations: 3 | runner: seq_wr=228831 rand_rd=937317 cpu=123 composite=41986.3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "49699333+dependabot[bot]@users.noreply.github.com",
            "name": "dependabot[bot]",
            "username": "dependabot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b441d571717624461e02c82870c31aefd7246333",
          "message": "chore(deps): update nanoid requirement from 0.4.0 to 0.5.0 (#241)\n\n[//]: # (dependabot-start)\n⚠️  **Dependabot is rebasing this PR** ⚠️ \n\nRebasing might not happen immediately, so don't worry if this takes some\ntime.\n\nNote: if you make any changes to this PR yourself, they will take\nprecedence over the rebase.\n\n---\n\n[//]: # (dependabot-end)\n\nUpdates the requirements on\n[nanoid](https://github.com/mrdimidium/nanoid) to permit the latest\nversion.\n<details>\n<summary>Changelog</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/mrdimidium/nanoid/blob/main/CHANGELOG.md\">nanoid's\nchangelog</a>.</em></p>\n<blockquote>\n<h2>0.5.0</h2>\n<ul>\n<li>Bump <code>rand</code> to 0.9</li>\n<li>Add <code>rngs::thread_local</code> random source (<a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/36\">#36</a>)</li>\n<li><code>format</code> now accepts any <code>FnMut(usize) -&gt;\nVec&lt;u8&gt;</code> random generator, enabling\nseeded and stateful RNGs (<a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/32\">#32</a>,\n<a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/41\">#41</a>).\nNon-capturing <code>fn(usize) -&gt; Vec&lt;u8&gt;</code>\ncallers continue to work unchanged.</li>\n<li><code>nanoid!</code> macro size argument now accepts any expression,\nnot only a single\ntoken (<a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/28\">#28</a>)</li>\n<li>Specialized fast path for alphabets whose size is a power of two (<a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/35\">#35</a>).\nNote: for seeded RNGs paired with a power-of-two alphabet (e.g.\n<code>SAFE</code>, the\nnew <code>HEX_*</code> presets), the number of random bytes consumed per\nID has changed\n— the output for a given seed will differ from 0.4.0.</li>\n<li>Add <code>alphabet::HEX_LOWERCASE</code> and\n<code>alphabet::HEX_UPPERCASE</code> presets (<a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/39\">#39</a>)</li>\n<li>Optional <code>smartstring</code> feature for small-string-optimized\noutput (<a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/29\">#29</a>)</li>\n<li>Refreshed CI (GitHub Actions across OS matrix), drop\nTravis/AppVeyor</li>\n<li>Switched benchmarks to <code>criterion</code></li>\n</ul>\n<h2>0.4.0</h2>\n<ul>\n<li>merge <a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/25\">#25</a>,\nfrom <a href=\"https://github.com/fundon\"><code>@​fundon</code></a>: bump\nthe rand#0.8</li>\n<li>merge <a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/18\">#18</a>,\nfrom <a\nhref=\"https://github.com/svenstaro\"><code>@​svenstaro</code></a></li>\n<li>merge <a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/21\">#21</a>,\nfrom <a\nhref=\"https://github.com/svenstaro\"><code>@​svenstaro</code></a></li>\n<li>merge <a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/19\">#19</a>,\nfrom <a\nhref=\"https://github.com/svenstaro\"><code>@​svenstaro</code></a></li>\n<li>merge <a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/20\">#20</a>,\nfrom <a\nhref=\"https://github.com/svenstaro\"><code>@​svenstaro</code></a></li>\n<li>merge <a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/16\">#16</a>,\nfrom <a href=\"https://github.com/Exr0n\"><code>@​Exr0n</code></a></li>\n<li>merge <a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/15\">#15</a>,\nfrom <a href=\"https://github.com/Exr0n\"><code>@​Exr0n</code></a></li>\n<li>merge <a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/10\">#10</a>,\nfrom <a href=\"https://github.com/nbraud\"><code>@​nbraud</code></a></li>\n</ul>\n<h2>0.3.0</h2>\n<ul>\n<li>merge <a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/3\">#3</a>,\nfrom <a\nhref=\"https://github.com/TheIronBorn\"><code>@​TheIronBorn</code></a>:\nvarious small improvements</li>\n<li>merge <a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/4\">#4</a>,\nfrom <a\nhref=\"https://github.com/delimitry\"><code>@​delimitry</code></a>: fix\ntypo in function name</li>\n<li>Replace the <code>~</code> to <code>-</code> in alphabet</li>\n<li>Add the common macros</li>\n<li>Refactor structure. Remove pseudo-fast generator. Move format in\n<code>lib.rs</code></li>\n</ul>\n<h2>0.2.0</h2>\n<ul>\n<li>Added support for Windows</li>\n<li>Moved to system randomness generator</li>\n</ul>\n<h2>0.1.3</h2>\n<ul>\n<li>Renamed the safe alphabet</li>\n<li>Added readme from rustdoc</li>\n</ul>\n<h2>0.1.2</h2>\n<ul>\n<li>Updated the random number engine.</li>\n<li>Fixed bugs in documentation.</li>\n</ul>\n<h2>0.1.1</h2>\n<ul>\n<li>Integrated performance tests</li>\n<li>Added example of custom random number generator.</li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/mrdimidium/nanoid/commit/359c02d6f87260bd431e19374ccfca2890fdab1e\"><code>359c02d</code></a>\nchore: 0.5.0 release</li>\n<li><a\nhref=\"https://github.com/mrdimidium/nanoid/commit/f0ad07fc16b96b4c00d76fa10853fa377ad8ee05\"><code>f0ad07f</code></a>\n<a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/39\">#39</a>:\nAdd hex alphabets</li>\n<li><a\nhref=\"https://github.com/mrdimidium/nanoid/commit/7f961f211fa6a1162bb0fd6bd5c6ff195005ec00\"><code>7f961f2</code></a>\nMerge pull request <a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/35\">#35</a>\nfrom tmccombs/fast-impl</li>\n<li><a\nhref=\"https://github.com/mrdimidium/nanoid/commit/91a79fc9f11e463109dcba342d684ffdd5291862\"><code>91a79fc</code></a>\nUpdate fast impl for actual format signature</li>\n<li><a\nhref=\"https://github.com/mrdimidium/nanoid/commit/ed800e971f01b589cad8d2c8b09973f8eacfd1a1\"><code>ed800e9</code></a>\nfeat: Use specialized implementation for alphabets with size 2^n</li>\n<li><a\nhref=\"https://github.com/mrdimidium/nanoid/commit/fef0b2eace7dbda294cca78fd5c8e96c188a00bc\"><code>fef0b2e</code></a>\nMerge pull request <a\nhref=\"https://redirect.github.com/mrdimidium/nanoid/issues/41\">#41</a>\nfrom sidarth164/sid/fnmut</li>\n<li><a\nhref=\"https://github.com/mrdimidium/nanoid/commit/61e0606f9c558171238aa9caf29d02bf90fb7806\"><code>61e0606</code></a>\ndocs: update README and added an example</li>\n<li><a\nhref=\"https://github.com/mrdimidium/nanoid/commit/2004ff99bbe549e7323ac01bd43070aa1f11c33e\"><code>2004ff9</code></a>\nfeat: support passing mutable functions as random generators</li>\n<li><a\nhref=\"https://github.com/mrdimidium/nanoid/commit/3d405c51cd6932318cb5766d0a3adab0c0c704f5\"><code>3d405c5</code></a>\nFix ci for prs</li>\n<li><a\nhref=\"https://github.com/mrdimidium/nanoid/commit/7011b102d07f7f2ba2f7c96d0b196a9a349dc9cd\"><code>7011b10</code></a>\nFixup readme, delete old example</li>\n<li>Additional commits viewable in <a\nhref=\"https://github.com/mrdimidium/nanoid/compare/v0.4.0...v0.5.0\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop\nDependabot creating any more for this major version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop\nDependabot creating any more for this minor version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop\nDependabot creating any more for this dependency (unless you reopen the\nPR or upgrade to it yourself)\n\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-05-18T04:31:53+03:00",
          "tree_id": "3b793c56a6b6deb743d2a2eab930b2d0f9db6564",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/b441d571717624461e02c82870c31aefd7246333"
        },
        "date": 1779068381053,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 879633.0132183491,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1980414 ops/sec | factor: 0.444 | P50: 0.4us | P99: 1.7us | P99.9: 3.6us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=360190 rand_rd=1167997 cpu=117 composite=51782.4"
          },
          {
            "name": "fillrandom",
            "value": 474664.280043474,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1068663 ops/sec | factor: 0.444 | P50: 0.8us | P99: 2.3us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=360190 rand_rd=1167997 cpu=117 composite=51782.4"
          },
          {
            "name": "readrandom",
            "value": 238267.99056785816,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 536439 ops/sec | factor: 0.444 | P50: 1.7us | P99: 4.7us | P99.9: 9.6us\nthreads: 1 | elapsed: 0.37s | num: 200000 | iterations: 3 | runner: seq_wr=360190 rand_rd=1167997 cpu=117 composite=51782.4"
          },
          {
            "name": "readseq",
            "value": 1419492.544218858,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 3195858 ops/sec | factor: 0.444 | P50: 0.2us | P99: 3.2us | P99.9: 5.5us\nthreads: 1 | elapsed: 0.06s | num: 200000 | iterations: 3 | runner: seq_wr=360190 rand_rd=1167997 cpu=117 composite=51782.4"
          },
          {
            "name": "seekrandom",
            "value": 161851.69362592493,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 364394 ops/sec | factor: 0.444 | P50: 2.3us | P99: 5.9us | P99.9: 12.0us\nthreads: 1 | elapsed: 0.55s | num: 200000 | iterations: 3 | runner: seq_wr=360190 rand_rd=1167997 cpu=117 composite=51782.4"
          },
          {
            "name": "prefixscan",
            "value": 97001.06692143131,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 218389 ops/sec | factor: 0.444 | P50: 4.3us | P99: 5.8us | P99.9: 10.9us\nthreads: 1 | elapsed: 0.92s | num: 200000 | iterations: 3 | runner: seq_wr=360190 rand_rd=1167997 cpu=117 composite=51782.4"
          },
          {
            "name": "overwrite",
            "value": 481759.01556351944,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1084636 ops/sec | factor: 0.444 | P50: 0.8us | P99: 2.3us | P99.9: 5.5us\nthreads: 1 | elapsed: 0.18s | num: 200000 | iterations: 3 | runner: seq_wr=360190 rand_rd=1167997 cpu=117 composite=51782.4"
          },
          {
            "name": "mergerandom",
            "value": 361734.40903849294,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 814412 ops/sec | factor: 0.444 | P50: 0.4us | P99: 0.6us | P99.9: 3.8us\nthreads: 1 | elapsed: 0.25s | num: 200000 | iterations: 3 | runner: seq_wr=360190 rand_rd=1167997 cpu=117 composite=51782.4"
          },
          {
            "name": "readwhilewriting",
            "value": 197713.86889063963,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 445135 ops/sec | factor: 0.444 | P50: 2.0us | P99: 4.4us | P99.9: 9.6us\nthreads: 1 | elapsed: 0.45s | num: 200000 | iterations: 3 | runner: seq_wr=360190 rand_rd=1167997 cpu=117 composite=51782.4"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "49699333+dependabot[bot]@users.noreply.github.com",
            "name": "dependabot[bot]",
            "username": "dependabot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a2ff86b71d153ae24d68b71fc05ff1d2c01b5167",
          "message": "ci(deps): bump dependabot/fetch-metadata from 2 to 3 (#239)\n\nBumps\n[dependabot/fetch-metadata](https://github.com/dependabot/fetch-metadata)\nfrom 2 to 3.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/dependabot/fetch-metadata/releases\">dependabot/fetch-metadata's\nreleases</a>.</em></p>\n<blockquote>\n<h2>v3.0.0</h2>\n<p>The breaking change is requiring Node.js version v24 as the Actions\nruntime.</p>\n<h2>What's Changed</h2>\n<ul>\n<li>feat: Parse versions from metadata links by <a\nhref=\"https://github.com/ppkarwasz\"><code>@​ppkarwasz</code></a> in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/632\">dependabot/fetch-metadata#632</a></li>\n<li>Upgrade actions core and actions github packages by <a\nhref=\"https://github.com/truggeri\"><code>@​truggeri</code></a> in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/649\">dependabot/fetch-metadata#649</a></li>\n<li>docs: Add notes for using <code>alert-lookup</code> with App Token\nby <a href=\"https://github.com/sue445\"><code>@​sue445</code></a> in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/656\">dependabot/fetch-metadata#656</a></li>\n<li>feat!: update Node.js version to v24 by <a\nhref=\"https://github.com/sturman\"><code>@​sturman</code></a> in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/671\">dependabot/fetch-metadata#671</a></li>\n<li>Switch build tooling from ncc to esbuild by <a\nhref=\"https://github.com/truggeri\"><code>@​truggeri</code></a> in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/676\">dependabot/fetch-metadata#676</a></li>\n<li>Add --legal-comments=none to esbuild build commands by <a\nhref=\"https://github.com/jeffwidman\"><code>@​jeffwidman</code></a> in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/679\">dependabot/fetch-metadata#679</a></li>\n<li>Bump tsconfig target from es2022 to es2024 by <a\nhref=\"https://github.com/jeffwidman\"><code>@​jeffwidman</code></a> in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/680\">dependabot/fetch-metadata#680</a></li>\n<li>Remove vestigial outDir from tsconfig.json by <a\nhref=\"https://github.com/jeffwidman\"><code>@​jeffwidman</code></a> in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/681\">dependabot/fetch-metadata#681</a></li>\n<li>Switch tsconfig module resolution to bundler by <a\nhref=\"https://github.com/jeffwidman\"><code>@​jeffwidman</code></a> in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/682\">dependabot/fetch-metadata#682</a></li>\n<li>Remove skipLibCheck from tsconfig.json by <a\nhref=\"https://github.com/jeffwidman\"><code>@​jeffwidman</code></a> in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/683\">dependabot/fetch-metadata#683</a></li>\n<li>Add typecheck step to CI by <a\nhref=\"https://github.com/jeffwidman\"><code>@​jeffwidman</code></a> in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/685\">dependabot/fetch-metadata#685</a></li>\n<li>Enable noImplicitAny in tsconfig.json by <a\nhref=\"https://github.com/jeffwidman\"><code>@​jeffwidman</code></a> in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/684\">dependabot/fetch-metadata#684</a></li>\n<li>Upgrade <code>@​actions/core</code> to ^3.0.0 by <a\nhref=\"https://github.com/truggeri\"><code>@​truggeri</code></a> in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/677\">dependabot/fetch-metadata#677</a></li>\n<li>Upgrade <code>@​actions/github</code> to ^9.0.0 and\n<code>@​octokit/request-error</code> to ^7.1.0 by <a\nhref=\"https://github.com/truggeri\"><code>@​truggeri</code></a> in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/678\">dependabot/fetch-metadata#678</a></li>\n<li>Bump qs from 6.14.0 to 6.14.1 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/651\">dependabot/fetch-metadata#651</a></li>\n<li>Bump hono from 4.11.1 to 4.11.4 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/652\">dependabot/fetch-metadata#652</a></li>\n<li>Bump hono from 4.11.4 to 4.11.7 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/653\">dependabot/fetch-metadata#653</a></li>\n<li>Bump hono from 4.11.7 to 4.12.0 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/657\">dependabot/fetch-metadata#657</a></li>\n<li>Bump qs from 6.14.1 to 6.14.2 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/655\">dependabot/fetch-metadata#655</a></li>\n<li>Bump <code>@​modelcontextprotocol/sdk</code> from 1.25.1 to 1.26.0\nby <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/654\">dependabot/fetch-metadata#654</a></li>\n<li>Bump <code>@​hono/node-server</code> from 1.19.9 to 1.19.10 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/665\">dependabot/fetch-metadata#665</a></li>\n<li>Bump hono from 4.12.2 to 4.12.5 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/664\">dependabot/fetch-metadata#664</a></li>\n<li>Bump minimatch from 3.1.2 to 3.1.5 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/667\">dependabot/fetch-metadata#667</a></li>\n<li>Bump hono from 4.12.5 to 4.12.7 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/668\">dependabot/fetch-metadata#668</a></li>\n<li>Bump actions/create-github-app-token from 2.2.1 to 3.0.0 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/669\">dependabot/fetch-metadata#669</a></li>\n<li>Bump flatted from 3.3.3 to 3.4.2 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/670\">dependabot/fetch-metadata#670</a></li>\n<li>build(deps-dev): bump picomatch from 2.3.1 to 2.3.2 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/674\">dependabot/fetch-metadata#674</a></li>\n</ul>\n<h2>New Contributors</h2>\n<ul>\n<li><a href=\"https://github.com/ppkarwasz\"><code>@​ppkarwasz</code></a>\nmade their first contribution in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/632\">dependabot/fetch-metadata#632</a></li>\n<li><a href=\"https://github.com/truggeri\"><code>@​truggeri</code></a>\nmade their first contribution in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/649\">dependabot/fetch-metadata#649</a></li>\n<li><a href=\"https://github.com/sue445\"><code>@​sue445</code></a> made\ntheir first contribution in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/656\">dependabot/fetch-metadata#656</a></li>\n<li><a href=\"https://github.com/sturman\"><code>@​sturman</code></a> made\ntheir first contribution in <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/671\">dependabot/fetch-metadata#671</a></li>\n</ul>\n<p><strong>Full Changelog</strong>: <a\nhref=\"https://github.com/dependabot/fetch-metadata/compare/v2...v3.0.0\">https://github.com/dependabot/fetch-metadata/compare/v2...v3.0.0</a></p>\n<h2>v2.5.0</h2>\n<h2>What's Changed</h2>\n<ul>\n<li>Bump actions/publish-immutable-action from 0.0.3 to 0.0.4 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/628\">dependabot/fetch-metadata#628</a></li>\n<li>Bump the dev-dependencies group with 11 updates by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/629\">dependabot/fetch-metadata#629</a></li>\n<li>Bump actions/create-github-app-token from 2.0.6 to 2.1.1 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/635\">dependabot/fetch-metadata#635</a></li>\n<li>Bump actions/create-github-app-token from 2.1.1 to 2.1.4 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/638\">dependabot/fetch-metadata#638</a></li>\n<li>Bump actions/checkout from 4 to 5 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/636\">dependabot/fetch-metadata#636</a></li>\n<li>Bump actions/setup-node from 4 to 5 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/637\">dependabot/fetch-metadata#637</a></li>\n<li>Bump actions/setup-node from 5 to 6 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/639\">dependabot/fetch-metadata#639</a></li>\n<li>Bump actions/create-github-app-token from 2.1.4 to 2.2.0 by <a\nhref=\"https://github.com/dependabot\"><code>@​dependabot</code></a>[bot]\nin <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/pull/643\">dependabot/fetch-metadata#643</a></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/dependabot/fetch-metadata/commit/25dd0e34f4fe68f24cc83900b1fe3fe149efef98\"><code>25dd0e3</code></a>\nv3.1.0 (<a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/issues/692\">#692</a>)</li>\n<li><a\nhref=\"https://github.com/dependabot/fetch-metadata/commit/e073f50d732cb48d48fb80afedb4fa61361626e9\"><code>e073f50</code></a>\nMerge pull request <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/issues/705\">#705</a>\nfrom dependabot/dependabot/npm_and_yarn/hono-4.12.14</li>\n<li><a\nhref=\"https://github.com/dependabot/fetch-metadata/commit/0670e167df1fbee1b0d07121de6a182ddebdd674\"><code>0670e16</code></a>\nbuild(deps-dev): bump hono from 4.12.12 to 4.12.14</li>\n<li><a\nhref=\"https://github.com/dependabot/fetch-metadata/commit/7a7fe10a42310e65df80af6c771e9aa5d59842d1\"><code>7a7fe10</code></a>\nMerge pull request <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/issues/702\">#702</a>\nfrom dependabot/dependabot/npm_and_yarn/dependencies-...</li>\n<li><a\nhref=\"https://github.com/dependabot/fetch-metadata/commit/5168191cea3d4daa635bff6c796b4f0faeba522d\"><code>5168191</code></a>\nUpdating dist build</li>\n<li><a\nhref=\"https://github.com/dependabot/fetch-metadata/commit/23882e175b2f16bc495c89aa50940399c6a17504\"><code>23882e1</code></a>\nbuild(deps): bump <code>@​actions/github</code> in the dependencies\ngroup</li>\n<li><a\nhref=\"https://github.com/dependabot/fetch-metadata/commit/1072469591c13fda1d8dba1d1ac2e80187e247d7\"><code>1072469</code></a>\nMerge pull request <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/issues/701\">#701</a>\nfrom dependabot/dependabot/github_actions/actions/cre...</li>\n<li><a\nhref=\"https://github.com/dependabot/fetch-metadata/commit/43f8a0055c8e32587be67e097dff89a6823c9752\"><code>43f8a00</code></a>\nbuild(deps): bump actions/create-github-app-token from 3.0.0 to\n3.1.1</li>\n<li><a\nhref=\"https://github.com/dependabot/fetch-metadata/commit/b4d904a50935c8ebe744da148ea8a18a43fe72e1\"><code>b4d904a</code></a>\nMerge pull request <a\nhref=\"https://redirect.github.com/dependabot/fetch-metadata/issues/703\">#703</a>\nfrom dependabot/dependabot/npm_and_yarn/globals-17.5.0</li>\n<li><a\nhref=\"https://github.com/dependabot/fetch-metadata/commit/c8046bb877d9989cc848797de1b944bc3e93ef82\"><code>c8046bb</code></a>\nbuild(deps-dev): bump globals from 17.4.0 to 17.5.0</li>\n<li>Additional commits viewable in <a\nhref=\"https://github.com/dependabot/fetch-metadata/compare/v2...v3\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\n[![Dependabot compatibility\nscore](https://dependabot-badges.githubapp.com/badges/compatibility_score?dependency-name=dependabot/fetch-metadata&package-manager=github_actions&previous-version=2&new-version=3)](https://docs.github.com/en/github/managing-security-vulnerabilities/about-dependabot-security-updates#about-compatibility-scores)\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop\nDependabot creating any more for this major version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop\nDependabot creating any more for this minor version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop\nDependabot creating any more for this dependency (unless you reopen the\nPR or upgrade to it yourself)\n\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-05-18T04:32:10+03:00",
          "tree_id": "f2ba0a64a98d1430e9d059ab959cc942fe2ff6af",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/a2ff86b71d153ae24d68b71fc05ff1d2c01b5167"
        },
        "date": 1779068458173,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1185627.9864564762,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2160052 ops/sec | factor: 0.549 | P50: 0.3us | P99: 1.9us | P99.9: 4.9us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=230554 rand_rd=927535 cpu=123 composite=41902.9"
          },
          {
            "name": "fillrandom",
            "value": 665160.0249044984,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1211830 ops/sec | factor: 0.549 | P50: 0.7us | P99: 2.5us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.17s | num: 200000 | iterations: 3 | runner: seq_wr=230554 rand_rd=927535 cpu=123 composite=41902.9"
          },
          {
            "name": "readrandom",
            "value": 306441.2166736294,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 558294 ops/sec | factor: 0.549 | P50: 1.6us | P99: 5.2us | P99.9: 13.1us\nthreads: 1 | elapsed: 0.36s | num: 200000 | iterations: 3 | runner: seq_wr=230554 rand_rd=927535 cpu=123 composite=41902.9"
          },
          {
            "name": "readseq",
            "value": 1394371.064376817,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2540353 ops/sec | factor: 0.549 | P50: 0.2us | P99: 3.8us | P99.9: 7.4us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=230554 rand_rd=927535 cpu=123 composite=41902.9"
          },
          {
            "name": "seekrandom",
            "value": 212616.61931269694,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 387358 ops/sec | factor: 0.549 | P50: 2.3us | P99: 6.0us | P99.9: 14.0us\nthreads: 1 | elapsed: 0.52s | num: 200000 | iterations: 3 | runner: seq_wr=230554 rand_rd=927535 cpu=123 composite=41902.9"
          },
          {
            "name": "prefixscan",
            "value": 102569.0487410001,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 186867 ops/sec | factor: 0.549 | P50: 5.0us | P99: 7.3us | P99.9: 17.2us\nthreads: 1 | elapsed: 1.07s | num: 200000 | iterations: 3 | runner: seq_wr=230554 rand_rd=927535 cpu=123 composite=41902.9"
          },
          {
            "name": "overwrite",
            "value": 675932.700675605,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1231457 ops/sec | factor: 0.549 | P50: 0.7us | P99: 2.5us | P99.9: 5.7us\nthreads: 1 | elapsed: 0.16s | num: 200000 | iterations: 3 | runner: seq_wr=230554 rand_rd=927535 cpu=123 composite=41902.9"
          },
          {
            "name": "mergerandom",
            "value": 406451.49929081334,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 740499 ops/sec | factor: 0.549 | P50: 0.3us | P99: 0.6us | P99.9: 3.4us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=230554 rand_rd=927535 cpu=123 composite=41902.9"
          },
          {
            "name": "readwhilewriting",
            "value": 261785.02215907152,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 476936 ops/sec | factor: 0.549 | P50: 1.9us | P99: 5.4us | P99.9: 13.4us\nthreads: 1 | elapsed: 0.42s | num: 200000 | iterations: 3 | runner: seq_wr=230554 rand_rd=927535 cpu=123 composite=41902.9"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "49699333+dependabot[bot]@users.noreply.github.com",
            "name": "dependabot[bot]",
            "username": "dependabot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3ee0cbee8b00d3cb7587484d8bc8563750b1d33c",
          "message": "chore(deps): update rand_chacha requirement from 0.3 to 0.10 (#243)\n\nUpdates the requirements on\n[rand_chacha](https://github.com/rust-random/rand) to permit the latest\nversion.\n<details>\n<summary>Release notes</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/rust-random/rand/releases\">rand_chacha's\nreleases</a>.</em></p>\n<blockquote>\n<h2>0.10.0</h2>\n<h2>[0.10.0] - 2026-02-08</h2>\n<h3>Changes</h3>\n<ul>\n<li>The dependency on <code>rand_chacha</code> has been replaced with a\ndependency on <code>chacha20</code>. This changes the implementation\nbehind <code>StdRng</code>, but the output remains the same. There may\nbe some API breakage when using the ChaCha-types directly as these are\nnow the ones in <code>chacha20</code> instead of\n<code>rand_chacha</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1642\">#1642</a>).</li>\n<li>Rename fns <code>IndexedRandom::choose_multiple</code> -&gt;\n<code>sample</code>, <code>choose_multiple_array</code> -&gt;\n<code>sample_array</code>, <code>choose_multiple_weighted</code> -&gt;\n<code>sample_weighted</code>, struct <code>SliceChooseIter</code> -&gt;\n<code>IndexedSamples</code> and fns\n<code>IteratorRandom::choose_multiple</code> -&gt; <code>sample</code>,\n<code>choose_multiple_fill</code> -&gt; <code>sample_fill</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1632\">#1632</a>)</li>\n<li>Use Edition 2024 and MSRV 1.85 (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1653\">#1653</a>)</li>\n<li>Let <code>Fill</code> be implemented for element types, not\nsliceable types (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1652\">#1652</a>)</li>\n<li>Fix <code>OsError::raw_os_error</code> on UEFI targets by returning\n<code>Option&lt;usize&gt;</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1665\">#1665</a>)</li>\n<li>Replace fn <code>TryRngCore::read_adapter(..) -&gt;\nRngReadAdapter</code> with simpler struct <code>RngReader</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1669\">#1669</a>)</li>\n<li>Remove fns <code>SeedableRng::from_os_rng</code>,\n<code>try_from_os_rng</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1674\">#1674</a>)</li>\n<li>Remove <code>Clone</code> support for <code>StdRng</code>,\n<code>ReseedingRng</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1677\">#1677</a>)</li>\n<li>Use <code>postcard</code> instead of <code>bincode</code> to test\nthe serde feature (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1693\">#1693</a>)</li>\n<li>Avoid excessive allocation in <code>IteratorRandom::sample</code>\nwhen <code>amount</code> is much larger than iterator size (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1695\">#1695</a>)</li>\n<li>Rename <code>os_rng</code> -&gt; <code>sys_rng</code>,\n<code>OsRng</code> -&gt; <code>SysRng</code>, <code>OsError</code> -&gt;\n<code>SysError</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1697\">#1697</a>)</li>\n<li>Rename <code>Rng</code> -&gt; <code>RngExt</code> as upstream\n<code>rand_core</code> has renamed <code>RngCore</code> -&gt;\n<code>Rng</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1717\">#1717</a>)</li>\n</ul>\n<h3>Additions</h3>\n<ul>\n<li>Add fns <code>IndexedRandom::choose_iter</code>,\n<code>choose_weighted_iter</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1632\">#1632</a>)</li>\n<li>Pub export <code>Xoshiro128PlusPlus</code>,\n<code>Xoshiro256PlusPlus</code> prngs (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1649\">#1649</a>)</li>\n<li>Pub export <code>ChaCha8Rng</code>, <code>ChaCha12Rng</code>,\n<code>ChaCha20Rng</code> behind <code>chacha</code> feature (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1659\">#1659</a>)</li>\n<li>Fn <code>rand::make_rng() -&gt; R where R: SeedableRng</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1734\">#1734</a>)</li>\n</ul>\n<h3>Removals</h3>\n<ul>\n<li>Removed <code>ReseedingRng</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1722\">#1722</a>)</li>\n<li>Removed unused feature &quot;nightly&quot; (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1732\">#1732</a>)</li>\n<li>Removed feature <code>small_rng</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1732\">#1732</a>)</li>\n</ul>\n<p><a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1632\">#1632</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1632\">rust-random/rand#1632</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1642\">#1642</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1642\">rust-random/rand#1642</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1649\">#1649</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1649\">rust-random/rand#1649</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1652\">#1652</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1652\">rust-random/rand#1652</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1653\">#1653</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1653\">rust-random/rand#1653</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1659\">#1659</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1659\">rust-random/rand#1659</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1665\">#1665</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1665\">rust-random/rand#1665</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1669\">#1669</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1669\">rust-random/rand#1669</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1674\">#1674</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1674\">rust-random/rand#1674</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1677\">#1677</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1677\">rust-random/rand#1677</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1693\">#1693</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1693\">rust-random/rand#1693</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1695\">#1695</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1695\">rust-random/rand#1695</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1697\">#1697</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1697\">rust-random/rand#1697</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1717\">#1717</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1717\">rust-random/rand#1717</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1722\">#1722</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1722\">rust-random/rand#1722</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1732\">#1732</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1732\">rust-random/rand#1732</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1734\">#1734</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1734\">rust-random/rand#1734</a></p>\n<h2>New Contributors</h2>\n<ul>\n<li><a href=\"https://github.com/hpenne\"><code>@​hpenne</code></a> made\ntheir first contribution in <a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1642\">rust-random/rand#1642</a></li>\n<li><a href=\"https://github.com/usamoi\"><code>@​usamoi</code></a> made\ntheir first contribution in <a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1665\">rust-random/rand#1665</a></li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Changelog</summary>\n<p><em>Sourced from <a\nhref=\"https://github.com/rust-random/rand/blob/master/CHANGELOG.md\">rand_chacha's\nchangelog</a>.</em></p>\n<blockquote>\n<h2>[0.10.0] - 2026-02-08</h2>\n<h3>Changes</h3>\n<ul>\n<li>The dependency on <code>rand_chacha</code> has been replaced with a\ndependency on <code>chacha20</code>. This changes the implementation\nbehind <code>StdRng</code>, but the output remains the same. There may\nbe some API breakage when using the ChaCha-types directly as these are\nnow the ones in <code>chacha20</code> instead of\n<code>rand_chacha</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1642\">#1642</a>).</li>\n<li>Rename fns <code>IndexedRandom::choose_multiple</code> -&gt;\n<code>sample</code>, <code>choose_multiple_array</code> -&gt;\n<code>sample_array</code>, <code>choose_multiple_weighted</code> -&gt;\n<code>sample_weighted</code>, struct <code>SliceChooseIter</code> -&gt;\n<code>IndexedSamples</code> and fns\n<code>IteratorRandom::choose_multiple</code> -&gt; <code>sample</code>,\n<code>choose_multiple_fill</code> -&gt; <code>sample_fill</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1632\">#1632</a>)</li>\n<li>Use Edition 2024 and MSRV 1.85 (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1653\">#1653</a>)</li>\n<li>Let <code>Fill</code> be implemented for element types, not\nsliceable types (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1652\">#1652</a>)</li>\n<li>Fix <code>OsError::raw_os_error</code> on UEFI targets by returning\n<code>Option&lt;usize&gt;</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1665\">#1665</a>)</li>\n<li>Replace fn <code>TryRngCore::read_adapter(..) -&gt;\nRngReadAdapter</code> with simpler struct <code>RngReader</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1669\">#1669</a>)</li>\n<li>Remove fns <code>SeedableRng::from_os_rng</code>,\n<code>try_from_os_rng</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1674\">#1674</a>)</li>\n<li>Remove <code>Clone</code> support for <code>StdRng</code>,\n<code>ReseedingRng</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1677\">#1677</a>)</li>\n<li>Use <code>postcard</code> instead of <code>bincode</code> to test\nthe serde feature (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1693\">#1693</a>)</li>\n<li>Avoid excessive allocation in <code>IteratorRandom::sample</code>\nwhen <code>amount</code> is much larger than iterator size (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1695\">#1695</a>)</li>\n<li>Rename <code>os_rng</code> -&gt; <code>sys_rng</code>,\n<code>OsRng</code> -&gt; <code>SysRng</code>, <code>OsError</code> -&gt;\n<code>SysError</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1697\">#1697</a>)</li>\n<li>Rename <code>Rng</code> -&gt; <code>RngExt</code> as upstream\n<code>rand_core</code> has renamed <code>RngCore</code> -&gt;\n<code>Rng</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1717\">#1717</a>)</li>\n</ul>\n<h3>Additions</h3>\n<ul>\n<li>Add fns <code>IndexedRandom::choose_iter</code>,\n<code>choose_weighted_iter</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1632\">#1632</a>)</li>\n<li>Pub export <code>Xoshiro128PlusPlus</code>,\n<code>Xoshiro256PlusPlus</code> prngs (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1649\">#1649</a>)</li>\n<li>Pub export <code>ChaCha8Rng</code>, <code>ChaCha12Rng</code>,\n<code>ChaCha20Rng</code> behind <code>chacha</code> feature (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1659\">#1659</a>)</li>\n<li>Fn <code>rand::make_rng() -&gt; R where R: SeedableRng</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1734\">#1734</a>)</li>\n</ul>\n<h3>Removals</h3>\n<ul>\n<li>Removed <code>ReseedingRng</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1722\">#1722</a>)</li>\n<li>Removed unused feature &quot;nightly&quot; (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1732\">#1732</a>)</li>\n<li>Removed feature <code>small_rng</code> (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1732\">#1732</a>)</li>\n</ul>\n<p><a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1632\">#1632</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1632\">rust-random/rand#1632</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1642\">#1642</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1642\">rust-random/rand#1642</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1649\">#1649</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1649\">rust-random/rand#1649</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1652\">#1652</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1652\">rust-random/rand#1652</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1653\">#1653</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1653\">rust-random/rand#1653</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1659\">#1659</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1659\">rust-random/rand#1659</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1665\">#1665</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1665\">rust-random/rand#1665</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1669\">#1669</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1669\">rust-random/rand#1669</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1674\">#1674</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1674\">rust-random/rand#1674</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1677\">#1677</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1677\">rust-random/rand#1677</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1693\">#1693</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1693\">rust-random/rand#1693</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1695\">#1695</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1695\">rust-random/rand#1695</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1697\">#1697</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1697\">rust-random/rand#1697</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1717\">#1717</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1717\">rust-random/rand#1717</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1722\">#1722</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1722\">rust-random/rand#1722</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1732\">#1732</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1732\">rust-random/rand#1732</a>\n<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1734\">#1734</a>:\n<a\nhref=\"https://redirect.github.com/rust-random/rand/pull/1734\">rust-random/rand#1734</a></p>\n<h2>[0.9.2] - 2025-07-20</h2>\n<h3>Deprecated</h3>\n<ul>\n<li>Deprecate <code>rand::rngs::mock</code> module and\n<code>StepRng</code> generator (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1634\">#1634</a>)</li>\n</ul>\n<!-- raw HTML omitted -->\n</blockquote>\n<p>... (truncated)</p>\n</details>\n<details>\n<summary>Commits</summary>\n<ul>\n<li><a\nhref=\"https://github.com/rust-random/rand/commit/acc5f246d3338ffea40aa0f25a46f84d6d19db8d\"><code>acc5f24</code></a>\nPrepare v0.10.0 releases (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1729\">#1729</a>)</li>\n<li><a\nhref=\"https://github.com/rust-random/rand/commit/95c51651c904ca8e77cdec5ebb6f218bb505f18f\"><code>95c5165</code></a>\nAdd fn rand::make_rng (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1734\">#1734</a>)</li>\n<li><a\nhref=\"https://github.com/rust-random/rand/commit/146da581490e534332a6018c15d7765b4c16851e\"><code>146da58</code></a>\nCHANGELOG: add PR links (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1738\">#1738</a>)</li>\n<li><a\nhref=\"https://github.com/rust-random/rand/commit/8cacd6da6df9256d13d8ceb499310844227379fd\"><code>8cacd6d</code></a>\nREADME tweaks (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1737\">#1737</a>)</li>\n<li><a\nhref=\"https://github.com/rust-random/rand/commit/28e3df866fdf2a1892abce84a0832c1eb15511ef\"><code>28e3df8</code></a>\nUpdate chacha20: use ChaChaCore directly; remove bytes_until_reseed\nfield (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1\">#1</a>...</li>\n<li><a\nhref=\"https://github.com/rust-random/rand/commit/03db3110d0224cf5c9ae7b4462e58f4dca4a5293\"><code>03db311</code></a>\nReplace fn reseed_and_generate with try_to_reseed</li>\n<li><a\nhref=\"https://github.com/rust-random/rand/commit/b14483e6abd464c2745ed701cebf214a0f6fb374\"><code>b14483e</code></a>\nApply inline attr to fn generate</li>\n<li><a\nhref=\"https://github.com/rust-random/rand/commit/fda8f74872f759cf72514c84dec30033c04f60d1\"><code>fda8f74</code></a>\nRemove bytes_until_reseed field</li>\n<li><a\nhref=\"https://github.com/rust-random/rand/commit/213bb3bd4270df73bdd4885c2bf5682dce73c03d\"><code>213bb3b</code></a>\nBump chacha20 to 0.10.0-rc.11</li>\n<li><a\nhref=\"https://github.com/rust-random/rand/commit/72afe1e973fcd83d840cf597888223072bbdb04c\"><code>72afe1e</code></a>\nMinor tweaks; prepare v0.10.0-rc.9 (<a\nhref=\"https://redirect.github.com/rust-random/rand/issues/1736\">#1736</a>)</li>\n<li>Additional commits viewable in <a\nhref=\"https://github.com/rust-random/rand/compare/rand_chacha-0.3.0...0.10.0\">compare\nview</a></li>\n</ul>\n</details>\n<br />\n\n\nDependabot will resolve any conflicts with this PR as long as you don't\nalter it yourself. You can also trigger a rebase manually by commenting\n`@dependabot rebase`.\n\n[//]: # (dependabot-automerge-start)\n[//]: # (dependabot-automerge-end)\n\n---\n\n<details>\n<summary>Dependabot commands and options</summary>\n<br />\n\nYou can trigger Dependabot actions by commenting on this PR:\n- `@dependabot rebase` will rebase this PR\n- `@dependabot recreate` will recreate this PR, overwriting any edits\nthat have been made to it\n- `@dependabot show <dependency name> ignore conditions` will show all\nof the ignore conditions of the specified dependency\n- `@dependabot ignore this major version` will close this PR and stop\nDependabot creating any more for this major version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this minor version` will close this PR and stop\nDependabot creating any more for this minor version (unless you reopen\nthe PR or upgrade to it yourself)\n- `@dependabot ignore this dependency` will close this PR and stop\nDependabot creating any more for this dependency (unless you reopen the\nPR or upgrade to it yourself)\n\n\n</details>\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-05-18T04:32:41+03:00",
          "tree_id": "17d7afe590877012cad3fa5a3d00c6f496c579e5",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/3ee0cbee8b00d3cb7587484d8bc8563750b1d33c"
        },
        "date": 1779068562225,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1145849.6793289648,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2093954 ops/sec | factor: 0.547 | P50: 0.3us | P99: 2.0us | P99.9: 5.0us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=220232 rand_rd=967360 cpu=123 composite=42030.8"
          },
          {
            "name": "fillrandom",
            "value": 668371.2889687901,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1221398 ops/sec | factor: 0.547 | P50: 0.6us | P99: 2.4us | P99.9: 5.9us\nthreads: 1 | elapsed: 0.16s | num: 200000 | iterations: 3 | runner: seq_wr=220232 rand_rd=967360 cpu=123 composite=42030.8"
          },
          {
            "name": "readrandom",
            "value": 300633.04472905607,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 549384 ops/sec | factor: 0.547 | P50: 1.6us | P99: 5.3us | P99.9: 13.0us\nthreads: 1 | elapsed: 0.36s | num: 200000 | iterations: 3 | runner: seq_wr=220232 rand_rd=967360 cpu=123 composite=42030.8"
          },
          {
            "name": "readseq",
            "value": 1352430.36783219,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2471464 ops/sec | factor: 0.547 | P50: 0.2us | P99: 3.8us | P99.9: 7.9us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=220232 rand_rd=967360 cpu=123 composite=42030.8"
          },
          {
            "name": "seekrandom",
            "value": 201107.06153274217,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 367508 ops/sec | factor: 0.547 | P50: 2.4us | P99: 6.2us | P99.9: 13.9us\nthreads: 1 | elapsed: 0.54s | num: 200000 | iterations: 3 | runner: seq_wr=220232 rand_rd=967360 cpu=123 composite=42030.8"
          },
          {
            "name": "prefixscan",
            "value": 96383.84229482104,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 176134 ops/sec | factor: 0.547 | P50: 5.3us | P99: 7.8us | P99.9: 17.1us\nthreads: 1 | elapsed: 1.14s | num: 200000 | iterations: 3 | runner: seq_wr=220232 rand_rd=967360 cpu=123 composite=42030.8"
          },
          {
            "name": "overwrite",
            "value": 676311.4903974828,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1235908 ops/sec | factor: 0.547 | P50: 0.7us | P99: 2.6us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.16s | num: 200000 | iterations: 3 | runner: seq_wr=220232 rand_rd=967360 cpu=123 composite=42030.8"
          },
          {
            "name": "mergerandom",
            "value": 410788.16871689184,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 750684 ops/sec | factor: 0.547 | P50: 0.3us | P99: 0.5us | P99.9: 2.8us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=220232 rand_rd=967360 cpu=123 composite=42030.8"
          },
          {
            "name": "readwhilewriting",
            "value": 273655.5079303307,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 500085 ops/sec | factor: 0.547 | P50: 1.8us | P99: 4.1us | P99.9: 13.3us\nthreads: 1 | elapsed: 0.40s | num: 200000 | iterations: 3 | runner: seq_wr=220232 rand_rd=967360 cpu=123 composite=42030.8"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fc790a55c032bb0e112de6270e0cbc929f050607",
          "message": "perf(util): SIMD longest_shared_prefix_length() (Phase 2.1) (#245)\n\n## Summary\n\nReplaces the byte-by-byte `iter().zip().take_while().count()` in\n`longest_shared_prefix_length` with runtime-dispatched SIMD kernels:\n\n- **x86_64 + AVX2** (runtime via `is_x86_feature_detected!`): 32-byte\nlanes, `_mm256_cmpeq_epi8` + `_mm256_movemask_epi8`.\n- **aarch64 little-endian**: 16-byte NEON lanes via `vceqq_u8` +\ndual-u64 mask reduction. Restricted to LE because `trailing_zeros()/8`\nmismatch math and `vgetq_lane_u64` lane order both depend on LE byte\norder; big-endian aarch64 falls through to scalar.\n- **Everywhere else** (incl. x86_64 without AVX2, BE aarch64): 8-byte\n`u64` word stride via XOR + `trailing_zeros()/8`. Endian-aware via\n`target_endian` cfg.\n\nPer CLAUDE.md principle 3: runtime CPU detection only, never\n`#[cfg(target_feature)]`. Same binary ships to all x86_64 CPUs; scalar\nfallback always present.\n\n## Measured speedup (aarch64 NEON, M1, criterion `--quick`)\n\n|        Size | Pattern        | byte_loop | dispatched | speedup |\n|------------:|----------------|----------:|-----------:|--------:|\n|        256B | full match     |  121.0 ns |    12.0 ns |  **10×** |\n|        256B | quarter match  |   28.2 ns |     4.7 ns |   **6×** |\n|       1024B | full match     |  452.8 ns |    44.5 ns |  **10×** |\n|       1024B | quarter match  |  130.7 ns |    12.3 ns |  **11×** |\n\nx86_64 AVX2 numbers will land via CI on the bench gh-pages dashboard\n(#244 once merged).\n\n## Coverage\n\n- **Unit tests** (6 new + 1 original) — boundary stride sizes\n(0/7/8/9/15/16/17/31/32/33/63/64/127/128/255/256), asymmetric lengths\n(scalar + dispatched), extreme byte patterns (all-zero, all-FF,\nzero-vs-FF, alternating), one-empty pairs.\n- **Property tests** (proptest, 256 cases each) — `lsp_scalar` and\ndispatched `longest_shared_prefix_length` must both equal the\nbyte-by-byte reference for any random input up to 1 KiB.\n- **Integration** — function is the sole per-key cost in\n`src/table/block/encoder.rs:142`. All 1276 table-writing / flush /\ncompaction tests in the existing suite continue to pass.\n- **Bench** — new `benches/lsp.rs` with 6 sizes × 2 mismatch patterns ×\n{dispatched, byte_loop}, throughput in GiB/s.\n- **Cross-target CI** — verified on x86_64 (lint+test), aarch64-gnu,\naarch64-musl, i686, powerpc64 (BE), riscv64gc — all pass; scalar\nfallback handles BE and non-SIMD targets correctly.\n\n## Test plan\n\n- [x] `cargo check --all-features --all-targets` clean\n- [x] `cargo clippy --all-features --all-targets -- -D warnings` clean\n(both aarch64 + x86_64-apple-darwin)\n- [x] `cargo nextest run --all-features` — 1276 passed, 6 skipped\n- [x] `cargo test --doc --all-features` — 41 passed, 2 ignored\n- [x] `cargo bench --bench lsp -- --quick` runs and shows expected\nspeedup\n- [x] SIMD/scalar parity proven for all boundary sizes via deterministic\n+ property tests\n- [x] CI green on all cross-compile targets (incl. BE powerpc64 →\nexercises scalar fallback)\n\nCloses #219\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **Chores**\n* Added a local microbenchmark target for shared-prefix performance and\nenabled it for local runs.\n* Updated an optional dependency version and exposed the shared-prefix\nhelper at the crate root.\n\n* **Refactor**\n* Reworked shared-prefix computation to use platform-accelerated paths\nwith safe fallbacks for broader performance.\n\n* **Tests**\n* Expanded unit, kernel-specific, and property-based tests covering\nboundaries, patterns, truncation, and randomized inputs.\n\n<!-- review_stack_entry_start -->\n\n[![Review Change\nStack](https://storage.googleapis.com/coderabbit_public_assets/review-stack-in-coderabbit-ui.svg)](https://app.coderabbit.ai/change-stack/structured-world/coordinode-lsm-tree/pull/245?utm_source=github_walkthrough&utm_medium=github&utm_campaign=change_stack)\n\n<!-- review_stack_entry_end -->\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-05-18T10:30:00+03:00",
          "tree_id": "cf9cdbadeb1340a644b20b28de7e79989bbe9393",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/fc790a55c032bb0e112de6270e0cbc929f050607"
        },
        "date": 1779089474725,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 1239037.199664856,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1929449 ops/sec | factor: 0.642 | P50: 0.4us | P99: 2.5us | P99.9: 5.8us\nthreads: 1 | elapsed: 0.10s | num: 200000 | iterations: 3 | runner: seq_wr=223814 rand_rd=702157 cpu=108 composite=35816.0"
          },
          {
            "name": "fillrandom",
            "value": 674617.1868398564,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1050525 ops/sec | factor: 0.642 | P50: 0.7us | P99: 3.2us | P99.9: 8.3us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=223814 rand_rd=702157 cpu=108 composite=35816.0"
          },
          {
            "name": "readrandom",
            "value": 281785.5185904247,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 438801 ops/sec | factor: 0.642 | P50: 2.1us | P99: 6.5us | P99.9: 13.7us\nthreads: 1 | elapsed: 0.46s | num: 200000 | iterations: 3 | runner: seq_wr=223814 rand_rd=702157 cpu=108 composite=35816.0"
          },
          {
            "name": "readseq",
            "value": 1488892.6348979492,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2318528 ops/sec | factor: 0.642 | P50: 0.2us | P99: 4.7us | P99.9: 9.6us\nthreads: 1 | elapsed: 0.09s | num: 200000 | iterations: 3 | runner: seq_wr=223814 rand_rd=702157 cpu=108 composite=35816.0"
          },
          {
            "name": "seekrandom",
            "value": 205098.63925836998,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 319383 ops/sec | factor: 0.642 | P50: 2.8us | P99: 7.3us | P99.9: 15.2us\nthreads: 1 | elapsed: 0.63s | num: 200000 | iterations: 3 | runner: seq_wr=223814 rand_rd=702157 cpu=108 composite=35816.0"
          },
          {
            "name": "prefixscan",
            "value": 117581.763529307,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 183100 ops/sec | factor: 0.642 | P50: 5.1us | P99: 6.9us | P99.9: 16.4us\nthreads: 1 | elapsed: 1.09s | num: 200000 | iterations: 3 | runner: seq_wr=223814 rand_rd=702157 cpu=108 composite=35816.0"
          },
          {
            "name": "overwrite",
            "value": 679907.6962262535,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1058763 ops/sec | factor: 0.642 | P50: 0.7us | P99: 3.2us | P99.9: 9.0us\nthreads: 1 | elapsed: 0.19s | num: 200000 | iterations: 3 | runner: seq_wr=223814 rand_rd=702157 cpu=108 composite=35816.0"
          },
          {
            "name": "mergerandom",
            "value": 480427.78048191196,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 748130 ops/sec | factor: 0.642 | P50: 0.4us | P99: 2.2us | P99.9: 3.6us\nthreads: 1 | elapsed: 0.27s | num: 200000 | iterations: 3 | runner: seq_wr=223814 rand_rd=702157 cpu=108 composite=35816.0"
          },
          {
            "name": "readwhilewriting",
            "value": 260953.53568945095,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 406361 ops/sec | factor: 0.642 | P50: 2.3us | P99: 5.7us | P99.9: 14.3us\nthreads: 1 | elapsed: 0.49s | num: 200000 | iterations: 3 | runner: seq_wr=223814 rand_rd=702157 cpu=108 composite=35816.0"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mail@polaz.com",
            "name": "Dmitry Prudnikov",
            "username": "polaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "2416cf3c4b97177bd1c88b05d57e96beb869f5fe",
          "message": "fix(encryption): restore --features encryption build (aes-gcm 0.11.0-rc.3 + rand_chacha 0.10) (#258)\n\n## Summary\n\nRestores `cargo build --features encryption` on `main` after the\ndependabot bump in #243 broke trait-bound resolution. AAD-bound block\nidentity is intentionally NOT in this PR — that's the separate #250 /\n#251 / #252 / #253 track.\n\n## Changes\n\n- **`Cargo.toml`** — `aes-gcm = \"=0.11.0-rc.3\"` (exact pin, prerelease\ncan churn) with `rand_core` feature so `aead::rand_core` is re-exported\non the new trait family; `rand_chacha = \"0.10\"`\n- **`src/encryption.rs`** — API migration:\n- `AeadInPlace` → `AeadInOut` (`encrypt_inout_detached` /\n`decrypt_inout_detached`)\n  - `GenericArray::from_slice` → `Nonce::try_from` / `Tag::try_from`\n  - `Aes256Gcm::generate_nonce(rng)` → `Nonce::generate_from_rng(rng)`\n- `ChaCha20Rng::from_rng(OsRng).expect(..)` → `<[u8; 32]>::generate()` +\n`from_seed` (preserves fork-aware reseed + thread-local CSPRNG perf)\n- test: `Rng::next_u64` (rand_core 0.10 marker-trait shape — `RngCore`\nis now a marker, methods on `Rng` supertrait)\n\n## Why rand_chacha is preserved (deliberate)\n\nThread-local `ChaCha20Rng` seeded once per thread from `OsRng`, reseeded\non PID change:\n- **Performance**: avoids `getrandom` syscall on every nonce generation\n(1-10 µs per block under contention)\n- **Fork safety**: `ForkAwareRng` reseeds when it detects PID mismatch —\nAES-GCM is catastrophically broken on nonce reuse, a forked process\nsharing parent's RNG state would reuse nonces under the same key\n\n## CI hardening (already applied)\n\nMain-branch ruleset patched to require:\n- `lint`\n- `test (stable, ubuntu-latest)`\n\nAfter this, a PR with `lint: FAILURE` or `test: SKIPPED` cannot be\nauto-promoted by dependabot or any other path — `gh pr merge --auto`\nhonors required checks. No workflow file rewrite needed.\n\n## Test plan\n\n- [x] `cargo build --features encryption` clean\n- [x] `cargo clippy --all-features --all-targets -- -D warnings` clean\n- [x] `cargo nextest run --all-features` — 1277/1277 pass, including 16\n`src/encryption.rs::tests::aes256gcm::*` and\n`tests/encryption_roundtrip.rs`\n- [x] Ruleset checked: `lint` and `test (stable, ubuntu-latest)` listed\nas required\n\n## Out of scope\n\nAAD-bound block identity (binding ciphertext to `table_id` +\n`block_offset` + `dict_id` + `window_log` to defeat block-swap,\ndict-substitution, decompression-bomb, key-epoch attacks) lives in #250\n(spec) → #252 (BlockIdentity refactor) → #251 (wire impl) → #253\n(threat-model regression suite). Per Phase 6 of #215.\n\nCloses #246\n\n<!-- This is an auto-generated comment: release notes by coderabbit.ai\n-->\n## Summary by CodeRabbit\n\n* **Chores**\n* Pinned and updated cryptography and RNG dependencies to specific newer\nversions for consistent behavior.\n\n* **Refactor**\n* Updated encryption internals to use the newer crypto API while\npreserving on-disk data layout and error distinctions.\n* Improved RNG seeding and fork-resilience, and adjusted tests to\nreflect deterministic reseeding behavior.\n\n<!-- review_stack_entry_start -->\n\n[![Review Change\nStack](https://storage.googleapis.com/coderabbit_public_assets/review-stack-in-coderabbit-ui.svg)](https://app.coderabbit.ai/change-stack/structured-world/coordinode-lsm-tree/pull/258?utm_source=github_walkthrough&utm_medium=github&utm_campaign=change_stack)\n\n<!-- review_stack_entry_end -->\n<!-- end of auto-generated comment: release notes by coderabbit.ai -->",
          "timestamp": "2026-05-18T17:17:27+03:00",
          "tree_id": "10c454d4d2cec7cac40425387d81ada33cb44ecb",
          "url": "https://github.com/structured-world/coordinode-lsm-tree/commit/2416cf3c4b97177bd1c88b05d57e96beb869f5fe"
        },
        "date": 1779113930055,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "fillseq",
            "value": 2946833.88329379,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2518617 ops/sec | factor: 1.170 | P50: 0.3us | P99: 2.0us | P99.9: 4.6us\nthreads: 1 | elapsed: 0.08s | num: 200000 | iterations: 3 | runner: seq_wr=18258 rand_rd=849464 cpu=140 composite=19657.8"
          },
          {
            "name": "fillrandom",
            "value": 1681571.5742026318,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1437215 ops/sec | factor: 1.170 | P50: 0.5us | P99: 2.5us | P99.9: 5.3us\nthreads: 1 | elapsed: 0.14s | num: 200000 | iterations: 3 | runner: seq_wr=18258 rand_rd=849464 cpu=140 composite=19657.8"
          },
          {
            "name": "readrandom",
            "value": 695934.405930532,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 594805 ops/sec | factor: 1.170 | P50: 1.5us | P99: 5.1us | P99.9: 10.7us\nthreads: 1 | elapsed: 0.34s | num: 200000 | iterations: 3 | runner: seq_wr=18258 rand_rd=849464 cpu=140 composite=19657.8"
          },
          {
            "name": "readseq",
            "value": 3476216.9296776154,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 2971073 ops/sec | factor: 1.170 | P50: 0.2us | P99: 3.7us | P99.9: 6.8us\nthreads: 1 | elapsed: 0.07s | num: 200000 | iterations: 3 | runner: seq_wr=18258 rand_rd=849464 cpu=140 composite=19657.8"
          },
          {
            "name": "seekrandom",
            "value": 525636.2268096313,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 449254 ops/sec | factor: 1.170 | P50: 2.0us | P99: 5.6us | P99.9: 11.4us\nthreads: 1 | elapsed: 0.45s | num: 200000 | iterations: 3 | runner: seq_wr=18258 rand_rd=849464 cpu=140 composite=19657.8"
          },
          {
            "name": "prefixscan",
            "value": 276884.5887519276,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 236649 ops/sec | factor: 1.170 | P50: 4.0us | P99: 5.1us | P99.9: 13.3us\nthreads: 1 | elapsed: 0.85s | num: 200000 | iterations: 3 | runner: seq_wr=18258 rand_rd=849464 cpu=140 composite=19657.8"
          },
          {
            "name": "overwrite",
            "value": 1717883.3622892906,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 1468250 ops/sec | factor: 1.170 | P50: 0.5us | P99: 2.4us | P99.9: 5.2us\nthreads: 1 | elapsed: 0.14s | num: 200000 | iterations: 3 | runner: seq_wr=18258 rand_rd=849464 cpu=140 composite=19657.8"
          },
          {
            "name": "mergerandom",
            "value": 704011.4092745412,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 601708 ops/sec | factor: 1.170 | P50: 0.3us | P99: 1.8us | P99.9: 2.6us\nthreads: 1 | elapsed: 0.33s | num: 200000 | iterations: 3 | runner: seq_wr=18258 rand_rd=849464 cpu=140 composite=19657.8"
          },
          {
            "name": "readwhilewriting",
            "value": 651795.2070071183,
            "unit": "ops/sec (normalized)",
            "extra": "raw: 557080 ops/sec | factor: 1.170 | P50: 1.6us | P99: 3.9us | P99.9: 10.5us\nthreads: 1 | elapsed: 0.36s | num: 200000 | iterations: 3 | runner: seq_wr=18258 rand_rd=849464 cpu=140 composite=19657.8"
          }
        ]
      }
    ]
  }
}