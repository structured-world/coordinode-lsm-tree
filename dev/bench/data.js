window.BENCHMARK_DATA = {
  "lastUpdate": 1774255217479,
  "repoUrl": "https://github.com/structured-world/lsm-tree",
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
      }
    ]
  }
}