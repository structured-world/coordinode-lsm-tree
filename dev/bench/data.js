window.BENCHMARK_DATA = {
  "lastUpdate": 1774317263286,
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
      }
    ],
    "coordinode-lsm-tree db_bench": [
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
      }
    ]
  }
}