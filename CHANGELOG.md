# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Fork:** [structured-world/coordinode-lsm-tree](https://github.com/structured-world/coordinode-lsm-tree),
> a maintained fork of [fjall-rs/lsm-tree](https://github.com/fjall-rs/lsm-tree).
> Fork releases use `v`-prefixed tags (`v4.0.0`); upstream uses bare tags (`3.1.2`).

## [Unreleased]

## [5.4.0](https://github.com/structured-world/coordinode-lsm-tree/compare/v5.3.0...v5.4.0) - 2026-06-16

### Added

- *(integrity)* per-KV residence checksum at memtable insert ([#481](https://github.com/structured-world/coordinode-lsm-tree/pull/481))
- *(fs)* raw io_uring Fs / FsFile backend (no_std) ([#477](https://github.com/structured-world/coordinode-lsm-tree/pull/477))
- *(read)* opt-in row cache for point reads ([#476](https://github.com/structured-world/coordinode-lsm-tree/pull/476))
- *(table)* retrieval-ribbon point-read locator (O(1) point reads) ([#468](https://github.com/structured-world/coordinode-lsm-tree/pull/468))
- *(fs)* no_std io_uring driver core on raw Linux syscalls ([#471](https://github.com/structured-world/coordinode-lsm-tree/pull/471))

### Fixed

- *(build)* pin standalone tool crates as their own workspace roots ([#473](https://github.com/structured-world/coordinode-lsm-tree/pull/473))

### Performance

- *(memtable)* devirtualize the default comparator in skiplist search ([#480](https://github.com/structured-world/coordinode-lsm-tree/pull/480))
- *(table)* expand leb128 decode as a macro at call sites ([#478](https://github.com/structured-world/coordinode-lsm-tree/pull/478))
- *(table)* move seqno bounds to a parallel section ([#474](https://github.com/structured-world/coordinode-lsm-tree/pull/474))
- *(table)* no-std SIMD key-comparison dispatch ([#469](https://github.com/structured-world/coordinode-lsm-tree/pull/469))

## [5.3.0](https://github.com/structured-world/coordinode-lsm-tree/compare/v5.2.1...v5.3.0) - 2026-06-15

### Added

- *(bench)* blob_tree (KV-separation) arm + read-gap attribution ([#462](https://github.com/structured-world/coordinode-lsm-tree/pull/462))
- *(bench)* RocksDbParity preset + Benchmark Symmetry Invariant ([#461](https://github.com/structured-world/coordinode-lsm-tree/pull/461))
- *(time)* injectable Clock trait + no-std-check as a required gate ([#460](https://github.com/structured-world/coordinode-lsm-tree/pull/460))
- *(tooling)* repair KV-separated (blob) trees ([#459](https://github.com/structured-world/coordinode-lsm-tree/pull/459))
- *(recovery)* cross-process directory lock for exclusive tree access ([#458](https://github.com/structured-world/coordinode-lsm-tree/pull/458))
- *(metrics)* unified ECC-recovery counters, wire SEC-DED reads ([#457](https://github.com/structured-world/coordinode-lsm-tree/pull/457))
- *(ecc)* patrol scrub for proactive latent-error correction ([#456](https://github.com/structured-world/coordinode-lsm-tree/pull/456))

### Documentation

- *(compaction)* clarify major_compact watermark + clean subcompaction profile ([#467](https://github.com/structured-world/coordinode-lsm-tree/pull/467))

### Performance

- *(table)* cut point-read per-get overhead ([#465](https://github.com/structured-world/coordinode-lsm-tree/pull/465))

### Refactored

- *(filter)* remove dead from-keys BuildHasher path from vendored ribbon ([#455](https://github.com/structured-world/coordinode-lsm-tree/pull/455))

## [5.2.1](https://github.com/structured-world/coordinode-lsm-tree/compare/v5.2.0...v5.2.1) - 2026-06-12

### Refactored

- *(no-std)* make the core LSM path no_std + alloc capable ([#451](https://github.com/structured-world/coordinode-lsm-tree/pull/451))

## [5.2.0](https://github.com/structured-world/coordinode-lsm-tree/compare/v5.1.0...v5.2.0) - 2026-06-10

### Added

- *(cache)* replace quick_cache with in-tree sharded S3-FIFO cache ([#429](https://github.com/structured-world/coordinode-lsm-tree/pull/429)) ([#448](https://github.com/structured-world/coordinode-lsm-tree/pull/448))
- *(tooling)* key-free forensic block structure dump ([#256](https://github.com/structured-world/coordinode-lsm-tree/pull/256)) ([#447](https://github.com/structured-world/coordinode-lsm-tree/pull/447))
- *(ecc)* self-heal SSTs after a parity-corrected read ([#446](https://github.com/structured-world/coordinode-lsm-tree/pull/446))
- *(tooling)* reconstruct block AAD for offline tag verification ([#445](https://github.com/structured-world/coordinode-lsm-tree/pull/445))
- *(tooling)* key-free forensic dump-block for encrypted SSTs ([#444](https://github.com/structured-world/coordinode-lsm-tree/pull/444))
- *(verify)* verify_checksum scrubber with parallelism + throttle ([#435](https://github.com/structured-world/coordinode-lsm-tree/pull/435))
- *(ecc)* SEC-DED single-bit fast path on the Page ECC read path ([#437](https://github.com/structured-world/coordinode-lsm-tree/pull/437))
- *(compression)* pin inner-frame dict id as configurable-checksum defense-in-depth ([#441](https://github.com/structured-world/coordinode-lsm-tree/pull/441))

### Fixed

- *(verify)* don't sleep the scrub throttle after the last SST ([#443](https://github.com/structured-world/coordinode-lsm-tree/pull/443))
- *(manifest)* reject torn edit-log tail under AbsoluteConsistency ([#438](https://github.com/structured-world/coordinode-lsm-tree/pull/438))

## [5.1.0](https://github.com/structured-world/coordinode-lsm-tree/compare/v5.0.0...v5.1.0) - 2026-06-09

### Added

- *(scan)* scan_since_seqno CDC stream + synchronous clear() reclaim ([#433](https://github.com/structured-world/coordinode-lsm-tree/pull/433))

### Fixed

- *(fs)* silence unfulfilled non_snake_case expect on Windows ([#431](https://github.com/structured-world/coordinode-lsm-tree/pull/431))

## [5.0.0](https://github.com/structured-world/coordinode-lsm-tree/compare/v4.5.0...v5.0.0) - 2026-06-08

### Added

- *(fs)* filesystem-aware capability framework (CoW-disable + reflink) ([#428](https://github.com/structured-world/coordinode-lsm-tree/pull/428))
- *(encryption)* activate AAD-bound block encryption on the live path ([#423](https://github.com/structured-world/coordinode-lsm-tree/pull/423))
- *(manifest)* incremental manifest via append-only edit log ([#421](https://github.com/structured-world/coordinode-lsm-tree/pull/421))
- *(query)* cold-block partial decode with resumable zstd ([#415](https://github.com/structured-world/coordinode-lsm-tree/pull/415))
- *(ecc)* configurable ECC-at-rest scheme (XOR / Reed-Solomon) on the Page ECC path ([#414](https://github.com/structured-world/coordinode-lsm-tree/pull/414))
- *(tooling)* repair_db — rebuild MANIFEST from on-disk SSTs ([#409](https://github.com/structured-world/coordinode-lsm-tree/pull/409))
- *(compaction)* compaction-time range tombstones + bottommost seqno-zeroing ([#407](https://github.com/structured-world/coordinode-lsm-tree/pull/407))
- *(compaction)* parallel sub-compaction across key ranges ([#402](https://github.com/structured-world/coordinode-lsm-tree/pull/402))
- *(compaction)* parallel block compression ([#401](https://github.com/structured-world/coordinode-lsm-tree/pull/401))
- *(bench)* add point-read workload to compare-rocksdb harness ([#372](https://github.com/structured-world/coordinode-lsm-tree/pull/372))
- *(index)* per-block seqno bounds in SST index entries ([#371](https://github.com/structured-world/coordinode-lsm-tree/pull/371))
- *(integrity)* [**breaking**] per-KV checksum protection + flag-based ECC parity (V5 block header) ([#369](https://github.com/structured-world/coordinode-lsm-tree/pull/369))
- *(encryption)* outer Reed-Solomon ECC frame codec over block ciphertext ([#365](https://github.com/structured-world/coordinode-lsm-tree/pull/365))
- *(manifest)* Blocks-based manifest hardening ([#297](https://github.com/structured-world/coordinode-lsm-tree/pull/297)) ([#357](https://github.com/structured-world/coordinode-lsm-tree/pull/357))
- *(config)* runtime-toggleable RuntimeConfig foundation ([#352](https://github.com/structured-world/coordinode-lsm-tree/pull/352)) ([#355](https://github.com/structured-world/coordinode-lsm-tree/pull/355))
- *(version)* PointInTimeRecovery + SkipAnyCorruptedRecords + per-record framing ([#323](https://github.com/structured-world/coordinode-lsm-tree/pull/323)) ([#342](https://github.com/structured-world/coordinode-lsm-tree/pull/342))
- *(io)* local Read/Write/Seek trait surface to lift Fs off std::io ([#311](https://github.com/structured-world/coordinode-lsm-tree/pull/311)) ([#347](https://github.com/structured-world/coordinode-lsm-tree/pull/347))
- *(bench)* foundation for compare-rocksdb head-to-head harness ([#244](https://github.com/structured-world/coordinode-lsm-tree/pull/244)) ([#345](https://github.com/structured-world/coordinode-lsm-tree/pull/345))
- *(encryption)* top-level encrypt_block/decrypt_block + KeyChain + wire format ([#251](https://github.com/structured-world/coordinode-lsm-tree/pull/251)) ([#344](https://github.com/structured-world/coordinode-lsm-tree/pull/344))
- *(ecc)* per-block Reed-Solomon Page ECC ([#267](https://github.com/structured-world/coordinode-lsm-tree/pull/267)) ([#343](https://github.com/structured-world/coordinode-lsm-tree/pull/343))
- *(sst-dump)* dump subcommand + public inspect::iter_data_block_entries facade ([#335](https://github.com/structured-world/coordinode-lsm-tree/pull/335))
- *(sst-dump)* filter-stats subcommand + public inspect::read_filter_stats facade ([#334](https://github.com/structured-world/coordinode-lsm-tree/pull/334))
- *(encryption)* AEAD dispatch with AES-256-GCM + ChaCha20-Poly1305 (#251 PR2) ([#338](https://github.com/structured-world/coordinode-lsm-tree/pull/338))
- *(encryption)* AAD construction + decode-time error types (#251 foundation) ([#336](https://github.com/structured-world/coordinode-lsm-tree/pull/336))
- *(sst-dump)* index-dump subcommand + public inspect facade extension ([#333](https://github.com/structured-world/coordinode-lsm-tree/pull/333))
- *(sst-dump)* properties subcommand + public inspect facade ([#328](https://github.com/structured-world/coordinode-lsm-tree/pull/328))
- *(table)* mirror TLI block near file tail for torn-write safety ([#325](https://github.com/structured-world/coordinode-lsm-tree/pull/325))
- *(sst-dump)* hex subcommand for raw block-region dump with header decode ([#327](https://github.com/structured-world/coordinode-lsm-tree/pull/327))
- *(tooling)* sst-dump CLI scaffold + verify subcommand ([#301](https://github.com/structured-world/coordinode-lsm-tree/pull/301)) ([#316](https://github.com/structured-world/coordinode-lsm-tree/pull/316))
- *(config)* ManifestRecoveryMode + TolerateCorruptedTailRecords ([#299](https://github.com/structured-world/coordinode-lsm-tree/pull/299)) ([#317](https://github.com/structured-world/coordinode-lsm-tree/pull/317))
- *(verify)* per-block XXH3 scrub for proactive bit-rot detection (#300 part 1) ([#313](https://github.com/structured-world/coordinode-lsm-tree/pull/313))
- *(fs)* FileHint enum + FsFile::hint() primitive for posix_fadvise (#133 Phase 1a) ([#307](https://github.com/structured-world/coordinode-lsm-tree/pull/307))
- *(seeking-merger)* allow self-coordinating independent-cursor sources via broadened CoherentMergeSource ([#280](https://github.com/structured-world/coordinode-lsm-tree/pull/280)) ([#305](https://github.com/structured-world/coordinode-lsm-tree/pull/305))
- *(range)* wire SeekingMerger into Tree::range read path ([#222](https://github.com/structured-world/coordinode-lsm-tree/pull/222)) ([#288](https://github.com/structured-world/coordinode-lsm-tree/pull/288))
- *(merge)* SeekingMerger — RocksDB-style dual loser trees ([#222](https://github.com/structured-world/coordinode-lsm-tree/pull/222)) ([#284](https://github.com/structured-world/coordinode-lsm-tree/pull/284))
- *(checkpoint)* [**breaking**] hard-link snapshot for PITR backup (V5 storage) ([#276](https://github.com/structured-world/coordinode-lsm-tree/pull/276))
- *(filter)* replace standard bloom with BuRR ([#269](https://github.com/structured-world/coordinode-lsm-tree/pull/269))

### Documentation

- fix broken and redundant intra-doc links across the crate ([#368](https://github.com/structured-world/coordinode-lsm-tree/pull/368))
- *(format_version)* document on-disk version bump policy ([#360](https://github.com/structured-world/coordinode-lsm-tree/pull/360))
- add Manifest recovery modes section to README ([#332](https://github.com/structured-world/coordinode-lsm-tree/pull/332))
- *(encryption)* AAD-bound encrypted block wire format spec ([#250](https://github.com/structured-world/coordinode-lsm-tree/pull/250)) ([#318](https://github.com/structured-world/coordinode-lsm-tree/pull/318))

### Fixed

- *(compaction/leveled)* tie level-count assertion to config ([#359](https://github.com/structured-world/coordinode-lsm-tree/pull/359))
- *(table/index)* default partitioned index ON at every level ([#329](https://github.com/structured-world/coordinode-lsm-tree/pull/329)) ([#340](https://github.com/structured-world/coordinode-lsm-tree/pull/340))
- *(table)* mirror meta block at mid-file for tail-corruption resilience ([#295](https://github.com/structured-world/coordinode-lsm-tree/pull/295)) ([#314](https://github.com/structured-world/coordinode-lsm-tree/pull/314))

### Performance

- *(memtable)* stop zeroing the arena, restore cheap fixed-block decode ([#420](https://github.com/structured-world/coordinode-lsm-tree/pull/420))
- *(memtable)* geometric arena chunks (no 64 MiB zeroing per flush) ([#417](https://github.com/structured-world/coordinode-lsm-tree/pull/417))
- *(compression)* pass source-size hint on zstd block compression ([#404](https://github.com/structured-world/coordinode-lsm-tree/pull/404))
- *(table)* raise index spill threshold to 4 MiB ([#399](https://github.com/structured-world/coordinode-lsm-tree/pull/399))
- *(table)* size-adaptive block index — single-level for small SSTs ([#397](https://github.com/structured-world/coordinode-lsm-tree/pull/397))
- *(read)* lock-free latest-version fast path for point reads ([#394](https://github.com/structured-world/coordinode-lsm-tree/pull/394))
- *(compaction)* I/O rate limiter (leaky token bucket) ([#389](https://github.com/structured-world/coordinode-lsm-tree/pull/389))
- *(io)* drop cold-level SST output from the page cache after write ([#392](https://github.com/structured-world/coordinode-lsm-tree/pull/392))
- *(table)* extend longest_shared_prefix_length SIMD to 32-bit x86 (i686) ([#382](https://github.com/structured-world/coordinode-lsm-tree/pull/382))
- *(table)* AVX-512BW 64-byte lane for longest_shared_prefix_length ([#380](https://github.com/structured-world/coordinode-lsm-tree/pull/380))
- *(fs)* pure hard_link; single SyncMode-aware cross-fs copy path ([#378](https://github.com/structured-world/coordinode-lsm-tree/pull/378))
- *(fs)* configurable SyncMode; default plain fsync on macOS ([#374](https://github.com/structured-world/coordinode-lsm-tree/pull/374))
- *(read)* borrow index block on point-read, parse trailer once ([#376](https://github.com/structured-world/coordinode-lsm-tree/pull/376))
- *(zstd)* retain dictionary id in frame header ([#366](https://github.com/structured-world/coordinode-lsm-tree/pull/366))
- *(fs)* O_DIRECT foundation — AlignedBuf + FsOpenOptions::direct_io (#133 phase 2) ([#310](https://github.com/structured-world/coordinode-lsm-tree/pull/310))
- *(table/scanner)* bump compaction readahead 32 KiB → 2 MiB (#133 Phase 1c) ([#308](https://github.com/structured-world/coordinode-lsm-tree/pull/308))
- *(ci)* proptest env-var budgets + slow-timeout audit (#158 items 2-5) ([#306](https://github.com/structured-world/coordinode-lsm-tree/pull/306))
- *(table)* add Table::batch_get for sorted multi-key point reads (#223 phase 1) ([#290](https://github.com/structured-world/coordinode-lsm-tree/pull/290))
- *(loser_tree)* eliminate comparator dispatch, close #283 fully ([#287](https://github.com/structured-world/coordinode-lsm-tree/pull/287))
- *(loser_tree)* close N≥16 regression, reduce small-N gap (#283 partial) ([#286](https://github.com/structured-world/coordinode-lsm-tree/pull/286))
- *(compression)* pre-parse zstd dict once via OnceCell + no-std foundation ([#273](https://github.com/structured-world/coordinode-lsm-tree/pull/273))

### Refactored

- *(block)* collapse 4 Block I/O paths into a single BlockTransform enum ([#248](https://github.com/structured-world/coordinode-lsm-tree/pull/248)) ([#337](https://github.com/structured-world/coordinode-lsm-tree/pull/337))
- *(block)* [**breaking**] thread BlockIdentity through Block I/O API ([#252](https://github.com/structured-world/coordinode-lsm-tree/pull/252)) ([#294](https://github.com/structured-world/coordinode-lsm-tree/pull/294))
- *(table/block/tests)* extract write_block_to_tempfile helper (#128 part 1) ([#293](https://github.com/structured-world/coordinode-lsm-tree/pull/293))
- *(filter/burr)* re-deny indexing/expect/unwrap with per-site justifications ([#270](https://github.com/structured-world/coordinode-lsm-tree/pull/270)) ([#282](https://github.com/structured-world/coordinode-lsm-tree/pull/282))

### Testing

- *(compare)* honest L3 compaction baseline vs RocksDB ([#427](https://github.com/structured-world/coordinode-lsm-tree/pull/427))
- *(compare)* add surrealkv as a third bench engine ([#425](https://github.com/structured-world/coordinode-lsm-tree/pull/425))
- *(compare-rocksdb)* zstd-22 compression axis + scan/seek/overwrite scenarios ([#386](https://github.com/structured-world/coordinode-lsm-tree/pull/386))
- *(encryption)* AAD threat-model regression suite (first wave) ([#361](https://github.com/structured-world/coordinode-lsm-tree/pull/361))
- *(table/index)* tighten blast-radius assertion to corruption variants ([#341](https://github.com/structured-world/coordinode-lsm-tree/pull/341))
- triage all #[ignore] annotations across the crate ([#326](https://github.com/structured-world/coordinode-lsm-tree/pull/326))
- *(table)* pin global-seqno translation on Table::get return path ([#321](https://github.com/structured-world/coordinode-lsm-tree/pull/321)) ([#322](https://github.com/structured-world/coordinode-lsm-tree/pull/322))
- *(verify)* pin DataReadError routing for truncated data segment ([#315](https://github.com/structured-world/coordinode-lsm-tree/pull/315)) ([#319](https://github.com/structured-world/coordinode-lsm-tree/pull/319))
- *(encryption)* mixed-load stress test across encryption × compression matrix (#128 part 2) ([#304](https://github.com/structured-world/coordinode-lsm-tree/pull/304))
- *(bench)* P99/P999 tail-latency reporting for BuRR/ribbon probes ([#271](https://github.com/structured-world/coordinode-lsm-tree/pull/271)) ([#281](https://github.com/structured-world/coordinode-lsm-tree/pull/281))

## [4.5.0](https://github.com/structured-world/coordinode-lsm-tree/compare/v4.4.0...v4.5.0) - 2026-05-19

### Added

- *(ci)* smart upstream-monitor with release intelligence + path-aware CodeRabbit profile ([#264](https://github.com/structured-world/coordinode-lsm-tree/pull/264))
- *(vlog)* dictionary compression for blob files ([#233](https://github.com/structured-world/coordinode-lsm-tree/pull/233))

### Fixed

- *(encryption)* restore --features encryption build (aes-gcm 0.11.0-rc.3 + rand_chacha 0.10) ([#258](https://github.com/structured-world/coordinode-lsm-tree/pull/258))

### Performance

- devirtualize lexicographic comparator on block binary-search hot path ([#266](https://github.com/structured-world/coordinode-lsm-tree/pull/266))
- *(util)* SIMD longest_shared_prefix_length() (Phase 2.1) ([#245](https://github.com/structured-world/coordinode-lsm-tree/pull/245))

## [4.4.0](https://github.com/structured-world/coordinode-lsm-tree/compare/v4.3.1...v4.4.0) - 2026-04-09

### Added

- *(compression)* enable dictionary compression in pure Rust backend ([#229](https://github.com/structured-world/coordinode-lsm-tree/pull/229))

### Performance

- *(compression)* cache pre-compiled Dictionary across block decompress calls ([#227](https://github.com/structured-world/coordinode-lsm-tree/pull/227))

## [4.3.1](https://github.com/structured-world/coordinode-lsm-tree/compare/v4.3.0...v4.3.1) - 2026-04-06

### Performance

- *(compression)* use numeric zstd levels in pure Rust backend ([#226](https://github.com/structured-world/coordinode-lsm-tree/pull/226))
- batch multi_get + PinnableSlice + WriteBatch ([#214](https://github.com/structured-world/coordinode-lsm-tree/pull/214))

## [4.3.0](https://github.com/structured-world/coordinode-lsm-tree/compare/v4.2.0...v4.3.0) - 2026-04-05

### Added

- *(fs)* MemFs — in-memory Fs implementation for testing and in-memory trees ([#211](https://github.com/structured-world/coordinode-lsm-tree/pull/211))

### Fixed

- *(table)* validate block type on cache-hit path ([#203](https://github.com/structured-world/coordinode-lsm-tree/pull/203))
- *(table)* two-level index scan stops prematurely on empty child partitions ([#202](https://github.com/structured-world/coordinode-lsm-tree/pull/202))

### Performance

- *(table)* add infallible OwnedIndexBlockIter constructor for pre-validated blocks ([#206](https://github.com/structured-world/coordinode-lsm-tree/pull/206))

### Refactored

- *(fs)* migrate Tree::open recovery path to Fs trait ([#212](https://github.com/structured-world/coordinode-lsm-tree/pull/212))
- *(table)* make index block bound-cursor helpers fallible ([#205](https://github.com/structured-world/coordinode-lsm-tree/pull/205))
- *(table)* make all meta/trailer reads fallible for truncated blocks ([#204](https://github.com/structured-world/coordinode-lsm-tree/pull/204))
- *(table)* make block decoder trailer validation fallible ([#199](https://github.com/structured-world/coordinode-lsm-tree/pull/199))

## [4.2.0](https://github.com/structured-world/coordinode-lsm-tree/compare/v4.1.0...v4.2.0) - 2026-03-26

### Added

- comparator-aware range tombstones ([#180](https://github.com/structured-world/coordinode-lsm-tree/pull/180))
- *(compression)* CompressionProvider trait + pure Rust zstd backend ([#176](https://github.com/structured-world/coordinode-lsm-tree/pull/176))
- *(error)* RouteMismatch error, blocked_bloom cleanup, bench/clippy fixes ([#166](https://github.com/structured-world/coordinode-lsm-tree/pull/166))
- *(config)* per-level Fs routing for tiered storage ([#163](https://github.com/structured-world/coordinode-lsm-tree/pull/163))

### Performance

- *(bench)* consolidate benchmarks + nextest + flamegraph pipeline ([#175](https://github.com/structured-world/coordinode-lsm-tree/pull/175))

### Testing

- *(table)* add zstd dict helper coverage ([#181](https://github.com/structured-world/coordinode-lsm-tree/pull/181))

## [4.1.0](https://github.com/structured-world/coordinode-lsm-tree/compare/v4.0.0...v4.1.0) - 2026-03-24

### Added

- *(fs)* io_uring Fs implementation for high-throughput I/O ([#106](https://github.com/structured-world/coordinode-lsm-tree/pull/106))
- *(compression)* zstd dictionary compression support ([#131](https://github.com/structured-world/coordinode-lsm-tree/pull/131))

### Documentation

- add benchmark dashboard link and update badges ([#151](https://github.com/structured-world/coordinode-lsm-tree/pull/151))
- add v4.0.0 fork epoch changelog (all changes since upstream v3.1.1)

### Fixed

- *(version)* fsync version file before rewriting CURRENT pointer ([#152](https://github.com/structured-world/coordinode-lsm-tree/pull/152))
- thread UserComparator through ingestion guards and range overlap ([#139](https://github.com/structured-world/coordinode-lsm-tree/pull/139))

### Performance

- *(bench)* add multi-threaded support to all db_bench workloads ([#155](https://github.com/structured-world/coordinode-lsm-tree/pull/155))
- *(merge)* replace IntervalHeap with sorted-vec heap + replace_min/replace_max ([#148](https://github.com/structured-world/coordinode-lsm-tree/pull/148))
- *(compaction)* merge input ranges before L2 overlap query ([#146](https://github.com/structured-world/coordinode-lsm-tree/pull/146))

### Refactored

- *(version)* comparator API cleanup — TransformContext + rename Run::push() ([#153](https://github.com/structured-world/coordinode-lsm-tree/pull/153))
- add #[non_exhaustive] to CompressionType enum

## [4.0.0] — Fork Epoch (2026-03-23)

First release of `coordinode-lsm-tree` — maintained fork of [fjall-rs/lsm-tree](https://github.com/fjall-rs/lsm-tree) v3.1.1.
Published to [crates.io](https://crates.io/crates/coordinode-lsm-tree). All changes since upstream v3.1.1.

### Added

- Merge operators for commutative LSM operations (#28)
- Range tombstones (delete_range / delete_prefix) with V4 disk format (#21)
- Block-level encryption at rest (AES-256-GCM) (#71)
- Custom key comparison / UserComparator (#67)
- Prefix bloom filters for graph key encoding (#43, #64, #68, #70)
- Arena-based skiplist for memtable (#79)
- Fs trait for pluggable filesystem backends (#80, #109, #107, #112)
- Zstd compression support
- SequenceNumberGenerator trait (#10)
- multi_get() for batch point reads (#9)
- verify_integrity() for full-file checksum verification (#4)
- Intra-L0 compaction for overlapping runs (#5)
- Optimized contains_prefix() method (#6)
- Size-tiered, dynamic leveling, and multi-level compaction strategies (#66)
- db_bench benchmark suite (#45)
- Per-source RT visibility in range/prefix iteration
- Write-side size cap enforcement
- Seqno-aware seek for iterator bounds

### Fixed

- Resolve L0 stale reads when optimize_runs reorders SSTs (#56)
- Select highest-seqno entry across all L0 tables (#54)
- Cursor wrap on exact block fill corrupts arena (#130)
- Thread UserComparator through Run, KeyRange, and Version methods (#117)
- Preserve range tombstones covering gaps between output tables (#137)
- Scanner should not treat corrupted magic matching META as EOF (#63)
- Replace panic paths in vlog Metadata::from_slice with Result (#62)
- Decompression buffer validation (#7)
- V4 blob frame header checksum (#44)
- 100+ correctness fixes for range tombstones, compaction, MVCC

### Performance

- Partition-aware bloom filtering for point-read pipeline (#102)
- Lazy iterator pipeline initialization for point reads (#110)
- Replace OsRng with thread-local seeded CSPRNG (#104)
- Reduce allocations in encrypt/decrypt block pipeline (#105)
- Optimize range tombstone lookup in table-skip and point-read (#55)
- Seqno-aware seek in data block point reads (#8)
- Compute L2 overlaps per-range in multi-level compaction (#108)
- Unify merge resolution via bloom-filtered iterator pipeline (#69)

### Refactored

- Centralize OwnedIndexBlockIter adapter pattern (#99)
- Return CompactionResult from Tree::compact (#103)
- Thread Fs through FileAccessor, DescriptorTable, table::Writer, BlobFile (#107, #112)
- Seal AbstractTree internals
- Replace Mutex with RwLock for range tombstone concurrency
- Add #[non_exhaustive] to CompressionType enum

### Testing

- 43 new test suites: property-based oracle, custom comparator, encryption, corruption, concurrency
- Integration tests for compaction/merge with custom comparator (#100)
- BTreeMap oracle with multi-byte prefix keys (#65)
- End-to-end corruption test for seqno metadata (#96)
