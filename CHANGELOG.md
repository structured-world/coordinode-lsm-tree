# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Fork:** [structured-world/coordinode-lsm-tree](https://github.com/structured-world/coordinode-lsm-tree),
> a maintained fork of [fjall-rs/lsm-tree](https://github.com/fjall-rs/lsm-tree).
> Fork releases use `v`-prefixed tags (`v4.0.0`); upstream uses bare tags (`3.1.2`).

## [Unreleased]

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
