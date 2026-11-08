# AGENTS.md

`coordinode-lsm-tree` is the storage engine under several products, and its
purpose is to beat the competing engines (RocksDB first) on every workload
those products run. The order of priorities is fixed:

1. **Correctness**: no undefined behaviour, no data loss, no wrong result, in
   every feature combination and after every crash.
2. **Performance**: throughput and tail latency (p99, p999) on the hot paths
   listed below.
3. **Size on disk**: bytes written per entry, block and file. Fewer bytes are
   fewer reads, so size is part of performance.

Developer convenience comes after all three. Longer code, `unsafe` with a
proven invariant, macro-expanded kernels and hand-hoisted pointers are
accepted when they buy one of the above.

## Code Review Rules

Review the diff against the three priorities above. A finding in any of the
sections below is **P1** unless it says otherwise; raise it even when the code
compiles, passes its tests and looks idiomatic.

### Correctness under every build

- The crate builds and must be correct under default features,
  `--all-features`, and `--no-default-features --features alloc` (no `std`).
  Check `unsafe` code, layouts and invariants against each of them.
- `UserValue` / `Slice` has two backends: `ByteView` by default and
  `bytes::Bytes` under `bytes_1`. A representation assumption (zeroed memory
  as an empty value, an inline layout, a niche) must hold for both.
- Uninitialised memory is never read, dropped or compared. When a slot can be
  left unwritten (a write that panics or unwinds between reserving and
  writing), the code that later drops or reads it must not see it.
- Unwinding is reachable: without `std`, the default allocation error handler
  panics, and callers may catch panics. State reachable after a caught panic
  must be valid.
- Data integrity: merge order, tombstone propagation, seqno visibility,
  off-by-one at block, segment and range boundaries, checksums over the right
  bytes, validation of every offset and length read from disk.
- Crash safety: data durable before the metadata that describes it; no reader
  sees a file still being written; recovery derives state from what is on
  disk, never from what was in memory.
- Concurrency: races on shared lists, lock order, an `Ordering` too weak for
  the happens-before the code relies on.
- Arithmetic on sizes and offsets: overflow and truncation. `saturating_*`
  that hides a broken invariant is a finding; `checked_*` with explicit
  handling, or plain arithmetic with a stated bound, is the norm.
- No `unwrap()` / `expect()` on I/O, deserialisation or user input; no
  `let _ =` on a `Result` that carries data integrity.
- A change to a type or signature that a standalone crate under `tools/`
  uses (the RocksDB comparison harness in `tools/compare-rocksdb` included)
  must update that crate too: it is built outside the main workspace.

### Performance on hot paths

Hot paths: memtable insert and get, point read, multi-get, iterators and
scans, block and index decode, filter probes, cache lookup and insert, flush,
the compaction merge loop, blob file read and write.

Raise as P1 any change on a hot path that adds, per item or per request:

- a heap allocation, a `clone()` of an owned buffer, a refcount bump, an
  atomic read-modify-write, a lock, or a syscall the work does not need;
- zeroing, copying or re-encoding bytes that are overwritten, discarded or
  could be borrowed;
- a second decode, parse, checksum or validation of the same data;
- work done eagerly that the caller may never use;
- `std::sync::Mutex`, `std::collections::HashMap` with SipHash, or `Vec` where
  `parking_lot`, `FxHashMap` / `hashbrown`, or `SmallVec` would do;
- a SIMD path selected at compile time (`#[cfg(target_feature)]`) instead of
  runtime detection with a scalar fallback that gives identical results.

When you propose a fix for a correctness finding, prefer the fix that leaves
the common path unchanged (a cold path, a marker checked on an existing
branch) over one that charges every operation, and state what your fix costs
per operation. A fix that adds per-operation cost where a zero-cost one exists
is itself a P1 finding.

A PR that changes a hot path states its measurement against `main` in its
body (the benchmark, before and after). Its absence is a P1 finding.

### Size on disk

Raise as P1 a change that writes more bytes per entry, block or file without a
measured gain in speed stated in the PR: wider fields, padding, per-record
headers or checksums where one per block suffices, metadata duplicated across
files. An on-disk format change must also say how existing data is converted.

### Not findings

- Code that is longer, uses `unsafe` with a `// SAFETY:` comment that proves
  the invariant, raw pointers hoisted out of a loop, `#[inline]`, or macro
  kernels, when it removes a measured or stated cost.
- An allocation without zeroing (`alloc` instead of `alloc_zeroed`) where
  every slot is written before it is read or dropped.
- Naming, import order, comment wording, repeated setup in tests.
- Edge cases every caller in the diff already handles, and collisions the
  type system rules out.
- A style or API choice a comment explains. A comment never settles a
  correctness, crash-safety or performance finding: check the code it
  describes.

## Repository conventions

- Lint: `cargo clippy --all-features --all-targets -- -D warnings` and the
  same without `--all-features`. Suppressions use
  `#[expect(lint, reason = "...")]`, not `#[allow]`.
- Tests: `cargo nextest run --all-features`; doc tests `cargo test --doc`.
  No mocks of the storage layer: tests run on `StdFs` in a `tempdir` or on
  `MemFs`. Names follow `fn <what>_<condition>_<expected>()` (only
  `src/compaction/leveled/test.rs` keeps `fn test_<scenario>()`). A corruption
  test tampers the field on disk through the production serialisation path.
- no-std: library code prefers `core::*`, then `alloc::*`, then a no-std
  crate, then `std::*` behind `#[cfg(feature = "std")]`. The `no-std-check`
  job builds for `thumbv7em-none-eabihf` with
  `--no-default-features --features alloc` and its error count never grows.
  Tests, benches and binaries may use `std` freely.
- Commits: `<type>(scope): <description>`, a body with what and why.
