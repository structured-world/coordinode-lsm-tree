# Storage Engine Invariants

The load-bearing invariants the engine enforces, grouped by subsystem. Each entry
states the **rule**, **why** it holds, and **where** it is enforced (the module or
test that would break if the invariant were violated), so a reviewer can answer
"does this change violate invariant X?" without re-deriving the contract.

This is a living reference: when a change adds or alters an invariant, update the
matching entry (and add one for a new subsystem).

---

## SST block / data layout

- **Keys within a data block are sorted by internal key.** A block iterator and
  the binary search over restart points both assume ascending order; an unsorted
  block would silently return wrong seek results. Enforced by the writer emitting
  entries in sorted order (`src/table/writer`) and verified by the block
  round-trip tests (`src/table/data_block`).

- **Internal key order is (user_key ascending, seqno descending).** For one user
  key the newest version (highest seqno) sorts first, so a forward scan meets the
  visible version before older ones. Enforced in the comparator
  (`src/value.rs` `InternalKey` ordering; tests `pik_cmp_user_key`,
  `pik_cmp_seqno`).

- **A block's checksum covers exactly its payload.** The per-block XXH3 (or
  optional CRC32C) is computed over the block bytes only; parity / per-KV footers
  sit outside that scope and are sized from the per-SST descriptor, not guessed.
  A reader that mis-scopes the checksum reports spurious corruption. Enforced in
  `src/table/block` (header + trailer) and the block-verify walk (`src/verify.rs`).

- **SST block headers omit the `block_flags` byte; self-describing blocks carry
  it.** `Data` / `Index` / `Filter` / `RangeTombstone` derive parity / footer
  presence from the per-SST meta descriptor, while `Meta` / `Manifest` /
  `ManifestFooter` carry an explicit `block_flags` byte (V5 format). The block
  magic is bumped so a pre-V5 reader rejects V5 blocks at header decode. Enforced
  in `src/table/block/header.rs` and the format-version gate.

- **A per-block zone map conservatively bounds its block.** The stored
  `[min, max]` key range and row count cover every key in the block: a query key
  outside `[min, max]` is provably absent (so a range scan may skip the block),
  but the bounds never exclude a key that is present: a false skip would lose
  data. Enforced in `src/table/zone_map.rs` (+ the zone-map round-trip /
  selectivity tests).

## Filter (AMQ)

- **The filter never produces a false negative.** A `BuRR` membership filter may
  report a key present when it is absent (a false positive, at rate ≈ 2⁻ʳ), but it
  must NEVER report a key absent when it is present: a false negative would skip a
  real key and silently lose data on read. Enforced by the build / probe symmetry
  in `src/table/filter/ribbon/burr` (every built key reports present; the
  round-trip tests assert no built key is ever rejected).

## Block cache

- **A cache lookup returns the same bytes a fresh read would.** The block cache is
  keyed by `(block_type, tree, table, offset)`, an exact physical address, so a
  hit returns precisely that block, indistinguishable from a disk read (no key
  comparison is needed because the offset is exact, not hashed). The separate
  point-read (row) cache is keyed by a key *hash* (the same hash the bloom filter
  uses), so on a hit it compares the stored `user_key` against the lookup key and
  rejects a hash collision rather than serving a wrong value. Enforced in
  `src/cache.rs` (`get_block` / `get_row`) and the cache tests.

## Manifest

- **A manifest version installs atomically.** A new version becomes current only
  by flipping the current pointer after the new manifest is fully written and
  synced; a crash mid-write leaves the previous version current. Enforced in
  `src/manifest.rs` / `src/manifest_blocks` (writer + current-pointer flip).

- **Recovery accepts only a complete prefix.** On open, the manifest is replayed
  up to the last intact, checksum-valid record; a torn tail (partial final write)
  is discarded, not half-applied. Enforced by the manifest reader's framing +
  checksum checks (`src/manifest_blocks/reader.rs`, `footer.rs`).

## Compaction

- **Compaction output preserves every MVCC version visible to a live snapshot.**
  A merge may physically drop a version only when no open snapshot can observe it;
  otherwise a snapshot read would change answer across a compaction. Enforced in
  `src/compaction/worker.rs` (merge + tombstone handling) and the compaction
  integration tests (`src/compaction/leveled`).

- **A weak tombstone is dropped below the lowest live snapshot, or when it
  collapses with its matching value.** Dropping a delete marker that a snapshot
  still needs would resurrect the deleted key for that snapshot. Beyond the
  snapshot-watermark rule, the compaction stream also drops a weak tombstone when
  the next entry for the same key is a `Value` (single-delete semantics: the weak
  delete annihilates exactly that one put). Enforced in the merge / drop logic
  (`src/compaction/stream.rs`, `src/range_tombstone_filter.rs`).

- **Sequence numbers are zeroed only at the bottommost level, and only when no
  range tombstone covers the key.** Packing a seqno to zero is safe only at the
  bottommost level with no live snapshot beneath. It additionally requires that no
  range tombstone anywhere in the version covers the key: range tombstones are
  applied by seqno comparison (`RT@r` suppresses `K@s` iff it covers `K` and
  `s < r`), so zeroing `K@s` to `K@0` would let any covering tombstone with
  `r > 0` wrongly suppress it, even one older than the original entry. Tombstones
  are gathered from every level, not just this compaction's inputs. Enforced in the
  bottommost seqno-zeroer (`src/compaction/seqno_zeroer.rs`).

- **L0 compaction is triggered by table (file) count, and pending-compaction debt
  is measured the same way.** Both the `choose` trigger and `compaction_debt`
  count tables, not runs, so a multi-table L0 run is not under-counted. Enforced
  in `src/compaction/leveled` (test `compaction_debt_flags_l0_over_threshold_then_clears`).

## File lifecycle and concurrency

- **A file referenced by a live version or an in-flight reader is never deleted.**
  Compaction installs the new version and only then drops inputs that no version or
  open iterator / snapshot still references; an obsolete SST is unlinked after its
  last reader releases it, never while in use. A premature delete would fault an
  active read. Enforced by the version ref-counting in `src/version` and the file
  GC in `src/compaction` / `src/tree`.

- **A directory has at most one writer process.** Opening a tree acquires an
  exclusive lock on a `LOCK` file; a second open of the same directory fails with
  `Error::Locked` rather than corrupting shared state through two uncoordinated
  writers. Enforced in the open path via the `Fs` advisory lock
  (`Config::with_directory_lock`, default on) and the cross-process lock tests.

## Snapshot / sequence number

- **Sequence numbers are monotonic and caller-assigned.** The engine honors the
  seqno passed to `insert`; the caller draws them from a monotonic source. A read
  at read-seqno `R` sees only versions with `seqno < R` (the read seqno is an
  exclusive upper bound); the newest such version of each key. The visible
  watermark is published as the last applied seqno + 1, so a write at seqno `s` is
  visible at read seqno `s + 1`. Enforced in the read path (`src/tree`, `src/mvcc_stream.rs`) and the
  seqno ordering (`src/seqno.rs`, `src/value.rs`).

- **Re-applying a put / delete at its original seqno is idempotent; a merge
  operand is NOT.** For a put or delete, the same (key, value, seqno) reproduces
  the same MVCC version (an overwrite), which is what makes external-WAL replay
  safe. A merge operand re-applied on top of its already-persisted self is folded
  a second time by merge resolution (see [Merge operators](#merge-operators)
  below), so a counter would double-count. External-WAL replay must therefore
  apply each record exactly once, strictly above the durable watermark, never
  relying on over-replay being harmless (see [external-wal.md](external-wal.md)).

## Range tombstones

- **A range tombstone covers the half-open interval it encodes, at its seqno.** A
  point version is shadowed by a covering range tombstone only when the tombstone's
  seqno is newer than the point's; ordering vs point writes is by seqno, same as
  point deletes. Enforced in `src/range_tombstone.rs` /
  `src/range_tombstone_filter.rs` and the range-delete tests.

## Merge operators

- **A merge operand is resolved by folding it over the base value and older
  operands in seqno order.** A read (or compaction) materializes a key by applying
  operands oldest-to-newest on top of the base value; a partial fold during
  compaction must yield the same result as a full fold at read time, so the
  operator is required to be associative. A non-associative operator would make
  the answer depend on compaction timing. Enforced in `src/merge.rs` /
  `src/merge_source.rs` and the merge-operator tests.

## Recovery / ECC

- **ECC verification is three-state: OK, FAIL, or WARN.** A recognized scheme that
  verifies is OK; a recognized scheme that fails to correct is FAIL; an
  unrecognized scheme is WARN (soft-fail, recompaction re-stamps) rather than a
  hard reject. Enforced in `src/verify.rs` / `src/ecc.rs` / `src/secded.rs`.

- **On-read ECC correction is bounded by the scheme.** SEC-DED corrects a single
  bit per word and detects a double-bit error; Reed-Solomon recovers up to its
  parity-shard budget. A fault beyond the bound surfaces as a read error, never
  silent wrong data. Enforced in `src/secded.rs`, `src/ecc.rs`, and the ECC
  recovery counters (`src/metrics.rs`).

- **ENOSPC during a write leaves an orphan, not corruption.** A write that runs
  out of space fails before publishing a new version; the partial file is an
  orphan reclaimed on recovery, and the last durable version is unchanged.
  Enforced in the write / flush path and recovery (`src/tree`, `src/version/recovery.rs`).

## Encryption at rest

> `encryption` feature.

- **An encrypted block authenticates against its bound AAD.** The AEAD seal binds
  the block's table id and on-disk header fields as additional authenticated data
  and protects ciphertext integrity: any tampered byte, or a block substituted from
  a different table, fails to decrypt rather than yielding altered plaintext.
  (Same-table relocation is outside the AAD scope by design; cross-tree isolation
  is provided by key separation, not AAD.) Enforced in `src/encryption/aad.rs`
  (wire spec [docs/aad-block-format.md](aad-block-format.md) §5.3) and the
  encryption round-trip / threat-model tests.

## Key-value separation (BlobTree)

- **Every live blob pointer resolves, and GC drops only unreferenced blobs.** With
  KV separation a large value is written to a blob file and the SST holds an
  indirection pointer; a read transparently resolves the pointer to the user value,
  and blob GC reclaims a blob only once no live SST entry references it (tracked via
  the fragmentation map). A dangling pointer or a prematurely collected blob would
  lose the value. Enforced in `src/blob_tree` / `src/vlog` (GC in
  `src/blob_tree/gc.rs`).

## Columnar (PAX) / delete-bitmap

> `columnar` feature.

- **A columnar row's visibility follows the same seqno rule as a row-oriented
  entry.** Decoding a PAX row group yields the key / seqno / value-type / value
  sub-columns; a read at read-seqno `R` sees a row only if its seqno `< R` (the
  same exclusive upper bound as the row-oriented path; see
  [Snapshot / sequence number](#snapshot--sequence-number)). Enforced in
  `src/table/columnar.rs`.

- **The positional delete bitmap is a pure membership set; MVCC correctness is
  established when it is built, not at read time.** The bitmap carries no per-row
  seqno: a set bit unconditionally hides the row at that position for every reader
  of the segment. The MVCC reconciliation happens at materialization time: a row's
  bit is set only once its deleting tombstone is visible to every live snapshot
  (its seqno below the compaction threshold), so a still-visible older version is
  never masked. Enforced in `src/table/delete_bitmap.rs`.

- **Sub-column framing is self-describing and bounds-checked.** Each sub-column's
  offset / length is validated against the block before decode; a truncated or
  inconsistent frame is rejected, never read past its bound. Enforced in
  `src/table/columnar.rs` (decode + the columnar round-trip tests).
