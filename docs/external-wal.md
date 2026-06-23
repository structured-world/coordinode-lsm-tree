# External Write-Ahead Log Integration

This engine has **no internal write-ahead log**: a write lands in the active
memtable and becomes durable only when that memtable is flushed to an SST. A
crash between a write and the next flush loses every unflushed write. Durability
is therefore the caller's responsibility: if you need it, you log each write to
your own WAL before applying it and replay the tail on restart.

This document specifies the contract for building that external WAL on top of the
existing public API. No engine callbacks are required (see
[Why no hook API](#why-no-hook-api)); the contract is expressed entirely through
the write methods (`insert`, `remove`, `remove_weak`, `remove_range`, `merge`, and
`WriteBatch`), `flush_active_memtable`, `get_highest_persisted_seqno`, and
recover-on-open.

## The sequence number is the durability cursor

Every write carries a caller-supplied sequence number:

```rust,ignore
fn insert<K: Into<UserKey>, V: Into<UserValue>>(&self, key: K, value: V, seqno: SeqNo) -> (u64, u64);
```

The engine does not assign seqnos; the caller does, typically by drawing
monotonically increasing values from a [`SequenceNumberCounter`]. Because the
seqno is an input, it is the single cursor that ties your WAL records to engine
state: a WAL record and the write it produces share one seqno, and recovery is
expressed as "replay every WAL record with a seqno above your trim watermark `W`"
(the gap-free applied-and-persisted prefix defined in section 3, not the raw
persisted maximum).

MVCC visibility follows the seqno: a read at read-seqno `R` sees the newest version
of each key with `seqno < R`: the read seqno is an *exclusive* upper bound. The
visible watermark is published as the last applied seqno + 1, so a write at seqno
`s` becomes visible once the watermark reaches `s + 1` (see
[INVARIANTS.md](INVARIANTS.md), Snapshot / seqno). Re-applying a put or delete at its original seqno reproduces the same
version (an overwrite); a merge operand is the exception (re-applying folds it
twice), so replay must apply each record exactly once (see
[Recovery replay](#3-recovery-replay)).

## 1. Log before apply

For each write (or batch):

1. Draw the seqno(s) the write will carry (`SequenceNumberCounter::next`, or your
   own monotonic source).
2. Append the record (keys, values, and the seqno) to your WAL and make it
   durable (`fsync`, or your log's equivalent).
3. Only then call the original write API at that seqno (`insert` for a put,
   `remove` for a point delete, `remove_weak` for a weak/single delete,
   `remove_range` for a range tombstone, `merge` for a merge operand, or a
   `WriteBatch`). Apply the same operation that was logged, not always `insert`.

The ordering is what guarantees recoverability: if the process dies after step 2
but before or during step 3, the record is in your WAL and replay re-applies it.
If it dies before step 2, the write never happened from the caller's point of
view, so there is nothing to lose. Never apply before logging: a write that
reaches the memtable but not the WAL is unrecoverable after a crash that drops the
memtable.

## 2. Durability points: when a seqno is safe to trim

A write is durable once the memtable holding it has been flushed:

```rust,ignore
fn flush_active_memtable(&self, eviction_seqno: SeqNo) -> crate::Result<()>;
```

When this returns `Ok`, the active memtable has been written and synced as an SST,
so every seqno it contained is now on disk and survives a crash. To learn the
watermark, query:

```rust,ignore
fn get_highest_persisted_seqno(&self) -> Option<SeqNo>;
```

This returns the highest seqno present in the persisted SSTs (`None` for an empty
tree): the *maximum*, not a contiguity guarantee. A record is trimmable only once
it has both been **applied** (its `insert` / `remove` / `merge` / ... returned)
AND **persisted**. A record fsynced to your WAL but not yet applied (a crash
between the log write and the apply) is absent from every SST and must stay in the
WAL for replay, even if a later seqno was applied and flushed past it. So trim only
a gap-free prefix of applied-and-persisted records: when you apply in strict seqno
order and every record up to some seqno has been applied,
`get_highest_persisted_seqno()` is that contiguous watermark and records with
`seqno <= it` may be trimmed. If applies can be reordered or skipped (concurrent
appliers, or a failed apply that leaves a gap), the maximum is NOT contiguous, so
track the applied-and-persisted prefix yourself and never trim against the raw
maximum.

`create_checkpoint` gives the same guarantee for a point-in-time copy: it flushes
the active memtable first, then hard-links every resulting SST into the checkpoint
directory, so the checkpoint contains every write that had reached the active
memtable at the call (the persisted watermark advances to cover the flushed
writes).

Note `get_highest_persisted_seqno` is the *persisted* watermark, distinct from
`get_highest_seqno` (the max over memtable + SSTs, including not-yet-durable
writes). Trim against the persisted one only.

## 3. Recovery replay

On `Config::open` the engine recovers its state from the persisted SSTs alone (it
has no log of its own to replay). After open:

1. Recover from your **trim watermark `W`**, not the raw persisted maximum. `W` is
   the gap-free applied-and-persisted prefix you trimmed to (section 2); replay
   every WAL record that survived the trim, i.e. `seqno > W`. With strict gap-free
   in-order apply `W == get_highest_persisted_seqno()`, but if you retained a lower
   record across a gap (a logged-but-unapplied seqno below a flushed higher one) it
   is still in the WAL and MUST be replayed, so never use the raw maximum as the
   boundary, which would skip it. (Phrase the bound as `> W`, not a literal
   `W + 1`, which would overflow at the top of the seqno range.)
2. Replay each surviving record with its **original operation** and seqno: the
   same call it was logged for (`insert` for a put, `remove` for a point delete,
   `remove_weak` for a weak/single delete, `remove_range` for a range tombstone,
   `merge` for a merge operand, or the original `WriteBatch` for a batch). Never
   collapse every record to `insert`, which loses deletes, range tombstones, and
   merge semantics.
3. Do NOT re-apply records at or below `W`. For put / delete that would be harmless
   (re-applying at the original seqno reproduces the same MVCC version, an
   overwrite), but a **merge operand** re-applied on top of its already-persisted
   self is folded twice by merge resolution, so a counter would double-count. The
   strict `> W` boundary is correct for every record type, so use it
   unconditionally rather than relying on over-replay being idempotent. For
   merge-bearing workloads, apply gap-free so that `W` equals the persisted maximum
   and no already-persisted operand can sit above `W` to be replayed.

The strict boundary still covers the crash window in step 1 of
[Log before apply](#1-log-before-apply): a record that was logged and applied but
not yet flushed is, by definition, absent from the SSTs, so its seqno is above
`durable` and step 2 replays it exactly once.

## Why no hook API

A thin observability hook surface (`before_write_batch`, `after_flush`,
`after_checkpoint`) was considered and is **not** provided: the existing API
already expresses the full contract. The seqno is a caller input, so the caller
already knows every seqno it applied without a callback; `flush_active_memtable` /
`create_checkpoint` return `Ok` exactly when their durability guarantee holds; and
`get_highest_persisted_seqno` reports the watermark to trim against. Adding
callbacks would duplicate information the caller already has and couple the engine
to a notification lifecycle it does not need. If a future requirement cannot be
expressed through this surface, a hook trait can be added then; document-first
until proven necessary.

[`SequenceNumberCounter`]: https://docs.rs/coordinode-lsm-tree/latest/lsm_tree/struct.SequenceNumberCounter.html
