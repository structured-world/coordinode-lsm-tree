# External Write-Ahead Log Integration

This engine has **no internal write-ahead log**: a write lands in the active
memtable and becomes durable only when that memtable is flushed to an SST. A
crash between a write and the next flush loses every unflushed write. Durability
is therefore the caller's responsibility — if you need it, you log each write to
your own WAL before applying it and replay the tail on restart.

This document specifies the contract for building that external WAL on top of the
existing public API. No engine callbacks are required (see
[Why no hook API](#why-no-hook-api)); the contract is expressed entirely through
`insert`, `flush_active_memtable`, `get_highest_persisted_seqno`, and
recover-on-open.

## The sequence number is the durability cursor

Every write carries a caller-supplied sequence number:

```rust,ignore
fn insert<K: Into<UserKey>, V: Into<UserValue>>(&self, key: K, value: V, seqno: SeqNo) -> (u64, u64);
```

The engine does not assign seqnos — the caller does, typically by drawing
monotonically increasing values from a [`SequenceNumberCounter`]. Because the
seqno is an input, it is the single cursor that ties your WAL records to engine
state: a WAL record and the `insert` it produces share one seqno, and recovery is
expressed as "replay every WAL record with a seqno above what the engine already
persisted".

MVCC visibility follows the seqno: a read at snapshot `N` sees the newest version
of each key with `seqno <= N` (see [INVARIANTS.md](INVARIANTS.md), Snapshot /
seqno). Re-applying a put or delete at its original seqno reproduces the same
version (an overwrite); a merge operand is the exception — re-applying folds it
twice — so replay must apply each record exactly once (see
[Recovery replay](#3-recovery-replay)).

## 1. Log before apply

For each write (or batch):

1. Draw the seqno(s) the write will carry (`SequenceNumberCounter::next`, or your
   own monotonic source).
2. Append the record — keys, values, and the seqno — to your WAL and make it
   durable (`fsync`, or your log's equivalent).
3. Only then call `insert(key, value, seqno)` (or a `WriteBatch` at that seqno).

The ordering is what guarantees recoverability: if the process dies after step 2
but before or during step 3, the record is in your WAL and replay re-applies it.
If it dies before step 2, the write never happened from the caller's point of
view, so there is nothing to lose. Never apply before logging — a write that
reaches the memtable but not the WAL is unrecoverable after a crash that drops the
memtable.

## 2. Durability points — when a seqno is safe to trim

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
tree) — the *maximum*, not a contiguity guarantee. When writes are applied in
strict seqno order (the single-writer log-before-apply pattern above guarantees
this), that maximum *is* a contiguous prefix: **every WAL record with
`seqno <= get_highest_persisted_seqno()` is on disk and may be trimmed**. If you
apply writes out of seqno order (concurrent appliers can flush a higher seqno
while a lower one is still pending), the maximum is NOT contiguous — a delayed
lower seqno may be absent from every SST — so trim only against a contiguous
applied-and-persisted prefix you track yourself, never against the raw maximum.

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

1. Read the durable watermark: `let durable = tree.get_highest_persisted_seqno();`
   (`None` ⇒ empty tree ⇒ replay everything).
2. Replay every WAL record with **`seqno > durable`** (a strict lower bound — phrase
   it as `> durable`, not a literal `durable + 1`, which would overflow at the top
   of the seqno range), re-applying each with its **original operation** and seqno:
   a logged put via `insert`, a delete via `remove`, a merge via `merge`. Never
   collapse every record to `insert` — that loses deletes and merge semantics.
3. Do NOT re-apply records at or below `durable`. For put / delete that would be
   harmless (re-applying at the original seqno reproduces the same MVCC version, an
   overwrite), but a **merge operand** re-applied on top of its already-persisted
   self is folded twice by merge resolution — a counter would double-count. The
   strict `> durable` boundary is correct for every record type, so use it
   unconditionally rather than relying on over-replay being idempotent.

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
expressed through this surface, a hook trait can be added then — document-first
until proven necessary.

[`SequenceNumberCounter`]: https://docs.rs/coordinode-lsm-tree/latest/lsm_tree/struct.SequenceNumberCounter.html
