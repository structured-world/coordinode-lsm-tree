# Compression

## The btultra2 two-pass seed at levels 19-22

zstd levels 19 to 22 select the `btultra2` strategy, which walks a block once
to seed its statistics before the pass that actually emits. The seed roughly
doubles the time it takes to compress a block.

It is switchable through `RuntimeConfig::zstd_two_pass_seed`, and it is **on by
default**, which is the codec's own behaviour at those levels.

### Why on by default

Level 22 is not a level anyone reaches by accident. It is chosen when ratio is
worth paying for in time, and that is the only reason to pay its cost at all.
Turning the seed off by default would quietly take back part of what that
choice bought, for every deployment already running at 19 or above, without
anyone asking for it. A deployment that would rather have the write speed can
say so; one that asked for maximum ratio keeps getting it.

The reverse default would also be silent in the worst way: the output stays a
valid frame and decodes identically, only slightly larger. Nothing fails, no
counter moves, and the loss is invisible until someone measures bytes on disk.

### What it costs and what it buys

Measured on 4 KiB blocks at level 22 on a reused compressor (one per thread,
many blocks, as the engine drives it), default `btultra2` against an explicit
single-pass `btultra`:

| block shape | bytes with seed | bytes without | time with seed | time without |
|---|---|---|---|---|
| records of a counter key and a constant value | 97 | 97 (same) | 3158 us | 1306 us |
| text | 1579 | 1585 (+0.4%) | 718 us | 358 us |
| keys with random suffixes, small-alphabet values | 1416 | 1428 (+0.8%) | 756 us | 392 us |
| incompressible | 4106 | 4106 (same) | 8 us | 8 us |

So the seed buys nothing on uniform or incompressible blocks, and a fraction of
a percent on blocks with structure to find, which is what most real data looks
like. It costs close to 2x the write time wherever it runs.

### When to turn it off

Turn it off when write throughput at levels 19-22 matters more than a fraction
of a percent of ratio: a bulk load, a backfill, a tier whose contents are
rewritten often enough that the extra bytes never accumulate. Leave it on for a
cold tier written once and kept, which is the case level 22 exists for.

Below level 19 the setting does nothing at all: those levels select another
strategy, which has no seed pass to skip.

### What the setting does not affect

Nothing about a block records which setting wrote it. The seed changes how the
encoder searches, never what the frame means, so:

- blocks written under either setting decode identically, with no reader
  involvement and no format version;
- a tree can hold blocks written under both, which is what happens whenever the
  setting is changed on a live tree;
- changing it needs no migration. Existing blocks stay as they are and are
  rewritten under the current setting whenever a compaction next touches them.

It applies to every path that compresses: flush, compaction, ingestion, blob
writes, and the index and filter blocks, which carry their own compression
policy and so run their own encoders.
