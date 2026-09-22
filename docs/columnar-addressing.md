# Columnar addressing

What identifies a row inside a columnar segment, and which of those
identifiers the 6.0 page format is allowed to change.

This document exists because four pieces of work depend on the same answer and
cannot each invent their own: independently readable column pages, physical
type descriptors on fixed-width columns, field-level blob references, and the
offline 5.x conversion. The conversion is where a disagreement between them
would surface, and by then the wire format is fixed. So the addressing is
settled first, here.

## The coordinates that exist today

Four distinct things address a row, and they are not the same coordinate.

| Coordinate | Where it is produced | Where it is consumed |
|---|---|---|
| `block_id` | the data block's 0-based ordinal **in key order**, which the SST index yields in iteration order | the retrieval locator packs `(block_id, slot)` into one BuRR value (`src/table/locator.rs`) |
| `slot` | a row's position **within its block** | the same locator value; `slot_bits = 0` degrades it to block-only precision |
| global row position | the prefix sum of `row_count` over the blocks before this one | the positional delete bitmap (`src/table/delete_bitmap.rs`), which is a set of positions in the whole segment |
| local seqno | the seqno column stores a value **relative to the table's global seqno**, globalized after the block is read | `src/tree/columnar_scan.rs` |

The last two are the ones that bite.

**The global row position is currently derived from a physical offset.**
`Table::delete_block_starts` maps a data block's **file offset** to its
starting row position, and the delete filter reads it as
`starts.get(&handle.offset().0)` (`src/table/mod.rs`). The position space is
logical, but the key into it is physical. Any format change that moves a row
group's bytes, or splits them across several physical extents, breaks that
lookup even though every row kept its position.

**The local seqno is a second coordinate on the same column.** A predicate
pushed down at the SST level sees the local value; the caller's snapshot is
global. Today the mismatch is unreachable because a fixed-width column is
inert to both the zone map and `matching_rows` — and the read path says so in
a comment that names this document's subject as the thing making it reachable.
Physical type descriptors remove exactly that inertness.

## The rule

**A row group is the unit of logical addressing. A page is a physical
subdivision of a row group and is invisible to addressing.**

Everything else follows from it:

1. **`block_id` identifies a row group, not a physical block.** It stays the
   0-based ordinal in key order. Where the two coincide today they may stop
   coinciding; the locator's meaning does not change, because it never named
   bytes.
2. **`slot` stays a row's ordinal within its row group**, counted in key
   order, unaffected by which page holds which of that row's columns.
3. **The global row position is the prefix sum of `row_count` over the row
   groups before this one, in key order.** It is defined by the logical
   sequence and by nothing physical. A reader may still build a lookup keyed
   by an offset, but that is a cache of the definition, never the definition —
   and the page format must let the definition be evaluated without it.
4. **A row group's boundaries are part of its identity.** Two segments with
   the same rows split into different row groups are different segments as far
   as every coordinate above is concerned.
5. **The seqno column stays local to its table**, and a predicate on it is
   evaluated in the coordinate the column is stored in, or translated
   explicitly before evaluation. "Evaluated in whichever coordinate happened
   to be at hand" is the defect this rule exists to forbid.

## What the page format must therefore express

- A row group's boundaries and its ordinal, independently of where its bytes
  sit.
- A row's ordinal within its row group, without decoding columns the reader
  did not ask for. (A projection that had to decode the key column to count
  rows would make every projection pay for the keys.)
- The identity of each independently readable page: which table, which row
  group, which page slot. This extends the identity the AAD already binds
  (see `aad-block-format.md`); it may not weaken it. A page moved between
  tables, between row groups or between slots has to fail verification, the
  way a misdirected block does today.
- The mapping from a global row position to a row group and slot, evaluable
  from the directory alone.

The five granularities the format separates (SST, statistics zone, compression
unit, I/O buffer, read page) are **physical** parameters. None of them is an
addressing unit, and none may become one: a statistics zone that also defined
row identity would make pruning granularity a compatibility constraint, which
is precisely the coupling the separation is for.

## What this means for the conversion

The offline converter preserves the logical geometry and changes only the
physical placement: row groups keep their boundaries, rows keep their
ordinals, `block_id` keeps its meaning. The locator is repacked to address the
same logical units; the delete bitmap's positions are unchanged **because the
positions are unchanged**, though the lookup that finds them is rebuilt, since
it was keyed by a file offset that no longer exists.

A restriction bound is a KEY, not a position, so it converts unchanged. The
hole punch underneath it does not: it names a byte prefix of a file the
conversion does not produce. A converted table is written whole, and its
prefix is reclaimed again by a later tight-space compaction or not at all.

If the page format turns out not to express the geometry above, that is a
finding about the format and it goes back to the page work before the wire
format is fixed. It is never a licence for the converter to renumber: a
migration that moved row ordinals would have to translate every dependent
reference through an explicit mapping, multiplying the invariants in flight
for no benefit that the next compaction does not deliver anyway.

## What this means for field-level blob references

A blob reference stored in a column is addressed like any other cell: row
group, slot, column. The reference's own identity — what the GC accounts for —
is a separate question from the row's, and deliberately so. A reference may
outlive the row that first wrote it, because a later version can keep it while
changing only the compact fields; that is the point of the feature. So:

- a reference is **not** identified by the row that holds it, and dropping a
  row is not a statement about the object;
- the row's coordinates above say where a reference is stored, never whether
  the object it names is reachable.

The reachability rule belongs with the reference lifecycle, not here. What
belongs here is the boundary: row addressing does not answer object
reachability, and any design that made it answer both would reintroduce the
identity the shared reference exists to break.
