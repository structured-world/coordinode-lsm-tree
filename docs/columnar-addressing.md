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

## The geometry the separation requires

Pages only pay if a page is worth reading on its own, and today's row group is
far too small for that. A columnar row group is flushed at the data-block size
target, 4 KiB by default (`src/tree/ingest.rs` compares `data_size()` against
`data_block_size_policy`). The formats this design draws on are three orders
of magnitude coarser: FastLanes' row group is a fixed multiple of 1024 rows,
64 × 1024 in its defaults, and Vortex chunks a column at 2 MB of uncompressed
data while pruning at 8k rows.

What 4 KiB buys, for the record shapes the mixed-layout benchmark measures:

| Shape | Bytes per row | Rows in a 4 KiB row group | A one-field page over those rows |
|---|---|---|---|
| narrow | 61 | 67 | 536 B |
| 256-byte values | 285 | 14 | 112 B |
| 4 KiB payload | 4157 | 1 | 8 B |

A 536-byte page, or a 112-byte one, is below the granularity any device will
serve: the read still costs a sector. Splitting a 4 KiB row group into pages
therefore saves decode work — which `decode_projected` already saves — and
saves nothing on the read the issue is about. **An eight-byte-per-row page
needs 512 rows to reach 4 KiB at all**, which puts the row group at 30 KiB for
narrow records and 142 KiB for 256-byte ones.

So independently readable pages and a larger row group are one change, not
two, and they are each other's precondition:

- pages are pointless while the row group is 4 KiB, because the whole group is
  one device read;
- a row group of hundreds of kilobytes is unaffordable **unless** pages are
  independently readable, because a point read would otherwise fetch the whole
  group to return one row.

That mutual dependence is also what keeps the change honest. A row group grows
exactly as far as the page it makes readable is worth reading, and no further;
the sizing is a measurement against the read path, not a number adopted from a
format whose unit is written once per dataset rather than once per flush.

Two consequences for the format follow directly:

- **Integrity has to be per page, not per row group.** At 4 KiB a single
  checksum or authentication tag over the whole block costs nothing extra to
  verify. At 256 KiB it forces the whole group to be read to verify anything
  inside it, which is precisely the ceiling the pages exist to remove.
- **A point read stops being a whole-group read.** It fetches the page holding
  keys and the pages holding the fields it was asked for. The key page is on
  the path of every read, which is the first reason the grouping puts keys and
  MVCC metadata in a page of their own rather than beside the payload.

## Where the page directory lives, and how many pages there are

The reference formats both keep their page directory in the file footer.
Vortex registers every segment in a footer table as `SegmentSpec { offset:
u64, length: u32, alignment_exponent: u8 }`, and its layout tree references
those by index. FastLanes puts a descriptor per row group in the footer, each
carrying a descriptor per column, each carrying a descriptor per segment
(entry-point offset and size, data offset and size).

**Their row group is large enough for that to be cheap, and ours is not.**
FastLanes' Figure 7 shows one string column encoded as `DICT_FFOR_UINT8`
occupying **five** segments (dictionary bytes, dictionary offsets, bit-packed
codes, bases, bit widths), a constant column occupying **zero** (its value
lives in the metadata), and a dictionary-only column occupying **one**. So the
page count per row group is not a small constant chosen by us — it is however
many parts the chosen encoding expressions name, and it is data-dependent.

At a 16-byte descriptor, a 64 MiB SST works out as:

| Row group | Groups per SST | 8 pages each | 20 pages each |
|---|---|---|---|
| 32 KiB | 2048 | 256 KiB of directory | 640 KiB |
| 128 KiB | 512 | 64 KiB | 160 KiB |
| 256 KiB | 256 | 32 KiB | 80 KiB |

A footer is read whole when the table opens. Several hundred kilobytes of it
per SST, across every open table, is a different cost from the one the
reference formats pay — theirs is a file opened once per query against a row
group of 64 × 1024 rows.

**So the directory is per row group, not per table**, sitting with the group
it describes and fetched with it, and the table-level footer gains nothing per
page. This also keeps the directory's lifetime right: it is the thing a read
consults after the index has already chosen a row group, so it is on the same
path as the group's own bytes, not on the open path of every table.

## What the neighbouring work requires of a page

The page boundary is not free to choose, because four other pieces of work
address it:

- **Light column codecs.** The encoding is an expression over operators and
  each operator names the parts of its encoded form. Those parts are what a
  page holds: a page is one part of one encoding for a run of rows, which is
  what makes "all the bases together, all the bit-packed bodies together"
  expressible at all. The page count therefore follows the expressions, as
  above.
- **Column-native compaction.** What may be carried across a compaction
  without re-encoding is "a self-contained decodable unit with its
  dependencies preserved" — exactly the parts an expression names, carried
  together. A page that is not self-contained cannot be copied, so the
  boundary has to fall where a decodable unit ends, not where a convenient
  byte count does.
- **Selective late materialization.** The reader groups outstanding fetches
  **by page**, and the guarantee is stated per page rather than per row: a
  page holding one surviving row is still read whole. It also asks for useful
  and incidentally-read bytes to be reported separately, which only means
  something if a page is small enough for the distinction to exist.
- **Resumable reads.** A suspension point carries a set of outstanding
  requests, and a page is the natural request. A page addressed as a byte
  range needs the request type to express a range rather than a block handle.

There is a tension here worth stating rather than discovering later.
**Per-page identity and cross-compaction copyability pull against each
other.** The tighter a page's authentication binds it to its logical position
(which table, which row group, which slot), the less of it can be carried
across a compaction that changes any of those. That is already true today:
`relocate_columnar_with_deletes` reuses blocks verbatim and carries
restrictions around encryption and ECC for exactly this reason. The format
does not get to have both without a decision about which binding is worth its
cost, and that decision belongs with the page identity scheme, measured — not
assumed in either direction.

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
