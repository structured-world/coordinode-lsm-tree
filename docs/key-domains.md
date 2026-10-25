# Key domains

A key domain is a caller-defined interval of the key space whose rows share a
lifetime, such as one retention epoch of time-bucketed keys. Under a
`Config::key_domains` policy no table holding rows, and no blob file, holds
keys of two domains, so expiring a domain removes its files whole instead of
rewriting the survivors that would otherwise share them.

```rust
use lsm_tree::{Config, KeyDomains, SequenceNumberCounter, UserKey};
use std::sync::Arc;

/// One domain per epoch: the key's first four bytes, big-endian. Every key of
/// this tree starts with its epoch; a shorter one would get `None`, a domain
/// running to the end of the key space.
struct Epochs;

impl KeyDomains for Epochs {
    fn domain_end(&self, key: &[u8]) -> Option<UserKey> {
        let epoch = u32::from_be_bytes(key.get(..4)?.try_into().ok()?);
        Some(UserKey::from(epoch.checked_add(1)?.to_be_bytes().as_slice()))
    }
}

let tree = Config::new(&folder, SequenceNumberCounter::default(), SequenceNumberCounter::default())
    .key_domains(Arc::new(Epochs))
    .open()?;
```

## The policy

`KeyDomains::domain_end(key)` returns the first key past the domain holding
`key`, in the tree's comparator order, or `None` when that domain runs to the
end of the key space. A domain is an interval: every key from `key` up to the
returned one lies in `key`'s domain, and the answer is the same for any key of
one domain. Nothing about the policy is written to disk: a table holds one
domain when its key range does, so tables written before the policy, or under
another one, are judged by the same test, and the policy may be added or
replaced on reopen.

## What the engine guarantees

- **Tables.** Flush, ingest and compaction end an output before the first key of
  the next domain, even below the target size; a domain larger than the target
  still splits by size inside it. A columnar batch whose keys reach past a
  domain's end is written as one block per domain.
- **Range tombstones.** An output's share of the range tombstones stops at its
  domain's end. The pieces beyond, from that end to the next output's first key
  and, for a flush, below its first output and past its last output's domain,
  go to tables of tombstones alone. Such a table may span domains with no row in
  it.
- **Blob files.** A key-value separated tree closes every blob file it is
  filling, including those a lifetime class switch keeps open, before a value of
  the next domain.
- **The one edge.** A key range is recorded with an inclusive largest key. A
  table whose range tombstone reaches its domain's end records that end, the
  next domain's first key, as its largest key; a range tombstone over the domain
  then does not span it, and an ordinary compaction reclaims it instead.

## Expiring a domain

Delete the domain with one range tombstone over `[start, domain_end)`. Once it
is flushed and below the compaction watermark, the next compaction unlinks
every table it spans whole and is newer than; with the policy in place that is
every table of the domain, with no rewrite. The domain's blob files hold no
value of another domain, so once those tables are gone nothing references them.

Late rows of a domain that already expired are new rows: they land in tables of
that domain like any other write, after the tombstone, and read back.

## One tree with domains, or a tree per domain

Separate trees, one per domain, give the same layout without a policy. The
`key_domains_retention` bench writes the same rolling-retention workload three
ways: one tree without domains, one tree with them, and a tree per epoch.
Every epoch writes its rows in time order across 64 series; 1% of them arrive
during the next epoch; an epoch older than the retention expires through a
range tombstone (one tree) or by removing its tree. Both shapes write 102.5 MiB
of payload with 4 MiB memtables. Median of three rounds on ext4; bytes, space
and table counts are the same in every round. The peak is the largest space
taken once an epoch settles; a compaction running in between may hold more
for a moment.

| shape | layout | written MiB | write amp. | epoch peak / end MiB | tables | descriptors | reopen ms |
|---|---|---|---|---|---|---|---|
| 48 epochs, 12 retained | one tree | 516.2 | 5.03 | 41.3 / 36.9 | 3 | 1 | 9.3 |
| | one tree, domains | 314.9 | 3.07 | 28.6 / 28.4 | 20 | 1 | 6.5 |
| | tree per epoch | 106.1 | 1.03 | 28.6 / 28.6 | 13 | 13 | 58.3 |
| 8 epochs, 2 retained | one tree | 464.8 | 4.53 | 52.2 / 52.2 | 5 | 1 | 8.1 |
| | one tree, domains | 404.7 | 3.95 | 39.1 / 39.1 | 10 | 1 | 9.7 |
| | tree per epoch | 364.4 | 3.55 | 39.4 / 39.4 | 12 | 3 | 39.6 |

- Domains bring one tree to the space a tree per epoch takes, since an expired
  epoch's tables go whole: 23% and 25% less space at the end than one tree
  without them, 31% and 25% less at the epoch peak.
- They write 39% and 13% less than one tree without them.
- A tree per epoch writes less still where an epoch is smaller than a memtable:
  its epoch flushes once, while one shared memtable flushes the epoch in pieces
  that later merge. With epochs several memtables large the gap is 10%.
- It pays a descriptor and a manifest per retained epoch, and reopens 4 to 9
  times slower.

On btrfs the space and the table counts are the same and the layouts rank the
same; the bytes written are 3% to 18% higher, the filesystem's own metadata
writes counted with them.

Choose a tree per domain when domains are few and smaller than a memtable and
write volume matters most; choose domains in one tree when domains are many,
or a reader needs one consistent view across them without opening a tree per
domain.
