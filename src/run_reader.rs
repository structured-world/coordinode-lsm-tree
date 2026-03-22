// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{version::Run, BoxedIterator, InternalValue, Table, UserKey};
use std::{
    ops::{Bound, Deref, RangeBounds},
    sync::Arc,
};

type OwnedRange = (Bound<UserKey>, Bound<UserKey>);

fn to_owned_range<R: RangeBounds<UserKey>>(range: &R) -> OwnedRange {
    (
        match range.start_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        },
        match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        },
    )
}

/// Optional bloom filter hints for lazy per-table skipping inside [`RunReader`].
///
/// When set, the reader checks each table's bloom filter before creating
/// an iterator for it. Tables whose filter reports definite absence are
/// skipped without building a table iterator or reading their data blocks,
/// turning the cost from O(N) upfront into O(visited) lazily checked tables.
#[derive(Clone, Default)]
pub struct BloomHints {
    pub prefix_hash: Option<u64>,
    pub key_hash: Option<u64>,

    #[cfg(feature = "metrics")]
    pub metrics: Option<Arc<crate::Metrics>>,
}

impl BloomHints {
    /// Returns `true` if the table should be skipped based on bloom filters.
    ///
    /// Checks prefix bloom first, then key bloom. Returns `true` (skip) only
    /// when a filter definitively reports absence. Filter I/O errors and
    /// conservative "maybe" results both yield `false` (don't skip).
    pub fn should_skip(&self, table: &Table) -> bool {
        if let Some(prefix_hash) = self.prefix_hash {
            match table.maybe_contains_prefix(prefix_hash) {
                Ok(false) => {
                    #[cfg(feature = "metrics")]
                    if let Some(m) = &self.metrics {
                        m.prefix_bloom_skips
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    return true;
                }
                Err(e) => {
                    log::debug!("prefix bloom check failed for table {:?}: {e}", table.id(),);
                }
                _ => {}
            }
        }

        if let Some(key_hash) = self.key_hash {
            match table.bloom_may_contain_key_hash(key_hash) {
                Ok(false) => return true,
                Err(e) => {
                    log::debug!("key bloom check failed for table {:?}: {e}", table.id(),);
                }
                _ => {}
            }
        }

        false
    }
}

/// Reads through a disjoint run with lazy reader initialization.
///
/// `lo_reader` and `hi_reader` are constructed on first `next()` /
/// `next_back()` respectively, deferring the `table.range()` seek.
/// When [`BloomHints`] are attached, boundary tables are bloom-checked
/// during lazy init (skipping reader creation entirely if rejected),
/// and intermediate tables are checked lazily when the iterator
/// advances to them.
pub struct RunReader {
    run: Arc<Run<Table>>,
    range: OwnedRange,
    lo: usize,
    hi: usize,
    lo_reader: Option<BoxedIterator<'static>>,
    hi_reader: Option<BoxedIterator<'static>>,
    lo_initialized: bool,
    hi_initialized: bool,
    bloom: BloomHints,
}

impl RunReader {
    #[must_use]
    pub fn new<R: RangeBounds<UserKey> + Clone + Send + 'static>(
        run: Arc<Run<Table>>,
        range: R,
    ) -> Option<Self> {
        assert!(!run.is_empty(), "level reader cannot read empty level");

        let (lo, hi) = run.range_overlap_indexes(&range)?;

        Some(Self::culled(run, range, (Some(lo), Some(hi))))
    }

    #[must_use]
    pub fn culled<R: RangeBounds<UserKey> + Clone + Send + 'static>(
        run: Arc<Run<Table>>,
        range: R,
        (lo, hi): (Option<usize>, Option<usize>),
    ) -> Self {
        let lo = lo.unwrap_or_default();
        let hi = hi.unwrap_or(run.len() - 1);
        let owned_range = to_owned_range(&range);

        Self {
            run,
            range: owned_range,
            lo,
            hi,
            lo_reader: None,
            hi_reader: None,
            lo_initialized: false,
            hi_initialized: lo >= hi,
            bloom: BloomHints::default(),
        }
    }

    /// Attaches bloom filter hints for lazy per-table skipping.
    ///
    /// Boundary tables (lo/hi) are bloom-checked during lazy init in
    /// `ensure_lo/hi_initialized`. If rejected, initialization is marked
    /// done but no reader is created — the iteration loop skips them
    /// naturally. Intermediate tables are checked lazily during iteration.
    #[must_use]
    pub fn with_bloom_hints(mut self, bloom: BloomHints) -> Self {
        self.bloom = bloom;
        self
    }

    fn ensure_lo_initialized(&mut self) {
        if !self.lo_initialized {
            #[expect(
                clippy::expect_used,
                reason = "we trust the caller to pass valid indexes"
            )]
            let lo_table = self.run.deref().get(self.lo).expect("should exist");

            // Bloom-check the lo boundary table before creating the reader.
            // If rejected, mark as initialized but leave lo_reader as None.
            if !self.bloom.should_skip(lo_table) {
                self.lo_reader = Some(Box::new(lo_table.range(self.range.clone())));
            }
            self.lo_initialized = true;
        }
    }

    fn ensure_hi_initialized(&mut self) {
        if !self.hi_initialized {
            #[expect(
                clippy::expect_used,
                reason = "we trust the caller to pass valid indexes"
            )]
            let hi_table = self.run.deref().get(self.hi).expect("should exist");

            if !self.bloom.should_skip(hi_table) {
                self.hi_reader = Some(Box::new(hi_table.range(self.range.clone())));
            }
            self.hi_initialized = true;
        }
    }
}

impl Iterator for RunReader {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.ensure_lo_initialized();

        loop {
            if let Some(lo_reader) = &mut self.lo_reader {
                if let Some(item) = lo_reader.next() {
                    return Some(item);
                }

                // NOTE: Lo reader is empty, get next one
                self.lo_reader = None;
                self.lo += 1;

                // Strict `<`: when lo reaches hi, this branch is skipped and
                // the hi table is read via ensure_hi_initialized (which uses
                // table.range() to respect the range end bound). `.iter()` is
                // only used for middle tables that are fully consumed.
                // Bloom-rejected intermediate tables are skipped without I/O.
                while self.lo < self.hi {
                    #[expect(
                        clippy::expect_used,
                        reason = "hi is at most equal to the last slot; so because 0 <= lo < hi, it must be a valid index"
                    )]
                    let table = self.run.get(self.lo).expect("should exist");

                    if self.bloom.should_skip(table) {
                        self.lo += 1;
                        continue;
                    }

                    self.lo_reader = Some(Box::new(table.iter()));
                    break;
                }
            } else {
                // Lo exhausted — initialize hi reader if needed and consume from it
                self.ensure_hi_initialized();

                if let Some(hi_reader) = &mut self.hi_reader {
                    return hi_reader.next();
                }
                return None;
            }
        }
    }
}

impl DoubleEndedIterator for RunReader {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.ensure_hi_initialized();

        loop {
            if let Some(hi_reader) = &mut self.hi_reader {
                if let Some(item) = hi_reader.next_back() {
                    return Some(item);
                }

                // NOTE: Hi reader is empty, get prev one
                self.hi_reader = None;
                self.hi -= 1;

                while self.lo < self.hi {
                    #[expect(
                        clippy::expect_used,
                        reason = "because 0 <= lo <= hi, and hi monotonically decreases, hi must be a valid index"
                    )]
                    let table = self.run.get(self.hi).expect("should exist");

                    if self.bloom.should_skip(table) {
                        if self.hi == 0 {
                            break;
                        }
                        self.hi -= 1;
                        continue;
                    }

                    self.hi_reader = Some(Box::new(table.iter()));
                    break;
                }
            } else {
                // Hi exhausted — initialize lo reader if needed and consume from it
                self.ensure_lo_initialized();

                if let Some(lo_reader) = &mut self.lo_reader {
                    return lo_reader.next_back();
                }
                return None;
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{AbstractTree, SequenceNumberCounter, Slice};
    use test_log::test;

    #[test]
    fn run_reader_skip() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let tree = crate::Config::new(
            &tempdir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        let ids = [
            ["a", "b", "c"],
            ["d", "e", "f"],
            ["g", "h", "i"],
            ["j", "k", "l"],
        ];

        for batch in ids {
            for id in batch {
                tree.insert(id, vec![], 0);
            }
            tree.flush_active_memtable(0)?;
        }

        let tables = tree
            .current_version()
            .iter_tables()
            .cloned()
            .collect::<Vec<_>>();

        let level = Arc::new(Run::new(tables).unwrap());

        assert!(RunReader::new(level.clone(), UserKey::from("y")..=UserKey::from("z"),).is_none());

        assert!(RunReader::new(level, UserKey::from("y")..).is_none());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn run_reader_basic() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let tree = crate::Config::new(
            &tempdir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        let ids = [
            ["a", "b", "c"],
            ["d", "e", "f"],
            ["g", "h", "i"],
            ["j", "k", "l"],
        ];

        for batch in ids {
            for id in batch {
                tree.insert(id, vec![], 0);
            }
            tree.flush_active_memtable(0)?;
        }

        let tables = tree
            .current_version()
            .iter_tables()
            .cloned()
            .collect::<Vec<_>>();

        let level = Arc::new(Run::new(tables).unwrap());

        {
            let multi_reader = RunReader::culled(level.clone(), .., (Some(1), None));
            let mut iter = multi_reader.flatten();

            assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
            assert!(iter.next().is_none());
        }

        {
            let multi_reader = RunReader::new(level.clone(), ..).unwrap();

            let mut iter = multi_reader.flatten();

            assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
            assert!(iter.next().is_none());
        }

        {
            let multi_reader = RunReader::new(level.clone(), ..).unwrap();

            let mut iter = multi_reader.rev().flatten();

            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
            assert!(iter.next().is_none());
        }

        {
            let multi_reader = RunReader::new(level.clone(), ..).unwrap();

            let mut iter = multi_reader.flatten();

            assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"l"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next_back().unwrap().key.user_key);
            assert!(iter.next().is_none());
        }

        {
            let multi_reader = RunReader::new(level.clone(), UserKey::from("g")..).unwrap();

            let mut iter = multi_reader.flatten();

            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
            assert!(iter.next().is_none());
        }

        {
            let multi_reader = RunReader::new(level, UserKey::from("g")..).unwrap();

            let mut iter = multi_reader.flatten().rev();

            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert!(iter.next().is_none());
        }

        Ok(())
    }

    /// Creates a 4-table disjoint run for bloom skip tests.
    /// Tables contain: [a,b,c], [d,e,f], [g,h,i], [j,k,l]
    fn make_bloom_test_run() -> crate::Result<(Arc<Run<Table>>, tempfile::TempDir)> {
        let tempdir = tempfile::tempdir()?;
        let tree = crate::Config::new(
            &tempdir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        let ids = [
            ["a", "b", "c"],
            ["d", "e", "f"],
            ["g", "h", "i"],
            ["j", "k", "l"],
        ];

        for batch in ids {
            for id in batch {
                tree.insert(id, vec![], 0);
            }
            tree.flush_active_memtable(0)?;
        }

        let tables = tree
            .current_version()
            .iter_tables()
            .cloned()
            .collect::<Vec<_>>();

        let run = Arc::new(Run::new(tables).unwrap());
        Ok((run, tempdir))
    }

    /// Probes candidate hashes until finding one that every table in the
    /// run definitively rejects (`Ok(false)`). This avoids coupling tests
    /// to a specific bits-per-key configuration or hash function.
    ///
    /// With 4 tables of 3 single-byte keys each and default BPK, the
    /// first probe typically succeeds (FPR ~1% per table, ~4% combined).
    fn find_rejected_hash(run: &Run<Table>) -> u64 {
        for i in 0u64..10_000 {
            let candidate = crate::table::filter::standard_bloom::Builder::get_hash(
                format!("absent_key_{i}").as_bytes(),
            );
            let all_reject = run
                .iter()
                .all(|table| matches!(table.maybe_contains_prefix(candidate), Ok(false)));
            if all_reject {
                return candidate;
            }
        }
        panic!("could not find a universally rejected hash in 10 000 probes");
    }

    #[test]
    fn bloom_skip_forward_all_tables() -> crate::Result<()> {
        let (level, _dir) = make_bloom_test_run()?;
        let hash = find_rejected_hash(&level);

        let hints = BloomHints {
            prefix_hash: Some(hash),
            key_hash: None,
            #[cfg(feature = "metrics")]
            metrics: None,
        };

        let reader = RunReader::new(level, ..).unwrap().with_bloom_hints(hints);
        let items: Vec<_> = reader.flatten().collect();
        assert_eq!(items.len(), 0);

        Ok(())
    }

    #[test]
    fn bloom_skip_reverse_all_tables() -> crate::Result<()> {
        let (level, _dir) = make_bloom_test_run()?;
        let hash = find_rejected_hash(&level);

        let hints = BloomHints {
            prefix_hash: Some(hash),
            key_hash: None,
            #[cfg(feature = "metrics")]
            metrics: None,
        };

        let reader = RunReader::new(level, ..).unwrap().with_bloom_hints(hints);
        let items: Vec<_> = reader.rev().flatten().collect();
        assert_eq!(items.len(), 0);

        Ok(())
    }

    #[test]
    fn bloom_skip_no_hints_reads_all_tables() -> crate::Result<()> {
        let (level, _dir) = make_bloom_test_run()?;

        let reader = RunReader::new(level, ..).unwrap();
        let items: Vec<_> = reader.flatten().collect();

        assert_eq!(items.len(), 12);
        assert_eq!(Slice::from(*b"a"), items[0].key.user_key);
        assert_eq!(Slice::from(*b"l"), items[11].key.user_key);

        Ok(())
    }

    #[test]
    fn bloom_skip_lo_boundary_only() -> crate::Result<()> {
        let (level, _dir) = make_bloom_test_run()?;

        let hash = crate::table::filter::standard_bloom::Builder::get_hash(b"a");

        let hints = BloomHints {
            prefix_hash: Some(hash),
            key_hash: None,
            #[cfg(feature = "metrics")]
            metrics: None,
        };

        let reader = RunReader::new(level, ..).unwrap().with_bloom_hints(hints);
        let items: Vec<_> = reader.flatten().collect();

        assert!(
            items.len() >= 3,
            "lo boundary table must survive: got {}",
            items.len()
        );
        assert_eq!(Slice::from(*b"a"), items[0].key.user_key);
        assert_eq!(Slice::from(*b"b"), items[1].key.user_key);
        assert_eq!(Slice::from(*b"c"), items[2].key.user_key);

        Ok(())
    }

    /// Regression: verify all tables are visited in a 3-table run when
    /// bloom passes for all of them (no table silently skipped).
    #[test]
    fn bloom_hints_all_pass_3_table_run() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let tree = crate::Config::new(
            &tempdir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("x", "v1", 1);
        tree.flush_active_memtable(0)?;
        tree.insert("x", "v2", 2);
        tree.flush_active_memtable(0)?;
        tree.insert("x", "v3", 3);
        tree.flush_active_memtable(0)?;

        let tables = tree
            .current_version()
            .iter_tables()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(tables.len(), 3, "need exactly 3 tables");
        let run = Arc::new(Run::new(tables).unwrap());

        let hash_x = crate::table::filter::standard_bloom::Builder::get_hash(b"x");
        let hints = BloomHints {
            prefix_hash: Some(hash_x),
            key_hash: None,
            #[cfg(feature = "metrics")]
            metrics: None,
        };

        let reader = RunReader::new(run.clone(), ..)
            .unwrap()
            .with_bloom_hints(hints);
        let items: Vec<_> = reader.flatten().collect();
        assert_eq!(items.len(), 3, "all 3 tables must be visited");

        let hints2 = BloomHints {
            prefix_hash: Some(hash_x),
            key_hash: None,
            #[cfg(feature = "metrics")]
            metrics: None,
        };
        let reader = RunReader::new(run, ..).unwrap().with_bloom_hints(hints2);
        let items: Vec<_> = reader.rev().flatten().collect();
        assert_eq!(items.len(), 3, "all 3 tables must be visited in reverse");

        Ok(())
    }

    #[test]
    fn bloom_skip_via_key_hash() -> crate::Result<()> {
        let (level, _dir) = make_bloom_test_run()?;
        let hash = find_rejected_hash(&level);

        // Exercise the key_hash path (not prefix_hash).
        // find_rejected_hash probes via maybe_contains_prefix, but both
        // prefix and key bloom checks call the same bloom_may_contain_hash
        // on the same filter — a hash absent from one is absent from both.
        let hints = BloomHints {
            prefix_hash: None,
            key_hash: Some(hash),
            #[cfg(feature = "metrics")]
            metrics: None,
        };

        let reader = RunReader::new(level, ..).unwrap().with_bloom_hints(hints);
        let items: Vec<_> = reader.flatten().collect();
        assert_eq!(items.len(), 0);

        Ok(())
    }

    #[test]
    fn bloom_hints_struct_fields() {
        let hints = BloomHints {
            prefix_hash: Some(42),
            key_hash: Some(99),
            #[cfg(feature = "metrics")]
            metrics: None,
        };

        assert_eq!(hints.prefix_hash, Some(42));
        assert_eq!(hints.key_hash, Some(99));
    }
}
