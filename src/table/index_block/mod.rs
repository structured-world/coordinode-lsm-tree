// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

mod block_handle;
mod iter;

pub use block_handle::{BlockHandle, KeyedBlockHandle};
pub use iter::Iter;

use super::{
    Block,
    block::{BlockOffset, Encoder, Trailer},
};
use crate::Slice;
use crate::{
    SeqNo,
    table::{
        block::{Decoder, ParsedItem},
        util::{SliceIndexes, compare_prefixed_slice},
    },
};
use std::io::{Error, ErrorKind};

#[derive(Debug)]
pub struct IndexBlockParsedItem {
    pub offset: BlockOffset,
    pub size: u32,
    pub prefix: Option<SliceIndexes>,
    pub end_key: SliceIndexes,
    pub seqno: SeqNo,
}

impl ParsedItem<KeyedBlockHandle> for IndexBlockParsedItem {
    fn compare_key(
        &self,
        needle: &[u8],
        bytes: &[u8],
        cmp: &dyn crate::comparator::UserComparator,
    ) -> std::cmp::Ordering {
        // SAFETY: slice indexes come from the block parser which validates them
        // during decoding. The block format guarantees they are within bounds.
        if let Some(prefix) = &self.prefix {
            let prefix = unsafe { bytes.get_unchecked(prefix.0..prefix.1) };
            let rest_key = unsafe { bytes.get_unchecked(self.end_key.0..self.end_key.1) };
            compare_prefixed_slice(prefix, rest_key, needle, cmp)
        } else {
            // No allocation to avoid for a contiguous key — `compare()` is
            // already optimal for any comparator here. The lex fast path is
            // kept only on the prefix branch above (where
            // `compare_prefixed_slice_lexicographic` avoids the prefix+suffix
            // concatenation) and at the binary-search predicate construction
            // sites in `iter.rs` (where one `is_lexicographic()` is hoisted
            // to amortise across all BS probes). An extra `is_lexicographic()`
            // call per linear-scan step would cost custom comparators a
            // second vtable dispatch without any matching saving on the
            // default path.
            let key = unsafe { bytes.get_unchecked(self.end_key.0..self.end_key.1) };
            cmp.compare(key, needle)
        }
    }

    fn seqno(&self) -> SeqNo {
        self.seqno
    }

    fn key_offset(&self) -> usize {
        self.end_key.0
    }

    fn key_end_offset(&self) -> usize {
        self.end_key.1
    }

    fn materialize(&self, bytes: &Slice) -> KeyedBlockHandle {
        // NOTE: We consider the prefix and key slice indexes to be trustworthy
        #[expect(clippy::indexing_slicing)]
        let key = if let Some(prefix) = &self.prefix {
            let prefix_key = &bytes[prefix.0..prefix.1];
            let rest_key = &bytes[self.end_key.0..self.end_key.1];
            Slice::fused(prefix_key, rest_key)
        } else {
            bytes.slice(self.end_key.0..self.end_key.1)
        };

        KeyedBlockHandle::new(key, self.seqno, BlockHandle::new(self.offset, self.size))
    }
}

/// Block that contains block handles (file offset + size)
#[derive(Clone)]
pub struct IndexBlock {
    pub inner: Block,
}

impl IndexBlock {
    #[must_use]
    pub fn new(inner: Block) -> Self {
        Self { inner }
    }

    /// Accesses the inner raw bytes
    #[must_use]
    pub fn as_slice(&self) -> &Slice {
        &self.inner.data
    }

    /// Returns the number of items in the block.
    #[must_use]
    #[expect(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        Trailer::new(&self.inner).item_count()
    }

    /// Creates a fallible iterator over the index block.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidTag`] if the block is not an index block,
    /// or [`crate::Error::InvalidTrailer`] if the block trailer is malformed
    /// (e.g. `restart_interval == 0`).
    pub fn try_iter(
        &self,
        comparator: crate::comparator::SharedComparator,
    ) -> crate::Result<Iter<'_>> {
        use crate::table::block::BlockType;

        if self.inner.header.block_type != BlockType::Index {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                self.inner.header.block_type.into(),
            )));
        }
        Ok(Iter::new(
            Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::try_new(&self.inner)?,
            comparator,
        ))
    }

    #[must_use]
    pub fn iter(&self, comparator: crate::comparator::SharedComparator) -> Iter<'_> {
        Iter::new(
            Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(&self.inner),
            comparator,
        )
    }

    pub fn encode_into_vec(items: &[KeyedBlockHandle]) -> crate::Result<Vec<u8>> {
        Self::encode_into_vec_with_restart_interval(items, 1)
    }

    /// Builds an index block with the given restart interval into a new `Vec`.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::ErrorKind::InvalidInput`] when `restart_interval == 0`.
    ///
    /// # Panics
    ///
    /// Panics if `items` is empty.
    pub fn encode_into_vec_with_restart_interval(
        items: &[KeyedBlockHandle],
        restart_interval: u8,
    ) -> crate::Result<Vec<u8>> {
        let mut buf = vec![];

        Self::encode_into_with_restart_interval(&mut buf, items, restart_interval)?;

        Ok(buf)
    }

    /// Builds an index block.
    ///
    /// # Panics
    ///
    /// Panics if the given item array if empty.
    pub fn encode_into(writer: &mut Vec<u8>, items: &[KeyedBlockHandle]) -> crate::Result<()> {
        Self::encode_into_with_restart_interval(writer, items, 1)
    }

    /// Builds an index block using the provided restart interval.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::ErrorKind::InvalidInput`] when `restart_interval == 0`.
    ///
    /// # Panics
    ///
    /// Panics if `items` is empty.
    pub fn encode_into_with_restart_interval(
        writer: &mut Vec<u8>,
        items: &[KeyedBlockHandle],
        restart_interval: u8,
    ) -> crate::Result<()> {
        if restart_interval == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "index block restart interval must be greater than zero",
            )
            .into());
        }

        #[expect(clippy::expect_used)]
        let first_key = items.first().expect("chunk should not be empty").end_key();

        let mut serializer = Encoder::<'_, BlockOffset, KeyedBlockHandle>::new(
            writer,
            items.len(),
            restart_interval,
            0.0, // Index blocks do not support hash index
            first_key,
        );

        for item in items {
            serializer.write(item)?;
        }

        serializer.finish()
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing, reason = "test code")]
mod tests {
    use super::*;
    use crate::table::BlockHandle;

    fn make_shared_prefix_handles(count: usize) -> Vec<KeyedBlockHandle> {
        (0..count)
            .map(|i| {
                let key = format!("adj:out:vertex-0001:edge-{i:04}:target-0001");
                KeyedBlockHandle::new(
                    key.into(),
                    i as u64,
                    BlockHandle::new(BlockOffset((i as u64) * 4096), 4096),
                )
            })
            .collect()
    }

    #[test]
    fn higher_restart_interval_reduces_index_block_size_for_shared_prefix_keys() {
        let handles = make_shared_prefix_handles(256);

        let legacy = IndexBlock::encode_into_vec_with_restart_interval(&handles, 1).unwrap();
        let compressed = IndexBlock::encode_into_vec_with_restart_interval(&handles, 16).unwrap();

        assert!(
            compressed.len() < legacy.len(),
            "compressed={} should be smaller than legacy={}",
            compressed.len(),
            legacy.len(),
        );
    }

    #[test]
    fn zero_restart_interval_is_rejected() {
        let handles = make_shared_prefix_handles(2);
        let Err(err) = IndexBlock::encode_into_vec_with_restart_interval(&handles, 0) else {
            panic!("restart interval of zero must be rejected");
        };
        assert!(matches!(err, crate::Error::Io(e) if e.kind() == ErrorKind::InvalidInput));
    }

    #[test]
    fn try_iter_zero_restart_interval_returns_invalid_trailer() {
        use crate::table::block::{BlockType, Header, Trailer};

        let handles = make_shared_prefix_handles(4);
        let mut bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 2).unwrap();

        let block = Block {
            data: bytes.clone().into(),
            header: Header::test_dummy(BlockType::Index),
        };
        let trailer_offset = Trailer::new(&block).trailer_offset();
        bytes[trailer_offset] = 0;

        let corrupt_index = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header::test_dummy(BlockType::Index),
        });

        let cmp = crate::comparator::default_comparator();
        assert!(
            matches!(
                corrupt_index.try_iter(cmp),
                Err(crate::Error::InvalidTrailer)
            ),
            "zero restart_interval must return InvalidTrailer",
        );
    }

    // Regression tests for binary-search-predicate devirtualization on the
    // lexicographic fast path. Mirrors `data_block::iter_test::devirt`:
    // index-block `seek` / `seek_upper` apply the same `is_lexicographic()`
    // branching to skip `dyn UserComparator::compare` vtable dispatch on the
    // BS probe loop. These tests use a counting-comparator wrapper to assert:
    //   1. lex path makes ZERO compare() calls (no vtable in the BS loop)
    //   2. dyn path makes >= log2(restart_heads) compare() calls (BS predicate
    //      actually invokes vtable — guards against lex closure leaking)
    //   3. lex and dyn paths produce identical landing positions on boundary
    //      needles
    mod devirt {
        use super::*;
        use crate::comparator::UserComparator;
        use crate::table::BlockHandle;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        struct CountingComparator {
            /// Counts `compare()` invocations — proves the lex devirt path
            /// successfully bypasses the `dyn UserComparator::compare` vtable.
            count: Arc<AtomicUsize>,
            /// Counts `is_lexicographic()` invocations. Sanity counter:
            /// asserts the BS predicate factory in iter.rs actually
            /// consulted `is_lexicographic()` to pick a closure.
            ///
            /// After the review-driven revert of the `compare_key`
            /// no-prefix lex fast path, the only `is_lex` call site touched
            /// by these tests is the BS predicate factory itself (one call
            /// per seek entry point, hoisted out of the BS loop). So
            /// `is_lex_count > 0` reliably proves the factory ran. The
            /// TRUE proof that it selected the lex closure is
            /// `count <= LEX_PATH_LINEAR_SCAN_BOUND` (a dyn closure would
            /// produce >= `DYN_MIN_BS_PROBES` from BS probes alone).
            is_lex_count: Arc<AtomicUsize>,
            lex: bool,
        }

        impl UserComparator for CountingComparator {
            fn name(&self) -> &'static str {
                "counting"
            }
            fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
                self.count.fetch_add(1, AtomicOrdering::Relaxed);
                a.cmp(b)
            }
            fn is_lexicographic(&self) -> bool {
                self.is_lex_count.fetch_add(1, AtomicOrdering::Relaxed);
                self.lex
            }
        }

        /// Build an index block tuned to make BS probes dominate any potential
        /// linear-scan contribution:
        ///   - 128 handles with distinct sortable `end_key`s
        ///   - `restart_interval=1` → each handle IS a restart head, AND
        ///     the `advance_while` / `trim_back_to_upper_bound` linear-scan
        ///     branches in `seek_with_cache_resets` / `seek_upper_impl` are
        ///     bypassed entirely (those are gated on `restart_interval > 1`)
        ///   - binary search: log2(128) = 7 probes
        ///
        /// `assert delta >= DYN_MIN_BS_PROBES` (= 7) cleanly distinguishes
        /// the lex-leak case (BS contributes 0, no linear scan to add to
        /// the count) from a working dyn path (BS contributes exactly 7).
        /// See [`DYN_MIN_BS_PROBES`] for the full discrimination math.
        fn build_index_block_bs_dominated() -> IndexBlock {
            use crate::table::block::{BlockType, Header};

            let handles: Vec<_> = (0_u64..128)
                .map(|i| {
                    KeyedBlockHandle::new(
                        i.to_be_bytes().to_vec().into(),
                        i,
                        BlockHandle::new(BlockOffset(i * 4096), 4096),
                    )
                })
                .collect();
            let bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 1).unwrap();
            IndexBlock::new(Block {
                data: bytes.into(),
                header: Header::test_dummy(BlockType::Index),
            })
        }

        /// Minimum number of `compare()` calls a working dyn BS makes on
        /// the BS-dominated index block: `⌈log2(128)⌉ = 7` probes.
        ///
        /// For `restart_interval == 1` (used by [`build_index_block_bs_dominated`]),
        /// the index-block `seek` / `seek_upper` paths skip the
        /// `advance_while` / `trim_back_to_upper_bound` linear-scan branches
        /// in `seek_with_cache_resets` / `seek_upper_impl`, so the dyn count
        /// equals the BS probe count exactly. lex-leak makes 0 calls.
        ///
        /// `assert delta >= 7` cleanly catches the lex-leak. A weaker
        /// threshold could match an inflated lex-leak count if linear-scan
        /// contributions were possible.
        const DYN_MIN_BS_PROBES: usize = 7;

        /// Above-max needle (9 bytes > any 8-byte encoded key) — used to
        /// bound any potential linear-scan contribution.
        fn above_max_needle() -> Vec<u8> {
            let mut v = 127_u64.to_be_bytes().to_vec();
            v.push(0xFF);
            v
        }

        /// Upper bound on `compare()` calls a lex-path index seek can produce
        /// from non-BS sources. With `restart_interval == 1`, the index-block
        /// `advance_while` / `trim_back_to_upper_bound` linear branches are
        /// bypassed entirely, so this bound is effectively 0 — but we allow
        /// a small slack for any auxiliary lookups the iterator may perform.
        const LEX_PATH_LINEAR_SCAN_BOUND: usize = 2;

        #[test]
        fn index_block_seek_lex_path_skips_vtable() {
            // Both devirtualized entry points (seek, seek_upper) must route
            // through static-dispatch closures when is_lexicographic() == true.
            // Per-entry-point snapshot localises any regression.
            // Above-max needle (paired with restart_interval=1) bounds the
            // post-BS contribution to <= LEX_PATH_LINEAR_SCAN_BOUND across
            // both public `seek` / `seek_upper` and the pub(crate)
            // `seek_upper_bound_cursor` path. A regression where any of
            // these BS predicates fell back to the dyn closure would produce
            // >= DYN_MIN_BS_PROBES (= 7) calls — well above the bound.
            let index_block = build_index_block_bs_dominated();
            let count = Arc::new(AtomicUsize::new(0));
            let is_lex_count = Arc::new(AtomicUsize::new(0));
            let cmp: Arc<dyn UserComparator> = Arc::new(CountingComparator {
                count: count.clone(),
                is_lex_count: is_lex_count.clone(),
                lex: true,
            });
            let needle = above_max_needle();

            let before = count.load(AtomicOrdering::Relaxed);
            let before_lex = is_lex_count.load(AtomicOrdering::Relaxed);
            {
                let mut iter = index_block.iter(cmp.clone());
                let _ = iter.seek(&needle, crate::SeqNo::MAX);
            }
            let after_seek = count.load(AtomicOrdering::Relaxed);
            let after_seek_lex = is_lex_count.load(AtomicOrdering::Relaxed);
            let seek_delta = after_seek - before;
            assert!(
                seek_delta <= LEX_PATH_LINEAR_SCAN_BOUND,
                "index seek lex path leaked into dyn BS: {seek_delta} compare() calls (expected <= {LEX_PATH_LINEAR_SCAN_BOUND})",
            );
            assert!(
                after_seek_lex - before_lex >= 1,
                "index seek lex path must consult is_lexicographic() to select the lex closure, got {} calls",
                after_seek_lex - before_lex,
            );

            {
                let mut iter = index_block.iter(cmp.clone());
                let _ = iter.seek_upper(&needle, crate::SeqNo::MAX);
            }
            let after_upper = count.load(AtomicOrdering::Relaxed);
            let after_upper_lex = is_lex_count.load(AtomicOrdering::Relaxed);
            let upper_delta = after_upper - after_seek;
            assert!(
                upper_delta <= LEX_PATH_LINEAR_SCAN_BOUND,
                "index seek_upper lex path leaked into dyn BS: {upper_delta} compare() calls (expected <= {LEX_PATH_LINEAR_SCAN_BOUND})",
            );
            assert!(
                after_upper_lex - after_seek_lex >= 1,
                "index seek_upper lex path must consult is_lexicographic(), got {} calls",
                after_upper_lex - after_seek_lex,
            );

            // seek_upper_bound_cursor takes the OTHER branch inside
            // seek_upper_impl at restart_interval == 1 (`check_back_cache=false`,
            // predicate `<=` instead of `<`). The public seek_upper above only
            // exercises check_back_cache=true; this call covers the forward-limit
            // path used by block-index upper-bound cursors.
            {
                let mut iter = index_block.iter(cmp);
                let _ = iter.seek_upper_bound_cursor(&needle, crate::SeqNo::MAX);
            }
            let after_cursor = count.load(AtomicOrdering::Relaxed);
            let after_cursor_lex = is_lex_count.load(AtomicOrdering::Relaxed);
            let cursor_delta = after_cursor - after_upper;
            assert!(
                cursor_delta <= LEX_PATH_LINEAR_SCAN_BOUND,
                "index seek_upper_bound_cursor lex path leaked into dyn BS: {cursor_delta} compare() calls (expected <= {LEX_PATH_LINEAR_SCAN_BOUND})",
            );
            assert!(
                after_cursor_lex - after_upper_lex >= 1,
                "index seek_upper_bound_cursor lex path must consult is_lexicographic(), got {} calls",
                after_cursor_lex - after_upper_lex,
            );
        }

        #[test]
        fn index_block_seek_dyn_path_invokes_compare() {
            // BS-dominated block: a working dyn BS makes >= log2(128) = 7 calls.
            // Lex closure leak would yield at most 1 call (linear scan only).
            let index_block = build_index_block_bs_dominated();
            let count = Arc::new(AtomicUsize::new(0));
            let cmp: Arc<dyn UserComparator> = Arc::new(CountingComparator {
                count: count.clone(),
                is_lex_count: Arc::new(AtomicUsize::new(0)),
                lex: false,
            });
            let needle = above_max_needle();

            let before = count.load(AtomicOrdering::Relaxed);
            {
                let mut iter = index_block.iter(cmp);
                let _ = iter.seek(&needle, crate::SeqNo::MAX);
            }
            let delta = count.load(AtomicOrdering::Relaxed) - before;
            assert!(
                delta >= DYN_MIN_BS_PROBES,
                "index seek dyn BS must call compare() at least {DYN_MIN_BS_PROBES} times \
                 (log2(128 restart heads) probes), got {delta} — lex closure leaked into dyn BS?",
            );
        }

        #[test]
        fn index_block_seek_upper_dyn_path_invokes_compare() {
            let index_block = build_index_block_bs_dominated();
            let count = Arc::new(AtomicUsize::new(0));
            let cmp: Arc<dyn UserComparator> = Arc::new(CountingComparator {
                count: count.clone(),
                is_lex_count: Arc::new(AtomicUsize::new(0)),
                lex: false,
            });
            let needle = above_max_needle();

            let before = count.load(AtomicOrdering::Relaxed);
            {
                let mut iter = index_block.iter(cmp);
                let _ = iter.seek_upper(&needle, crate::SeqNo::MAX);
            }
            let delta = count.load(AtomicOrdering::Relaxed) - before;
            assert!(
                delta >= DYN_MIN_BS_PROBES,
                "index seek_upper dyn BS must call compare() at least {DYN_MIN_BS_PROBES} times \
                 (log2(128 restart heads) probes), got {delta} — lex closure leaked into dyn BS?",
            );
        }

        #[test]
        fn index_block_seek_upper_bound_cursor_dyn_path_invokes_compare() {
            // The `check_back_cache == false` branch in `seek_upper_impl`
            // uses a DIFFERENT BS predicate (`<= needle` instead of `< needle`
            // at restart_interval == 1) than the public seek_upper. Reached
            // only via this pub(crate) entry point — public seek_upper
            // doesn't cover it. Verify the dyn closure for THIS predicate
            // also invokes compare().
            let index_block = build_index_block_bs_dominated();
            let count = Arc::new(AtomicUsize::new(0));
            let cmp: Arc<dyn UserComparator> = Arc::new(CountingComparator {
                count: count.clone(),
                is_lex_count: Arc::new(AtomicUsize::new(0)),
                lex: false,
            });
            let needle = above_max_needle();

            let before = count.load(AtomicOrdering::Relaxed);
            {
                let mut iter = index_block.iter(cmp);
                let _ = iter.seek_upper_bound_cursor(&needle, crate::SeqNo::MAX);
            }
            let delta = count.load(AtomicOrdering::Relaxed) - before;
            assert!(
                delta >= DYN_MIN_BS_PROBES,
                "index seek_upper_bound_cursor dyn BS must call compare() at least {DYN_MIN_BS_PROBES} times, \
                 got {delta} — lex closure leaked into dyn BS of the check_back_cache=false predicate?",
            );
        }

        #[test]
        #[expect(
            clippy::too_many_lines,
            reason = "exhaustive equivalence matrix: 6 boundary needles × 3 entry points × (call + assert + landing-read + assert) is the actual coverage surface this test is meant to provide"
        )]
        fn index_block_seek_lex_and_dyn_agree_on_landing_position() {
            use crate::table::block::{BlockType, Header};

            // Smaller block where boundary needle behaviour is what we care
            // about (rather than BS-vs-scan call-count discrimination).
            let handles: Vec<_> = (0_u64..32)
                .map(|i| {
                    KeyedBlockHandle::new(
                        i.to_be_bytes().to_vec().into(),
                        i,
                        BlockHandle::new(BlockOffset(i * 4096), 4096),
                    )
                })
                .collect();
            let bytes = IndexBlock::encode_into_vec_with_restart_interval(&handles, 4).unwrap();
            let index_block = IndexBlock::new(Block {
                data: bytes.into(),
                header: Header::test_dummy(BlockType::Index),
            });

            let lex: Arc<dyn UserComparator> = Arc::new(CountingComparator {
                count: Arc::new(AtomicUsize::new(0)),
                is_lex_count: Arc::new(AtomicUsize::new(0)),
                lex: true,
            });
            let dyn_cmp: Arc<dyn UserComparator> = Arc::new(CountingComparator {
                count: Arc::new(AtomicUsize::new(0)),
                is_lex_count: Arc::new(AtomicUsize::new(0)),
                lex: false,
            });

            // Boundary needles covering the full `partition_point` table:
            //   - empty slice          → BELOW min (sorts before any non-empty)
            //   - exact-min (key 0)
            //   - 9-byte between-keys  → strictly between [0…0,16] and [0…0,17]
            //   - exact-mid (key 16)
            //   - exact-tail (key 31)  → last key, exercises left==len
            //   - above-max (9 bytes)  → above any 8-byte key
            //
            // The previous version used only `to_be_bytes()` values which were
            // all exact-keys + one above-max, missing the genuine below-min
            // and between-key cases.
            let between_16_and_17: Vec<u8> = {
                let mut v = 16_u64.to_be_bytes().to_vec();
                v.push(0); // 9 bytes: > 16, < 17 lexicographically
                v
            };
            let above_max: Vec<u8> = {
                let mut v = 31_u64.to_be_bytes().to_vec();
                v.push(0xFF);
                v
            };
            let needles: Vec<(&str, Vec<u8>)> = vec![
                ("below-min (empty slice)", vec![]),
                ("exact-min (key 0)", 0_u64.to_be_bytes().to_vec()),
                ("between keys 16 and 17", between_16_and_17),
                ("exact-mid (key 16)", 16_u64.to_be_bytes().to_vec()),
                ("exact-tail (key 31)", 31_u64.to_be_bytes().to_vec()),
                ("above-max (key 31 + 0xFF)", above_max),
            ];

            // Exercise both devirtualized entry points (`seek` and
            // `seek_upper`) against the same boundary needle table. The
            // two have different predicate shapes (forward seqno-aware
            // 3-way vs reverse `<` / `<=`), so the call-count assertions
            // above wouldn't catch a landing mismatch from a wrong
            // operator in the lex closure of either one.
            for (label, needle) in &needles {
                // seek (forward, seqno-aware)
                let mut lex_iter = index_block.iter(lex.clone());
                let lex_seek = lex_iter.seek(needle, crate::SeqNo::MAX);
                let mut dyn_iter = index_block.iter(dyn_cmp.clone());
                let dyn_seek = dyn_iter.seek(needle, crate::SeqNo::MAX);
                assert_eq!(
                    lex_seek, dyn_seek,
                    "index seek result must match for needle {label} ({needle:?})",
                );
                let lex_landing = lex_iter
                    .next()
                    .map(|h| h.materialize(index_block.as_slice()).end_key().clone());
                let dyn_landing = dyn_iter
                    .next()
                    .map(|h| h.materialize(index_block.as_slice()).end_key().clone());
                assert_eq!(
                    lex_landing.as_ref().map(|s| s.as_ref().to_vec()),
                    dyn_landing.as_ref().map(|s| s.as_ref().to_vec()),
                    "index seek landing must match for needle {label} ({needle:?})",
                );

                // seek_upper (reverse upper-bound — exercises seek_upper_impl
                // with check_back_cache=true). This test block uses
                // restart_interval = 4, so both check_back_cache branches
                // land in the `restart_interval > 1` arm of seek_upper_impl
                // which uses the same `<=` predicate. The `restart_interval
                // == 1` branches where `<` vs `<=` predicates diverge are
                // separately covered by the BS-dominated lex/dyn tests above
                // (which build the index block with restart_interval = 1).
                let mut lex_iter = index_block.iter(lex.clone());
                let lex_upper = lex_iter.seek_upper(needle, crate::SeqNo::MAX);
                let mut dyn_iter = index_block.iter(dyn_cmp.clone());
                let dyn_upper = dyn_iter.seek_upper(needle, crate::SeqNo::MAX);
                assert_eq!(
                    lex_upper, dyn_upper,
                    "index seek_upper result must match for needle {label} ({needle:?})",
                );
                let lex_upper_landing = lex_iter
                    .next_back()
                    .map(|h| h.materialize(index_block.as_slice()).end_key().clone());
                let dyn_upper_landing = dyn_iter
                    .next_back()
                    .map(|h| h.materialize(index_block.as_slice()).end_key().clone());
                assert_eq!(
                    lex_upper_landing.as_ref().map(|s| s.as_ref().to_vec()),
                    dyn_upper_landing.as_ref().map(|s| s.as_ref().to_vec()),
                    "index seek_upper landing must match for needle {label} ({needle:?})",
                );

                // seek_upper_bound_cursor — same seek_upper_impl with
                // check_back_cache=false. With this test block's
                // restart_interval = 4 it shares the `restart_interval > 1`
                // arm with the public seek_upper above. The divergent
                // `restart_interval == 1` paths (where check_back_cache=false
                // uses `<=` and check_back_cache=true uses strict `<`) are
                // covered by the dedicated dyn-path and lex-path tests above
                // that build the index block with restart_interval = 1.
                // This arm still verifies that the lex/dyn closures agree
                // on landing position when both seek_upper variants are
                // routed through the shared restart_interval>1 predicate.
                let mut lex_iter = index_block.iter(lex.clone());
                let lex_cursor = lex_iter
                    .seek_upper_bound_cursor(needle, crate::SeqNo::MAX)
                    .unwrap();
                let mut dyn_iter = index_block.iter(dyn_cmp.clone());
                let dyn_cursor = dyn_iter
                    .seek_upper_bound_cursor(needle, crate::SeqNo::MAX)
                    .unwrap();
                assert_eq!(
                    lex_cursor, dyn_cursor,
                    "index seek_upper_bound_cursor result must match for needle {label} ({needle:?})",
                );
                let lex_cursor_landing = lex_iter
                    .next_back()
                    .map(|h| h.materialize(index_block.as_slice()).end_key().clone());
                let dyn_cursor_landing = dyn_iter
                    .next_back()
                    .map(|h| h.materialize(index_block.as_slice()).end_key().clone());
                assert_eq!(
                    lex_cursor_landing.as_ref().map(|s| s.as_ref().to_vec()),
                    dyn_cursor_landing.as_ref().map(|s| s.as_ref().to_vec()),
                    "index seek_upper_bound_cursor landing must match for needle {label} ({needle:?})",
                );
            }
        }
    }
}
