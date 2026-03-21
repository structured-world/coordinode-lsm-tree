// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Extracts prefixes from keys for prefix bloom filter indexing.
///
/// When a `PrefixExtractor` is configured on a tree, the bloom filter indexes
/// not only full keys but also the prefixes returned by [`PrefixExtractor::prefixes`].
/// This allows prefix scans to skip entire segments that contain no keys with a
/// matching prefix, dramatically reducing I/O for prefix-heavy workloads (e.g.,
/// graph adjacency lists, time-series buckets).
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` because the extractor is shared across
/// flush, compaction, and read threads via `Arc`.
///
/// # Example
///
/// ```
/// use lsm_tree::PrefixExtractor;
///
/// /// Extracts prefixes at each ':' separator boundary.
/// ///
/// /// For key `adj:out:42:KNOWS`, yields:
/// ///   `adj:`, `adj:out:`, `adj:out:42:`, `adj:out:42:KNOWS`
/// struct ColonSeparatedPrefix;
///
/// impl PrefixExtractor for ColonSeparatedPrefix {
///     fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
///         Box::new(
///             key.iter()
///                 .enumerate()
///                 .filter(|(_, b)| **b == b':')
///                 .map(move |(i, _)| &key[..=i])
///                 .chain(std::iter::once(key)),
///         )
///     }
/// }
/// ```
pub trait PrefixExtractor:
    Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe
{
    /// Returns an iterator of prefixes to index for the given key.
    ///
    /// Each yielded prefix will be hashed and inserted into the segment's
    /// bloom filter. During a prefix scan, the scan prefix is hashed and
    /// checked against the bloom — segments without a match are skipped.
    ///
    /// Implementations should return prefixes from shortest to longest.
    /// The full key itself may or may not be included (it is always indexed
    /// separately by the standard bloom path).
    ///
    /// # Performance note
    ///
    /// Returns `Box<dyn Iterator>` for object safety (`Arc<dyn PrefixExtractor>`).
    /// Most extractors yield 1–5 prefixes per key, so the allocation is negligible
    /// compared to the bloom hash + I/O cost.
    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use test_log::test;

    struct ColonSeparatedPrefix;

    impl PrefixExtractor for ColonSeparatedPrefix {
        fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
            Box::new(
                key.iter()
                    .enumerate()
                    .filter(|(_, b)| **b == b':')
                    .map(move |(i, _)| &key[..=i]),
            )
        }
    }

    #[test]
    fn colon_separated_prefixes() {
        let extractor = ColonSeparatedPrefix;
        let key = b"adj:out:42:KNOWS";
        let prefixes: Vec<&[u8]> = extractor.prefixes(key).collect();
        assert_eq!(
            prefixes,
            vec![
                b"adj:" as &[u8],
                b"adj:out:" as &[u8],
                b"adj:out:42:" as &[u8],
            ]
        );
    }

    #[test]
    fn no_separator() {
        let extractor = ColonSeparatedPrefix;
        let key = b"noseparator";
        let prefixes: Vec<&[u8]> = extractor.prefixes(key).collect();
        assert!(prefixes.is_empty());
    }

    #[test]
    fn single_separator_at_end() {
        let extractor = ColonSeparatedPrefix;
        let key = b"prefix:";
        let prefixes: Vec<&[u8]> = extractor.prefixes(key).collect();
        assert_eq!(prefixes, vec![b"prefix:" as &[u8]]);
    }

    #[test]
    fn empty_key() {
        let extractor = ColonSeparatedPrefix;
        let prefixes: Vec<&[u8]> = extractor.prefixes(b"").collect();
        assert!(prefixes.is_empty());
    }
}
