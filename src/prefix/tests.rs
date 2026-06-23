use super::*;
// Shadows std's #[test] with test_log's version for structured logging.
// This IS used — #[test] on each function below resolves to this import.
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

#[test]
fn is_valid_scan_boundary_colon_terminated() {
    let extractor = ColonSeparatedPrefix;
    // "adj:" is a valid boundary — extractor emits it for "adj:" input
    assert!(extractor.is_valid_scan_boundary(b"adj:"));
    assert!(extractor.is_valid_scan_boundary(b"adj:out:"));
    assert!(extractor.is_valid_scan_boundary(b"adj:out:42:"));
}

#[test]
fn is_valid_scan_boundary_non_boundary() {
    let extractor = ColonSeparatedPrefix;
    // "adj" (no trailing colon) is NOT a valid boundary
    assert!(!extractor.is_valid_scan_boundary(b"adj"));
    assert!(!extractor.is_valid_scan_boundary(b"adj:out"));
    assert!(!extractor.is_valid_scan_boundary(b"noseparator"));
}

#[test]
fn is_valid_scan_boundary_empty() {
    let extractor = ColonSeparatedPrefix;
    assert!(!extractor.is_valid_scan_boundary(b""));
}

/// Extractor that overrides `is_valid_scan_boundary` with an O(1) length
/// check instead of iterating all prefixes via the default implementation.
struct FixedLengthPrefix;

impl PrefixExtractor for FixedLengthPrefix {
    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        if let Some(prefix) = key.get(..4) {
            Box::new(std::iter::once(prefix))
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn is_valid_scan_boundary(&self, prefix: &[u8]) -> bool {
        prefix.len() == 4
    }
}

#[test]
fn fixed_length_prefixes() {
    let extractor = FixedLengthPrefix;
    // Key longer than 4 bytes yields a single 4-byte prefix
    let prefixes: Vec<&[u8]> = extractor.prefixes(b"usr:data").collect();
    assert_eq!(prefixes, vec![b"usr:" as &[u8]]);

    // Key shorter than 4 bytes yields nothing
    let prefixes: Vec<&[u8]> = extractor.prefixes(b"ab").collect();
    assert!(prefixes.is_empty());

    // Key exactly 4 bytes yields itself
    let prefixes: Vec<&[u8]> = extractor.prefixes(b"abcd").collect();
    assert_eq!(prefixes, vec![b"abcd" as &[u8]]);
}

#[test]
fn custom_scan_boundary_valid() {
    let extractor = FixedLengthPrefix;
    assert!(extractor.is_valid_scan_boundary(b"usr:"));
    assert!(extractor.is_valid_scan_boundary(b"abcd"));
}

#[test]
fn custom_scan_boundary_invalid() {
    let extractor = FixedLengthPrefix;
    assert!(!extractor.is_valid_scan_boundary(b"ab"));
    assert!(!extractor.is_valid_scan_boundary(b"toolong"));
    assert!(!extractor.is_valid_scan_boundary(b""));
}
