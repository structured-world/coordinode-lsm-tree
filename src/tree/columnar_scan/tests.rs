use super::*;
use crate::table::columnar::entries_to_column_batch;
use crate::{InternalValue, ValueType};
use test_log::test;

/// A scan with no segments, only the fields `globalize_seqnos` reads.
fn empty_scan(metrics: alloc::sync::Arc<crate::Metrics>) -> ColumnarScan {
    ColumnarScan {
        groups: alloc::collections::VecDeque::new(),
        buffered: alloc::collections::VecDeque::new(),
        projection: Vec::new(),
        predicate: None,
        support: PredicateSupport::Exact,
        comparator: crate::comparator::default_comparator(),
        seqno: SeqNo::MAX,
        lo: Bound::Unbounded,
        hi: Bound::Unbounded,
        metrics,
    }
}

/// Globalizing a seqno column rewrites it row by row into a new buffer. When a
/// later row's effective seqno overflows, the rows before it were already
/// written, and those bytes must be charged even though the batch is refused.
#[test]
fn globalize_seqnos_charges_the_rows_rewritten_before_an_overflow() -> crate::Result<()> {
    let mut batch = entries_to_column_batch(&[
        InternalValue::from_components(b"a", b"v", 1, ValueType::Value),
        InternalValue::from_components(b"b", b"v", u64::MAX - 1, ValueType::Value),
    ])?;
    let metrics = alloc::sync::Arc::new(crate::Metrics::default());
    let scan = empty_scan(metrics.clone());

    let result = scan.globalize_seqnos(&mut batch, 5);
    assert!(
        matches!(result, Err(Error::InvalidHeader(m)) if m.contains("overflows")),
        "the second row's effective seqno must overflow, got {result:?}",
    );
    assert_eq!(
        metrics.bytes_copied(),
        8,
        "the first row's rewritten seqno was written before the overflow",
    );
    Ok(())
}
