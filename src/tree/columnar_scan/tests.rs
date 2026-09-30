use super::*;
use crate::table::columnar::entries_to_column_batch;
use crate::{InternalValue, ValueType};
use test_log::test;

/// A scan with no segments, only the fields `globalize_seqnos` reads.
fn empty_scan(metrics: alloc::sync::Arc<crate::Metrics>) -> ColumnarScan {
    ColumnarScan {
        groups: alloc::collections::VecDeque::new(),
        current: None,
        projection: Vec::new(),
        fields: Vec::new(),
        projector: None,
        declared: false,
        resolver: None,
        predicate: None,
        support: PredicateSupport::Exact,
        comparator: crate::comparator::default_comparator(),
        seqno: SeqNo::MAX,
        lo: Bound::Unbounded,
        hi: Bound::Unbounded,
        budget: crate::config::DEFAULT_COLUMNAR_SCAN_BUDGET,
        peak_payload: core::cell::Cell::new(0),
        oversized: core::cell::Cell::new(0),
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

/// A table records one value layout, so an ingestion that writes a row and
/// then a columnar batch leaves each in a table of its own layout: the row's
/// value whole, the batch's split into its fields.
#[test]
fn an_ingestion_of_rows_and_batches_writes_one_value_layout_per_table() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::table::columnar::{Column, TypeTag};
    use crate::table::meta::ValueLayout;

    let folder = crate::get_tmp_folder();
    let any = crate::Config::new(
        folder.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .open()?;
    let crate::AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    tree.update_runtime_config(|cfg| cfg.columnar = true)?;

    let mut batch = entries_to_column_batch(&[InternalValue::from_components(
        b"b",
        b"ignored",
        0,
        ValueType::Value,
    )])?;
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: alloc::vec![1, 0, 0, 0].into(),
    });
    let mut ingestion = any.ingestion()?;
    ingestion.write(b"a".to_vec(), b"row".to_vec())?;
    ingestion.write_columnar_batch(&batch)?;
    ingestion.finish()?;

    let version = tree.current_version();
    let mut layouts: Vec<(Vec<u8>, ValueLayout)> = version
        .iter_tables()
        .map(|t| (t.metadata.key_range.min().to_vec(), t.metadata.value_layout))
        .collect();
    layouts.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        alloc::vec![
            (b"a".to_vec(), ValueLayout::Whole),
            (b"b".to_vec(), ValueLayout::Split),
        ],
        layouts,
    );
    Ok(())
}
