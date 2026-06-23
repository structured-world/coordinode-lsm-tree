use super::{
    Column, ColumnBatch, ColumnRangePredicate, ColumnStats, TypeTag, byte_eq_mask, byte_eq_scalar,
    filter_batch,
};
use crate::table::columnar::{column_batch_to_entries, entries_to_column_batch};
use crate::{Slice, ValueType, key::InternalKey, value::InternalValue};

fn entry(key: &[u8], seqno: u64, value: &[u8]) -> InternalValue {
    InternalValue {
        key: InternalKey::new(Slice::from(key), seqno, ValueType::Value),
        value: Slice::from(value),
    }
}

fn stats(column_id: u32, min: &[u8], max: &[u8]) -> ColumnStats {
    ColumnStats {
        column_id,
        type_tag: 1,
        codec_id: 0,
        null_count: 0,
        row_count: 2,
        min: min.to_vec(),
        max: max.to_vec(),
    }
}

#[test]
fn can_skip_block_when_range_is_disjoint() {
    // Predicate on the user-key column (id 0) for keys in [m, z].
    let pred = ColumnRangePredicate {
        column_id: 0,
        lower: Some(b"m".to_vec()),
        upper: Some(b"z".to_vec()),
    };
    // Block whose keys are [a, c]: entirely below the lower bound -> skip.
    assert!(pred.can_skip_block(&[stats(0, b"a", b"c")]));
    // Block whose keys are [za, zz]: entirely above the upper bound -> skip.
    assert!(pred.can_skip_block(&[stats(0, b"za", b"zz")]));
    // Block whose keys are [p, t]: overlaps -> cannot skip.
    assert!(!pred.can_skip_block(&[stats(0, b"p", b"t")]));
    // No stats for the column -> conservative, cannot skip.
    assert!(!pred.can_skip_block(&[stats(7, b"a", b"c")]));
}

#[test]
fn matching_rows_filters_the_key_column() {
    // Two rows with keys "alpha" and "bravo"; filter to keys >= "b".
    let batch = entries_to_column_batch(&[entry(b"alpha", 10, b"v1"), entry(b"bravo", 9, b"v2")])
        .expect("transpose");
    let pred = ColumnRangePredicate {
        column_id: 0, // user-key column
        lower: Some(b"b".to_vec()),
        upper: None,
    };
    assert_eq!(pred.matching_rows(&batch), vec![false, true]);
}

#[test]
fn matching_rows_all_true_when_column_absent() {
    let batch = entries_to_column_batch(&[entry(b"k", 1, b"v")]).expect("transpose");
    // No column 99 in the batch -> cannot filter, every row matches.
    let pred = ColumnRangePredicate {
        column_id: 99,
        lower: Some(b"z".to_vec()),
        upper: None,
    };
    assert_eq!(pred.matching_rows(&batch), vec![true]);
}

#[test]
fn filter_batch_keeps_only_masked_rows() {
    // Three rows; keep rows 0 and 2. The round trip through the transpose
    // checks every intrinsic column (key, seqno, value type, value) is
    // compacted correctly.
    let entries = vec![
        entry(b"aaa", 3, b"va"),
        entry(b"bbb", 2, b"vb"),
        entry(b"ccc", 1, b"vc"),
    ];
    let batch = entries_to_column_batch(&entries).expect("transpose");
    let filtered = filter_batch(&batch, &[true, false, true]);
    assert_eq!(filtered.row_count, 2);

    let back = column_batch_to_entries(&filtered).expect("untranspose");
    assert_eq!(back.len(), 2);
    assert_eq!(&*back[0].key.user_key, b"aaa");
    assert_eq!(back[0].key.seqno, 3);
    assert_eq!(&*back[0].value, b"va");
    assert_eq!(&*back[1].key.user_key, b"ccc");
    assert_eq!(back[1].key.seqno, 1);
    assert_eq!(&*back[1].value, b"vc");
}

#[test]
fn byte_eq_simd_matches_scalar_on_a_corpus() {
    // A 1000-byte value-type corpus (values 0..3), filtered to value 1. On
    // this host the dispatch runs the widest available kernel; it must be
    // bit-identical to the portable scalar reference.
    let mut data = Vec::new();
    for i in 0..1000u32 {
        data.push(u8::try_from(i % 4).unwrap_or(0));
    }
    let batch = ColumnBatch {
        row_count: u32::try_from(data.len()).unwrap_or(0),
        columns: vec![Column {
            column_id: 2,
            type_tag: TypeTag::Fixed(1),
            validity: None,
            data: data.clone(),
        }],
    };
    assert_eq!(
        byte_eq_mask(&batch, 2, 1),
        byte_eq_scalar(&data, 1),
        "the SIMD byte-eq kernel must equal the scalar reference"
    );
}

/// Builds a `Bytes` column from row values: a `(rows + 1)` u32 offset table
/// followed by the concatenated payload.
fn bytes_column(column_id: u16, validity: Option<Vec<u8>>, rows: &[&[u8]]) -> Column {
    let mut data = Vec::new();
    let mut acc = 0u32;
    data.extend_from_slice(&acc.to_le_bytes());
    for r in rows {
        acc += u32::try_from(r.len()).unwrap_or(0);
        data.extend_from_slice(&acc.to_le_bytes());
    }
    for r in rows {
        data.extend_from_slice(r);
    }
    Column {
        column_id,
        type_tag: TypeTag::Bytes,
        validity,
        data,
    }
}

#[test]
fn matching_rows_excludes_null_rows_and_respects_both_bounds() {
    // Keys a / b / c with row 1 (b) null; predicate [a, z] keeps the two
    // non-null in-range rows and drops the null one.
    let batch = ColumnBatch {
        row_count: 3,
        // rows 0 and 2 valid, row 1 null.
        columns: vec![bytes_column(
            0,
            Some(vec![0b0000_0101]),
            &[b"a", b"b", b"c"],
        )],
    };
    let pred = ColumnRangePredicate {
        column_id: 0,
        lower: Some(b"a".to_vec()),
        upper: Some(b"z".to_vec()),
    };
    assert_eq!(pred.matching_rows(&batch), vec![true, false, true]);
}

#[test]
fn matching_rows_all_true_for_a_non_bytes_column() {
    // A fixed-width column is not row-filterable (its stored form is not the
    // comparable encoding), so every row passes.
    let batch = ColumnBatch {
        row_count: 2,
        columns: vec![Column {
            column_id: 1,
            type_tag: TypeTag::Fixed(8),
            validity: None,
            data: vec![0u8; 16],
        }],
    };
    let pred = ColumnRangePredicate {
        column_id: 1,
        lower: Some(vec![5]),
        upper: None,
    };
    assert_eq!(pred.matching_rows(&batch), vec![true, true]);
}

#[test]
fn byte_eq_mask_all_true_when_inapplicable() {
    let batch = ColumnBatch {
        row_count: 2,
        columns: vec![bytes_column(0, None, &[b"a", b"b"])],
    };
    // Absent column -> all true.
    assert_eq!(byte_eq_mask(&batch, 99, 1), vec![true, true]);
    // Present but not fixed-1 -> all true.
    assert_eq!(byte_eq_mask(&batch, 0, 1), vec![true, true]);
}

#[test]
fn filter_batch_compacts_fixed_data_and_validity() {
    // Fixed-1 column with row 1 null; keep rows 0 and 2.
    let batch = ColumnBatch {
        row_count: 3,
        columns: vec![Column {
            column_id: 2,
            type_tag: TypeTag::Fixed(1),
            validity: Some(vec![0b0000_0101]),
            data: vec![10, 20, 30],
        }],
    };
    let filtered = filter_batch(&batch, &[true, false, true]);
    assert_eq!(filtered.row_count, 2);
    let col = &filtered.columns[0];
    assert_eq!(col.data, vec![10, 30], "fixed data keeps rows 0 and 2");
    // Both kept rows were valid, compacted to the low two bits.
    assert_eq!(col.validity, Some(vec![0b0000_0011]));
}
