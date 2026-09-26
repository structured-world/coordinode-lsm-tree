use super::{
    Column, ColumnBatch, ColumnRangePredicate, ColumnStats, PredicateApply, PredicateSupport,
    TypeTag, byte_eq_mask, byte_eq_scalar, filter_batch, take_rows,
};
use crate::table::columnar::{
    ByteOrder, Number, NumberKind, column_batch_to_entries, entries_to_column_batch,
};
use crate::{Slice, ValueType, key::InternalKey, value::InternalValue};
use proptest::prelude::*;

/// A filtering predicate over `column_id`.
fn filter(column_id: u16, lower: Option<Vec<u8>>, upper: Option<Vec<u8>>) -> ColumnRangePredicate {
    ColumnRangePredicate {
        column_id,
        lower,
        upper,
        apply: PredicateApply::Filter,
    }
}

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
    let pred = filter(0, Some(b"m".to_vec()), Some(b"z".to_vec()));
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
    // Column 0 is the user-key column.
    let pred = filter(0, Some(b"b".to_vec()), None);
    assert_eq!(pred.matching_rows(&batch), vec![false, true]);
}

#[test]
fn matching_rows_all_true_when_column_absent() {
    let batch = entries_to_column_batch(&[entry(b"k", 1, b"v")]).expect("transpose");
    // No column 99 in the batch -> cannot filter, every row matches, and the
    // predicate says it did not run.
    let pred = filter(99, Some(b"z".to_vec()), None);
    assert_eq!(pred.matching_rows(&batch), vec![true]);
    assert_eq!(pred.support_in(&batch), PredicateSupport::Unsupported);
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
    let filtered = filter_batch(&batch, &[true, false, true]).expect("filter");
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
            data: data.clone().into(),
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
        data: data.into(),
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
    let pred = filter(0, Some(b"a".to_vec()), Some(b"z".to_vec()));
    assert_eq!(pred.matching_rows(&batch), vec![true, false, true]);
}

#[test]
fn matching_rows_all_true_for_an_opaque_fixed_column() {
    // An opaque fixed-width column has no order, so the filter cannot run:
    // every row passes and the predicate reports it did not run, which is
    // what tells this mask apart from one where every row matched.
    let batch = ColumnBatch {
        row_count: 2,
        columns: vec![Column {
            column_id: 1,
            type_tag: TypeTag::Fixed(8),
            validity: None,
            data: vec![0u8; 16].into(),
        }],
    };
    let pred = filter(1, Some(vec![5]), None);
    assert_eq!(pred.matching_rows(&batch), vec![true, true]);
    assert_eq!(pred.support_in(&batch), PredicateSupport::Unsupported);
    // No statistics describe it either, so no block is skipped on it.
    assert!(batch.zone_stats().is_empty());
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
            data: vec![10, 20, 30].into(),
        }],
    };
    let filtered = filter_batch(&batch, &[true, false, true]).expect("filter");
    assert_eq!(filtered.row_count, 2);
    let col = &filtered.columns[0];
    assert_eq!(col.data, vec![10, 30], "fixed data keeps rows 0 and 2");
    // Both kept rows were valid, compacted to the low two bits.
    assert_eq!(col.validity, Some(vec![0b0000_0011]));
}

#[test]
fn take_rows_repeats_a_bytes_value_and_keeps_offsets_monotonic() {
    // A gather may list the same index more than once. The Bytes value must be
    // emitted once per occurrence and the offset table stay monotonic — the
    // accumulator adds each repeat's length (checked, so a genuinely oversized
    // gather errors cleanly rather than wrapping the u32 table into a desynced
    // frame; a normal gather like this one never approaches the limit).
    let batch = entries_to_column_batch(&[
        entry(b"k0", 3, b"aaa"),
        entry(b"k1", 2, b"bb"),
        entry(b"k2", 1, b"c"),
    ])
    .expect("transpose");

    // Gather rows [0, 0, 2]: row 0 ("aaa") twice, then row 2 ("c").
    let taken = take_rows(&batch, &[0, 0, 2]).expect("gather within the u32 offset limit");
    assert_eq!(taken.row_count, 3);

    let entries = column_batch_to_entries(&taken).expect("transpose back");
    let values: Vec<&[u8]> = entries.iter().map(|e| e.value.as_ref()).collect();
    assert_eq!(
        values,
        vec![&b"aaa"[..], &b"aaa"[..], &b"c"[..]],
        "the repeated index emits its value each time, framed by a monotonic offset table",
    );
}

/// Every number descriptor the format has, both byte orders.
fn numbers() -> Vec<Number> {
    let mut out = Vec::new();
    for (kind, widths) in [
        (NumberKind::Unsigned, &[1u8, 2, 4, 8, 16][..]),
        (NumberKind::Signed, &[1, 2, 4, 8, 16][..]),
        (NumberKind::Float, &[4, 8][..]),
    ] {
        for &width in widths {
            for order in [ByteOrder::Little, ByteOrder::Big] {
                out.push(Number::new(kind, width, order).expect("a width the kind has"));
            }
        }
    }
    out
}

/// A value of `number` from `raw` (most significant byte first, at least
/// `width` bytes), stored in the number's byte order. For a float, `special`
/// below 6 picks a NaN, an infinity or a zero of either sign instead, so the
/// order is exercised where it departs from numeric comparison.
fn native(number: Number, raw: &[u8], special: u8) -> Vec<u8> {
    let width = usize::from(number.width());
    let mut be = if number.kind() == NumberKind::Float && special < 6 {
        match (width, special) {
            (4, 0) => f32::NAN.to_bits().to_be_bytes().to_vec(),
            (4, 1) => (-f32::NAN).to_bits().to_be_bytes().to_vec(),
            (4, 2) => f32::INFINITY.to_bits().to_be_bytes().to_vec(),
            (4, 3) => f32::NEG_INFINITY.to_bits().to_be_bytes().to_vec(),
            (4, 4) => 0.0f32.to_bits().to_be_bytes().to_vec(),
            (4, _) => (-0.0f32).to_bits().to_be_bytes().to_vec(),
            (_, 0) => f64::NAN.to_bits().to_be_bytes().to_vec(),
            (_, 1) => (-f64::NAN).to_bits().to_be_bytes().to_vec(),
            (_, 2) => f64::INFINITY.to_bits().to_be_bytes().to_vec(),
            (_, 3) => f64::NEG_INFINITY.to_bits().to_be_bytes().to_vec(),
            (_, 4) => 0.0f64.to_bits().to_be_bytes().to_vec(),
            (_, _) => (-0.0f64).to_bits().to_be_bytes().to_vec(),
        }
    } else {
        raw[..width].to_vec()
    };
    if number.order() == ByteOrder::Little {
        be.reverse();
    }
    be
}

/// How two values of `number` compare as the numbers they are, computed from
/// the standard library's own types: the oracle the ordinal is checked
/// against. A float compares by `total_cmp`, which is IEEE 754 totalOrder.
fn numeric_cmp(number: Number, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
    let be = |v: &[u8]| {
        let mut v = v.to_vec();
        if number.order() == ByteOrder::Little {
            v.reverse();
        }
        let mut wide = [0u8; 16];
        wide[16 - v.len()..].copy_from_slice(&v);
        u128::from_be_bytes(wide)
    };
    let (a, b) = (be(a), be(b));
    let bits = 8 * u32::from(number.width());
    match number.kind() {
        NumberKind::Unsigned => a.cmp(&b),
        NumberKind::Signed => {
            // Sign-extend from the column's width.
            let signed = |v: u128| (v << (128 - bits)).cast_signed() >> (128 - bits);
            signed(a).cmp(&signed(b))
        }
        NumberKind::Float if bits == 32 => {
            let f = |v: u128| f32::from_bits(u32::try_from(v).expect("a 4-byte value"));
            f(a).total_cmp(&f(b))
        }
        NumberKind::Float => {
            let f = |v: u128| f64::from_bits(u64::try_from(v).expect("an 8-byte value"));
            f(a).total_cmp(&f(b))
        }
    }
}

/// One number column over `values` with the null rows `nulls` marks, and the
/// same values as a `Bytes` column of their comparable encodings: the column a
/// caller would store to filter numbers before the engine could.
fn number_and_bytes(
    number: Number,
    values: &[Vec<u8>],
    nulls: &[bool],
) -> (ColumnBatch, ColumnBatch) {
    let rows = u32::try_from(values.len()).expect("few rows");
    let validity = nulls.iter().any(|&n| n).then(|| {
        let mut bits = vec![0u8; values.len().div_ceil(8)];
        for (i, &null) in nulls.iter().enumerate() {
            if !null {
                bits[i / 8] |= 1 << (i % 8);
            }
        }
        bits
    });
    let comparable: Vec<Vec<u8>> = values
        .iter()
        .map(|v| number.comparable(v).expect("a value of the column's width"))
        .collect();
    let refs: Vec<&[u8]> = comparable.iter().map(Vec::as_slice).collect();
    let numbers = ColumnBatch {
        row_count: rows,
        columns: vec![Column {
            column_id: 5,
            type_tag: TypeTag::Number(number),
            validity: validity.clone(),
            data: values.concat().into(),
        }],
    };
    let bytes = ColumnBatch {
        row_count: rows,
        columns: vec![bytes_column(5, validity, &refs)],
    };
    (numbers, bytes)
}

/// A descriptor, values of it, their null marks, and two optional bounds.
type NumberCase = (
    Number,
    Vec<Vec<u8>>,
    Vec<bool>,
    Option<Vec<u8>>,
    Option<Vec<u8>>,
);

/// Strategy: a descriptor, rows of it, their null marks, and two optional
/// bounds of any length around the column's width.
fn number_case() -> impl Strategy<Value = NumberCase> {
    let bound = || proptest::option::of(proptest::collection::vec(any::<u8>(), 0..=18));
    (
        proptest::sample::select(numbers()),
        proptest::collection::vec(
            (
                proptest::collection::vec(any::<u8>(), 16),
                0u8..12,
                any::<bool>(),
            ),
            1..40,
        ),
        any::<bool>(),
        bound(),
        bound(),
    )
        .prop_map(|(number, rows, all_null, lower, upper)| {
            let values = rows
                .iter()
                .map(|(raw, s, _)| native(number, raw, *s))
                .collect();
            let nulls = rows.iter().map(|(_, _, null)| all_null || *null).collect();
            (number, values, nulls, lower, upper)
        })
}

proptest! {
    // The comparable encoding orders every kind of number the way the numbers
    // themselves order: an ordinal disagreeing with the oracle would let the
    // filter and the statistics mis-order a column.
    #[test]
    fn a_number_orders_by_its_comparable_encoding(
        number in proptest::sample::select(numbers()),
        a in proptest::collection::vec(any::<u8>(), 16),
        b in proptest::collection::vec(any::<u8>(), 16),
        sa in 0u8..12,
        sb in 0u8..12,
    ) {
        let (a, b) = (native(number, &a, sa), native(number, &b, sb));
        let comparable = |v: &[u8]| number.comparable(v).expect("a value of the width");
        let by_bytes = comparable(&a).cmp(&comparable(&b));
        prop_assert_eq!(by_bytes, numeric_cmp(number, &a, &b));
    }

    // A range over a number column returns exactly the rows the same range
    // returns over those values stored as order-preserving bytes, for every
    // descriptor, NaN and signed zeros, nulls and all-null columns, and bounds
    // of any length.
    #[test]
    fn a_number_predicate_returns_the_rows_of_its_bytes_twin(
        (number, values, nulls, lower, upper) in number_case(),
    ) {
        let (numbers, bytes) = number_and_bytes(number, &values, &nulls);
        let pred = filter(5, lower, upper);
        prop_assert_eq!(pred.matching_rows(&numbers), pred.matching_rows(&bytes));
        prop_assert_eq!(pred.support_in(&numbers), PredicateSupport::Exact);
    }

    // A number block is skipped only when none of its rows matches: its
    // statistics are its bytes twin's, so the skip is exactly as safe.
    #[test]
    fn a_number_block_is_skipped_only_when_no_row_matches(
        (number, values, nulls, lower, upper) in number_case(),
    ) {
        let (numbers, bytes) = number_and_bytes(number, &values, &nulls);
        let stats = numbers.zone_stats();
        let twin = bytes.zone_stats();
        prop_assert_eq!(stats.len(), 1);
        prop_assert_eq!((&stats[0].min, &stats[0].max, stats[0].null_count),
            (&twin[0].min, &twin[0].max, twin[0].null_count));
        let pred = filter(5, lower, upper);
        if pred.can_skip_block(&stats) {
            prop_assert!(!pred.matching_rows(&numbers).contains(&true));
        }
    }
}

#[test]
fn support_is_reported_for_each_outcome() {
    // Exact: an ordered column, filtered. Prune-only: the same column, the
    // scan asked only to prune. Unsupported: an opaque fixed column, or none.
    let number = Some(TypeTag::Number(Number::U64_LE));
    let exact = filter(5, None, None);
    let prune = ColumnRangePredicate {
        apply: PredicateApply::Prune,
        ..exact.clone()
    };
    assert_eq!(exact.support(number), PredicateSupport::Exact);
    assert_eq!(exact.support(Some(TypeTag::Bytes)), PredicateSupport::Exact);
    assert_eq!(prune.support(number), PredicateSupport::PruneOnly);
    assert_eq!(
        exact.support(Some(TypeTag::Fixed(8))),
        PredicateSupport::Unsupported
    );
    assert_eq!(exact.support(None), PredicateSupport::Unsupported);
    // Folding segments' answers keeps the weakest.
    assert_eq!(
        PredicateSupport::Exact.min(PredicateSupport::PruneOnly),
        PredicateSupport::PruneOnly
    );
    assert_eq!(
        PredicateSupport::PruneOnly.min(PredicateSupport::Unsupported),
        PredicateSupport::Unsupported
    );
}

#[test]
fn a_bound_off_the_column_width_is_a_point_in_byte_order() {
    // A u16 column holding 0x0100, 0x0101 and 0x0200. A one-byte upper bound
    // [0x01] sits below every two-byte value starting 0x01, so it admits
    // none of them; a three-byte lower bound [0x01, 0x00, 0x00] sits above
    // 0x0100, so it admits 0x0101 onwards.
    let number = Number::new(NumberKind::Unsigned, 2, ByteOrder::Big).expect("a u16");
    let values = [vec![1, 0], vec![1, 1], vec![2, 0]];
    let (numbers, bytes) = number_and_bytes(number, &values, &[false; 3]);
    let upper = filter(5, None, Some(vec![1]));
    assert_eq!(upper.matching_rows(&numbers), vec![false, false, false]);
    assert_eq!(upper.matching_rows(&bytes), vec![false, false, false]);
    let lower = filter(5, Some(vec![1, 0, 0]), None);
    assert_eq!(lower.matching_rows(&numbers), vec![false, true, true]);
    assert_eq!(lower.matching_rows(&bytes), vec![false, true, true]);
}
