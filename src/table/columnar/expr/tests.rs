use super::{
    Choice, Expression, Ints, MAX_DEPTH, Values, candidates, choose, int_trials, put_varint,
    values_trials, zigzag,
};
use crate::Slice;
use crate::table::columnar::{ByteOrder, Number, NumberKind, TypeTag, frame_bytes_column};
use alloc::vec::Vec;
use proptest::prelude::*;

/// A bytes column's layout for `cells`.
fn bytes_layout(cells: &[&[u8]]) -> Vec<u8> {
    frame_bytes_column(cells.len(), || cells.iter().copied())
        .expect("frame")
        .to_vec()
}

/// A number column's layout for `values`, each stored in `number`'s width and
/// order from its low bytes.
fn number_layout(number: Number, values: &[u128]) -> Vec<u8> {
    let width = usize::from(number.width());
    let mut out = Vec::with_capacity(values.len() * width);
    for &value in values {
        let le = value.to_le_bytes();
        let low = &le[..width];
        match number.order() {
            ByteOrder::Little => out.extend_from_slice(low),
            ByteOrder::Big => out.extend(low.iter().rev()),
        }
    }
    out
}

/// Decodes `bytes` as `rows` rows of `type_tag` through a page of its own, as
/// a read does.
fn decode(type_tag: TypeTag, rows: u32, bytes: &[u8]) -> crate::Result<Slice> {
    let page = Slice::from(bytes);
    Values::parse(type_tag, rows, &page)?.materialize(type_tag, rows, &page)
}

/// Every candidate the writer considers for `data` decodes back to `data`, and
/// the chosen one is the cheapest of them.
fn assert_every_candidate_round_trips(type_tag: TypeTag, rows: u32, data: &[u8]) {
    let cells = super::Cells {
        type_tag,
        rows,
        data,
    };
    let trials = values_trials(&cells).expect("trials");
    assert!(!trials.is_empty(), "plain is always a candidate");
    for trial in &trials {
        let decoded = decode(type_tag, rows, &trial.bytes)
            .unwrap_or_else(|e| panic!("{} does not decode: {e:?}", trial.expression));
        assert_eq!(&*decoded, data, "{} does not round-trip", trial.expression);
        let parsed = Values::parse(type_tag, rows, &trial.bytes).expect("parse");
        assert_eq!(
            parsed.describe().to_string(),
            describe_stored(&trial.expression)
        );
        // A point read takes single rows from the encoding; each must be the
        // row the layout holds, and a row past the column must be refused.
        let access = parsed.rows(type_tag, rows).expect("rows");
        for row in 0..rows {
            let got = access
                .get(type_tag, rows, row)
                .unwrap_or_else(|e| panic!("{} row {row}: {e:?}", trial.expression));
            let want = super::cell(type_tag, data, rows, row).expect("layout row");
            assert_eq!(&*got, want, "{} row {row} differs", trial.expression);
        }
        assert!(
            access.get(type_tag, rows, rows).is_err(),
            "{} serves a row past the column",
            trial.expression
        );
    }
    let Choice { bytes, .. } = choose(type_tag, rows, data).expect("choose");
    let candidates = candidates(type_tag, rows, data).expect("candidates");
    let cheapest = candidates
        .iter()
        .map(|c| c.cost)
        .min()
        .expect("a candidate");
    assert_eq!(
        candidates
            .iter()
            .find(|c| c.cost == cheapest)
            .map(|c| c.bytes),
        Some(bytes.len()),
        "the chosen encoding is the cheapest candidate",
    );
}

/// What a read describes a stored expression as: a run's ends are decoded on
/// the parse and not described again.
fn describe_stored(expression: &Expression) -> String {
    fn stored(e: &Expression) -> Expression {
        match e {
            Expression::Rle { values, .. } => Expression::Rle {
                values: Box::new(stored(values)),
                ends: Box::new(Expression::Plain),
            },
            Expression::Dict { values, codes } => Expression::Dict {
                values: Box::new(stored(values)),
                codes: Box::new(stored(codes)),
            },
            Expression::Ordinals(inner) => Expression::Ordinals(Box::new(stored(inner))),
            Expression::Delta(inner) => Expression::Delta(Box::new(stored(inner))),
            Expression::Lengths(inner) => Expression::Lengths(Box::new(stored(inner))),
            other => other.clone(),
        }
    }
    stored(expression).to_string()
}

/// Every integer encoding the writer considers decodes back to `values`.
fn assert_every_int_trial_round_trips(values: &[u64]) {
    let n = u32::try_from(values.len()).expect("rows");
    for trial in int_trials(values, false) {
        let mut rest = &trial.bytes[..];
        let ints = Ints::parse(&mut rest, n, 0)
            .unwrap_or_else(|e| panic!("{} does not parse: {e:?}", trial.expression));
        assert!(rest.is_empty(), "{} leaves bytes over", trial.expression);
        let mut out = Vec::new();
        ints.decode_into(n, &mut out).expect("decode");
        assert_eq!(out, values, "{} does not round-trip", trial.expression);
    }
}

fn numbers() -> impl Strategy<Value = Number> {
    (
        prop_oneof![
            Just(NumberKind::Unsigned),
            Just(NumberKind::Signed),
            Just(NumberKind::Float)
        ],
        prop_oneof![Just(1u8), Just(2), Just(4), Just(8), Just(16)],
        prop_oneof![Just(ByteOrder::Little), Just(ByteOrder::Big)],
    )
        .prop_filter_map("a width the kind has", |(kind, width, order)| {
            Number::new(kind, width, order).ok()
        })
}

/// Values of `number` drawn so that runs, repeats, narrow ranges and the
/// type's boundaries all occur: a small set of distinct raw values, some of
/// them the extremes, repeated in runs.
fn number_values(number: Number) -> impl Strategy<Value = Vec<u128>> {
    let max = number.max_ordinal();
    let pool = prop_oneof![
        Just(0u128),
        Just(max),
        Just(max >> 1),
        Just((max >> 1) + 1),
        (0u128..=7),
        any::<u128>().prop_map(move |v| v & max),
    ];
    proptest::collection::vec(proptest::collection::vec(pool, 1..4), 1..40).prop_map(|groups| {
        groups
            .into_iter()
            .flat_map(|g| {
                let times = 1 + (g.len() % 3);
                core::iter::repeat_n(g, times).flatten()
            })
            .collect()
    })
}

proptest! {
    /// Every encoding of a number column round-trips byte-exactly, over every
    /// kind, width and byte order, boundary values and runs included.
    #[test]
    fn number_columns_round_trip_under_every_candidate(
        (number, values) in numbers().prop_flat_map(|n| (Just(n), number_values(n)))
    ) {
        let rows = u32::try_from(values.len()).expect("rows");
        let data = number_layout(number, &values);
        assert_every_candidate_round_trips(TypeTag::Number(number), rows, &data);
    }

    /// Every encoding of an opaque fixed column round-trips byte-exactly.
    #[test]
    fn opaque_columns_round_trip_under_every_candidate(
        width in 1u8..=12,
        picks in proptest::collection::vec(0usize..4, 1..60),
        seed in any::<u64>(),
    ) {
        let pool: Vec<Vec<u8>> = (0..4u64)
            .map(|i| {
                (0..width)
                    .map(|b| (seed.rotate_left(u32::from(b)).wrapping_mul(i + 1) >> 7).to_le_bytes()[0])
                    .collect()
            })
            .collect();
        let data: Vec<u8> = picks.iter().flat_map(|&p| pool[p].clone()).collect();
        let rows = u32::try_from(picks.len()).expect("rows");
        assert_every_candidate_round_trips(TypeTag::Fixed(width), rows, &data);
    }

    /// Every encoding of a bytes column round-trips byte-exactly, empty cells
    /// and repeats included.
    #[test]
    fn bytes_columns_round_trip_under_every_candidate(
        cells in proptest::collection::vec(
            prop_oneof![
                Just(Vec::new()),
                Just(b"a".to_vec()),
                Just(b"same".to_vec()),
                proptest::collection::vec(any::<u8>(), 0..40),
            ],
            1..50,
        )
    ) {
        let refs: Vec<&[u8]> = cells.iter().map(Vec::as_slice).collect();
        let data = bytes_layout(&refs);
        let rows = u32::try_from(cells.len()).expect("rows");
        assert_every_candidate_round_trips(TypeTag::Bytes, rows, &data);
    }

    /// Every integer encoding round-trips, whatever the spread of the values.
    #[test]
    fn integer_vectors_round_trip_under_every_candidate(
        values in proptest::collection::vec(
            prop_oneof![Just(0u64), Just(u64::MAX), 0u64..16, any::<u64>()],
            1..80,
        )
    ) {
        assert_every_int_trial_round_trips(&values);
    }
}

/// One row, of each type, round-trips: the smallest page a writer emits.
#[test]
fn a_single_row_round_trips_for_every_type() {
    let u32_le = Number::new(NumberKind::Unsigned, 4, ByteOrder::Little).expect("number");
    assert_every_candidate_round_trips(TypeTag::Number(u32_le), 1, &7u32.to_le_bytes());
    assert_every_candidate_round_trips(TypeTag::Fixed(3), 1, &[1, 2, 3]);
    assert_every_candidate_round_trips(TypeTag::Bytes, 1, &bytes_layout(&[b"x"]));
    assert_every_candidate_round_trips(TypeTag::Bytes, 1, &bytes_layout(&[b""]));
}

/// A float column's zeros, infinities and NaNs round-trip exactly, sign and
/// payload bits included: the ordinal mapping must invert on every one.
#[test]
fn float_specials_round_trip_bit_for_bit() {
    let f64_le = Number::new(NumberKind::Float, 8, ByteOrder::Little).expect("number");
    let values = [
        0.0f64.to_bits(),
        (-0.0f64).to_bits(),
        f64::INFINITY.to_bits(),
        f64::NEG_INFINITY.to_bits(),
        f64::NAN.to_bits(),
        (-f64::NAN).to_bits(),
        0x7ff0_0000_0000_0001,
        1.5f64.to_bits(),
        (-1.5f64).to_bits(),
    ];
    let data: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    assert_every_candidate_round_trips(TypeTag::Number(f64_le), 9, &data);
}

/// Values `0..=7` with one row of `1000` encode at the three bits the rest
/// need, the outlier patched in as one exception, and decode to the original.
#[test]
fn one_outlier_is_an_exception_not_a_wider_width() {
    let mut values: Vec<u64> = (0..64).map(|i| i % 8).collect();
    values[37] = 1000;
    let trial = super::ffor(&values);
    assert_eq!(
        trial.expression,
        Expression::Ffor {
            bit_width: 3,
            exceptions: 1
        },
    );
    assert_every_int_trial_round_trips(&values);
}

/// A vector of values no common width holds, every one of them wide, is not
/// encoded as exceptions everywhere: FFOR falls back to the full width with
/// none, and the column as a whole stays plain.
#[test]
fn a_column_of_outliers_falls_back_rather_than_degenerating() {
    // splitmix64: neither the values nor their differences share a width.
    let values: Vec<u64> = (0..32u64)
        .map(|i| {
            let mut z = i.wrapping_add(1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        })
        .collect();
    let trial = super::ffor(&values);
    let Expression::Ffor { exceptions, .. } = trial.expression else {
        panic!("an FFOR trial");
    };
    assert!(
        exceptions * 4 < 32,
        "most rows fit the chosen width: {exceptions} exceptions of 32",
    );
    let data: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    let chosen = choose(TypeTag::Number(Number::U64_LE), 32, &data).expect("choose");
    assert_eq!(chosen.expression, Expression::Plain);
}

/// A dictionary of four one-byte rows holds fewer value bytes than the plain
/// column, but its fields (the operator, the size, the codes' base and width)
/// make the page larger, so the plain form is chosen. The candidates report
/// that overhead.
#[test]
fn per_unit_overhead_decides_against_a_smaller_payload() {
    let data = [5u8, 5, 6, 6];
    let chosen = choose(TypeTag::Fixed(1), 4, &data).expect("choose");
    assert_eq!(chosen.expression, Expression::Plain);
    let considered = candidates(TypeTag::Fixed(1), 4, &data).expect("candidates");
    let plain = &considered[0];
    assert_eq!(plain.expression, Expression::Plain);
    let dict = considered
        .iter()
        .find(|c| matches!(c.expression, Expression::Dict { .. }))
        .expect("a dictionary candidate");
    assert!(
        dict.bytes - dict.overhead < plain.bytes - plain.overhead,
        "the dictionary holds fewer value bytes ({dict:?} against {plain:?})",
    );
    assert!(
        dict.bytes > plain.bytes,
        "but a larger page once its fields are counted"
    );
}

/// A column of one repeated value is a constant; a sorted low-cardinality
/// bytes column is a dictionary or runs, never plain.
#[test]
fn repetition_is_encoded_as_a_constant_runs_or_a_dictionary() {
    let one = bytes_layout(&[&b"status-ok"[..]; 50]);
    assert_eq!(
        choose(TypeTag::Bytes, 50, &one).expect("choose").expression,
        Expression::Constant
    );
    let cells: Vec<&[u8]> = (0..60)
        .map(|i| [&b"alpha"[..], b"bravo", b"charlie"][i % 3])
        .collect();
    let mixed = bytes_layout(&cells);
    assert!(matches!(
        choose(TypeTag::Bytes, 60, &mixed)
            .expect("choose")
            .expression,
        Expression::Dict { .. }
    ));
    let runs: Vec<&[u8]> = (0..60)
        .map(|i| [&b"alpha"[..], b"bravo", b"charlie"][i / 20])
        .collect();
    let sorted = bytes_layout(&runs);
    assert!(matches!(
        choose(TypeTag::Bytes, 60, &sorted)
            .expect("choose")
            .expression,
        Expression::Rle { .. } | Expression::Dict { .. }
    ));
}

/// A dictionary's values are stored in the column's order, which for a
/// signed number is numeric rather than byte-wise: a range over the values is
/// then a range over the codes.
#[test]
fn a_dictionary_is_sorted_in_the_column_order() {
    let i16_le = Number::new(NumberKind::Signed, 2, ByteOrder::Little).expect("number");
    let raw: Vec<i16> = (0..40).map(|i| [-3i16, 200, -300, 7][i % 4]).collect();
    let data: Vec<u8> = raw.iter().flat_map(|v| v.to_le_bytes()).collect();
    let cells = super::Cells {
        type_tag: TypeTag::Number(i16_le),
        rows: 40,
        data: &data,
    };
    let dict = values_trials(&cells)
        .expect("trials")
        .into_iter()
        .find(|t| matches!(t.expression, Expression::Dict { .. }))
        .expect("a dictionary candidate");
    let page = Slice::from(dict.bytes);
    let Values::Dict { values, size, .. } =
        Values::parse(TypeTag::Number(i16_le), 40, &page).expect("parse")
    else {
        panic!("a dictionary");
    };
    let dictionary = values
        .materialize(TypeTag::Number(i16_le), size, &page)
        .expect("dictionary");
    let decoded: Vec<i16> = dictionary
        .chunks_exact(2)
        .map(|c| i16::from_le_bytes([c[0], c[1]]))
        .collect();
    assert_eq!(decoded, [-300, -3, 7, 200]);
}

// --- Refusals ------------------------------------------------------------------

/// An FFOR expression's bytes for `n` values, built by hand.
fn ffor_bytes(base: u64, bit_width: u8, packed: &[u8], exceptions: &[(u64, u64)]) -> Vec<u8> {
    let mut out = vec![super::FFOR];
    put_varint(&mut out, base);
    out.push(bit_width);
    out.extend_from_slice(packed);
    put_varint(&mut out, exceptions.len() as u64);
    for &(gap, value) in exceptions {
        put_varint(&mut out, gap);
        put_varint(&mut out, value);
    }
    out
}

/// `bytes` is refused by a decode and by a point read: the one-row access
/// either fails to prepare or fails on some row, never serving one silently.
fn assert_refused(type_tag: TypeTag, rows: u32, bytes: &[u8], what: &str) {
    let result = decode(type_tag, rows, bytes);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader(_))),
        "{what} must be refused, got {result:?}",
    );
    let read = Values::parse(type_tag, rows, bytes).and_then(|values| {
        let access = values.rows(type_tag, rows)?;
        (0..rows).try_for_each(|row| access.get(type_tag, rows, row).map(drop))
    });
    assert!(
        matches!(read, Err(crate::Error::InvalidHeader(_))),
        "{what} must be refused by a point read, got {read:?}",
    );
}

#[test]
fn an_unknown_operator_is_refused_not_misread() {
    for op in [8u8, 0x7F, 0xFF] {
        assert_refused(TypeTag::Fixed(1), 1, &[op, 0], "an unknown values operator");
        let mut bytes = vec![super::ORDINALS, op];
        bytes.extend_from_slice(&[0; 8]);
        assert_refused(
            TypeTag::Number(Number::U64_LE),
            1,
            &bytes,
            "an unknown integer operator",
        );
    }
}

#[test]
fn an_operator_the_type_has_no_use_for_is_refused() {
    // Ordinals on an opaque column, lengths on a fixed one, ordinals on a
    // 16-byte number, and a dictionary nested in a dictionary's values.
    assert_refused(
        TypeTag::Fixed(8),
        1,
        &[super::ORDINALS, super::CONSTANT, 1],
        "ordinals of an opaque column",
    );
    assert_refused(
        TypeTag::Fixed(1),
        1,
        &[super::LENGTHS, super::CONSTANT, 1, 1, 9],
        "lengths of a fixed column",
    );
    let u128_le = Number::new(NumberKind::Unsigned, 16, ByteOrder::Little).expect("number");
    assert_refused(
        TypeTag::Number(u128_le),
        1,
        &[super::ORDINALS, super::CONSTANT, 1],
        "ordinals of a 16-byte number",
    );
    assert_refused(
        TypeTag::Fixed(1),
        2,
        &[
            super::DICT,
            1,
            super::DICT,
            1,
            super::PLAIN,
            9,
            super::CONSTANT,
            0,
            super::CONSTANT,
            0,
        ],
        "a dictionary inside a dictionary's values",
    );
}

#[test]
fn an_expression_nested_past_the_limit_is_refused() {
    let mut bytes = vec![super::ORDINALS];
    bytes.extend(core::iter::repeat_n(
        super::DELTA,
        usize::from(MAX_DEPTH) + 1,
    ));
    bytes.extend_from_slice(&[super::CONSTANT, 0]);
    assert_refused(
        TypeTag::Number(Number::U64_LE),
        1,
        &bytes,
        "a delta chain past the depth limit",
    );
}

#[test]
fn a_code_past_its_dictionary_is_refused() {
    // Two values, and a constant code of 2 for both rows.
    let bytes = [super::DICT, 2, super::PLAIN, 1, 2, super::CONSTANT, 2];
    assert_refused(TypeTag::Fixed(1), 2, &bytes, "a code past the dictionary");
}

#[test]
fn run_ends_out_of_order_or_short_of_the_rows_are_refused() {
    // Two runs of a fixed(1) column over three rows: ends [2, 1] decrease,
    // ends [1, 2] stop short, ends [2, 4] run past.
    for ends in [[2u8, 1], [1, 2], [2, 4]] {
        let mut bytes = vec![super::RLE, 2, super::PLAIN, 7, 8];
        bytes.extend_from_slice(&ffor_bytes(0, 8, &ends, &[]));
        assert_refused(
            TypeTag::Fixed(1),
            3,
            &bytes,
            "run ends that do not cover the rows",
        );
    }
}

#[test]
fn exception_positions_past_the_rows_or_repeated_are_refused() {
    let past = ffor_bytes(0, 0, &[], &[(4, 9)]);
    let mut bytes = vec![super::ORDINALS];
    bytes.extend_from_slice(&past);
    assert_refused(
        TypeTag::Number(Number::U64_LE),
        4,
        &bytes,
        "an exception past the last row",
    );
    // Positions 1 then 1 + 1 + u64::MAX overflow rather than wrap.
    let overflowing = ffor_bytes(0, 0, &[], &[(1, 9), (u64::MAX, 9)]);
    let mut bytes = vec![super::ORDINALS];
    bytes.extend_from_slice(&overflowing);
    assert_refused(
        TypeTag::Number(Number::U64_LE),
        4,
        &bytes,
        "an exception position that overflows",
    );
}

#[test]
fn an_ordinal_no_value_of_the_width_has_is_refused() {
    let u8_le = Number::new(NumberKind::Unsigned, 1, ByteOrder::Little).expect("number");
    assert_refused(
        TypeTag::Number(u8_le),
        1,
        &[super::ORDINALS, super::CONSTANT, 0x80, 0x02],
        "ordinal 256 in a one-byte column",
    );
}

#[test]
fn an_offset_past_the_base_s_range_is_refused() {
    // Base u64::MAX plus an offset of 1 names no u64.
    let mut bytes = vec![super::ORDINALS];
    bytes.extend_from_slice(&ffor_bytes(u64::MAX, 1, &[1], &[]));
    assert_refused(
        TypeTag::Number(Number::U64_LE),
        1,
        &bytes,
        "an offset past u64::MAX",
    );
}

#[test]
fn lengths_that_do_not_fill_the_payload_are_refused() {
    // Lengths 2 and 2 over a 5-byte payload, then over a 3-byte one.
    for payload in [&b"abcde"[..], b"abc"] {
        let mut bytes = vec![super::LENGTHS, super::CONSTANT, 2];
        put_varint(&mut bytes, payload.len() as u64);
        bytes.extend_from_slice(payload);
        assert_refused(TypeTag::Bytes, 2, &bytes, "lengths that miss the payload");
    }
}

#[test]
fn a_plain_bytes_column_with_bad_offsets_is_refused() {
    let mut data = bytes_layout(&[b"hi", b"abc"]);
    data[0] = 1; // the first offset must be zero
    let mut bytes = vec![super::PLAIN];
    bytes.extend_from_slice(&data);
    assert_refused(TypeTag::Bytes, 2, &bytes, "a first offset past zero");
}

#[test]
fn a_bit_width_past_64_and_truncated_bytes_are_refused() {
    let mut bytes = vec![super::ORDINALS];
    bytes.extend_from_slice(&ffor_bytes(0, 65, &[0; 9], &[]));
    assert_refused(TypeTag::Number(Number::U64_LE), 1, &bytes, "a 65-bit width");
    let full = {
        let mut b = vec![super::ORDINALS];
        b.extend_from_slice(&ffor_bytes(0, 8, &[1, 2, 3, 4], &[]));
        b
    };
    for cut in 0..full.len() {
        assert_refused(
            TypeTag::Number(Number::U64_LE),
            4,
            &full[..cut],
            "a truncated encoding",
        );
    }
    assert_eq!(
        &*decode(TypeTag::Number(Number::U64_LE), 4, &full).expect("whole"),
        &[1u64, 2, 3, 4]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<_>>()[..],
    );
}

#[test]
fn bytes_left_after_the_expression_are_refused() {
    assert_refused(
        TypeTag::Fixed(1),
        1,
        &[super::PLAIN, 9, 0],
        "a trailing byte",
    );
}

#[test]
fn zigzag_inverts_on_every_boundary() {
    for v in [0u64, 1, u64::MAX, u64::MAX / 2, (u64::MAX / 2) + 1] {
        assert_eq!(super::unzigzag(zigzag(v)), v);
    }
}
