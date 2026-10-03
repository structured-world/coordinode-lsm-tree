use super::*;
use crate::table::column_type::{ByteOrder, Number, NumberKind};
use crate::vlog::ValueHandle;
use test_log::test;

fn indirection(blob_file_id: u64, offset: u64, size: u32) -> BlobIndirection {
    BlobIndirection {
        vhandle: ValueHandle {
            blob_file_id,
            offset,
            on_disk_size: size,
        },
        size,
    }
}

fn reference(indirection: BlobIndirection) -> RowCell<'static> {
    RowCell::Ref {
        indirection,
        owner: false,
    }
}

fn u64_le() -> TypeTag {
    TypeTag::Number(Number::new(NumberKind::Unsigned, 8, ByteOrder::Little).unwrap())
}

/// The first field id, under the name the tests use for it.
const C: u16 = FIRST_FIELD_COLUMN;

/// A row of values and references decodes to the same fields, references
/// marked as such, so the engine can list them without the caller's schema.
#[test]
fn a_row_round_trips_its_values_and_references() {
    let payload = indirection(7, 4_096, 1_000);
    let row = encode_row(&[
        RowField::bytes(C, RowCell::Value(b"status")),
        RowField::bytes(C + 6, reference(payload)),
        RowField::bytes(C + 9, RowCell::Value(b"")),
    ])
    .unwrap();
    let fields = decode_row(&row).unwrap();
    assert_eq!(fields.len(), 3);
    assert_eq!(
        fields.iter().map(|f| f.column).collect::<Vec<_>>(),
        [C, C + 6, C + 9]
    );
    assert!(matches!(fields[0].cell, RowCell::Value(b"status")));
    assert!(matches!(
        fields[1].cell,
        RowCell::Ref { indirection, owner: false } if indirection.vhandle == payload.vhandle
    ));
    assert!(matches!(fields[2].cell, RowCell::Value(b"")));
    let refs = row_refs(&row).unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].0.vhandle, payload.vhandle);
    assert!(!refs[0].1);
}

/// Each field's type comes back as written, fixed-width numbers included, so
/// a stored row reads with the layout it was written with.
#[test]
fn a_row_round_trips_its_field_types() {
    let tags = [
        TypeTag::Bytes,
        TypeTag::Fixed(3),
        u64_le(),
        TypeTag::Number(Number::new(NumberKind::Float, 4, ByteOrder::Big).unwrap()),
    ];
    let (eight, four) = ([1u8; 8], [2u8; 4]);
    let values: [&[u8]; 4] = [b"var", b"abc", &eight, &four];
    let fields: Vec<_> = tags
        .iter()
        .zip(values)
        .zip(C..)
        .map(|((tag, value), column)| RowField {
            column,
            tag: *tag,
            cell: RowCell::Value(value),
        })
        .collect();
    let row = encode_row(&fields).unwrap();
    let decoded = decode_row(&row).unwrap();
    assert_eq!(decoded.iter().map(|f| f.tag).collect::<Vec<_>>(), tags);
}

/// Ownership is per field: the owning reference and the borrowed one come
/// back as written, so the GC charges an object only through its owner.
#[test]
fn ownership_round_trips_per_reference() {
    let owned = indirection(4, 0, 30);
    let borrowed = indirection(4, 30, 40);
    let row = encode_row(&[
        RowField::bytes(
            C,
            RowCell::Ref {
                indirection: owned,
                owner: true,
            },
        ),
        RowField::bytes(C + 1, reference(borrowed)),
    ])
    .unwrap();
    let refs = row_refs(&row).unwrap();
    assert_eq!(refs.len(), 2);
    assert_eq!((refs[0].0.vhandle, refs[0].1), (owned.vhandle, true));
    assert_eq!((refs[1].0.vhandle, refs[1].1), (borrowed.vhandle, false));
}

/// Past eight fields both bitmaps span a second byte; a reference in it, and
/// its ownership, are still found.
#[test]
fn a_reference_past_the_eighth_field_is_found() {
    let far = indirection(3, 64, 10);
    let mut fields: Vec<RowField<'_>> = (C..C + 9)
        .map(|column| RowField::bytes(column, RowCell::Value(b"x")))
        .collect();
    fields.push(RowField::bytes(
        C + 9,
        RowCell::Ref {
            indirection: far,
            owner: true,
        },
    ));
    let row = encode_row(&fields).unwrap();
    let refs = row_refs(&row).unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].0.vhandle, far.vhandle);
    assert!(refs[0].1);
}

/// A truncated or padded row is refused rather than read as fewer or more
/// fields than it holds.
#[test]
fn a_malformed_row_is_refused() {
    let row = encode_row(&[
        RowField::bytes(C, RowCell::Value(b"abc")),
        RowField::bytes(C + 1, reference(indirection(1, 0, 5))),
    ])
    .unwrap();
    for cut in 0..row.len() {
        assert!(
            decode_row(&row[..cut]).is_err(),
            "a row cut to {cut} bytes decoded"
        );
    }
    let mut padded = row;
    padded.push(0);
    assert!(decode_row(&padded).is_err(), "trailing bytes decoded");
}

/// An owner bit on a value field names no object: the row is damaged, and is
/// refused rather than read with the bit ignored.
#[test]
fn an_owning_value_field_is_refused() {
    let mut row = encode_row(&[RowField::bytes(C, RowCell::Value(b"abc"))]).unwrap();
    // Count (2 bytes), reference bitmap (1 byte), then the owner bitmap.
    row[3] |= 1;
    assert!(matches!(decode_row(&row), Err(Error::InvalidHeader(_))));
}

/// A descriptor naming a type that does not exist is damage, refused rather
/// than read with some other layout.
#[test]
fn an_unknown_field_type_is_refused() {
    let mut row = encode_row(&[RowField::bytes(C, RowCell::Value(b"abc"))]).unwrap();
    // Count, two one-byte bitmaps, then the column id; the tag follows it.
    row[6] = 0xFF;
    assert!(matches!(decode_row(&row), Err(Error::InvalidHeader(_))));
}

/// A stored row whose fields are out of column order, repeat a column or name
/// an id that is not a field's is damage: every written row is in ascending
/// field order, which is what lets a columnar table give it back as written.
#[test]
fn a_stored_row_out_of_field_order_is_refused() {
    for columns in [[C + 1, C], [C, C], [0, C], [C, u16::MAX]] {
        let row = encode_row(&[
            RowField::bytes(columns[0], RowCell::Value(b"a")),
            RowField::bytes(columns[1], RowCell::Value(b"b")),
        ])
        .unwrap();
        assert!(
            matches!(decode_row(&row), Err(Error::InvalidHeader(_))),
            "{columns:?} decoded"
        );
    }
}

/// Fields given in any order are stored in ascending column order.
#[test]
fn fields_are_put_in_column_order() {
    let mut fields = [
        RowField::bytes(C + 7, RowCell::Value(b"c")),
        RowField::bytes(C, RowCell::Value(b"a")),
        RowField::bytes(C + 2, RowCell::Value(b"b")),
    ];
    order_fields(&mut fields).unwrap();
    assert_eq!(fields.map(|f| f.column), [C, C + 2, C + 7]);
}

/// Two fields in one column would make the row's layout ambiguous: the
/// write is refused.
#[test]
fn two_fields_in_one_column_are_refused() {
    let result = order_fields(&mut [
        RowField::bytes(C + 1, RowCell::Value(b"a")),
        RowField::bytes(C, RowCell::Value(b"x")),
        RowField::bytes(C + 1, RowCell::Value(b"b")),
    ]);
    assert!(matches!(result, Err(Error::CellRow(_))), "{result:?}");
}

/// A column id that is not a field id (an intrinsic column's, or the one a
/// columnar table keeps whole values under) is refused.
#[test]
fn a_column_that_is_not_a_field_id_is_refused() {
    for column in [0, 1, 2, u16::MAX] {
        let result = order_fields(&mut [RowField::bytes(column, RowCell::Value(b"a"))]);
        assert!(
            matches!(result, Err(Error::CellRow(_))),
            "{column}: {result:?}"
        );
    }
    assert!(order_fields(&mut [RowField::bytes(u16::MAX - 1, RowCell::Value(b"a"))]).is_ok());
}

/// A fixed-width field whose value, or whose referenced object, is not its
/// type's width is refused; one of the right width passes.
#[test]
fn a_fixed_width_field_of_another_width_is_refused() {
    let field = |cell| RowField {
        column: C,
        tag: u64_le(),
        cell,
    };
    assert!(order_fields(&mut [field(RowCell::Value(&[0; 8]))]).is_ok());
    for cell in [
        RowCell::Value(&[0; 7]),
        RowCell::Value(&[0; 9]),
        reference(indirection(1, 0, 4)),
    ] {
        let result = order_fields(&mut [field(cell)]);
        assert!(matches!(result, Err(Error::CellRow(_))), "{result:?}");
    }
    assert!(order_fields(&mut [field(reference(indirection(1, 0, 8)))]).is_ok());
}

/// A zero-width fixed type has no wire form: a row holding it would not
/// decode, so the write is refused.
#[test]
fn a_zero_width_fixed_field_is_refused() {
    let result = order_fields(&mut [RowField {
        column: C,
        tag: TypeTag::Fixed(0),
        cell: RowCell::Value(b""),
    }]);
    assert!(matches!(result, Err(Error::CellRow(_))), "{result:?}");
}

/// The logical value is the fields in order with references replaced by
/// their objects, a variable-width field length-prefixed and a fixed-width
/// one bare: the framing the columnar format gives a row's value sub-columns.
#[test]
fn a_row_resolves_to_its_fields_with_objects_in_place() {
    let object = crate::Slice::from(vec![b'p'; 5]);
    let row = encode_row(&[
        RowField::bytes(C, RowCell::Value(b"ab")),
        RowField {
            column: C + 1,
            tag: u64_le(),
            cell: RowCell::Value(&[7, 0, 0, 0, 0, 0, 0, 0]),
        },
        RowField::bytes(C + 2, reference(indirection(1, 0, 5))),
    ])
    .unwrap();
    let value = resolve_row(&row, |_| Ok(object.clone())).unwrap();
    assert_eq!(value, b"\x02\0\0\0ab\x07\0\0\0\0\0\0\0\x05\0\0\0ppppp");
    assert_eq!(logical_len(&row).unwrap() as usize, value.len());
}

/// Separation is decided per column: a field goes to a blob file at or above
/// its own column's threshold, wherever it sits in the row.
#[test]
fn separation_follows_the_column_threshold() {
    let row = encode_row(&[
        RowField::bytes(C, RowCell::Value(b"tiny")),
        RowField::bytes(C + 5, RowCell::Value(b"large-but-kept")),
    ])
    .unwrap();
    let mut written = Vec::new();
    let separated = separate_row(
        &row,
        |column| if column == C { 0 } else { u32::MAX },
        |bytes| {
            written.push(bytes.to_vec());
            Ok(ValueHandle {
                blob_file_id: 1,
                offset: 0,
                on_disk_size: 4,
            })
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(written, [b"tiny".to_vec()]);
    let fields = decode_row(&separated).unwrap();
    assert!(matches!(fields[0].cell, RowCell::Ref { owner: true, .. }));
    assert_eq!(fields[0].column, C);
    assert!(matches!(fields[1].cell, RowCell::Value(b"large-but-kept")));
}

/// An object whose length differs from the size its reference records is an
/// error: a dangling or mis-typed reference never reads as some other value.
#[test]
fn an_object_of_another_size_is_an_error() {
    let row = encode_row(&[RowField::bytes(C, reference(indirection(1, 0, 5)))]).unwrap();
    let result = resolve_row(&row, |_| Ok(crate::Slice::from(vec![b'p'; 4])));
    assert!(matches!(result, Err(Error::InvalidHeader(_))), "{result:?}");
}

/// Two references are the same object when they name the same frame,
/// whichever key they were read from.
#[test]
fn references_to_one_frame_are_equal() {
    let at = |offset| BlobRef {
        indirection: indirection(2, offset, 9),
        key: b"k",
        tree: 0,
        source: 0,
    };
    assert_eq!(at(128), at(128));
    assert_ne!(at(128), at(256));
}
