use super::*;
use crate::table::columnar::bytes_column_row;

fn u32_field(id: u16, absent: Absent) -> ProjectedField {
    ProjectedField::new(id, TypeTag::Fixed(4), absent).unwrap()
}

fn fixed_column(id: u16, cells: &[[u8; 4]], validity: Option<Vec<u8>>) -> Column {
    Column {
        column_id: id,
        type_tag: TypeTag::Fixed(4),
        validity,
        data: Slice::from(cells.concat()),
    }
}

fn bytes_column(id: u16, cells: &[&[u8]], validity: Option<Vec<u8>>) -> Column {
    Column {
        column_id: id,
        type_tag: TypeTag::Bytes,
        validity,
        data: frame_bytes_column(cells.len(), || cells.iter().copied()).unwrap(),
    }
}

/// A default that does not fit a fixed-width field is refused when the field
/// is declared, not when a scan first needs it.
#[test]
fn a_default_of_the_wrong_width_is_refused_at_declaration() {
    let field = ProjectedField::new(
        5,
        TypeTag::Fixed(4),
        Absent::Default(Slice::from(&[1u8][..])),
    );
    assert!(matches!(field, Err(Error::Projection(_))));
}

/// Intrinsic columns have a fixed type and are always present, so they are
/// projected by id and cannot be declared.
#[test]
fn an_intrinsic_column_cannot_be_declared() {
    let field = ProjectedField::new(COL_USER_KEY, TypeTag::Bytes, Absent::Null);
    assert!(matches!(field, Err(Error::Projection(_))));
    assert_eq!(
        Some(TypeTag::Bytes),
        ProjectedField::by_id(COL_USER_KEY).type_tag()
    );
    assert_eq!(None, ProjectedField::by_id(7).type_tag());
}

/// A column the segment does not have reads as its declaration says: null,
/// the default, or an error; one projected by id alone is an error.
#[test]
fn a_missing_column_reads_as_declared() {
    let batch = || ColumnBatch {
        row_count: 3,
        columns: vec![fixed_column(4, &[[1; 4], [2; 4], [3; 4]], None)],
    };

    let null = conform(batch(), &[u32_field(5, Absent::Null)]).unwrap();
    let column = null.columns.first().unwrap();
    assert_eq!(5, column.column_id);
    assert_eq!(Some(vec![0u8]), column.validity);
    assert_eq!(12, column.data.len());

    let default = conform(
        batch(),
        &[u32_field(5, Absent::Default(Slice::from(&[9u8; 4][..])))],
    )
    .unwrap();
    let column = default.columns.first().unwrap();
    assert_eq!(None, column.validity);
    assert_eq!(&[9u8; 12][..], &*column.data);

    assert!(matches!(
        conform(batch(), &[u32_field(5, Absent::Error)]),
        Err(Error::Projection(_))
    ));
    assert!(matches!(
        conform(batch(), &[ProjectedField::by_id(5)]),
        Err(Error::Projection(_))
    ));
}

/// A null cell counts as absent: it is kept null, filled with the default, or
/// an error, and the other cells are untouched.
#[test]
fn a_null_cell_reads_as_declared() {
    // Row 1 is null.
    let batch = || ColumnBatch {
        row_count: 3,
        columns: vec![
            fixed_column(5, &[[1; 4], [0; 4], [3; 4]], Some(vec![0b101])),
            bytes_column(6, &[b"a", b"", b"c"], Some(vec![0b101])),
        ],
    };

    let kept = conform(
        batch(),
        &[
            u32_field(5, Absent::Null),
            ProjectedField::new(6, TypeTag::Bytes, Absent::Null).unwrap(),
        ],
    )
    .unwrap();
    assert_eq!(batch().columns, kept.columns);

    let filled = conform(
        batch(),
        &[
            u32_field(5, Absent::Default(Slice::from(&[9u8; 4][..]))),
            ProjectedField::new(6, TypeTag::Bytes, Absent::Default(Slice::from(&b"zz"[..])))
                .unwrap(),
        ],
    )
    .unwrap();
    let [fixed, bytes] = filled.columns.as_slice() else {
        panic!("two columns");
    };
    assert_eq!(None, fixed.validity);
    assert_eq!([[1u8; 4], [9; 4], [3; 4]].concat(), &*fixed.data);
    assert_eq!(None, bytes.validity);
    let cells: Vec<&[u8]> = (0..3)
        .map(|row| bytes_column_row(&bytes.data, 3, row).unwrap())
        .collect();
    assert_eq!(vec![&b"a"[..], b"zz", b"c"], cells);

    assert!(matches!(
        conform(batch(), &[u32_field(5, Absent::Error)]),
        Err(Error::Projection(_))
    ));
}

/// A column with a validity bitmap and no null row is returned as stored, and
/// a required field is satisfied by it.
#[test]
fn a_validity_bitmap_without_nulls_is_kept_and_satisfies_a_required_field() {
    let column = fixed_column(5, &[[1; 4], [2; 4]], Some(vec![0b11]));
    let batch = ColumnBatch {
        row_count: 2,
        columns: vec![column.clone()],
    };
    let out = conform(batch, &[u32_field(5, Absent::Error)]).unwrap();
    assert_eq!(vec![column], out.columns);
}

/// A segment that stores a declared field under another type is an error,
/// not a misread.
#[test]
fn a_column_of_another_type_is_refused() {
    let batch = ColumnBatch {
        row_count: 1,
        columns: vec![bytes_column(5, &[b"a"], None)],
    };
    assert!(matches!(
        conform(batch, &[u32_field(5, Absent::Null)]),
        Err(Error::Projection(_))
    ));
}

/// The columns come back in the projection's order, with a column no field
/// names kept after them.
#[test]
fn columns_come_back_in_projection_order() {
    let batch = ColumnBatch {
        row_count: 1,
        columns: vec![
            fixed_column(4, &[[4; 4]], None),
            fixed_column(5, &[[5; 4]], None),
            fixed_column(6, &[[6; 4]], None),
        ],
    };
    let out = conform(batch, &[ProjectedField::by_id(6), ProjectedField::by_id(4)]).unwrap();
    let ids: Vec<u16> = out.columns.iter().map(|c| c.column_id).collect();
    assert_eq!(vec![6, 4, 5], ids);
}

/// A projector writes only declared fields of the declared width.
#[test]
fn a_projected_row_checks_what_the_projector_writes() {
    let fields = [u32_field(5, Absent::Null), ProjectedField::by_id(6)];
    let mut cells = vec![None, None];
    let mut row = ProjectedRow::new(&fields, &mut cells);
    row.set(0, &[1, 2, 3, 4]).unwrap();
    assert!(matches!(row.set(0, &[1, 2]), Err(Error::Projection(_))));
    assert!(matches!(row.set(1, b"x"), Err(Error::Projection(_))));
    assert!(matches!(row.set(2, b"x"), Err(Error::Projection(_))));
    assert_eq!(vec![Some(vec![1, 2, 3, 4]), None], cells);
}
