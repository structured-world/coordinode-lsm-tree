use super::*;
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

/// A row of values and references decodes to the same cells, references
/// marked as such, so the engine can list them without the caller's schema.
#[test]
fn a_row_round_trips_its_values_and_references() {
    let payload = indirection(7, 4_096, 1_000);
    let row = encode_row(&[
        RowCell::Value(b"status"),
        reference(payload),
        RowCell::Value(b""),
    ])
    .unwrap();
    let cells = decode_row(&row).unwrap();
    assert_eq!(cells.len(), 3);
    assert!(matches!(cells[0], RowCell::Value(b"status")));
    assert!(matches!(
        cells[1],
        RowCell::Ref { indirection, owner: false } if indirection.vhandle == payload.vhandle
    ));
    assert!(matches!(cells[2], RowCell::Value(b"")));
    let refs = row_refs(&row).unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].0.vhandle, payload.vhandle);
    assert!(!refs[0].1);
}

/// Ownership is per cell: the owning reference and the borrowed one come
/// back as written, so the GC charges an object only through its owner.
#[test]
fn ownership_round_trips_per_reference() {
    let owned = indirection(4, 0, 30);
    let borrowed = indirection(4, 30, 40);
    let row = encode_row(&[
        RowCell::Ref {
            indirection: owned,
            owner: true,
        },
        reference(borrowed),
    ])
    .unwrap();
    let refs = row_refs(&row).unwrap();
    assert_eq!(refs.len(), 2);
    assert_eq!((refs[0].0.vhandle, refs[0].1), (owned.vhandle, true));
    assert_eq!((refs[1].0.vhandle, refs[1].1), (borrowed.vhandle, false));
}

/// Past eight cells both bitmaps span a second byte; a reference in it, and
/// its ownership, are still found.
#[test]
fn a_reference_past_the_eighth_cell_is_found() {
    let far = indirection(3, 64, 10);
    let mut cells: Vec<RowCell<'_>> = (0..9).map(|_| RowCell::Value(b"x")).collect();
    cells.push(RowCell::Ref {
        indirection: far,
        owner: true,
    });
    let row = encode_row(&cells).unwrap();
    let refs = row_refs(&row).unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].0.vhandle, far.vhandle);
    assert!(refs[0].1);
}

/// A truncated or padded row is refused rather than read as fewer or more
/// cells than it holds.
#[test]
fn a_malformed_row_is_refused() {
    let row = encode_row(&[RowCell::Value(b"abc"), reference(indirection(1, 0, 5))]).unwrap();
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

/// An owner bit on a value cell names no object: the row is damaged, and is
/// refused rather than read with the bit ignored.
#[test]
fn an_owning_value_cell_is_refused() {
    let mut row = encode_row(&[RowCell::Value(b"abc")]).unwrap();
    // Count (2 bytes), reference bitmap (1 byte), then the owner bitmap.
    row[3] |= 1;
    assert!(matches!(decode_row(&row), Err(Error::InvalidHeader(_))));
}

/// The logical value is every cell length-prefixed, references replaced by
/// their objects: the framing of byte cells a plain read returns.
#[test]
fn a_row_resolves_to_its_cells_with_objects_in_place() {
    let object = crate::Slice::from(vec![b'p'; 5]);
    let row = encode_row(&[RowCell::Value(b"ab"), reference(indirection(1, 0, 5))]).unwrap();
    let value = resolve_row(&row, |_| Ok(object.clone())).unwrap();
    assert_eq!(value, b"\x02\0\0\0ab\x05\0\0\0ppppp");
}

/// An object whose length differs from the size its reference records is an
/// error: a dangling or mis-typed reference never reads as some other value.
#[test]
fn an_object_of_another_size_is_an_error() {
    let row = encode_row(&[reference(indirection(1, 0, 5))]).unwrap();
    let result = resolve_row(&row, |_| Ok(crate::Slice::from(vec![b'p'; 4])));
    assert!(matches!(result, Err(Error::InvalidHeader(_))), "{result:?}");
}

/// Two references are the same object when they name the same frame,
/// whichever key they were read from.
#[test]
fn references_to_one_frame_are_equal() {
    let at = |offset| BlobRef {
        indirection: indirection(2, offset, 9),
        key: crate::UserKey::from("k"),
    };
    assert_eq!(at(128), at(128));
    assert_ne!(at(128), at(256));
}
