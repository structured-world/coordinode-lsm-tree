use super::*;
use crate::blob_tree::field_row::{RowCell, RowField, encode_row, row_refs};
use crate::vlog::ValueHandle;
use test_log::test;

fn object(offset: u64) -> BlobIndirection {
    BlobIndirection {
        vhandle: ValueHandle {
            blob_file_id: 1,
            offset,
            on_disk_size: 100,
        },
        size: 100,
    }
}

fn cell_row(key: &str, seqno: u64, refs: &[(BlobIndirection, bool)]) -> InternalValue {
    let cells: Vec<RowField<'_>> = refs
        .iter()
        .zip(crate::blob_tree::field_row::FIRST_FIELD_COLUMN..)
        .map(|(&(indirection, owner), column)| {
            RowField::bytes(column, RowCell::Ref { indirection, owner })
        })
        .collect();
    InternalValue::from_components(key, encode_row(&cells).unwrap(), seqno, ValueType::CellRow)
}

fn value(key: &str, seqno: u64) -> InternalValue {
    InternalValue::from_components(key, "v", seqno, ValueType::Value)
}

/// Runs `events` through a ledger: `Ok` rows are kept and admitted, `Err`
/// rows are dropped. Returns what was written, in order, and the charges.
fn run(
    events: Vec<Result<InternalValue, InternalValue>>,
) -> (Vec<InternalValue>, FragmentationMap) {
    run_with(OwnershipLedger::default(), events)
}

/// [`run`] through `ledger`.
fn run_with(
    mut ledger: OwnershipLedger,
    events: Vec<Result<InternalValue, InternalValue>>,
) -> (Vec<InternalValue>, FragmentationMap) {
    let mut written = Vec::new();
    for event in events {
        match event {
            Ok(kept) => ledger
                .admit(kept, &mut |row| {
                    written.push(row);
                    Ok(())
                })
                .unwrap(),
            Err(dropped) => ledger.dropped(&dropped).unwrap(),
        }
    }
    let (frag, _) = ledger
        .finish(&mut |row| {
            written.push(row);
            Ok(())
        })
        .unwrap();
    (written, frag)
}

fn order(rows: &[InternalValue]) -> Vec<(&[u8], u64)> {
    rows.iter()
        .map(|row| (&row.key.user_key[..], row.key.seqno))
        .collect()
}

/// A dropped owner whose object a kept row of its key still holds charges
/// nothing: ownership passes to that row, rewritten before it is written.
#[test]
fn a_dropped_owner_passes_its_object_to_a_kept_holder() {
    let x = object(0);
    let (written, frag) = run(vec![
        Ok(cell_row("k", 3, &[(x, false)])),
        Err(cell_row("k", 1, &[(x, true)])),
    ]);
    assert!(frag.is_empty(), "nothing is garbage: {frag:?}");
    assert_eq!(order(&written), vec![(&b"k"[..], 3)]);
    assert!(row_refs(&written[0].value).unwrap()[0].1);
}

/// With several kept holders, the oldest takes the object, keeping the owner
/// the oldest holder.
#[test]
fn the_oldest_kept_holder_takes_the_object() {
    let x = object(0);
    let (written, frag) = run(vec![
        Ok(cell_row("k", 5, &[(x, false)])),
        Ok(cell_row("k", 3, &[(x, false)])),
        Err(cell_row("k", 1, &[(x, true)])),
    ]);
    assert!(frag.is_empty());
    let owners: Vec<bool> = written
        .iter()
        .map(|row| row_refs(&row.value).unwrap()[0].1)
        .collect();
    assert_eq!(owners, vec![false, true]);
}

/// A dropped owner with no kept holder charges its object, once.
#[test]
fn a_dropped_owner_without_a_kept_holder_is_charged() {
    let x = object(0);
    let (written, frag) = run(vec![
        Ok(value("k", 3)),
        Err(cell_row("k", 2, &[(x, false)])),
        Err(cell_row("k", 1, &[(x, true)])),
    ]);
    assert_eq!(order(&written), vec![(&b"k"[..], 3)]);
    assert_eq!(
        frag.get(&1).map(|entry| (entry.len, entry.bytes)),
        Some((1, 100))
    );
}

/// A key's rows after its first cell row are held with it, so the output
/// keeps the order the compaction emitted, before any later key's rows.
#[test]
fn held_rows_keep_their_order_before_later_keys() {
    let x = object(0);
    let (written, _) = run(vec![
        Ok(value("a", 5)),
        Ok(cell_row("a", 4, &[(x, false)])),
        Ok(value("a", 3)),
        Err(cell_row("a", 1, &[(x, true)])),
        Ok(value("b", 9)),
    ]);
    assert_eq!(
        order(&written),
        vec![
            (&b"a"[..], 5),
            (&b"a"[..], 4),
            (&b"a"[..], 3),
            (&b"b"[..], 9)
        ]
    );
}

/// A dropped owner of the next key, met while the previous key's rows are
/// still held, is settled with its own key: a kept holder that follows it
/// takes the object, and the held rows are written first.
#[test]
fn a_drop_of_the_next_key_settles_with_its_own_key() {
    let x = object(0);
    let y = object(100);
    let (written, frag) = run(vec![
        Ok(cell_row("a", 2, &[(x, true)])),
        Err(cell_row("b", 4, &[(y, true)])),
        Ok(cell_row("b", 3, &[(y, false)])),
    ]);
    assert!(frag.is_empty(), "{frag:?}");
    assert_eq!(order(&written), vec![(&b"a"[..], 2), (&b"b"[..], 3)]);
    assert!(row_refs(&written[1].value).unwrap()[0].1);
}

/// In a pass that relocates its file, an object every kept holder borrows (its
/// owner went with a whole-table drop before the pass) passes to the oldest
/// holder, so its copy has one owner. An object already owned, or in a file
/// the pass does not relocate, is left as it is.
#[test]
fn an_ownerless_object_in_a_relocated_file_passes_to_its_oldest_holder() {
    let x = object(0);
    let owners = |written: &[InternalValue]| -> Vec<bool> {
        written
            .iter()
            .map(|row| row_refs(&row.value).unwrap()[0].1)
            .collect()
    };
    let relocating = |files: &[u64]| {
        let mut ledger = OwnershipLedger::default();
        ledger.relocating(files.iter().copied());
        ledger
    };
    let borrowed = || {
        vec![
            Ok(cell_row("k", 5, &[(x, false)])),
            Ok(cell_row("k", 3, &[(x, false)])),
        ]
    };

    let (written, frag) = run_with(relocating(&[1]), borrowed());
    assert!(frag.is_empty(), "nothing is charged: {frag:?}");
    assert_eq!(
        owners(&written),
        vec![false, true],
        "the oldest holder owns it"
    );

    let (written, _) = run_with(relocating(&[2]), borrowed());
    assert_eq!(
        owners(&written),
        vec![false, false],
        "a file the pass keeps"
    );
    let (written, _) = run(borrowed());
    assert_eq!(
        owners(&written),
        vec![false, false],
        "a pass that relocates nothing"
    );

    let (written, _) = run_with(
        relocating(&[1]),
        vec![
            Ok(cell_row("k", 5, &[(x, false)])),
            Ok(cell_row("k", 3, &[(x, true)])),
        ],
    );
    assert_eq!(owners(&written), vec![false, true], "one owner, kept");
}

/// A dropped indirection is charged as before, whatever the ledger holds.
#[test]
fn a_dropped_indirection_is_charged_at_once() {
    use crate::coding::Encode;
    let x = object(0);
    let indirection =
        InternalValue::from_components("k", x.encode_into_vec(), 1, ValueType::Indirection);
    let (_, frag) = run(vec![Ok(value("k", 2)), Err(indirection)]);
    assert_eq!(frag.get(&1).map(|entry| entry.len), Some(1));
}
