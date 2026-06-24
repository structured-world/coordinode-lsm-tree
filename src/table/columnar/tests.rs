use super::{
    COL_SEQNO, COL_USER_KEY, COL_VALUE, COL_VALUE_TYPE, CodecId, Column, ColumnBatch, TypeTag,
    column_batch_into_entries, column_batch_match_entries, column_batch_to_entries,
    entries_to_column_batch, frame_value_cells, frame_value_cells_nullable, unframe_value_cells,
    unframe_value_cells_nullable, unframe_value_cells_with_defaults,
    validate_columnar_ingest_batch,
};
use crate::{Slice, ValueType, key::InternalKey, value::InternalValue};

#[test]
fn fixed_only_value_framing_has_no_overhead_and_round_trips() {
    // Fixed-width cells are stored verbatim (width recoverable from the tag),
    // so the framed blob is the bare concatenation: zero per-cell overhead.
    let tags = [TypeTag::Fixed(4), TypeTag::Fixed(4)];
    let blob = frame_value_cells(&[
        (TypeTag::Fixed(4), &[1, 0, 0, 0][..]),
        (TypeTag::Fixed(4), &[2, 0, 0, 0][..]),
    ])
    .expect("frame");
    assert_eq!(blob, vec![1, 0, 0, 0, 2, 0, 0, 0], "no length prefixes");
    let cells = unframe_value_cells(&blob, &tags).expect("unframe");
    assert_eq!(cells, vec![&[1, 0, 0, 0][..], &[2, 0, 0, 0][..]]);
}

#[test]
fn mixed_value_framing_round_trips() {
    let tags = [TypeTag::Bytes, TypeTag::Fixed(1), TypeTag::Bytes];
    let blob = frame_value_cells(&[
        (TypeTag::Bytes, b"abc"),
        (TypeTag::Fixed(1), &[7][..]),
        (TypeTag::Bytes, b""),
    ])
    .expect("frame");
    let cells = unframe_value_cells(&blob, &tags).expect("unframe");
    assert_eq!(cells, vec![&b"abc"[..], &[7][..], &b""[..]]);
}

#[test]
fn empty_value_framing_round_trips() {
    let blob = frame_value_cells(&[]).expect("frame");
    assert!(blob.is_empty());
    assert!(unframe_value_cells(&blob, &[]).expect("unframe").is_empty());
}

#[test]
fn unframe_rejects_truncated_blob() {
    // A fixed(4) tag over a 2-byte blob is truncated.
    assert!(unframe_value_cells(&[1, 2], &[TypeTag::Fixed(4)]).is_err());
    // A bytes tag whose declared length runs past the end.
    let mut blob = 8u32.to_le_bytes().to_vec();
    blob.extend_from_slice(b"abc"); // 3 bytes present, length claims 8
    assert!(unframe_value_cells(&blob, &[TypeTag::Bytes]).is_err());
}

#[test]
fn unframe_rejects_trailing_bytes() {
    // One fixed(2) cell, but the blob carries an extra byte the tags do not
    // cover: the blob and the tag list disagree.
    assert!(unframe_value_cells(&[1, 2, 3], &[TypeTag::Fixed(2)]).is_err());
}

#[test]
fn frame_rejects_fixed_cell_length_mismatch() {
    // A fixed(4) tag with a 3-byte cell would misframe (and shift later cells),
    // so framing rejects it.
    assert!(frame_value_cells(&[(TypeTag::Fixed(4), &[1, 2, 3][..])]).is_err());
}

#[test]
fn nullable_framing_round_trips_mixed_present_and_null() {
    let tags = [TypeTag::Fixed(4), TypeTag::Bytes, TypeTag::Fixed(2)];
    let blob = frame_value_cells_nullable(&[
        (TypeTag::Fixed(4), Some(&[1, 0, 0, 0][..])),
        (TypeTag::Bytes, None),
        (TypeTag::Fixed(2), Some(&[9, 9][..])),
    ])
    .expect("frame");
    // Presence bitmap (1 byte): bits 0 and 2 set, bit 1 clear -> 0b101 = 5.
    assert_eq!(blob[0], 0b0000_0101, "presence bitmap marks the null cell");
    assert_eq!(
        unframe_value_cells_nullable(&blob, &tags).expect("unframe"),
        vec![Some(&[1, 0, 0, 0][..]), None, Some(&[9, 9][..])],
    );
}

#[test]
fn nullable_framing_all_null_and_all_present() {
    let tags = [TypeTag::Bytes, TypeTag::Fixed(1)];
    let all_null = frame_value_cells_nullable(&[(TypeTag::Bytes, None), (TypeTag::Fixed(1), None)])
        .expect("frame");
    assert_eq!(all_null, vec![0u8], "bitmap only, no bodies");
    assert_eq!(
        unframe_value_cells_nullable(&all_null, &tags).expect("unframe"),
        vec![None, None],
    );

    let all_present = frame_value_cells_nullable(&[
        (TypeTag::Bytes, Some(&b"ab"[..])),
        (TypeTag::Fixed(1), Some(&[7][..])),
    ])
    .expect("frame");
    assert_eq!(
        unframe_value_cells_nullable(&all_present, &tags).expect("unframe"),
        vec![Some(&b"ab"[..]), Some(&[7][..])],
    );
}

#[test]
fn nullable_framing_rejects_truncated_and_trailing() {
    // A present fixed(4) cell whose bytes are missing after the bitmap.
    let truncated = alloc::vec![0b0000_0001u8, 1, 2]; // bit 0 set, only 2 of 4 bytes
    assert!(unframe_value_cells_nullable(&truncated, &[TypeTag::Fixed(4)]).is_err());
    // A valid all-null encoding with an extra trailing byte.
    let mut trailing = frame_value_cells_nullable(&[(TypeTag::Bytes, None)]).expect("frame");
    trailing.push(0);
    assert!(unframe_value_cells_nullable(&trailing, &[TypeTag::Bytes]).is_err());
}

#[test]
fn nullable_framing_rejects_a_fixed_cell_length_mismatch() {
    // A Fixed(4) tag with a 3-byte present cell would misframe (and shift
    // later cells), so the nullable framing rejects it like the non-nullable
    // path does.
    assert!(
        frame_value_cells_nullable(&[(TypeTag::Fixed(4), Some(&[1, 2, 3][..]))]).is_err(),
        "a present fixed cell whose length differs from its tag width must be rejected",
    );
}

#[test]
fn unframe_with_defaults_substitutes_for_null_cells() {
    let blob = frame_value_cells_nullable(&[
        (TypeTag::Fixed(2), Some(&[7, 7][..])),
        (TypeTag::Fixed(2), None),
    ])
    .expect("frame");
    let cells = unframe_value_cells_with_defaults(
        &blob,
        &[
            (TypeTag::Fixed(2), &[7, 7][..]),
            (TypeTag::Fixed(2), &[0, 0][..]),
        ],
    )
    .expect("unframe with defaults");
    assert_eq!(cells, vec![&[7, 7][..], &[0, 0][..]]);
}

#[test]
fn first_user_key_rejects_a_non_key_first_column() {
    // A first column that is not the non-null bytes user-key column is
    // rejected before any low-level offset read.
    let fixed_first = ColumnBatch {
        row_count: 1,
        columns: alloc::vec![Column {
            column_id: COL_USER_KEY,
            type_tag: TypeTag::Fixed(4),
            validity: None,
            data: alloc::vec![0, 0, 0, 0],
        }],
    };
    assert!(fixed_first.first_user_key().is_err());

    let missing = ColumnBatch {
        row_count: 1,
        columns: alloc::vec![Column {
            column_id: 99,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: alloc::vec![0, 0, 0, 0, 0, 0, 0, 0],
        }],
    };
    assert!(missing.first_user_key().is_err());
}

#[test]
fn first_user_key_returns_the_first_row_key() {
    // A two-row key column: the first key is read without decoding the batch.
    let mut data = Vec::new();
    for off in [0u32, 2, 5] {
        data.extend_from_slice(&off.to_le_bytes());
    }
    data.extend_from_slice(b"k0k11");
    let batch = ColumnBatch {
        row_count: 2,
        columns: alloc::vec![Column {
            column_id: COL_USER_KEY,
            type_tag: TypeTag::Bytes,
            validity: None,
            data,
        }],
    };
    assert_eq!(batch.first_user_key().expect("first key"), Some(&b"k0"[..]));
}

fn entry(user_key: &[u8], seqno: u64, value_type: ValueType, value: &[u8]) -> InternalValue {
    InternalValue {
        key: InternalKey {
            user_key: Slice::from(user_key),
            seqno,
            value_type,
        },
        value: Slice::from(value),
    }
}

fn assert_entries_eq(a: &[InternalValue], b: &[InternalValue]) {
    assert_eq!(a.len(), b.len(), "entry count mismatch");
    for (x, y) in a.iter().zip(b) {
        assert_eq!(x.key.user_key.as_ref(), y.key.user_key.as_ref());
        assert_eq!(x.key.seqno, y.key.seqno);
        assert!(x.key.value_type == y.key.value_type, "value_type mismatch");
        assert_eq!(x.value.as_ref(), y.value.as_ref());
    }
}

#[test]
fn delta_codec_round_trips_fixed8_column() {
    // A fixed-8 (u64) column auto-selects Delta and must round-trip exactly,
    // including a repeat and a decrease (wrapping delta).
    let seqnos: [u64; 5] = [100, 105, 105, 200, 199];
    let data: Vec<u8> = seqnos.iter().flat_map(|s| s.to_le_bytes()).collect();
    let batch = ColumnBatch {
        row_count: 5,
        columns: vec![Column {
            column_id: 1,
            type_tag: TypeTag::Fixed(8),
            validity: None,
            data: data.clone(),
        }],
    };
    let encoded = batch.encode(CodecId::Plain).expect("encode");
    // The codec byte (row_count 4 + col_count 4 + id 2 + type 1 + width 1 =
    // offset 12) must record Delta, auto-selected for the fixed-8 column.
    assert_eq!(encoded[12], u8::from(CodecId::Delta));
    let decoded = ColumnBatch::decode(&encoded).expect("decode");
    assert_eq!(
        decoded.columns[0].data, data,
        "delta column must round-trip"
    );
}

#[test]
fn auto_codec_is_delta_only_for_the_seqno_column() {
    // A fixed-8 column that is not the seqno column keeps the default codec
    // (Plain): delta-encoding a non-monotonic column would only inflate it.
    let batch = ColumnBatch {
        row_count: 2,
        columns: vec![Column {
            column_id: 99, // not the intrinsic seqno column
            type_tag: TypeTag::Fixed(8),
            validity: None,
            data: vec![0u8; 16],
        }],
    };
    let encoded = batch.encode(CodecId::Plain).expect("encode");
    assert_eq!(
        encoded[12],
        u8::from(CodecId::Plain),
        "a non-seqno fixed-8 column must not auto-select Delta"
    );
}

#[test]
fn encode_rejects_delta_on_a_non_fixed8_column() {
    // Forcing Delta on a Bytes column (via the fallback codec) is rejected
    // rather than silently truncating its bytes.
    let mut bytes_data = Vec::new();
    bytes_data.extend_from_slice(&0u32.to_le_bytes());
    bytes_data.extend_from_slice(&3u32.to_le_bytes());
    bytes_data.extend_from_slice(b"abc");
    let batch = ColumnBatch {
        row_count: 1,
        columns: vec![Column {
            column_id: 50,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: bytes_data,
        }],
    };
    assert!(batch.encode(CodecId::Delta).is_err());
}

#[test]
fn from_columnar_block_rejects_a_zero_row_block() {
    // A zero-row columnar block is corrupt (the writer never spills empty);
    // reconstructing it must error, not panic in the row encoder.
    let empty = entries_to_column_batch(&[])
        .expect("transpose")
        .encode(CodecId::Plain)
        .expect("encode");
    assert!(crate::table::data_block::DataBlock::from_columnar_block(&empty, 16).is_err());
}

#[test]
fn decode_projected_decodes_only_the_wanted_columns() {
    // Project just the user-key column: the result carries ONLY that column,
    // proving the value / seqno / value-type columns were never decoded.
    let entries = vec![
        entry(b"alpha", 10, ValueType::Value, b"v1"),
        entry(b"bravo", 9, ValueType::Value, b"v2"),
    ];
    let bytes = entries_to_column_batch(&entries)
        .expect("transpose")
        .encode(CodecId::Plain)
        .expect("encode");

    let projected =
        ColumnBatch::decode_projected(&bytes, &[COL_USER_KEY]).expect("decode_projected");
    assert_eq!(projected.row_count, 2);
    assert_eq!(
        projected.columns.len(),
        1,
        "only the projected column is decoded"
    );
    assert_eq!(projected.columns[0].column_id, COL_USER_KEY);
    assert!(
        !projected.columns.iter().any(|c| c.column_id == COL_VALUE),
        "a key-only projection must not decode the value column"
    );

    // Projecting every column equals a full decode.
    let all = [COL_USER_KEY, COL_SEQNO, COL_VALUE_TYPE, COL_VALUE];
    assert_eq!(
        ColumnBatch::decode_projected(&bytes, &all).expect("decode_projected all"),
        ColumnBatch::decode(&bytes).expect("decode"),
    );
}

#[test]
fn intrinsic_transpose_round_trips_entries() {
    // A mix of value kinds, including a tombstone (empty value) and a merge
    // operand, exercises the value column's variable width and the
    // value-type column.
    let entries = vec![
        entry(b"alpha", 10, ValueType::Value, b"v1"),
        entry(b"bravo", 9, ValueType::Tombstone, b""),
        entry(b"charlie", 8, ValueType::MergeOperand, b"+1"),
    ];
    let batch = entries_to_column_batch(&entries).expect("transpose");
    assert_eq!(batch.row_count, 3);
    assert_eq!(batch.columns.len(), 4, "four intrinsic columns");

    // Direct reconstruction.
    let back = column_batch_to_entries(&batch).expect("untranspose");
    assert_entries_eq(&entries, &back);

    // And through the block encode / decode, so the transpose composes with
    // the on-disk columnar format.
    let bytes = batch.encode(CodecId::Plain).expect("encode");
    let decoded = ColumnBatch::decode(&bytes).expect("decode");
    let back2 = column_batch_to_entries(&decoded).expect("untranspose decoded");
    assert_entries_eq(&entries, &back2);
}

#[test]
fn intrinsic_untranspose_rejects_wrong_layout() {
    // A batch whose columns are not the four intrinsic columns is refused.
    let bad = ColumnBatch {
        row_count: 1,
        columns: vec![Column {
            column_id: 0,
            type_tag: TypeTag::Fixed(4),
            validity: None,
            data: vec![0; 4],
        }],
    };
    assert!(column_batch_to_entries(&bad).is_err());
}

#[test]
fn intrinsic_untranspose_rejects_nullable_intrinsic() {
    // Intrinsic fields are never null; a validity bitmap on one marks a
    // malformed batch even if it is otherwise well-framed.
    let mut batch =
        entries_to_column_batch(&[entry(b"k", 1, ValueType::Value, b"v")]).expect("transpose");
    batch.columns[0].validity = Some(vec![0b1]); // one valid row, but nullable
    assert!(column_batch_to_entries(&batch).is_err());
}

#[test]
fn intrinsic_untranspose_rejects_empty_key() {
    // An empty user key violates the engine's key invariant.
    let batch =
        entries_to_column_batch(&[entry(b"", 1, ValueType::Value, b"v")]).expect("transpose");
    assert!(column_batch_to_entries(&batch).is_err());
}

#[test]
fn intrinsic_untranspose_rejects_huge_row_count() {
    // A hand-built batch claiming billions of rows but carrying tiny columns
    // is rejected by per-column validation before any allocation.
    let bad = ColumnBatch {
        row_count: u32::MAX,
        columns: vec![
            Column {
                column_id: 0,
                type_tag: TypeTag::Bytes,
                validity: None,
                data: 0u32.to_le_bytes().to_vec(),
            },
            Column {
                column_id: 1,
                type_tag: TypeTag::Fixed(8),
                validity: None,
                data: Vec::new(),
            },
            Column {
                column_id: 2,
                type_tag: TypeTag::Fixed(1),
                validity: None,
                data: Vec::new(),
            },
            Column {
                column_id: 3,
                type_tag: TypeTag::Bytes,
                validity: None,
                data: 0u32.to_le_bytes().to_vec(),
            },
        ],
    };
    assert!(column_batch_to_entries(&bad).is_err());
}

#[test]
fn untranspose_frames_multiple_value_subcolumns() {
    // A consumer batch: the three intrinsic columns plus two value
    // sub-columns (a fixed-4 and a variable-width bytes). Each row's
    // reconstructed value is the framed concat of its two sub-cells, which
    // unframe_value_cells reverses.
    let mut batch = entries_to_column_batch(&[
        entry(b"k0", 1, ValueType::Value, b"ignored"),
        entry(b"k1", 2, ValueType::Value, b"ignored"),
    ])
    .expect("transpose");
    // Replace the single opaque value column with two value sub-columns.
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: vec![1, 0, 0, 0, 2, 0, 0, 0], // row 0 = 1, row 1 = 2
    });
    let mut bytes_data = Vec::new();
    for off in [0u32, 2, 5] {
        bytes_data.extend_from_slice(&off.to_le_bytes());
    }
    bytes_data.extend_from_slice(b"aabbb"); // row 0 = "aa", row 1 = "bbb"
    batch.columns.push(Column {
        column_id: 4,
        type_tag: TypeTag::Bytes,
        validity: None,
        data: bytes_data,
    });

    let entries = column_batch_to_entries(&batch).expect("untranspose");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].key.user_key.as_ref(), b"k0");
    assert_eq!(entries[1].key.seqno, 2);

    let tags = [TypeTag::Fixed(4), TypeTag::Bytes];
    assert_eq!(
        unframe_value_cells(entries[0].value.as_ref(), &tags).expect("unframe 0"),
        vec![&[1, 0, 0, 0][..], &b"aa"[..]],
    );
    assert_eq!(
        unframe_value_cells(entries[1].value.as_ref(), &tags).expect("unframe 1"),
        vec![&[2, 0, 0, 0][..], &b"bbb"[..]],
    );
}

#[test]
fn untranspose_rejects_value_subcolumn_id_collisions() {
    // Projection selects value sub-columns by id, so a value sub-column that
    // reuses an intrinsic id (0/1/2) or duplicates another value id would make
    // projected results ambiguous. The untranspose must reject both.
    let make = |value_cols: Vec<Column>| -> ColumnBatch {
        let mut batch = entries_to_column_batch(&[entry(b"k0", 1, ValueType::Value, b"ignored")])
            .expect("transpose");
        batch.columns.pop(); // drop the single opaque value column
        batch.columns.extend(value_cols);
        batch
    };

    // A value sub-column reusing COL_USER_KEY (0).
    let collide_intrinsic = make(vec![Column {
        column_id: COL_USER_KEY,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: vec![0, 0, 0, 0],
    }]);
    assert!(
        column_batch_to_entries(&collide_intrinsic).is_err(),
        "a value sub-column reusing an intrinsic id must be rejected",
    );

    // Two value sub-columns sharing id 5.
    let duplicate = make(vec![
        Column {
            column_id: 5,
            type_tag: TypeTag::Fixed(4),
            validity: None,
            data: vec![0, 0, 0, 0],
        },
        Column {
            column_id: 5,
            type_tag: TypeTag::Fixed(4),
            validity: None,
            data: vec![1, 0, 0, 0],
        },
    ]);
    assert!(
        column_batch_to_entries(&duplicate).is_err(),
        "duplicate value sub-column ids must be rejected",
    );
}

#[test]
fn validate_ingest_rejects_an_invalid_value_type() {
    // The eager ingest validation must reject a malformed value-type byte on
    // the submitting call, not defer it to the flush-time decode.
    let mut batch =
        entries_to_column_batch(&[entry(b"k0", 0, ValueType::Value, b"v")]).expect("transpose");
    batch.columns[2].data[0] = 99; // not a valid ValueType tag
    assert!(
        validate_columnar_ingest_batch(&batch, &crate::comparator::default_comparator()).is_err(),
        "an invalid value-type tag must be rejected during eager validation",
    );
}

#[test]
fn untranspose_reconstructs_nullable_value_subcolumns() {
    // Two rows, two value sub-columns; the second (fixed-4) is nullable with
    // row 1 absent. Each row's reconstructed value is a presence-bitmap frame
    // that unframe_value_cells_nullable reverses to the original cells.
    let mut batch = entries_to_column_batch(&[
        InternalValue::from_components(b"k0", b"ignored", 0, ValueType::Value),
        InternalValue::from_components(b"k1", b"ignored", 0, ValueType::Value),
    ])
    .expect("transpose");
    batch.columns.pop();
    // Bytes sub-column (always present): "aa", "bbb".
    let mut bytes_data = Vec::new();
    for off in [0u32, 2, 5] {
        bytes_data.extend_from_slice(&off.to_le_bytes());
    }
    bytes_data.extend_from_slice(b"aabbb");
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Bytes,
        validity: None,
        data: bytes_data,
    });
    // Fixed-4 sub-column, row 0 present (=1), row 1 null. validity bit 0 set.
    batch.columns.push(Column {
        column_id: 4,
        type_tag: TypeTag::Fixed(4),
        validity: Some(alloc::vec![0b0000_0001]),
        data: alloc::vec![1, 0, 0, 0, 0, 0, 0, 0],
    });

    let entries = column_batch_to_entries(&batch).expect("untranspose");
    let tags = [TypeTag::Bytes, TypeTag::Fixed(4)];
    assert_eq!(
        unframe_value_cells_nullable(entries[0].value.as_ref(), &tags).expect("row 0"),
        vec![Some(&b"aa"[..]), Some(&[1, 0, 0, 0][..])],
    );
    assert_eq!(
        unframe_value_cells_nullable(entries[1].value.as_ref(), &tags).expect("row 1"),
        vec![Some(&b"bbb"[..]), None],
        "row 1's fixed sub-cell is absent",
    );
}

#[test]
fn untranspose_reconstructs_a_nullable_bytes_subcolumn() {
    // A nullable variable-width (`Bytes`) value sub-column: row 0 present,
    // row 1 null. The null row must still carry valid offset-table entries
    // (here equal consecutive offsets => an empty span) because `validate`
    // frames every row regardless of the validity bitmap; the validity bit,
    // not the offsets, is what marks the row absent.
    let mut batch = entries_to_column_batch(&[
        InternalValue::from_components(b"k0", b"ignored", 0, ValueType::Value),
        InternalValue::from_components(b"k1", b"ignored", 0, ValueType::Value),
    ])
    .expect("transpose");
    batch.columns.pop();
    // Offsets [0, 2, 2]: row 0 spans 0..2 ("hi"), row 1 spans 2..2 (empty).
    let mut data = Vec::new();
    for off in [0u32, 2, 2] {
        data.extend_from_slice(&off.to_le_bytes());
    }
    data.extend_from_slice(b"hi");
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Bytes,
        validity: Some(alloc::vec![0b0000_0001]), // row 0 present, row 1 null
        data,
    });

    let entries = column_batch_to_entries(&batch).expect("untranspose");
    let tags = [TypeTag::Bytes];
    assert_eq!(
        unframe_value_cells_nullable(entries[0].value.as_ref(), &tags).expect("row 0"),
        vec![Some(&b"hi"[..])],
    );
    assert_eq!(
        unframe_value_cells_nullable(entries[1].value.as_ref(), &tags).expect("row 1"),
        vec![None],
        "the null bytes row reconstructs as absent, not as an empty cell",
    );
}

#[test]
fn append_concatenates_fixed_bytes_and_nullable_columns() {
    // Build a two-row consumer batch with a fixed-4 (id 3), a bytes (id 4),
    // and a nullable fixed-2 (id 5) value sub-column.
    let build = |keys: [&[u8]; 2],
                 fixed: [u32; 2],
                 bytes: [&[u8]; 2],
                 nf2: [Option<[u8; 2]>; 2]|
     -> ColumnBatch {
        let mut batch = entries_to_column_batch(&[
            InternalValue::from_components(keys[0], b"x", 0, ValueType::Value),
            InternalValue::from_components(keys[1], b"x", 0, ValueType::Value),
        ])
        .expect("transpose");
        batch.columns.pop();
        batch.columns.push(Column {
            column_id: 3,
            type_tag: TypeTag::Fixed(4),
            validity: None,
            data: fixed.iter().flat_map(|v| v.to_le_bytes()).collect(),
        });
        let mut bytes_data = Vec::new();
        let mut acc = 0u32;
        bytes_data.extend_from_slice(&acc.to_le_bytes());
        for c in bytes {
            acc += u32::try_from(c.len()).expect("bytes cell length fits u32");
            bytes_data.extend_from_slice(&acc.to_le_bytes());
        }
        for c in bytes {
            bytes_data.extend_from_slice(c);
        }
        batch.columns.push(Column {
            column_id: 4,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: bytes_data,
        });
        let mut nf2_data = Vec::new();
        let mut nf2_valid = 0u8;
        for (i, v) in nf2.iter().enumerate() {
            nf2_data.extend_from_slice(&v.unwrap_or([0, 0]));
            if v.is_some() {
                nf2_valid |= 1 << i;
            }
        }
        batch.columns.push(Column {
            column_id: 5,
            type_tag: TypeTag::Fixed(2),
            validity: Some(alloc::vec![nf2_valid]),
            data: nf2_data,
        });
        batch
    };

    let mut a = build([b"k0", b"k1"], [1, 2], [b"a", b"bb"], [Some([9, 9]), None]);
    let b = build(
        [b"k2", b"k3"],
        [3, 4],
        [b"ccc", b"dddd"],
        [Some([7, 7]), None],
    );
    a.append(&b).expect("append");
    assert_eq!(a.row_count, 4);

    let entries = column_batch_to_entries(&a).expect("untranspose combined");
    let tags = [TypeTag::Fixed(4), TypeTag::Bytes, TypeTag::Fixed(2)];
    let check =
        |entry: &InternalValue, key: &[u8], fixed: [u8; 4], bytes: &[u8], nf2: Option<[u8; 2]>| {
            assert_eq!(entry.key.user_key.as_ref(), key);
            let cells = unframe_value_cells_nullable(entry.value.as_ref(), &tags).expect("unframe");
            assert_eq!(cells[0], Some(&fixed[..]));
            assert_eq!(cells[1], Some(bytes));
            assert_eq!(cells[2], nf2.as_ref().map(|n| &n[..]));
        };
    check(&entries[0], b"k0", [1, 0, 0, 0], b"a", Some([9, 9]));
    check(&entries[1], b"k1", [2, 0, 0, 0], b"bb", None);
    check(&entries[2], b"k2", [3, 0, 0, 0], b"ccc", Some([7, 7]));
    check(&entries[3], b"k3", [4, 0, 0, 0], b"dddd", None);
}

fn sample_batch() -> ColumnBatch {
    // 3 rows: a fixed u32 column (row 1 null) and a variable-width Bytes
    // column (all valid). Bytes data = offset array [0,2,2,5] + payload.
    let bytes_offsets: Vec<u8> = [0u32, 2, 2, 5]
        .iter()
        .flat_map(|o| o.to_le_bytes())
        .collect();
    let mut bytes_data = bytes_offsets;
    bytes_data.extend_from_slice(b"hi"); // row 0
    // row 1 empty (offset 2..2)
    bytes_data.extend_from_slice(b"abc"); // row 2
    ColumnBatch {
        row_count: 3,
        columns: vec![
            Column {
                column_id: 1,
                type_tag: TypeTag::Fixed(4),
                validity: Some(vec![0b0000_0101]), // rows 0 and 2 valid, row 1 null
                data: vec![10, 0, 0, 0, 0, 0, 0, 0, 30, 0, 0, 0],
            },
            Column {
                column_id: 2,
                type_tag: TypeTag::Bytes,
                validity: None,
                data: bytes_data,
            },
        ],
    }
}

#[test]
fn columnar_batch_round_trips_through_plain_codec() {
    let batch = sample_batch();
    let encoded = batch.encode(CodecId::Plain).expect("encode");
    let decoded = ColumnBatch::decode(&encoded).expect("decode");
    assert_eq!(decoded, batch, "columnar batch must survive a round-trip");
}

#[test]
fn columnar_decode_rejects_truncated_payload() {
    let encoded = sample_batch().encode(CodecId::Plain).expect("encode");
    // Drop the last byte: the final column's data is now short.
    let truncated = &encoded[..encoded.len() - 1];
    assert!(ColumnBatch::decode(truncated).is_err());
}

#[test]
fn columnar_encode_rejects_fixed_width_length_mismatch() {
    // A fixed(4) column claiming 3 rows but carrying 8 bytes (= 2 rows) is
    // rejected at encode, so no non-round-trippable payload is produced.
    let bad = ColumnBatch {
        row_count: 3,
        columns: vec![Column {
            column_id: 1,
            type_tag: TypeTag::Fixed(4),
            validity: None,
            data: vec![0; 8],
        }],
    };
    assert!(bad.encode(CodecId::Plain).is_err());
}

#[test]
fn columnar_decode_rejects_unknown_codec_tag() {
    let mut encoded = sample_batch().encode(CodecId::Plain).expect("encode");
    // Codec byte of the first column sits after row_count(4) + col_count(4)
    // + column_id(2) + type_tag(1) + width(1) = offset 12.
    encoded[12] = 0xFF;
    assert!(ColumnBatch::decode(&encoded).is_err());
}

#[test]
fn columnar_encode_rejects_zero_width_fixed_column() {
    // Fixed(0) is publicly constructible but cannot round-trip; encode must
    // reject it rather than emit bytes decode would refuse.
    let bad = ColumnBatch {
        row_count: 1,
        columns: vec![Column {
            column_id: 1,
            type_tag: TypeTag::Fixed(0),
            validity: None,
            data: Vec::new(),
        }],
    };
    assert!(bad.encode(CodecId::Plain).is_err());
}

#[test]
fn columnar_encode_rejects_wrong_validity_length() {
    // 1 row needs a 1-byte bitmap; a 2-byte bitmap is rejected.
    let bad = ColumnBatch {
        row_count: 1,
        columns: vec![Column {
            column_id: 1,
            type_tag: TypeTag::Fixed(1),
            validity: Some(vec![0, 0]),
            data: vec![7],
        }],
    };
    assert!(bad.encode(CodecId::Plain).is_err());
}

#[test]
fn columnar_encode_rejects_validity_padding_bits() {
    // 1 row: only bit 0 is meaningful; a set padding bit (0xFF) is rejected.
    let bad = ColumnBatch {
        row_count: 1,
        columns: vec![Column {
            column_id: 1,
            type_tag: TypeTag::Fixed(1),
            validity: Some(vec![0xFF]),
            data: vec![7],
        }],
    };
    assert!(bad.encode(CodecId::Plain).is_err());
}

#[test]
fn columnar_decode_rejects_bytes_offset_out_of_bounds() {
    let mut encoded = sample_batch().encode(CodecId::Plain).expect("encode");
    // The Bytes column's first offset (must be 0) is at byte 41: col1 starts
    // at 31 (id 2 + type 1 + width 1 + codec 1 + has_validity 1 + len 4 = 10
    // header bytes), so its data / offset table begins at 41.
    encoded[41] = 9;
    assert!(ColumnBatch::decode(&encoded).is_err());
}

#[test]
fn columnar_decode_rejects_trailing_bytes() {
    let mut encoded = sample_batch().encode(CodecId::Plain).expect("encode");
    encoded.push(0); // one byte past the last declared column
    assert!(ColumnBatch::decode(&encoded).is_err());
}

#[test]
fn columnar_decode_rejects_huge_column_count() {
    // row_count = 0, column_count = u32::MAX, but no column bytes follow.
    let mut payload = 0u32.to_le_bytes().to_vec();
    payload.extend_from_slice(&u32::MAX.to_le_bytes());
    assert!(ColumnBatch::decode(&payload).is_err());
}

#[test]
fn columnar_decode_rejects_non_boolean_validity_flag() {
    let mut encoded = sample_batch().encode(CodecId::Plain).expect("encode");
    // has_validity flag of the first column is at byte 13.
    encoded[13] = 2;
    assert!(ColumnBatch::decode(&encoded).is_err());
}

#[test]
fn columnar_decode_rejects_non_zero_bytes_width() {
    let mut encoded = sample_batch().encode(CodecId::Plain).expect("encode");
    // The Bytes column's width byte (must be 0) is at byte 34.
    encoded[34] = 5;
    assert!(ColumnBatch::decode(&encoded).is_err());
}

#[test]
fn column_batch_into_entries_rejects_an_empty_key_row() {
    // A corrupt block can frame an empty key (offset table [0, 0], no payload) —
    // a byte layout the structural validator accepts. The scan reconstruction
    // must reject it rather than emit an empty-keyed entry.
    let mut batch =
        entries_to_column_batch(&[entry(b"k", 5, ValueType::Value, b"v")]).expect("valid batch");
    let key_col = batch.columns.get_mut(0).expect("key column");
    key_col.data = alloc::vec![0u8; 8];
    let err = column_batch_into_entries(batch).expect_err("empty key must be rejected");
    assert!(
        matches!(err, crate::Error::InvalidHeader(m) if m.contains("user key is empty")),
        "expected an empty-key InvalidHeader, got {err:?}",
    );
}

#[test]
fn column_batch_match_entries_rejects_an_empty_key_row() {
    // The point-read matcher applies the same non-empty-key invariant as the
    // scan path: a matched empty key in a corrupt block is an error, not a hit.
    let mut batch =
        entries_to_column_batch(&[entry(b"k", 5, ValueType::Value, b"v")]).expect("valid batch");
    let key_col = batch.columns.get_mut(0).expect("key column");
    key_col.data = alloc::vec![0u8; 8];
    let cmp = crate::comparator::default_comparator();
    let err = column_batch_match_entries(&batch, b"", &cmp, None)
        .expect_err("matched empty key must be rejected");
    assert!(
        matches!(err, crate::Error::InvalidHeader(m) if m.contains("user key is empty")),
        "expected an empty-key InvalidHeader, got {err:?}",
    );
}
