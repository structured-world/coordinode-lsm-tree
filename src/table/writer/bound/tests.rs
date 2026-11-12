use super::*;
use crate::fs::StdFs;
use crate::{InternalValue, ValueType};
use alloc::sync::Arc;

/// `count` entries with keys of up to `key_len` bytes and values of `value_len`
/// bytes that no codec shrinks, in key order: what makes a codec grow its
/// blocks rather than shrink them.
fn entries(count: usize, key_len: usize, value_len: usize) -> Vec<InternalValue> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut noise = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 56) as u8
    };
    (0..count)
        .map(|i| {
            let mut key = format!("k{i:08}").into_bytes();
            key.resize(key_len.max(key.len()), b'k');
            let value: Vec<u8> = (0..value_len).map(|_| noise()).collect();
            InternalValue::from_components(key, value, i as u64, ValueType::Value)
        })
        .collect()
}

/// What writing `entries` puts through a writer, as the bound counts it.
fn load_of(entries: &[InternalValue]) -> RewriteLoad {
    let mut load = RewriteLoad::default();
    for e in entries {
        load.take_entry(
            e.key.user_key.len() as u64,
            e.value.len() as u64,
            e.key.seqno,
            1,
        );
    }
    load
}

/// Writes `entries` as one table with the writer `configure` sets up and
/// checks the bytes it takes against the bound a writer of the same settings
/// gives before any of them is written.
fn assert_bound_holds(
    name: &str,
    entries: &[InternalValue],
    configure: impl Fn(Writer) -> Writer,
) -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    assert_bound_holds_for(name, entries, &load_of(entries), configure, dir.path()).map(drop)
}

/// [`assert_bound_holds`] for a bound taken over `load`, the table written
/// into `dir`; returns its path and checksum.
fn assert_bound_holds_for(
    name: &str,
    entries: &[InternalValue],
    load: &RewriteLoad,
    configure: impl Fn(Writer) -> Writer,
    dir: &std::path::Path,
) -> crate::Result<(std::path::PathBuf, crate::Checksum)> {
    let probe = configure(Writer::new(dir.join("probe"), 1, 0, Arc::new(StdFs))?);
    let bound = probe.rewrite_bound(load, 1, probe.meta_payload_len()?);
    drop(probe);

    let path = dir.join("table");
    let mut writer = configure(Writer::new(path.clone(), 2, 0, Arc::new(StdFs))?);
    for entry in entries {
        writer.write(entry.clone())?;
    }
    let Some((_, checksum)) = writer.finish()? else {
        panic!("{name}: the table is written");
    };
    let written = std::fs::metadata(&path)?.len();
    assert!(
        written <= bound,
        "{name}: the table takes {written} bytes past the bound of {bound}"
    );
    // A bound far above what is written refuses work that fits: a slice cut
    // to it moves less per pass, and a merge it gates is turned away.
    assert!(
        bound <= 2 * written,
        "{name}: a bound of {bound} for a table of {written} bytes is too loose to use"
    );
    Ok((path, checksum))
}

/// Opens the table written to `path` under `checksum`, and checks it records
/// the longest key and value of `load`: what a later rewrite of it is
/// bounded by.
fn recover_recording(
    path: std::path::PathBuf,
    checksum: crate::Checksum,
    load: &RewriteLoad,
) -> crate::Result<crate::table::Table> {
    let table = crate::table::Table::recover(crate::table::RecoverParams::new(
        path,
        checksum,
        2,
        Arc::new(StdFs),
        crate::comparator::default_comparator(),
        Arc::new(crate::Cache::with_capacity_bytes(1_000_000)),
    ))?;
    assert_eq!(table.metadata.longest_user_key, Some(load.longest_key));
    assert_eq!(table.metadata.longest_value, load.longest_value);
    Ok(table)
}

/// A table records its longest key and value, which bound a rewrite of it
/// before it is read.
#[test]
fn a_table_records_its_longest_key_and_value() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let rows = entries(500, 24, 300);
    let load = load_of(&rows);
    let (path, checksum) = assert_bound_holds_for("plain", &rows, &load, |w| w, dir.path())?;
    let table = recover_recording(path, checksum, &load)?;
    assert_eq!(table.metadata.cell_columns, None, "a table of whole values");
    Ok(())
}

/// Rows written as cells of several shapes, each opening other field columns:
/// a group of them holds every column any of its rows opens, a fixed-width
/// one padded with zeros in the rows without it, and the bound counts them
/// from the field columns the rows name. The table records those columns, so
/// a rewrite of it is bounded without reading a row.
#[cfg(feature = "columnar")]
#[test]
fn the_rewrite_bound_holds_for_rows_written_as_cells() -> crate::Result<()> {
    use crate::blob_tree::field_row::{RowCell, RowField, TypeTag, encode_row};

    let mut rows = Vec::new();
    for i in 0..2_000u32 {
        let wide = [7u8; 64];
        let id = i.to_le_bytes();
        let note = format!("note {i}");
        let mut fields = vec![RowField::bytes(3, RowCell::Value(note.as_bytes()))];
        if i % 3 == 0 {
            fields.push(RowField {
                column: 4,
                tag: TypeTag::Fixed(64),
                cell: RowCell::Value(&wide),
            });
        }
        if i % 5 == 0 {
            fields.push(RowField {
                column: 9,
                tag: TypeTag::Fixed(4),
                cell: RowCell::Value(&id),
            });
        }
        rows.push(InternalValue::from_components(
            format!("k{i:08}").into_bytes(),
            encode_row(&fields)?,
            u64::from(i),
            ValueType::CellRow,
        ));
    }
    let mut shapes = CellShapes::default();
    for row in &rows {
        shapes.take_row(&row.value);
    }
    let load = RewriteLoad {
        cell_columns: shapes.columns_with(&CellShapes::default()),
        ..load_of(&rows)
    };
    let dir = tempfile::tempdir()?;
    let (path, checksum) = assert_bound_holds_for(
        "cells",
        &rows,
        &load,
        |w| {
            w.use_columnar(true)
                .use_cell_rows(true)
                .use_row_group_size(16 * 1_024)
                .use_columnar_page_size(4_096)
                .use_zone_map(true)
        },
        dir.path(),
    )?;
    let table = recover_recording(path, checksum, &load)?;
    assert_eq!(table.metadata.cell_columns, Some(shapes));
    Ok(())
}

/// A row taken past the columns a slice already holds takes only what widens
/// them: a column the slice holds at least as wide is not taken again, and a
/// new column, a wider type or the first references column is, and reported.
/// A slice decides from that report whether a row changes its bound.
#[test]
fn a_row_taken_past_known_columns_takes_only_what_widens_them() -> crate::Result<()> {
    use crate::blob_tree::field_row::{RowCell, RowField, TypeTag, encode_row};

    let narrow = [1u8; 4];
    let wide = [2u8; 8];
    fn fixed(column: u16, cell: &[u8]) -> crate::Result<RowField<'_>> {
        let width = u8::try_from(cell.len()).map_err(|_| crate::Error::Unrecoverable)?;
        Ok(RowField {
            column,
            tag: TypeTag::Fixed(width),
            cell: RowCell::Value(cell),
        })
    }
    let known_row = encode_row(&[
        RowField::bytes(3, RowCell::Value(b"note")),
        fixed(4, &narrow)?,
    ])?;
    let mut known = CellShapes::default();
    known.take_row(&known_row);

    let mut more = CellShapes::default();
    assert!(
        !more.take_row_past(&known_row, &known),
        "a row of the known columns widens nothing",
    );
    assert_eq!(more, CellShapes::default(), "and takes nothing in");

    let wider = encode_row(&[fixed(4, &wide)?])?;
    assert!(more.take_row_past(&wider, &known), "a wider type widens");
    let other = encode_row(&[fixed(9, &narrow)?])?;
    assert!(more.take_row_past(&other, &known), "a new column widens");
    assert!(
        !more.take_row_past(&other, &known),
        "a column taken once is not reported again",
    );

    let mut both = known.clone();
    both.absorb(&more);
    let mut all = CellShapes::default();
    for row in [&known_row, &wider, &other] {
        all.take_row(row);
    }
    assert_eq!(both, all, "what a row past known takes is what it adds");
    Ok(())
}

/// The bound holds for each setting that changes what a table takes: the
/// codecs, encryption, parity, per-KV checksums, hash index, sections and a
/// partitioned index and filter, alone and together, over values no codec
/// shrinks.
#[test]
fn the_rewrite_bound_holds_for_every_setting() -> crate::Result<()> {
    let small = entries(3_000, 16, 100);
    let large = entries(300, 40, 20_000);
    for (label, rows) in [("small values", &small), ("large values", &large)] {
        assert_bound_holds(&format!("{label}, defaults"), rows, |w| w)?;
        assert_bound_holds(&format!("{label}, small blocks"), rows, |w| {
            w.use_data_block_size(512)
                .use_data_block_restart_interval(1)
        })?;
        #[cfg(feature = "lz4")]
        assert_bound_holds(&format!("{label}, lz4"), rows, |w| {
            w.use_data_block_compression(CompressionType::Lz4)
                .use_index_block_compression(CompressionType::Lz4)
        })?;
        #[cfg(zstd_any)]
        assert_bound_holds(&format!("{label}, zstd"), rows, |w| {
            w.use_data_block_compression(CompressionType::Zstd(3))
                .use_index_block_compression(CompressionType::Zstd(3))
        })?;
        assert_bound_holds(&format!("{label}, hash index"), rows, |w| {
            w.use_data_block_hash_ratio(4.0)
        })?;
        assert_bound_holds(&format!("{label}, kv checksums"), rows, |w| {
            w.use_kv_checksums(
                crate::runtime_config::KvChecksumPolicy::AllLevels,
                crate::runtime_config::ChecksumAlgorithm::Xxh3_64,
            )
        })?;
        assert_bound_holds(&format!("{label}, sections"), rows, |w| {
            w.use_seqno_in_index(true).use_zone_map(true)
        })?;
        assert_bound_holds(&format!("{label}, partitioned"), rows, |w| {
            w.use_data_block_size(512)
                .use_adaptive_index(0)
                .use_index_partition_size(256)
                .use_partitioned_filter()
                .use_filter_partition_size(256)
        })?;
        #[cfg(feature = "encryption")]
        assert_bound_holds(&format!("{label}, encrypted"), rows, |w| {
            w.use_encryption(Some(Arc::new(crate::encryption::Aes256GcmProvider::new(
                &[7u8; 32],
            ))))
        })?;
        #[cfg(feature = "page_ecc")]
        assert_bound_holds(&format!("{label}, parity"), rows, |w| {
            w.use_ecc(Some(crate::table::block::EccParams::RS_4_2))
        })?;
        #[cfg(feature = "columnar")]
        assert_bound_holds(&format!("{label}, columnar"), rows, |w| {
            w.use_columnar(true)
                .use_row_group_size(4_096)
                .use_columnar_page_size(1_024)
                .use_zone_map(true)
        })?;
        #[cfg(all(feature = "lz4", feature = "encryption", feature = "page_ecc"))]
        assert_bound_holds(&format!("{label}, everything"), rows, |w| {
            w.use_data_block_size(512)
                .use_data_block_compression(CompressionType::Lz4)
                .use_index_block_compression(CompressionType::Lz4)
                .use_data_block_hash_ratio(4.0)
                .use_kv_checksums(
                    crate::runtime_config::KvChecksumPolicy::AllLevels,
                    crate::runtime_config::ChecksumAlgorithm::Xxh3_64,
                )
                .use_seqno_in_index(true)
                .use_zone_map(true)
                .use_adaptive_index(0)
                .use_index_partition_size(256)
                .use_partitioned_filter()
                .use_filter_partition_size(256)
                .use_encryption(Some(Arc::new(crate::encryption::Aes256GcmProvider::new(
                    &[7u8; 32],
                ))))
                .use_ecc(Some(crate::table::block::EccParams::RS_4_2))
        })?;
    }
    Ok(())
}
