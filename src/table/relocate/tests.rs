use super::*;
use crate::cache::Cache;
use crate::descriptor_table::DescriptorTable;
use crate::fs::StdFs;
use crate::table::Writer;
#[cfg(feature = "columnar")]
use crate::{SeqNo, hash::hash64};
use alloc::sync::Arc;
use test_log::test;

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

fn recover_at(file: &Path, checksum: Checksum, table_id: TableId) -> crate::Result<Table> {
    #[cfg(feature = "metrics")]
    let metrics = Arc::new(Metrics::default());
    Table::recover(
        file.to_path_buf(),
        checksum,
        0,
        0,
        table_id,
        Arc::new(Cache::with_capacity_bytes(1_000_000)),
        Some(Arc::new(DescriptorTable::new(10))),
        Arc::new(StdFs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        metrics,
    )
}

#[cfg(feature = "columnar")]
#[test]
fn relocate_reuses_blocks_and_masks_deleted_rows() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let src_path = dir.path().join("src");
    let out_path = dir.path().join("out");

    let n = 96u32;
    // Positions follow write (= key) order.
    let deleted = [4u32, 7, 40, 95];

    // Source: a columnar segment with a zone map and NO deletes.
    let mut writer = Writer::new(src_path.clone(), 0, 0, Arc::new(StdFs))?
        .use_columnar(true)
        .use_zone_map(true);
    for i in 0..n {
        let key = format!("k{i:04}").into_bytes();
        writer.write(InternalValue::from_components(
            key,
            b"val",
            1,
            crate::ValueType::Value,
        ))?;
    }
    let (_, src_checksum) = writer.finish()?.expect("source table written");
    let source = recover_at(&src_path, src_checksum, 0)?;

    // Relocate into a new segment (id 1) carrying the bitmap.
    let mut bitmap = DeleteBitmap::new();
    for &row in &deleted {
        bitmap.insert(row);
    }
    let out_checksum =
        source.relocate_columnar_with_deletes(&out_path, &StdFs, 1, &bitmap, SyncMode::Normal)?;

    let relocated = recover_at(&out_path, out_checksum, 1)?;

    // (i) format flags + id preserved; (ii) deleted rows masked, live found.
    assert_eq!(relocated.metadata.id, 1, "meta carries the new table id");
    assert!(relocated.metadata.columnar, "columnar flag preserved");
    for i in 0..n {
        let key = format!("k{i:04}").into_bytes();
        let got = relocated.get(&key, SeqNo::MAX, hash64(&key))?;
        if deleted.contains(&i) {
            assert!(
                got.is_none(),
                "deleted row {i} must read absent after relocate"
            );
        } else {
            let got = got.expect("live row must survive relocate");
            assert_eq!(&*got.value, b"val", "live value preserved verbatim");
        }
    }
    Ok(())
}

#[test]
fn relocate_rejects_row_major_segment() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let src_path = dir.path().join("src");

    // Row-major (no columnar): block reuse must refuse and let the caller CoW.
    let mut writer = Writer::new(src_path.clone(), 0, 0, Arc::new(StdFs))?.use_zone_map(true);
    writer.write(InternalValue::from_components(
        b"a",
        b"v",
        1,
        crate::ValueType::Value,
    ))?;
    let (_, checksum) = writer.finish()?.expect("table written");
    let source = recover_at(&src_path, checksum, 0)?;

    let out_path = dir.path().join("out");
    let mut bitmap = DeleteBitmap::new();
    bitmap.insert(0);
    let err = source
        .relocate_columnar_with_deletes(&out_path, &StdFs, 1, &bitmap, SyncMode::Normal)
        .unwrap_err();
    assert!(
        matches!(err, crate::Error::FeatureUnsupported(_)),
        "row-major segment must be rejected, got {err:?}",
    );
    Ok(())
}
