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

/// A merge-on-read relocated columnar SST appends a NEW delete bitmap, so its
/// re-encoded meta must describe THAT bitmap: `verify_metadata_bounds` (the
/// repair-time forgery cross-check) authenticates `descriptor#delete_bitmap_len`
/// and `descriptor#delete_bitmap_hash` against the on-disk section. If the
/// relocation copied the source's (absent-bitmap) descriptors verbatim, the
/// check would flag the healthy table and `repair_with_salvage` would quarantine
/// it. The descriptors must be repointed to the appended bitmap.
#[cfg(feature = "columnar")]
#[test]
fn relocated_mor_table_passes_metadata_bounds_cross_check() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let src_path = dir.path().join("src");
    let out_path = dir.path().join("out");

    let n = 96u32;
    let mut writer = Writer::new(src_path.clone(), 0, 0, Arc::new(StdFs))?
        .use_columnar(true)
        .use_zone_map(true);
    for i in 0..n {
        writer.write(InternalValue::from_components(
            format!("k{i:04}").into_bytes(),
            b"val",
            1,
            crate::ValueType::Value,
        ))?;
    }
    let (_, src_checksum) = writer.finish()?.expect("source table written");
    let source = recover_at(&src_path, src_checksum, 0)?;

    let mut bitmap = DeleteBitmap::new();
    for &row in &[4u32, 7, 40, 95] {
        bitmap.insert(row);
    }
    let out_checksum =
        source.relocate_columnar_with_deletes(&out_path, &StdFs, 1, &bitmap, SyncMode::Normal)?;
    let relocated = recover_at(&out_path, out_checksum, 1)?;

    // The re-encoded meta describes the appended bitmap, so the forgery
    // cross-check accepts the healthy relocated table.
    relocated.verify_metadata_bounds(false)?;
    Ok(())
}

/// A tight-space RESTRICTED view reads its live suffix only: `scan()` starts at
/// the bound and numbers rows from zero, while the relocation copies the whole
/// physical data section — punched prefix blocks included — and publishes the
/// copy WITHOUT the restriction. The bitmap would then mask shifted rows and
/// reads could reach the copied zero blocks, so block reuse must refuse and let
/// the caller rewrite copy-on-write.
#[cfg(feature = "columnar")]
#[test]
fn relocate_rejects_a_restricted_view() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let src_path = dir.path().join("src");

    let mut writer = Writer::new(src_path.clone(), 0, 0, Arc::new(StdFs))?
        .use_columnar(true)
        .use_zone_map(true);
    for i in 0..32u32 {
        writer.write(InternalValue::from_components(
            format!("k{i:04}").into_bytes(),
            b"val",
            1,
            crate::ValueType::Value,
        ))?;
    }
    let (_, checksum) = writer.finish()?.expect("table written");
    let source = recover_at(&src_path, checksum, 0)?;
    let restricted = source.reopen_restricted(crate::UserKey::from(&b"k0016"[..]))?;

    let out_path = dir.path().join("out");
    let mut bitmap = DeleteBitmap::new();
    bitmap.insert(0);
    let err = restricted
        .relocate_columnar_with_deletes(&out_path, &StdFs, 1, &bitmap, SyncMode::Normal)
        .unwrap_err();
    assert!(
        matches!(err, crate::Error::FeatureUnsupported(_)),
        "a restricted view must be rejected, got {err:?}",
    );
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

/// A merge-on-read relocated columnar SST keeps its PHYSICAL rows (the
/// delete bitmap only masks them) and copies the source `linked_blob_files`
/// accounting verbatim — so the blob-link cross-check must derive its
/// accounting from the UNMASKED physical rows. Deriving from the masked
/// view marks a healthy relocated table corrupt, and repair-with-salvage
/// would then quarantine it (its retained range tombstones make it
/// unsalvageable).
#[cfg(feature = "columnar")]
#[test]
fn relocated_mor_table_passes_the_blob_link_cross_check() -> crate::Result<()> {
    use crate::blob_tree::handle::BlobIndirection;
    use crate::coding::Encode;
    use crate::vlog::ValueHandle;

    let dir = tempfile::tempdir()?;
    let src_path = dir.path().join("src");
    let out_path = dir.path().join("out");

    // A columnar segment whose every row is a blob indirection into file 5,
    // with the blob-link accounting the writer derives from those rows.
    let n = 8u32;
    let mut writer = Writer::new(src_path.clone(), 0, 0, Arc::new(StdFs))?
        .use_columnar(true)
        .use_zone_map(true);
    let mut bytes_sum = 0u64;
    let mut on_disk_sum = 0u64;
    for i in 0..n {
        let ind = BlobIndirection {
            vhandle: ValueHandle {
                blob_file_id: 5,
                offset: u64::from(i) * 100,
                on_disk_size: 40,
            },
            size: 80,
        };
        bytes_sum += u64::from(ind.size);
        on_disk_sum += u64::from(ind.vhandle.on_disk_size);
        let mut val = alloc::vec::Vec::new();
        ind.encode_into(&mut val)?;
        writer.write(InternalValue::from_components(
            format!("k{i:04}").into_bytes(),
            val,
            1,
            crate::ValueType::Indirection,
        ))?;
    }
    writer.link_blob_file(5, n as usize, bytes_sum, on_disk_sum);
    let (_, src_checksum) = writer.finish()?.expect("source table written");
    let source = recover_at(&src_path, src_checksum, 0)?;

    // Relocate with one indirection row MASKED: the physical rows (and the
    // copied accounting) still include it.
    let mut bitmap = DeleteBitmap::new();
    bitmap.insert(3);
    let out_checksum =
        source.relocate_columnar_with_deletes(&out_path, &StdFs, 1, &bitmap, SyncMode::Normal)?;
    let relocated = recover_at(&out_path, out_checksum, 1)?;

    relocated
        .verify_blob_links()
        .expect("a healthy relocated MoR table passes the blob-link cross-check");
    Ok(())
}
