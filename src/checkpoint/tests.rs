use super::*;
use crate::fs::{FsOpenOptions, MemFs, StdFs};
use std::io::{Read, Write};

/// `link_or_copy_cross_fs` must transparently stream bytes through
/// both trait objects when source and destination back ends differ
/// (here: `StdFs` source vs. `MemFs` target — the `MemFs` backend
/// has no way to see the on-disk source file, so the hard-link
/// attempt returns `NotFound` and we fall through to a streamed
/// copy). Verifies BOTH the copy lands AND the two filesystems
/// stay independent under subsequent mutation.
#[test]
fn cross_fs_link_or_copy_streams_through_trait() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("payload.bin");
    std::fs::write(&src, b"cross-fs-payload").unwrap();

    let std_fs: Arc<dyn Fs> = Arc::new(StdFs);
    let mem_fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    mem_fs.create_dir_all(Path::new("/dst")).unwrap();

    let dst = Path::new("/dst/payload.bin");
    // use_reflink = true, but src/dst are different backends (StdFs vs
    // MemFs) so the reflink + hard_link paths are both gated out by the
    // shared-namespace check, exercising the cross-fs streamed copy.
    let bytes = link_or_copy_cross_fs(&std_fs, &src, &mem_fs, dst, SyncMode::Normal, true).unwrap();
    assert_eq!(bytes, b"cross-fs-payload".len() as u64);

    // Bytes landed in MemFs.
    let mut buf = String::new();
    mem_fs
        .open(dst, &FsOpenOptions::new().read(true))
        .unwrap()
        .read_to_string(&mut buf)
        .unwrap();
    assert_eq!(buf, "cross-fs-payload");

    // Mutating `dst` via MemFs must NOT affect the StdFs source —
    // proves the streamed copy produced an independent file rather
    // than aliasing.
    let mut writer = mem_fs
        .open(dst, &FsOpenOptions::new().write(true).truncate(true))
        .unwrap();
    writer.write_all(b"mutated-via-mem-fs").unwrap();
    drop(writer);

    assert_eq!(std::fs::read(&src).unwrap(), b"cross-fs-payload");

    let mut after = String::new();
    mem_fs
        .open(dst, &FsOpenOptions::new().read(true))
        .unwrap()
        .read_to_string(&mut after)
        .unwrap();
    assert_eq!(after, "mutated-via-mem-fs");
}

/// The checkpoint records a restricted table's recovery bound from the CAPTURED
/// version view (`restrict_lower_bound`), NOT by re-reading the live
/// `.restrict-bound` sidecar file — a concurrent tight-space compaction can be
/// rewriting that file for the same SST id. Proven by leaving a STALE live sidecar
/// (a DIFFERENT bound) beside the source SST: the checkpoint's sidecar must encode
/// the captured bound, never the stale file's. Under the pre-fix copy-the-file
/// logic the checkpoint carried the stale bound instead.
#[test]
fn checkpoint_binds_restrict_sidecar_to_captured_view_not_live_file() -> crate::Result<()> {
    use crate::blob_tree::FragmentationMap;
    use crate::cache::Cache;
    use crate::descriptor_table::DescriptorTable;
    use crate::table::{Table, Writer};
    use crate::version::{BlobFileList, Level, Run, Version};
    use crate::{InternalValue, TreeType, ValueType};

    let src_dir = tempfile::tempdir()?;
    let dst_dir = tempfile::tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let src_tables = src_dir.path().join(TABLES_FOLDER);
    fs.create_dir_all(&src_tables)?;
    let sst = src_tables.join("0");

    // A multi-block SST so a restriction bound lands on a real block boundary.
    let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    for i in 0..256u32 {
        w.write(InternalValue::from_components(
            format!("k{i:05}").into_bytes(),
            format!("v{i}").into_bytes(),
            u64::from(i) + 1,
            ValueType::Value,
        ))?;
    }
    let (_, checksum) = w.finish()?.expect("table written");

    // Recover, then build the restricted VIEW at k00100 (no punch needed here:
    // reopen_restricted reads the live suffix digest off the whole file).
    let table = Table::recover(
        sst.clone(),
        checksum,
        0,
        0,
        0,
        Arc::new(Cache::with_capacity_bytes(1_000_000)),
        Some(Arc::new(DescriptorTable::new(10))),
        Arc::clone(&fs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )?;
    let restricted = table.reopen_restricted(b"k00100".to_vec().into())?;

    // A STALE live sidecar with a DIFFERENT bound beside the SOURCE SST: the copy
    // path would carry this, the captured-bound path must ignore it.
    crate::restrict_bound::write(&*fs, &sst, None, 0, b"k00050", SyncMode::Normal)?;

    // Build a one-table version and checkpoint (link) it into dst.
    let run = Arc::new(Run::new(vec![restricted]).expect("non-empty run"));
    let version = Version::from_levels(
        1,
        TreeType::Standard,
        vec![Level::from_runs(vec![run])],
        BlobFileList::default(),
        FragmentationMap::default(),
    );
    let dst_root = dst_dir.path();
    fs.create_dir_all(&dst_root.join(TABLES_FOLDER))?;
    link_tables(&version, dst_root, &fs, SyncMode::Normal, false)?;

    // The checkpoint's sidecar encodes the CAPTURED bound (k00100), not the stale
    // live file's (k00050).
    let dst_sst = dst_root.join(TABLES_FOLDER).join("0");
    match crate::restrict_bound::read(&*fs, &dst_sst, None)? {
        crate::restrict_bound::SidecarRead::Present(id, bound) => {
            assert_eq!(id, 0, "the sidecar binds the table id");
            assert_eq!(
                bound, b"k00100",
                "checkpoint must record the captured bound, not the stale live file's",
            );
        }
        _ => {
            panic!("checkpoint must write a valid restrict-bound sidecar for the restricted table")
        }
    }
    Ok(())
}

// Removed: `write_current_for_version_rejects_corrupt_footer_size_hint`.
// The checkpoint write path now goes through
// `ManifestArchiveReader::open` (canonical CURRENT digest path),
// which has head-mirror fallback for a torn tail size hint —
// recovery succeeds instead of erroring, which is the correct
// behaviour. Tail / head-mirror bounds checks are covered by
// `manifest_blocks::reader::tests::reader_fails_when_tail_corrupt_and_no_mirror`
// and siblings.
