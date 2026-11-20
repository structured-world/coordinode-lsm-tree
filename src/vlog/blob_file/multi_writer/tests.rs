/// A writer copies whole the blocks of at most [`super::MAX_COPY_SOURCES`]
/// sources, each holding a file of its own open: a source it copies already
/// still fits, one more does not.
#[test]
fn whole_block_copies_are_held_to_a_bounded_number_of_sources() -> crate::Result<()> {
    use super::*;
    use crate::fs::StdFs;

    let folder = tempfile::tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let mut writer =
        MultiWriter::new(SequenceNumberCounter::default(), folder.path(), 7, None, fs)?;
    assert!(
        writer.can_copy_block(0, b"a", b"z"),
        "the first source fits"
    );
    writer.copy_sources = (0..MAX_COPY_SOURCES as u64).collect();
    assert!(
        writer.can_copy_block(3, b"a", b"z"),
        "a source it copies already"
    );
    assert!(
        !writer.can_copy_block(MAX_COPY_SOURCES as u64, b"a", b"z"),
        "one more source than the bound"
    );
    Ok(())
}

/// A synced flush cut into more blob files than
/// [`crate::fs::KEEP_OPEN_LIMIT`] keeps only that many writing handles open
/// for its install: the rest close at their rotation.
#[test]
fn a_flush_of_many_blob_files_keeps_a_bounded_number_of_files_open() -> crate::Result<()> {
    use super::*;
    use crate::fs::{FaultFs, StdFs, SyncKind};

    let folder = tempfile::tempdir()?;
    let faulty = FaultFs::new(StdFs);
    let injector = faulty.injector();
    let fs: Arc<dyn Fs> = Arc::new(faulty);
    let mut writer =
        MultiWriter::new(SequenceNumberCounter::default(), folder.path(), 7, None, fs)?
            .use_target_size(1)
            .use_durability(crate::config::FlushDurability::Deferred)
            .use_keep_open(true);
    let files = 3 * crate::fs::KEEP_OPEN_LIMIT;
    for i in 0..files {
        let key = format!("key{i:05}");
        writer.write_fresh(key.as_bytes(), 0, b"value", CompressionType::None, 0)?;
    }
    let (written, kept) = writer.finish_keeping_files()?;
    assert_eq!(written.len(), files, "one file per value");
    assert_eq!(kept.len(), crate::fs::KEEP_OPEN_LIMIT);
    // Each file past the limit is synced through its handle before it
    // closes, once; the kept ones are left to the install.
    let kept_ids: Vec<BlobFileId> = kept.iter().map(|(id, _)| *id).collect();
    let mut expected: Vec<std::path::PathBuf> = written
        .iter()
        .filter(|file| !kept_ids.contains(&file.id()))
        .map(|file| file.path().to_path_buf())
        .collect();
    expected.sort();
    let mut synced: Vec<std::path::PathBuf> = injector
        .take_syncs()
        .into_iter()
        .filter(|(_, kind)| matches!(kind, SyncKind::File | SyncKind::FileData))
        .map(|(path, _)| path)
        .collect();
    synced.sort();
    assert_eq!(expected.len(), files - crate::fs::KEEP_OPEN_LIMIT);
    assert_eq!(synced, expected, "each excess file synced once");
    Ok(())
}

/// A file past the keep-open limit whose sync at its rotation fails is
/// removed, its handle given up: no version will name it. Guards against the
/// file's handle dropping unmarked, which leaves its descriptor in the shared
/// table and the file on disk.
#[test]
fn a_file_whose_sync_at_rotation_fails_is_removed() -> crate::Result<()> {
    use super::*;
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, StdFs, SyncKind};

    let folder = tempfile::tempdir()?;
    let faulty = FaultFs::new(StdFs);
    let injector = faulty.injector();
    let fs: Arc<dyn Fs> = Arc::new(faulty);
    let mut writer =
        MultiWriter::new(SequenceNumberCounter::default(), folder.path(), 7, None, fs)?
            .use_target_size(1)
            .use_durability(crate::config::FlushDurability::Deferred)
            .use_keep_open(true);
    // The kept files take no sync: the first one is the first file past the
    // limit, at the rotation that closes it.
    injector
        .arm(FaultRule::new(FaultOp::SyncData, Fault::Error(crate::io::ErrorKind::Other)).once());
    let mut failed = false;
    for i in 0..2 * crate::fs::KEEP_OPEN_LIMIT {
        let key = format!("key{i:05}");
        if writer
            .write_fresh(key.as_bytes(), 0, b"value", CompressionType::None, 0)
            .is_err()
        {
            failed = true;
            break;
        }
    }
    assert!(failed, "the sync at a rotation failed the write");
    let synced: Vec<std::path::PathBuf> = injector
        .take_syncs()
        .into_iter()
        .filter(|(_, kind)| matches!(kind, SyncKind::FileData))
        .map(|(path, _)| path)
        .collect();
    let [failed_file] = synced.as_slice() else {
        panic!("one file sync, the failed one: {synced:?}");
    };
    assert!(
        !failed_file.exists(),
        "the file whose sync failed is removed: {}",
        failed_file.display()
    );
    drop(writer);
    Ok(())
}

/// A writer holds values once one is written, in any class, and still after
/// the class switch that parks the file holding it: a flush finishes it on a
/// thread of its own only then.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn a_writer_holds_values_once_any_class_was_written() -> crate::Result<()> {
    use super::*;
    use crate::fs::StdFs;

    let folder = tempfile::tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let mut writer =
        MultiWriter::new(SequenceNumberCounter::default(), folder.path(), 7, None, fs)?
            .use_target_size(u64::MAX)
            .use_lifetime_groups(crate::config::LifetimeGroups::new(2).expect("two groups"));
    assert!(!writer.holds_values(), "nothing written yet");
    writer.write_fresh(b"a", 0, b"value", CompressionType::None, 0)?;
    assert!(writer.holds_values(), "a value in the active file");
    writer.select_output(CompressionType::None, 1)?;
    assert!(writer.holds_values(), "the value's file is parked");
    assert!(
        writer.finish()?.len() == 1,
        "the one file holding the value"
    );
    Ok(())
}

/// Values of two key domains, in two lifetime classes, land in blob files of
/// one domain each: the file a class switch parks is closed at the domain's
/// end with the active one, so expiring a domain leaves its files wholly dead.
#[test_log::test]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test code: every key is non-empty"
)]
fn blob_files_never_hold_values_of_two_key_domains() -> crate::Result<()> {
    use super::*;
    use crate::fs::StdFs;

    /// One domain per first byte.
    struct FirstByte;
    impl crate::KeyDomains for FirstByte {
        fn domain_end(&self, key: &[u8]) -> Option<crate::UserKey> {
            match key.first() {
                Some(&b) if b < u8::MAX => Some(crate::UserKey::from([b + 1].as_slice())),
                _ => None,
            }
        }
    }

    let folder = tempfile::tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let mut writer =
        MultiWriter::new(SequenceNumberCounter::default(), folder.path(), 7, None, fs)?
            .use_target_size(u64::MAX)
            .use_lifetime_groups(crate::config::LifetimeGroups::new(2).expect("two groups"))
            .use_key_domains(
                Some(Arc::new(FirstByte)),
                crate::comparator::default_comparator(),
            );
    for (seqno, (key, class)) in [(b"a1", 0), (b"a2", 1), (b"a3", 0), (b"b1", 1), (b"b2", 0)]
        .into_iter()
        .enumerate()
    {
        writer.write_fresh(key, seqno as u64, b"value", CompressionType::None, class)?;
    }
    let files = writer.finish()?;

    let ranges: Vec<(u8, u8)> = files
        .iter()
        .map(|file| {
            let range = &file.0.meta.key_range;
            (range.min()[0], range.max()[0])
        })
        .collect();
    assert!(
        ranges.iter().all(|(lo, hi)| lo == hi),
        "a blob file holds values of two domains: {ranges:?}",
    );
    assert_eq!(
        files.iter().map(|file| file.0.meta.item_count).sum::<u64>(),
        5,
        "every value is written once",
    );
    Ok(())
}

/// A blob file whose dictionary cannot be resolved must leave nothing behind.
///
/// The writer compresses the frames under the codec the file records, so the
/// missing dictionary fails the finish when the last block is compressed.
/// No `BlobFile` exists then, so the `Drop` that normally unlinks the file and
/// evicts its descriptor never runs: the file would linger as an orphan no
/// version references, and the shared descriptor table would keep an open
/// handle to it for the life of the process.
// Imported inside the test rather than at module scope: every item here is
// zstd-only, and the module itself is not.
#[test_log::test]
#[cfg(zstd_any)]
fn a_finished_file_whose_dictionary_is_missing_leaves_nothing_behind() -> crate::Result<()> {
    use super::*;
    use crate::fs::StdFs;

    let folder = tempfile::tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let descriptor_table = Arc::new(DescriptorTable::new(10));

    // Records a dictionary NEITHER the (empty) set NOR the writer holds, which
    // is what a relocation does when it stamps a source's codec.
    let unheld = CompressionType::ZstdDict {
        level: 3,
        dict_id: 0xDEAD_BEEF,
    };

    let mut writer = MultiWriter::new(
        SequenceNumberCounter::default(),
        folder.path(),
        7,
        Some(descriptor_table.clone()),
        fs.clone(),
    )?
    .use_target_size(u64::MAX)
    .use_passthrough_compression(unheld);

    let blob_file_id = writer.active_writer.blob_file_id();
    let path = writer.active_writer.path.clone();
    // A small value waits for its block, which compresses when the file
    // finishes.
    writer.write(b"key", 0, b"a value under a dictionary nobody holds")?;

    // `BlobFile` is not `Debug`, so match rather than `expect_err`.
    let Err(err) = writer.finish() else {
        panic!("an unresolvable recorded dictionary must fail the finish");
    };
    assert!(
        matches!(
            err,
            crate::Error::ZstdDictMismatch {
                expected: 0xDEAD_BEEF,
                ..
            }
        ),
        "the failure names the id the file records; got {err:?}",
    );

    assert!(
        !fs.exists(&path)?,
        "the finished file is removed, not left as an orphan no version names",
    );
    assert!(
        descriptor_table
            .access_for_blob_file(&(7, blob_file_id).into())
            .is_none(),
        "its descriptor is not left in the shared table",
    );
    Ok(())
}

/// A writer that fails to finish takes the files already finished with it.
///
/// `finish` closes one file per codec. When a later one fails, the files it
/// already published are handles nobody will ever receive: dropping them
/// unmarked leaves their bytes on disk and their descriptors in the shared
/// table, for a relocation that as a whole did not happen.
#[test_log::test]
#[cfg(zstd_any)]
fn a_failed_finish_leaves_no_file_behind() -> crate::Result<()> {
    use super::*;
    use crate::fs::StdFs;

    let folder = tempfile::tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let descriptor_table = Arc::new(DescriptorTable::new(10));

    let mut writer = MultiWriter::new(
        SequenceNumberCounter::default(),
        folder.path(),
        7,
        Some(descriptor_table.clone()),
        fs.clone(),
    )?
    .use_target_size(u64::MAX);

    // A dictionary nothing can resolve, parked behind a plain writer that
    // finishes first and succeeds.
    writer.select_output(
        CompressionType::ZstdDict {
            level: 3,
            dict_id: 0xDEAD_BEEF,
        },
        0,
    )?;
    writer.write(b"a", 0, b"a value under a dictionary nobody holds")?;
    let dict_path = writer.active_writer.path.clone();
    writer.select_output(CompressionType::None, 0)?;
    writer.write(b"b", 0, b"plain")?;
    let plain_id = writer.active_writer.blob_file_id();
    let plain_path = writer.active_writer.path.clone();

    assert!(
        writer.finish().is_err(),
        "the unresolvable file fails the finish"
    );
    assert!(!fs.exists(&dict_path)?, "the failing file is removed");
    assert!(
        !fs.exists(&plain_path)?,
        "and so is the one that finished before it"
    );
    assert!(
        descriptor_table
            .access_for_blob_file(&(7, plain_id).into())
            .is_none(),
        "its descriptor is not left in the shared table",
    );
    Ok(())
}

/// Values of interleaved lifetime classes fill one file per class, each file
/// records its class, and a class past the configured groups goes to the last
/// group rather than opening another file: the open files stay bounded by the
/// group count.
#[test_log::test]
#[expect(clippy::expect_used, reason = "3 is within the group bound")]
fn interleaved_lifetime_classes_fill_one_file_per_group() -> crate::Result<()> {
    use super::*;
    use crate::{config::LifetimeGroups, fs::StdFs};

    let folder = tempfile::tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let groups = LifetimeGroups::new(3).expect("within the bound");

    let mut writer =
        MultiWriter::new(SequenceNumberCounter::default(), folder.path(), 7, None, fs)?
            .use_target_size(u64::MAX)
            .use_lifetime_groups(groups);

    let mut handles = Vec::new();
    for i in 0u8..120 {
        // Classes 0..=5 against three groups: 3, 4 and 5 belong to group 2.
        let class = i % 6;
        writer.select_lifetime_class(class)?;
        let key = format!("key-{i:05}");
        let handle = writer.write(key.as_bytes(), 0, b"value")?;
        handles.push((class.min(2), handle));
    }
    let files = writer.finish()?;

    assert_eq!(
        files.len(),
        3,
        "one file per group, however many classes asked"
    );
    for (class, handle) in &handles {
        let Some(file) = files.iter().find(|f| f.id() == handle.vhandle.blob_file_id) else {
            panic!("handle {handle:?} points at a file the writer did not produce");
        };
        assert_eq!(
            file.lifetime_class(),
            *class,
            "every value lands in a file of its own group",
        );
    }
    Ok(())
}

/// With a single group every value shares one file of class 0, exactly as
/// without lifetime grouping.
#[test_log::test]
fn a_single_lifetime_group_writes_one_class() -> crate::Result<()> {
    use super::*;
    use crate::fs::StdFs;

    let folder = tempfile::tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer =
        MultiWriter::new(SequenceNumberCounter::default(), folder.path(), 7, None, fs)?
            .use_target_size(u64::MAX);
    for i in 0u8..10 {
        writer.select_lifetime_class(i)?;
        writer.write(format!("key-{i:03}").as_bytes(), 0, b"value")?;
    }
    let files = writer.finish()?;

    assert_eq!(files.len(), 1);
    assert!(files.iter().all(|file| file.lifetime_class() == 0));
    Ok(())
}

/// Sources of different codecs interleaved by key fill one file per codec.
///
/// A relocation merges its sources by key, so when two compression generations
/// cover the same keys the codec of consecutive frames alternates on nearly
/// every item. Rotating the output on each change would turn one relocation
/// into a file per value.
#[test_log::test]
#[cfg(zstd_any)]
fn interleaved_source_codecs_fill_one_file_each() -> crate::Result<()> {
    use super::*;
    use crate::fs::StdFs;

    let folder = tempfile::tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer =
        MultiWriter::new(SequenceNumberCounter::default(), folder.path(), 7, None, fs)?
            .use_target_size(u64::MAX);

    let mut handles = Vec::new();
    for i in 0u32..100 {
        let codec = if i % 2 == 0 {
            CompressionType::None
        } else {
            CompressionType::Zstd(3)
        };
        writer.select_output(codec, 0)?;
        let key = format!("key-{i:05}");
        let handle = writer.write(key.as_bytes(), 0, b"frame-bytes")?;
        handles.push((codec, handle));
    }
    let files = writer.finish()?;

    assert_eq!(files.len(), 2, "one file per codec, not one per switch");
    for (codec, handle) in &handles {
        let Some(file) = files.iter().find(|f| f.id() == handle.vhandle.blob_file_id) else {
            panic!("handle {handle:?} points at a file the writer did not produce");
        };
        assert_eq!(
            file.compression(),
            *codec,
            "every frame lands in a file that records its own codec",
        );
    }
    Ok(())
}
