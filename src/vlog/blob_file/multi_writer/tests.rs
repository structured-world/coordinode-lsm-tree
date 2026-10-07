/// A finished blob file whose recorded dictionary cannot be resolved must leave
/// nothing behind.
///
/// `finish` writes and syncs the file, then the handle is built. If building it
/// fails, no `BlobFile` exists, so the `Drop` that normally unlinks the file and
/// evicts its descriptor never runs: the file would linger as an orphan no
/// version references, and the shared descriptor table would keep an open handle
/// to it for the life of the process.
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
    // Raw, because passthrough means the caller hands over already-encoded
    // bytes; the writer itself compresses nothing.
    writer.write_raw(b"key", 0, b"already-encoded-by-the-source", 29)?;

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
    writer.write_raw(b"a", 0, b"encoded-under-a-dictionary", 26)?;
    let dict_path = writer.active_writer.path.clone();
    writer.select_output(CompressionType::None, 0)?;
    writer.write_raw(b"b", 0, b"plain", 5)?;
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
        let Some(file) = files.iter().find(|f| f.id() == handle.blob_file_id) else {
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
        let handle = writer.write_raw(key.as_bytes(), 0, b"frame-bytes", 11)?;
        handles.push((codec, handle));
    }
    let files = writer.finish()?;

    assert_eq!(files.len(), 2, "one file per codec, not one per switch");
    for (codec, handle) in &handles {
        let Some(file) = files.iter().find(|f| f.id() == handle.blob_file_id) else {
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
