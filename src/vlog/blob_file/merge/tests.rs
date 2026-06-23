use super::super::scanner::Scanner;
use super::*;
use crate::{Slice, fs::StdFs, vlog::blob_file::writer::Writer as BlobFileWriter};
use tempfile::tempdir;
use test_log::test;

#[test]
fn blob_file_merger_seqno() -> crate::Result<()> {
    let dir = tempdir()?;

    let blob_file_path = dir.path().join("0");
    {
        {
            let mut writer = BlobFileWriter::new(&blob_file_path, 0, 0, &StdFs)?;

            writer.write(b"a", 1, &b"1".repeat(100))?;
            writer.write(b"a", 0, &b"0".repeat(100))?;

            writer.finish()?;
        }
    }

    {
        let mut merger = MergeScanner::new(vec![Scanner::new(&blob_file_path, &StdFs, 0)?]);

        assert_eq!(
            (Slice::from(b"a"), Slice::from(b"1".repeat(100))),
            merger
                .next()
                .map(|result| result.map(|(entry, _)| (entry.key, entry.value)))
                .unwrap()?,
        );
        assert_eq!(
            (Slice::from(b"a"), Slice::from(b"0".repeat(100))),
            merger
                .next()
                .map(|result| result.map(|(entry, _)| (entry.key, entry.value)))
                .unwrap()?,
        );

        assert!(merger.next().is_none());
    }

    Ok(())
}

#[test]
fn blob_file_merger() -> crate::Result<()> {
    let dir = tempdir()?;

    let blob_file_0_path = dir.path().join("0");
    let blob_file_1_path = dir.path().join("1");

    {
        let keys = [b"a", b"c", b"e"];

        {
            let mut writer = BlobFileWriter::new(&blob_file_0_path, 0, 0, &StdFs)?;

            for key in keys {
                writer.write(key, 0, &key.repeat(100))?;
            }

            writer.finish()?;
        }
    }

    {
        let keys = [b"b", b"d"];

        {
            let mut writer = BlobFileWriter::new(&blob_file_1_path, 1, 0, &StdFs)?;

            for key in keys {
                writer.write(key, 1, &key.repeat(100))?;
            }

            writer.finish()?;
        }
    }

    {
        let mut merger = MergeScanner::new(vec![
            Scanner::new(&blob_file_0_path, &StdFs, 0)?,
            Scanner::new(&blob_file_1_path, &StdFs, 1)?,
        ]);

        let merged_keys = [b"a", b"b", b"c", b"d", b"e"];

        for key in merged_keys {
            assert_eq!(
                (Slice::from(key), Slice::from(key.repeat(100))),
                merger
                    .next()
                    .map(|result| result.map(|(entry, _)| (entry.key, entry.value)))
                    .unwrap()?,
            );
        }

        assert!(merger.next().is_none());
    }

    Ok(())
}
