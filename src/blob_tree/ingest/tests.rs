// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::fs::MemFs;
use crate::{AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter};
use std::sync::Arc;

/// Ingested values go into blob files written as a flush writes them: a zstd
/// file builds a dictionary of its own from its first values, as the tree's
/// options ask.
#[test]
#[cfg(zstd_any)]
fn bulk_ingest_builds_each_files_own_dictionary() -> crate::Result<()> {
    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;

    let tree = match Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs)
    .blob_compression(crate::CompressionType::Zstd(3))
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(1)
            .file_dictionary(1_024),
    ))
    .open()?
    {
        AnyTree::Blob(t) => t,
        AnyTree::Standard(_) => panic!("expected a blob tree"),
    };

    let mut ingestion = super::BlobIngestion::new(&tree)?;
    for x in 0..64u8 {
        let value: alloc::vec::Vec<u8> = (0..200_u8).map(|at| (at % 7) ^ x).collect();
        ingestion.write(u64::from(x).to_be_bytes().into(), value.into())?;
    }
    ingestion.finish()?;

    let version = tree.current_version();
    assert!(
        version.blob_files.len() > 0,
        "the fixture must separate values out of line",
    );
    for bf in version.blob_files.iter() {
        assert!(
            bf.has_own_dictionary(),
            "blob file {} holds no dictionary of its own",
            bf.id()
        );
    }
    Ok(())
}

/// Bulk ingestion publishes blob files through its own version edit, so it is a
/// second publication path with the same obligation as compaction's: every file
/// it makes reachable must carry the tree's deletion pause. An unbound file's
/// `Drop` can unlink it while a checkpoint is capturing, and a tight-space
/// prefix punch can zero bytes the checkpoint has already hard-linked.
#[test]
fn bulk_ingest_binds_every_blob_file_it_publishes() -> crate::Result<()> {
    let memfs = Arc::new(MemFs::new());
    let root = std::path::absolute("/db")?;

    let tree = match Config::new(
        &root,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(memfs)
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?
    {
        AnyTree::Blob(t) => t,
        AnyTree::Standard(_) => panic!("expected a blob tree"),
    };

    let mut ingestion = super::BlobIngestion::new(&tree)?;
    for x in 0..64u64 {
        ingestion.write(x.to_be_bytes().into(), alloc::vec![b'v'; 128].into())?;
    }
    ingestion.finish()?;

    let version = tree.current_version();
    let blob_files: Vec<_> = version.blob_files.iter().collect();
    assert!(
        !blob_files.is_empty(),
        "the fixture must separate values out of line",
    );
    for bf in blob_files {
        assert!(
            bf.deletion_pause_for_test().is_some(),
            "blob file {} was published unbound: its Drop would bypass the \
             deletion pause a checkpoint relies on",
            bf.id(),
        );
    }
    Ok(())
}
