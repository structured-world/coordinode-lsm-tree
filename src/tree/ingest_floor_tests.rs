// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The L0 recency floor an ingestion holds while it runs.

use super::ingest::Ingestion;
use crate::{Config, SequenceNumberCounter, Tree};
use test_log::test;

fn open(dir: &std::path::Path) -> crate::Result<Tree> {
    let any = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    let crate::AnyTree::Standard(tree) = any else {
        panic!("expected Standard tree");
    };
    Ok(tree)
}

/// A finished ingestion gives its floor back while it still holds the flush
/// lock: a flush waiting on the lock must not be stamped with the floor of an
/// ingestion that is already installed, or it would lay out behind it though
/// it installs after it.
#[test]
fn ingestion_finish_releases_floor_under_flush_lock() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = open(dir.path())?;

    let mut ingestion = Ingestion::new(&tree)?;
    ingestion.write("k".into(), "v".into())?;
    ingestion.finish()?;

    assert_eq!(None, tree.lowest_ingest_floor());
    assert_eq!(
        vec![true],
        *tree.floor_releases_under_flush_lock.lock(),
        "the floor is released before the flush lock"
    );
    Ok(())
}

/// The same holds for a blob tree's ingestion, whose `finish` takes the index
/// tree's flush lock itself.
#[test]
fn blob_ingestion_finish_releases_floor_under_flush_lock() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let any = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::config::KvSeparationOptions::default()))
    .open()?;
    let crate::AnyTree::Blob(tree) = any else {
        panic!("expected Blob tree");
    };

    let mut ingestion = crate::blob_tree::ingest::BlobIngestion::new(&tree)?;
    ingestion.write("k".into(), "v".into())?;
    ingestion.finish()?;

    assert_eq!(None, tree.index.lowest_ingest_floor());
    assert_eq!(
        vec![true],
        *tree.index.floor_releases_under_flush_lock.lock(),
        "the floor is released before the flush lock"
    );
    Ok(())
}

/// An ingestion given up without `finish` releases its floor too.
#[test]
fn ingestion_dropped_releases_floor() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = open(dir.path())?;

    let mut ingestion = Ingestion::new(&tree)?;
    ingestion.write("k".into(), "v".into())?;
    assert!(tree.lowest_ingest_floor().is_some());
    drop(ingestion);

    assert_eq!(None, tree.lowest_ingest_floor());
    Ok(())
}
