// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A write that took its seqno and is not published yet stays invisible to a
//! snapshot at the visible seqno, whatever the tree installs meanwhile.

use lsm_tree::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter};

fn open(
    folder: &tempfile::TempDir,
    seqno: &SequenceNumberCounter,
    visible: &SequenceNumberCounter,
) -> lsm_tree::Result<AnyTree> {
    Config::new(folder, seqno.clone(), visible.clone()).open()
}

/// A batch writes its keys one at a time under one seqno and publishes it
/// after the last. A flush or a compaction that installs between two of its
/// keys must not publish the first without the second.
#[test]
fn an_install_does_not_publish_half_of_a_batch() -> lsm_tree::Result<()> {
    for install in ["flush", "compaction", "collecting compaction"] {
        let folder = tempfile::tempdir()?;
        let seqno = SequenceNumberCounter::default();
        let visible = SequenceNumberCounter::default();
        let tree = open(&folder, &seqno, &visible)?;

        let first = seqno.next();
        tree.insert("earlier", "x", first);
        visible.fetch_max(first + 1);
        if install != "flush" {
            tree.flush_active_memtable(0)?;
        }

        let batch = seqno.next();
        tree.insert("a", "1", batch);
        match install {
            "flush" => {
                tree.flush_active_memtable(0)?;
            }
            "compaction" => {
                tree.major_compact(u64::MAX, 0)?;
            }
            _ => {
                // A watermark at the visible seqno collects only what no
                // snapshot from there on reads.
                tree.major_compact(u64::MAX, visible.get())?;
            }
        }
        let snapshot = visible.get();
        assert!(
            tree.get("a", snapshot)?.is_none(),
            "a {install} published the first key of a batch still being written",
        );
        assert!(tree.get("earlier", snapshot)?.is_some(), "{install}");

        tree.insert("b", "1", batch);
        visible.fetch_max(batch + 1);
        let snapshot = visible.get();
        assert!(tree.get("a", snapshot)?.is_some(), "{install}");
        assert!(tree.get("b", snapshot)?.is_some(), "{install}");
    }
    Ok(())
}

/// Two trees share the counter pair, as the keyspaces of one database do. A
/// batch applied to the first and not yet to the second stays invisible in the
/// first while the second flushes.
#[test]
fn an_install_in_one_tree_does_not_publish_a_batch_in_another() -> lsm_tree::Result<()> {
    let seqno = SequenceNumberCounter::default();
    let visible = SequenceNumberCounter::default();
    let (records_at, index_at) = (tempfile::tempdir()?, tempfile::tempdir()?);
    let records = open(&records_at, &seqno, &visible)?;
    let index = open(&index_at, &seqno, &visible)?;

    let earlier = seqno.next();
    index.insert("earlier", "x", earlier);
    visible.fetch_max(earlier + 1);

    let batch = seqno.next();
    records.insert("record", "1", batch);
    // The other tree installs before the batch reaches it.
    index.flush_active_memtable(0)?;
    let snapshot = visible.get();
    assert!(
        records.get("record", snapshot)?.is_none(),
        "the other tree's flush published half of a batch",
    );

    index.insert("entry", "1", batch);
    visible.fetch_max(batch + 1);
    let snapshot = visible.get();
    assert!(records.get("record", snapshot)?.is_some());
    assert!(index.get("entry", snapshot)?.is_some());
    Ok(())
}

/// With nothing in flight, a read at the visible seqno returns after a flush
/// and a compaction what it returned before them, and the visible seqno is
/// where the writer left it.
#[test]
fn an_install_leaves_the_visible_seqno_where_the_writer_put_it() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let visible = SequenceNumberCounter::default();
    let tree = open(&folder, &seqno, &visible)?;

    for key in ["a", "b"] {
        let write = seqno.next();
        tree.insert(key, "1", write);
        visible.fetch_max(write + 1);
    }
    let published = visible.get();
    tree.flush_active_memtable(0)?;
    assert_eq!(visible.get(), published, "a flush moved the visible seqno");
    tree.major_compact(u64::MAX, published)?;
    assert_eq!(
        visible.get(),
        published,
        "a collection below the visible seqno moved it"
    );
    for key in ["a", "b"] {
        assert!(tree.get(key, published)?.is_some(), "{key}");
    }
    Ok(())
}

/// An install that raises the retention floor to the visible seqno moves the
/// visible seqno past it, so a read at the visible seqno is served right after
/// it: a clear, a replacing ingestion, and a collection under a watermark
/// above the visible seqno.
#[test]
fn an_install_that_raises_the_floor_is_readable_at_the_visible_seqno() -> lsm_tree::Result<()> {
    for install in ["clear", "replace", "collect everything"] {
        let folder = tempfile::tempdir()?;
        let seqno = SequenceNumberCounter::default();
        let visible = SequenceNumberCounter::default();
        let tree = open(&folder, &seqno, &visible)?;

        for value in ["1", "2"] {
            let write = seqno.next();
            tree.insert("a", value, write);
            visible.fetch_max(write + 1);
        }
        tree.flush_active_memtable(0)?;
        match install {
            "clear" => tree.clear()?,
            "replace" => {
                let mut ingestion = tree.replacing_ingestion()?;
                ingestion.write("a", "3")?;
                ingestion.finish()?;
            }
            _ => {
                tree.major_compact(u64::MAX, SeqNo::MAX)?;
            }
        }
        let expected = match install {
            "clear" => None,
            "replace" => Some(b"3".as_slice()),
            _ => Some(b"2".as_slice()),
        };
        assert_eq!(
            tree.get("a", visible.get())?.as_deref(),
            expected,
            "{install}"
        );
    }
    Ok(())
}
