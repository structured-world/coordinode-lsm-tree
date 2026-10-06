use lsm_tree::{AbstractTree, Config, SeqNo, SequenceNumberCounter, get_tmp_folder};

#[test]
fn ingestion_persisted_seqno() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let mut ingest = tree.ingestion()?;
    ingest.write("a", "a")?;
    ingest.finish()?;
    assert_eq!(Some(0), tree.get_highest_persisted_seqno());

    let mut ingest = tree.ingestion()?;
    ingest.write("b", "b")?;
    ingest.finish()?;
    assert_eq!(Some(1), tree.get_highest_persisted_seqno());

    Ok(())
}

/// Verify that get_highest_persisted_seqno reflects the global offset
/// after mixed insert + ingest, and that ingested data is visible.
#[test]
fn ingestion_seqno_after_regular_inserts() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    let tree = Config::new(&folder, seqno.clone(), visible_seqno.clone()).open()?;

    // Regular inserts advance the seqno counter
    let s0 = seqno.next();
    tree.insert("x", "x0", s0);
    visible_seqno.fetch_max(s0 + 1);

    let s1 = seqno.next();
    tree.insert("y", "y0", s1);
    visible_seqno.fetch_max(s1 + 1);

    tree.flush_active_memtable(0)?;
    assert_eq!(tree.get_highest_persisted_seqno(), Some(s1));

    // Capture counter before ingestion — ingestion allocates this
    // value as global_seqno via seqno.next()
    let ingest_global_seqno = seqno.get();

    // Bulk-ingest: items get local seqno 0 but the table carries
    // a global_seqno offset
    let mut ingestion = tree.ingestion()?;
    ingestion.write("a", "a0")?;
    ingestion.write("b", "b0")?;
    ingestion.finish()?;

    // effective = global_seqno + local_max (0)
    let expected_seqno = ingest_global_seqno;

    assert_eq!(
        tree.get_highest_persisted_seqno(),
        Some(expected_seqno),
        "ingested table must report effective seqno (global_seqno + local_seqno)"
    );

    // Verify data is visible
    assert!(tree.get("a", SeqNo::MAX)?.is_some());
    assert!(tree.get("b", SeqNo::MAX)?.is_some());

    Ok(())
}

/// An ingestion takes its table ids when it starts, while a write made during
/// it reaches a table only at the flush that runs before the ingestion is
/// installed, with a higher id. The ingestion is the newer of the two: with
/// both holding one key at one sequence number, a read returns the ingested
/// value, before and after a reopen.
#[test]
fn an_ingestion_stays_ahead_of_a_flush_it_installs_after() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();
    let open = || Config::new(&folder, seqno.clone(), visible_seqno.clone()).open();

    {
        let tree = open()?;
        // The sequence number the ingestion is installed at.
        let tie = seqno.get();

        let mut ingestion = tree.ingestion()?;
        ingestion.write("k", "ingested")?;
        // A write made while the ingestion runs, at the same sequence number.
        tree.insert("k", "flushed", tie);
        visible_seqno.fetch_max(tie + 1);
        ingestion.finish()?;

        assert_eq!(
            tree.get("k", SeqNo::MAX)?.as_deref(),
            Some(b"ingested".as_slice()),
            "the ingestion installed after the flush is the newer write"
        );
    }

    let tree = open()?;
    assert_eq!(
        tree.get("k", SeqNo::MAX)?.as_deref(),
        Some(b"ingested".as_slice()),
        "the order holds across a reopen"
    );
    Ok(())
}
