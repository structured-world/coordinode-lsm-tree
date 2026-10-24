// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The scoped change scan against tables whose range tombstones reach past
//! their key range.

use crate::{
    AbstractTree, AnyTree, Config, InternalValue, ScanSinceEvent, SequenceNumberCounter, ValueType,
    range_tombstone::RangeTombstone,
};

/// A table's tombstones can reach past its key range: a compaction cuts them
/// at the next output's first key without widening the range up to it, and
/// the writer widens only tables without rows. A scope inside that stretch,
/// which no table's key range reaches, must still get the deletion.
#[test]
fn a_scoped_scan_gets_a_tombstone_past_its_tables_key_range() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let AnyTree::Standard(tree) = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?
    else {
        panic!("a standard tree");
    };

    let id = tree.get_next_table_id();
    let path = tree
        .config
        .path
        .join(crate::file::TABLES_FOLDER)
        .join(id.to_string());
    let mut writer = crate::table::Writer::new(path.clone(), id, 0, tree.config.fs.clone())?;
    writer.write_range_tombstone(RangeTombstone::new("b".into(), "q".into(), 5));
    writer.write(InternalValue::from_components(
        "b",
        "v",
        4,
        ValueType::Value,
    ))?;
    let (_, checksum) = writer.finish()?.expect("the table is not empty");
    let table = crate::Table::recover(crate::table::RecoverParams::new(
        path,
        checksum,
        id,
        tree.config.fs.clone(),
        tree.config.comparator.clone(),
        tree.config.cache.clone(),
    ))?;
    assert_eq!(
        table.metadata.key_range.max().as_ref(),
        b"b",
        "premise: the tombstone reaches past the key range"
    );
    tree.register_tables(&[table], None, None, &[], 0, false)?;

    let got: Vec<ScanSinceEvent> = tree
        .scan_since_seqno_in_range(0, b"n".as_slice()..=b"o".as_slice())?
        .collect();
    assert!(
        got.iter()
            .any(|e| matches!(e, ScanSinceEvent::RangeTombstone { seqno: 5, .. })),
        "the deletion over the scope must be delivered: {got:?}",
    );
    Ok(())
}
