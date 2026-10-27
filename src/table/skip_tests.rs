// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A table reader moving past a key's versions on its index: the front past
//! all of them, the back up to the newest one a snapshot sees.

use super::*;
use crate::fs::StdFs;
use test_log::test;

/// A table of `items`, a data block cut every `per_block` of them, given to
/// `f`.
fn with_table(
    items: &[InternalValue],
    per_block: usize,
    f: impl FnOnce(&Table) -> crate::Result<()>,
) -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("table");
    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?;
    for (idx, item) in items.iter().enumerate() {
        if idx > 0 && idx % per_block == 0 {
            writer.spill_block()?;
        }
        writer.write(item.clone())?;
    }
    let (_, checksum) = writer.finish()?.expect("the table holds items");
    let mut params = RecoverParams::new(
        file,
        checksum,
        0,
        Arc::new(StdFs),
        crate::comparator::default_comparator(),
        Arc::new(Cache::with_capacity_bytes(1_000_000)),
    );
    params.descriptor_table = Some(Arc::new(DescriptorTable::new(10)));
    f(&Table::recover(params)?)
}

fn iv(key: &str, seqno: SeqNo) -> InternalValue {
    InternalValue::from_components(key, b"v", seqno, crate::ValueType::Value)
}

/// `a`, forty versions of `b`, newest first, and `c`.
fn chain() -> Vec<InternalValue> {
    let mut items = vec![iv("a", 0)];
    items.extend((1..=40).rev().map(|seqno| iv("b", seqno)));
    items.push(iv("c", 0));
    items
}

/// An item shown as its key and seqno.
fn shown(item: Option<crate::Result<InternalValue>>) -> Option<String> {
    item.map(|item| {
        let item = item.expect("the read succeeds");
        format!(
            "{}{}",
            String::from_utf8_lossy(&item.key.user_key),
            item.key.seqno
        )
    })
}

/// The front passes the blocks holding only the key's versions and goes on
/// at the next key.
#[test]
fn table_front_jump_lands_on_the_next_key() -> crate::Result<()> {
    with_table(&chain(), 3, |table| {
        let mut iter = table.range_iter(..);
        assert_eq!(shown(iter.next()).as_deref(), Some("a0"));
        assert_eq!(shown(iter.next()).as_deref(), Some("b40"));
        assert!(iter.skip_past(b"b")?);
        assert_eq!(shown(iter.next()).as_deref(), Some("c0"));
        assert_eq!(shown(iter.next()), None);
        Ok(())
    })
}

/// Once the back read the blocks the front would open next, the front passes
/// the key in the block the back stands in, and the two ends meet.
#[test]
fn table_front_jump_goes_on_in_the_block_the_back_read() -> crate::Result<()> {
    let items = [iv("a", 0), iv("b", 3), iv("b", 2), iv("b", 1), iv("c", 0)];
    with_table(&items, 2, |table| {
        let mut iter = table.range_iter(..);
        assert_eq!(shown(iter.next_back()).as_deref(), Some("c0"));
        assert_eq!(shown(iter.next_back()).as_deref(), Some("b1"));
        assert_eq!(shown(iter.next()).as_deref(), Some("a0"));
        assert!(!iter.skip_past(b"b")?, "nothing is left past b");
        assert_eq!(shown(iter.next()), None);
        assert_eq!(shown(iter.next_back()), None);
        Ok(())
    })
}

/// The back, having walked a key's oldest versions, moves up to the newest
/// one the snapshot sees, passing the blocks of older ones on the index.
#[test]
fn table_back_jump_lands_on_the_newest_visible_version() -> crate::Result<()> {
    with_table(&chain(), 3, |table| {
        for (snapshot, newest) in [(SeqNo::MAX, "b40"), (21, "b20")] {
            let mut iter = table.range_iter(..);
            assert_eq!(shown(iter.next_back()).as_deref(), Some("c0"));
            assert_eq!(shown(iter.next_back()).as_deref(), Some("b1"));
            assert!(iter.skip_back_to_newest(b"b", snapshot)?);
            assert_eq!(shown(iter.next_back()).as_deref(), Some(newest));
        }
        Ok(())
    })
}

/// A back with nothing of the key ahead of it moves nothing: the block it
/// looked at is held, and either end reads it next.
#[test]
fn table_back_jump_short_of_the_key_holds_the_block_it_read() -> crate::Result<()> {
    with_table(&chain(), 3, |table| {
        let expected: Vec<String> = table
            .range_iter(..)
            .rev()
            .map(|item| shown(Some(item)).expect("an item"))
            .collect();

        let mut iter = table.range_iter(..);
        assert!(!iter.skip_back_to_newest(b"z", SeqNo::MAX)?);
        let got: Vec<String> = iter
            .rev()
            .map(|item| shown(Some(item)).expect("an item"))
            .collect();
        assert_eq!(got, expected, "from the back");

        let mut iter = table.range_iter(..);
        assert!(!iter.skip_back_to_newest(b"z", SeqNo::MAX)?);
        let mut forward: Vec<String> = iter
            .map(|item| shown(Some(item)).expect("an item"))
            .collect();
        forward.reverse();
        assert_eq!(forward, expected, "from the front");
        Ok(())
    })
}

/// A range past every key leaves the reader nothing to move.
#[test]
fn table_jumps_in_an_empty_range_move_nothing() -> crate::Result<()> {
    with_table(&chain(), 3, |table| {
        let past = (
            core::ops::Bound::Included(crate::UserKey::from("x")),
            core::ops::Bound::<crate::UserKey>::Unbounded,
        );
        let mut iter = table.range_iter(past.clone());
        assert!(!iter.skip_past(b"x")?);
        let mut iter = table.range_iter(past);
        assert!(!iter.skip_back_to_newest(b"x", SeqNo::MAX)?);
        assert_eq!(shown(iter.next_back()), None);
        Ok(())
    })
}
