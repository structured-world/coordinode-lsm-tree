// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::align_row_pages;
use crate::{InternalValue, ValueType};

fn row(key: u32, seqno: u64, value: &str) -> InternalValue {
    InternalValue::from_components(
        format!("k{key:04}").into_bytes(),
        value.as_bytes(),
        seqno,
        ValueType::Value,
    )
}

/// Three row pages of four rows each, keys 0..12.
fn source() -> (Vec<InternalValue>, Vec<u32>) {
    ((0..12).map(|k| row(k, 1, "v")).collect(), vec![4, 4, 4])
}

/// A changed row in the middle row page leaves the other two copied, and
/// the middle one takes the rows the merge emitted for it.
#[test]
fn align_one_changed_row_copies_the_other_row_pages() {
    let (source, pages) = source();
    let mut emitted = source.clone();
    emitted[5] = row(5, 1, "changed");
    assert_eq!(
        align_row_pages(&source, &pages, &emitted),
        Some((vec![4, 4, 4], vec![true, false, true]))
    );
}

/// A row the merge added within the first row page's keys lands in it, the
/// row pages after it keep their ordinals and are copied.
#[test]
fn align_inserted_row_grows_its_row_page_only() {
    let (source, pages) = source();
    let mut emitted = source.clone();
    // A newer version of key 1, ahead of the one read.
    emitted.insert(1, row(1, 9, "newer"));
    assert_eq!(
        align_row_pages(&source, &pages, &emitted),
        Some((vec![5, 4, 4], vec![false, true, true]))
    );
}

/// Every row of a row page dropped: the row pages after it could only keep
/// their ordinals with an empty one, which a group cannot hold.
#[test]
fn align_emptied_row_page_refuses() {
    let (source, pages) = source();
    let emitted: Vec<_> = source
        .iter()
        .enumerate()
        .filter(|(i, _)| !(4..8).contains(i))
        .map(|(_, r)| r.clone())
        .collect();
    assert_eq!(align_row_pages(&source, &pages, &emitted), None);
}

/// Rows past the last row page, an older version of its last key from
/// another input, join it, so it is no longer the source's own.
#[test]
fn align_rows_past_the_last_row_page_join_it() {
    let (source, pages) = source();
    let mut emitted = source.clone();
    emitted.push(row(11, 0, "older"));
    assert_eq!(
        align_row_pages(&source, &pages, &emitted),
        Some((vec![4, 4, 5], vec![true, true, false]))
    );
}

/// No row page comes out as it was read: there is nothing to copy.
#[test]
fn align_every_row_page_changed_refuses() {
    let (source, pages) = source();
    let emitted: Vec<_> = (0..12).map(|k| row(k, 1, "changed")).collect();
    assert_eq!(align_row_pages(&source, &pages, &emitted), None);
}

/// A run of two changed row pages between copied ones is spread over both,
/// each keeping at least one row.
#[test]
fn align_changed_run_spreads_over_its_row_pages() {
    let source: Vec<_> = (0..16).map(|k| row(k, 1, "v")).collect();
    let pages = vec![4, 4, 4, 4];
    let mut emitted = source.clone();
    // Keys 5 and 9 rewritten, key 6 dropped: rows 4..12 come out as seven.
    emitted[5] = row(5, 1, "changed");
    emitted[9] = row(9, 1, "changed");
    emitted.remove(6);
    assert_eq!(
        align_row_pages(&source, &pages, &emitted),
        Some((vec![4, 4, 3, 4], vec![true, false, false, true]))
    );
}

/// A changed row page fits while its rows stay within the limit, or when it
/// holds one row however large, which the writer cannot split either; a
/// copied page keeps its own bytes and is not weighed, and a cut that does
/// not match the rows does not fit.
#[test]
fn changed_pages_fit_weighs_only_the_pages_written_anew() {
    use super::changed_pages_fit;

    let big = "x".repeat(100);
    let rows: Vec<_> = (0..4).map(|k| row(k, 1, &big)).collect();
    // Two pages of two rows each, about 210 bytes a page.
    assert!(changed_pages_fit(&rows, (&[2, 2], &[false, false]), 300));
    assert!(!changed_pages_fit(&rows, (&[2, 2], &[false, true]), 150));
    assert!(changed_pages_fit(&rows, (&[2, 2], &[true, true]), 150));
    // One row a page passes whatever its size.
    assert!(changed_pages_fit(&rows, (&[1, 1, 1, 1], &[false; 4]), 10));
    // A cut past the rows.
    assert!(!changed_pages_fit(&rows, (&[3, 3], &[false, false]), 1_000));
}

std::thread_local! {
    /// The most bytes of rows a matcher held at once on this thread.
    static PEAK_HELD: core::cell::Cell<u64> = const { core::cell::Cell::new(0) };
}

pub(super) fn note_held(rows: &[InternalValue]) {
    let bytes = rows
        .iter()
        .map(|row| (row.key.user_key.len() + row.value.len()) as u64)
        .sum::<u64>();
    PEAK_HELD.with(|peak| peak.set(peak.get().max(bytes)));
}

/// A columnar tree whose tables are compressed alike at every level, so its
/// groups can be copied.
fn columnar_tree(folder: &std::path::Path) -> crate::Result<crate::Tree> {
    let crate::AnyTree::Standard(tree) = crate::Config::new(
        folder,
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .open()?
    else {
        panic!("expected standard tree");
    };
    tree.update_runtime_config(|cfg| {
        cfg.columnar = true;
        cfg.data_block_compression_policy =
            crate::config::CompressionPolicy::all(crate::CompressionType::None);
    })?;
    Ok(tree)
}

fn key(i: u32) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

/// A group of small rows the merge fills with much larger newer versions is
/// held only while what it holds stays near the group's own size: past it,
/// the rows go out as written, however few of them there are.
#[test]
fn held_rows_are_bounded_by_their_bytes() -> crate::Result<()> {
    use crate::AbstractTree;

    let folder = crate::get_tmp_folder();
    let tree = columnar_tree(folder.path())?;
    for i in 0..1_000 {
        tree.insert(key(i), vec![b's'; 16], 1);
    }
    tree.flush_active_memtable(0)?;
    // Every third key of the first groups' range, at 64 KiB.
    for i in (0..300).step_by(3) {
        tree.insert(key(i), vec![b'L'; 64 * 1024], 2);
    }
    tree.flush_active_memtable(0)?;

    PEAK_HELD.with(|peak| peak.set(0));
    tree.major_compact(64 * 1024 * 1024, 0)?;
    let peak = PEAK_HELD.with(core::cell::Cell::get);

    assert!(peak < 256 * 1024, "{peak} bytes held for one group");
    assert_eq!(tree.iter(crate::SeqNo::MAX, None).count(), 1_000);
    assert_eq!(
        tree.get(key(3), crate::SeqNo::MAX)?.map(|v| v.len()),
        Some(64 * 1024)
    );
    Ok(())
}

/// A group rebuilt around copied pages keeps the source's row pages, so the
/// rows of a changed page all land in one page: when newer versions make
/// that page larger than a row group, the group is written as rows instead,
/// as the writer would cut them. Smaller changes are still rebuilt.
#[cfg(feature = "metrics")]
#[test]
fn a_changed_page_past_a_group_is_written_not_rebuilt() -> crate::Result<()> {
    use crate::AbstractTree;

    for (len, rebuilt) in [(40, true), (8 * 1024, false)] {
        let folder = crate::get_tmp_folder();
        let tree = columnar_tree(folder.path())?;
        // Three neighbouring keys of one row page.
        for i in 0..1_000 {
            tree.insert(key(i), vec![b's'; 16], 1);
        }
        tree.flush_active_memtable(0)?;
        for i in 1..4 {
            tree.insert(key(i), vec![b'L'; len], 2);
        }
        tree.flush_active_memtable(0)?;

        // Every version kept, so the rows the merge leaves alone stay as
        // they were read.
        tree.major_compact(64 * 1024 * 1024, 0)?;

        assert_eq!(
            tree.metrics().compaction_groups_partly_carried() > 0,
            rebuilt,
            "values of {len} bytes"
        );
        assert_eq!(tree.iter(crate::SeqNo::MAX, None).count(), 1_000);
        assert_eq!(
            tree.get(key(2), crate::SeqNo::MAX)?.map(|v| v.len()),
            Some(len)
        );
    }
    Ok(())
}
