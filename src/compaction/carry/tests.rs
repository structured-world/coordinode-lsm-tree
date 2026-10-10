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

/// With the levels past level 0 shifted two places down, a source level is
/// judged by the policies of its canonical level, as the destination is: a
/// physical level whose canonical level cuts pages as the destination does
/// qualifies, and one whose physical index would wrongly match does not.
#[test]
fn levels_share_shape_judges_a_shifted_level_by_its_canonical_policy() {
    use super::levels_share_shape;
    use crate::config::BlockSizePolicy;

    // Pages of 4 KiB at canonical level 1, 16 KiB at canonical level 2.
    let config = crate::Config::new(
        "unused",
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .columnar_page_size_policy(BlockSizePolicy::new([
        4_096, 4_096, 16_384, 16_384, 4_096, 4_096, 4_096,
    ]));
    let (shift, dest_canonical) = (2, 2);

    // Physical level 4 is canonical level 2, the destination's.
    assert!(levels_share_shape(&config, 4, shift, dest_canonical));
    // Physical level 3 is canonical level 1, whose pages differ, though
    // physical index 3 names the destination's page size.
    assert!(!levels_share_shape(&config, 3, shift, dest_canonical));
    // Physical levels 1 and 2 sit above the shifted ones and hold no table.
    assert!(!levels_share_shape(&config, 1, shift, dest_canonical));
    assert!(!levels_share_shape(&config, 2, shift, dest_canonical));
    // Level 0 is canonical level 0.
    assert!(!levels_share_shape(&config, 0, shift, dest_canonical));
    assert!(levels_share_shape(&config, 0, shift, 0));
}
