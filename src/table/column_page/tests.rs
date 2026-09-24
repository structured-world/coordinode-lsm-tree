// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The directory decides where every page of a row group is, so the cases
//! that matter are the ones where a wrong answer is still a plausible one: a
//! payload that decodes into a different set of pages than it was written
//! from, and a payload whose entries describe a placement no writer produces.

use super::{PageDirectory, PageEntry, PageId, PageStamp, VERSION};

fn id(column_id: u16, part: u8) -> PageId {
    PageId { column_id, part }
}

fn entry(offset: u32, length: u32, column_id: u16, part: u8, row_page: u16) -> PageEntry {
    PageEntry {
        offset,
        length,
        id: id(column_id, part),
        row_page,
    }
}

const ROWS: u32 = 512;

/// A tag wide enough that a field read at the wrong width or byte order
/// would not round-trip by accident.
const TAG: u64 = 0x0102_0304_0506_0708;

/// Two row pages of 200 and 312 rows, and three column parts with a page for
/// each: the grid a writer produces.
fn directory() -> PageDirectory {
    PageDirectory::new(
        ROWS,
        TAG,
        vec![200, 312],
        vec![
            entry(0, 64, 0, 0, 0),
            entry(64, 80, 0, 0, 1),
            entry(144, 2_048, 3, 0, 0),
            entry(2_192, 2_048, 3, 0, 1),
            entry(4_240, 32, 3, 1, 0),
            entry(4_272, 40, 3, 1, 1),
        ],
    )
    .expect("ascending, non-overlapping, a complete grid")
}

/// Byte offset of the first entry's `flags` field: the header, the two row
/// pages, then the entry's offset, length, column id and part.
const FIRST_FLAGS_AT: usize = (1 + 2 + 4 + 8 + 2) + 2 * 4 + (4 + 4 + 2 + 1);

#[test]
fn a_directory_round_trips_through_its_wire_form() {
    let original = directory();
    let mut bytes = Vec::new();
    original.encode_into(&mut bytes);

    let decoded = PageDirectory::decode(&bytes).expect("decode");
    assert_eq!(decoded, original, "the wire form must preserve every page");
    assert_eq!(
        decoded.row_count(),
        ROWS,
        "the row count must survive, since a reader needs it before any page",
    );
    assert_eq!(
        decoded.group_tag(),
        TAG,
        "the tag must survive, since every page's stamp is checked against it",
    );
    assert_eq!(decoded.row_pages(), [200, 312]);
    assert_eq!(
        (decoded.row_page_start(0), decoded.row_page_start(1)),
        (Some(0), Some(200)),
        "a row page starts where the ones before it end",
    );
    assert_eq!(decoded.row_page_rows(1), Some(312));
    assert_eq!(
        decoded.row_page_start(2),
        None,
        "there is no third row page"
    );
}

#[test]
fn a_page_stamp_round_trips_and_names_its_directory_entry() {
    // The stamp is what a reader compares a page against, so it has to come
    // back exactly as written and has to be the one the directory implies for
    // that entry: the group's tag, the entry's column part and its row page.
    let directory = directory();
    let entry = directory.entries()[5];
    let stamp = directory.stamp_for(&entry);
    assert_eq!(
        stamp,
        PageStamp {
            group_tag: TAG,
            id: id(3, 1),
            row_page: 1,
        },
    );

    let mut bytes = Vec::new();
    stamp.encode_into(&mut bytes);
    assert_eq!(bytes.len(), PageStamp::LEN);
    let array: [u8; PageStamp::LEN] = bytes.try_into().expect("stamp length");
    assert_eq!(PageStamp::decode(array), stamp);
}

#[test]
fn two_row_pages_of_one_column_part_carry_different_stamps() {
    // Two row pages of the same column part hold different rows. Their stamps
    // are what refuses one in the other's place, so they must differ.
    let directory = directory();
    let first = directory.stamp_for(&directory.entries()[2]);
    let second = directory.stamp_for(&directory.entries()[3]);
    assert_eq!(first.id, second.id, "the same column part");
    assert_ne!(
        first, second,
        "row pages must be told apart by their stamps"
    );
}

#[test]
fn an_unknown_version_is_refused_rather_than_parsed() {
    // The directory is what makes every page addressable, so a version this
    // build does not know is not a local error to skip past: parsing it under
    // the current layout would resolve every page to the wrong bytes.
    let mut bytes = Vec::new();
    directory().encode_into(&mut bytes);
    bytes[0] = VERSION.wrapping_add(1);

    let err = PageDirectory::decode(&bytes).expect_err("an unknown version must be refused");
    assert!(
        format!("{err:?}").contains("version"),
        "the error must name the version, got {err:?}",
    );
}

#[test]
fn overlapping_pages_are_refused_at_construction() {
    // A directory that maps one byte into two pages has no correct reading,
    // so it is rejected where it is still fixable rather than at read time.
    let err = PageDirectory::new(
        ROWS,
        TAG,
        vec![ROWS],
        vec![entry(0, 200, 0, 0, 0), entry(128, 64, 1, 0, 0)],
    )
    .expect_err("overlapping pages must be refused");
    assert!(
        format!("{err:?}").contains("overlapping"),
        "the error must name the overlap, got {err:?}",
    );
}

#[test]
fn descending_pages_are_refused_at_construction() {
    let err = PageDirectory::new(
        ROWS,
        TAG,
        vec![ROWS],
        vec![entry(4_096, 64, 1, 0, 0), entry(0, 128, 0, 0, 0)],
    )
    .expect_err("descending pages must be refused");
    assert!(
        format!("{err:?}").contains("ascending"),
        "the error must name the ordering, got {err:?}",
    );
}

#[test]
fn two_pages_claiming_one_column_part_and_row_page_are_refused() {
    // Byte ranges that do not overlap can still be ambiguous: two pages that
    // both claim column 3 part 0 of row page 0 leave a reader no way to tell
    // which one holds those rows. Whichever it picked, the other page is
    // silently unreachable, and a lookup that returns the first match would
    // hide it.
    let err = PageDirectory::new(
        ROWS,
        TAG,
        vec![ROWS],
        vec![entry(0, 64, 3, 0, 0), entry(64, 64, 3, 0, 0)],
    )
    .expect_err("a duplicated column part must be refused");
    assert!(
        format!("{err:?}").contains("same column part"),
        "the error must name the duplicated part, got {err:?}",
    );
}

#[test]
fn a_column_part_missing_a_row_page_is_refused() {
    // A row page is assembled from one page of each column part. A part with
    // no page for one row page would leave those rows without that column,
    // and a reader could only guess, so the grid must be complete.
    let err = PageDirectory::new(
        ROWS,
        TAG,
        vec![200, 312],
        vec![
            entry(0, 64, 0, 0, 0),
            entry(64, 64, 0, 0, 1),
            entry(128, 64, 3, 0, 0),
        ],
    )
    .expect_err("an incomplete grid must be refused");
    assert!(
        format!("{err:?}").contains("missing a row page"),
        "the error must name the missing row page, got {err:?}",
    );
}

#[test]
fn a_column_part_missing_a_middle_row_page_is_refused() {
    // Same failure with the gap inside the part's run rather than at its end,
    // which a check that only counted pages per part would not see.
    let err = PageDirectory::new(
        300,
        TAG,
        vec![100, 100, 100],
        vec![entry(0, 64, 0, 0, 0), entry(64, 64, 0, 0, 2)],
    )
    .expect_err("a gap in a part's row pages must be refused");
    assert!(
        format!("{err:?}").contains("missing a row page"),
        "the error must name the missing row page, got {err:?}",
    );
}

#[test]
fn row_pages_that_do_not_sum_to_the_group_are_refused() {
    let err = PageDirectory::new(ROWS, TAG, vec![200, 200], vec![])
        .expect_err("row pages short of the group must be refused");
    assert!(
        format!("{err:?}").contains("sum"),
        "the error must name the sum, got {err:?}",
    );
}

#[test]
fn an_empty_row_page_is_refused() {
    let err = PageDirectory::new(ROWS, TAG, vec![ROWS, 0], vec![])
        .expect_err("an empty row page must be refused");
    assert!(
        format!("{err:?}").contains("empty row page"),
        "the error must name the empty row page, got {err:?}",
    );
}

#[test]
fn a_page_naming_a_row_page_that_does_not_exist_is_refused() {
    let err = PageDirectory::new(ROWS, TAG, vec![ROWS], vec![entry(0, 64, 0, 0, 1)])
        .expect_err("a page past the last row page must be refused");
    assert!(
        format!("{err:?}").contains("does not exist"),
        "the error must name the missing row page, got {err:?}",
    );
}

#[test]
fn a_page_extent_that_overflows_is_refused() {
    let err = PageDirectory::new(
        ROWS,
        TAG,
        vec![ROWS],
        vec![entry(u32::MAX - 8, 16, 0, 0, 0)],
    )
    .expect_err("an extent past u32 must be refused");
    assert!(
        format!("{err:?}").contains("overflow"),
        "the error must name the overflow, got {err:?}",
    );
}

#[test]
fn more_pages_than_the_count_field_holds_are_refused_at_construction() {
    // The wire count is a u16. A directory with more pages than that cannot be
    // written faithfully: encoding would have to either truncate the list,
    // leaving pages unaddressable, or write a count that disagrees with the
    // entries that follow. Both are silent, so the only honest place to stop
    // it is construction.
    let too_many: Vec<PageEntry> = (0..=u32::from(u16::MAX))
        .map(|i| {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "spreading ids over column and part keeps them distinct"
            )]
            let (column_id, part) = ((i >> 8) as u16, i as u8);
            entry(i * 4, 4, column_id, part, 0)
        })
        .collect();
    let err = PageDirectory::new(ROWS, TAG, vec![ROWS], too_many)
        .expect_err("a page count past u16 must be refused");
    assert!(
        format!("{err:?}").contains("page count"),
        "the error must name the page count, got {err:?}",
    );
}

#[test]
fn a_directory_of_the_largest_declarable_size_decodes_in_bounded_time() {
    // The directory is read from disk, so its page count is whatever the
    // bytes say, up to u16::MAX. Validation has to stay near-linear in that
    // count: a check that is quadratic in it turns one corrupt block into
    // billions of comparisons on the read path, which is a denial of service
    // by a damaged file rather than a slow corner case.
    let entries: Vec<PageEntry> = (0..u32::from(u16::MAX))
        .map(|i| {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "spreading ids over column and part keeps them distinct"
            )]
            let (column_id, part) = ((i >> 8) as u16, i as u8);
            entry(i * 40, 40, column_id, part, 0)
        })
        .collect();
    let mut bytes = Vec::new();
    PageDirectory::new(ROWS, TAG, vec![ROWS], entries.clone())
        .expect("distinct and ascending")
        .encode_into(&mut bytes);

    let started = std::time::Instant::now();
    let decoded = PageDirectory::decode(&bytes).expect("a valid maximal directory decodes");
    let elapsed = started.elapsed();

    assert_eq!(decoded.entries().len(), entries.len());
    // Linear work here is well under a millisecond; quadratic is seconds.
    // The bound sits orders of magnitude between the two, so it does not
    // flake on a slow runner and still fails a quadratic check outright.
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "decoding a maximal directory took {elapsed:?}, which is not linear",
    );
}

#[test]
fn trailing_bytes_are_refused_rather_than_ignored() {
    // A payload longer than it declares is either a writer this build does
    // not understand or a corruption. Ignoring the tail would carry either
    // one silently into every page lookup the directory then serves.
    let mut bytes = Vec::new();
    directory().encode_into(&mut bytes);
    bytes.push(0);

    let err = PageDirectory::decode(&bytes).expect_err("trailing bytes must be refused");
    assert!(
        format!("{err:?}").contains("trailing"),
        "the error must name the trailing bytes, got {err:?}",
    );
}

#[test]
fn a_truncated_payload_is_refused() {
    let mut bytes = Vec::new();
    directory().encode_into(&mut bytes);
    bytes.truncate(bytes.len() - 1);

    PageDirectory::decode(&bytes).expect_err("a truncated payload must be refused");
}

#[test]
fn a_set_reserved_flag_is_refused() {
    // The flag field is the format's room to grow. A reader that ignored an
    // unknown bit would read a page whose meaning has changed as if it had
    // not, which is the failure the bit exists to prevent.
    let mut bytes = Vec::new();
    directory().encode_into(&mut bytes);
    bytes[FIRST_FLAGS_AT] = 1;

    let err = PageDirectory::decode(&bytes).expect_err("a reserved flag must be refused");
    assert!(
        format!("{err:?}").contains("reserved"),
        "the error must name the reserved flag, got {err:?}",
    );
}

#[test]
fn a_contiguous_layout_places_each_page_where_the_previous_one_ends() {
    // The writer's layout, and the one the reader's extent check enforces: the
    // pages follow the directory back to back. Offsets are a running sum of
    // lengths, and the pages' total is where the group ends.
    let directory = PageDirectory::contiguous(
        ROWS,
        TAG,
        vec![ROWS],
        [(id(0, 0), 0, 100), (id(1, 0), 0, 40), (id(2, 0), 0, 7)],
    )
    .expect("three pages");
    let offsets: Vec<u32> = directory.entries().iter().map(|e| e.offset).collect();
    assert_eq!(
        offsets,
        [0, 100, 140],
        "each page starts where the last one ended"
    );
    assert_eq!(
        directory.pages_len(),
        147,
        "the pages' total is their summed length"
    );

    assert_eq!(
        PageDirectory::contiguous(0, TAG, vec![], [])
            .expect("no pages")
            .pages_len(),
        0,
        "a directory with no pages spans nothing",
    );
}

#[test]
fn a_contiguous_layout_that_overflows_is_refused() {
    let err = PageDirectory::contiguous(
        ROWS,
        TAG,
        vec![ROWS],
        [(id(0, 0), 0, u32::MAX), (id(1, 0), 0, 1)],
    )
    .expect_err("a group length past u32 must be refused");
    assert!(
        format!("{err:?}").contains("overflow"),
        "the error must name the overflow, got {err:?}",
    );
}
