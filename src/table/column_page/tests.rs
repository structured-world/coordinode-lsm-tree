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

fn entry(offset: u32, length: u32, column_id: u16, part: u8) -> PageEntry {
    PageEntry {
        offset,
        length,
        id: id(column_id, part),
    }
}

const ROWS: u32 = 512;

/// A tag wide enough that a field read at the wrong width or byte order
/// would not round-trip by accident.
const TAG: u64 = 0x0102_0304_0506_0708;

fn directory() -> PageDirectory {
    PageDirectory::new(
        ROWS,
        TAG,
        vec![
            entry(0, 128, 0, 0),
            entry(128, 4_096, 3, 0),
            entry(4_224, 64, 3, 1),
        ],
    )
    .expect("ascending, non-overlapping, distinct")
}

/// Byte offset of the first entry's `flags` field: the header, then the
/// entry's offset, length, column id and part.
const FIRST_FLAGS_AT: usize = (1 + 2 + 4 + 8) + (4 + 4 + 2 + 1);

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
}

#[test]
fn a_page_stamp_round_trips_and_names_its_directory_entry() {
    // The stamp is what a reader compares a page against, so it has to come
    // back exactly as written and has to be the one the directory implies for
    // that entry: the group's tag and the entry's own column part.
    let directory = directory();
    let entry = directory.entries()[2];
    let stamp = directory.stamp_for(&entry);
    assert_eq!(
        stamp,
        PageStamp {
            group_tag: TAG,
            id: id(3, 1),
        },
    );

    let mut bytes = Vec::new();
    stamp.encode_into(&mut bytes);
    assert_eq!(bytes.len(), PageStamp::LEN);
    let array: [u8; PageStamp::LEN] = bytes.try_into().expect("stamp length");
    assert_eq!(PageStamp::decode(array), stamp);
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
    let err = PageDirectory::new(ROWS, TAG, vec![entry(0, 200, 0, 0), entry(128, 64, 1, 0)])
        .expect_err("overlapping pages must be refused");
    assert!(
        format!("{err:?}").contains("overlapping"),
        "the error must name the overlap, got {err:?}",
    );
}

#[test]
fn descending_pages_are_refused_at_construction() {
    let err = PageDirectory::new(ROWS, TAG, vec![entry(4_096, 64, 1, 0), entry(0, 128, 0, 0)])
        .expect_err("descending pages must be refused");
    assert!(
        format!("{err:?}").contains("ascending"),
        "the error must name the ordering, got {err:?}",
    );
}

#[test]
fn two_pages_claiming_one_column_part_are_refused() {
    // Byte ranges that do not overlap can still be ambiguous: two pages that
    // both claim column 3 part 0 leave a reader no way to tell which one is
    // the column's data. Whichever it picked, the other page is silently
    // unreachable, and a lookup that returns the first match would hide it.
    let err = PageDirectory::new(ROWS, TAG, vec![entry(0, 64, 3, 0), entry(64, 64, 3, 0)])
        .expect_err("a duplicated column part must be refused");
    assert!(
        format!("{err:?}").contains("same column part"),
        "the error must name the duplicated part, got {err:?}",
    );
}

#[test]
fn a_page_extent_that_overflows_is_refused() {
    let err = PageDirectory::new(ROWS, TAG, vec![entry(u32::MAX - 8, 16, 0, 0)])
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
            entry(i * 4, 4, column_id, part)
        })
        .collect();
    let err =
        PageDirectory::new(ROWS, TAG, too_many).expect_err("a page count past u16 must be refused");
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
            entry(i * 40, 40, column_id, part)
        })
        .collect();
    let mut bytes = Vec::new();
    PageDirectory::new(ROWS, TAG, entries.clone())
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
    let directory =
        PageDirectory::contiguous(ROWS, TAG, [(id(0, 0), 100), (id(1, 0), 40), (id(2, 0), 7)])
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
        PageDirectory::contiguous(ROWS, TAG, [])
            .expect("no pages")
            .pages_len(),
        0,
        "a directory with no pages spans nothing",
    );
}

#[test]
fn a_contiguous_layout_that_overflows_is_refused() {
    let err = PageDirectory::contiguous(ROWS, TAG, [(id(0, 0), u32::MAX), (id(1, 0), 1)])
        .expect_err("a group length past u32 must be refused");
    assert!(
        format!("{err:?}").contains("overflow"),
        "the error must name the overflow, got {err:?}",
    );
}
