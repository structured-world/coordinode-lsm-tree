// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The directory decides where every page of a row group is, so the cases
//! that matter are the ones where a wrong answer is still a plausible one: a
//! payload that decodes into a different set of pages than it was written
//! from, and a payload whose entries describe a placement no writer produces.

use super::{
    PageDirectory, PageEntry, PageId, PageStamp, PageZones, VERSION, ZONE_BOUND_LEN, ZoneBlock,
};

fn id(column_id: u16, part: u8) -> PageId {
    PageId { column_id, part }
}

/// No statistics zones: the directories below that are about placement, not
/// statistics.
fn no_zones() -> PageZones {
    PageZones::default()
}

/// Zones for column 0 over the fixture's two row pages: the first page's
/// values run `apple..mango`, and the second has 12 nulls among its rows.
fn key_zones() -> PageZones {
    let mut zones = PageZones::new(vec![0]);
    zones.push(0, Some((b"apple", b"mango")));
    zones.push(12, Some((b"melon", b"zucchini")));
    zones
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
        key_zones(),
        vec![],
    )
    .expect("ascending, non-overlapping, a complete grid")
}

/// Byte offset of the first entry's `flags` field: the header, the two row
/// pages, then the entry's offset, length, column id and part.
const FIRST_FLAGS_AT: usize = (1 + 2 + 4 + 8 + 2 + 2) + 2 * 4 + (4 + 4 + 2 + 1);

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
        no_zones(),
        vec![],
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
        no_zones(),
        vec![],
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
        no_zones(),
        vec![],
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
        no_zones(),
        vec![],
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
        no_zones(),
        vec![],
    )
    .expect_err("a gap in a part's row pages must be refused");
    assert!(
        format!("{err:?}").contains("missing a row page"),
        "the error must name the missing row page, got {err:?}",
    );
}

#[test]
fn row_pages_that_do_not_sum_to_the_group_are_refused() {
    let err = PageDirectory::new(ROWS, TAG, vec![200, 200], vec![], no_zones(), vec![])
        .expect_err("row pages short of the group must be refused");
    assert!(
        format!("{err:?}").contains("sum"),
        "the error must name the sum, got {err:?}",
    );
}

#[test]
fn an_empty_row_page_is_refused() {
    let err = PageDirectory::new(ROWS, TAG, vec![ROWS, 0], vec![], no_zones(), vec![])
        .expect_err("an empty row page must be refused");
    assert!(
        format!("{err:?}").contains("empty row page"),
        "the error must name the empty row page, got {err:?}",
    );
}

#[test]
fn a_page_naming_a_row_page_that_does_not_exist_is_refused() {
    let err = PageDirectory::new(
        ROWS,
        TAG,
        vec![ROWS],
        vec![entry(0, 64, 0, 0, 1)],
        no_zones(),
        vec![],
    )
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
        no_zones(),
        vec![],
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
    let err = PageDirectory::new(ROWS, TAG, vec![ROWS], too_many, no_zones(), vec![])
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
    PageDirectory::new(ROWS, TAG, vec![ROWS], entries.clone(), no_zones(), vec![])
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
        no_zones(),
        vec![],
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
        PageDirectory::contiguous(0, TAG, vec![], [], no_zones(), vec![])
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
        no_zones(),
        vec![],
    )
    .expect_err("a group length past u32 must be refused");
    assert!(
        format!("{err:?}").contains("overflow"),
        "the error must name the overflow, got {err:?}",
    );
}

/// A directory over the fixture's grid carrying `zones` in place of its own.
fn with_zones(zones: PageZones) -> crate::Result<PageDirectory> {
    let base = directory();
    PageDirectory::new(
        ROWS,
        TAG,
        base.row_pages().to_vec(),
        base.entries().to_vec(),
        zones,
        vec![],
    )
}

#[test]
fn zones_round_trip_and_answer_per_row_page_and_column() {
    // A reader prunes a row page by its zone before reading it, so the zone
    // it looks up has to be that row page's, for that column, exactly as
    // written, and a column without zones must answer none rather than
    // another column's.
    let mut bytes = Vec::new();
    directory().encode_into(&mut bytes);
    let decoded = PageDirectory::decode(&bytes).expect("decode");

    let zones = decoded.zones();
    let first = zones.zone(0, 0).expect("row page 0 of column 0");
    assert_eq!((first.null_count, first.min), (0, &b"apple"[..]));
    assert_eq!(first.max, Some(&b"mango"[..]));
    let second = zones.zone(1, 0).expect("row page 1 of column 0");
    assert_eq!((second.null_count, second.min), (12, &b"melon"[..]));
    assert_eq!(second.max, Some(&b"zucchini"[..]));
    assert!(zones.zone(0, 3).is_none(), "column 3 has no zones");
    assert!(zones.zone(2, 0).is_none(), "there is no third row page");
}

#[test]
fn a_long_minimum_is_cut_to_a_prefix_and_a_long_maximum_raised() {
    // A cut bound must still bound: the prefix of the minimum is no greater
    // than it, and the cut maximum, its last kept byte raised, is greater
    // than every value that starts with the kept bytes.
    let min = [b'a'; 100];
    let mut max = [b'b'; 100];
    max[ZONE_BOUND_LEN - 2] = u8::MAX;
    max[ZONE_BOUND_LEN - 1] = u8::MAX;
    let mut zones = PageZones::new(vec![0]);
    zones.push(0, Some((&min, &max)));
    zones.push(0, Some((b"x", b"y")));

    let directory = with_zones(zones).expect("cut bounds are within the limit");
    let zone = directory.zones().zone(0, 0).expect("zone");
    assert_eq!(
        zone.min,
        &min[..ZONE_BOUND_LEN],
        "the minimum keeps its prefix"
    );
    let bound = zone.max.expect("the maximum has a byte to raise");
    let mut expected = vec![b'b'; ZONE_BOUND_LEN - 3];
    expected.push(b'c');
    assert_eq!(
        bound,
        expected.as_slice(),
        "the last byte below 0xFF is raised"
    );
    assert!(bound > &max[..], "the cut maximum is above the value");
    assert!(zone.min <= &min[..], "the cut minimum is below the value");
}

#[test]
fn a_maximum_whose_prefix_is_all_ff_has_no_upper_bound() {
    // No byte of the prefix can be raised, so no short bound lies above the
    // value. The zone keeps no upper bound rather than a wrong one, and says
    // so on the wire.
    let max = [u8::MAX; ZONE_BOUND_LEN + 1];
    let mut zones = PageZones::new(vec![0]);
    zones.push(0, Some((b"a", &max)));
    zones.push(0, Some((b"a", b"b")));
    let original = with_zones(zones).expect("an unbounded zone is valid");
    assert_eq!(original.zones().zone(0, 0).expect("zone").max, None);

    let mut bytes = Vec::new();
    original.encode_into(&mut bytes);
    let decoded = PageDirectory::decode(&bytes).expect("decode");
    assert_eq!(decoded, original, "the missing bound survives the wire");
}

#[test]
fn an_all_null_zone_records_the_empty_range() {
    let mut zones = PageZones::new(vec![0]);
    zones.push(200, None);
    zones.push(0, Some((b"", b"")));
    let directory = with_zones(zones).expect("all-null and all-empty zones are valid");
    let zone = directory.zones().zone(0, 0).expect("zone");
    assert_eq!(
        (zone.null_count, zone.min, zone.max),
        (200, &[][..], Some(&[][..]))
    );
}

/// The refusal a directory carrying `zones` meets.
fn refused(zones: PageZones) -> String {
    format!(
        "{:?}",
        with_zones(zones).expect_err("the zones must be refused")
    )
}

#[test]
fn zones_that_would_prune_a_matching_row_page_are_refused() {
    // Each of these zones, read as written, proves something false about its
    // row page, and a reader acting on it would skip rows that match.
    let mut more_nulls_than_rows = PageZones::new(vec![0]);
    more_nulls_than_rows.push(201, Some((b"a", b"b")));
    more_nulls_than_rows.push(0, Some((b"a", b"b")));
    assert!(refused(more_nulls_than_rows).contains("more nulls"));

    let mut inverted = PageZones::new(vec![0]);
    inverted.push_bounds(0, b"z", Some(b"a"));
    inverted.push(0, Some((b"a", b"b")));
    assert!(refused(inverted).contains("above its upper bound"));

    let mut all_null_with_range = PageZones::new(vec![0]);
    all_null_with_range.push_bounds(200, b"a", Some(b"b"));
    all_null_with_range.push(0, Some((b"a", b"b")));
    assert!(refused(all_null_with_range).contains("all-null"));

    let long = [b'a'; ZONE_BOUND_LEN + 1];
    let mut uncut = PageZones::new(vec![0]);
    uncut.push_bounds(0, &long, Some(&long));
    uncut.push(0, Some((b"a", b"b")));
    assert!(refused(uncut).contains("longer than a zone keeps"));
}

#[test]
fn zones_that_do_not_match_the_grid_are_refused() {
    let mut missing_row_page = PageZones::new(vec![0]);
    missing_row_page.push(0, Some((b"a", b"b")));
    assert!(refused(missing_row_page).contains("every row page"));

    let mut unknown_column = PageZones::new(vec![9]);
    unknown_column.push(0, Some((b"a", b"b")));
    unknown_column.push(0, Some((b"a", b"b")));
    assert!(refused(unknown_column).contains("does not have"));

    let mut twice = PageZones::new(vec![0, 0]);
    for _ in 0..4 {
        twice.push(0, Some((b"a", b"b")));
    }
    assert!(refused(twice).contains("two zones"));
}

#[test]
fn a_zone_flag_the_reader_does_not_know_is_refused() {
    // The flags byte of the last zone sits after its null count; the fixture's
    // last zone holds `melon` and `zucchini`.
    let mut bytes = Vec::new();
    directory().encode_into(&mut bytes);
    let flags_at = bytes.len() - (1 + 5 + 1 + 8) - 1;
    bytes[flags_at] = 2;
    let err = PageDirectory::decode(&bytes).expect_err("an unknown zone flag must be refused");
    assert!(
        format!("{err:?}").contains("reserved zone flag"),
        "got {err:?}"
    );

    bytes[flags_at] = 1;
    let err = PageDirectory::decode(&bytes).expect_err("an unbounded zone with a bound");
    assert!(
        format!("{err:?}").contains("records an upper bound"),
        "got {err:?}"
    );
}

/// The zones of column 3 over the fixture's two row pages, as a zone block
/// carries them.
fn block_zones() -> PageZones {
    let mut zones = PageZones::new(vec![3]);
    zones.push(0, Some((b"aa", b"bb")));
    zones.push(312, None);
    zones
}

/// The fixture's directory with a zone block for column 3 of `length` bytes.
fn with_zone_block(length: u32) -> crate::Result<PageDirectory> {
    let base = directory();
    PageDirectory::new(
        ROWS,
        TAG,
        base.row_pages().to_vec(),
        base.entries().to_vec(),
        key_zones(),
        vec![ZoneBlock {
            column_id: 3,
            length,
        }],
    )
}

#[test]
fn a_zone_block_round_trips_against_its_directory() {
    // A zone block holds one column's zones, for the same row pages as the
    // directory: the directory lists it with its length, and read back against
    // that listing it answers what was written.
    let directory = with_zone_block(96).expect("a directory listing a zone block");
    let mut bytes = Vec::new();
    directory.encode_into(&mut bytes);
    let decoded = PageDirectory::decode(&bytes).expect("decode");
    assert_eq!(
        decoded, directory,
        "the zone block listing survives the wire"
    );
    assert_eq!(decoded.zone_block(3), Some((0, 96)));
    assert_eq!(
        decoded.zone_block(0),
        None,
        "the key's zones are in the directory"
    );

    let mut payload = Vec::new();
    PageDirectory::encode_zone_block(TAG, &block_zones(), &mut payload);
    let zones = directory
        .decode_zone_block(3, &payload)
        .expect("column 3's zones for the group's row pages");
    assert_eq!(zones, block_zones());
    assert_eq!(zones.zone(1, 3).map(|z| z.null_count), Some(312));
}

#[test]
fn zone_blocks_are_placed_one_after_another() {
    // A reader finds a column's zone block by the lengths of the blocks
    // listed before it.
    let base = directory();
    let mut entries = base.entries().to_vec();
    let last = entries.last().copied().expect("the fixture has pages");
    entries.push(entry(last.offset + last.length, 16, 5, 0, 0));
    entries.push(entry(last.offset + last.length + 16, 16, 5, 0, 1));
    let directory = PageDirectory::new(
        ROWS,
        TAG,
        base.row_pages().to_vec(),
        entries,
        key_zones(),
        vec![
            ZoneBlock {
                column_id: 3,
                length: 96,
            },
            ZoneBlock {
                column_id: 5,
                length: 40,
            },
        ],
    )
    .expect("two zone blocks");
    assert_eq!(directory.zone_block(3), Some((0, 96)));
    assert_eq!(directory.zone_block(5), Some((96, 40)));
    assert_eq!(
        directory.group_len(100),
        Some(100 + directory.pages_len() + 136),
        "the group ends after the last zone block",
    );
}

#[test]
fn a_zone_block_listing_that_cannot_be_right_is_refused() {
    let base = directory();
    let listing = |blocks: Vec<ZoneBlock>| {
        PageDirectory::new(
            ROWS,
            TAG,
            base.row_pages().to_vec(),
            base.entries().to_vec(),
            key_zones(),
            blocks,
        )
    };
    let block = |column_id, length| ZoneBlock { column_id, length };
    let refusal = |blocks| format!("{:?}", listing(blocks).expect_err("must be refused"));

    assert!(refusal(vec![block(3, 8), block(3, 8)]).contains("two zone blocks"));
    assert!(refusal(vec![block(9, 8)]).contains("does not have"));
    // Column 0's zones are in the directory; a second set would be a second
    // answer for the same row pages.
    assert!(refusal(vec![block(0, 8)]).contains("directory and a zone block"));
    assert!(refusal(vec![block(3, 0)]).contains("empty zone block"));
}

#[test]
fn a_zone_block_that_does_not_fit_its_directory_is_refused() {
    let directory = with_zone_block(96).expect("a directory listing a zone block");

    let mut trailing = Vec::new();
    PageDirectory::encode_zone_block(TAG, &block_zones(), &mut trailing);
    trailing.push(0);
    let err = directory
        .decode_zone_block(3, &trailing)
        .expect_err("trailing bytes must be refused");
    assert!(format!("{err:?}").contains("trailing"), "got {err:?}");

    // Another column's zones in column 3's place: they would prune column 3's
    // row pages by another column's values.
    let mut elsewhere = PageZones::new(vec![0]);
    elsewhere.push(0, Some((b"a", b"b")));
    elsewhere.push(0, Some((b"a", b"b")));
    let mut payload = Vec::new();
    PageDirectory::encode_zone_block(TAG, &elsewhere, &mut payload);
    let err = directory
        .decode_zone_block(3, &payload)
        .expect_err("another column's zones must be refused");
    assert!(format!("{err:?}").contains("another column"), "got {err:?}");

    // Column 3's own zones from another group: they describe that group's
    // row pages, not these.
    let mut foreign = Vec::new();
    PageDirectory::encode_zone_block(TAG + 1, &block_zones(), &mut foreign);
    let err = directory
        .decode_zone_block(3, &foreign)
        .expect_err("another group's zone block must be refused");
    assert!(
        format!("{err:?}").contains("another row group"),
        "got {err:?}"
    );

    let err = directory
        .decode_zone_block(3, &[0; 7])
        .expect_err("a payload shorter than the tag must be refused");
    assert!(format!("{err:?}").contains("group tag"), "got {err:?}");

    // Zones for one row page of a group of two.
    let mut short = PageZones::new(vec![3]);
    short.push(0, Some((b"a", b"b")));
    let mut payload = Vec::new();
    PageDirectory::encode_zone_block(TAG, &short, &mut payload);
    let err = directory
        .decode_zone_block(3, &payload)
        .expect_err("zones for fewer row pages than the group has must be refused");
    assert!(
        matches!(err, crate::Error::InvalidHeader("ColumnZones")),
        "got {err:?}"
    );
}
