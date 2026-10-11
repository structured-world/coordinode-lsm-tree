// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::*;
use crate::fs::MemFs;

const MIB: u64 = 1024 * 1024;

fn check(quota_headroom: u64) -> Check {
    Check {
        quota_headroom,
        floor: Floor::MayConsume,
    }
}

fn demand<'a>(fs: &'a MemFs, path: &'a str, bytes: u64) -> Demand<'a> {
    Demand {
        fs,
        path: Path::new(path),
        bytes,
    }
}

/// Two compactions on one volume cannot both take the same free bytes: the
/// second is checked against what the first holds, and the room the first
/// gives back as it goes is the second's again.
#[test]
fn second_reservation_sees_what_the_first_holds() {
    let fs = MemFs::with_capacity(100 * MIB);
    let pool = Arc::new(CapacityPool::default());

    let first = pool
        .reserve(1, &[demand(&fs, "/a", 60 * MIB)], Some(check(u64::MAX)))
        .expect("60 of 100 MiB fits");
    assert!(
        pool.reserve(2, &[demand(&fs, "/b", 60 * MIB)], Some(check(u64::MAX)))
            .is_none(),
        "the second 60 MiB would take bytes the first holds",
    );
    assert_eq!(pool.available(&fs, Path::new("/b")), 40 * MIB);

    drop(first);
    assert_eq!(
        pool.held(Pool::Volume(fs.volume_id(Path::new("/a")).expect("id"))),
        0
    );
    assert!(
        pool.reserve(2, &[demand(&fs, "/b", 60 * MIB)], Some(check(u64::MAX)))
            .is_some()
    );
}

/// Folders of one volume share its room; folders on volumes proven apart do
/// not.
#[test]
fn folders_share_a_volume_and_volumes_apart_do_not() {
    let shared = MemFs::with_capacity(100 * MIB);
    let other = MemFs::with_capacity(100 * MIB);
    let pool = Arc::new(CapacityPool::default());

    let _held = pool
        .reserve(1, &[demand(&shared, "/tables", 60 * MIB)], None)
        .expect("unchecked");
    assert!(
        !pool.would_fit(2, &[demand(&shared, "/blobs", 60 * MIB)], check(u64::MAX)),
        "another folder of the same volume",
    );
    assert!(
        pool.would_fit(2, &[demand(&other, "/blobs", 60 * MIB)], check(u64::MAX)),
        "a volume proven apart",
    );
}

/// The demands of one compaction on one volume add up.
#[test]
fn demands_of_one_compaction_on_one_volume_add_up() {
    let fs = MemFs::with_capacity(100 * MIB);
    let pool = Arc::new(CapacityPool::default());
    assert!(!pool.would_fit(
        1,
        &[
            demand(&fs, "/tables", 60 * MIB),
            demand(&fs, "/blobs", 60 * MIB)
        ],
        check(u64::MAX),
    ));
    assert!(pool.would_fit(
        1,
        &[
            demand(&fs, "/tables", 60 * MIB),
            demand(&fs, "/blobs", 30 * MIB)
        ],
        check(u64::MAX),
    ));
}

/// A folder no volume id is reported for is proven apart from none: it takes
/// room counted in every volume, and every volume's room counts it.
#[test]
fn an_unproven_folder_shares_every_volume() {
    assert!(Pool::Unproven.shares(Pool::Volume(1)));
    assert!(Pool::Volume(1).shares(Pool::Unproven));
    assert!(!Pool::Volume(1).shares(Pool::Volume(2)));
    assert!(!Pool::Volume(1).shares(Pool::Quota(1)));
    assert!(!Pool::Quota(1).shares(Pool::Quota(2)));

    let mut held = crate::HashMap::default();
    held.insert(Pool::Unproven, 30);
    held.insert(Pool::Volume(1), 10);
    held.insert(Pool::Volume(2), 20);
    assert_eq!(held_against(&held, Pool::Volume(1)), 40);
    assert_eq!(held_against(&held, Pool::Unproven), 60);
}

/// The quota caps what a tree's compactions hold together, on whatever
/// volumes they write.
#[test]
fn the_quota_caps_what_a_trees_compactions_hold_together() {
    let a = MemFs::with_capacity(100 * MIB);
    let b = MemFs::with_capacity(100 * MIB);
    let pool = Arc::new(CapacityPool::default());
    let _first = pool
        .reserve(1, &[demand(&a, "/t", 30 * MIB)], Some(check(50 * MIB)))
        .expect("30 of a 50 MiB quota");
    assert!(
        pool.reserve(1, &[demand(&b, "/t", 30 * MIB)], Some(check(50 * MIB)))
            .is_none(),
        "60 MiB in all past a 50 MiB quota",
    );
    assert!(
        pool.reserve(2, &[demand(&b, "/t", 30 * MIB)], Some(check(50 * MIB)))
            .is_some(),
        "another tree's quota is its own",
    );
    assert_eq!(pool.quota_held(1), 30 * MIB);
}

/// What lands is the volume's to count: the grant holds that much less, and
/// the rest goes back when the reservation does. The quota's grant is held
/// whole until then.
#[test]
fn landed_bytes_leave_the_grant_and_the_rest_goes_back_on_drop() -> crate::Result<()> {
    let fs = MemFs::with_capacity(100 * MIB);
    let volume = Pool::Volume(fs.volume_id(Path::new("/t")).expect("id"));
    let pool = Arc::new(CapacityPool::default());
    let room = pool
        .reserve(1, &[demand(&fs, "/t", 60 * MIB)], Some(check(u64::MAX)))
        .expect("fits");

    room.landed(&fs, Path::new("/t"), 25 * MIB)?;
    assert_eq!(pool.held(volume), 35 * MIB);
    assert_eq!(pool.quota_held(1), 60 * MIB);

    drop(room);
    assert_eq!(pool.held(volume), 0);
    assert_eq!(pool.quota_held(1), 0);
    Ok(())
}

/// An output past its grant is taken on while the volume keeps the flush
/// floor beside it, and stops its compaction when it does not.
#[test]
fn an_output_past_its_grant_grows_it_or_stops_the_compaction() -> crate::Result<()> {
    let fs = MemFs::with_capacity(100 * MIB);
    let pool = Arc::new(CapacityPool::default());
    let room = pool
        .reserve(1, &[demand(&fs, "/t", 10 * MIB)], Some(check(u64::MAX)))
        .expect("fits");

    // Plenty free: the excess is taken on, and the quota holds it.
    room.landed(&fs, Path::new("/t"), 15 * MIB)?;
    assert!(!room.refused());
    assert_eq!(pool.quota_held(1), 15 * MIB);

    // The volume keeps less than the floor: the compaction stops.
    fs.set_capacity(MIB / 2);
    let err = room
        .landed(&fs, Path::new("/t"), MIB)
        .expect_err("no room past the grant");
    assert!(matches!(err, crate::Error::StorageFull { .. }), "{err:?}");
    assert!(room.refused());
    Ok(())
}

/// An unchecked reservation refuses nothing, but a checked one sharing the
/// pool counts it.
#[test]
fn an_unchecked_reservation_is_counted_by_a_checked_one() -> crate::Result<()> {
    let fs = MemFs::with_capacity(100 * MIB);
    let pool = Arc::new(CapacityPool::default());
    let unchecked = pool
        .reserve(1, &[demand(&fs, "/t", 150 * MIB)], None)
        .expect("an unchecked reservation always holds");
    assert!(!pool.would_fit(2, &[demand(&fs, "/t", MIB)], check(u64::MAX)));
    // Past its grant on a volume with no room left, it still refuses nothing.
    fs.set_capacity(0);
    unchecked.landed(&fs, Path::new("/t"), 200 * MIB)?;
    assert!(!unchecked.refused());
    Ok(())
}

/// Taking more on after admission holds it for the others to see.
#[test]
fn added_room_is_held_until_the_reservation_goes() {
    let fs = MemFs::with_capacity(100 * MIB);
    let volume = Pool::Volume(fs.volume_id(Path::new("/t")).expect("id"));
    let pool = Arc::new(CapacityPool::default());
    let room = pool
        .reserve(1, &[demand(&fs, "/t", 10 * MIB)], Some(check(u64::MAX)))
        .expect("fits");
    room.add(&demand(&fs, "/blobs", 20 * MIB));
    assert_eq!(pool.held(volume), 30 * MIB);
    assert_eq!(pool.quota_held(1), 30 * MIB);
    drop(room);
    assert_eq!(pool.held(volume), 0);
    assert_eq!(pool.quota_held(1), 0);
}
