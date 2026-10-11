// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Room compactions take on the volumes they write to: reserved before a
//! compaction writes, so that two compactions cannot both count on the same
//! free bytes, and held until what it wrote is installed or removed.
//!
//! A reservation is checked against the free space a volume reports less what
//! the reservations already on it have still to write. Every output file a
//! compaction finishes lands on its volume, whose report counts it from then
//! on, so the reservation holds that much less there. A compaction whose
//! outputs outgrow its reservation, as a compaction filter or a merge operator
//! can make them, takes the excess on as it lands, or stops when the volume
//! keeps no room beyond it, its inputs left as they were.

use crate::fs::Fs;
use crate::path::Path;
use crate::tree::inner::TreeId;
use alloc::sync::Arc;
use alloc::vec::Vec;
use portable_atomic::{AtomicBool, Ordering::Relaxed};

#[cfg(feature = "std")]
use parking_lot::Mutex;
// no-std: spin mirrors parking_lot's Mutex API without an allocator.
#[cfg(not(feature = "std"))]
use spin::Mutex;

/// Where reserved bytes are counted.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Pool {
    /// A volume, by the id its backend reports for it.
    Volume(u64),
    /// Every folder whose backend reports no volume id: it is proven apart
    /// from no other, so it is taken to share every volume.
    Unproven,
    /// A tree's quota.
    Quota(TreeId),
}

impl Pool {
    fn of(fs: &dyn Fs, path: &Path) -> Self {
        fs.volume_id(path).map_or(Self::Unproven, Self::Volume)
    }

    /// Whether bytes held in `other` take room counted in `self`: every
    /// volume shares the unproven pool, and a quota is its own.
    fn shares(self, other: Self) -> bool {
        match (self, other) {
            (Self::Quota(_), _) | (_, Self::Quota(_)) => self == other,
            (Self::Unproven, _) | (_, Self::Unproven) => true,
            (Self::Volume(a), Self::Volume(b)) => a == b,
        }
    }
}

/// Whether a reservation may take the room the engine keeps free for flushes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Floor {
    /// Only room the floor leaves: work that may as well wait.
    Keep,
    /// The floor as well, which breaks the deadlock of a compaction that
    /// frees space and has none to start in.
    MayConsume,
}

/// The room compactions in progress hold on the volumes they write to,
/// shared by every tree given the same pool.
///
/// Every tree keeps a pool of its own unless it is given one with
/// [`Config::compaction_capacity`](crate::Config::compaction_capacity). Trees
/// whose files share volumes should share a pool: each then reserves against
/// what the others have still to write instead of counting on the same free
/// bytes. The account is kept in this process: room another process takes
/// shows only in the free space the volume reports.
///
/// # Examples
///
/// ```
/// use lsm_tree::{CapacityPool, Config, SequenceNumberCounter};
/// use std::sync::Arc;
///
/// # let a_dir = tempfile::tempdir()?;
/// # let b_dir = tempfile::tempdir()?;
/// let pool = Arc::new(CapacityPool::default());
/// let a = Config::new(&a_dir, SequenceNumberCounter::default(), SequenceNumberCounter::default())
///     .compaction_capacity(Arc::clone(&pool));
/// let b = Config::new(&b_dir, SequenceNumberCounter::default(), SequenceNumberCounter::default())
///     .compaction_capacity(Arc::clone(&pool));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Default)]
pub struct CapacityPool {
    held: Mutex<crate::HashMap<Pool, u64>>,
}

impl core::fmt::Debug for CapacityPool {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CapacityPool").finish_non_exhaustive()
    }
}

/// Bytes a compaction is about to write into the folder `path` of `fs`.
pub struct Demand<'a> {
    pub fs: &'a dyn Fs,
    pub path: &'a Path,
    pub bytes: u64,
}

/// How a reservation is checked.
#[derive(Clone, Copy, Debug)]
pub struct Check {
    /// The room left under the tree's quota, `u64::MAX` without one.
    pub quota_headroom: u64,
    /// Whether the flush floor may be taken.
    pub floor: Floor,
}

/// What the account holds that takes room counted in `pool`.
fn held_against(held: &crate::HashMap<Pool, u64>, pool: Pool) -> u64 {
    held.iter()
        .filter(|(other, _)| pool.shares(**other))
        .map(|(_, bytes)| *bytes)
        .sum()
}

/// Whether `bytes` fit in `room`, leaving the flush floor unless it may be
/// taken.
fn fits(bytes: u64, room: u64, floor: Floor) -> bool {
    const FLOOR: u64 = crate::tree::MIN_RESERVED_HEADROOM;
    (room >= FLOOR && bytes <= room - FLOOR) || (floor == Floor::MayConsume && bytes <= room)
}

/// The bytes `demands` put into each pool, with the free space of the pool
/// when `probe` asks for it (`u64::MAX` otherwise), and their total.
fn gather(demands: &[Demand<'_>], probe: bool) -> (Vec<(Pool, u64, u64)>, u64) {
    // Read before the account is locked, so the lock covers arithmetic only.
    let mut volumes: Vec<(Pool, u64, u64)> = Vec::new();
    let mut total = 0u64;
    for demand in demands {
        let pool = Pool::of(demand.fs, demand.path);
        // Bounded by the bytes the volumes hold.
        total += demand.bytes;
        if let Some(slot) = volumes.iter_mut().find(|(p, _, _)| *p == pool) {
            slot.1 += demand.bytes;
            continue;
        }
        let free = if probe {
            demand.fs.available_space(demand.path).unwrap_or(u64::MAX)
        } else {
            u64::MAX
        };
        volumes.push((pool, demand.bytes, free));
    }
    (volumes, total)
}

/// Whether `volumes`, `total` bytes in all, fit beside what `held` holds:
/// each pool's room past what reservations sharing it hold takes what this
/// compaction writes into every pool sharing it, and the tree's quota takes
/// the total.
fn admits(
    held: &crate::HashMap<Pool, u64>,
    tree: TreeId,
    volumes: &[(Pool, u64, u64)],
    total: u64,
    check: Check,
) -> bool {
    volumes.iter().all(|&(pool, _, free)| {
        let own: u64 = volumes
            .iter()
            .filter(|(other, _, _)| pool.shares(*other))
            .map(|(_, bytes, _)| bytes)
            .sum();
        // Room past what is reserved is all there is: none, not an error,
        // once the reservations take it all.
        fits(
            own,
            free.saturating_sub(held_against(held, pool)),
            check.floor,
        )
    }) && total
        <= check
            .quota_headroom
            .saturating_sub(held_against(held, Pool::Quota(tree)))
}

impl CapacityPool {
    /// The free space the folder `path` reports, less what the reservations
    /// sharing its volume have still to write; `u64::MAX` when the backend
    /// reports none.
    pub(crate) fn available(&self, fs: &dyn Fs, path: &Path) -> u64 {
        let Ok(free) = fs.available_space(path) else {
            return u64::MAX;
        };
        // Free space past what is reserved is all there is to give: none,
        // not an error, once the reservations take it all.
        free.saturating_sub(held_against(&self.held.lock(), Pool::of(fs, path)))
    }

    /// What the compactions of `tree` in progress hold of its quota.
    pub(crate) fn quota_held(&self, tree: TreeId) -> u64 {
        self.held
            .lock()
            .get(&Pool::Quota(tree))
            .copied()
            .unwrap_or(0)
    }

    /// Whether `demands` of a compaction of `tree` would fit beside what the
    /// pool holds, as [`Self::reserve`] checks them, reserving nothing.
    pub(crate) fn would_fit(&self, tree: TreeId, demands: &[Demand<'_>], check: Check) -> bool {
        let (volumes, total) = gather(demands, true);
        admits(&self.held.lock(), tree, &volumes, total, check)
    }

    /// Reserves `demands` for a compaction of `tree`, or `None` when `check`
    /// finds no room for them. Without a check the room is reserved whatever
    /// is free, so that checked compactions sharing the pool still count it.
    pub(crate) fn reserve(
        self: &Arc<Self>,
        tree: TreeId,
        demands: &[Demand<'_>],
        check: Option<Check>,
    ) -> Option<Reservation> {
        let (volumes, total) = gather(demands, check.is_some());
        let quota = Pool::Quota(tree);
        let mut held = self.held.lock();
        if let Some(check) = check
            && !admits(&held, tree, &volumes, total, check)
        {
            return None;
        }
        for &(pool, bytes, _) in &volumes {
            *held.entry(pool).or_default() += bytes;
        }
        *held.entry(quota).or_default() += total;
        drop(held);
        Some(Reservation {
            pool: Arc::clone(self),
            check,
            grants: Mutex::new(Grants {
                volumes: volumes
                    .into_iter()
                    .map(|(pool, granted, _)| Grant {
                        pool,
                        granted,
                        landed: 0,
                    })
                    .collect(),
                quota: (quota, total),
            }),
            refused: AtomicBool::new(false),
        })
    }

    /// Gives `bytes` of `pool` back.
    fn release(&self, pool: Pool, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut held = self.held.lock();
        if let Some(slot) = held.get_mut(&pool) {
            debug_assert!(*slot >= bytes, "a pool gives back only what it held");
            *slot -= bytes;
            if *slot == 0 {
                held.remove(&pool);
            }
        }
    }

    /// What `pool` holds now.
    #[cfg(test)]
    fn held(&self, pool: Pool) -> u64 {
        self.held.lock().get(&pool).copied().unwrap_or(0)
    }
}

/// The room one volume gave a reservation, and how much of it has landed.
struct Grant {
    pool: Pool,
    granted: u64,
    landed: u64,
}

/// What a reservation holds: its grant on each volume, and on its tree's
/// quota, which is held whole until the reservation goes since an output
/// counts against the quota once it is installed, not once it lands.
struct Grants {
    volumes: Vec<Grant>,
    quota: (Pool, u64),
}

/// The room one compaction holds: given back as its outputs land on their
/// volumes, and wholly once it is dropped.
pub struct Reservation {
    pool: Arc<CapacityPool>,
    check: Option<Check>,
    grants: Mutex<Grants>,
    refused: AtomicBool,
}

impl Reservation {
    /// Takes `demand` on as well, without checking it: the caller found it to
    /// fit beside what the pool holds.
    pub(crate) fn add(&self, demand: &Demand<'_>) {
        if demand.bytes == 0 {
            return;
        }
        let pool = Pool::of(demand.fs, demand.path);
        let mut grants = self.grants.lock();
        if let Some(grant) = grants.volumes.iter_mut().find(|grant| grant.pool == pool) {
            grant.granted += demand.bytes;
        } else {
            grants.volumes.push(Grant {
                pool,
                granted: demand.bytes,
                landed: 0,
            });
        }
        grants.quota.1 += demand.bytes;
        let quota = grants.quota.0;
        drop(grants);
        let mut held = self.pool.held.lock();
        *held.entry(pool).or_default() += demand.bytes;
        *held.entry(quota).or_default() += demand.bytes;
    }

    /// A file of `len` bytes was written into the folder `path` of `fs`.
    ///
    /// The volume counts it now, so its grant holds that much less. A file
    /// the grant does not cover takes the excess on; a checked reservation
    /// then stops its compaction when the volume, past what the other
    /// reservations on it have still to write, keeps no more than the flush
    /// floor, or the tree's quota has no room for the excess.
    ///
    /// # Errors
    ///
    /// [`crate::Error::StorageFull`] when the compaction must stop.
    pub(crate) fn landed(&self, fs: &dyn Fs, path: &Path, len: u64) -> crate::Result<()> {
        let pool = Pool::of(fs, path);
        let mut grants = self.grants.lock();
        let (covered, excess) =
            if let Some(grant) = grants.volumes.iter_mut().find(|grant| grant.pool == pool) {
                let covered = len.min(grant.granted - grant.landed);
                grant.landed += len;
                grant.granted += len - covered;
                (covered, len - covered)
            } else {
                // A volume nothing was reserved on: all of it is excess.
                grants.volumes.push(Grant {
                    pool,
                    granted: len,
                    landed: len,
                });
                (0, len)
            };
        drop(grants);
        self.pool.release(pool, covered);
        if excess == 0 {
            return Ok(());
        }
        self.grow(fs, path, pool, excess)
    }

    /// Takes on `excess` bytes landed in `pool` beyond its grant.
    fn grow(&self, fs: &dyn Fs, path: &Path, pool: Pool, excess: u64) -> crate::Result<()> {
        let Some(check) = self.check else {
            return Ok(());
        };
        let free = fs.available_space(path).unwrap_or(u64::MAX);
        let mut grants = self.grants.lock();
        let (quota, granted) = grants.quota;
        let mut held = self.pool.held.lock();
        // The excess lies on the volume already: what is asked is whether the
        // volume keeps the floor beside it and what the others still write.
        let room = free.saturating_sub(held_against(&held, pool));
        // This reservation's own grant is among what its quota holds.
        let quota_room = check
            .quota_headroom
            .saturating_sub(held_against(&held, quota) - granted);
        if fits(0, room, Floor::Keep) && granted + excess <= quota_room {
            *held.entry(quota).or_default() += excess;
            grants.quota.1 += excess;
            return Ok(());
        }
        drop(held);
        drop(grants);
        self.refused.store(true, Relaxed);
        Err(crate::Error::StorageFull {
            used: granted + excess,
            limit: quota_room,
        })
    }

    /// Whether the reservation stopped its compaction.
    pub(crate) fn refused(&self) -> bool {
        self.refused.load(Relaxed)
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let grants = self.grants.get_mut();
        for grant in grants.volumes.drain(..) {
            self.pool.release(grant.pool, grant.granted - grant.landed);
        }
        self.pool.release(grants.quota.0, grants.quota.1);
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;

#[cfg(all(test, feature = "std"))]
#[expect(clippy::expect_used, reason = "test code")]
mod tree_tests;
