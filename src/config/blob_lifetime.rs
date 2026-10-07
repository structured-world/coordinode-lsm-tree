// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::fs::WriteLifetime;
use alloc::sync::Arc;

/// How many lifetime groups blob values are split into; see
/// [`KvSeparationOptions::lifetime_groups`](crate::KvSeparationOptions::lifetime_groups).
///
/// Every group a write meets keeps its own blob file open, so the count bounds
/// the files a flush or a relocation writes at once. It never exceeds
/// [`Self::MAX`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct LifetimeGroups(u8);

impl LifetimeGroups {
    /// One group: every value shares the same files, as without lifetime
    /// grouping. The default.
    pub const ONE: Self = Self(1);

    /// The largest number of groups, one per [`WriteLifetime`] a device can be
    /// told about.
    pub const MAX: Self = Self(4);

    /// `count` groups, or `None` when `count` is zero or above [`Self::MAX`].
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::config::LifetimeGroups;
    ///
    /// assert!(LifetimeGroups::new(3).is_some());
    /// assert!(LifetimeGroups::new(0).is_none());
    /// assert!(LifetimeGroups::new(5).is_none());
    /// ```
    #[must_use]
    pub const fn new(count: u8) -> Option<Self> {
        if count == 0 || count > Self::MAX.0 {
            None
        } else {
            Some(Self(count))
        }
    }

    /// The number of groups.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }

    /// `class` brought into range: a class past the last group goes to the
    /// last group, which holds the longest-lived values.
    #[must_use]
    pub(crate) fn cap(self, class: u8) -> u8 {
        class.min(self.0 - 1)
    }

    /// The class a value written for the first time gets, when nothing says
    /// it is short-lived: the second group, or the only one.
    #[must_use]
    pub(crate) fn fresh(self) -> u8 {
        self.cap(1)
    }

    /// The class of a value relocated out of a file of `source` class: one
    /// group older, since it outlived the file it was in.
    #[must_use]
    pub(crate) fn relocated(self, source: u8) -> u8 {
        // `cap` leaves at most `MAX - 1`, so the increment cannot overflow.
        self.cap(self.cap(source) + 1)
    }

    /// What the device is told about the files of `class`, or nothing when
    /// values are not grouped by lifetime.
    #[must_use]
    pub(crate) fn write_lifetime(self, class: u8) -> Option<WriteLifetime> {
        if self.0 == 1 {
            return None;
        }
        Some(match self.cap(class) {
            0 => WriteLifetime::Short,
            1 => WriteLifetime::Medium,
            2 => WriteLifetime::Long,
            _ => WriteLifetime::Extreme,
        })
    }
}

impl Default for LifetimeGroups {
    fn default() -> Self {
        Self::ONE
    }
}

/// Picks the lifetime class of a value written for the first time.
#[derive(Copy, Clone)]
pub struct LifetimeClassifier<'a> {
    pub(crate) groups: LifetimeGroups,
    pub(crate) hint: Option<&'a LifetimeHint>,
}

impl LifetimeClassifier<'_> {
    /// Whether values are split by lifetime at all.
    #[must_use]
    pub(crate) fn is_grouping(&self) -> bool {
        self.groups != LifetimeGroups::ONE
    }

    /// The class a value of `key` written now goes to: the hint's when it
    /// answers, else the shortest-lived group when the write found the key
    /// `overwritten`, else the group of a fresh value.
    #[must_use]
    pub(crate) fn class_of(&self, key: &[u8], overwritten: bool) -> u8 {
        if !self.is_grouping() {
            return 0;
        }
        match self.hint.and_then(|hint| hint.class_of(key)) {
            Some(class) => self.groups.cap(class),
            None if overwritten => 0,
            None => self.groups.fresh(),
        }
    }
}

/// The caller's estimate of how long the value of a key lives.
///
/// It returns a lifetime class: `0` is the shortest-lived group, higher
/// classes live longer, and `None` leaves the key to the observed estimate. See
/// [`KvSeparationOptions::lifetime_hint`](crate::KvSeparationOptions::lifetime_hint).
pub type LifetimeHintFn = dyn Fn(&[u8]) -> Option<u8> + Send + Sync;

/// A [`LifetimeHintFn`] held by the options.
///
/// Two hints are equal only when they are the same function object, which is
/// all options comparison can tell about a closure.
#[derive(Clone)]
pub struct LifetimeHint(pub(crate) Arc<LifetimeHintFn>);

impl LifetimeHint {
    /// Wraps `hint`.
    pub fn new(hint: impl Fn(&[u8]) -> Option<u8> + Send + Sync + 'static) -> Self {
        Self(Arc::new(hint))
    }

    /// The class `hint` gives `key`, if any.
    pub(crate) fn class_of(&self, key: &[u8]) -> Option<u8> {
        (self.0)(key)
    }
}

impl core::fmt::Debug for LifetimeHint {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("LifetimeHint")
    }
}

impl PartialEq for LifetimeHint {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
