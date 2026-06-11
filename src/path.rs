// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

//! Path types for the storage layer.
//!
//! Dual, mirroring [`crate::io`]: under `std` these ARE
//! [`std::path::Path`] / [`std::path::PathBuf`] (re-export), so the default
//! `StdFs` backend and existing call sites are unchanged. Under
//! `no_std + alloc` they are thin newtypes over `str` / `String` — a storage
//! "path" is an object KEY (e.g. `"tables/123"`, `"MANIFEST"`), not a
//! filesystem path, which is exactly what an embedded / WASM-IndexedDB backend
//! addresses by. Components are `/`-separated.
//!
//! Filesystem *queries* (`exists` / `is_file` / `is_dir`) are deliberately NOT
//! on these types — under `no_std` there is no ambient filesystem; the engine
//! routes those through the injected [`crate::fs::Fs`] backend instead.

#[cfg(feature = "std")]
pub use std::path::{Path, PathBuf};

/// Absolute-path helper. Identity under `no_std` (object keys have no
/// process-relative ambiguity to resolve); canonicalises against the process
/// CWD under `std`.
#[cfg(feature = "std")]
#[must_use]
pub fn absolute_path(path: &Path) -> PathBuf {
    // Not sure if this can even fail realistically
    #[expect(clippy::expect_used, reason = "not much we can do about it")]
    std::path::absolute(path).expect("should be absolute path")
}

#[cfg(not(feature = "std"))]
pub use nostd::{Path, PathBuf};

/// Identity under `no_std`: an object key is already absolute (no CWD).
#[cfg(not(feature = "std"))]
#[must_use]
pub fn absolute_path(path: &Path) -> PathBuf {
    path.to_path_buf()
}

#[cfg(not(feature = "std"))]
mod nostd {
    use alloc::borrow::ToOwned;
    use alloc::string::String;
    use core::fmt;
    use core::ops::Deref;

    /// Borrowed path slice, mirroring the subset of [`std::path::Path`] the
    /// storage layer uses. `/`-separated object key. `#[repr(transparent)]`
    /// over `str` so `&str ⇄ &Path` is a zero-cost reference cast.
    #[repr(transparent)]
    pub struct Path {
        inner: str,
    }

    impl Path {
        /// Wraps a string slice as a `Path` (zero-cost).
        pub fn new<S: AsRef<str> + ?Sized>(s: &S) -> &Self {
            // SAFETY: `Path` is `#[repr(transparent)]` over `str`, so a
            // `&str` and a `&Path` have identical layout; the cast only
            // re-labels the reference type.
            unsafe { &*(core::ptr::from_ref::<str>(s.as_ref()) as *const Self) }
        }

        /// The underlying string slice.
        #[must_use]
        pub const fn as_str(&self) -> &str {
            &self.inner
        }

        /// Always `Some` (the whole key is valid UTF-8 by construction);
        /// mirrors [`std::path::Path::to_str`].
        #[must_use]
        pub const fn to_str(&self) -> Option<&str> {
            Some(&self.inner)
        }

        /// Lossless owned form; mirrors [`std::path::Path::to_string_lossy`]
        /// (always lossless here).
        #[must_use]
        pub fn to_string_lossy(&self) -> &str {
            &self.inner
        }

        /// Owned copy.
        #[must_use]
        pub fn to_path_buf(&self) -> PathBuf {
            PathBuf {
                inner: self.inner.to_owned(),
            }
        }

        /// Identity (already a `&Path`); mirrors
        /// [`std::path::Path::as_path`] on `PathBuf`-like deref chains.
        #[must_use]
        pub const fn as_path(&self) -> &Self {
            self
        }

        /// Appends `component`, inserting a `/` separator as needed.
        #[must_use]
        pub fn join<S: AsRef<str>>(&self, component: S) -> PathBuf {
            let mut buf = self.to_path_buf();
            buf.push(component);
            buf
        }

        /// Parent key (everything before the last `/`), or `None` at the root.
        #[must_use]
        pub fn parent(&self) -> Option<&Self> {
            self.inner
                .rfind('/')
                .map(|idx| Self::new(self.inner.split_at(idx).0))
        }

        /// Final `/`-separated component, or `None` if empty.
        #[must_use]
        pub fn file_name(&self) -> Option<&str> {
            if self.inner.is_empty() {
                return None;
            }
            Some(self.inner.rsplit('/').next().unwrap_or(&self.inner))
        }

        /// Whether this key begins with `base` on a component boundary.
        #[must_use]
        pub fn starts_with<S: AsRef<str>>(&self, base: S) -> bool {
            let base = base.as_ref().trim_end_matches('/');
            if base.is_empty() {
                return true;
            }
            &self.inner == base
                || self
                    .inner
                    .strip_prefix(base)
                    .is_some_and(|rest| rest.starts_with('/'))
        }

        /// Strips a leading `base` component prefix.
        ///
        /// # Errors
        /// Returns `Err(StripPrefixError)` if `self` does not start with `base`.
        pub fn strip_prefix<S: AsRef<str>>(&self, base: S) -> Result<&Self, StripPrefixError> {
            let base = base.as_ref().trim_end_matches('/');
            if base.is_empty() {
                return Ok(self);
            }
            self.inner
                .strip_prefix(base)
                .map(|rest| Self::new(rest.trim_start_matches('/')))
                .ok_or(StripPrefixError(()))
        }

        /// `/`-separated components, skipping empty segments.
        pub fn components(&self) -> impl Iterator<Item = &str> {
            self.inner.split('/').filter(|s| !s.is_empty())
        }

        /// `Display`-able view (the raw key).
        #[must_use]
        pub const fn display(&self) -> &str {
            &self.inner
        }

        /// Underlying bytes view; mirrors [`std::path::Path::as_os_str`].
        /// The key is always valid UTF-8 here, so this is the raw `str`.
        #[must_use]
        pub const fn as_os_str(&self) -> &str {
            &self.inner
        }
    }

    impl alloc::borrow::ToOwned for Path {
        type Owned = PathBuf;
        fn to_owned(&self) -> PathBuf {
            self.to_path_buf()
        }
    }

    impl fmt::Debug for Path {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            fmt::Debug::fmt(&self.inner, f)
        }
    }

    impl fmt::Display for Path {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.inner)
        }
    }

    impl PartialEq for Path {
        fn eq(&self, other: &Self) -> bool {
            self.inner == other.inner
        }
    }

    impl Eq for Path {}

    impl PartialOrd for Path {
        fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for Path {
        fn cmp(&self, other: &Self) -> core::cmp::Ordering {
            self.inner.cmp(&other.inner)
        }
    }

    impl core::hash::Hash for Path {
        fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
            self.inner.hash(state);
        }
    }

    impl AsRef<Path> for Path {
        fn as_ref(&self) -> &Path {
            self
        }
    }

    impl AsRef<str> for Path {
        fn as_ref(&self) -> &str {
            &self.inner
        }
    }

    /// Owned path key, mirroring the subset of [`std::path::PathBuf`] used.
    #[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct PathBuf {
        inner: String,
    }

    impl PathBuf {
        /// Empty path.
        #[must_use]
        pub const fn new() -> Self {
            Self {
                inner: String::new(),
            }
        }

        /// Borrow as a [`Path`].
        #[must_use]
        pub fn as_path(&self) -> &Path {
            Path::new(&self.inner)
        }

        /// Appends `component`, inserting a `/` separator when neither side
        /// already supplies one. An absolute `component` (leading `/`)
        /// replaces the buffer, matching `std`'s root-push semantics.
        pub fn push<S: AsRef<str>>(&mut self, component: S) {
            let c = component.as_ref();
            if c.starts_with('/') {
                self.inner.clear();
                self.inner.push_str(c);
                return;
            }
            if !self.inner.is_empty() && !self.inner.ends_with('/') {
                self.inner.push('/');
            }
            self.inner.push_str(c);
        }

        /// Removes the last component; returns `false` at the root.
        pub fn pop(&mut self) -> bool {
            match self.inner.rfind('/') {
                Some(idx) => {
                    self.inner.truncate(idx);
                    true
                }
                None if self.inner.is_empty() => false,
                None => {
                    self.inner.clear();
                    true
                }
            }
        }

        /// `Display`-able view.
        #[must_use]
        pub fn display(&self) -> &str {
            &self.inner
        }
    }

    impl Deref for PathBuf {
        type Target = Path;
        fn deref(&self) -> &Path {
            self.as_path()
        }
    }

    impl PartialEq<Path> for PathBuf {
        fn eq(&self, other: &Path) -> bool {
            self.as_path() == other
        }
    }

    impl PartialEq<PathBuf> for Path {
        fn eq(&self, other: &PathBuf) -> bool {
            self == other.as_path()
        }
    }

    impl PartialEq<&Path> for PathBuf {
        fn eq(&self, other: &&Path) -> bool {
            self.as_path() == *other
        }
    }

    impl alloc::borrow::Borrow<Path> for PathBuf {
        fn borrow(&self) -> &Path {
            self.as_path()
        }
    }

    impl AsRef<Path> for PathBuf {
        fn as_ref(&self) -> &Path {
            self.as_path()
        }
    }

    impl From<String> for PathBuf {
        fn from(inner: String) -> Self {
            Self { inner }
        }
    }

    impl From<&str> for PathBuf {
        fn from(s: &str) -> Self {
            Self {
                inner: s.to_owned(),
            }
        }
    }

    impl From<&Path> for PathBuf {
        fn from(p: &Path) -> Self {
            p.to_path_buf()
        }
    }

    impl AsRef<str> for PathBuf {
        fn as_ref(&self) -> &str {
            &self.inner
        }
    }

    impl fmt::Debug for PathBuf {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            fmt::Debug::fmt(&self.inner, f)
        }
    }

    impl fmt::Display for PathBuf {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.inner)
        }
    }

    /// Error returned by [`Path::strip_prefix`] when the prefix does not match.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct StripPrefixError(());

    impl fmt::Display for StripPrefixError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("prefix not found")
        }
    }
}
