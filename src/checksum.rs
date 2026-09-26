// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum ChecksumType {
    Xxh3,
}

impl From<ChecksumType> for u8 {
    fn from(val: ChecksumType) -> Self {
        match val {
            ChecksumType::Xxh3 => 0,
        }
    }
}

/// An 128-bit checksum
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Checksum(u128);

impl From<crate::sfa::Checksum> for Checksum {
    fn from(value: crate::sfa::Checksum) -> Self {
        Self(value.into_u128())
    }
}

impl core::fmt::Display for Checksum {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Checksum {
    /// Wraps a checksum value.
    #[must_use]
    pub fn from_raw(value: u128) -> Self {
        Self(value)
    }

    /// Returns the raw 128-bit integer.
    #[must_use]
    pub fn into_u128(self) -> u128 {
        self.0
    }

    pub(crate) fn check(&self, expected: Self) -> crate::Result<()> {
        if self.0 == expected.0 {
            Ok(())
        } else {
            Err(crate::Error::ChecksumMismatch {
                expected,
                got: *self,
            })
        }
    }
}

pub struct ChecksummedWriter<W: crate::io::Write> {
    inner: W,
    hasher: xxhash_rust::xxh3::Xxh3Default,
    /// The stream position the next write lands at, counted rather than asked
    /// for: asking a buffered writer flushes it.
    position: u64,
}

// `crate::io::{Write,Seek}` is the std trait (via the std-mode supertrait
// blanket) under `std` and the native trait under `no_std`; the two impls
// differ only in the trait path and the `Result`/`SeekFrom` type they name,
// so each is gated to its build.
#[cfg(feature = "std")]
impl<W: crate::io::Write + crate::io::Seek> std::io::Seek for ChecksummedWriter<W> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let position = self.inner.seek(pos)?;
        self.position = position;
        Ok(position)
    }
}

#[cfg(not(feature = "std"))]
impl<W: crate::io::Write + crate::io::Seek> crate::io::Seek for ChecksummedWriter<W> {
    fn seek(&mut self, pos: crate::io::SeekFrom) -> crate::io::Result<u64> {
        let position = self.inner.seek(pos)?;
        self.position = position;
        Ok(position)
    }
}

impl<W: crate::io::Write> ChecksummedWriter<W> {
    /// Wraps `writer`, which must be positioned at the start of its stream.
    pub fn new(writer: W) -> Self {
        Self {
            inner: writer,
            hasher: xxhash_rust::xxh3::Xxh3Default::new(),
            position: 0,
        }
    }

    pub fn checksum(&self) -> Checksum {
        Checksum::from_raw(self.hasher.digest128())
    }

    /// The stream position the next write lands at.
    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.inner
    }
}

#[cfg(feature = "std")]
impl<W: crate::io::Write> std::io::Write for ChecksummedWriter<W> {
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }

    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Only what the inner writer took is part of the stream: a short
        // write is retried from its end, and hashing the whole buffer would
        // hash the retried tail twice.
        let n = self.inner.write(buf)?;
        self.hasher.update(buf.get(..n).unwrap_or(buf));
        self.position += n as u64;
        Ok(n)
    }
}

#[cfg(not(feature = "std"))]
impl<W: crate::io::Write> crate::io::Write for ChecksummedWriter<W> {
    fn flush(&mut self) -> crate::io::Result<()> {
        self.inner.flush()
    }

    fn write(&mut self, buf: &[u8]) -> crate::io::Result<usize> {
        // As the std impl: only the bytes taken are hashed and counted.
        let n = self.inner.write(buf)?;
        self.hasher.update(buf.get(..n).unwrap_or(buf));
        self.position += n as u64;
        Ok(n)
    }
}

#[cfg(all(test, feature = "std"))]
mod tests;
