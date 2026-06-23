// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::sfa::checksum::Checksum;

/// Error type
#[derive(Debug)]
pub enum Error {
    /// IO error
    Io(crate::io::Error),

    /// Invalid header
    InvalidHeader,

    /// Unsupported file format version
    InvalidVersion,

    /// Unsupported checksum type
    UnsupportedChecksumType,

    /// Checksum mismatch
    ChecksumMismatch {
        /// The calculated checksum
        got: Checksum,

        /// The expected checksum as defined in the file format
        expected: Checksum,
    },
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "SfaError: {self:?}")
    }
}

impl From<crate::io::Error> for Error {
    fn from(inner: crate::io::Error) -> Self {
        Self::Io(inner)
    }
}

// Std file-I/O paths (the archive reader/writer over `std::fs`) surface
// `std::io::Error`; bridge it through `crate::io::Error` so `?` keeps working
// in those call sites.
#[cfg(feature = "std")]
impl From<std::io::Error> for Error {
    fn from(inner: std::io::Error) -> Self {
        Self::Io(inner.into())
    }
}

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Io(inner) => Some(inner),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;
