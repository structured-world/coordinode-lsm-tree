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
mod tests {
    use super::*;
    use core::error::Error as _;

    #[test]
    fn from_crate_io_error_wraps_io_variant() {
        let inner = crate::io::Error::from_kind(crate::io::ErrorKind::UnexpectedEof);
        let err = Error::from(inner);
        assert!(matches!(err, Error::Io(_)));
    }

    #[cfg(feature = "std")]
    #[test]
    fn from_std_io_error_bridges_through_crate_io() {
        let std_err = std::io::Error::from(std::io::ErrorKind::NotFound);
        let err = Error::from(std_err);
        match err {
            Error::Io(inner) => assert_eq!(inner.kind(), crate::io::ErrorKind::NotFound),
            other => panic!("expected Io variant, got {other:?}"),
        }
    }

    #[test]
    fn source_is_some_only_for_io_variant() {
        let io = Error::Io(crate::io::Error::from_kind(crate::io::ErrorKind::Other));
        assert!(io.source().is_some(), "Io must expose its inner as source");

        let non_io = Error::InvalidHeader;
        assert!(non_io.source().is_none(), "non-Io variants have no source");
    }

    #[test]
    fn display_prefixes_sfa_error() {
        // Display defers to the Debug rendering behind a fixed prefix.
        assert_eq!(
            alloc::format!("{}", Error::InvalidVersion),
            "SfaError: InvalidVersion"
        );
    }
}
