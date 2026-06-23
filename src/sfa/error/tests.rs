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
    assert!(
        matches!(&err, Error::Io(inner) if inner.kind() == crate::io::ErrorKind::NotFound),
        "expected Io(NotFound), got {err:?}"
    );
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
