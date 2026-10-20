// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::Error;
use core::error::Error as _;

fn io(kind: crate::io::ErrorKind) -> Error {
    Error::Io(crate::io::Error::new(kind, "injected"))
}

/// The wrappers that carry a cause hand it on through the standard chain, so
/// a caller's error logging and retry classifier reach the step that failed.
#[test]
fn a_wrapped_failure_hands_on_its_cause() {
    for wrapped in [
        Error::SalvageOutput(Box::new(io(crate::io::ErrorKind::Other))),
        Error::InstallOutcomeUnknown(Box::new(io(crate::io::ErrorKind::Other))),
    ] {
        let source = wrapped.source().map(ToString::to_string);
        assert_eq!(
            source,
            Some(io(crate::io::ErrorKind::Other).to_string()),
            "{wrapped:?}"
        );
    }
}

/// An install of unknown outcome is as environmental as the failure behind
/// it: a full disk asks for a retry, a damaged file does not.
#[test]
fn an_unknown_install_outcome_is_classified_by_its_cause() {
    assert!(
        Error::InstallOutcomeUnknown(Box::new(io(crate::io::ErrorKind::PermissionDenied)))
            .is_environmental()
    );
    assert!(!Error::InstallOutcomeUnknown(Box::new(Error::Unrecoverable)).is_environmental());
    assert!(
        Error::SalvageOutput(Box::new(Error::Unrecoverable)).is_environmental(),
        "a salvage's output failure never implicates the source's bytes"
    );
}
