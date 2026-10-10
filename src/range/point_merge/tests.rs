// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::take_versions;
use crate::{InternalValue, ValueType};

fn version(seqno: u64, value_type: ValueType) -> InternalValue {
    InternalValue::from_components(b"k" as &[u8], b"v" as &[u8], seqno, value_type)
}

/// A source's versions are taken down to its first base; the older ones it
/// hides are left unread.
#[test]
fn take_versions_stops_at_the_first_base() -> crate::Result<()> {
    let mut entries = Vec::new();
    let read = core::cell::Cell::new(0);
    take_versions(
        [
            version(9, ValueType::MergeOperand),
            version(8, ValueType::MergeOperand),
            version(7, ValueType::Tombstone),
            version(6, ValueType::Value),
        ]
        .into_iter()
        .inspect(|_| read.set(read.get() + 1))
        .map(Ok),
        &mut entries,
    )?;
    let seqnos: Vec<u64> = entries.iter().map(|e| e.key.seqno).collect();
    assert_eq!(seqnos, [9, 8, 7]);
    assert_eq!(read.get(), 3, "the version below the base is not read");
    Ok(())
}

/// A source holding only operands is taken whole.
#[test]
fn take_versions_takes_every_operand_without_a_base() -> crate::Result<()> {
    let mut entries = Vec::new();
    take_versions(
        [
            version(3, ValueType::MergeOperand),
            version(2, ValueType::MergeOperand),
        ]
        .into_iter()
        .map(Ok),
        &mut entries,
    )?;
    assert_eq!(entries.len(), 2);
    Ok(())
}

/// A failed read ends the take with its error, keeping what came before.
#[test]
fn take_versions_returns_the_error_of_a_failed_read() {
    let mut entries = Vec::new();
    let result = take_versions(
        [
            Ok(version(5, ValueType::MergeOperand)),
            Err(crate::Error::Unrecoverable),
            Ok(version(3, ValueType::Value)),
        ]
        .into_iter(),
        &mut entries,
    );
    assert!(matches!(result, Err(crate::Error::Unrecoverable)));
    assert_eq!(entries.len(), 1);
}
