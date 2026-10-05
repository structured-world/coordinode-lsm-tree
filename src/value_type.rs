// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

/// Value type (regular value or tombstone)
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(strum::EnumIter))]
pub enum ValueType {
    /// Existing value
    Value,

    /// Deleted value
    Tombstone,

    /// "Weak" deletion (a.k.a. `SingleDelete` in `RocksDB`)
    WeakTombstone,

    /// Merge operand
    ///
    /// Stores a partial update that will be combined with other operands
    /// and/or a base value via a user-provided [`crate::MergeOperator`].
    MergeOperand = 3,

    /// Value pointer
    ///
    /// Points to a blob in a blob file.
    Indirection = 4,

    /// A row written as cells, some of which may keep their bytes in a blob
    /// file (see [`crate::blob_tree::field_row`]). Reads as a put, like a
    /// value, with each referenced cell resolved to its object.
    CellRow = 5,
}

impl ValueType {
    /// Returns `true` if the type is a tombstone marker (either normal or weak).
    #[must_use]
    pub fn is_tombstone(self) -> bool {
        self == Self::Tombstone || self == Self::WeakTombstone
    }

    /// Whether the entry is a put in any of its physical forms: a value, a
    /// value kept in a blob file, or a row written as cells.
    pub(crate) fn is_put(self) -> bool {
        matches!(self, Self::Value | Self::Indirection | Self::CellRow)
    }

    pub(crate) fn is_indirection(self) -> bool {
        self == Self::Indirection
    }

    /// Whether the entry is a row written as cells.
    pub(crate) fn is_cell_row(self) -> bool {
        self == Self::CellRow
    }

    /// Returns `true` if the type is a merge operand.
    #[must_use]
    pub fn is_merge_operand(self) -> bool {
        self == Self::MergeOperand
    }
}

impl TryFrom<u8> for ValueType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Value),
            1 => Ok(Self::Tombstone),
            2 => Ok(Self::WeakTombstone),
            3 => Ok(Self::MergeOperand),
            4 => Ok(Self::Indirection),
            5 => Ok(Self::CellRow),
            _ => Err(()),
        }
    }
}

impl From<ValueType> for u8 {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Value => 0,
            ValueType::Tombstone => 1,
            ValueType::WeakTombstone => 2,
            ValueType::MergeOperand => 3,
            ValueType::Indirection => 4,
            ValueType::CellRow => 5,
        }
    }
}
