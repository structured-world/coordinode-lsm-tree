// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Disk format version
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FormatVersion {
    /// Version for 1.x.x releases
    V1 = 1,

    /// Version for 2.x.x releases
    V2,

    /// Version for 3.x.x releases
    V3,

    /// Version for range-tombstone SST semantics
    V4,

    /// `BuRR` (Bumped Ribbon Retrieval) filter wire format. Filter
    /// blocks are no longer Bloom-encoded; the magic + `filter_type` +
    /// per-layer header layout is documented in
    /// `src/table/filter/ribbon/burr/wire.rs`. V4 binaries cannot read
    /// V5 tables and vice versa — the filter block decoder rejects
    /// the wrong magic at open time.
    V5,
}

impl std::fmt::Display for FormatVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", u8::from(*self))
    }
}

impl From<FormatVersion> for u8 {
    fn from(value: FormatVersion) -> Self {
        match value {
            FormatVersion::V1 => 1,
            FormatVersion::V2 => 2,
            FormatVersion::V3 => 3,
            FormatVersion::V4 => 4,
            FormatVersion::V5 => 5,
        }
    }
}

impl TryFrom<u8> for FormatVersion {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            3 => Ok(Self::V3),
            4 => Ok(Self::V4),
            5 => Ok(Self::V5),
            _ => Err(()),
        }
    }
}
