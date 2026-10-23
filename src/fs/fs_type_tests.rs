// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::{FsCapabilities, linux_fs_type_capabilities};

/// Each file system the backends recognize by its `statfs` magic gets its
/// profile, and any other magic the conservative default: a mount that is not
/// known never reports a capability it may lack (a network mount refusing a
/// punch would surprise tight-space compaction).
#[test]
fn each_known_fs_type_gets_its_profile_and_any_other_the_default() {
    let full = FsCapabilities {
        per_block_integrity_on_read: true,
        background_scrub: true,
        copy_on_write: true,
        reflink: true,
        native_snapshot: true,
        punch_hole: true,
        ..FsCapabilities::default()
    };
    let punch_only = FsCapabilities {
        punch_hole: true,
        ..FsCapabilities::default()
    };
    for (name, magic, expected) in [
        ("btrfs", 0x9123_683E, full),
        ("zfs", 0x2FC1_2FC1, full),
        (
            "xfs",
            0x5846_5342,
            FsCapabilities {
                copy_on_write: true,
                reflink: true,
                punch_hole: true,
                ..FsCapabilities::default()
            },
        ),
        ("ext2/3/4", 0x0000_EF53, punch_only),
        ("tmpfs", 0x0102_1994, punch_only),
        ("nfs", 0x0000_6969, FsCapabilities::default()),
        ("unknown", 0, FsCapabilities::default()),
    ] {
        assert_eq!(linux_fs_type_capabilities(magic), expected, "{name}");
    }
}
