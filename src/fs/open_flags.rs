// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Platform open flags shared between [`StdFs`] and `IoUringFs`:
//! `O_DIRECT` for [`FsOpenOptions::direct_io`] and `O_NOFOLLOW` (or the
//! Windows reparse-point open) for [`FsOpenOptions::no_follow`].
//!
//! Lives here (rather than inline in each backend's `open()`) so the
//! arch-gating list and the bit values are defined in exactly one place.
//! Two backends with their own copy would silently diverge if one was
//! updated to support a new arch and the other wasn't. Both flags go
//! through one `custom_flags` call, which replaces whatever an earlier
//! call set.
//!
//! Doc-contract for the `direct_io` flag is on the
//! [`FsOpenOptions::direct_io`](field@super::FsOpenOptions::direct_io)
//! field (disambiguated against the same-named builder method): the
//! flag is best-effort, may be silently dropped, and correctness
//! must not depend on it.
//!
//! # `std` dependency
//!
//! This module touches `std::fs::OpenOptions` directly, so it is
//! std-only and the parent `mod open_flags;` declaration in
//! `fs/mod.rs` is gated behind `#[cfg(feature = "std")]`. That gate
//! is the first concrete honest step toward the no-std backend
//! split, but it does NOT by itself unblock a `no_std + alloc`
//! build: the wider `Fs` / `FsFile` trait surface (`std::io::{Read,
//! Write, Seek}`, `std::path::Path`) is std-bound at the trait
//! definition level, so even `MemFs` (alloc-only in its body) can't
//! compile under `--no-default-features --features alloc`. Porting
//! the traits off `std::io` / `std::path` is tracked separately
//! (issue `#311`), as a prerequisite for the rest of `fs::*` to
//! become honestly feature-gateable. When that lands, this module's
//! gate becomes load-bearing; until then it's a forward-looking
//! marker that keeps new std-side helpers honest with the policy.
//!
//! [`StdFs`]: super::StdFs
//! [`FsOpenOptions::direct_io`]: field@super::FsOpenOptions::direct_io
//! [`FsOpenOptions::no_follow`]: field@super::FsOpenOptions::no_follow

use super::FsOpenOptions;

/// The `O_DIRECT` bit when `direct_io` is requested AND the running target
/// supports the authoritative `asm-generic/fcntl.h` value.
#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "s390x",
    ),
))]
fn direct_io_bits(direct_io: bool) -> i32 {
    /// `asm-generic/fcntl.h`: `#define O_DIRECT 00040000`. Authoritative
    /// on every arch listed in the `cfg`. Arches with a divergent bit (arm
    /// `0o200000`, mips `0o100000`, parisc, sparc) are excluded from the
    /// `cfg` rather than handled here so we never risk passing the wrong
    /// bit to `open(2)`.
    const O_DIRECT: i32 = 0o0_040_000;
    if direct_io { O_DIRECT } else { 0 }
}

/// No `O_DIRECT` outside Linux/Android on a supported arch. macOS would need
/// `F_NOCACHE` via `fcntl` post-open, Windows `FILE_FLAG_NO_BUFFERING` at
/// `CreateFile` time, divergent Linux arches a different `O_DIRECT` bit: all
/// out of scope per the
/// [`FsOpenOptions::direct_io`](field@crate::fs::FsOpenOptions::direct_io)
/// best-effort contract.
#[cfg(all(
    unix,
    not(all(
        any(target_os = "linux", target_os = "android"),
        any(
            target_arch = "x86",
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "riscv32",
            target_arch = "riscv64",
            target_arch = "loongarch64",
            target_arch = "s390x",
        ),
    ))
))]
fn direct_io_bits(_direct_io: bool) -> i32 {
    0
}

/// Applies the platform flags `opts` asks for to a `std::fs::OpenOptions`
/// builder.
#[cfg(unix)]
pub fn apply_open_flags(builder: &mut std::fs::OpenOptions, opts: &FsOpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    let no_follow = if opts.no_follow { libc::O_NOFOLLOW } else { 0 };
    let flags = direct_io_bits(opts.direct_io) | no_follow;
    if flags != 0 {
        builder.custom_flags(flags);
    }
}

/// Applies the platform flags `opts` asks for to a `std::fs::OpenOptions`
/// builder. Direct I/O is not wired here (see
/// [`FsOpenOptions::direct_io`](field@crate::fs::FsOpenOptions::direct_io)).
#[cfg(windows)]
pub fn apply_open_flags(builder: &mut std::fs::OpenOptions, opts: &FsOpenOptions) {
    use std::os::windows::fs::OpenOptionsExt;
    /// `winbase.h`: opens a reparse point (a symbolic link) itself rather
    /// than the file it points to.
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    if opts.no_follow {
        builder.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
}

/// No platform flags on a target that is neither Unix nor Windows.
#[cfg(not(any(unix, windows)))]
pub fn apply_open_flags(_builder: &mut std::fs::OpenOptions, _opts: &FsOpenOptions) {}
