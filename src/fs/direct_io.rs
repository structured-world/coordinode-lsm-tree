// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! `O_DIRECT` flag application shared between [`StdFs`] and
//! [`IoUringFs`] backends.
//!
//! Lives here (rather than inline in each backend's `open()`) so the
//! arch-gating list and the `O_DIRECT` bit value are defined in
//! exactly one place. Two backends with their own copy would silently
//! diverge if one was updated to support a new arch and the other
//! wasn't.
//!
//! Doc-contract for `direct_io` is on
//! [`FsOpenOptions::direct_io`](super::FsOpenOptions::direct_io):
//! the flag is best-effort, may be silently dropped, and correctness
//! must not depend on it.
//!
//! # `std` dependency
//!
//! This module touches `std::fs::OpenOptions` directly, so it is
//! std-only. No `#[cfg(feature = "std")]` gate is added here on
//! purpose: its sole consumers — [`StdFs`] and [`IoUringFs`] —
//! are themselves unconditionally std-bound today (the entire
//! `fs::*` backend builds on `std::fs`). Gating *only* this module
//! while leaving the consumers ungated would be a no-op — the std
//! backend would still compile and drag this module in
//! transitively, then fail. The unit of gating is the whole
//! `fs::*` std backend; that move is tracked under the no-std
//! migration epic (issue `#274`), where the
//! `#[cfg(feature = "std")]` gate will land on `pub mod fs::std_fs`
//! (and `io_uring_fs`) and this module follows automatically.
//!
//! [`StdFs`]: super::StdFs
//! [`IoUringFs`]: super::IoUringFs

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
mod apply {
    /// `asm-generic/fcntl.h`: `#define O_DIRECT 00040000`. Authoritative
    /// on every arch listed in the parent `cfg`. Arches with a
    /// divergent bit (arm `0o200000`, mips `0o100000`, parisc, sparc)
    /// are excluded from the `cfg` rather than handled here so we
    /// never risk passing the wrong bit to `open(2)`.
    const O_DIRECT: i32 = 0o0_040_000;

    /// Apply the `O_DIRECT` flag to a `std::fs::OpenOptions` builder
    /// when `direct_io` is requested AND the running target supports
    /// the authoritative `asm-generic/fcntl.h` value.
    pub fn apply_direct_io_flag(builder: &mut std::fs::OpenOptions, direct_io: bool) {
        if direct_io {
            use std::os::unix::fs::OpenOptionsExt;
            builder.custom_flags(O_DIRECT);
        }
    }
}

#[cfg(not(all(
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
)))]
mod apply {
    /// No-op outside Linux/Android on a supported arch. macOS would
    /// need `F_NOCACHE` via `fcntl` post-open, Windows would need
    /// `FILE_FLAG_NO_BUFFERING` at `CreateFile` time, divergent Linux
    /// arches need a different `O_DIRECT` bit — all out of scope per
    /// the [`super::FsOpenOptions::direct_io`] best-effort contract.
    pub fn apply_direct_io_flag(_builder: &mut std::fs::OpenOptions, _direct_io: bool) {}
}

pub(super) use apply::apply_direct_io_flag;
