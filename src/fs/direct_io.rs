// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! `O_DIRECT` flag application shared between [`StdFs`] and
//! `IoUringFs` backends.
//!
//! Lives here (rather than inline in each backend's `open()`) so the
//! arch-gating list and the `O_DIRECT` bit value are defined in
//! exactly one place. Two backends with their own copy would silently
//! diverge if one was updated to support a new arch and the other
//! wasn't.
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
//! std-only and the parent `mod direct_io;` declaration in
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
    /// the [`FsOpenOptions::direct_io`](field@crate::fs::FsOpenOptions::direct_io)
    /// best-effort contract (disambiguated against the same-named
    /// builder method).
    pub fn apply_direct_io_flag(_builder: &mut std::fs::OpenOptions, _direct_io: bool) {}
}

pub(super) use apply::apply_direct_io_flag;
