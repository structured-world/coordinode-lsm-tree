// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use std::path::{Path, PathBuf};

pub fn absolute_path(path: &Path) -> PathBuf {
    // Not sure if this can even fail realistically
    #[expect(clippy::expect_used, reason = "not much we can do about it")]
    std::path::absolute(path).expect("should be absolute path")
}
