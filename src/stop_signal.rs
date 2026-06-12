// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use alloc::sync::Arc;
use core::sync::atomic::AtomicBool;

#[derive(Clone, Debug, Default)]
pub struct StopSignal(Arc<AtomicBool>);

impl StopSignal {
    pub fn send(&self) {
        self.0.store(true, core::sync::atomic::Ordering::Release);
    }

    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.0.load(core::sync::atomic::Ordering::Acquire)
    }
}
