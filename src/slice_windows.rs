// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

pub trait GrowingWindowsExt<T> {
    fn growing_windows<'a>(&'a self) -> impl Iterator<Item = &'a [T]>
    where
        T: 'a;
}

impl<T> GrowingWindowsExt<T> for [T] {
    fn growing_windows<'a>(&'a self) -> impl Iterator<Item = &'a [T]>
    where
        T: 'a,
    {
        (1..=self.len()).flat_map(|size| self.windows(size))
    }
}

pub trait ShrinkingWindowsExt<T> {
    fn shrinking_windows<'a>(&'a self) -> impl Iterator<Item = &'a [T]>
    where
        T: 'a;
}

impl<T> ShrinkingWindowsExt<T> for [T] {
    fn shrinking_windows<'a>(&'a self) -> impl Iterator<Item = &'a [T]>
    where
        T: 'a,
    {
        (1..=self.len()).rev().flat_map(|size| self.windows(size))
    }
}

#[cfg(test)]
mod tests;
