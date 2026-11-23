//! Stable hash functions used across the filter and table subsystems.
//!
//! `hash64` is the canonical 64-bit key hash piped into `BuRR` filters; it
//! is xxh3-based and deterministic across runs / processes / hosts.

/// Generates a 64-bit hash using xxh3.
#[must_use]
pub fn hash64(bytes: &[u8]) -> u64 {
    crate::xxh3::hash64(bytes)
}

/// Generates a 128-bit hash using xxh3.
#[must_use]
pub fn hash128(bytes: &[u8]) -> u128 {
    crate::xxh3::hash128(bytes)
}

#[cfg(test)]
mod tests;
