use crate::config::BenchConfig;
use lsm_tree::{AbstractTree, AnyTree, SeqNo};
use std::sync::atomic::{AtomicU64, Ordering};

/// Prefill a tree with sequential keys for read benchmarks.
pub fn prefill_sequential(
    tree: &AnyTree,
    config: &BenchConfig,
    seqno: &AtomicU64,
) -> lsm_tree::Result<()> {
    let batch_size = 10_000u64;

    for i in 0..config.num {
        let key = make_sequential_key(i, config.key_size);
        let value = make_value(config.value_size);
        let seq = seqno.fetch_add(1, Ordering::Relaxed);
        tree.insert(key, value, seq);

        // Flush every batch_size ops to build SSTs on disk.
        if (i + 1) % batch_size == 0 {
            tree.flush_active_memtable(0)?;
        }
    }

    // Final flush.
    tree.flush_active_memtable(0)?;

    eprintln!(
        "Prefilled {} keys ({} bytes/entry), {} tables on disk",
        config.num,
        config.entry_size(),
        tree.table_count(),
    );

    Ok(())
}

/// Prefill a tree with structured prefix keys for prefix scan benchmarks.
///
/// Keys have the format: `{prefix_u16_be}{suffix_u16_be}`, zero-padded to `key_size`.
pub fn prefill_prefix_keys(
    tree: &AnyTree,
    config: &BenchConfig,
    seqno: &AtomicU64,
    num_prefixes: u16,
) -> lsm_tree::Result<()> {
    // Integer division may leave a remainder — the `total >= config.num` guard
    // below ensures we insert exactly config.num keys (distributing the remainder
    // across the first few prefixes via the outer break).
    let mut keys_per_prefix = config.num / num_prefixes as u64;
    if keys_per_prefix == 0 && config.num > 0 {
        keys_per_prefix = 1;
    }
    let batch_size = 10_000u64;
    let mut total = 0u64;

    for prefix in 0..num_prefixes {
        if total >= config.num {
            break;
        }
        let prefix_bytes = prefix.to_be_bytes();
        for suffix in 0..keys_per_prefix {
            if total >= config.num {
                break;
            }
            let mut key = Vec::with_capacity(config.key_size);
            key.extend_from_slice(&prefix_bytes);
            // Use u16 suffix to keep minimum key size at 4 bytes (2+2).
            // Break if suffix exceeds u16 range (> 65535 keys per prefix).
            // This caps total keys at 256 * 65535 ≈ 16M for NUM_PREFIXES=256.
            let Ok(suffix_u16) = u16::try_from(suffix) else {
                eprintln!(
                    "Warning: prefix {prefix} truncated at {} keys (u16 suffix overflow)",
                    u16::MAX
                );
                break;
            };
            let suffix_bytes = suffix_u16.to_be_bytes();
            key.extend_from_slice(&suffix_bytes);
            key.resize(config.key_size, 0);

            let value = make_value(config.value_size);
            let seq = seqno.fetch_add(1, Ordering::Relaxed);
            tree.insert(key, value, seq);

            total += 1;
            if total.is_multiple_of(batch_size) {
                tree.flush_active_memtable(0)?;
            }
        }
    }

    tree.flush_active_memtable(0)?;

    eprintln!(
        "Prefilled {} keys across {} prefixes, {} tables on disk",
        total,
        num_prefixes,
        tree.table_count(),
    );

    Ok(())
}

/// Create a sequential key from a u64 index, padded or truncated to key_size.
///
/// For key_size >= 8: full BE u64 + zero-padding.
/// For key_size < 8: trailing (least-significant) bytes so small indices
/// produce distinct keys (e.g. key_size=4, index=1 → `[0,0,0,1]`).
/// # Panics
/// Panics if `key_size` is 0. All CLI workloads use `key_size >= 2` (validated
/// at workload level) and clap defaults to 16, so this cannot be triggered
/// through normal usage.
#[inline]
pub fn make_sequential_key(index: u64, key_size: usize) -> Vec<u8> {
    assert!(key_size > 0, "key_size must be > 0");
    let be_bytes = index.to_be_bytes();
    let mut key = Vec::with_capacity(key_size);

    if key_size >= 8 {
        key.extend_from_slice(&be_bytes);
        key.resize(key_size, 0);
    } else {
        // SAFETY (release builds): All workloads validate key_size at entry
        // (mergerandom >= 2, prefixscan >= 4, default 16), so collisions are
        // rejected before reaching here. The debug_assert below is a dev-time
        // belt-and-suspenders check and is intentionally compiled out in release.
        debug_assert!(
            index < (1u64 << (key_size * 8)),
            "index {index} exceeds unique key space for key_size {key_size}"
        );
        // Use trailing bytes so that sequential indices are distinct.
        key.extend_from_slice(&be_bytes[8 - key_size..]);
    }

    key
}

/// Create a random key of the given size.
///
/// `rand::rng()` returns a thread-local cached RNG (rand 0.9+), so calling
/// this in a tight loop does NOT re-seed on each invocation.
#[inline]
pub fn make_random_key(key_size: usize) -> Vec<u8> {
    use rand::Rng;
    let mut key = vec![0u8; key_size];
    rand::rng().fill(&mut key[..]);
    key
}

/// Create a deterministic value of the given size.
#[inline]
pub fn make_value(value_size: usize) -> Vec<u8> {
    vec![0x42u8; value_size]
}

/// Read the current seqno for point reads (must see all prefilled data).
pub fn read_seqno(seqno: &AtomicU64) -> SeqNo {
    seqno.load(Ordering::Relaxed)
}
