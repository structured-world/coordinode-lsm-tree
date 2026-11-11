use super::*;
use crate::fs::StdFs;
use crate::{InternalValue, ValueType};
use alloc::sync::Arc;

/// `count` entries with keys of up to `key_len` bytes and values of `value_len`
/// bytes that no codec shrinks, in key order: what makes a codec grow its
/// blocks rather than shrink them.
fn entries(count: usize, key_len: usize, value_len: usize) -> Vec<InternalValue> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut noise = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 56) as u8
    };
    (0..count)
        .map(|i| {
            let mut key = format!("k{i:08}").into_bytes();
            key.resize(key_len.max(key.len()), b'k');
            let value: Vec<u8> = (0..value_len).map(|_| noise()).collect();
            InternalValue::from_components(key, value, i as u64, ValueType::Value)
        })
        .collect()
}

/// What writing `entries` puts through a writer, as the bound counts it.
fn load_of(entries: &[InternalValue]) -> RewriteLoad {
    RewriteLoad {
        entries: entries.len() as u64,
        key_bytes: entries.iter().map(|e| e.key.user_key.len() as u64).sum(),
        value_bytes: entries.iter().map(|e| e.value.len() as u64).sum(),
        filter_hashes: entries.len() as u64,
        longest_key: entries
            .iter()
            .map(|e| e.key.user_key.len() as u64)
            .max()
            .unwrap_or(0),
        longest_value: entries.iter().map(|e| e.value.len() as u64).max(),
        ..RewriteLoad::default()
    }
}

/// Writes `entries` as one table with the writer `configure` sets up and
/// checks the bytes it takes against the bound a writer of the same settings
/// gives before any of them is written.
fn assert_bound_holds(
    name: &str,
    entries: &[InternalValue],
    configure: impl Fn(Writer) -> Writer,
) -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let probe = configure(Writer::new(
        dir.path().join("probe"),
        1,
        0,
        Arc::new(StdFs),
    )?);
    let bound = probe.rewrite_bound(&load_of(entries), 1)?;
    drop(probe);

    let path = dir.path().join("table");
    let mut writer = configure(Writer::new(path.clone(), 2, 0, Arc::new(StdFs))?);
    for entry in entries {
        writer.write(entry.clone())?;
    }
    writer.finish()?;
    let written = std::fs::metadata(&path)?.len();
    assert!(
        written <= bound,
        "{name}: the table takes {written} bytes past the bound of {bound}"
    );
    // A bound far above what is written refuses work that fits: a slice cut
    // to it moves less per pass, and a merge it gates is turned away.
    assert!(
        bound <= 2 * written,
        "{name}: a bound of {bound} for a table of {written} bytes is too loose to use"
    );
    Ok(())
}

/// The bound holds for each setting that changes what a table takes: the
/// codecs, encryption, parity, per-KV checksums, hash index, sections and a
/// partitioned index and filter, alone and together, over values no codec
/// shrinks.
#[test]
fn the_rewrite_bound_holds_for_every_setting() -> crate::Result<()> {
    let small = entries(3_000, 16, 100);
    let large = entries(300, 40, 20_000);
    for (label, rows) in [("small values", &small), ("large values", &large)] {
        assert_bound_holds(&format!("{label}, defaults"), rows, |w| w)?;
        assert_bound_holds(&format!("{label}, small blocks"), rows, |w| {
            w.use_data_block_size(512)
                .use_data_block_restart_interval(1)
        })?;
        #[cfg(feature = "lz4")]
        assert_bound_holds(&format!("{label}, lz4"), rows, |w| {
            w.use_data_block_compression(CompressionType::Lz4)
                .use_index_block_compression(CompressionType::Lz4)
        })?;
        #[cfg(zstd_any)]
        assert_bound_holds(&format!("{label}, zstd"), rows, |w| {
            w.use_data_block_compression(CompressionType::Zstd(3))
                .use_index_block_compression(CompressionType::Zstd(3))
        })?;
        assert_bound_holds(&format!("{label}, hash index"), rows, |w| {
            w.use_data_block_hash_ratio(4.0)
        })?;
        assert_bound_holds(&format!("{label}, kv checksums"), rows, |w| {
            w.use_kv_checksums(
                crate::runtime_config::KvChecksumPolicy::AllLevels,
                crate::runtime_config::ChecksumAlgorithm::Xxh3_64,
            )
        })?;
        assert_bound_holds(&format!("{label}, sections"), rows, |w| {
            w.use_seqno_in_index(true).use_zone_map(true)
        })?;
        assert_bound_holds(&format!("{label}, partitioned"), rows, |w| {
            w.use_data_block_size(512)
                .use_adaptive_index(0)
                .use_index_partition_size(256)
                .use_partitioned_filter()
                .use_filter_partition_size(256)
        })?;
        #[cfg(feature = "encryption")]
        assert_bound_holds(&format!("{label}, encrypted"), rows, |w| {
            w.use_encryption(Some(Arc::new(crate::encryption::Aes256GcmProvider::new(
                &[7u8; 32],
            ))))
        })?;
        #[cfg(feature = "page_ecc")]
        assert_bound_holds(&format!("{label}, parity"), rows, |w| {
            w.use_ecc(Some(crate::table::block::EccParams::RS_4_2))
        })?;
        #[cfg(feature = "columnar")]
        assert_bound_holds(&format!("{label}, columnar"), rows, |w| {
            w.use_columnar(true)
                .use_row_group_size(4_096)
                .use_columnar_page_size(1_024)
                .use_zone_map(true)
        })?;
        #[cfg(all(feature = "lz4", feature = "encryption", feature = "page_ecc"))]
        assert_bound_holds(&format!("{label}, everything"), rows, |w| {
            w.use_data_block_size(512)
                .use_data_block_compression(CompressionType::Lz4)
                .use_index_block_compression(CompressionType::Lz4)
                .use_data_block_hash_ratio(4.0)
                .use_kv_checksums(
                    crate::runtime_config::KvChecksumPolicy::AllLevels,
                    crate::runtime_config::ChecksumAlgorithm::Xxh3_64,
                )
                .use_seqno_in_index(true)
                .use_zone_map(true)
                .use_adaptive_index(0)
                .use_index_partition_size(256)
                .use_partitioned_filter()
                .use_filter_partition_size(256)
                .use_encryption(Some(Arc::new(crate::encryption::Aes256GcmProvider::new(
                    &[7u8; 32],
                ))))
                .use_ecc(Some(crate::table::block::EccParams::RS_4_2))
        })?;
    }
    Ok(())
}
