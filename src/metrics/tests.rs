use super::*;
use core::sync::atomic::Ordering::Relaxed;

#[test]
fn range_tombstone_counters_default_zero() {
    let m = Metrics::default();
    assert_eq!(0, m.range_tombstone_block_load_count());
    assert_eq!(0, m.range_tombstone_block_load_cached_count());
    assert_eq!(0, m.range_tombstone_block_io());
}

#[test]
fn record_ecc_recovery_attributes_each_kind_to_its_own_counter() {
    use crate::table::block::EccRecoveryKind;
    let m = Metrics::default();
    assert_eq!(0, m.ecc_recovered_count(), "counters start at zero");

    m.record_ecc_recovery(EccRecoveryKind::Secded);
    m.record_ecc_recovery(EccRecoveryKind::Secded);
    m.record_ecc_recovery(EccRecoveryKind::Shard);

    assert_eq!(2, m.ecc_secded_corrected_count(), "two SEC-DED heals");
    assert_eq!(1, m.ecc_shard_recovered_count(), "one RS shard recovery");
    // The two mechanisms are disjoint and sum to the total.
    assert_eq!(3, m.ecc_recovered_count(), "total across both mechanisms");
}

#[test]
fn range_tombstone_block_load_count_sums_cached_and_io() {
    let m = Metrics::default();
    m.range_tombstone_block_load_cached.store(3, Relaxed);
    m.range_tombstone_block_load_io.store(7, Relaxed);
    assert_eq!(10, m.range_tombstone_block_load_count());
}

#[test]
fn range_tombstone_cache_hit_rate_no_loads_returns_one() {
    let m = Metrics::default();
    assert!((m.range_tombstone_block_cache_hit_rate() - 1.0).abs() < f64::EPSILON);
}

#[test]
fn range_tombstone_cache_hit_rate_mixed_loads() {
    let m = Metrics::default();
    m.range_tombstone_block_load_cached.store(3, Relaxed);
    m.range_tombstone_block_load_io.store(1, Relaxed);
    assert!((m.range_tombstone_block_cache_hit_rate() - 0.75).abs() < f64::EPSILON);
}

#[test]
fn zero_query_cache_hit_rates_return_one() {
    let m = Metrics::default();
    assert!((m.data_block_cache_hit_rate() - 1.0).abs() < f64::EPSILON);
    assert!((m.filter_block_cache_hit_rate() - 1.0).abs() < f64::EPSILON);
    assert!((m.index_block_cache_hit_rate() - 1.0).abs() < f64::EPSILON);
}

#[test]
fn block_io_includes_range_tombstone() {
    let m = Metrics::default();
    m.data_block_io_requested.store(10, Relaxed);
    m.index_block_io_requested.store(20, Relaxed);
    m.filter_block_io_requested.store(30, Relaxed);
    m.range_tombstone_block_io_requested.store(40, Relaxed);
    assert_eq!(100, m.block_io());
}

#[test]
fn block_load_io_count_includes_range_tombstone() {
    let m = Metrics::default();
    m.data_block_load_io.store(1, Relaxed);
    m.index_block_load_io.store(2, Relaxed);
    m.filter_block_load_io.store(3, Relaxed);
    m.range_tombstone_block_load_io.store(4, Relaxed);
    assert_eq!(10, m.block_load_io_count());
}

#[test]
fn block_load_cached_count_includes_range_tombstone() {
    let m = Metrics::default();
    m.data_block_load_cached.store(5, Relaxed);
    m.index_block_load_cached.store(6, Relaxed);
    m.filter_block_load_cached.store(7, Relaxed);
    m.range_tombstone_block_load_cached.store(8, Relaxed);
    assert_eq!(26, m.block_load_cached_count());
}
