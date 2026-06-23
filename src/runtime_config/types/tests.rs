use super::*;

#[test]
fn ecc_defaults_are_off_secded_block() {
    // ECC master default is OFF (page_ecc=false); when enabled without
    // an explicit scheme the on-default is the cheapest tier (Secded),
    // and the granularity default is Block. Pinning these guards the
    // efficiency-first contract (never RS(4,2)/+50% by default).
    let c = RuntimeConfig::default();
    assert!(!c.page_ecc);
    assert_eq!(c.ecc_scheme, EccScheme::Secded);
    assert_eq!(c.ecc_granularity, EccGranularity::Block);
}

#[test]
fn ecc_scheme_shard_params_match_scheme() {
    // Secded is per-word, not shard-based. Xor is single-parity.
    assert_eq!(EccScheme::Secded.shard_params(), None);
    assert_eq!(
        EccScheme::Xor { data_shards: 10 }.shard_params(),
        Some((10, 1))
    );
    assert_eq!(
        EccScheme::ReedSolomon {
            data_shards: 8,
            parity_shards: 2,
        }
        .shard_params(),
        Some((8, 2)),
    );
}

#[test]
fn ecc_descriptor_roundtrips_off_and_every_scheme() {
    // The per-SST descriptor codec is a faithful round-trip serializer:
    // "off" and every recognized scheme + granularity decode back to the
    // exact value written. (Whether a recognized scheme is *applicable*
    // for recovery is decided at the read layer, not here.)
    assert_eq!(ecc_descriptor_bytes(None), [0, 0, 0, 0]);
    assert_eq!(
        ecc_descriptor_from_bytes(&[0, 0, 0, 0]).expect("decode"),
        EccDescriptor::Off,
    );
    let cases = [
        (EccScheme::Secded, EccGranularity::Block),
        (EccScheme::Secded, EccGranularity::Page),
        (EccScheme::Xor { data_shards: 10 }, EccGranularity::Block),
        (
            EccScheme::ReedSolomon {
                data_shards: 8,
                parity_shards: 2,
            },
            EccGranularity::Page,
        ),
    ];
    for (scheme, gran) in cases {
        let bytes = ecc_descriptor_bytes(Some((scheme, gran)));
        assert_eq!(
            ecc_descriptor_from_bytes(&bytes).expect("decode"),
            EccDescriptor::Recognized(scheme, gran),
            "roundtrip {scheme:?}/{gran:?}",
        );
    }
}

#[test]
fn ecc_descriptor_wrong_length_is_error() {
    // The only hard error: a value that is not a 4-byte descriptor at
    // all. Any 4-byte value decodes (possibly to `Unrecognized`).
    assert!(ecc_descriptor_from_bytes(&[0, 0, 0]).is_err()); // too short
    assert!(ecc_descriptor_from_bytes(&[0, 0, 0, 0, 0]).is_err()); // too long
}

#[test]
fn ecc_descriptor_unparseable_layouts_decode_as_unrecognized() {
    // Unknown kind / granularity, non-canonical reserved bytes, and
    // non-canonical shard layouts are NOT hard errors: they decode to
    // `Unrecognized` so the read path can warn (and recommend
    // recompaction) instead of failing the read.
    for bytes in [
        [9, 0, 0, 0], // unknown kind
        [1, 0, 0, 7], // unknown granularity
        [2, 0, 1, 0], // Xor data_shards = 0
        [2, 8, 2, 0], // Xor parity byte != 1 (non-canonical)
        [3, 8, 0, 0], // RS parity_shards = 0
        [3, 8, 1, 0], // RS parity_shards = 1 (should be Xor)
        [0, 8, 2, 1], // Off with non-canonical reserved bytes
        [0, 0, 0, 1],
        [1, 8, 2, 0], // Secded with non-canonical reserved shard bytes
    ] {
        assert_eq!(
            ecc_descriptor_from_bytes(&bytes).expect("4 bytes always decode"),
            EccDescriptor::Unrecognized,
            "{bytes:?} must decode as Unrecognized",
        );
    }
    // Canonical encodings still decode to their recognized form.
    assert_eq!(
        ecc_descriptor_from_bytes(&[0, 0, 0, 0]).expect("off"),
        EccDescriptor::Off,
    );
    assert_eq!(
        ecc_descriptor_from_bytes(&[1, 0, 0, 0]).expect("secded"),
        EccDescriptor::Recognized(EccScheme::Secded, EccGranularity::Block),
    );
}

#[test]
fn kv_checksum_policy_default_is_off() {
    // Off is the zero-overhead default: a tree that never opts in
    // produces plain data blocks (KV_CHECKSUM_FOOTER bit clear) and
    // pays no per-entry cost. A regression flipping this default would
    // silently change the on-disk format for every existing user.
    assert_eq!(KvChecksumPolicy::default(), KvChecksumPolicy::Off);
}

#[test]
fn kv_checksum_compute_point_default_is_at_block_compile() {
    // AtBlockCompile is the zero-memtable-overhead default. Flipping
    // it to AtInsert would change the memtable hot path for everyone,
    // so the default is pinned.
    assert_eq!(
        KvChecksumComputePoint::default(),
        KvChecksumComputePoint::AtBlockCompile
    );
    assert_eq!(
        RuntimeConfig::default().kv_checksum_compute_point,
        KvChecksumComputePoint::AtBlockCompile
    );
}

#[test]
fn kv_checksum_policy_off_never_applies() {
    // Off must reject every (level, table) pair — no per-KV footer
    // is ever emitted under the default policy.
    let p = KvChecksumPolicy::Off;
    assert!(!p.applies(0, 0));
    assert!(!p.applies(7, u64::MAX));
}

#[test]
fn kv_checksum_policy_all_levels_always_applies() {
    // AllLevels must select every (level, table) pair, including
    // out-of-mask-range levels (>= 8) that PerLevel can't reach.
    let p = KvChecksumPolicy::AllLevels;
    assert!(p.applies(0, 0));
    assert!(p.applies(9, 12345));
}

#[test]
fn kv_checksum_policy_per_level_gates_on_mask() {
    // PerLevel applies only to levels whose bit is set. Hot-tier
    // selection (L0 + L1) must include 0 and 1 and exclude the
    // rest, regardless of table id.
    let mask = LevelMask::none().with_level(0).with_level(1);
    let p = KvChecksumPolicy::PerLevel(mask);
    assert!(p.applies(0, 999));
    assert!(p.applies(1, 999));
    assert!(!p.applies(2, 999));
    assert!(!p.applies(6, 999));
}

#[test]
fn level_mask_out_of_range_level_is_never_selected() {
    // A u8 mask covers levels 0..=7. with_level on an out-of-range
    // level must be a no-op (not wrap into bit 0 via shift overflow)
    // and contains must report false for those levels.
    let mask = LevelMask::none().with_level(8).with_level(255);
    assert_eq!(mask.bits(), 0, "out-of-range levels must not set any bit");
    assert!(!mask.contains(8));
    assert!(!mask.contains(255));
}

#[test]
fn level_mask_bits_roundtrip() {
    // Raw-bits constructor and accessor must round-trip so a
    // persisted mask byte reconstructs the same selection.
    let mask = LevelMask::none().with_level(0).with_level(3);
    assert_eq!(mask.bits(), 0b0000_1001);
    assert_eq!(LevelMask::from_bits(0b0000_1001), mask);
}

#[test]
fn kv_checksum_policy_per_table_gates_on_inclusive_range() {
    // PerTable applies only inside the inclusive [start, end] span,
    // independent of level. Both endpoints are members; one past
    // each end is not.
    let p = KvChecksumPolicy::PerTable(TableIdRange::new(10, 20));
    assert!(p.applies(0, 10));
    assert!(p.applies(5, 20));
    assert!(!p.applies(0, 9));
    assert!(!p.applies(0, 21));
}

#[test]
fn table_id_range_inverted_selects_nothing() {
    // A range with start > end is empty rather than panicking, so a
    // misconfigured range degrades to "no per-KV checksums" instead
    // of a crash or an all-match.
    let r = TableIdRange::new(20, 10);
    assert!(!r.contains(10));
    assert!(!r.contains(15));
    assert!(!r.contains(20));
}

#[test]
fn runtime_config_default_kv_checksums_off() {
    // The wired RuntimeConfig field must default Off so the struct
    // default stays wire-compatible with pre-per-KV trees.
    assert_eq!(RuntimeConfig::default().kv_checksums, KvChecksumPolicy::Off);
}

#[test]
fn checksum_algorithm_default_is_xxh3_64() {
    // Default chosen for speed on modern SIMD hardware and
    // codebase consistency (every other hash site uses XXH3).
    // Locked here so a regression switching the default to
    // something slower (e.g. CRC32C) lights up the test.
    assert_eq!(ChecksumAlgorithm::default(), ChecksumAlgorithm::Xxh3_64);
}

#[test]
fn checksum_algorithm_compute_xxh3_64_matches_canonical_hash() {
    // Xxh3_64 must produce exactly the crate's canonical hash64 so a
    // per-KV digest is byte-identical to every other XXH3 site (the
    // reader recomputes with hash64 on verify).
    let data = b"per-kv checksum payload bytes";
    assert_eq!(
        ChecksumAlgorithm::Xxh3_64.compute(data),
        Some(crate::hash::hash64(data))
    );
}

#[test]
fn checksum_algorithm_compute_xxh3low32_is_low_32_bits() {
    // Xxh3Low32 is the low 32 bits of the same digest: same compute,
    // half the stored width. The high bits must be zero so the
    // stored 4 bytes round-trip without truncation surprises.
    let data = b"per-kv checksum payload bytes";
    let full = crate::hash::hash64(data);
    let got = ChecksumAlgorithm::Xxh3Low32
        .compute(data)
        .expect("Xxh3Low32 is always available");
    assert_eq!(got, full & 0xFFFF_FFFF);
    assert_eq!(got >> 32, 0, "high 32 bits must be clear");
}

#[test]
#[cfg(feature = "crc32c")]
fn checksum_algorithm_compute_crc32c_when_feature_on() {
    // With the crc32c feature, Crc32c computes a non-trivial digest
    // that fits in 32 bits and is order-sensitive (a real checksum,
    // not a stub returning a constant).
    let a = ChecksumAlgorithm::Crc32c
        .compute(b"abc")
        .expect("crc32c feature enabled");
    let b = ChecksumAlgorithm::Crc32c
        .compute(b"acb")
        .expect("crc32c feature enabled");
    assert_eq!(a >> 32, 0, "CRC32C digest fits in 32 bits");
    assert_ne!(a, b, "CRC32C must be order-sensitive");
}

#[test]
#[cfg(not(feature = "crc32c"))]
fn checksum_algorithm_compute_crc32c_none_when_feature_off() {
    // Without the feature, selecting Crc32c must surface as None so a
    // caller translates it into a typed "not compiled in" error
    // rather than silently substituting another algorithm.
    assert_eq!(ChecksumAlgorithm::Crc32c.compute(b"abc"), None);
}

#[test]
fn checksum_algorithm_compute_chunks_matches_one_shot() {
    // compute_chunks is the per-KV digest hot path; it MUST produce the
    // identical digest to a one-shot compute over the concatenation, or
    // on-disk per-KV digests would silently change. A multi-chunk split
    // with an empty chunk exercises the streaming boundary handling and
    // guards against a chunk-ordering or 32-bit-truncation regression.
    let chunks: &[&[u8]] = &[b"alpha", b"", b"-bravo-", b"charlie"];
    let mut concat = Vec::new();
    for c in chunks {
        concat.extend_from_slice(c);
    }

    for algo in [ChecksumAlgorithm::Xxh3_64, ChecksumAlgorithm::Xxh3Low32] {
        assert_eq!(
            algo.compute_chunks(chunks),
            algo.compute(&concat),
            "{algo:?}: streamed digest must equal one-shot over the concat",
        );
    }

    #[cfg(feature = "crc32c")]
    assert_eq!(
        ChecksumAlgorithm::Crc32c.compute_chunks(chunks),
        ChecksumAlgorithm::Crc32c.compute(&concat),
        "Crc32c: streamed digest must equal one-shot over the concat",
    );
    #[cfg(not(feature = "crc32c"))]
    assert_eq!(ChecksumAlgorithm::Crc32c.compute_chunks(chunks), None);
}

#[test]
fn checksum_algorithm_digest_sizes_match_spec() {
    // Per design (#298 Q5): Xxh3_64 stores 8 bytes; Xxh3Low32
    // and Crc32c store 4 bytes. Wire format depends on this
    // — wrong value would silently mis-frame downstream blocks.
    assert_eq!(ChecksumAlgorithm::Xxh3_64.digest_size(), 8);
    assert_eq!(ChecksumAlgorithm::Xxh3Low32.digest_size(), 4);
    assert_eq!(ChecksumAlgorithm::Crc32c.digest_size(), 4);
}

#[test]
fn checksum_algorithm_wire_tag_roundtrip() {
    // Every variant must roundtrip through its on-disk
    // discriminator. If we ever add a new variant without
    // wiring it into both directions, this catches the gap
    // before it ships as a corrupt-looking block on disk.
    for algo in [
        ChecksumAlgorithm::Xxh3_64,
        ChecksumAlgorithm::Xxh3Low32,
        ChecksumAlgorithm::Crc32c,
    ] {
        let tag = algo.wire_tag();
        assert_eq!(ChecksumAlgorithm::from_wire_tag(tag), Some(algo));
    }
}

#[test]
fn checksum_algorithm_wire_tag_rejects_unknown() {
    // Forward-incompatible blocks (newer writer, older reader)
    // must surface as a parse failure rather than be silently
    // misinterpreted as a known algorithm.
    assert_eq!(ChecksumAlgorithm::from_wire_tag(255), None);
    assert_eq!(ChecksumAlgorithm::from_wire_tag(3), None);
}

#[test]
fn runtime_config_default_uses_xxh3_64_everywhere() {
    // Default RuntimeConfig must match `RocksDB`-like baseline
    // for benchmark symmetry (#353): block-level checksum on,
    // no per-KV machinery configured yet (downstream PRs add
    // policy fields). Both algo slots default to Xxh3_64.
    let cfg = RuntimeConfig::default();
    assert_eq!(cfg.block_checksum_algo, ChecksumAlgorithm::Xxh3_64);
    assert_eq!(cfg.kv_checksum_algo, ChecksumAlgorithm::Xxh3_64);
}

#[test]
fn runtime_config_default_manifest_safety_on() {
    // Per V5-2 Q-decisions: manifest footer mirror and per-KV
    // checksums default ON. Footer mirror gives partial-write
    // recovery for ~4 KiB cost; per-KV checksums match `RocksDB`
    // MANIFEST per-record CRC granularity so apples-to-apples
    // benchmarks aren't paying for an opt-in we don't ship.
    // Both defaults are load-bearing — flipping them silently
    // would regress durability for new users.
    let cfg = RuntimeConfig::default();
    assert!(cfg.manifest_footer_mirror);
    assert!(cfg.manifest_kv_checksums);
}

#[test]
fn runtime_config_default_page_ecc_off_with_no_overrides() {
    // ECC is explicit opt-in per Q3: zero cost unless enabled.
    // Per-scope overrides default to None (inherit global).
    let cfg = RuntimeConfig::default();
    assert!(!cfg.page_ecc);
    assert_eq!(cfg.data_block_ecc_override, None);
    assert_eq!(cfg.kv_checksums_ecc_override, None);
}

#[test]
fn runtime_config_fs_aware_defaults_on() {
    // FS-aware optimizations default ON: out of the box we clear Btrfs `CoW`
    // on write-once SSTs (~20% throughput) and reflink checkpoints. Both
    // are no-ops on filesystems that don't support them, so the default is
    // safe everywhere. A regression flipping either to false would silently
    // drop the optimization for every new tree (and, per #353 Benchmark
    // Symmetry, the RocksDbParity preset relies on being able to turn these
    // OFF to match RocksDB's lack of FS-aware behaviour).
    let c = RuntimeConfig::default();
    assert!(c.disable_cow_on_sst_files);
    assert!(c.use_reflink_for_checkpoint);
}

#[test]
fn runtime_config_default_seqno_in_index_off() {
    // seqno_in_index is explicit opt-in (#224): default false emits no
    // seqno_bounds section, so SSTs carry zero extra bytes. A regression
    // flipping this default would add the section to every new tree.
    assert!(!RuntimeConfig::default().seqno_in_index);
}

#[test]
fn runtime_config_default_index_partition_spill_threshold_is_4mib() {
    // Default keeps SSTs up to a few-hundred-MB single-level (fast point
    // reads; single-level beats two-level at every measured size up to
    // 1M keys) while genuinely huge indexes still partition. On cold
    // levels the single-level block is cache-managed (evictable), so the
    // raised threshold does not pin unbounded index RAM. A regression
    // here would change the index layout — and thus point-read cost — of
    // newly written SSTs.
    assert_eq!(
        RuntimeConfig::default().index_partition_spill_threshold,
        4 * 1024 * 1024,
    );
}

#[test]
fn runtime_config_ecc_helpers_inherit_global_when_no_override() {
    // Helper methods are the call-site API for "should I emit
    // ECC parity here". With no override, every scope tracks
    // the global page_ecc flag.
    let on = RuntimeConfig {
        page_ecc: true,
        ..RuntimeConfig::default()
    };
    assert!(on.data_block_ecc());
    assert!(on.kv_checksums_ecc());
    assert!(on.manifest_ecc());

    let off = RuntimeConfig::default();
    assert!(!off.data_block_ecc());
    assert!(!off.kv_checksums_ecc());
    assert!(!off.manifest_ecc());
}

#[test]
fn runtime_config_ecc_overrides_take_precedence_over_global() {
    // Per Q3 refinement: per-scope override beats global. This
    // is what enables "manifest-only ECC" (global ON, data
    // override Some(false)) and "data without kv-checksum-region
    // ECC" (global ON, kv override Some(false)). Manifest has no
    // override knob — that's also locked in here.
    let suppressed = RuntimeConfig {
        page_ecc: true,
        data_block_ecc_override: Some(false),
        kv_checksums_ecc_override: Some(false),
        ..RuntimeConfig::default()
    };
    assert!(!suppressed.data_block_ecc());
    assert!(!suppressed.kv_checksums_ecc());
    // Manifest ignores per-scope overrides — always tracks global.
    assert!(suppressed.manifest_ecc());

    let forced = RuntimeConfig {
        page_ecc: false,
        data_block_ecc_override: Some(true),
        kv_checksums_ecc_override: Some(true),
        ..RuntimeConfig::default()
    };
    assert!(forced.data_block_ecc());
    assert!(forced.kv_checksums_ecc());
    // Same for the inverse: manifest stays at global, which is
    // off here, even when data + kv overrides force ECC on.
    assert!(!forced.manifest_ecc());
}
