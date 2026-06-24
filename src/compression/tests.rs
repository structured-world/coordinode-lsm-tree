use super::*;
use test_log::test;

#[test]
fn compression_serialize_none() {
    let serialized = CompressionType::None.encode_into_vec();
    assert_eq!(1, serialized.len());
}

#[cfg(feature = "lz4")]
mod lz4 {
    use super::*;
    use test_log::test;

    #[test]
    fn compression_serialize_lz4() {
        let serialized = CompressionType::Lz4.encode_into_vec();
        assert_eq!(1, serialized.len());
    }
}

#[cfg(zstd_any)]
mod zstd {
    use super::*;
    use test_log::test;

    #[test]
    fn compression_serialize_zstd() {
        let serialized = CompressionType::Zstd(3).encode_into_vec();
        assert_eq!(2, serialized.len());
    }

    #[test]
    fn compression_roundtrip_zstd() {
        for level in [1, 3, 9, 19] {
            let original = CompressionType::Zstd(level);
            let serialized = original.encode_into_vec();
            let decoded =
                CompressionType::decode_from(&mut &serialized[..]).expect("decode failed");
            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn compression_display_zstd() {
        assert_eq!(format!("{}", CompressionType::Zstd(3)), "zstd");
    }

    #[test]
    fn compression_zstd_rejects_out_of_range_level() {
        // Above the zstd max (22), or below what one signed byte can persist
        // (i8::MIN); negative "fast" levels and 0 are valid (see the round-trip
        // test below).
        for invalid_level in [23, 100, 200, -129, i32::MIN] {
            let result = CompressionType::zstd(invalid_level);
            assert!(result.is_err(), "level {invalid_level} should be rejected");
        }
    }

    #[test]
    fn compression_zstd_accepts_negative_and_default_levels() {
        // zstd negative "fast" levels, 0 (= default), and 1..=22 all serialize and
        // round-trip through the single-signed-byte wire format.
        for level in [-128, -22, -1, 0, 1, 3, 22] {
            let original = CompressionType::zstd(level)
                .unwrap_or_else(|_| panic!("level {level} must be accepted"));
            let serialized = original.encode_into_vec();
            let decoded =
                CompressionType::decode_from(&mut &serialized[..]).expect("decode failed");
            assert_eq!(original, decoded, "level {level} must round-trip");
        }
    }

    #[test]
    fn compression_roundtrip_zstd_negative_level() {
        // A negative fast level must produce a valid frame that decompresses back
        // to the original (the level only affects the compressor's strategy, not
        // the self-describing frame format).
        use super::CompressionProvider;
        let data = b"the quick brown fox jumps over the lazy dog".repeat(64);
        let compressed = super::ZstdBackend::compress(&data, -22).expect("compress at -22");
        let back = super::ZstdBackend::decompress(&compressed, data.len()).expect("decompress");
        assert_eq!(back, data, "a negative-level frame round-trips");
    }

    #[test]
    fn compression_zstd_decode_rejects_invalid_level() {
        // Serialize a valid zstd value, then corrupt the level byte to a value
        // above the zstd max (the wire is a signed byte, so only 23..=127 are
        // out of the accepted -128..=22 range).
        let valid = CompressionType::Zstd(3).encode_into_vec();
        assert_eq!(valid.len(), 2);

        for invalid in [23u8, 100] {
            let corrupted = vec![valid[0], invalid];
            let result = CompressionType::decode_from(&mut &corrupted[..]);
            assert!(
                result.is_err(),
                "level {invalid} should be rejected on decode"
            );
        }
    }

    #[test]
    fn compression_serialize_zstd_dict() {
        let serialized = CompressionType::ZstdDict {
            level: 3,
            dict_id: 0xDEAD_BEEF,
        }
        .encode_into_vec();
        // tag=4, level=3 as i8, dict_id=0xDEAD_BEEF in little-endian
        assert_eq!(serialized, [4, 3, 0xEF, 0xBE, 0xAD, 0xDE]);
    }

    #[test]
    fn compression_roundtrip_zstd_dict() {
        for level in [1, 3, 9, 19] {
            for dict_id in [0, 1, 0xDEAD_BEEF, u32::MAX] {
                let original = CompressionType::ZstdDict { level, dict_id };
                let serialized = original.encode_into_vec();
                let decoded =
                    CompressionType::decode_from(&mut &serialized[..]).expect("decode failed");
                assert_eq!(original, decoded);
            }
        }
    }

    #[test]
    fn compression_display_zstd_dict() {
        assert_eq!(
            format!(
                "{}",
                CompressionType::ZstdDict {
                    level: 3,
                    dict_id: 42
                }
            ),
            "zstd+dict"
        );
    }

    #[test]
    fn compression_zstd_dict_rejects_out_of_range_level() {
        // Negative "fast" levels and 0 are valid; only above the zstd max (22) or
        // below the i8 the wire can persist are rejected.
        for invalid_level in [23, 100, 200, -129, i32::MIN] {
            let result = CompressionType::zstd_dict(invalid_level, 42);
            assert!(result.is_err(), "level {invalid_level} should be rejected");
        }
    }

    #[test]
    fn compression_zstd_dict_decode_rejects_invalid_level() {
        // Serialize a valid ZstdDict, then corrupt the level byte above the max.
        let mut buf = CompressionType::ZstdDict {
            level: 3,
            dict_id: 42,
        }
        .encode_into_vec();
        assert_eq!(buf[0], 4); // tag
        buf[1] = 100; // corrupt level to 100 (above the zstd max of 22)

        let result = CompressionType::decode_from(&mut &buf[..]);
        assert!(result.is_err(), "level 100 should be rejected on decode");
    }

    #[test]
    fn zstd_dictionary_id_deterministic() {
        let dict_bytes = b"sample dictionary content for testing";
        let d1 = ZstdDictionary::new(dict_bytes);
        let d2 = ZstdDictionary::new(dict_bytes);
        assert_eq!(d1.id(), d2.id());
    }

    #[test]
    fn zstd_dictionary_different_content_different_id() {
        let d1 = ZstdDictionary::new(b"dictionary one");
        let d2 = ZstdDictionary::new(b"dictionary two");
        assert_ne!(d1.id(), d2.id());
    }

    #[test]
    fn zstd_dictionary_raw_roundtrip() {
        let raw = b"my dictionary bytes";
        let dict = ZstdDictionary::new(raw);
        assert_eq!(dict.raw(), raw);
    }

    #[test]
    fn zstd_dictionary_debug_format() {
        let dict = ZstdDictionary::new(b"test");
        let debug = format!("{dict:?}");
        assert!(debug.contains("ZstdDictionary"));
        assert!(debug.contains("size: 4"));
    }

    // --- prepared_handle: pre-parsed `DictionaryHandle` cache ---
    //
    // The whole point of #232: parse the dictionary ONCE per
    // `ZstdDictionary` instance and reuse the Arc-backed handle on every
    // subsequent decompress call, across all threads. The tests below
    // pin the contract: success / memoization / shared-OnceCell-across-
    // clones / both finalized + raw-content paths / error surfacing.

    #[cfg(feature = "zstd")]
    #[test]
    fn prepared_handle_raw_content_dict_parses_and_memoizes() {
        // Raw-content path: no magic prefix. structured-zstd builds a
        // `Dictionary` from the bytes treated as LZ77 history. First
        // call parses; second call must hit the OnceCell cache and
        // return a handle that compares-equal to the first.
        let dict = ZstdDictionary::new(b"raw-content training bytes here");
        let h1 = dict
            .prepared_handle()
            .expect("first call must parse raw-content dict");
        let h2 = dict
            .prepared_handle()
            .expect("second call must hit the cache");
        assert_eq!(
            h1.id(),
            h2.id(),
            "cached handle must report the same dict id"
        );
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn prepared_handle_rejects_corrupted_finalized_magic() {
        // Bytes that LOOK like a finalized dict (magic prefix matches)
        // but are otherwise malformed must surface a parse error
        // through `prepared_handle` rather than panicking. The OnceCell
        // must NOT be populated with anything on failure — otherwise a
        // future caller would skip the (now-deterministically-failing)
        // parse and silently fall back to a stale cached value, breaking
        // the retry-on-failure contract.
        let mut bad = vec![0x37, 0xA4, 0x30, 0xEC]; // valid magic
        bad.extend_from_slice(&[0xFF; 16]); // garbage payload
        let dict = ZstdDictionary::new(&bad);
        let result = dict.prepared_handle();
        assert!(
            result.is_err(),
            "corrupted finalized dict must surface parse error",
        );
        assert!(
            dict.prepared.get().is_none(),
            "failed parse must NOT populate the OnceCell — retry-on-failure contract",
        );
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn prepared_handle_shared_across_clones() {
        // `ZstdDictionary::clone` shares the inner `Arc<OnceCell<…>>`.
        // Parsing through one clone must be visible to the other —
        // otherwise each clone would re-parse independently, defeating
        // the purpose of the cache when dictionaries are distributed
        // across threads via clones.
        let dict_a = ZstdDictionary::new(b"shared dict bytes for clone test");
        let dict_b = dict_a.clone();

        let _ = dict_a
            .prepared_handle()
            .expect("parse via dict_a must succeed");
        // After dict_a parsed, dict_b's OnceCell (same Arc) must be
        // populated. We cannot directly observe "did not re-parse"
        // without instrumentation, but we can assert the cached
        // handle round-trips through dict_b and reports the same id.
        let h_b = dict_b
            .prepared_handle()
            .expect("dict_b must see cached handle");
        assert_eq!(h_b.id(), dict_a.id());
        // Cross-check OnceCell state directly: it is .get()-readable
        // from both clones.
        assert!(
            dict_b.prepared.get().is_some(),
            "OnceCell must be populated on dict_b after dict_a parsed",
        );
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn prepared_handle_is_lazy_and_populated_after_first_call() {
        // The cache contract is lazy-init: `ZstdDictionary::new` must
        // NOT eagerly parse, and the OnceCell must transition from
        // `None` to `Some(_)` precisely on the first `prepared_handle`
        // call. This pins both halves of the contract — a regression
        // either way (eager parse OR no caching) lights up the assert.
        //
        // The end-to-end "real finalized dict parses successfully" path
        // is exercised by the existing `zstd_backend` round-trip suite
        // (which feeds real compressed frames through `decompress_with_dict`,
        // implicitly going through `prepared_handle`); duplicating the
        // dict-builder here would require linking the zstd dict trainer
        // and adds no coverage over what the backend tests already give.
        let dict = ZstdDictionary::new(b"laziness test bytes");
        assert!(
            dict.prepared.get().is_none(),
            "ZstdDictionary::new must NOT eagerly parse the dictionary",
        );
        let _ = dict.prepared_handle().expect("explicit parse must succeed");
        assert!(
            dict.prepared.get().is_some(),
            "OnceCell must be populated after first prepared_handle call",
        );
    }
}
