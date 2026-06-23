use super::*;

#[test]
fn plain_transform_reports_no_compression_no_encryption_no_ecc() {
    let t = BlockTransform::Plain;
    assert_eq!(t.compression(), CompressionType::None);
    assert!(t.encryption().is_none());
    assert!(!t.page_ecc());
}

#[test]
fn plain_constant_matches_plain_variant() {
    let t = BlockTransform::PLAIN;
    assert!(matches!(t, BlockTransform::Plain));
    assert!(!t.page_ecc());
}

#[cfg(feature = "page_ecc")]
#[test]
fn plain_ecc_variant_reports_ecc_enabled_no_other_transform() {
    let t = BlockTransform::PlainEcc(EccParams::RS_4_2);
    assert_eq!(t.compression(), CompressionType::None);
    assert!(t.encryption().is_none());
    assert!(t.page_ecc());
}

#[cfg(all(feature = "page_ecc", feature = "lz4"))]
#[test]
fn compressed_ecc_carries_compression_kind_and_reports_ecc() {
    let Ok(ctx) = CompressionContext::new(CompressionType::Lz4) else {
        panic!("Lz4 ctx construction is total");
    };
    let t = BlockTransform::CompressedEcc(ctx, EccParams::RS_4_2);
    assert_eq!(t.compression(), CompressionType::Lz4);
    assert!(t.encryption().is_none());
    assert!(t.page_ecc());
}

#[test]
fn eccparams_try_new_rejects_zero_shards() {
    // Zero in either position has no valid parity layout, so the only
    // public constructor must refuse it (the invariant the private
    // fields exist to protect).
    assert!(matches!(
        EccParams::try_new(0, 2),
        Err(crate::Error::FeatureUnsupported(_))
    ));
    assert!(matches!(
        EccParams::try_new(8, 0),
        Err(crate::Error::FeatureUnsupported(_))
    ));
    let ok = EccParams::try_new(8, 2).expect("non-zero shards are accepted");
    assert_eq!((ok.data_shards(), ok.parity_shards()), (8, 2));
    assert_eq!(ok.as_shards(), (8, 2));
}

#[cfg(feature = "page_ecc")]
#[test]
fn with_ecc_upgrades_plain_to_plain_ecc() {
    let p = EccParams::try_new(8, 2).expect("valid shards");
    let t = BlockTransform::Plain.with_ecc(p);
    assert!(matches!(t, BlockTransform::PlainEcc(_)));
    assert_eq!(t.ecc_params(), Some(p));
    assert_eq!(t.compression(), CompressionType::None);
    assert!(t.encryption().is_none());
    // Re-stamping an already-Ecc variant replaces the params.
    let p2 = EccParams::try_new(4, 2).expect("valid shards");
    assert_eq!(t.with_ecc(p2).ecc_params(), Some(p2));
}

#[cfg(all(feature = "page_ecc", feature = "encryption"))]
#[test]
fn with_ecc_upgrades_encrypted_variants() {
    let p = EccParams::try_new(8, 2).expect("valid shards");
    let enc = crate::encryption::Aes256GcmProvider::new(&[0x11; 32]);

    let t = BlockTransform::Encrypted(&enc).with_ecc(p);
    assert!(matches!(t, BlockTransform::EncryptedEcc(_, _)));
    assert_eq!(t.ecc_params(), Some(p));
    assert!(t.encryption().is_some());
    assert_eq!(t.compression(), CompressionType::None);

    #[cfg(feature = "lz4")]
    {
        let ctx = CompressionContext::new(CompressionType::Lz4).expect("lz4 ctx");
        let t = BlockTransform::CompressedAndEncrypted(ctx, &enc).with_ecc(p);
        assert!(matches!(
            t,
            BlockTransform::CompressedAndEncryptedEcc(_, _, _)
        ));
        assert_eq!(t.ecc_params(), Some(p));
        assert!(t.encryption().is_some());
        assert_eq!(t.compression(), CompressionType::Lz4);
    }
}
