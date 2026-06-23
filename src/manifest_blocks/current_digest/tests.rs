use super::*;
use crate::manifest_blocks::FLAG_FOOTER_MIRROR_ENABLED;

fn entry(name: &str, offset: u64, size: u32, checksum: u128) -> TocEntry {
    TocEntry {
        name: name.to_string(),
        block_offset: offset,
        block_size: size,
        section_checksum: checksum,
    }
}

#[test]
fn digest_is_deterministic() {
    // Same inputs → same hash. Foundation property — without
    // this nothing else in the layer works.
    let payload = FooterPayload::new(
        FLAG_FOOTER_MIRROR_ENABLED,
        vec![entry("tables", 4096, 128, 0xAA)],
    );
    let h1 = compute(7, &payload).unwrap();
    let h2 = compute(7, &payload).unwrap();
    assert_eq!(h1, h2);
}

#[test]
fn digest_differs_for_different_version_id() {
    // T1 mislinking detection: same TOC, different `version_id`
    // → distinct digests. Required so a CURRENT pointing at
    // `v{N}` can't be mistaken for one pointing at `v{M}`
    // even when the underlying manifests have identical TOCs.
    let payload = FooterPayload::new(0, vec![entry("a", 4096, 64, 0xAA)]);
    assert_ne!(compute(0, &payload).unwrap(), compute(1, &payload).unwrap());
}

#[test]
fn digest_differs_when_section_checksum_changes() {
    // Per-section content binding: flipping one section's
    // checksum (which mirrors the section Block's own XXH3-128)
    // must change the CURRENT digest. This is the chain that
    // binds section content into CURRENT without the CURRENT
    // layer having to hash raw section bytes.
    let p1 = FooterPayload::new(0, vec![entry("tables", 4096, 64, 0xAA)]);
    let p2 = FooterPayload::new(0, vec![entry("tables", 4096, 64, 0xBB)]);
    assert_ne!(compute(0, &p1).unwrap(), compute(0, &p2).unwrap());
}

#[test]
fn digest_is_order_independent() {
    // TOC sorting in the canonical form means two manifests
    // that differ only in the encoded order of their sections
    // produce the same CURRENT digest. Reading order does
    // matter for byte-level layout (offsets differ), but those
    // differences ARE captured in the per-entry offset field
    // — so this property is really "no spurious mismatch from
    // re-ordering a TOC that has the same content".
    let common = vec![entry("a", 4096, 64, 0xAA), entry("b", 4160, 64, 0xBB)];
    let reordered = vec![entry("b", 4160, 64, 0xBB), entry("a", 4096, 64, 0xAA)];
    let p1 = FooterPayload::new(0, common);
    let p2 = FooterPayload::new(0, reordered);
    assert_eq!(compute(0, &p1).unwrap(), compute(0, &p2).unwrap());
}

#[test]
fn digest_differs_when_section_offset_changes() {
    // Offset / size are part of the canonical form (they bind
    // the on-disk layout into CURRENT). Two TOCs with same
    // name + checksum but different offsets must hash to
    // distinct digests — a file whose sections moved is a
    // different file at the CURRENT-pointer layer.
    let p1 = FooterPayload::new(0, vec![entry("tables", 4096, 64, 0xAA)]);
    let p2 = FooterPayload::new(0, vec![entry("tables", 8192, 64, 0xAA)]);
    assert_ne!(compute(0, &p1).unwrap(), compute(0, &p2).unwrap());
}

#[test]
fn digest_differs_when_layout_version_changes() {
    // layout_version bump = format-incompatible manifest. The
    // digest must surface that as a mismatch even if everything
    // else is identical, so a reader can't silently accept a
    // manifest written under a different layout convention.
    let mut p1 = FooterPayload::new(0, vec![entry("a", 4096, 64, 0xAA)]);
    let mut p2 = p1.clone();
    p1.layout_version = 2;
    p2.layout_version = 3;
    assert_ne!(compute(0, &p1).unwrap(), compute(0, &p2).unwrap());
}
