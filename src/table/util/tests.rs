use super::*;
use crate::comparator::DefaultUserComparator;
use test_log::test;

#[test]
fn test_longest_shared_prefix_length() {
    assert_eq!(3, longest_shared_prefix_length(b"abc", b"abc"));
    assert_eq!(1, longest_shared_prefix_length(b"abc", b"a"));
    assert_eq!(1, longest_shared_prefix_length(b"a", b"abc"));
    assert_eq!(0, longest_shared_prefix_length(b"abc", b""));
    assert_eq!(0, longest_shared_prefix_length(b"", b"abc"));
    assert_eq!(0, longest_shared_prefix_length(b"", b""));
    assert_eq!(0, longest_shared_prefix_length(b"", b""));
    assert_eq!(0, longest_shared_prefix_length(b"abc", b"def"));
    assert_eq!(1, longest_shared_prefix_length(b"abc", b"acc"));
}

/// Reference implementation used by cross-impl equality tests.
/// Identical to the pre-SIMD byte-by-byte version so the SIMD/scalar
/// kernels must agree with it on every input.
fn lsp_reference(s1: &[u8], s2: &[u8]) -> usize {
    s1.iter().zip(s2.iter()).take_while(|(a, b)| a == b).count()
}

#[test]
fn lsp_scalar_on_boundaries_matches_reference() {
    // 0 / 7 / 8 / 9 / 15 / 16 / 17 / 31 / 32 / 33 / 63 / 64 / 127 / 128 — covers all
    // word-stride and AVX2-stride boundary cases.
    for total_len in [
        0_usize, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 127, 128,
    ] {
        for mismatch_at in 0..=total_len {
            let mut a = vec![0xAA; total_len];
            let mut b = a.clone();
            if mismatch_at < total_len {
                #[expect(
                    clippy::expect_used,
                    reason = "test: mismatch_at < total_len = b.len() guarantees in-bounds"
                )]
                {
                    *b.get_mut(mismatch_at).expect("in bounds") ^= 0xFF;
                }
            }
            let got = lsp_scalar(&a, &b);
            let want = lsp_reference(&a, &b);
            assert_eq!(
                want, got,
                "scalar @ len={total_len} mismatch_at={mismatch_at}"
            );

            // Also test asymmetric lengths.
            a.truncate(mismatch_at);
            let got_short = lsp_scalar(&a, &b);
            let want_short = lsp_reference(&a, &b);
            assert_eq!(
                want_short, got_short,
                "scalar asym len={mismatch_at} vs {total_len}"
            );
        }
    }
}

#[test]
fn longest_shared_prefix_length_on_boundaries_matches_reference() {
    // Same coverage as `lsp_scalar_on_boundaries_matches_reference`, but exercises
    // the *dispatched* path — on x86_64 this hits AVX2 when available and SSE2
    // otherwise, on little-endian aarch64 it hits NEON, and on other targets it
    // falls back to the scalar kernel.
    for total_len in [
        0_usize, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 127, 128, 255, 256,
    ] {
        for mismatch_at in 0..=total_len {
            let mut a = vec![0xAA; total_len];
            let mut b = a.clone();
            if mismatch_at < total_len {
                #[expect(
                    clippy::expect_used,
                    reason = "test: mismatch_at < total_len = b.len() guarantees in-bounds"
                )]
                {
                    *b.get_mut(mismatch_at).expect("in bounds") ^= 0xFF;
                }
            }
            let got = longest_shared_prefix_length(&a, &b);
            let want = lsp_reference(&a, &b);
            assert_eq!(
                want, got,
                "dispatch @ len={total_len} mismatch_at={mismatch_at}"
            );

            // Asymmetric lengths — truncate `a` to mismatch_at, leaving `b` at full length.
            a.truncate(mismatch_at);
            let got_short = longest_shared_prefix_length(&a, &b);
            let want_short = lsp_reference(&a, &b);
            assert_eq!(
                want_short, got_short,
                "dispatch asym len={mismatch_at} vs {total_len}"
            );
        }
    }
}

/// SIMD kernels are sensitive to inputs that yield all-equal or all-different
/// 32-byte / 16-byte lanes — both extremes must round-trip to the reference impl.
/// Also covers asymmetric "one empty" pairs across all kernels.
#[test]
fn lsp_extreme_byte_patterns_match_reference() {
    for &(label, byte_a, byte_b) in &[
        ("all_zero_equal", 0x00_u8, 0x00_u8),
        ("all_ff_equal", 0xFF, 0xFF),
        ("zero_vs_ff", 0x00, 0xFF),
        ("alternating_match", 0x55, 0x55),
    ] {
        for len in [0_usize, 1, 8, 15, 16, 31, 32, 33, 63, 64, 128, 1023] {
            let a = vec![byte_a; len];
            let b = vec![byte_b; len];
            let want = lsp_reference(&a, &b);
            assert_eq!(want, lsp_scalar(&a, &b), "scalar {label} len={len}");
            assert_eq!(
                want,
                longest_shared_prefix_length(&a, &b),
                "dispatch {label} len={len}"
            );
        }
    }

    // One-empty pairs — important boundary because every SIMD kernel's main
    // loop is skipped (min_len == 0).
    for len in [0_usize, 1, 8, 32, 128, 1024] {
        let nonempty = vec![0x42_u8; len];
        assert_eq!(0, lsp_scalar(&nonempty, &[]));
        assert_eq!(0, lsp_scalar(&[], &nonempty));
        assert_eq!(0, longest_shared_prefix_length(&nonempty, &[]));
        assert_eq!(0, longest_shared_prefix_length(&[], &nonempty));
    }
}

/// Shared boundary-walk used by every per-kernel test: for each `total_len`
/// in the SIMD-stride-boundary set, flip the byte at `mismatch_at` (or none,
/// at `mismatch_at == total_len`) and assert the kernel under test agrees
/// with the byte-by-byte reference, both at full and asymmetric lengths.
//
// cfg-gated to mirror the union of its callers (SSE2/AVX2/AVX-512 on x86_64
// and 32-bit x86, NEON on LE aarch64). On other targets (riscv64, powerpc64,
// BE aarch64) no per-kernel test fires, so the helper would trip dead_code
// without the cfg.
#[cfg(any(
    target_arch = "x86_64",
    target_arch = "x86",
    all(target_arch = "aarch64", target_endian = "little")
))]
fn assert_kernel_matches_reference<F: Fn(&[u8], &[u8]) -> usize>(label: &str, kernel: F) {
    for total_len in [
        0_usize, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 127, 128, 255, 256,
    ] {
        for mismatch_at in 0..=total_len {
            let mut a = vec![0xAA; total_len];
            let mut b = a.clone();
            if mismatch_at < total_len {
                #[expect(
                    clippy::expect_used,
                    reason = "test: mismatch_at < total_len = b.len() guarantees in-bounds"
                )]
                {
                    *b.get_mut(mismatch_at).expect("in bounds") ^= 0xFF;
                }
            }
            let want = lsp_reference(&a, &b);
            assert_eq!(
                want,
                kernel(&a, &b),
                "{label} @ len={total_len} mismatch_at={mismatch_at}"
            );

            // Asymmetric: truncate `a` to `mismatch_at`, leave `b` at full length.
            a.truncate(mismatch_at);
            let want_short = lsp_reference(&a, &b);
            assert_eq!(
                want_short,
                kernel(&a, &b),
                "{label} asym len={mismatch_at} vs {total_len}"
            );
        }
    }
}

/// Direct test for the SSE2 16-byte kernel. Required because CI `x86_64` runners
/// have AVX2 → the dispatch path never hits `lsp_sse2` and coverage would be
/// 0% even though all dispatched tests pass. Calling the kernel directly
/// guarantees the SSE2-path exercise the boundary cases.
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[test]
fn lsp_sse2_on_boundaries_matches_reference() {
    // On x86_64 SSE2 is the mandatory ISA baseline; on 32-bit x86 it is not
    // guaranteed (pre-Pentium-4 lacks it), so runtime-detect before calling.
    #[cfg(target_arch = "x86")]
    if !std::is_x86_feature_detected!("sse2") {
        return;
    }
    // SAFETY: SSE2 is the x86_64 baseline / runtime-verified on x86 above.
    assert_kernel_matches_reference("sse2", |a, b| unsafe { lsp_sse2(a, b) });
}

/// Direct test for the AVX2 32-byte kernel, gated by runtime CPU detection.
/// Without this, AVX2 lines are only exercised via the dispatched path, which
/// is fine for coverage on AVX2-capable runners — but the direct test makes
/// the AVX2 contract explicit (matches reference on every boundary case).
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[test]
fn lsp_avx2_on_boundaries_matches_reference() {
    if !std::is_x86_feature_detected!("avx2") {
        // No AVX2 on this host — nothing to verify directly.
        return;
    }
    // SAFETY: AVX2 availability just verified via runtime CPU feature detection.
    assert_kernel_matches_reference("avx2", |a, b| unsafe { lsp_avx2(a, b) });
}

/// Direct test for the AVX-512BW 64-byte kernel, gated by runtime CPU detection.
/// CI `x86_64` runners may or may not expose AVX-512 (consumer Intel 11th gen+
/// dropped it; AMD Zen4+ and Intel server keep it), so the dispatched path can't
/// be relied on to reach `lsp_avx512`. This direct test exercises it whenever the
/// host supports AVX-512BW and is a no-op otherwise.
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[test]
fn lsp_avx512_on_boundaries_matches_reference() {
    if !std::is_x86_feature_detected!("avx512bw") {
        // No AVX-512BW on this host — nothing to verify directly.
        return;
    }
    // SAFETY: AVX-512BW availability just verified via runtime CPU feature detection.
    assert_kernel_matches_reference("avx512bw", |a, b| unsafe { lsp_avx512(a, b) });
}

/// Exhaustively covers the kernel-selection ladder on any x86 host: the
/// runtime dispatch can only exercise the lane the test CPU exposes, but
/// `select_lsp_kernel` is a pure function of the detected-feature flags, so
/// every arm (AVX-512BW, AVX2, SSE2, scalar fallback) is reachable here.
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[test]
fn select_lsp_kernel_picks_widest_available_lane() {
    use core::ptr::fn_addr_eq;
    let pick = |avx512bw, avx2, sse2| {
        select_lsp_kernel(LspCpuFeatures {
            avx512bw,
            avx2,
            sse2,
        })
    };
    assert!(fn_addr_eq(pick(true, true, true), lsp_avx512 as LspKernel));
    assert!(fn_addr_eq(pick(false, true, true), lsp_avx2 as LspKernel));
    assert!(fn_addr_eq(pick(false, false, true), lsp_sse2 as LspKernel));
    assert!(fn_addr_eq(
        pick(false, false, false),
        lsp_scalar as LspKernel
    ));
    // The widest available lane wins even when narrower lanes are absent.
    assert!(fn_addr_eq(
        pick(true, false, false),
        lsp_avx512 as LspKernel
    ));
}

/// Direct test for the NEON 16-byte kernel on LE aarch64. Required because
/// CI codecov runs on `x86_64` Linux — the NEON kernel never executes there
/// even when cross-compiling, but this test gates onto LE aarch64 runners
/// (e.g. macos-latest on M1/M2) to keep the path covered.
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[test]
fn lsp_neon_on_boundaries_matches_reference() {
    // SAFETY: NEON is mandatory in the ARMv8 baseline (`target_arch = "aarch64"`).
    assert_kernel_matches_reference("neon", |a, b| unsafe { lsp_neon(a, b) });
}

// All implementations must agree with the byte-by-byte reference for any input —
// proptest version with random byte patterns up to 1 KiB.
proptest::proptest! {
    #[test]
    fn lsp_scalar_equals_reference(
        s1 in proptest::collection::vec(proptest::num::u8::ANY, 0..=1024),
        s2 in proptest::collection::vec(proptest::num::u8::ANY, 0..=1024),
    ) {
        proptest::prop_assert_eq!(lsp_scalar(&s1, &s2), lsp_reference(&s1, &s2));
    }

    #[test]
    fn longest_shared_prefix_length_equals_reference(
        s1 in proptest::collection::vec(proptest::num::u8::ANY, 0..=1024),
        s2 in proptest::collection::vec(proptest::num::u8::ANY, 0..=1024),
    ) {
        proptest::prop_assert_eq!(longest_shared_prefix_length(&s1, &s2), lsp_reference(&s1, &s2));
    }
}

#[test]
fn test_compare_prefixed_slice() {
    use core::cmp::Ordering::{Equal, Greater, Less};

    assert_eq!(
        Greater,
        compare_prefixed_slice(&[0, 161], &[], &[0], &DefaultUserComparator)
    );

    assert_eq!(
        Equal,
        compare_prefixed_slice(b"abc", b"xyz", b"abcxyz", &DefaultUserComparator)
    );
    assert_eq!(
        Equal,
        compare_prefixed_slice(b"abc", b"", b"abc", &DefaultUserComparator)
    );
    assert_eq!(
        Equal,
        compare_prefixed_slice(b"abc", b"abc", b"abcabc", &DefaultUserComparator)
    );
    assert_eq!(
        Equal,
        compare_prefixed_slice(b"", b"", b"", &DefaultUserComparator)
    );
    assert_eq!(
        Less,
        compare_prefixed_slice(b"a", b"", b"y", &DefaultUserComparator)
    );
    assert_eq!(
        Less,
        compare_prefixed_slice(b"a", b"", b"yyy", &DefaultUserComparator)
    );
    assert_eq!(
        Less,
        compare_prefixed_slice(b"a", b"", b"yyy", &DefaultUserComparator)
    );
    assert_eq!(
        Less,
        compare_prefixed_slice(b"yyyy", b"a", b"yyyyb", &DefaultUserComparator)
    );
    assert_eq!(
        Less,
        compare_prefixed_slice(b"yyy", b"b", b"yyyyb", &DefaultUserComparator)
    );
    assert_eq!(
        Less,
        compare_prefixed_slice(b"abc", b"d", b"abce", &DefaultUserComparator)
    );
    assert_eq!(
        Less,
        compare_prefixed_slice(b"ab", b"", b"ac", &DefaultUserComparator)
    );
    assert_eq!(
        Greater,
        compare_prefixed_slice(b"a", b"", b"", &DefaultUserComparator)
    );
    assert_eq!(
        Greater,
        compare_prefixed_slice(b"", b"a", b"", &DefaultUserComparator)
    );
    assert_eq!(
        Greater,
        compare_prefixed_slice(b"a", b"a", b"", &DefaultUserComparator)
    );
    assert_eq!(
        Greater,
        compare_prefixed_slice(b"b", b"a", b"a", &DefaultUserComparator)
    );
    assert_eq!(
        Greater,
        compare_prefixed_slice(b"a", b"b", b"a", &DefaultUserComparator)
    );
    assert_eq!(
        Greater,
        compare_prefixed_slice(b"abc", b"xy", b"abcw", &DefaultUserComparator)
    );
    assert_eq!(
        Greater,
        compare_prefixed_slice(b"ab", b"cde", b"a", &DefaultUserComparator)
    );
    assert_eq!(
        Greater,
        compare_prefixed_slice(b"abcd", b"zz", b"abc", &DefaultUserComparator)
    );
    assert_eq!(
        Greater,
        compare_prefixed_slice(b"abc", b"d", b"abc", &DefaultUserComparator)
    );
    assert_eq!(
        Greater,
        compare_prefixed_slice(b"aaaa", b"aaab", b"aaaaaaaa", &DefaultUserComparator)
    );
    assert_eq!(
        Greater,
        compare_prefixed_slice(b"aaaa", b"aaba", b"aaaaaaaa", &DefaultUserComparator)
    );
    assert_eq!(
        Greater,
        compare_prefixed_slice(b"abcd", b"x", b"abc", &DefaultUserComparator)
    );

    assert_eq!(
        Less,
        compare_prefixed_slice(&[0x7F], &[], &[0x80], &DefaultUserComparator)
    );
    assert_eq!(
        Greater,
        compare_prefixed_slice(&[0xFF], &[], &[0x10], &DefaultUserComparator)
    );
}

/// Reverse comparator to exercise the Vec-allocation slow path.
struct ReverseComparator;
impl crate::comparator::UserComparator for ReverseComparator {
    fn name(&self) -> &'static str {
        "test-reverse"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
        b.cmp(a)
    }
}

#[test]
fn test_compare_prefixed_slice_custom_comparator() {
    use core::cmp::Ordering::{Equal, Greater, Less};

    use crate::comparator::UserComparator as _;
    assert_eq!(ReverseComparator.name(), "test-reverse");

    // With reverse comparator, "abc" > "xyz" (reversed)
    assert_eq!(
        Greater,
        compare_prefixed_slice(b"ab", b"c", b"xyz", &ReverseComparator)
    );
    assert_eq!(
        Less,
        compare_prefixed_slice(b"xy", b"z", b"abc", &ReverseComparator)
    );
    assert_eq!(
        Equal,
        compare_prefixed_slice(b"ab", b"c", b"abc", &ReverseComparator)
    );
    // Empty cases
    assert_eq!(
        Equal,
        compare_prefixed_slice(b"", b"", b"", &ReverseComparator)
    );
    assert_eq!(
        Less, // reversed: non-empty > empty
        compare_prefixed_slice(b"a", b"", b"", &ReverseComparator)
    );
}
