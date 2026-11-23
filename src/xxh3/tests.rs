use super::kernel::Kernel;
use super::{DEFAULT_SECRET, Hasher, MID_SIZE_MAX, hash64, hash128, hash128_with_seed};

/// Bytes no two lengths or offsets share a pattern of.
fn corpus(len: usize) -> alloc::vec::Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[3]
        })
        .collect()
}

/// Lengths around every boundary the long path has: the short paths' end,
/// a stripe, a block (1,024 bytes), the streaming buffer (256), and a few
/// long ones.
fn lengths() -> impl Iterator<Item = usize> {
    (0..=2_200).chain([4_095, 4_096, 4_097, 65_535, 65_536, 1 << 20, (1 << 20) + 13])
}

const SEEDS: [u64; 4] = [0, 1, 0xC0FF_EE00_1234_5678, u64::MAX];

/// Every kernel this CPU runs gives the digests xxh3 defines, at every length
/// around the loop's boundaries, under the default secret and seeded ones.
/// A kernel that drops or misplaces one lane, stripe or scramble differs at
/// some length here.
#[test]
fn every_kernel_digests_as_xxh3_defines() {
    let data = corpus((1 << 20) + 13);
    for (name, kernel) in Kernel::available() {
        for len in lengths().filter(|&len| len > MID_SIZE_MAX) {
            let input = &data[..len];
            for seed in SEEDS {
                let secret = super::secret_for(seed);
                let acc = super::long_acc(kernel, input, &secret);
                assert_eq!(
                    super::merge128(&acc, &secret, len as u64),
                    xxhash_rust::xxh3::xxh3_128_with_seed(input, seed),
                    "{name}: 128 bits, {len} bytes, seed {seed:#x}"
                );
                assert_eq!(
                    super::merge64(&acc, &secret, len as u64),
                    xxhash_rust::xxh3::xxh3_64_with_seed(input, seed),
                    "{name}: 64 bits, {len} bytes, seed {seed:#x}"
                );
            }
        }
    }
}

/// The one-shot functions give xxh3's digests on either side of the short
/// paths' end, through whichever kernel this CPU takes.
#[test]
fn one_shot_digests_match_xxh3_at_every_length() {
    let data = corpus((1 << 20) + 13);
    for len in lengths() {
        let input = &data[..len];
        assert_eq!(
            hash64(input),
            xxhash_rust::xxh3::xxh3_64(input),
            "{len} bytes"
        );
        assert_eq!(
            hash128(input),
            xxhash_rust::xxh3::xxh3_128(input),
            "{len} bytes"
        );
        for seed in SEEDS {
            assert_eq!(
                hash128_with_seed(input, seed),
                xxhash_rust::xxh3::xxh3_128_with_seed(input, seed),
                "{len} bytes, seed {seed:#x}"
            );
        }
    }
}

/// The secret a seed derives is xxh3's: zero keeps the default, and a seeded
/// one hashes as `xxhash-rust`'s seeded hash does (checked above), so here
/// only that it differs from the default and is deterministic.
#[test]
fn a_seed_derives_its_own_secret_and_zero_keeps_the_default() {
    assert_eq!(super::secret_for(0), DEFAULT_SECRET);
    assert_ne!(super::secret_for(1), DEFAULT_SECRET);
    assert_eq!(super::secret_for(7), super::secret_for(7));
}

/// Hashing input in parts gives the digest of the parts joined, whatever the
/// cut: parts that fill the buffer exactly, overflow it by one, cross a
/// block, and come one byte at a time; and digests taken midway do not
/// disturb the ones after.
#[test]
fn streamed_digests_equal_the_digest_of_the_whole() {
    let data = corpus(70_000);
    let cuts: &[&[usize]] = &[
        &[1],
        &[63, 1],
        &[64],
        &[255],
        &[256],
        &[257],
        &[100, 156, 1],
        &[1_000, 24, 1_025],
        &[5_000],
        &[17, 4_096, 3],
    ];
    for seed in SEEDS {
        for pattern in cuts {
            for len in [
                0, 1, 200, 240, 241, 256, 257, 300, 1_024, 1_025, 2_048, 9_999, 70_000,
            ] {
                let input = &data[..len];
                let mut hasher = Hasher::with_seed(seed);
                let mut at = 0;
                let mut step = pattern.iter().cycle();
                while at < len {
                    let part = (*step.next().unwrap_or(&1)).min(len - at);
                    hasher.update(&input[at..at + part]);
                    at += part;
                    // A digest midway reads the state without changing it.
                    let _ = hasher.digest128();
                }
                assert_eq!(
                    hasher.digest128(),
                    xxhash_rust::xxh3::xxh3_128_with_seed(input, seed),
                    "{len} bytes in parts of {pattern:?}, seed {seed:#x}"
                );
                assert_eq!(
                    hasher.digest(),
                    xxhash_rust::xxh3::xxh3_64_with_seed(input, seed),
                    "{len} bytes in parts of {pattern:?}, seed {seed:#x}"
                );
            }
        }
    }
}

/// A hasher given nothing digests the empty input, and one built by default
/// is the unseeded one.
#[test]
fn an_empty_hasher_digests_the_empty_input() {
    let hasher = Hasher::default();
    assert_eq!(hasher.digest(), xxhash_rust::xxh3::xxh3_64(&[]));
    assert_eq!(hasher.digest128(), xxhash_rust::xxh3::xxh3_128(&[]));
    assert_eq!(Hasher::new().digest128(), hasher.digest128());
}

/// A digest that differs from xxh3's in one bit of input is caught: the
/// checks the engine makes on blocks depend on the kernel distinguishing
/// inputs, not only reproducing equal ones.
#[test]
fn a_flipped_bit_changes_the_long_digest() {
    let mut data = corpus(4_096);
    let before = hash128(&data);
    data[2_049] ^= 1;
    assert_ne!(hash128(&data), before);
    assert_eq!(hash128(&data), xxhash_rust::xxh3::xxh3_128(&data));
}
