use super::super::burr::packed::transpose_into;
use super::*;
use crate::table::filter::ribbon::hashing::SplitMix64;

/// Rows of a narrow band as a solve leaves them: an occupied row has bit 0 set
/// and the rows it names exist, an unoccupied one is zero and its value slot
/// stale, as a reused scratch leaves it.
fn rows(m: usize, r: usize, seed: u64) -> (Vec<u64>, Vec<u64>) {
    let mask = if r == 64 { u64::MAX } else { (1 << r) - 1 };
    let mut rng = SplitMix64::new(seed);
    let (mut coeff, mut rhs) = (vec![0; m], vec![0; m]);
    for i in 0..m {
        rhs[i] = rng.next_u64() & mask;
        if rng.next_u64().is_multiple_of(8) {
            continue;
        }
        let room = (m - i).min(64) as u32;
        coeff[i] = (rng.next_u64() | 1) & (u64::MAX >> (64 - room));
    }
    (coeff, rhs)
}

/// What the columns must solve to: each row's value XORed with the solved rows
/// its coefficient names, bit by bit, bit-sliced as the wire format stores it.
fn payload_by_row_walk(coeff: &[u64], rhs: &[u64], r: usize) -> Vec<u8> {
    let mut rows = vec![0u64; coeff.len()];
    for i in (0..coeff.len()).rev() {
        if coeff[i] & 1 == 0 {
            continue;
        }
        let mut row = rhs[i];
        let mut upper = coeff[i] & !1;
        while upper != 0 {
            row ^= rows[i + upper.trailing_zeros() as usize];
            upper &= upper - 1;
        }
        rows[i] = row;
    }
    let r = r as u8;
    let mut payload = vec![0; z_byte_len(coeff.len(), r).unwrap_or(0)];
    transpose_into(&rows, r, &mut payload);
    payload
}

/// The output of a filter is on-disk bytes: the columns solve to what walking
/// each row's bits and transposing them does, on every kernel, at every
/// fingerprint width, whole last segment or partial.
#[test]
fn every_kernel_solves_to_the_bytes_of_the_row_walk() -> Result<(), ConstructionFailure> {
    for r in [1, 7, 8, 16, 33, 64] {
        for m in [64, 65, 127, 128, 1_000] {
            let (coeff, rhs) = rows(m, r, (r * m) as u64);
            let expected = payload_by_row_walk(&coeff, &rhs, r);

            let mut scalar = vec![0; expected.len()];
            back_substitute_columns_scalar(&coeff, &rhs, r, &mut scalar)?;
            assert_eq!(scalar, expected, "scalar, r {r}, m {m}");

            let mut best = vec![0; expected.len()];
            back_substitute_columns_best(&coeff, &rhs, r, &mut best)?;
            assert_eq!(best, expected, "best, r {r}, m {m}");
        }
    }
    Ok(())
}

/// A coefficient reaching past the last slot names a row that does not exist.
#[test]
fn a_row_reaching_past_the_slots_is_refused() {
    let coeff = [0, 0, 0b101, 0];
    let mut out = vec![0; 8];
    let failure = back_substitute_columns_best(&coeff, &[0; 4], 1, &mut out);
    assert!(
        matches!(
            failure,
            Err(ConstructionFailure::OutOfBounds {
                row_index: 4,
                m: 4,
                ..
            })
        ),
        "{failure:?}",
    );
}
