#[macro_use]
extern crate afl;

use arbitrary::{Arbitrary, Unstructured};
use lsm_tree::{DefaultUserComparator, UserComparator, table::util::compare_prefixed_slice};

/// Byte order without the lexicographic flag, so `compare_prefixed_slice`
/// takes the path that materialises `prefix + suffix` (stack buffer up to 256
/// bytes, a `Vec` beyond).
struct Materialising;

impl UserComparator for Materialising {
    fn name(&self) -> &'static str {
        "fuzz-materialising"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }
}

/// Reverse byte order: checks the slow path passes the operands in order.
struct Reverse;

impl UserComparator for Reverse {
    fn name(&self) -> &'static str {
        "fuzz-reverse"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        b.cmp(a)
    }
}

fn main() {
    fuzz!(|data: &[u8]| {
        let mut unstructured = Unstructured::new(data);

        let Ok(prefix) = Vec::<u8>::arbitrary(&mut unstructured) else {
            return;
        };
        let Ok(suffix) = Vec::<u8>::arbitrary(&mut unstructured) else {
            return;
        };
        let Ok(needle) = Vec::<u8>::arbitrary(&mut unstructured) else {
            return;
        };

        let combined: Vec<u8> = prefix.iter().chain(suffix.iter()).copied().collect();
        let expected = combined.as_slice().cmp(&needle);

        for (name, cmp, want) in [
            (
                "default",
                &DefaultUserComparator as &dyn UserComparator,
                expected,
            ),
            ("materialising", &Materialising, expected),
            ("reverse", &Reverse, expected.reverse()),
        ] {
            let result = compare_prefixed_slice(&prefix, &suffix, &needle, cmp);
            assert_eq!(
                result, want,
                "{name}: compare_prefixed_slice({prefix:?}, {suffix:?}, {needle:?})"
            );
        }
    });
}
