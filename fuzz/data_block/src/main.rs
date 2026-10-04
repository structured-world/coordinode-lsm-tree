#[macro_use]
extern crate afl;

use arbitrary::{Arbitrary, Result, Unstructured};
use lsm_tree::{
    InternalValue, SeqNo, SharedComparator, ValueType,
    table::{
        Block, DataBlock,
        block::{BlockType, Header, ParsedItem},
    },
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

#[derive(Arbitrary, Clone, Debug)]
enum FuzzyValueType {
    Value,
    Tombstone,
    // TODO: single delete
}

impl From<FuzzyValueType> for ValueType {
    fn from(value: FuzzyValueType) -> Self {
        match value {
            FuzzyValueType::Value => Self::Value,
            FuzzyValueType::Tombstone => Self::Tombstone,
        }
    }
}

struct FuzzyValue(InternalValue);

impl<'a> Arbitrary<'a> for FuzzyValue {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let key = Vec::<u8>::arbitrary(u)?;
        let value = Vec::<u8>::arbitrary(u)?;
        let seqno = u64::arbitrary(u)?;
        let vtype = FuzzyValueType::arbitrary(u)?;

        let key = if key.is_empty() { vec![0] } else { key };
        let vtype: ValueType = vtype.into();
        // A block stores no value for a tombstone, so the model carries none.
        let value = if vtype == ValueType::Tombstone {
            Vec::new()
        } else {
            value
        };

        Ok(Self(InternalValue::from_components(
            key, value, seqno, vtype,
        )))
    }
}

/// What an item is, compared field by field (`InternalValue` has no
/// whole-value equality).
type Parts = (Vec<u8>, SeqNo, ValueType, Vec<u8>);

fn parts(value: &InternalValue) -> Parts {
    (
        value.key.user_key.to_vec(),
        value.key.seqno,
        value.key.value_type,
        value.value.to_vec(),
    )
}

fn all_parts<'a>(values: impl IntoIterator<Item = &'a InternalValue>) -> Vec<Parts> {
    values.into_iter().map(parts).collect()
}

fn generate_ping_pong_code(seed: u64, len: usize) -> Vec<u8> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    (0..len).map(|_| rng.random_range(0..=1)).collect()
}

/// Takes items from the front (0) or the back (1) of `iter` as `code` says.
fn ping_pong<T>(mut iter: impl DoubleEndedIterator<Item = T>, code: &[u8]) -> Vec<T> {
    code.iter()
        .map(|&side| {
            if side == 0 {
                iter.next().expect("front item")
            } else {
                iter.next_back().expect("back item")
            }
        })
        .collect()
}

fn main() {
    fuzz!(|data: &[u8]| {
        let mut unstructured = Unstructured::new(data);

        let Ok(seed) = u64::arbitrary(&mut unstructured) else {
            return;
        };
        let Ok(restart_interval) = u8::arbitrary(&mut unstructured) else {
            return;
        };
        let restart_interval = restart_interval.max(1);

        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let item_count = rng.random_range(1..100);
        let hash_ratio: f32 = rng.random_range(0.0..8.0);

        let Ok(mut items) = (0..item_count)
            .map(|_| FuzzyValue::arbitrary(&mut unstructured).map(|v| v.0))
            .collect::<Result<Vec<_>>>()
        else {
            return;
        };

        // Block order is the internal key order; one entry per (key, seqno).
        items.sort_by(|a, b| a.key.cmp(&b.key));
        items.dedup_by(|a, b| a.key == b.key);

        let comparator: SharedComparator = std::sync::Arc::new(lsm_tree::DefaultUserComparator);

        let bytes = DataBlock::encode_into_vec(&items, restart_interval, hash_ratio)
            .expect("encode data block");

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_flags: 0,
                stored_checksum: lsm_tree::Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                block_type: BlockType::Data,
            },
        });

        assert_eq!(data_block.len(), items.len());

        if data_block.binary_index_len() > 254 {
            assert!(data_block.hash_bucket_count().is_none());
        } else if hash_ratio > 0.0 {
            assert!(data_block.hash_bucket_count().is_some_and(|n| n > 0));
        }

        for needle in &items {
            if needle.key.seqno == SeqNo::MAX {
                continue;
            }

            let at_own_seqno = data_block
                .point_read(&needle.key.user_key, needle.key.seqno + 1, &comparator)
                .expect("point read");
            assert_eq!(Some(parts(needle)), at_own_seqno.as_ref().map(parts));

            let newest = data_block
                .point_read(&needle.key.user_key, SeqNo::MAX, &comparator)
                .expect("point read")
                .expect("key is present");
            let expected = items
                .iter()
                .find(|item| {
                    item.key.user_key == needle.key.user_key && item.key.seqno < SeqNo::MAX
                })
                .expect("needle itself qualifies");
            assert_eq!(parts(expected), parts(&newest));
        }

        let materialize = |x: lsm_tree::table::data_block::DataBlockParsedItem| {
            x.materialize(data_block.as_slice())
        };

        assert_eq!(
            all_parts(&items),
            all_parts(
                &data_block
                    .iter(comparator.clone())
                    .map(materialize)
                    .collect::<Vec<_>>()
            )
        );
        assert_eq!(
            all_parts(items.iter().rev()),
            all_parts(
                &data_block
                    .iter(comparator.clone())
                    .rev()
                    .map(materialize)
                    .collect::<Vec<_>>()
            )
        );

        let code = generate_ping_pong_code(seed, items.len());
        assert_eq!(
            all_parts(ping_pong(items.iter(), &code)),
            all_parts(&ping_pong(
                data_block.iter(comparator.clone()).map(materialize),
                &code
            ))
        );
        assert_eq!(
            all_parts(ping_pong(items.iter().rev(), &code)),
            all_parts(&ping_pong(
                data_block.iter(comparator.clone()).rev().map(materialize),
                &code
            ))
        );

        {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let mut lo = rng.random_range(0..items.len());
            let mut hi = rng.random_range(0..items.len());
            if lo > hi {
                std::mem::swap(&mut lo, &mut hi);
            }

            // Seeking a key lands on its first (newest) version, and seeking
            // upper on its last one, so widen the model range to whole keys.
            let lo_key = items[lo].key.user_key.clone();
            let hi_key = items[hi].key.user_key.clone();
            let lo = items
                .iter()
                .position(|it| it.key.user_key == lo_key)
                .expect("lo key present");
            let hi = items
                .iter()
                .rposition(|it| it.key.user_key == hi_key)
                .expect("hi key present");

            let mut iter = data_block.iter(comparator.clone());
            assert!(iter.seek(&lo_key, SeqNo::MAX), "should seek");
            assert!(iter.seek_upper(&hi_key, SeqNo::MAX), "should seek");

            assert_eq!(
                all_parts(&items[lo..=hi]),
                all_parts(&iter.map(materialize).collect::<Vec<_>>()),
            );
        }
    });
}
