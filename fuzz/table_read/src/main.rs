#[macro_use]
extern crate afl;

use arbitrary::{Arbitrary, Result, Unstructured};
use lsm_tree::{
    Cache, InternalValue, SeqNo, SharedComparator, ValueType,
    fs::{Fs, MemFs},
    table::{RecoverParams, Table, Writer},
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::sync::Arc;

#[derive(Arbitrary, Eq, PartialEq, Debug, Copy, Clone)]
enum IndexType {
    Full,
    Volatile,
    TwoLevel,
}

#[derive(Arbitrary, Eq, PartialEq, Debug, Copy, Clone)]
enum FilterType {
    Full,
    Volatile,
    Partitioned,
}

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

        // Seqnos never have a leading 1 (they are 63-bit numbers, not 64)
        let seqno = u64::arbitrary(u)? & 0x7FFF_FFFF_FFFF_FFFF;

        let vtype = FuzzyValueType::arbitrary(u)?;

        let key = if key.is_empty() { vec![0] } else { key };

        Ok(Self(InternalValue::from_components(
            key,
            value,
            seqno,
            vtype.into(),
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

fn read_all(iter: impl Iterator<Item = lsm_tree::Result<InternalValue>>) -> Vec<Parts> {
    iter.map(|item| parts(&item.expect("table read"))).collect()
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
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let Ok(restart_interval) = u8::arbitrary(&mut unstructured) else {
            return;
        };
        let restart_interval = restart_interval.max(1);

        let Ok(index_type) = IndexType::arbitrary(&mut unstructured) else {
            return;
        };
        let Ok(filter_type) = FilterType::arbitrary(&mut unstructured) else {
            return;
        };

        let data_block_size: u32 = rng.random_range(1..64_000);
        let item_count = rng.random_range(1..200);
        let hash_ratio: f32 = rng.random_range(0.0..8.0);

        let Ok(mut items) = (0..item_count)
            .map(|_| FuzzyValue::arbitrary(&mut unstructured).map(|v| v.0))
            .collect::<Result<Vec<_>>>()
        else {
            return;
        };

        // Table order is the internal key order; one entry per (key, seqno).
        items.sort_by(|a, b| a.key.cmp(&b.key));
        items.dedup_by(|a, b| a.key == b.key);

        let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
        let folder = std::path::absolute("/fuzz").expect("absolute folder");
        fs.create_dir_all(&folder).expect("create folder");
        let file = folder.join("table");

        let checksum = {
            let mut writer = Writer::new(file.clone(), 0, 0, Arc::clone(&fs))
                .expect("create writer")
                .use_data_block_restart_interval(restart_interval)
                .use_data_block_size(data_block_size)
                .use_data_block_hash_ratio(hash_ratio);

            if index_type == IndexType::TwoLevel {
                writer = writer.use_partitioned_index();
            }
            if filter_type == FilterType::Partitioned {
                writer = writer.use_partitioned_filter();
            }

            for item in &items {
                writer.write(item.clone()).expect("write item");
            }

            let (_, checksum) = writer
                .finish()
                .expect("finish table")
                .expect("the table holds items");
            checksum
        };

        let comparator: SharedComparator = Arc::new(lsm_tree::DefaultUserComparator);
        let mut params = RecoverParams::new(
            file,
            checksum,
            0,
            fs,
            comparator,
            Arc::new(Cache::with_capacity_bytes(0)),
        );
        params.pin_filter = filter_type == FilterType::Full;
        params.pin_index = index_type == IndexType::Full;
        let table = Table::recover(params).expect("recover table");

        assert_eq!(table.metadata.item_count, items.len() as u64);

        assert_eq!(all_parts(&items), read_all(table.iter()));
        assert_eq!(all_parts(items.iter().rev()), read_all(table.iter().rev()));

        for needle in &items {
            if needle.key.seqno == SeqNo::MAX {
                continue;
            }

            let key_hash = lsm_tree::hash::hash64(&needle.key.user_key);

            let at_own_seqno = table
                .get(&needle.key.user_key, needle.key.seqno + 1, key_hash)
                .expect("point read");
            assert_eq!(Some(parts(needle)), at_own_seqno.as_ref().map(parts));

            let newest = table
                .get(&needle.key.user_key, SeqNo::MAX, key_hash)
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

        assert_eq!(
            all_parts(&items),
            read_all(table.scan().expect("open scanner"))
        );

        let code = generate_ping_pong_code(seed, items.len());
        assert_eq!(
            all_parts(ping_pong(items.iter(), &code)),
            read_all(ping_pong(table.iter(), &code).into_iter())
        );
        assert_eq!(
            all_parts(ping_pong(items.iter().rev(), &code)),
            read_all(ping_pong(table.iter().rev(), &code).into_iter())
        );

        {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let mut lo = rng.random_range(0..items.len());
            let mut hi = rng.random_range(0..items.len());
            if lo > hi {
                std::mem::swap(&mut lo, &mut hi);
            }

            // A range covers every version of its bound keys.
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

            assert_eq!(
                all_parts(&items[lo..=hi]),
                read_all(table.range(lo_key..=hi_key)),
            );
        }
    });
}
