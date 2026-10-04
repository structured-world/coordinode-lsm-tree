#[macro_use]
extern crate afl;

use arbitrary::{Arbitrary, Unstructured};
use lsm_tree::{
    SeqNo, SharedComparator,
    table::{
        Block, BlockHandle, IndexBlock, KeyedBlockHandle,
        block::{BlockOffset, BlockType, Header, ParsedItem},
    },
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// What a handle is, compared field by field (`KeyedBlockHandle` has no
/// equality).
type Parts = (Vec<u8>, SeqNo, u64, u32);

fn parts(handle: &KeyedBlockHandle) -> Parts {
    (
        handle.end_key().to_vec(),
        handle.seqno(),
        *handle.offset(),
        handle.size(),
    )
}

fn all_parts<'a>(handles: impl IntoIterator<Item = &'a KeyedBlockHandle>) -> Vec<Parts> {
    handles.into_iter().map(parts).collect()
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
        let Ok(entries) = Vec::<(Vec<u8>, u64, u32)>::arbitrary(&mut unstructured) else {
            return;
        };
        if entries.is_empty() {
            return;
        }

        // Index order: end key ascending, seqno descending; one entry per
        // (end key, seqno).
        let mut entries: Vec<(Vec<u8>, SeqNo, u32)> = entries
            .into_iter()
            .map(|(key, seqno, size)| {
                let key = if key.is_empty() { vec![0] } else { key };
                (key, seqno & 0x7FFF_FFFF_FFFF_FFFF, size)
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        entries.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);

        // Blocks are laid out back to back, as a writer places them.
        let mut offset = 0u64;
        let items: Vec<KeyedBlockHandle> = entries
            .into_iter()
            .map(|(key, seqno, size)| {
                let handle = KeyedBlockHandle::new(
                    key.into(),
                    seqno,
                    BlockHandle::new(BlockOffset(offset), size),
                );
                offset += u64::from(size);
                handle
            })
            .collect();

        let comparator: SharedComparator = std::sync::Arc::new(lsm_tree::DefaultUserComparator);

        let bytes = IndexBlock::encode_into_vec_with_restart_interval(&items, restart_interval)
            .expect("encode index block");

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_flags: 0,
                stored_checksum: lsm_tree::Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                block_type: BlockType::Index,
            },
        });

        assert_eq!(index_block.len(), items.len());

        let materialize =
            |x| ParsedItem::<KeyedBlockHandle>::materialize(&x, index_block.as_slice());

        assert_eq!(
            all_parts(&items),
            all_parts(
                &index_block
                    .iter(comparator.clone())
                    .map(materialize)
                    .collect::<Vec<_>>()
            )
        );
        assert_eq!(
            all_parts(items.iter().rev()),
            all_parts(
                &index_block
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
                index_block.iter(comparator.clone()).map(materialize),
                &code
            ))
        );
        assert_eq!(
            all_parts(ping_pong(items.iter().rev(), &code)),
            all_parts(&ping_pong(
                index_block.iter(comparator.clone()).rev().map(materialize),
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

            // Seeking lands on the first entry of an end key and seeking upper
            // on its last, so widen the model range to whole keys.
            let lo_key = items[lo].end_key().clone();
            let hi_key = items[hi].end_key().clone();
            let lo = items
                .iter()
                .position(|it| *it.end_key() == lo_key)
                .expect("lo key present");
            let hi = items
                .iter()
                .rposition(|it| *it.end_key() == hi_key)
                .expect("hi key present");

            let mut iter = index_block.iter(comparator.clone());
            assert!(iter.seek(&lo_key, SeqNo::MAX), "should seek");
            assert!(iter.seek_upper(&hi_key, SeqNo::MAX), "should seek");

            assert_eq!(
                all_parts(&items[lo..=hi]),
                all_parts(&iter.map(materialize).collect::<Vec<_>>()),
            );
        }
    });
}
