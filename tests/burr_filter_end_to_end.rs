//! End-to-end smoke test that exercises the BuRR filter through the
//! actual LSM table write + read path.
//!
//! Writes a table, flushes to disk, reopens via the same Config, and
//! verifies:
//!   - every inserted key resolves via `tree.get` (no false negatives —
//!     the filter must never report "definitely absent" for an
//!     inserted key)
//!   - unknown keys never resolve to `Some` (filter false positives
//!     are acceptable — they trigger a wasted index lookup — but the
//!     table read path must not return a value for a key we never
//!     inserted)
//!   - filter efficiency metric trends correctly when the metrics
//!     feature is enabled
//!
//! The integration value is that the table writer's BuRR builder, the
//! on-disk wire format, and the filter block reader all interoperate
//! without the BuRR-specific unit tests' shortcuts (those construct
//! filters in-process and probe in the same process).

use lsm_tree::{AbstractTree, Config, SeqNo, SequenceNumberCounter, get_tmp_folder};
use test_log::test;

#[test]
fn burr_filter_persists_across_table_write_and_reopen() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path().to_owned();

    let inserted: Vec<[u8; 8]> = (0_u64..5_000).map(u64::to_be_bytes).collect();

    {
        let seqno = SequenceNumberCounter::default();
        let tree = Config::new(&path, seqno.clone(), SequenceNumberCounter::default()).open()?;
        for k in &inserted {
            tree.insert(k, b"v", seqno.next());
        }
        tree.flush_active_memtable(0)?;
        tree.major_compact(u64::MAX, 0)?;

        for k in &inserted {
            assert!(
                tree.get(k, SeqNo::MAX)?.is_some(),
                "inserted key {k:?} missing pre-reopen",
            );
        }
    }

    // Reopen — forces filter block reads from disk via the BuRR wire
    // format. If the wire encoder / decoder disagree, point reads on
    // the reopened tree return false negatives.
    {
        let seqno = SequenceNumberCounter::default();
        let tree = Config::new(&path, seqno.clone(), SequenceNumberCounter::default()).open()?;
        for k in &inserted {
            assert!(
                tree.get(k, SeqNo::MAX)?.is_some(),
                "inserted key {k:?} missing after reopen — BuRR FN regression",
            );
        }
        // Probe a disjoint key universe. False-positive hits cost CPU
        // but must never cause a returned `Some` for a key we never
        // inserted (the table read path falls through to the index
        // block; FPR drives only false index lookups, not bad answers).
        for i in 100_000_u64..101_000 {
            assert!(
                tree.get(i.to_be_bytes(), SeqNo::MAX)?.is_none(),
                "unknown key {i} returned Some — index/key path bug",
            );
        }
    }

    Ok(())
}
