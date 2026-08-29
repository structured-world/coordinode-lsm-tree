use lsm_tree::{AbstractTree, Config, SequenceNumberCounter, get_tmp_folder};
use test_log::test;

fn open(folder: &tempfile::TempDir) -> lsm_tree::Result<lsm_tree::AnyTree> {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
}

/// An abandoned heal copy has the exact `{table-id}.healtmp-{seq}` shape (both
/// parts numeric). Recovery owns it: the sweep removes it and the tree reopens.
#[test]
fn recovery_sweeps_exact_healtmp_artifact_and_reopens() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    {
        let tree = open(&folder)?;
        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
    }

    // Simulate a hard crash between the heal copy's creation and its rename.
    let artifact = folder.path().join("tables").join("0.healtmp-3");
    std::fs::File::create(&artifact)?;

    {
        let tree = open(&folder)?;
        assert_eq!(1, tree.table_count());
    }
    assert!(
        !artifact.try_exists()?,
        "the abandoned heal copy must be swept on recovery"
    );

    Ok(())
}

/// Only the exact `{numeric-id}.healtmp-{numeric-seq}` shape is owned cleanup
/// state. A file whose name merely CONTAINS `.healtmp` (an operator backup like
/// `0.healtmp.backup` or a stray `notes.healtmp`) is not engine state at all:
/// recovery passes over it and it survives, exactly as any other unowned name
/// under `tables/` does.
#[test]
fn recovery_ignores_a_non_artifact_healtmp_name_without_deleting_it() -> lsm_tree::Result<()> {
    for foreign_name in [
        "0.healtmp.backup",
        "notes.healtmp",
        "0.healtmp-3x",
        "x.healtmp-3",
    ] {
        let folder = get_tmp_folder();

        {
            let tree = open(&folder)?;
            tree.insert("a", "a", 0);
            tree.flush_active_memtable(0)?;
        }

        let foreign = folder.path().join("tables").join(foreign_name);
        std::fs::File::create(&foreign)?;

        let tree = open(&folder)?;
        assert_eq!(
            tree.get("a", lsm_tree::MAX_SEQNO)?.as_deref(),
            Some(&b"a"[..]),
            "an unowned name must not affect recovery: {foreign_name:?}",
        );
        assert!(
            foreign.try_exists()?,
            "recovery must never delete the foreign file {foreign_name:?}"
        );
    }

    Ok(())
}

/// Recovery owns four more sidecar artifact families and sweeps the disposable
/// ones while preserving the live ones: a `{id}.heal-attest` for a LIVE table
/// survives (the next scrub reconciles a crashed digest refresh through it), a
/// `{id}.heal-attest` for a RETIRED (absent-from-manifest) table is swept
/// (nothing can ever reconcile a table that no longer exists), and the crashed
/// publish temps `{id}.heal-attest.tmp` and `{id}.restrict-bound.tmp` are always
/// swept. The tree reopens cleanly throughout.
#[test]
fn recovery_sweeps_disposable_sidecar_artifacts_and_keeps_live_ones() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    {
        let tree = open(&folder)?;
        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count(), "the flush produced the live table 0");
    }
    let tables = folder.path().join("tables");

    // Live table 0's pending attestation: preserved.
    let live_attest = tables.join("0.heal-attest");
    std::fs::File::create(&live_attest)?;
    // A retired (never-in-manifest) id's attestation: swept.
    let orphan_attest = tables.join("999.heal-attest");
    std::fs::File::create(&orphan_attest)?;
    // Crashed atomic-publish temps of both sidecar kinds: swept.
    let attest_tmp = tables.join("0.heal-attest.tmp");
    std::fs::File::create(&attest_tmp)?;
    let restrict_tmp = tables.join("0.restrict-bound.tmp");
    std::fs::File::create(&restrict_tmp)?;

    {
        let tree = open(&folder)?;
        assert_eq!(
            1,
            tree.table_count(),
            "the tree reopens with its single table"
        );
    }

    assert!(
        live_attest.try_exists()?,
        "a LIVE table's heal attestation must be preserved for the next scrub",
    );
    assert!(
        !orphan_attest.try_exists()?,
        "a retired table's orphaned heal attestation must be swept",
    );
    assert!(
        !attest_tmp.try_exists()?,
        "an abandoned heal-attest publish temp must be swept",
    );
    assert!(
        !restrict_tmp.try_exists()?,
        "an abandoned restrict-bound publish temp must be swept",
    );

    Ok(())
}
