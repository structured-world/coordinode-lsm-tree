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
/// state. A foreign file whose name merely CONTAINS `.healtmp` (an operator
/// backup like `0.healtmp.backup` or a stray `notes.healtmp`) must NOT be
/// deleted — recovery fails on the unparseable name, exactly as it does for
/// any other unrecognized file under `tables/`, and the file survives.
#[test]
fn recovery_with_non_artifact_healtmp_name_fails_without_deleting() -> lsm_tree::Result<()> {
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

        let result = open(&folder);
        assert!(
            result.is_err(),
            "recovery must refuse the unrecognized file {foreign_name:?} instead of deleting it"
        );
        drop(result);
        assert!(
            foreign.try_exists()?,
            "recovery must never delete the foreign file {foreign_name:?}"
        );
    }

    Ok(())
}
