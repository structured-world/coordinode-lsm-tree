use super::{BlobGuard, IterGuard, IterGuardImpl, StandardGuard};
use crate::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
use test_log::test;

fn first_guard(blob: bool) -> crate::Result<IterGuardImpl> {
    let folder = crate::get_tmp_folder();
    let config = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );
    let config = if blob {
        config.with_kv_separation(Some(KvSeparationOptions::default()))
    } else {
        config
    };
    let tree = config.open()?;
    tree.insert("a", "1", 0);
    Ok(tree.iter(1, None).next().expect("one entry"))
}

#[test]
fn iter_guard_standard_converts_into_standard_guard_and_back() -> crate::Result<()> {
    let guard: Result<BlobGuard, _> = first_guard(false)?.try_into();
    assert_eq!(
        guard.err(),
        Some("Tried to convert variant Standard to Blob")
    );

    let guard: StandardGuard = first_guard(false)?.try_into().expect("a standard guard");
    let guard = IterGuardImpl::from(guard);
    assert!(matches!(guard, IterGuardImpl::Standard(_)));
    assert_eq!(&*guard.key()?, b"a");
    Ok(())
}

#[test]
fn iter_guard_blob_converts_into_blob_guard_and_back() -> crate::Result<()> {
    let guard: Result<StandardGuard, _> = first_guard(true)?.try_into();
    assert_eq!(
        guard.err(),
        Some("Tried to convert variant Blob to Standard")
    );

    let guard: BlobGuard = first_guard(true)?.try_into().expect("a blob guard");
    let guard = IterGuardImpl::from(guard);
    assert!(matches!(guard, IterGuardImpl::Blob(_)));
    assert_eq!(&*guard.value()?, b"1");
    Ok(())
}
