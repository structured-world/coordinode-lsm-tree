use super::*;
use crate::coding::Encode;
use crate::vlog::ValueHandle;
use test_log::test;

/// An indirection naming a blob file the version does not hold is damage:
/// resolving it is an error the caller can handle, never a panic and never
/// an empty value.
#[test]
fn a_dangling_indirection_resolves_to_an_error() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let crate::AnyTree::Blob(tree) = crate::Config::new(
        folder.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
    .open()?
    else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    let dangling = BlobIndirection {
        vhandle: ValueHandle {
            blob_file_id: 999,
            offset: 0,
            on_disk_size: 10,
        },
        size: 10,
    };
    let item = InternalValue::from_components(
        "k",
        dangling.encode_into_vec(),
        0,
        crate::ValueType::Indirection,
    );
    let version = tree.current_version();
    let result = resolve_value_handle(tree.id(), &tree.index.config.cache, &version, item);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader(_))),
        "{result:?}"
    );
    Ok(())
}
