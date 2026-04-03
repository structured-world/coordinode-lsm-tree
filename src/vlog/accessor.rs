// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    version::BlobFileList,
    vlog::{blob_file::reader::Reader, ValueHandle},
    Cache, GlobalTableId, TreeId, UserValue,
};
use std::path::Path;

pub struct Accessor<'a>(&'a BlobFileList);

impl<'a> Accessor<'a> {
    pub fn new(blob_files: &'a BlobFileList) -> Self {
        Self(blob_files)
    }

    pub fn get(
        &self,
        tree_id: TreeId,
        base_path: &Path,
        key: &[u8],
        vhandle: &ValueHandle,
        cache: &Cache,
    ) -> crate::Result<Option<UserValue>> {
        if let Some(value) = cache.get_blob(tree_id, vhandle) {
            return Ok(Some(value));
        }

        let Some(blob_file) = self.0.get(vhandle.blob_file_id) else {
            return Ok(None);
        };

        let bf_id = GlobalTableId::from((tree_id, blob_file.id()));

        let (file, _) = blob_file
            .file_accessor()
            .get_or_open_blob_file(&bf_id, &base_path.join(vhandle.blob_file_id.to_string()))?;

        let value = Reader::new(blob_file, file.as_ref()).get(key, vhandle)?;
        cache.insert_blob(tree_id, vhandle, value.clone());

        Ok(Some(value))
    }
}
