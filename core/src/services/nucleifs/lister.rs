// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fs::ReadDir;
use std::path;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use nuclei::{block_on, Handle, spawn_blocking};
use pin_project_lite::pin_project;

use crate::raw::*;
use crate::EntryMode;
use crate::Metadata;
use crate::raw::oio::Entry;
use crate::Result;

pin_project! {
    pub struct AsyncFsLister {
        root: PathBuf,

        #[pin]
        rd: ReadDir,
    }
}

impl AsyncFsLister {
    pub fn new(root: &Path, rd: ReadDir) -> Self {
        Self {
            root: root.to_owned(),
            rd,
        }
    }

    fn lister(rd: &mut &mut AsyncFsLister) -> Result<Option<oio::Entry>> {
        let root = rd.root.to_owned();
        let Some(de) = rd.rd.next() else {
            return Ok(None);
        };
        let de = de.map_err(new_std_io_error)?;

        let ft = de.file_type().map_err(new_std_io_error)?;

        let entry_path = de.path();
        let rel_path = normalize_path(
            &entry_path
                .strip_prefix(&root)
                .expect("cannot fail because the prefix is iterated")
                .to_string_lossy()
                .replace('\\', std::path::MAIN_SEPARATOR_STR),
        );

        let d = if ft.is_file() {
            oio::Entry::new(&rel_path, Metadata::new(EntryMode::FILE))
        } else if ft.is_dir() {
            // Make sure we are returning the correct path.
            oio::Entry::new(&format!("{rel_path}{}", path::MAIN_SEPARATOR), Metadata::new(EntryMode::DIR))
        } else {
            oio::Entry::new(&rel_path, Metadata::new(EntryMode::Unknown))
        };

        Ok(Some(d))
    }
}

impl oio::List for AsyncFsLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let mut rd = Pin::new(self).get_mut();

        block_on(async { Self::lister(&mut rd) })
    }
}

impl oio::BlockingList for AsyncFsLister {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        let de = match self.rd.next() {
            Some(de) => de.map_err(new_std_io_error)?,
            None => return Ok(None),
        };

        let entry_path = de.path();
        let rel_path = normalize_path(
            &entry_path
                .strip_prefix(&self.root)
                .expect("cannot fail because the prefix is iterated")
                .to_string_lossy()
                .replace('\\', std::path::MAIN_SEPARATOR_STR),
        );

        // On Windows and most Unix platforms this function is free
        // (no extra system calls needed), but some Unix platforms may
        // require the equivalent call to symlink_metadata to learn about
        // the target file type.
        let file_type = de.file_type().map_err(new_std_io_error)?;

        let entry = if file_type.is_file() {
            oio::Entry::new(&rel_path, Metadata::new(EntryMode::FILE))
        } else if file_type.is_dir() {
            // Make sure we are returning the correct path.
            oio::Entry::new(&format!("{rel_path}{}", std::path::MAIN_SEPARATOR), Metadata::new(EntryMode::DIR))
        } else {
            oio::Entry::new(&rel_path, Metadata::new(EntryMode::Unknown))
        };

        Ok(Some(entry))
    }
}
