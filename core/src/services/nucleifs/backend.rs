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


use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;
use log::debug;
use nuclei::Handle;
use uuid::Uuid;

use crate::raw::*;
use crate::*;
use crate::services::nucleifs::lister::AsyncFsLister;
use crate::services::nucleifs::reader::{AsyncFsReader};
use crate::services::nucleifs::writer::AsyncFsWriter;

/// Asynchronous POSIX alike file system.
#[doc = include_str!("docs.md")]
#[derive(Default, Debug)]
pub struct NucleiFsBuilder {
    root: Option<PathBuf>,
    atomic_write_dir: Option<PathBuf>,
}

impl NucleiFsBuilder {
    /// Set root for backend.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(PathBuf::from(root))
        };

        self
    }

    /// Set temp dir for atomic write.
    ///
    /// # Notes
    ///
    /// - When append is enabled, we will not use atomic write
    /// to avoid data loss and performance issue.
    pub fn atomic_write_dir(&mut self, dir: &str) -> &mut Self {
        self.atomic_write_dir = if dir.is_empty() {
            None
        } else {
            Some(PathBuf::from(dir))
        };

        self
    }

    /// OpenDAL requires all input path are normalized to make sure the
    /// behavior is consistent. By enable path check, we can make sure
    /// fs will behave the same as other services.
    ///
    /// Enabling this feature will lead to extra metadata call in all
    /// operations.
    #[deprecated(note = "path always checked since RFC-3243 List Prefix")]
    pub fn enable_path_check(&mut self) -> &mut Self {
        self
    }
}

impl Builder for NucleiFsBuilder {
    const SCHEME: Scheme = Scheme::Nucleifs;
    type Accessor = NucleifsBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = NucleiFsBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("atomic_write_dir")
            .map(|v| builder.atomic_write_dir(v));
        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = match self.root.take() {
            Some(root) => Ok(root),
            None => Err(Error::new(
                ErrorKind::ConfigInvalid,
                "root is not specified",
            )),
        }?;
        debug!("backend use root {}", root.to_string_lossy());

        // If root dir is not exist, we must create it.
        if let Err(e) = std::fs::metadata(&root) {
            if e.kind() == std::io::ErrorKind::NotFound {
                std::fs::create_dir_all(&root).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "create root dir failed")
                        .with_operation("Builder::build")
                        .with_context("root", root.to_string_lossy())
                        .set_source(e)
                })?;
            }
        }

        let atomic_write_dir = self.atomic_write_dir.take();

        // If atomic write dir is not exist, we must create it.
        if let Some(d) = &atomic_write_dir {
            if let Err(e) = std::fs::metadata(d) {
                if e.kind() == std::io::ErrorKind::NotFound {
                    std::fs::create_dir_all(d).map_err(|e| {
                        Error::new(ErrorKind::Unexpected, "create atomic write dir failed")
                            .with_operation("Builder::build")
                            .with_context("atomic_write_dir", d.to_string_lossy())
                            .set_source(e)
                    })?;
                }
            }
        }

        // Canonicalize the root directory. This should work since we already know that we can
        // get the metadata of the path.
        let root = root.canonicalize().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                "canonicalize of root directory failed",
            )
                .with_operation("Builder::build")
                .with_context("root", root.to_string_lossy())
                .set_source(e)
        })?;

        // Canonicalize the atomic_write_dir directory. This should work since we already know that
        // we can get the metadata of the path.
        let atomic_write_dir = atomic_write_dir
            .map(|p| {
                p.canonicalize().map(Some).map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "canonicalize of atomic_write_dir directory failed",
                    )
                        .with_operation("Builder::build")
                        .with_context("root", root.to_string_lossy())
                        .set_source(e)
                })
            })
            .unwrap_or(Ok(None))?;

        debug!("backend build finished: {:?}", &self);
        Ok(NucleifsBackend {
            root,
            atomic_write_dir,
        })
    }
}

/// Backend is used to serve `Accessor` support for async POSIX alike fs.
#[derive(Debug, Clone)]
pub struct NucleifsBackend {
    root: PathBuf,
    atomic_write_dir: Option<PathBuf>,
}

fn tmp_file_of(path: &str) -> String {
    let name = get_basename(path);
    let uuid = Uuid::new_v4().to_string();

    format!("{name}.{uuid}")
}

impl NucleifsBackend {

}

#[async_trait]
impl Accessor for NucleifsBackend {
    type Reader = AsyncFsReader;
    type Writer = AsyncFsWriter;
    type Lister = AsyncFsLister;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        todo!()
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        todo!()
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        todo!()
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        todo!()
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        todo!()
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        todo!()
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        todo!()
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        todo!()
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        todo!()
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        todo!()
    }

    async fn batch(&self, args: OpBatch) -> Result<RpBatch> {
        todo!()
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        todo!()
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        todo!()
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        todo!()
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        todo!()
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> Result<RpDelete> {
        todo!()
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        todo!()
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        todo!()
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        todo!()
    }
}