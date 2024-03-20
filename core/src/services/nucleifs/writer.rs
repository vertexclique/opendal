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

use std::fs;
use bytes::Bytes;

use std::path::PathBuf;
use std::pin::Pin;
use std::borrow::Borrow;
use pin_project_lite::pin_project;

use futures_lite::AsyncWriteExt;
use nuclei::{block_on, Handle, spawn_blocking};

use crate::raw::*;
use crate::*;

pin_project! {
    pub struct AsyncFsWriter {
        target_path: PathBuf,
        tmp_path: Option<PathBuf>,

        #[pin]
        f: Handle<fs::File>,
    }
}

impl AsyncFsWriter {
    pub fn new(target_path: PathBuf, tmp_path: Option<PathBuf>, file: fs::File) -> Self {
        Self {
            target_path,
            tmp_path,

            f: Handle::new(file).unwrap(),
        }
    }
}

impl oio::Write for AsyncFsWriter {
    async fn write(&mut self, bs: Bytes) -> Result<usize> {
        let mut this = Pin::new(self).project();
        this.f.write(&bs).await.map_err(new_std_io_error)
    }

    async fn close(&mut self) -> Result<()> {
        // let mut this = Pin::new(self).project();
        self.f.flush().await.map_err(new_std_io_error)?;
        let sync = async {
            self.f.sync_all()
                .and_then(|_| {
                    if let Some(tmp_path) = &self.tmp_path {
                        fs::rename(tmp_path, &self.target_path)
                    } else {
                        Ok(())
                    }
                })
        };

        block_on(sync).map_err(new_std_io_error)?;

        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(tmp_path) = &self.tmp_path {
            block_on(async { fs::remove_file(tmp_path) })
                .map_err(new_std_io_error)
        } else {
            Err(Error::new(
                ErrorKind::Unsupported,
                "NucleiFs doesn't support abort if atomic_write_dir is not set",
            ))
        }
    }
}

impl oio::BlockingWrite for AsyncFsWriter {
    fn write(&mut self, bs: Bytes) -> Result<usize> {
        block_on(self.f.write(&bs)).map_err(new_std_io_error)
    }

    fn close(&mut self) -> Result<()> {
        self.f.sync_all().map_err(new_std_io_error)?;

        if let Some(tmp_path) = &self.tmp_path {
            std::fs::rename(tmp_path, &self.target_path).map_err(new_std_io_error)?;
        }

        Ok(())
    }
}
