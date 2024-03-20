use std::io::SeekFrom;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::pin::{pin, Pin};
use std::task::{Context, Poll};
use bytes::Bytes;
use futures::AsyncReadExt;
use futures_lite::{AsyncRead, AsyncSeek, Stream, AsyncSeekExt};
use nuclei::Handle;
use crate::raw::{new_std_io_error, oio};
use pin_project_lite::pin_project;
use crate::raw::oio::FuturesReader;
use std::{fs, io};
use tokio::io::ReadBuf;


pin_project! {
    /// AsyncReader implements [`oio::Read`] via [`AsyncRead`] + [`AsyncSeek`].
    pub struct AsyncFsReader
    {
        #[pin]
        inner: Handle<fs::File>,
        buf: Vec<u8>,
    }
}

impl AsyncFsReader
{
    /// Create a new async file reader.
    pub fn new(file: fs::File) -> Self {
        Self { inner: Handle::new(file).unwrap(), buf: Vec::with_capacity(64 * 1024) }
    }
}

impl oio::Read for AsyncFsReader
{
    async fn read(&mut self, limit: usize) -> crate::Result<Bytes> {
        let mut this = Pin::new(self).project();

        // Make sure buf has enough space.
        if this.buf.capacity() < limit {
            this.buf.reserve(limit);
        }
        let buf = this.buf.spare_capacity_mut();
        let mut read_buf: ReadBuf = ReadBuf::uninit(buf);

        // SAFETY: Read at most `size` bytes into `read_buf`.
        unsafe {
            read_buf.assume_init(limit);
        }

        let n = this
            .inner
            .read(read_buf.initialized_mut())
            .await
            .map_err(|err| {
                new_std_io_error(err)
                    .with_operation(oio::ReadOperation::Read)
                    .with_context("source", "AsyncFileReader")
            })?;
        read_buf.set_filled(n);

        Ok(Bytes::copy_from_slice(read_buf.filled()))
    }

    async fn seek(&mut self, pos: io::SeekFrom) -> crate::Result<u64> {
        let mut this = Pin::new(self).project();

        this.inner.seek(pos).await.map_err(|err| {
            new_std_io_error(err)
                .with_operation(oio::ReadOperation::Seek)
                .with_context("source", "AsyncFileReader")
        })
    }
}