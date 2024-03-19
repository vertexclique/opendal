use std::io::SeekFrom;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::pin::{pin, Pin};
use std::task::{Context, Poll};
use bytes::Bytes;
use futures::AsyncReadExt;
use futures_lite::{AsyncRead, AsyncSeek, Stream};
use nuclei::Handle;
use crate::raw::{new_std_io_error, oio};
use pin_project_lite::pin_project;
use crate::raw::oio::FuturesReader;
use std::fs;


pin_project! {
    /// AsyncReader implements [`oio::Read`] via [`AsyncRead`] + [`AsyncSeek`].
    pub struct AsyncFileReader
    {
        #[pin]
        inner: Handle<fs::File>,
    }
}

impl AsyncFileReader
{
    /// Create a new async file reader.
    pub fn new(file: fs::File) -> Self {
        Self { inner: Handle::new(file).unwrap() }
    }
}

impl oio::Read for AsyncFileReader
{
    fn poll_read(&mut self, cx: &mut Context<'_>, mut buf: &mut [u8]) -> Poll<crate::Result<usize>> {
        let mut this = Pin::new(self).project();

        this.inner.poll_read(cx, &mut buf).map_err(|err| {
            new_std_io_error(err)
                .with_operation(oio::ReadOperation::Read)
                .with_context("source", "AsyncFileReader")
        })
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<crate::Result<u64>> {
        let mut this = Pin::new(self).project();

        this.inner.poll_seek(cx, pos).map_err(|err| {
            new_std_io_error(err)
                .with_operation(oio::ReadOperation::Seek)
                .with_context("source", "AsyncFileReader")
        })
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<crate::Result<Bytes>>> {
        let mut this = Pin::new(self).project();

        this.inner.poll_next(cx)
    }
}