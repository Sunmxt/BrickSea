use error::{Result, Error};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::future::Future;

use super::{IOScheduler, AsyncBlockFile, BlockFile, IOOption};

struct BlockingIODriver {
    max_io_depth: usize,
}

struct BlockingIOFuture {
}

impl Future for BlockingIOFuture {
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl AsyncBlockFile for BlockingIOBlockFile {
    type IOFuture = BlockingIOFuture;

    fn preadv(&self, iovs: &[&mut [u8]], opt: IOOption) -> Self::IOFuture {
        BlockingIOFuture{}
    }

    fn pwritev(&mut self, iovs: &[&[u8]], opt: IOOption) -> Self::IOFuture {
        BlockingIOFuture{}
    }
}

struct BlockingIOBlockFile {
}

impl BlockFile for BlockingIOBlockFile {
    type AsyncT = BlockingIOBlockFile;

    fn preadv(&self, iovs: &[&mut [u8]], opt: IOOption) -> Result<usize> {
        Err(Error::NotImplemented)
    }

    fn pwritev(&mut self, iovs: &[&[u8]], opt: IOOption) -> Result<usize> {
        Err(Error::NotImplemented)
    }

    fn asyncify(self) -> Self::AsyncT {
        self
    }
}

impl BlockingIODriver {
    fn new(sched: &mut impl IOScheduler<BlockFileT = BlockingIOBlockFile>, max_io_depth: usize) -> BlockingIODriver {
        BlockingIODriver{
            max_io_depth,
        }
    }
}