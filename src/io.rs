pub mod blocking;
pub mod sched;

use error::Result;
use std::future::Future;
use std::option::Option;
use std::time::Duration;


#[derive(Debug, Copy, Clone)]
pub enum IODriverType {
    Blocking,
    AIO,
    IOUring,
}

pub trait IODriver<'a> {
    type BlockFileT: BlockFile;

    fn open_block_file(&'a mut self, path: String) -> Result<Self::BlockFileT>;
}

#[derive(Debug, Copy, Clone)]
pub enum SchedulerType {
    Noop, Prioritized,
}

pub enum IOScheduleResult<'a, OwnerT> {
    None,
    Delayed(Duration),
    Submit(IODescription<'a, OwnerT>),
}

pub trait IOScheduler {
    type OwnerT;

    fn submit(&mut self, io: IODescription<Self::OwnerT>) -> Result<()>;
    fn schedule(&mut self) -> IOScheduleResult<Self::OwnerT>;
    fn pending_io_count(&self) -> usize;

    fn scheduler_type() -> SchedulerType;
}

pub enum IOType<'a> {
    Dummy,
    Read(&'a [&'a mut [u8]]), 
    Write(&'a [&'a [u8]]),
}

#[derive(Debug, Copy, Clone)]
pub struct IOOptions {
    pub priority: u8,
    pub no_split: bool,
    pub no_merge: bool,
}

pub const DEFAULT_IO_OPTION: IOOptions = IOOptions {
    priority: 0,
    no_merge: false,
    no_split: false,
};

impl IOOptions {
    fn default() -> IOOptions {
        DEFAULT_IO_OPTION.clone()
    }

    fn new() -> IOOptions {
        Self::default()
    }

    fn with_priority(mut self, priority: u8) -> IOOptions {
        self.priority = priority;
        self
    }

    fn forbid_merge(mut self) -> IOOptions {
        self.no_merge = true;
        self
    }

    fn allow_merge(mut self) -> IOOptions {
        self.no_merge = false;
        self
    }

    fn forbid_split(mut self) -> IOOptions {
        self.no_split = true;
        self
    }

    fn allow_split(mut self) -> IOOptions {
        self.no_split = false;
        self
    }
}


pub struct IODescription<'a, OwnerT> {
    pub io_type: IOType<'a>,
    pub owner_id: u32,
    pub offset: usize,
    pub owner: Option<&'a OwnerT>,

    pub opt: IOOptions,
}

pub trait AsyncBlockFile {
    type IOFuture: Future<Output = Result<usize>>;

    fn pwritev(&mut self, offset: usize, iovs: &[&[u8]], opt: IOOptions) -> Self::IOFuture;
    fn preadv(&self, offset: usize, iovs: &[&mut [u8]], opt: IOOptions) -> Self::IOFuture;
}

pub trait BlockFile {
    type AsyncT: AsyncBlockFile;

    fn pwritev(&mut self, offset: usize, iovs: &[&[u8]], opt: IOOptions) -> Result<usize>;
    fn preadv(&self, offset: usize, iovs: &[&mut [u8]], opt: IOOptions) -> Result<usize>;

    fn asyncify(self) -> Self::AsyncT;
}