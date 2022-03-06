pub mod blocking;
pub mod sched;

use error::Result;
use std::future::Future;
use std::option::Option;
use std::time::Duration;

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

pub struct RangedSliceVector<'a, SliceT> {
    pub slices: &'a [SliceT],
    pub offset: usize,
    pub length: usize,
}

pub enum SliceVector<'a, SliceT> {
    Simple(&'a [SliceT]),
    Ranged(RangedSliceVector<'a, SliceT>),
}

pub struct SliceVectorIterator<'a, SliceT> {
    sv: &'a SliceVector<'a, SliceT>,
    current_offset: usize,
}

impl<'a> SliceVector<'a, &[u8]> {
    pub fn iterator_valid_parts(&self) -> SliceVectorIterator<'a, &[u8]> {
    }
    
    pub fn len(&self) -> usize {
        match *self {
            SliceVector::Simple(s) => s.len(),
            SliceVector::Ranged(rs) => rs.length,
        }
    }
}

impl<'a> SliceVector<'a, &mut [u8]> {
    pub fn iterator_valid_parts(&self) -> SliceVectorIterator<'a, &mut [u8]> {
    }

    pub fn len(&self) -> usize {
        match *self {
            SliceVector::Simple(s) => s.len(),
            SliceVector::Ranged(rs) => rs.length,
        }
    }
}

impl<'a> Iterator for SliceVectorIterator<'a, &[u8]> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
    }
}

impl<'a> Iterator for SliceVectorIterator<'a, &mut [u8]> {
    type Item = &'a mut [u8];

    fn next(&mut self) -> Option<Self::Item> {
    }
}

pub enum IOType<'a> {
    Dummy,
    Read(SliceVector<'a, &'a mut [u8]>), 
    Write(SliceVector<'a, &'a [u8]>), 
}

pub struct IODescription<'a, ContextT: Clone> {
    pub io_type: IOType<'a>,
    pub owner_id: u32,
    pub offset: usize,
    pub context: Option<ContextT>,

    pub opt: IOOptions,
}

#[derive(Debug, Copy, Clone)]
pub enum SchedulerType {
    Noop, Prioritized,
}

pub enum IOScheduleResult {
    None,
    Delayed(Duration),
    Submit,
}

pub trait IOScheduler {
    type ContextT: Clone;

    fn submit(&mut self, io: IODescription<Self::ContextT>) -> Result<()>;
    fn schedule(&mut self, submit_ios: &mut std::vec::Vec<IODescription<Self::ContextT>>) -> IOScheduleResult;
    fn pending_io_count(&self) -> usize;

    fn scheduler_type() -> SchedulerType;
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

#[derive(Debug, Copy, Clone)]
pub enum IODriverType {
    Blocking,
    AIO,
    IOUring,
}

pub trait IODriver {
    type BlockFileT: BlockFile;

    fn open_block_file(&mut self, path: String) -> Result<Self::BlockFileT>;
}
