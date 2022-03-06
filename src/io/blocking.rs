use std::convert::TryInto;
use std::pin::Pin;
use std::future::Future;
use std::io::ErrorKind;
use std::thread::JoinHandle;
use std::sync::{Condvar, Mutex, Arc, RwLock, MutexGuard};
use std::sync::atomic::{AtomicUsize, Ordering, AtomicU32};
use std::vec::Vec;

use std::os::unix::io::{IntoRawFd, RawFd};

#[cfg(target_os = "linux")]
use nix::sys::uio::{preadv, pwritev, IoVec};

use error::{Result, Error};
use futures::task::{ArcWake, Context, Waker, waker_ref, Poll};

use super::{IOScheduler, AsyncBlockFile, 
            BlockFile, IOOptions, IODriver,
            IODescription, IOType, IOScheduleResult, SliceVector};


enum BlockingIOFutureState {
    None,
    Waiting(Waker),
    Finished(Result<usize>),
}

const BLOCKING_CONTEXT_INVALID_IO_REF_COUNT: usize = std::usize::MAX;

struct BlockingIOFutureRefInfo {
    error: AtomicU32,
    fullfill_io_size: AtomicUsize,
    io_ref_count: AtomicUsize,
}

struct BlockingIOFuture {
    state: Arc<Mutex<BlockingIOFutureState>>,
    fd: RawFd,
}

struct BlockingIOContext {
    state: Arc<Mutex<BlockingIOFutureState>>,
    ref_info: Arc<BlockingIOFutureRefInfo>,
    fd: RawFd,
}

impl BlockingIOFuture {
    fn new(fd: RawFd) -> (BlockingIOFuture, BlockingIOContext) {
        let future = BlockingIOFuture {
            state: Arc::new(Mutex::new(BlockingIOFutureState::None)),
            fd,
        };

        let ctx = BlockingIOContext {
            state: future.state.clone(),
            ref_info: Arc::new(BlockingIOFutureRefInfo {
                error: AtomicU32::new(Error::Unknown as u32),
                fullfill_io_size: AtomicUsize::new(0),
                io_ref_count: AtomicUsize::new(1),
            }),
            fd: future.fd,
        };

        (future, ctx)
    }
}

impl Clone for BlockingIOContext {
    fn clone(&self) -> BlockingIOContext {
        self.ref_info.io_ref_count.fetch_add(1, Ordering::Relaxed);

        BlockingIOContext {
            state: self.state.clone(),
            ref_info: self.ref_info.clone(),
            fd: self.fd,
        }
    }
}

impl BlockingIOContext {
    fn finish(self, result: Result<usize>) {
        match result {
            Ok(size) => {
                self.ref_info.fullfill_io_size.fetch_add(size, Ordering::Relaxed);
            },

            Err(e) => {
                let current_errcode = self.ref_info.error.load(Ordering::Relaxed);
                if Error::from(current_errcode) != Error::Unknown {
                    self.ref_info.error.compare_exchange(current_errcode, e as u32, Ordering::Relaxed, Ordering::Relaxed);
                }
            }
        }

        let old_rc = self.ref_info.io_ref_count.fetch_sub(1, Ordering::Relaxed);
        // TODO(chadmai): maybe recover?
        assert!(old_rc != 0);

        if old_rc != 1 {
            // not the last io.
            return;
        }

        let errcode = self.ref_info.error.load(Ordering::Relaxed);
        let err = Error::from(errcode);
        let result = if err == Error::Unknown {
            Err(err)
        } else {
            Ok(self.ref_info.fullfill_io_size.load(Ordering::Relaxed))
        };

        // send result.
        // TODO(chadmai): deal with PoisonError
        let mut state = self.state.lock().unwrap();
        match *state {
            BlockingIOFutureState::None => {
                *state = BlockingIOFutureState::Finished(result);
            },
            BlockingIOFutureState::Waiting(waker) => {
                *state = BlockingIOFutureState::Finished(result);
                waker.wake();
            },
            BlockingIOFutureState::Finished(_) => unreachable!(),
        }
    }
}



impl Future for BlockingIOFuture {
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // TODO(chadmai): deal with PoisonError.
        let mut state = self.state.lock().unwrap();
        if let BlockingIOFutureState::Finished(result) = *state {
            Poll::Ready(result)

        } else {
            *state = BlockingIOFutureState::Waiting(cx.waker().clone());

            Poll::Pending
        }
    }
}


struct BlockingIODriver<IOSchedulerT>
where IOSchedulerT: IOScheduler<ContextT = BlockingIOContext> + Sync + Send + 'static {
    max_io_depth: usize,

    fid_counter: u32,

    submit_io_cond: Arc<Condvar>,
    control: Arc<Mutex<IOExecutorController<IOSchedulerT>>>,
    worker_join_handles: Vec<JoinHandle<()>>,
}

struct IOExecutorController<IOSchedulerT>
where IOSchedulerT: IOScheduler<ContextT = BlockingIOContext> + Sync + Send + 'static {
    running: bool,
    max_idle: usize,
    sched: Arc<RwLock<IOSchedulerT>>,
    idle_count: usize,
}

struct IORequestBuffer<'a> {
    io_desc: Vec::<IODescription<'a, BlockingIOContext>>,
    iovs: Vec::<IoVec<&'a [u8]>>,
    iovs_mut: Vec::<IoVec<&'a mut [u8]>>,
}

enum IOExecuteType { Read, Write, Unknown }

struct IOMergeSubmitter<'a> {
    buf: &'a mut IORequestBuffer<'a>,

    io_type: IOExecuteType,
    fd: RawFd, // TODO(chadmai): maybe use owner_id?
    relative_offset: usize,
    merge_start_index: usize,
}

impl<'a> IOMergeSubmitter<'a> {
    fn new(buf: &'a mut IORequestBuffer<'a>) -> Self {
        IOMergeSubmitter {
            buf,

            io_type: IOExecuteType::Unknown,
            fd: 0,
            relative_offset: 0,
            merge_start_index: 0,
        }
    }

    fn merge_read_iovs(&mut self, iovs: SliceVector<'a, &mut [u8]>) {
        for iov in iovs.iterator_valid_parts() {
            self.relative_offset += iov.len();
            self.buf.iovs_mut.push(IoVec::from_mut_slice(iov));
        }
    }

    fn merge_write_iovs(&mut self, iovs: SliceVector<'a, &[u8]>) {
        for iov in iovs.iterator_valid_parts() {
            self.relative_offset += iov.len();
            self.buf.iovs.push(IoVec::from_slice(iov));
        }
    }

    fn setup_first_io(&mut self, fd: RawFd, io_desc: &'a IODescription<BlockingIOContext>) {
        self.relative_offset = 0;
        self.fd = fd;

        self.io_type = match io_desc.io_type {
            IOType::Read(iovs) => {
                self.merge_read_iovs(iovs);

                IOExecuteType::Read
            },

            IOType::Write(iovs) => {
                self.merge_write_iovs(iovs);

                IOExecuteType::Write
            },

            IOType::Dummy => {
                self.merge_start_index += 1;

                IOExecuteType::Unknown
            },
        };
    }

    fn try_merge_following_io(&mut self, fd: RawFd, io_desc: &'a IODescription<BlockingIOContext>) -> bool {
        if self.fd != fd || self.relative_offset != io_desc.offset {
            return false;
        }

        match io_desc.io_type {
            IOType::Dummy => return true,
            IOType::Write(iovs) => {
                if let io_type = IOExecuteType::Write {
                    self.merge_write_iovs(iovs);
                    return true
                }
            },

            IOType::Read(iovs) => {
                if let io_type = IOExecuteType::Read {
                    self.merge_read_iovs(iovs);
                    return true
                }
            },
        }

        false
    }

    fn submit_io(&mut self, end_index: usize) {
        let first_desc = self.buf.io_desc[self.merge_start_index];

        let result = match self.io_type {
            IOExecuteType::Unknown => unreachable!(),

            IOExecuteType::Write => {
                #[cfg(target_os = "linux")]
                match first_desc.offset.try_into() {
                    Ok(offset) => {
                        match pwritev(self.fd, &self.buf.iovs, offset) {
                            Ok(read_size) => Ok(read_size),
                            Err(errno) => Err(Error::from(errno)),
                        }
                    },
                    Err(err) => {
                        eprintln!("invalid i/o offset {}. {}", first_desc.offset, err);

                        Err(Error::InvalidIOParameters)
                    }
                }

                #[cfg(not(target_os = "linux"))]
                Err(Error::NotImplemented)
            },

            IOExecuteType::Read => {
                #[cfg(target_os = "linux")]
                match first_desc.offset.try_into() {
                    Ok(offset) => {
                        match preadv(self.fd, &self.buf.iovs_mut, offset) {
                            Ok(read_size) => Ok(read_size),
                            Err(errno) => Err(Error::from(errno)),
                        }
                    },
                    Err(err) => {
                        eprintln!("invalid i/o offset {}. {}", first_desc.offset, err);

                        Err(Error::InvalidIOParameters)
                    }
                }

                #[cfg(not(target_os = "linux"))]
                Err(Error::NotImplemented)
            },
        };


        match result {
            Ok(mut size) => {
                let mut expected_size = 0;
                let done_size = size;
                for index in self.merge_start_index..end_index {
                    let mut io_desc = &self.buf.io_desc[index];

                    let mut io_length = match io_desc.io_type {
                        IOType::Read(iovs) => iovs.len(),
                        IOType::Write(iovs) => iovs.len(),

                        _ => continue,
                    };


                    match io_desc.context.take() {
                        None => continue,
                        Some(context) => {
                            expected_size += io_length;
                            
                            if io_length > size {
                                io_length = size;
                            }
                            size -= io_length;

                            context.finish(Ok(io_length))
                        },
                    }
                }

                if size > 0 {
                    println!("warning: system finished more bytes then the requested. expected={} actual={}", expected_size, done_size);
                }
            },

            Err(err) => {
                while self.merge_start_index < end_index {
                    let mut io_desc = &self.buf.io_desc[self.merge_start_index];

                    match io_desc.context.take() {
                        None => {},
                        Some(context) => context.finish(Err(err)),
                    }

                    self.merge_start_index += 1;
                }
            },
        }

        self.merge_start_index = end_index;
    }

    fn run(&'a mut self) {
        let mut index = 0;

        self.buf.iovs.clear();
        self.buf.iovs_mut.clear();
        for index in 0..self.buf.io_desc.len() {
            let io_desc = &self.buf.io_desc[index];

            let fd = match io_desc.context {
                None => 0,
                Some(context) => context.fd,
            };

            if self.merge_start_index == index { // leading i/o.
                self.setup_first_io(fd, io_desc);
                continue
            }

            if self.try_merge_following_io(fd, io_desc) {
                continue;
            }

            // cannot be merge. now execute existing merged i/o.
            self.submit_io(index);
            self.setup_first_io(fd, io_desc);
        }
    }
}

impl<IOSchedulerT> BlockingIODriver<IOSchedulerT>
where IOSchedulerT: IOScheduler<ContextT = BlockingIOContext> + Sync + Send + 'static {
    fn io_executor_run(control: Arc<Mutex<IOExecutorController<IOSchedulerT>>>, submit_io_cv: Arc<Condvar>) {
        let mut req_buf = IORequestBuffer {
            io_desc: Vec::<IODescription<BlockingIOContext>>::new(),
            iovs: Vec::<IoVec<&[u8]>>::new(),
            iovs_mut: Vec::<IoVec<&mut [u8]>>::new(),
        };

        let mut control_handle: Option<MutexGuard<IOExecutorController<IOSchedulerT>>> = None;

        loop {
            // TODO(chadmai): handle poison error.

            control_handle = match control_handle {
                None => Some(control.lock().unwrap()),

                Some(control) => {
                    if !control.running {
                        submit_io_cv.notify_one();
                        break;
                    }

                    req_buf.io_desc.clear();
                    let mut sched = control.sched.write().unwrap();
                    let schedule_result = sched.schedule(&mut req_buf.io_desc);

                    match schedule_result {
                        // no io request, should wait.
                        IOScheduleResult::None => {
                            drop(sched);

                            Some(
                                submit_io_cv.wait(control).unwrap()
                            )
                        },

                        // scheduler submits io.
                        IOScheduleResult::Submit => {
                            if sched.pending_io_count() > 1 { 
                                // more then one i/o are pending. wake more workers.
                                submit_io_cv.notify_one();
                            }

                            drop(sched);
                            drop(control);

                            if req_buf.io_desc.is_empty() {
                                println!("warning: scheduler schedules to Submit but no i/o given.");
                            } else {
                                IOMergeSubmitter::new(&mut req_buf).run();
                            }

                            None
                        },

                        // scheduler request for a delay.
                        IOScheduleResult::Delayed(duration) => { 
                            drop(sched);

                            let (control, _) = submit_io_cv.wait_timeout(control, duration).unwrap();

                            Some(control)
                        },
                    }
                },
            };
        }

        // TODO(chadmai): log exit.
    }

    pub fn new(sched: Arc<RwLock<IOSchedulerT>>, mut max_io_depth: usize) -> Self {
        if max_io_depth < 1 {
            max_io_depth = 1;
        }

        let mut driver = BlockingIODriver{
            fid_counter: 0,
            submit_io_cond: Arc::new(Condvar::new()),
            control: Arc::new(Mutex::new(IOExecutorController{
                sched,
                running: true,
                max_idle: max_io_depth,
                idle_count: 0,
            })),
            worker_join_handles: Vec::new(),
            max_io_depth,
        };

        driver.spawn_worker(driver.control.clone(), driver.submit_io_cond.clone(), max_io_depth);

        driver
    }

    fn spawn_worker(&mut self, control: Arc<Mutex<IOExecutorController<IOSchedulerT>>>, submit_io_cv: Arc<Condvar>, worker_count: usize) {
        for _ in 0..worker_count {
            let ctrl  = control.clone();
            let submit_cv = submit_io_cv.clone();

            let handle = std::thread::spawn(move || {
                Self::io_executor_run(ctrl, submit_cv);
            });

            self.worker_join_handles.push(handle);
        }
    }

    pub fn shutdown(&mut self) {
        // TODO(chadmai): deal with PoisonError.
        let mut control = self.control.lock().unwrap();
        control.running = false;
        self.submit_io_cond.notify_all();
        drop(control);

        for handle in self.worker_join_handles.drain(..) {
            let exit_res = handle.join();
            match exit_res {
                Ok(_) => {},
                Err(_panic_params) => {
                    // TODO(chadmai): log executor panic.
                    println!("blocking io executor panic!");
                }
            }
        }
    }
}

impl<IOSchedulerT> Drop for BlockingIODriver<IOSchedulerT> 
where IOSchedulerT: IOScheduler<ContextT = BlockingIOContext> + Sync + Send {
    fn drop(&mut self) {
        self.shutdown()
    }
}


impl<IOSchedulerT> IODriver for BlockingIODriver<IOSchedulerT> 
where IOSchedulerT: IOScheduler<ContextT = BlockingIOContext> + Sync + Send + 'static {
    type BlockFileT = BlockingIOBlockFile<IOSchedulerT>;

    fn open_block_file(&mut self, path: String) -> Result<Self::BlockFileT> {
        let file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(err) => {
                return Err(match err.kind() {
                    ErrorKind::NotFound => Error::FileNotFound,
                    ErrorKind::PermissionDenied => Error::PermissionDenied,
                    ErrorKind::InvalidInput => Error::InvalidIOParameters,
                    ErrorKind::AlreadyExists => Error::FileAlreadyExists,
                    // TODO(chadmai): log error
                    _ => Error::UnknownIOError,
                });
            }
        };

        let fid = self.fid_counter;
        self.fid_counter += 1;

        Ok(BlockingIOBlockFile::<IOSchedulerT>{
            fid,

            #[cfg(target_os = "linux")]
            fd: file.into_raw_fd(),

            pool_control: self.control.clone(),
            submit_io_cond: self.submit_io_cond.clone(),
        })
    }
}

struct BlockingIOWaiter {
    finish_cond: Condvar,
}

impl ArcWake for BlockingIOWaiter {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.finish_cond.notify_all()
    }
}

struct BlockingIOBlockFile<SchedulerT> 
where SchedulerT: IOScheduler<ContextT = BlockingIOContext> + Sync + Send + 'static {
    fid: u32,

    #[cfg(target_os = "linux")]
    fd: RawFd,

    //#[cfg(target_os = "macos")]
    //fds: std::collections::VecDeque<RawFd>,

    pool_control: Arc<Mutex<IOExecutorController<SchedulerT>>>,
    submit_io_cond: Arc<Condvar>,
}

impl<'a, SchedulerT> BlockingIOBlockFile<SchedulerT>
where SchedulerT: IOScheduler<ContextT = BlockingIOContext> + Sync + Send + 'static {
    fn send_io_to_executor(&self, io: IODescription<BlockingIOContext>) -> Result<()> {
        let control = self.pool_control.lock().unwrap();
        let mut sched = control.sched.write().unwrap();

        sched.submit(io)?;

        if control.idle_count > 0 {
            self.submit_io_cond.notify_one();
        }

        Ok(())
    }

    fn run_io(&self, fd: RawFd, offset: usize, io_type: IOType, opt: IOOptions) -> Result<usize> {
        let (future , context) = BlockingIOFuture::new(fd);

        // prepare io.
        // TODO(chadmai): deal with PoisonError.
        let state_lock = future.state.lock();
        let mut state = state_lock.unwrap();
        let waiter = Arc::new(BlockingIOWaiter{
            finish_cond: Condvar::new(),
        });

        let waker = waker_ref(&waiter);
        *state = BlockingIOFutureState::Waiting(waker.clone());

        self.send_io_to_executor(IODescription{
            io_type, offset, opt,
            owner_id: self.fid,
            context: Some(context),
        })?;

        // TODO(chadmai): deal with PoisonError.
        // block and wait for result.
        let state_lock = waiter.finish_cond.wait(state);
        let mut state = state_lock.unwrap();

        match *state {
            BlockingIOFutureState::Finished(result) => result,
            _ => unreachable!(),
        }
    }

    fn submit_io(&self, fd: RawFd, offset: usize, io_type: IOType, opt: IOOptions) -> BlockingIOFuture {
        let (future, context) = BlockingIOFuture::new(fd);

        if let Err(err) = self.send_io_to_executor(IODescription{
            io_type, offset, opt,
            owner_id: self.fid,
            context: Some(context),
        }) {
            let state = future.state.lock().unwrap();

            *state = BlockingIOFutureState::Finished(Err(err));
        }

        future
    }
}


impl<SchedulerT> BlockFile for BlockingIOBlockFile<SchedulerT> 
where SchedulerT: IOScheduler<ContextT = BlockingIOContext> + Sync + Send + 'static {
    type AsyncT = BlockingIOBlockFile<SchedulerT>;

    fn preadv(&self, offset: usize, iovs: &[&mut [u8]], opt: IOOptions) -> Result<usize> {
        self.run_io(self.fd, offset, IOType::Read(SliceVector::Simple(iovs)), opt)
    }

    fn pwritev(&mut self, offset: usize, iovs: &[&[u8]], opt: IOOptions) -> Result<usize> {
        self.run_io(self.fd, offset, IOType::Write(SliceVector::Simple(iovs)), opt)
    }

    fn asyncify(self) -> Self::AsyncT {
        self
    }
}

impl<'a, IOSchedulerT> AsyncBlockFile for BlockingIOBlockFile<IOSchedulerT>
where IOSchedulerT: IOScheduler<ContextT = BlockingIOContext> + Sync + Send + 'static {
    type IOFuture = BlockingIOFuture;

    fn preadv(&self, offset: usize, iovs: &[&mut [u8]], opt: IOOptions) -> Self::IOFuture {
        self.submit_io(self.fd, offset, IOType::Read(SliceVector::Simple(iovs)), opt)
    }

    fn pwritev(&mut self, offset: usize, iovs: &[&[u8]], opt: IOOptions) -> Self::IOFuture {
        self.submit_io(self.fd, offset, IOType::Write(SliceVector::Simple(iovs)), opt)
    }
}