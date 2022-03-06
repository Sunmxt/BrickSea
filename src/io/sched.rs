use error::{Result, Error};
use std::future::Future;
use std::collections::VecDeque;

use super::{BlockFile, IOScheduler, IODescription,
            IOScheduleResult, SchedulerType};

pub struct PrioriziedIOScheduler<ContextT> {
    requests: VecDeque<ContextT>,
}

impl<ContextT: Clone> IOScheduler for PrioriziedIOScheduler<ContextT> {
    type ContextT = ContextT;

    fn scheduler_type() -> SchedulerType {
        SchedulerType::Prioritized
    }

    fn submit(&mut self, io: IODescription<ContextT>) -> Result<()> {
        Err(Error::NotImplemented)
    }

    fn schedule(&mut self, submit_ios: &mut std::vec::Vec<IODescription<Self::ContextT>>) -> IOScheduleResult {
        IOScheduleResult::None
    }

    fn pending_io_count(&self) -> usize {
        0
    }
}


pub struct NoopIOScheduler<ContextT> {
    requests: VecDeque<ContextT>,
}

impl<ContextT: Clone> IOScheduler for NoopIOScheduler<ContextT> {
    type ContextT = ContextT;

    fn scheduler_type() -> SchedulerType {
        SchedulerType::Noop
    }

    fn submit(&mut self, io: IODescription<ContextT>) -> Result<()> {
        Err(Error::NotImplemented)
    }

    fn schedule(&mut self) -> IOScheduleResult {
        IOScheduleResult::None
    }
    
    fn pending_io_count(&self) -> usize {
        0
    }
}