use error::{Result, Error};
use std::future::Future;
use std::collections::VecDeque;

use super::{BlockFile, IOScheduler, IODescription,
            IOScheduleResult, SchedulerType};

pub struct PrioriziedIOScheduler<OwnerT> {
    requests: VecDeque<OwnerT>,
}

impl<OwnerT> IOScheduler for PrioriziedIOScheduler<OwnerT> {
    type OwnerT = OwnerT;

    fn scheduler_type() -> SchedulerType {
        SchedulerType::Prioritized
    }

    fn submit(&mut self, io: IODescription<Self::OwnerT>) -> Result<()> {
        Err(Error::NotImplemented)
    }

    fn schedule(&mut self) -> IOScheduleResult<Self::OwnerT> {
        IOScheduleResult::None
    }

    fn pending_io_count(&self) -> usize {
        0
    }
}


pub struct NoopIOScheduler<OwnerT> {
    requests: VecDeque<OwnerT>,
}

impl<OwnerT> IOScheduler for NoopIOScheduler<OwnerT> {
    type OwnerT = OwnerT;

    fn scheduler_type() -> SchedulerType {
        SchedulerType::Noop
    }

    fn submit(&mut self, io: IODescription<Self::OwnerT>) -> Result<()> {
        Err(Error::NotImplemented)
    }

    fn schedule(&mut self) -> IOScheduleResult<Self::OwnerT> {
        IOScheduleResult::None
    }
    
    fn pending_io_count(&self) -> usize {
        0
    }
}