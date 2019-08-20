use crate::{error::VelociError, persistence::Persistence};
use channel::PlanStepDataChannels;
use std::fmt::Display;

use core::fmt::Debug;

pub mod channel;
pub mod execution_plan;
pub mod plan;
pub mod plan_steps;

pub trait PlanStepTrait: Debug + Display + Sync + Send {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels;
    // fn get_output(&self) -> PlanDataReceiver;
    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), VelociError>;
}
