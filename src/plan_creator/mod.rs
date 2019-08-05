use crate::{error::VelociError, persistence::Persistence};
use channel::PlanStepDataChannels;

use core::fmt::Debug;

pub mod channel;
pub mod execution_plan;
pub mod plan;
pub mod plan_steps;

pub trait PlanStepTrait: Debug + Sync + Send {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels;
    fn get_step_description(&self) -> String;
    // fn get_output(&self) -> PlanDataReceiver;
    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), VelociError>;
}
