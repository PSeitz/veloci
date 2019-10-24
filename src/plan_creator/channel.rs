use crate::search::{result::SearchFieldResult, FilterResult};
use crossbeam_channel::{self, unbounded};
use std::sync::Arc;

pub(crate) type PlanDataSender = crossbeam_channel::Sender<SearchFieldResult>;
pub(crate) type PlanDataReceiver = crossbeam_channel::Receiver<SearchFieldResult>;
pub(crate) type PlanDataFilterSender = crossbeam_channel::Sender<Arc<FilterResult>>;
pub(crate) type PlanDataFilterReceiver = crossbeam_channel::Receiver<Arc<FilterResult>>;

#[derive(Debug, Clone)]
pub struct PlanStepDataChannels {
    pub input_prev_steps: Vec<PlanDataReceiver>,
    pub sender_to_next_steps: PlanDataSender,
    pub filter_receiver: Option<PlanDataFilterReceiver>,
    pub num_receivers: u32,
    pub receiver_for_next_step: PlanDataReceiver, // used in plan_creation
    pub filter_channel: Option<FilterChannel>,    // Sending result as filter output to receivers
}

#[derive(Debug, Clone)]
pub struct FilterChannel {
    // input_prev_steps: Vec<PlanDataReceiver>,
    // sender_to_next_steps: PlanDataSender,
    pub filter_sender: PlanDataFilterSender,
    pub filter_receiver: PlanDataFilterReceiver,
    pub num_receivers: u32,
}

impl Default for FilterChannel {
    fn default() -> FilterChannel {
        let (tx, rx): (PlanDataFilterSender, PlanDataFilterReceiver) = unbounded();
        FilterChannel {
            num_receivers: 0,
            filter_sender: tx,
            filter_receiver: rx,
        }
    }
}

impl Default for PlanStepDataChannels {
    fn default() -> PlanStepDataChannels {
        let (tx, rx): (PlanDataSender, PlanDataReceiver) = unbounded();
        PlanStepDataChannels {
            num_receivers: 1,
            input_prev_steps: vec![],
            sender_to_next_steps: tx,
            receiver_for_next_step: rx,
            filter_receiver: None,
            filter_channel: None,
        }
    }
}

impl PlanStepDataChannels {
    // pub(crate) fn create_channel_from(
    //     num_receivers: u32,
    //     sender_to_next_steps: PlanDataSender,
    //     receiver_for_next_step: PlanDataReceiver,
    //     input_prev_steps: Vec<PlanDataReceiver>,
    // ) -> Self {
    //     PlanStepDataChannels {
    //         num_receivers,
    //         input_prev_steps,
    //         sender_to_next_steps,
    //         receiver_for_next_step,
    //         // output_sending_to_next_steps_as_filter: None,
    //         filter_receiver: None,
    //         filter_channel: None,
    //     }
    // }

    pub(crate) fn open_channel(num_receivers: u32, input_prev_steps: Vec<PlanDataReceiver>) -> Self {
        let (tx, rx): (PlanDataSender, PlanDataReceiver) = unbounded();
        PlanStepDataChannels {
            num_receivers,
            input_prev_steps,
            sender_to_next_steps: tx,
            receiver_for_next_step: rx,
            // output_sending_to_next_steps_as_filter: None,
            filter_receiver: None,
            filter_channel: None,
        }
    }
}
