#![cfg_attr(feature = "cargo-clippy", allow(clippy::boxed_local))]
use crate::error::*;
use crate::persistence::Persistence;
use crate::persistence::*;
use crate::search::*;
use crate::util;
use crate::util::StringAdd;
use std::fmt::Debug;
use std::sync::Arc;

use fnv::FnvHashMap;
use fnv::FnvHashSet;

use std::boxed::Box;
use crate::steps::*;
use crate::steps::ToFieldPath;
use crossbeam_channel;
use crossbeam_channel::unbounded;

type PlanDataSender = crossbeam_channel::Sender<SearchFieldResult>;
type PlanDataReceiver = crossbeam_channel::Receiver<SearchFieldResult>;
// type PlanDataFilterSender = crossbeam_channel::Sender<Arc<SearchFieldResult>>;
// type PlanDataFilterReceiver = crossbeam_channel::Receiver<Arc<SearchFieldResult>>;
type PlanDataFilterSender = crossbeam_channel::Sender<Arc<FilterResult>>;
type PlanDataFilterReceiver = crossbeam_channel::Receiver<Arc<FilterResult>>;
type FieldRequestCache = FnvHashMap<RequestSearchPart, (usize, PlanStepFieldSearchToTokenIds)>;
type PlanStepId = usize;

#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq)]
pub struct PlanRequestSearchPart {
    pub request: RequestSearchPart,

    #[serde(default)]
    pub get_scores: bool,

    #[serde(default)]
    pub get_ids: bool,

    /// Internal data used for whyfound - read and highlight fields
    #[serde(skip_deserializing)]
    #[serde(default)]
    pub store_term_id_hits: bool,

    /// Internal data used for whyfound - highlight in original document
    #[serde(skip_deserializing)]
    #[serde(default)]
    pub store_term_texts: bool,

    //TODO MOVE TO RequestSearchPart?
    /// Also return the actual text
    #[serde(skip_serializing_if = "skip_false")]
    pub return_term: bool,

    //TODO MOVE TO RequestSearchPart?
    #[serde(skip_serializing_if = "skip_false")]
    pub return_term_lowercase: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Dependency {
    step_index: usize,
    depends_on: usize,
}

#[derive(Debug)]
/// Plan creates a plan based on a search::Request
pub struct Plan {
    pub steps: Vec<Box<dyn PlanStepTrait>>,
    dependencies: Vec<Dependency>,
    pub plan_result: Option<PlanDataReceiver>,
}

impl Default for Plan {
    fn default() -> Plan {
        Plan {
            steps: vec![],
            dependencies: vec![],
            plan_result: None,
        }
    }
}

impl Plan {
    fn add_dependency(&mut self, step_index: usize, depends_on: usize) {
        self.dependencies.push(Dependency { step_index, depends_on });
    }

    /// return the position in the array, which can be used as an id
    fn add_step(&mut self, step: Box<dyn PlanStepTrait>) -> usize {
        self.steps.push(step);
        self.steps.len() - 1
    }

    /// return the position in the array, which can be used as an id
    // fn get_step(&mut self, step_id: usize) -> &mut Box<dyn PlanStepTrait> {
    //     &mut self.steps[step_id]
    // }
    /// return the position in the array, which can be used as an id
    fn get_step_channel(&mut self, step_id: usize) -> &mut PlanStepDataChannels {
        self.steps[step_id].get_channel()
    }

    // fn get_dependencies(&self, step_index: usize) -> Vec<Dependency> {
    //     self.dependencies.iter().filter(|dep|dep.step_index == step_index).cloned().collect()
    // }

    pub fn get_ordered_steps(self) -> Vec<Vec<Box<dyn PlanStepTrait>>> {
        let mut ordered_steps = vec![];
        let mut remaining_steps: Vec<_> = self.steps.into_iter().enumerate().collect();
        let dep = self.dependencies;
        while !remaining_steps.is_empty() {
            let current_remaining_step_ids: Vec<_> = remaining_steps.iter().map(|el| el.0).collect();
            let steps_with_fullfilled_dependencies: Vec<_> = remaining_steps
                .drain_filter(|step_with_index| {
                    // let steps_dependencies = self.get_dependencies(step_with_index.0);
                    let steps_dependencies: Vec<Dependency> = dep.iter().filter(|dep| dep.step_index == step_with_index.0).cloned().collect();
                    let unfulfilled_dependencies: Vec<_> = steps_dependencies
                        .iter()
                        .filter(|dep| {
                            current_remaining_step_ids.iter().any(|step_id| *step_id == dep.depends_on) // check if depends_on is in current_remaining_step_ids
                        })
                        .collect();

                    unfulfilled_dependencies.is_empty()
                })
                .collect();

            if steps_with_fullfilled_dependencies.is_empty() {
                panic!("invalid plan created");
            }
            // ordered_steps.push(steps_with_fullfilled_dependencies.iter().map(|step_with_index|*step_with_index.1.clone()).collect());
            let vecco: Vec<_> = steps_with_fullfilled_dependencies.into_iter().map(|step_with_index| step_with_index.1).collect();
            ordered_steps.push(vecco);
        }
        ordered_steps
    }
}

#[derive(Debug, Clone)]
pub struct PlanStepDataChannels {
    input_prev_steps: Vec<PlanDataReceiver>,
    sender_to_next_steps: PlanDataSender,
    filter_receiver: Option<PlanDataFilterReceiver>,
    num_receivers: u32,
    receiver_for_next_step: PlanDataReceiver, // used in plan_creation
    filter_channel: Option<FilterChannel>,    // Sending result as filter output to receivers
}

#[derive(Debug, Clone)]
pub struct FilterChannel {
    // input_prev_steps: Vec<PlanDataReceiver>,
    // sender_to_next_steps: PlanDataSender,
    filter_sender: PlanDataFilterSender,
    filter_receiver: PlanDataFilterReceiver,
    num_receivers: u32,
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
    fn create_channel_from(num_receivers: u32, sender_to_next_steps: PlanDataSender, receiver_for_next_step: PlanDataReceiver, input_prev_steps: Vec<PlanDataReceiver>) -> Self {
        PlanStepDataChannels {
            num_receivers,
            input_prev_steps,
            sender_to_next_steps,
            receiver_for_next_step,
            // output_sending_to_next_steps_as_filter: None,
            filter_receiver: None,
            filter_channel: None,
        }
    }

    fn open_channel(num_receivers: u32, input_prev_steps: Vec<PlanDataReceiver>) -> Self {
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

pub trait PlanStepTrait: Debug + Sync + Send {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels;
    // fn get_output(&self) -> PlanDataReceiver;
    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), VelociError>;
}

#[derive(Clone, Debug)]
struct PlanStepFieldSearchToTokenIds {
    req: PlanRequestSearchPart,
    channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
struct ResolveTokenIdToAnchor {
    // req: PlanRequestSearchPart,
    request: RequestSearchPart,
    channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
struct ResolveTokenIdToTextId {
    request: RequestSearchPart,
    channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
struct ValueIdToParent {
    path: String,
    trace_info: String,
    channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
struct BoostPlanStepFromBoostRequest {
    req: RequestBoostPart,
    channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
struct BoostAnchorFromPhraseResults {
    channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
struct PlanStepPhrasePairToAnchorId {
    req: RequestPhraseBoost,
    channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
struct Union {
    ids_only: bool,
    channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
struct Intersect {
    ids_only: bool,
    channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
struct IntersectScoresWithIds {
    channel: PlanStepDataChannels,
}

impl PlanStepTrait for PlanStepFieldSearchToTokenIds {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels {
        &mut self.channel
    }

    fn execute_step(mut self: Box<Self>, persistence: &Persistence) -> Result<(), VelociError> {
        let field_result = search_field::get_term_ids_in_field(persistence, &mut self.req)?;
        send_result_to_channel(field_result, &self.channel)?;
        drop_channel(self.channel);
        Ok(())
    }
}

impl PlanStepTrait for ResolveTokenIdToAnchor {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels {
        &mut self.channel
    }

    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), VelociError> {
        let res = self.channel.input_prev_steps[0].recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?;
        let filter_res = if let Some(ref filter_receiver) = self.channel.filter_receiver {
            let search_field_result = filter_receiver.recv().map_err(|_| VelociError::PlanExecutionRecvFailedFilter)?;
            Some(search_field_result)
        } else {
            None
        };
        let field_result = resolve_token_to_anchor(persistence, &self.request, &filter_res, &res)?;
        send_result_to_channel(field_result, &self.channel)?;
        drop_channel(self.channel);
        Ok(())
    }
}
impl PlanStepTrait for ResolveTokenIdToTextId {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels {
        &mut self.channel
    }

    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), VelociError> {
        let mut field_result = self.channel.input_prev_steps[0].recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?;
        resolve_token_hits_to_text_id(persistence, &self.request, &mut field_result)?;
        send_result_to_channel(field_result, &self.channel)?;
        drop_channel(self.channel);
        Ok(())
    }
}

impl PlanStepTrait for ValueIdToParent {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels {
        &mut self.channel
    }

    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), VelociError> {
        send_result_to_channel(
            join_to_parent_with_score(
                persistence,
                &self.channel.input_prev_steps[0].recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?,
                &self.path,
                &self.trace_info,
            )?,
            &self.channel,
        )?;
        drop_channel(self.channel);
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct BoostToAnchor {
    path: String,
    trace_info: String,
    channel: PlanStepDataChannels,
    request: RequestSearchPart,
    boost: RequestBoostPart,
}
/// Token to text ids (TEXT_IDS)
/// text ids to parent valueid (VALUE_IDS)
/// ValueIds to boost values (VALUE_IDS, BOOST_VALUES)
/// value ids to anchor (ANCHOR_IDS, ANCHOR_IDS)
impl PlanStepTrait for BoostToAnchor {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels {
        &mut self.channel
    }

    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), VelociError> {
        let mut field_result = self.channel.input_prev_steps[0].recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?;
        let path = &self.request.path;

        dbg!(&"field_result**************************************************************************************");
        dbg!(&field_result);
        dbg!(&"field_result**************************************************************************************");
        //now finding steps to boost

        // let mut path_to_walk_down = vec![];

        let (walk_down, walk_up) = steps_between_field_paths(&path, &self.boost.path);

        //valueid to parent

        let token_to_text_id = persistence.get_valueid_to_parent(path.add(TEXTINDEX).add(TOKENS_TO_TEXT_ID))?;

        for step in &walk_down {
            let step = step.to_string().add(VALUE_ID_TO_PARENT);
            // dbg!(&field_result);
            field_result = join_to_parent_ids(persistence, &field_result, &step, "_trace_time_info: &str")?;
        }

        // dbg!(field_result);
        // let mut field_result = join_to_parent_ids(persistence, &field_result, "path: &str", "_trace_time_info: &str")?;


        //valueid to parent


        // let boost_field_path = (&self.boost.path).to_field_path();
        // let search_field_path = (&self.path).to_field_path();
        // let mut steps:Vec<_> = self.path.split(".").collect();
        // while !self.boost.path.path.contains(&steps.join(".")) {
        //     steps.pop();
        //     path_to_walk_down.push(steps.join("."));
        // }

        // dbg!(path_to_walk_down);
        dbg!(self.boost);
        dbg!(path);
        panic!("{:?}", walk_up);

        resolve_token_hits_to_text_id_ids_only(persistence, &self.request, &mut field_result)?;
        // let mut field_result = join_to_parent_ids(persistence, &field_result, "path: &str", "_trace_time_info: &str");
        panic!("{:?}", field_result.hits_ids);
        // send_result_to_channel(
        //     join_to_parent_with_score(
        //         persistence,
        //         &self.channel.input_prev_steps[0].recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?,
        //         &self.path,
        //         &self.trace_info,
        //     )?,
        //     &self.channel,
        // )?;
        drop_channel(self.channel);
        Ok(())
    }
}

impl PlanStepTrait for BoostPlanStepFromBoostRequest {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels {
        &mut self.channel
    }

    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), VelociError> {
        let mut input = self.channel.input_prev_steps[0].recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?;
        add_boost(persistence, &self.req, &mut input)?;
        send_result_to_channel(input, &self.channel)?;
        drop_channel(self.channel);
        Ok(())
    }
}

use itertools::Itertools;
use ordered_float::OrderedFloat;
fn sort_and_group_boosts_by_phrase_terms(mut boosts: Vec<SearchFieldResult>) -> Vec<SearchFieldResult> {
    info_time!("sort_and_group_boosts_by_phrase_terms");
    boosts.sort_unstable_by_key(|res| {
        let phrase_req = res.phrase_boost.as_ref().expect("could not find phrase_boost");
        (phrase_req.search1.terms[0].to_string(), phrase_req.search2.terms[0].to_string())
    });

    let mut new_vec = vec![];
    for (phrase, group) in &boosts.iter().group_by(|res| {
        let phrase_req = res.phrase_boost.as_ref().unwrap();
        (phrase_req.search1.terms[0].to_string(), phrase_req.search2.terms[0].to_string())
    }) {
        debug_time!("kmerge anchors for phrase {:?}", phrase);
        let boosts_iter: Vec<_> = group.map(|el| el.hits_ids.iter()).collect();
        let mut mergo: Vec<u32> = boosts_iter.into_iter().kmerge().cloned().collect();
        mergo.dedup();
        new_vec.push(SearchFieldResult {
            hits_ids: mergo,
            ..Default::default()
        });
    }

    new_vec
}

impl PlanStepTrait for BoostAnchorFromPhraseResults {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels {
        &mut self.channel
    }

    fn execute_step(self: Box<Self>, _persistence: &Persistence) -> Result<(), VelociError> {
        let input = self.channel.input_prev_steps[0].recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?;
        let boosts = get_data(&self.channel.input_prev_steps[1..])?;
        let mut boosts = sort_and_group_boosts_by_phrase_terms(boosts);
        //Set boost for phrases for the next step
        for boost_res in &mut boosts {
            boost_res.request.boost = Some(OrderedFloat(5.0));
        }

        send_result_to_channel(boost_hits_ids_vec_multi(input, &mut boosts), &self.channel)?;
        drop_channel(self.channel);
        Ok(())
    }
}
impl PlanStepTrait for PlanStepPhrasePairToAnchorId {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels {
        &mut self.channel
    }

    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), VelociError> {
        let res1 = self.channel.input_prev_steps[0].recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?;
        let res2 = self.channel.input_prev_steps[1].recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?;
        assert!(self.req.search1.path == self.req.search2.path);
        let mut res = get_anchor_for_phrases_in_search_results(persistence, &self.req.search1.path, &res1, &res2)?;
        res.phrase_boost = Some(self.req.clone());
        send_result_to_channel(res, &self.channel)?;
        drop_channel(self.channel);
        Ok(())
    }
}

impl PlanStepTrait for Union {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels {
        &mut self.channel
    }

    fn execute_step(self: Box<Self>, _persistence: &Persistence) -> Result<(), VelociError> {
        let res = if self.ids_only {
            union_hits_ids(get_data(&self.channel.input_prev_steps.clone())?)
        } else {
            union_hits_score(get_data(&self.channel.input_prev_steps.clone())?)
        };
        send_result_to_channel(res, &self.channel)?;
        // send_result_to_channel(union_hits_score(get_data(&self.channel.clone().input_prev_steps)?), &self.channel)?;
        drop(self.channel.sender_to_next_steps);
        Ok(())
    }
}

impl PlanStepTrait for Intersect {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels {
        &mut self.channel
    }

    fn execute_step(self: Box<Self>, _persistence: &Persistence) -> Result<(), VelociError> {
        let res = if self.ids_only {
            intersect_hits_ids(get_data(&self.channel.input_prev_steps.clone())?)
        } else {
            intersect_hits_score(get_data(&self.channel.input_prev_steps.clone())?)
        };
        send_result_to_channel(res, &self.channel)?;
        drop(self.channel.sender_to_next_steps);
        Ok(())
    }
}
impl PlanStepTrait for IntersectScoresWithIds {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels {
        &mut self.channel
    }

    fn execute_step(self: Box<Self>, _persistence: &Persistence) -> Result<(), VelociError> {
        info_time!("IntersectScoresWithIds");
        let scores_res = self.channel.input_prev_steps[0].recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?;
        let ids_res = self.channel.input_prev_steps[1].recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?;

        let res = intersect_score_hits_with_ids(scores_res, ids_res);
        send_result_to_channel(res, &self.channel)?;
        drop(self.channel.sender_to_next_steps);
        Ok(())
    }
}

fn get_data(input_prev_steps: &[PlanDataReceiver]) -> Result<Vec<SearchFieldResult>, VelociError> {
    let mut dat = vec![];
    for el in input_prev_steps {
        dat.push(el.recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?);
    }
    Ok(dat)
}

fn drop_channel(channel: PlanStepDataChannels) {
    drop(channel.sender_to_next_steps);
    for el in channel.input_prev_steps {
        drop(el);
    }
    if let Some(filter_channel) = channel.filter_channel {
        drop(filter_channel);
    }
}

fn send_result_to_channel(field_result: SearchFieldResult, channel: &PlanStepDataChannels) -> Result<(), VelociError> {
    //Send SearchFieldResult as Filter
    if let Some(ref filter_channel) = channel.filter_channel {
        debug_time!("convert filter");
        let res = Arc::new(FilterResult::from_result(&field_result.hits_ids));
        // let res = Arc::new(field_result.clone());
        for _ in 0..filter_channel.num_receivers {
            filter_channel.filter_sender.send(Arc::clone(&res)).map_err(|_| VelociError::PlanExecutionSendFailed)?;;
        }
    }
    let mut data = vec![field_result]; //splat data to vec, first one is free
    for _ in 0..channel.num_receivers - 1 {
        let clone = data[0].clone();
        data.push(clone);
    }
    for el in data {
        channel.sender_to_next_steps.send(el).map_err(|_| VelociError::PlanExecutionSendFailed)?;;
    }
    Ok(())
}

fn get_all_field_request_parts_and_propagate_settings<'a>(header_request: &Request, request: &'a mut Request, map: &mut FnvHashSet<&'a mut RequestSearchPart>) {
    request.explain |= header_request.explain;
    if let Some(phrase_boosts) = request.phrase_boosts.as_mut() {
        for el in phrase_boosts {
            //propagate explain
            el.search1.explain |= header_request.explain;
            el.search2.explain |= header_request.explain;
            map.insert(&mut el.search1);
            map.insert(&mut el.search2);
        }
    }

    if let Some(and_or) = request.and.as_mut().or(request.or.as_mut()) {
        for el in and_or {
            get_all_field_request_parts_and_propagate_settings(header_request, el, map);
        }
    }
    if let Some(search) = request.search.as_mut() {
        //propagate explain
        search.explain |= header_request.explain;
        map.insert(search);
    }
}

/// add first we collect all searches on the fields (virtually the leaf nodes in the execution plan) to avoid duplicate searches. This could also be done a tree level.
fn collect_all_field_request_into_cache(request: &mut Request, field_search_cache: &mut FieldRequestCache, plan: &mut Plan, ids_only: bool) {
    let mut field_requests = FnvHashSet::default();
    get_all_field_request_parts_and_propagate_settings(&request.clone(), request, &mut field_requests);
    for request_part in field_requests {
        // There could be the same query for filter and normal search, then we load scores and ids => TODO ADD TEST PLZ
        if let Some((_, field_search)) = field_search_cache.get_mut(&request_part) {
            field_search.req.get_ids |= ids_only;
            field_search.req.get_scores |= !ids_only;
            continue; // else doesn't work because field_search borrow scope expands else
        }
        let plan_request_part = PlanRequestSearchPart {
            request: request_part.clone(),
            get_scores: !ids_only,
            get_ids: ids_only,
            ..Default::default()
        };
        let field_search = PlanStepFieldSearchToTokenIds {
            req: plan_request_part,
            channel: PlanStepDataChannels::open_channel(0, vec![]),
        };
        let step_id = plan.add_step(Box::new(field_search.clone())); // this is actually only a placeholder in the plan, will replaced with the data from the field_search_cache after plan creation

        field_search_cache.insert(request_part.clone(), (step_id, field_search));
    }
}

pub fn plan_creator(mut request: Request, plan: &mut Plan) {
    let request_header = request.clone();
    let mut field_search_cache = FnvHashMap::default();
    collect_all_field_request_into_cache(&mut request, &mut field_search_cache, plan, false);

    let filter_final_step_id: Option<PlanStepId> = if let Some(filter) = request.filter.as_mut() {
        collect_all_field_request_into_cache(filter, &mut field_search_cache, plan, true);
        let final_output_filter = plan_creator_2(true, true, None, &request_header, &*filter, vec![], plan, None, None, &mut field_search_cache);
        Some(final_output_filter)
    } else {
        None
    };

    let boost = request.boost.clone();

    let mut final_step_id = {
        plan_creator_2(
            false,
            false,
            filter_final_step_id,
            &request_header,
            &request,
            boost.unwrap_or_else(|| vec![]),
            plan,
            None,
            filter_final_step_id,
            &mut field_search_cache,
        )
    };
    // Add intersect step the search result with the filter
    if let Some(filter_final_step_id) = filter_final_step_id {
        let final_step_channel = plan.get_step_channel(final_step_id).clone();
        let filter_receiver = plan.get_step_channel(filter_final_step_id).receiver_for_next_step.clone();
        let channel = PlanStepDataChannels::open_channel(1, vec![final_step_channel.receiver_for_next_step.clone(), filter_receiver]);
        let step = IntersectScoresWithIds { channel: channel.clone() };
        // step.get_channel().input_prev_steps = vec![final_output.0, filter_data_output.0];
        let id_step = plan.add_step(Box::new(step.clone()));
        plan.add_dependency(id_step, filter_final_step_id);
        plan.add_dependency(id_step, final_step_id);
        final_step_id = id_step;
    }

    if let Some(phrase_boosts) = request.phrase_boosts {
        final_step_id = add_phrase_boost_plan_steps(phrase_boosts, &mut field_search_cache, final_step_id, plan);
    }
    //update the field search steps in the plan from the field_search_cache
    for (_k, v) in field_search_cache.drain() {
        plan.steps[v.0] = Box::new(v.1);
    }
    plan.plan_result = Some(plan.get_step_channel(final_step_id).receiver_for_next_step.clone());
    // final_output.0
}

fn add_phrase_boost_plan_steps(
    phrase_boosts: Vec<RequestPhraseBoost>,
    field_search_cache: &mut FieldRequestCache,
    // search_output: PlanStepReceiverAndId,
    search_output_step: PlanStepId,
    plan: &mut Plan,
) -> PlanStepId {
    let mut phrase_outputs = vec![];
    for boost in phrase_boosts {
        let mut get_field_search = |req: &RequestSearchPart| -> (PlanDataReceiver, usize) {
            let field_search1 = field_search_cache
                .get_mut(req)
                .unwrap_or_else(|| panic!("PlanCreator: Could not find  request in field_search_cache {:?}", req));
            field_search1.1.req.get_ids = true;
            field_search1.1.channel.num_receivers += 1;
            let field_rx = field_search1.1.channel.receiver_for_next_step.clone();
            (field_rx, field_search1.0)
        };

        let (field_rx1, plan_id1) = get_field_search(&boost.search1);
        let (field_rx2, plan_id2) = get_field_search(&boost.search2);
        let channel = PlanStepDataChannels::open_channel(1, vec![field_rx1, field_rx2]);

        let step = PlanStepPhrasePairToAnchorId {
            req: boost.clone(),
            channel: channel.clone(),
        };

        phrase_outputs.push(channel.clone());
        let id_step = plan.add_step(Box::new(step));
        plan.add_dependency(id_step, plan_id1);
        plan.add_dependency(id_step, plan_id2);
    }

    //first is search result channel, rest are boost results
    let mut vecco = vec![plan.get_step_channel(search_output_step).receiver_for_next_step.clone()];
    for channel in phrase_outputs {
        vecco.push(channel.receiver_for_next_step);
    }

    //boost all results with phrase results
    let channel = PlanStepDataChannels::open_channel(1, vecco);
    let step = BoostAnchorFromPhraseResults { channel };
    let id_step = plan.add_step(Box::new(step));
    plan.add_dependency(id_step, search_output_step);
    (id_step)
}
fn merge_vec(boost: &[RequestBoostPart], opt: &Option<Vec<RequestBoostPart>>) -> Vec<RequestBoostPart> {
    let mut boost = boost.to_owned();
    if let Some(boosto) = opt.as_ref() {
        boost.extend_from_slice(&boosto);
    }
    // boost.extend_from_slice(&opt.as_ref().unwrap_or_else(||vec![]));
    boost
}

fn plan_creator_2(
    is_filter: bool,
    is_filter_channel: bool,
    filter_channel_step: Option<usize>, //  this channel is used to receive the result from the filter step
    request_header: &Request,
    request: &Request,
    mut boost: Vec<RequestBoostPart>,
    plan: &mut Plan,
    parent_step_dependecy: Option<usize>,
    depends_on_step: Option<usize>,
    field_search_cache: &mut FieldRequestCache,
) -> PlanStepId {
    // request.explain |= request_header.explain;
    if let Some(or) = request.or.as_ref() {
        let mut channel = PlanStepDataChannels::default();
        if let Some(step_id) = filter_channel_step {
            plan.get_step_channel(step_id).filter_channel.as_mut().unwrap().num_receivers += 1;
            channel.filter_receiver = Some(plan.get_step_channel(step_id).filter_channel.as_mut().unwrap().filter_receiver.clone());
        }
        if is_filter_channel {
            channel.filter_channel = Some(FilterChannel::default());
        }
        let step = Union { ids_only: is_filter, channel };
        let step_id = plan.add_step(Box::new(step.clone()));
        let result_channels_from_prev_steps = or
            .iter()
            .map(|x| {
                // x.explain = request_header.explain;
                let boost = merge_vec(&boost, &x.boost);
                let step_id = plan_creator_2(
                    is_filter,
                    false,
                    filter_channel_step,
                    request_header,
                    x,
                    boost,
                    plan,
                    Some(step_id),
                    depends_on_step,
                    field_search_cache,
                );
                plan.get_step_channel(step_id).receiver_for_next_step.clone()
            })
            .collect();
        plan.get_step_channel(step_id).input_prev_steps = result_channels_from_prev_steps;

        if let Some(parent_step_dependecy) = parent_step_dependecy {
            plan.add_dependency(parent_step_dependecy, step_id);
        }
        if let Some(depends_on_step) = depends_on_step {
            plan.add_dependency(step_id, depends_on_step);
        }

        (step_id)
    } else if let Some(ands) = request.and.as_ref() {
        let mut channel = PlanStepDataChannels::default();
        if let Some(step_id) = filter_channel_step {
            plan.get_step_channel(step_id).filter_channel.as_mut().unwrap().num_receivers += 1;
            channel.filter_receiver = Some(plan.get_step_channel(step_id).filter_channel.as_mut().unwrap().filter_receiver.clone());
        }
        if is_filter_channel {
            channel.filter_channel = Some(FilterChannel::default());
        }
        let step = Intersect { ids_only: is_filter, channel };
        let step_id = plan.add_step(Box::new(step.clone()));
        let result_channels_from_prev_steps = ands
            .iter()
            .map(|x| {
                // x.explain = request_header.explain;
                let boost = merge_vec(&boost, &x.boost);
                let step_id = plan_creator_2(
                    is_filter,
                    false,
                    filter_channel_step,
                    request_header,
                    x,
                    boost,
                    plan,
                    Some(step_id),
                    depends_on_step,
                    field_search_cache,
                );
                plan.get_step_channel(step_id).receiver_for_next_step.clone()
            })
            .collect();
        plan.get_step_channel(step_id).input_prev_steps = result_channels_from_prev_steps;

        if let Some(parent_step_dependecy) = parent_step_dependecy {
            plan.add_dependency(parent_step_dependecy, step_id);
        }
        if let Some(depends_on_step) = depends_on_step {
            plan.add_dependency(step_id, depends_on_step);
        }

        (step_id)
    } else if let Some(part) = request.search.as_ref() {
        // TODO Tokenize query according to field
        // part.terms = part.terms.iter().map(|el| util::normalize_text(el)).collect::<Vec<_>>();
        plan_creator_search_part(
            is_filter_channel,
            filter_channel_step,
            part,
            request,
            &mut boost,
            plan,
            parent_step_dependecy,
            depends_on_step,
            field_search_cache,
        )
    } else {
        //TODO HANDLE SUGGEST
        //TODO ADD ERROR
        // plan_creator_search_part(request.search.as_ref().unwrap().clone(), request)
        panic!("missing 'and' 'or' 'search' in request - suggest not yet handled in search api {:?}", request);
    }
}

fn plan_creator_search_part(
    is_filter_channel: bool,
    filter_channel_step: Option<usize>,
    request_part: &RequestSearchPart,
    request: &Request,
    boosts: &mut Vec<RequestBoostPart>,
    plan: &mut Plan,
    parent_step_dependecy: Option<usize>,
    depends_on_step: Option<usize>,
    field_search_cache: &mut FieldRequestCache,
) -> PlanStepId {
    let paths = util::get_steps_to_anchor(&request_part.path);
    // let (mut field_tx, mut field_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
    // let fast_field = boosts.is_empty() && !request_part.snippet.unwrap_or(false); // fast_field disabled for boosting or _highlighting_ currently
    let fast_field = !request_part.snippet.unwrap_or(false); // fast_field disabled for boosting or _highlighting_ currently
    let store_term_id_hits = request.why_found || request.text_locality;
    // let plan_request_part = PlanRequestSearchPart{request:request_part, get_scores: true, store_term_id_hits, store_term_texts: request.why_found, ..Default::default()};

    let (id, field_search_step) = field_search_cache
        .get_mut(&request_part)
        .unwrap_or_else(|| panic!("PlanCreator: Could not find  request in field_search_cache {:?}", request_part));
    field_search_step.req.store_term_texts |= request.why_found;
    field_search_step.req.store_term_id_hits |= store_term_id_hits;
    field_search_step.channel.num_receivers += 1;
    let field_rx = field_search_step.channel.receiver_for_next_step.clone();

    if fast_field {

        // check boost on 1:n fields, boost on anchor is done seperately
        if let Some(pos) = request_part.path.rfind("[]") {
            let end_obj = &request_part.path[..pos];
            //find where boost matches last path
            let boosto:Vec<&RequestBoostPart> = boosts.iter().flat_map(|el|{
                if let Some(pos) = el.path.rfind("[]") {
                    if &el.path[..pos] == end_obj {
                        return Some(el);
                    }
                }
                None
            }).collect();
            if !boosto.is_empty() {
                assert!(boosto.len() == 1);

                //              RESOLVE TO ANCHOR  (ANCHOR, SCORE) --------------------------------------------------------------------------------------------------------------------
                //              /                                                                                                                                                      \
                // SEARCH FIELD                                                                                                                                                         APPLY BOOST
                //              \                                                                                                                                                      /
                //              Token to text ids (TEXT_IDS) -> text ids to parent valueid (VALUE_IDS) -> ValueIds to boost values (VALUE_IDS, BOOST_VALUES) ->   value ids to anchor (ANCHOR_IDS, ANCHOR_IDS)

                //+1 for boost
                field_search_step.channel.num_receivers += 1;

                // This is the normal case, resolve field directly to anchor ids
                let mut channel = PlanStepDataChannels::open_channel(1, vec![field_rx.clone()]);
                if let Some(step_id) = filter_channel_step {
                    plan.get_step_channel(step_id).filter_channel.as_mut().unwrap().num_receivers += 1;
                    channel.filter_receiver = Some(plan.get_step_channel(step_id).filter_channel.as_mut().unwrap().filter_receiver.clone());
                }
                if is_filter_channel {
                    channel.filter_channel = Some(FilterChannel::default());
                }
                let token_to_anchor_step = ResolveTokenIdToAnchor {
                    request: request_part.clone(),
                    channel,
                };
                let token_to_anchor_step_id = plan.add_step(Box::new(token_to_anchor_step));
                plan.add_dependency(token_to_anchor_step_id, *id);

                if let Some(parent_step_dependecy) = parent_step_dependecy {
                    plan.add_dependency(parent_step_dependecy, *id);
                    plan.add_dependency(parent_step_dependecy, token_to_anchor_step_id);
                }
                if let Some(depends_on_step) = depends_on_step {
                    plan.add_dependency(token_to_anchor_step_id, depends_on_step);
                }


                let boost_channel = PlanStepDataChannels::open_channel(1, vec![field_rx]);
                let step = Box::new(BoostToAnchor {
                    path: paths.last().unwrap().add(VALUE_ID_TO_PARENT),
                    trace_info: "term hits hit to column".to_string(),
                    channel: boost_channel.clone(),
                    request: request_part.clone(),
                    boost: boosto[0].clone()
                });
                let step_id = plan.add_step(step);
                if let Some(parent_step_dependecy) = parent_step_dependecy {
                    plan.add_dependency(parent_step_dependecy, step_id);
                }


                //get boost scores and resolve to anchor
                // step_id
                // let mut step_id = add_step();

                return token_to_anchor_step_id;

            }
        }


        // This is the normal case, resolve field directly to anchor ids
        let mut channel = PlanStepDataChannels::open_channel(1, vec![field_rx]);
        if let Some(step_id) = filter_channel_step {
            plan.get_step_channel(step_id).filter_channel.as_mut().unwrap().num_receivers += 1;
            channel.filter_receiver = Some(plan.get_step_channel(step_id).filter_channel.as_mut().unwrap().filter_receiver.clone());
        }
        if is_filter_channel {
            channel.filter_channel = Some(FilterChannel::default());
        }
        let token_to_anchor_step = ResolveTokenIdToAnchor {
            request: request_part.clone(),
            channel,
        };
        let id1 = plan.add_step(Box::new(token_to_anchor_step));
        plan.add_dependency(id1, *id);

        if let Some(parent_step_dependecy) = parent_step_dependecy {
            plan.add_dependency(parent_step_dependecy, *id);
            plan.add_dependency(parent_step_dependecy, id1);
        }
        if let Some(depends_on_step) = depends_on_step {
            plan.add_dependency(id1, depends_on_step);
        }
        (id1)
    } else {
        // This is a special case, where boosts indices on fields are used.
        let mut add_step = |step: Box<dyn PlanStepTrait>| -> usize {
            let step_id = plan.add_step(step);
            if let Some(parent_step_dependecy) = parent_step_dependecy {
                plan.add_dependency(parent_step_dependecy, step_id);
            }
            step_id
        };

        let channel = PlanStepDataChannels::open_channel(1, vec![field_rx]);
        add_step(Box::new(ResolveTokenIdToTextId {
            request: request_part.clone(),
            channel: channel.clone(),
        }));

        let (mut tx, mut rx): (PlanDataSender, PlanDataReceiver) = unbounded();
        let mut channel = PlanStepDataChannels::create_channel_from(1, tx.clone(), rx.clone(), vec![channel.receiver_for_next_step.clone()]);
        let mut step_id = add_step(Box::new(ValueIdToParent {
            path: paths.last().unwrap().add(VALUE_ID_TO_PARENT),
            trace_info: "term hits hit to column".to_string(),
            channel: channel.clone(),
        }));

        for i in (0..paths.len() - 1).rev() {
            boosts.retain(|boost| {
                let apply_boost = boost.path.starts_with(&paths[i]);
                if apply_boost {
                    let (next_tx, next_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
                    tx = next_tx;
                    channel = PlanStepDataChannels::create_channel_from(1, tx.clone(), next_rx.clone(), vec![rx.clone()]);
                    step_id = add_step(Box::new(BoostPlanStepFromBoostRequest {
                        req: boost.clone(),
                        channel: channel.clone(),
                    }));

                    debug!("PlanCreator Step {:?}", boost);

                    rx = next_rx;
                }
                !apply_boost
            });

            let (next_tx, next_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
            tx = next_tx;
            channel = PlanStepDataChannels::create_channel_from(1, tx.clone(), next_rx.clone(), vec![rx.clone()]);
            step_id = add_step(Box::new(ValueIdToParent {
                path: paths[i].add(VALUE_ID_TO_PARENT),
                trace_info: "Joining to anchor".to_string(),
                channel: channel.clone(),
            }));

            debug!("PlanCreator Step {}", paths[i].add(VALUE_ID_TO_PARENT));

            rx = next_rx;
        }

        // Handling boost from anchor to value - ignoring 1:N!
        for boost in boosts.iter().filter(|el| !el.path.contains("[]")) {
            let (next_tx, next_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
            tx = next_tx;
            channel = PlanStepDataChannels::create_channel_from(1, tx.clone(), next_rx.clone(), vec![rx.clone()]);
            let id = add_step(Box::new(BoostPlanStepFromBoostRequest {
                req: boost.clone(),
                channel: channel.clone(),
            }));
            debug!("PlanCreator Step {:?}", boost);
            rx = next_rx;
            step_id = id;
        }
        // for step in steps {
        //     let id = plan.add_step(step);
        //     if let Some(parent_step_dependecy) = parent_step_dependecy {
        //         plan.add_dependency(parent_step_dependecy, id);
        //     }
        // }
        (step_id)
    }
}

use rayon::prelude::*;

//
// pub fn execute_steps(steps: Vec<PlanStepType>, persistence: &Persistence) -> Result<(), VelociError> {
//     let r: Result<Vec<_>, VelociError> = steps.into_par_iter().map(|step| step.execute_step(persistence)).collect();

//     if r.is_err() {
//         Err(r.unwrap_err())
//     } else {
//         Ok(())
//     }

//     // for step in steps {
//     //     step.execute_step(persistence)?;
//     // }
//     // Ok(())

//     // let err = steps.par_iter().map(|step|{
//     //     let res = execute_step(step.clone(), persistence);

//     //     match res {
//     //         Ok(()) => Some(1),
//     //         Err(err)=> None,
//     //     }

//     // }).while_some().collect::<Vec<_>>();

//     // err

//     // steps.par_iter().map(|step|{
//     //     execute_step(step.clone(), persistence)?;
//     // });

//     // for step in steps {
//     //     execute_step(step, persistence)?;
//     // }
//     // Ok(())
//     // Ok(hits)
// }

pub fn execute_steps(steps: Vec<Box<dyn PlanStepTrait>>, persistence: &Persistence) -> Result<(), VelociError> {
    let r: Result<Vec<_>, VelociError> = steps.into_par_iter().map(|step: Box<dyn PlanStepTrait>| step.execute_step(persistence)).collect();

    if r.is_err() {
        Err(r.unwrap_err())
    } else {
        Ok(())
    }

    // for step in steps {
    //     step.execute_step(persistence)?;
    // }
    // Ok(())

    // let err = steps.par_iter().map(|step|{
    //     let res = execute_step(step.clone(), persistence);

    //     match res {
    //         Ok(()) => Some(1),
    //         Err(err)=> None,
    //     }

    // }).while_some().collect::<Vec<_>>();

    // err

    // steps.par_iter().map(|step|{
    //     execute_step(step.clone(), persistence)?;
    // });

    // for step in steps {
    //     execute_step(step, persistence)?;
    // }
    // Ok(())
    // Ok(hits)
}

// use crossbeam;
//
// pub fn execute_step_in_parrael(steps: Vec<PlanStepType>, persistence: &Persistence) -> Result<(), VelociError> {

//     crossbeam::scope(|scope| {
//         for step in steps {
//             scope.spawn(move || {
//                 let res = step.execute_step(persistence);
//                 if res.is_err(){
//                     panic!("{:?}", res.unwrap_err());
//                 }
//             });
//         }
//     });

//     Ok(())
// }
