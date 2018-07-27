use std::fmt::Debug;
use persistence::Persistence;
use persistence::*;
use search::add_boost;
use search::*;
use search::{Request, RequestBoostPart, RequestSearchPart, SearchError};
use search_field;
use util;
use util::StringAdd;

use std::boxed::Box;

use crossbeam_channel;
use crossbeam_channel::unbounded;
use search_field::*;

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

    /// Also return the actual text
    #[serde(skip_serializing_if = "skip_false")]
    pub return_term: bool,

}

#[derive(Debug, Clone, Copy)]
pub struct Dependency {
    step_index: usize,
    depends_on: usize,
}

#[derive(Debug)]
pub struct Plan {
    pub steps: Vec<Box<dyn PlanStepTrait>>,
    dependencies: Vec<Dependency>
}

impl Default for Plan {
    fn default() -> Plan {
        Plan { steps: vec![], dependencies: vec![] }
    }
}

impl Plan {

    fn add_dependency(&mut self, step_index:usize, depends_on: usize) {
        self.dependencies.push(Dependency{step_index, depends_on});
    }

    /// return the position in the array, which can be used as an id
    fn add_step(&mut self, step:Box<dyn PlanStepTrait>) -> usize {
        self.steps.push(step);
        // self.steps.last_mut().unwrap()
        self.steps.len() - 1
    }

    /// return the position in the array, which can be used as an id
    fn get_step(&mut self, step_id:usize) -> &mut Box<dyn PlanStepTrait> {
        &mut self.steps[step_id]
    }

    // fn get_dependencies(&self, step_index: usize) -> Vec<Dependency> {
    //     self.dependencies.iter().filter(|dep|dep.step_index == step_index).cloned().collect()
    // }

    pub fn get_ordered_steps(self) -> Vec<Vec<Box<dyn PlanStepTrait>>> {
        let mut ordered_steps = vec![];
        let mut remaining_steps:Vec<_> = self.steps.into_iter().enumerate().collect();

        let dep = self.dependencies;

        while !remaining_steps.is_empty() {
            let current_remaining_step_ids:Vec<_> = remaining_steps.iter().map(|el|el.0).collect();
            let steps_with_fullfilled_dependencies: Vec<_> = remaining_steps.drain_filter(|step_with_index| {
                // let steps_dependencies = self.get_dependencies(step_with_index.0);
                let steps_dependencies:Vec<Dependency> = dep.iter().filter(|dep|dep.step_index == step_with_index.0).cloned().collect();
                let unfulfilled_dependencies:Vec<_> = steps_dependencies.iter().filter(|dep|{
                    current_remaining_step_ids.iter().any(|step_id| *step_id == dep.depends_on) // check if depends_on is in current_remaining_step_ids
                }).collect();

                unfulfilled_dependencies.is_empty()
            }).collect();

            // ordered_steps.push(steps_with_fullfilled_dependencies.iter().map(|step_with_index|*step_with_index.1.clone()).collect());
            let vecco: Vec<_> = steps_with_fullfilled_dependencies.into_iter().map(|step_with_index|step_with_index.1).collect();
            ordered_steps.push(vecco);
        }

        ordered_steps
    }
}


type PlanDataSender = crossbeam_channel::Sender<SearchFieldResult>;
type PlanDataReceiver = crossbeam_channel::Receiver<SearchFieldResult>;

#[derive(Debug, Clone, PartialEq)]
pub struct PlanStepDataChannels{
    input_prev_steps: Vec<PlanDataReceiver>,
    output_sending_to_next_steps: PlanDataSender,
    num_receivers: u32,
    plans_output_receiver_for_next_step: PlanDataReceiver, // used in plan_creation
}

pub trait PlanStepTrait: Debug + Sync + Send{
    fn get_channel(&mut self) -> &mut PlanStepDataChannels;
    // fn get_output(&self) -> PlanDataReceiver;
    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), SearchError>;
}

#[derive(Clone, Debug, PartialEq)]
struct PlanStepFieldSearchToTokenIds {
    req: PlanRequestSearchPart,
    channels: PlanStepDataChannels,
}
#[derive(Clone, Debug, PartialEq)]
struct ResolveTokenIdToAnchor {
    // req: PlanRequestSearchPart,
    request: RequestSearchPart,
    channels: PlanStepDataChannels,
}
#[derive(Clone, Debug, PartialEq)]
struct ResolveTokenIdToTextId {
    request: RequestSearchPart,
    channels: PlanStepDataChannels,
}
#[derive(Clone, Debug, PartialEq)]
struct ValueIdToParent {
    path: String,
    trace_info: String,
    channels: PlanStepDataChannels,
}
#[derive(Clone, Debug, PartialEq)]
struct BoostPlanStepFromBoostRequest {
    req: RequestBoostPart,
    channels: PlanStepDataChannels,
}
#[derive(Clone, Debug, PartialEq)]
struct BoostAnchorFromPhraseResults {
    channels: PlanStepDataChannels,
}
#[derive(Clone, Debug, PartialEq)]
struct PlanStepPhrasePairToAnchorId {
    req: RequestPhraseBoost,
    channels: PlanStepDataChannels,
}
#[derive(Clone, Debug, PartialEq)]
struct Union {
    channels: PlanStepDataChannels,
}
#[derive(Clone, Debug, PartialEq)]
struct Intersect {
    channels: PlanStepDataChannels,
}

impl PlanStepTrait for PlanStepFieldSearchToTokenIds {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    fn execute_step(mut self: Box<Self>, persistence: &Persistence) -> Result<(), SearchError>{
        let field_result = search_field::get_term_ids_in_field(persistence, &mut self.req)?;
        send_result_to_channel(field_result, &self.channels)?;
        drop_channel(self.channels);
        Ok(())
    }
}

impl PlanStepTrait for ResolveTokenIdToAnchor {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), SearchError>{
        let field_result = resolve_token_to_anchor(persistence, &self.request, None, &self.channels.input_prev_steps[0].recv()?)?;
        send_result_to_channel(field_result, &self.channels)?;
        drop_channel(self.channels);
        Ok(())
    }
}
impl PlanStepTrait for ResolveTokenIdToTextId {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), SearchError>{
        let mut field_result = self.channels.input_prev_steps[0].recv()?;
        resolve_token_hits_to_text_id(persistence, &self.request, None, &mut field_result)?;
        send_result_to_channel(field_result, &self.channels)?;
        drop_channel(self.channels);
        Ok(())
    }
}

impl PlanStepTrait for ValueIdToParent {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), SearchError>{
        send_result_to_channel(join_to_parent_with_score(persistence, &self.channels.input_prev_steps[0].recv()?, &self.path, &self.trace_info)?, &self.channels)?;
        drop_channel(self.channels);
        Ok(())
    }
}

impl PlanStepTrait for BoostPlanStepFromBoostRequest {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), SearchError>{
        let mut input = self.channels.input_prev_steps[0].recv()?;
        add_boost(persistence, &self.req, &mut input)?; //TODO Wrap
        send_result_to_channel(input, &self.channels)?;
        drop_channel(self.channels);
        Ok(())
    }
}

use ordered_float::OrderedFloat;
use itertools::Itertools;
fn sort_and_group_boosts_by_phrase_terms(mut boosts: Vec<SearchFieldResult>) -> Vec<SearchFieldResult> {
    info_time!("sort_and_group_boosts_by_phrase_terms");
    boosts.sort_unstable_by_key(|res|{
        let phrase_req = res.phrase_boost.as_ref().unwrap();
        (phrase_req.search1.terms[0].to_string(), phrase_req.search2.terms[0].to_string())
    });

    let mut new_vec = vec![];
    for (phrase, mut group) in &boosts.iter().group_by(|res|{
        let phrase_req = res.phrase_boost.as_ref().unwrap();
        (phrase_req.search1.terms[0].to_string(), phrase_req.search2.terms[0].to_string())
    }) {

        debug_time!("kmerge anchors for phrase {:?}", phrase);
        let boosts_iter:Vec<_> = group.map(|el|el.hits_ids.iter()).collect();
        let mut mergo:Vec<u32> = boosts_iter.into_iter().kmerge().cloned().collect();
        mergo.dedup();
        new_vec.push(SearchFieldResult{
            hits_ids: mergo,
            ..Default::default()
        });
    }

    new_vec
}

impl PlanStepTrait for BoostAnchorFromPhraseResults {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    fn execute_step(self: Box<Self>, _persistence: &Persistence) -> Result<(), SearchError>{
        let input = self.channels.input_prev_steps[0].recv()?;
        let boosts = get_data(&self.channels.input_prev_steps[1..])?;

        let mut boosts = sort_and_group_boosts_by_phrase_terms(boosts);
        //Set boost for phrases for the next step
        for boost_res in &mut boosts {
            boost_res.request.boost = Some(OrderedFloat(5.0));
        }

        send_result_to_channel(boost_hits_ids_vec_multi(input, &mut boosts), &self.channels)?;
        drop_channel(self.channels);
        Ok(())
    }
}
impl PlanStepTrait for PlanStepPhrasePairToAnchorId {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), SearchError>{
        let res1 = self.channels.input_prev_steps[0].recv()?;
        let res2 = self.channels.input_prev_steps[1].recv()?;
        assert!(self.req.search1.path == self.req.search2.path);
        let mut res = get_anchor_for_phrases_in_search_results(persistence, &self.req.search1.path, res1, res2)?;
        res.phrase_boost = Some(self.req.clone());
        send_result_to_channel(res, &self.channels)?;
        drop_channel(self.channels);
        Ok(())
    }
}

impl PlanStepTrait for Union {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    fn execute_step(self: Box<Self>, _persistence: &Persistence) -> Result<(), SearchError>{
        send_result_to_channel(union_hits_score(get_data(&self.channels.clone().input_prev_steps)?), &self.channels)?;
        drop(self.channels.output_sending_to_next_steps);
        Ok(())
    }
}

impl PlanStepTrait for Intersect {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    fn execute_step(self: Box<Self>, _persistence: &Persistence) -> Result<(), SearchError>{
        send_result_to_channel(intersect_hits_score(get_data(&self.channels.clone().input_prev_steps)?), &self.channels)?;
        drop(self.channels.output_sending_to_next_steps);
        Ok(())
    }
}

fn get_data(input_prev_steps: &[PlanDataReceiver]) -> Result<Vec<SearchFieldResult>, SearchError> {
    let mut dat = vec![];
    for el in input_prev_steps {
        dat.push(el.recv()?);
        drop(el);
    }
    Ok(dat)
}
// fn drop_inputs(inputs: &Vec<PlanDataReceiver>) {
//     for el in channels.input_prev_steps {
//         drop(el);
//     }
// }

fn drop_channel(channels: PlanStepDataChannels) {
    drop(channels.output_sending_to_next_steps);
    for el in channels.input_prev_steps {
        drop(el);
    }
}

fn send_result_to_channel(field_result: SearchFieldResult, channels: &PlanStepDataChannels) -> Result<(), SearchError>  {
    let mut data = vec![field_result]; //splat data to vec, first one is free
    for _ in 0..channels.num_receivers - 1 {
        let clone = data[0].clone();
        data.push(clone);
    }
    for el in data {
        channels.output_sending_to_next_steps.send(el)?;
    }
    Ok(())
}

use fnv::FnvHashSet;
use fnv::FnvHashMap;
fn get_all_field_request_parts(request: &Request) -> FnvHashSet<RequestSearchPart> {
    let mut map:FnvHashSet<RequestSearchPart> = FnvHashSet::default();

    if let Some(phrase_boosts) = request.phrase_boosts.as_ref() {
        map.extend(phrase_boosts.iter().map(|el|{
            vec![el.search1.clone(), el.search2.clone()]
        }).flat_map(|map|map.into_iter()));
    }

    if let Some(and_or) = request.and.as_ref().or(request.or.as_ref()) {
        map.extend(and_or.iter().map(|el|{
            get_all_field_request_parts(el)
        }).flat_map(|map|map.into_iter()));
    }
    if let Some(search) = request.search.clone() {
        map.extend([search].into_iter().cloned());
    }

    map
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn plan_creator(mut request: Request, plan: &mut Plan) -> PlanDataReceiver {

    let field_requests = get_all_field_request_parts(&request);
    let mut field_search_cache = FnvHashMap::default();
    for request_part in field_requests {
        let (tx, rx): (PlanDataSender, PlanDataReceiver) = unbounded();
        let plan_request_part = PlanRequestSearchPart{request:request_part.clone(), get_scores: true, ..Default::default()};
        let field_search = PlanStepFieldSearchToTokenIds {
            req: plan_request_part.clone(),
            channels: PlanStepDataChannels{
                num_receivers: 0,
                input_prev_steps: vec![],
                output_sending_to_next_steps: tx,
                plans_output_receiver_for_next_step: rx,
            }
        };
        let step_id = plan.add_step(Box::new(field_search.clone())); // actually only a placeholder, will replaced with the updated field search after plan creation
        field_search_cache.insert(request_part, (step_id, field_search));
    }
    let mut final_output = plan_creator_2(&mut request, plan, None, &mut field_search_cache);

    if let Some(phrase_boosts) = request.phrase_boosts {
        let mut phrase_outputs = vec![];
        for boost in phrase_boosts {
            let (tx, rx): (PlanDataSender, PlanDataReceiver) = unbounded();

            let mut get_field_search = |req: &RequestSearchPart| ->  (PlanDataReceiver, usize){
                let field_search1 = field_search_cache.get_mut(req).unwrap();
                field_search1.1.req.get_ids = true;
                field_search1.1.channels.num_receivers += 1;
                let field_rx = field_search1.1.channels.plans_output_receiver_for_next_step.clone();
                (field_rx, field_search1.0)
            };

            let (field_rx1, plan_id1) = get_field_search(&boost.search1);
            let (field_rx2, plan_id2) = get_field_search(&boost.search2);

            let step = PlanStepPhrasePairToAnchorId {
                req: boost.clone(),
                channels: PlanStepDataChannels{
                    num_receivers: 1,
                    input_prev_steps: vec![field_rx1, field_rx2],
                    output_sending_to_next_steps: tx,
                    plans_output_receiver_for_next_step: rx.clone(),
                }
            };
            phrase_outputs.push(rx);
            let id_step = plan.add_step(Box::new(step.clone()));
            plan.add_dependency(id_step, plan_id1);
            plan.add_dependency(id_step, plan_id2);
        }

        let mut v = Vec::new();
        v.push(final_output.0);
        v.extend_from_slice(&phrase_outputs[..]);

        //boost all results with phrase results
        let (tx, rx): (PlanDataSender, PlanDataReceiver) = unbounded();
        let step = BoostAnchorFromPhraseResults {
            channels: PlanStepDataChannels{
                num_receivers: 1,
                input_prev_steps: v,
                output_sending_to_next_steps: tx,
                plans_output_receiver_for_next_step: rx.clone(),
            }
        };
        let id_step = plan.add_step(Box::new(step.clone()));
        plan.add_dependency(id_step, final_output.1);
        final_output = (rx, id_step);
    }

    //update the field search steps
    for (_k, v) in field_search_cache.drain() {
        plan.steps[v.0] = Box::new(v.1);
    }

    final_output.0

}

#[cfg_attr(feature = "flame_it", flame)]
fn plan_creator_2(request: &mut Request, plan: &mut Plan, parent_step_dependecy: Option<usize>, field_search_cache: &mut FnvHashMap<RequestSearchPart, (usize, PlanStepFieldSearchToTokenIds)>) -> (PlanDataReceiver, usize) {
    let (tx, rx): (PlanDataSender, PlanDataReceiver) = unbounded();

    if let Some(ref mut or) = request.or {
        let mut step = Union {
            channels: PlanStepDataChannels{
                num_receivers: 1,
                input_prev_steps: vec![],
                output_sending_to_next_steps: tx,
                plans_output_receiver_for_next_step: rx.clone(),
            }
        };
        let step_id = plan.add_step(Box::new(step.clone()));
        let result_channels_from_prev_steps = or.iter_mut().map(|x| plan_creator_2(x, plan, Some(step_id), field_search_cache).0).collect();
        plan.get_step(step_id).get_channel().input_prev_steps = result_channels_from_prev_steps;

        if let Some(parent_step_dependecy) = parent_step_dependecy {
            plan.add_dependency(parent_step_dependecy, step_id);
        }

        (rx, step_id)
    } else if let Some(ref mut ands) = request.and {
        let mut step = Intersect {
            channels: PlanStepDataChannels{
                num_receivers: 1,
                input_prev_steps: vec![],
                output_sending_to_next_steps: tx,
                plans_output_receiver_for_next_step: rx.clone(),
            }
        };
        let step_id = plan.add_step(Box::new(step.clone()));
        let result_channels_from_prev_steps = ands.iter_mut().map(|x| plan_creator_2(x, plan, Some(step_id), field_search_cache).0).collect();
        plan.get_step(step_id).get_channel().input_prev_steps = result_channels_from_prev_steps;

        if let Some(parent_step_dependecy) = parent_step_dependecy {
            plan.add_dependency(parent_step_dependecy, step_id);
        }

        (rx, step_id)
    } else if let Some(part) = request.search.clone() {
        // TODO Tokenize query according to field
        // part.terms = part.terms.iter().map(|el| util::normalize_text(el)).collect::<Vec<_>>();
        plan_creator_search_part(part, request, plan, parent_step_dependecy, field_search_cache)
    } else {
        //TODO HANDLE SUGGEST
        //TODO ADD ERROR
        // plan_creator_search_part(request.search.as_ref().unwrap().clone(), request)
        panic!("missing 'and' 'or' 'search' in request - suggest not yet handled in search api {:?}", request);
    }
}

#[cfg_attr(feature = "flame_it", flame)]
fn plan_creator_search_part(request_part: RequestSearchPart,
        request: &mut Request,
        plan: &mut Plan,
        parent_step_dependecy: Option<usize>,
        field_search_cache: &mut FnvHashMap<RequestSearchPart, (usize, PlanStepFieldSearchToTokenIds)>
    ) -> (PlanDataReceiver, usize) {
    let paths = util::get_steps_to_anchor(&request_part.path);

    // let (mut field_tx, mut field_rx): (PlanDataSender, PlanDataReceiver) = unbounded();

    let fast_field = request.boost.is_none() && !request_part.snippet.unwrap_or(false); // fast_field disabled for boosting or _highlighting_ currently
    let store_term_id_hits = request.why_found || request.text_locality;
    // let plan_request_part = PlanRequestSearchPart{request:request_part, get_scores: true, store_term_id_hits, store_term_texts: request.why_found, ..Default::default()};


    let id = field_search_cache.get_mut(&request_part).unwrap().0;
    let field_search_step = &mut field_search_cache.get_mut(&request_part).unwrap().1;
    field_search_step.req.store_term_texts |= request.why_found;
    field_search_step.req.store_term_id_hits |= store_term_id_hits;
    field_search_step.channels.num_receivers += 1;
    let field_rx = field_search_step.channels.plans_output_receiver_for_next_step.clone();
    // let field_tx = field_search_step.channels.output_sending_to_next_steps.clone();

    if fast_field {

        // let field_search_step = PlanStepFieldSearchToTokenIds {
        //     req: plan_request_part.clone(),
        //     channels: PlanStepDataChannels{
        //         num_receivers: 1,
        //         input_prev_steps: vec![],
        //         output_sending_to_next_steps: field_tx,
        //         plans_output_receiver_for_next_step: field_rx.clone(),
        //     }
        // };
        let (next_field_tx, next_field_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
        // let id = plan.add_step(Box::new(field_search_step));
        let step = ResolveTokenIdToAnchor {
            request: request_part.clone(),
            channels: PlanStepDataChannels{
                num_receivers: 1,
                input_prev_steps: vec![field_rx],
                output_sending_to_next_steps: next_field_tx,
                plans_output_receiver_for_next_step: next_field_rx.clone(),
            }
        };
        let id1 = plan.add_step(Box::new(step.clone()));
        plan.add_dependency(id1, id);

        if let Some(parent_step_dependecy) = parent_step_dependecy {
            plan.add_dependency(parent_step_dependecy, id);
            plan.add_dependency(parent_step_dependecy, id1);
        }

        (next_field_rx, id1)
    } else {

        let mut add_step = |step: Box<dyn PlanStepTrait>| -> usize{
            let step_id = plan.add_step(step);
            if let Some(parent_step_dependecy) = parent_step_dependecy {
                plan.add_dependency(parent_step_dependecy, step_id);
            }
            step_id
        };

        // TODO ADD STEP DEPENDENCIES??
        // steps.push(Box::new(PlanStepFieldSearchToTokenIds {
        //     req: plan_request_part.clone(),
        //     channels: PlanStepDataChannels{
        //         num_receivers: 1,
        //         input_prev_steps: vec![],
        //         output_sending_to_next_steps: field_tx,
        //         plans_output_receiver_for_next_step: field_rx.clone(),
        //     }
        // }));

        let (next_field_tx, next_field_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
        add_step(Box::new(ResolveTokenIdToTextId {
            request: request_part.clone(),
            channels: PlanStepDataChannels{
                num_receivers: 1,
                input_prev_steps: vec![field_rx],
                output_sending_to_next_steps: next_field_tx,
                plans_output_receiver_for_next_step: next_field_rx.clone(),
            }
        }));

        let (mut tx, mut rx): (PlanDataSender, PlanDataReceiver) = unbounded();

        add_step(Box::new(ValueIdToParent {
            path: paths.last().unwrap().add(VALUE_ID_TO_PARENT),
            trace_info: "term hits hit to column".to_string(),
            channels: PlanStepDataChannels{
                num_receivers: 1,
                input_prev_steps: vec![next_field_rx.clone()],
                output_sending_to_next_steps: tx.clone(),
                plans_output_receiver_for_next_step: rx.clone(),
            }
        }));

        for i in (0..paths.len() - 1).rev() {
            if request.boost.is_some() {
                request.boost.as_mut().unwrap().retain(|boost| {
                    let apply_boost = boost.path.starts_with(&paths[i]);
                    if apply_boost {
                        let (next_tx, next_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
                        tx = next_tx;
                        add_step(Box::new(BoostPlanStepFromBoostRequest {
                            req: boost.clone(),
                            channels: PlanStepDataChannels{
                                num_receivers: 1,
                                input_prev_steps: vec![rx.clone()],
                                output_sending_to_next_steps: tx.clone(),
                                plans_output_receiver_for_next_step: next_rx.clone(),
                            }
                        }));

                        debug!("PlanCreator Step {:?}", boost);

                        rx = next_rx;
                    }
                    !apply_boost
                });
            }

            let (next_tx, next_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
            tx = next_tx;

            add_step(Box::new(ValueIdToParent {
                path: paths[i].add(VALUE_ID_TO_PARENT),
                trace_info: "Joining to anchor".to_string(),
                channels: PlanStepDataChannels{
                    num_receivers: 1,
                    input_prev_steps: vec![rx.clone()],
                    output_sending_to_next_steps: tx.clone(),
                    plans_output_receiver_for_next_step: next_rx.clone(),
                }
            }));

            debug!("PlanCreator Step {}", paths[i].add(VALUE_ID_TO_PARENT));

            rx = next_rx;
        }

        let mut step_id = 0;
        if let Some(ref boosts) = request.boost {
            // Handling boost from anchor to value - TODO FIXME Error when 1:N
            for boost in boosts {
                let (next_tx, next_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
                tx = next_tx;
                let id = add_step(Box::new(BoostPlanStepFromBoostRequest {
                    req: boost.clone(),
                    channels: PlanStepDataChannels{
                        num_receivers: 1,
                        input_prev_steps: vec![rx.clone()],
                        output_sending_to_next_steps: tx.clone(),
                        plans_output_receiver_for_next_step: next_rx.clone(),
                    }
                }));
                debug!("PlanCreator Step {:?}", boost);
                rx = next_rx;
                step_id = id;
            }
        }

        // for step in steps {
        //     let id = plan.add_step(step);
        //     if let Some(parent_step_dependecy) = parent_step_dependecy {
        //         plan.add_dependency(parent_step_dependecy, id);
        //     }
        // }

        (rx, step_id)
    }

}

use rayon::prelude::*;

// #[cfg_attr(feature = "flame_it", flame)]
// pub fn execute_steps(steps: Vec<PlanStepType>, persistence: &Persistence) -> Result<(), SearchError> {
//     let r: Result<Vec<_>, SearchError> = steps.into_par_iter().map(|step| step.execute_step(persistence)).collect();

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

#[cfg_attr(feature = "flame_it", flame)]
pub fn execute_steps(steps: Vec<Box<dyn PlanStepTrait>>, persistence: &Persistence) -> Result<(), SearchError> {
    let r: Result<Vec<_>, SearchError> = steps.into_par_iter().map(|step:Box<dyn PlanStepTrait>| step.execute_step(persistence)).collect();

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
// #[cfg_attr(feature = "flame_it", flame)]
// pub fn execute_step_in_parrael(steps: Vec<PlanStepType>, persistence: &Persistence) -> Result<(), SearchError> {

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
