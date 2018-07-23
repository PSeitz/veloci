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
struct FieldSearchToTokenIds {
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
struct Boost {
    req: RequestBoostPart,
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

impl PlanStepTrait for FieldSearchToTokenIds {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    // fn get_output(&self) -> PlanDataReceiver{
    //     self.channels.plans_output_receiver_for_next_step.clone()
    // }
    fn execute_step(mut self: Box<Self>, persistence: &Persistence) -> Result<(), SearchError>{
        let field_result = search_field::get_term_ids_in_field(persistence, &mut self.req)?;
        send_data_n_times_to_channel(field_result, &self.channels)?;
        drop_channel(self.channels);
        Ok(())
    }
}

impl PlanStepTrait for ResolveTokenIdToAnchor {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    // fn get_output(&self) -> PlanDataReceiver{
    //     self.channels.plans_output_receiver_for_next_step.clone()
    // }
    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), SearchError>{
        let field_result = resolve_token_to_anchor(persistence, &self.request, None, &self.channels.input_prev_steps[0].recv()?)?;
        send_data_n_times_to_channel(field_result, &self.channels)?;
        drop_channel(self.channels);
        Ok(())
    }
}
impl PlanStepTrait for ResolveTokenIdToTextId {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    // fn get_output(&self) -> PlanDataReceiver{
    //     self.channels.plans_output_receiver_for_next_step.clone()
    // }
    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), SearchError>{
        let mut field_result = self.channels.input_prev_steps[0].recv()?;
        resolve_token_hits_to_text_id(persistence, &self.request, None, &mut field_result)?;
        send_data_n_times_to_channel(field_result, &self.channels)?;
        drop_channel(self.channels);
        Ok(())
    }
}

impl PlanStepTrait for ValueIdToParent {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    // fn get_output(&self) -> PlanDataReceiver{
    //     self.channels.plans_output_receiver_for_next_step.clone()
    // }
    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), SearchError>{
        send_data_n_times_to_channel(join_to_parent_with_score(persistence, &self.channels.input_prev_steps[0].recv()?, &self.path, &self.trace_info)?, &self.channels)?;
        drop_channel(self.channels);
        Ok(())
    }
}

impl PlanStepTrait for Boost {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    // fn get_output(&self) -> PlanDataReceiver{
    //     self.channels.plans_output_receiver_for_next_step.clone()
    // }
    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), SearchError>{
        let mut input = self.channels.input_prev_steps[0].recv()?;
        add_boost(persistence, &self.req, &mut input)?; //TODO Wrap
        send_data_n_times_to_channel(input, &self.channels)?;
        drop_channel(self.channels);
        Ok(())
    }
}

impl PlanStepTrait for Union {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    // fn get_output(&self) -> PlanDataReceiver{
    //     self.channels.plans_output_receiver_for_next_step.clone()
    // }
    fn execute_step(self: Box<Self>, _persistence: &Persistence) -> Result<(), SearchError>{
        send_data_n_times_to_channel(union_hits_vec(get_data(self.channels.clone().input_prev_steps)?), &self.channels)?;
        drop(self.channels.output_sending_to_next_steps);
        Ok(())
    }
}

impl PlanStepTrait for Intersect {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels{
        &mut self.channels
    }
    // fn get_output(&self) -> PlanDataReceiver{
    //     self.channels.plans_output_receiver_for_next_step.clone()
    // }
    fn execute_step(self: Box<Self>, _persistence: &Persistence) -> Result<(), SearchError>{
        send_data_n_times_to_channel(intersect_hits_vec(get_data(self.channels.clone().input_prev_steps)?), &self.channels)?;
        drop(self.channels.output_sending_to_next_steps);
        Ok(())
    }
}

fn get_data(input_prev_steps: Vec<PlanDataReceiver>) -> Result<Vec<SearchFieldResult>, SearchError> {
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

fn send_data_n_times_to_channel(field_result: SearchFieldResult, channels: &PlanStepDataChannels) -> Result<(), SearchError>  {
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
    if let Some(and_or) = request.and.as_ref().or(request.or.as_ref()) {
        return and_or.iter().map(|el|{
            get_all_field_request_parts(el)
        }).flat_map(|map|map.into_iter()).collect();
    }
    if let Some(search) = request.search.clone() {
        return [search].into_iter().cloned().collect()
    }

    FnvHashSet::default()
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn plan_creator(request: Request, plan: &mut Plan) -> PlanDataReceiver {

    let field_requests = get_all_field_request_parts(&request);
    let mut map = FnvHashMap::default();
    for request_part in field_requests {
        let (tx, rx): (PlanDataSender, PlanDataReceiver) = unbounded();
        let plan_request_part = PlanRequestSearchPart{request:request_part.clone(), get_scores: true, ..Default::default()};
        let field_search = FieldSearchToTokenIds {
            req: plan_request_part.clone(),
            channels: PlanStepDataChannels{
                num_receivers: 0,
                input_prev_steps: vec![],
                output_sending_to_next_steps: tx,
                plans_output_receiver_for_next_step: rx,
            }
        };
        let step_id = plan.add_step(Box::new(field_search.clone())); // actually only a placeholder, will replaced with the updated field search after plan creation
        map.insert(request_part, (step_id, field_search));
    }
    let receiver = plan_creator_2(request, plan, None, &mut map);

    //update the field search steps
    for (_k, v) in map.drain() {
        plan.steps[v.0] = Box::new(v.1);
    }

    receiver

}

#[cfg_attr(feature = "flame_it", flame)]
fn plan_creator_2(request: Request, plan: &mut Plan, parent_step_dependecy: Option<usize>, step_cache: &mut FnvHashMap<RequestSearchPart, (usize, FieldSearchToTokenIds)>) -> PlanDataReceiver {
    let (tx, rx): (PlanDataSender, PlanDataReceiver) = unbounded();

    if let Some(or) = request.or {
        let mut step = Union {
            channels: PlanStepDataChannels{
                num_receivers: 1,
                input_prev_steps: vec![],
                output_sending_to_next_steps: tx,
                plans_output_receiver_for_next_step: rx.clone(),
            }
        };
        let step_id = plan.add_step(Box::new(step.clone()));
        let result_channels_from_prev_steps = or.iter().map(|x| plan_creator_2(x.clone(), plan, Some(step_id), step_cache)).collect();
        plan.get_step(step_id).get_channel().input_prev_steps = result_channels_from_prev_steps;

        if let Some(parent_step_dependecy) = parent_step_dependecy {
            plan.add_dependency(parent_step_dependecy, step_id);
        }

        rx
    } else if let Some(ands) = request.and {
        let mut step = Intersect {
            channels: PlanStepDataChannels{
                num_receivers: 1,
                input_prev_steps: vec![],
                output_sending_to_next_steps: tx,
                plans_output_receiver_for_next_step: rx.clone(),
            }
        };
        let step_id = plan.add_step(Box::new(step.clone()));
        let result_channels_from_prev_steps = ands.iter().map(|x| plan_creator_2(x.clone(), plan, Some(step_id), step_cache)).collect();
        plan.get_step(step_id).get_channel().input_prev_steps = result_channels_from_prev_steps;

        if let Some(parent_step_dependecy) = parent_step_dependecy {
            plan.add_dependency(parent_step_dependecy, step_id);
        }

        rx
    } else if let Some(part) = request.search.clone() {
        // TODO Tokenize query according to field
        // part.terms = part.terms.iter().map(|el| util::normalize_text(el)).collect::<Vec<_>>();
        plan_creator_search_part(part, request, plan, parent_step_dependecy, step_cache)
    } else {
        //TODO HANDLE SUGGEST
        //TODO ADD ERROR
        // plan_creator_search_part(request.search.as_ref().unwrap().clone(), request)
        panic!("missing 'and' 'or' 'search' in request - suggest not yet handled in search api {:?}", request);
    }
}

#[cfg_attr(feature = "flame_it", flame)]
fn plan_creator_search_part(request_part: RequestSearchPart, mut request: Request, plan: &mut Plan, parent_step_dependecy: Option<usize>, step_cache: &mut FnvHashMap<RequestSearchPart, (usize, FieldSearchToTokenIds)>) -> PlanDataReceiver {
    let paths = util::get_steps_to_anchor(&request_part.path);

    // let (mut field_tx, mut field_rx): (PlanDataSender, PlanDataReceiver) = unbounded();

    let fast_field = request.boost.is_none() && !request_part.snippet.unwrap_or(false); // fast_field disabled for boosting or _highlighting_ currently
    let store_term_id_hits = request.why_found || request.text_locality;
    // let plan_request_part = PlanRequestSearchPart{request:request_part, get_scores: true, store_term_id_hits, store_term_texts: request.why_found, ..Default::default()};


    let id = step_cache.get_mut(&request_part).unwrap().0;
    let field_search_step = &mut step_cache.get_mut(&request_part).unwrap().1;
    field_search_step.req.store_term_texts |= request.why_found;
    field_search_step.req.store_term_id_hits |= store_term_id_hits;
    field_search_step.channels.num_receivers += 1;
    let field_rx = field_search_step.channels.plans_output_receiver_for_next_step.clone();
    // let field_tx = field_search_step.channels.output_sending_to_next_steps.clone();

    if fast_field {

        // let field_search_step = FieldSearchToTokenIds {
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

        next_field_rx
    } else {
        let mut steps:Vec<Box<dyn PlanStepTrait>> = vec![];

        // TODO ADD STEP DEPENDENCIES??
        // steps.push(Box::new(FieldSearchToTokenIds {
        //     req: plan_request_part.clone(),
        //     channels: PlanStepDataChannels{
        //         num_receivers: 1,
        //         input_prev_steps: vec![],
        //         output_sending_to_next_steps: field_tx,
        //         plans_output_receiver_for_next_step: field_rx.clone(),
        //     }
        // }));

        let (next_field_tx, next_field_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
        steps.push(Box::new(ResolveTokenIdToTextId {
            request: request_part.clone(),
            channels: PlanStepDataChannels{
                num_receivers: 1,
                input_prev_steps: vec![field_rx],
                output_sending_to_next_steps: next_field_tx,
                plans_output_receiver_for_next_step: next_field_rx.clone(),
            }
        }));

        let (mut tx, mut rx): (PlanDataSender, PlanDataReceiver) = unbounded();

        steps.push(Box::new(ValueIdToParent {
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
                        steps.push(Box::new(Boost {
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

            steps.push(Box::new(ValueIdToParent {
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

        if let Some(boosts) = request.boost {
            // Handling boost from anchor to value - TODO FIXME Error when 1:N
            for boost in boosts {
                let (next_tx, next_rx): (PlanDataSender, PlanDataReceiver) = unbounded();
                tx = next_tx;
                steps.push(Box::new(Boost {
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
        }

        for step in steps {
            let id = plan.add_step(step);
            if let Some(parent_step_dependecy) = parent_step_dependecy {
                plan.add_dependency(parent_step_dependecy, id);
            }
        }

        rx
    }

    // (steps, rx)
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
