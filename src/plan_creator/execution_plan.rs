#![cfg_attr(feature = "cargo-clippy", allow(clippy::boxed_local))]
use crate::{
    error::*,
    persistence::{Persistence, *},
    plan_creator::{channel::*, plan::*, plan_steps::*, PlanStepTrait},
    search::*,
    util::{self, StringAdd},
};

use crossbeam_channel::{self, unbounded};
use fnv::{FnvHashMap, FnvHashSet};
use std::boxed::Box;

pub(crate) type FieldRequestCache = FnvHashMap<RequestSearchPart, (usize, PlanStepFieldSearchToTokenIds)>;
pub(crate) type PlanStepId = usize;

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

    let (field_search_step_id, field_search_step) = field_search_cache
        .get_mut(&request_part)
        .unwrap_or_else(|| panic!("PlanCreator: Could not find  request in field_search_cache {:?}", request_part));
    // if let Some(parent_step_dependecy) = parent_step_dependecy {
    //     plan.add_dependency(parent_step_dependecy, *field_search_step_id);
    // }
    field_search_step.req.store_term_texts |= request.why_found;
    field_search_step.req.store_term_id_hits |= store_term_id_hits;
    field_search_step.channel.num_receivers += 1;
    let field_rx = field_search_step.channel.receiver_for_next_step.clone();

    if fast_field {
        // check boost on 1:n fields, boost on anchor is done seperately
        if let Some(pos) = request_part.path.rfind("[]") {
            let end_obj = &request_part.path[..pos];
            //find where boost matches last path
            let boosto: Vec<&RequestBoostPart> = boosts
                .iter()
                .flat_map(|el| {
                    if let Some(pos) = el.path.rfind("[]") {
                        if &el.path[..pos] == end_obj {
                            return Some(el);
                        }
                    }
                    None
                })
                .collect();
            if !boosto.is_empty() {
                assert!(boosto.len() == 1);

                //              RESOLVE TO ANCHOR  (ANCHOR, SCORE) --------------------------------------------------------------------------------------------------------------------
                //              /                                                                                                                                                      \
                // SEARCH FIELD                                                                                                                                                         APPLY BOOST
                //              \                                                                                                                                                      /
                //              Token to text ids (TEXT_IDS) -> text ids to parent valueid (VALUE_IDS) -> ValueIds to boost values (VALUE_IDS, BOOST_VALUES) ->   value ids to anchor (ANCHOR_IDS, ANCHOR_IDS)

                //+1 for boost
                field_search_step.channel.num_receivers += 1;

                // STEP1.1: RESOLVE TO ANCHOR  (ANCHOR, SCORE)
                let mut channel = PlanStepDataChannels::open_channel(1, vec![field_rx.clone()]);

                //connect to incoming filter channel (optional)
                if let Some(step_id) = filter_channel_step {
                    plan.get_step_channel(step_id).filter_channel.as_mut().unwrap().num_receivers += 1;
                    channel.filter_receiver = Some(plan.get_step_channel(step_id).filter_channel.as_mut().unwrap().filter_receiver.clone());
                }
                let token_to_anchor_step = ResolveTokenIdToAnchor {
                    request: request_part.clone(),
                    channel: channel.clone(),
                };
                let token_to_anchor_step_id = plan.add_step(Box::new(token_to_anchor_step));

                // add dependencies to ensure correct execution order
                plan.add_dependency(token_to_anchor_step_id, *field_search_step_id);
                // if let Some(parent_step_dependecy) = parent_step_dependecy {
                //     plan.add_dependency(parent_step_dependecy, token_to_anchor_step_id);
                // }
                if let Some(depends_on_step) = depends_on_step {
                    plan.add_dependency(token_to_anchor_step_id, depends_on_step);
                }

                // STEP1.2: resolve anchor boost values
                let boost_to_anchor_channel = PlanStepDataChannels::open_channel(1, vec![field_rx]);
                let boost_step = Box::new(BoostToAnchor {
                    path: paths.last().unwrap().add(VALUE_ID_TO_PARENT),
                    trace_info: "BoostToAnchor".to_string(),
                    channel: boost_to_anchor_channel.clone(),
                    request: request_part.clone(),
                    boost: boosto[0].clone(),
                });
                let boost_step_id = plan.add_step(boost_step);
                plan.add_dependency(boost_step_id, *field_search_step_id); // TODO instead adding the dependency manually here, we should deduce the dependency by dataflow. In open_channel the output is connected (field_rx) and should be added as depedency
                                                                           // if let Some(parent_step_dependecy) = parent_step_dependecy {
                                                                           //     plan.add_dependency(parent_step_dependecy, boost_step_id);
                                                                           // }

                // STEP2: APPLY BOOST on anchor
                let token_to_anchor_rx = channel.receiver_for_next_step.clone();
                let boost_vals_rx = boost_to_anchor_channel.receiver_for_next_step.clone();
                let mut apply_boost_to_anchor_channel = PlanStepDataChannels::open_channel(1, vec![token_to_anchor_rx, boost_vals_rx]);

                // the last step gets set a filter channel to which he will send the result
                if is_filter_channel {
                    apply_boost_to_anchor_channel.filter_channel = Some(FilterChannel::default());
                }
                let step = Box::new(ApplyAnchorBoost {
                    trace_info: "ApplyAnchorBoost".to_string(),
                    channel: apply_boost_to_anchor_channel.clone(),
                    request: request_part.clone(),
                    boost: boosto[0].clone(),
                });
                let step_id = plan.add_step(step);
                plan.add_dependency(step_id, boost_step_id);
                plan.add_dependency(step_id, token_to_anchor_step_id);
                if let Some(parent_step_dependecy) = parent_step_dependecy {
                    plan.add_dependency(parent_step_dependecy, step_id);
                }
                // plan.add_dependency(boost_step, step_id);

                //get boost scores and resolve to anchor
                // step_id
                // let mut step_id = add_step();

                if let Some(depends_on_step) = depends_on_step {
                    plan.add_dependency(step_id, depends_on_step);
                }
                return step_id;
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
        plan.add_dependency(id1, *field_search_step_id);

        if let Some(parent_step_dependecy) = parent_step_dependecy {
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
