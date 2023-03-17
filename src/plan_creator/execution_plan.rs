#![cfg_attr(feature = "cargo-clippy", allow(clippy::boxed_local))]
use crate::{
    error::*,
    persistence::{Persistence, *},
    plan_creator::{channel::*, plan::*, plan_steps::*, PlanStepTrait},
    search::*,
    util::{self, StringAdd},
};

use fnv::FnvHashMap;
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

/// To three parts are settings propagates currently, the search request, the phrase boosts, and the filter query
fn get_all_field_request_parts_and_propagate_settings<'a>(header_request: &'a Request, request: &'a mut Request, map: &mut Vec<&'a mut RequestSearchPart>) {
    if let Some(phrase_boosts) = request.phrase_boosts.as_mut() {
        for el in phrase_boosts.iter_mut() {
            el.search1.options.explain |= header_request.explain;
            el.search2.options.explain |= header_request.explain;
            map.push(&mut el.search1);
            map.push(&mut el.search2);
        }
    }

    get_all_field_request_parts_and_propagate_settings_to_search_req(header_request, request.search_req.as_mut().unwrap(), map);
}

fn get_all_field_request_parts_and_propagate_settings_to_search_req<'a>(header_request: &'a Request, request: &'a mut SearchRequest, map: &mut Vec<&'a mut RequestSearchPart>) {
    request.get_options_mut().explain |= header_request.explain;

    match request {
        SearchRequest::And(SearchTree { queries, options: _ }) | SearchRequest::Or(SearchTree { queries, options: _ }) => {
            for el in queries {
                get_all_field_request_parts_and_propagate_settings_to_search_req(header_request, el, map);
            }
        }
        SearchRequest::Search(search) => {
            search.options.explain |= header_request.explain;
            map.push(search);
        }
    }
}

/// add first we collect all searches on the fields (virtually the leaf nodes in the execution plan) to avoid duplicate searches. This could also be done on a tree level.
///
/// The function also propagates settings before collecting requests, because this changes the equality. This should be probably done seperately.
///
fn collect_all_field_request_into_cache(header_request: &Request, request: &mut Request, plan: &mut Plan) -> FieldRequestCache {
    let mut field_search_cache = FnvHashMap::default();
    let mut field_requests = Vec::new();
    get_all_field_request_parts_and_propagate_settings(header_request, request, &mut field_requests);
    add_request_to_search_field_cache(field_requests, plan, &mut field_search_cache, false);

    // collect filter requests seperately and set to fetch ids
    // This way we can potentially reuse the same request to emit both, score and ids
    if let Some(filter) = request.filter.as_mut() {
        let mut field_requests = Vec::new();
        get_all_field_request_parts_and_propagate_settings_to_search_req(header_request, filter, &mut field_requests);
        add_request_to_search_field_cache(field_requests, plan, &mut field_search_cache, true);
    };

    field_search_cache
}

fn add_request_to_search_field_cache(field_requests: Vec<&mut RequestSearchPart>, plan: &mut Plan, field_search_cache: &mut FieldRequestCache, ids_only: bool) {
    for request_part in field_requests {
        // There could be the same query for filter and normal search, then we load scores and ids
        if let Some((_, field_search)) = field_search_cache.get_mut(request_part) {
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
        let step_id = plan.add_step(Box::new(field_search.clone())); // this is actually only a placeholder in the plan, will be replaced with the data from the field_search_cache after plan creation

        field_search_cache.insert(request_part.clone(), (step_id, field_search));
    }
}

pub fn plan_creator(mut request: Request, plan: &mut Plan) {
    let request_header = request.clone();

    let mut field_search_cache = collect_all_field_request_into_cache(&request_header, &mut request, plan);

    let filter_final_step_id: Option<PlanStepId> = if let Some(filter) = request.filter.as_mut() {
        // get_all_field_request_parts_and_propagate_settings_to_search_req(header_request, filter, map);
        // collect_all_field_request_into_cache(&request_header, filter, &mut field_search_cache, plan, true);
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
            &request.search_req.unwrap(),
            boost.unwrap_or_default(),
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
        let channel = PlanStepDataChannels::open_channel(1, vec![final_step_channel.receiver_for_next_step, filter_receiver]);
        let step = IntersectScoresWithIds { channel };
        // step.get_channel().input_prev_steps = vec![final_output.0, filter_data_output.0];
        let id_step = plan.add_step(Box::new(step));
        plan.add_dependency(id_step, filter_final_step_id);
        plan.add_dependency(id_step, final_step_id);
        final_step_id = id_step;
    }
    // Apply Boost from anchor
    if let Some(boosts) = request.boost {
        let anchor_boosts: Vec<&RequestBoostPart> = boosts.iter().filter(|el| !el.path.contains("[]")).collect();

        for boost in anchor_boosts {
            let final_step_channel = plan.get_step_channel(final_step_id).clone();
            let channel = PlanStepDataChannels::open_channel(1, vec![final_step_channel.receiver_for_next_step.clone()]);
            let step = BoostPlanStepFromBoostRequest {
                req: boost.clone(),
                channel: channel.clone(),
            };
            let id_step = plan.add_step(Box::new(step.clone()));
            plan.add_dependency(id_step, final_step_id);
            final_step_id = id_step;
        }
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
            // let field_search1 = field_search_cache
            //     .get_mut(req)
            //     .unwrap_or_else(|| panic!("PlanCreator: Could not find  request in field_search_cache {:?}", req));

            let val = field_search_cache.get_mut(req);

            let field_search1 = {
                if val.is_none() {
                    panic!(
                        "PlanCreator: Could not find phrase request in field_search_cache Req: {:#?}, \n Cache: {:#?}",
                        req,
                        field_search_cache.keys()
                    )
                }
                val.unwrap()
            };

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
    id_step
}
fn merge_vec(boost: &[RequestBoostPart], opt: &Option<&[RequestBoostPart]>) -> Vec<RequestBoostPart> {
    let mut boost = boost.to_owned();
    if let Some(boosto) = opt.as_ref() {
        boost.extend_from_slice(boosto);
    }
    // boost.extend_from_slice(&opt.as_ref().unwrap_or_else(||vec![]));
    boost
}

fn plan_creator_2(
    is_filter: bool,
    is_filter_channel: bool,
    filter_channel_step: Option<usize>, //  this channel is used to receive the result from the filter step
    request_header: &Request,
    request: &SearchRequest,
    mut boost: Vec<RequestBoostPart>,
    plan: &mut Plan,
    parent_step_dependecy: Option<usize>,
    depends_on_step: Option<usize>,
    field_search_cache: &mut FieldRequestCache,
) -> PlanStepId {
    // request.explain |= request_header.explain;

    match request {
        SearchRequest::Or(SearchTree { queries, options: _ }) => {
            let mut channel = PlanStepDataChannels::default();
            if let Some(step_id) = filter_channel_step {
                plan.get_step_channel(step_id).filter_channel.as_mut().unwrap().num_receivers += 1;
                channel.filter_receiver = Some(plan.get_step_channel(step_id).filter_channel.as_mut().unwrap().filter_receiver.clone());
            }
            if is_filter_channel {
                channel.filter_channel = Some(FilterChannel::default());
            }
            let step = Union { ids_only: is_filter, channel };
            let step_id = plan.add_step(Box::new(step));
            let result_channels_from_prev_steps = queries
                .iter()
                .map(|x| {
                    // x.explain = request_header.explain;
                    let boost = merge_vec(&boost, &x.get_boost());
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

            step_id
        }
        SearchRequest::And(SearchTree { queries, options: _ }) => {
            let mut channel = PlanStepDataChannels::default();
            if let Some(step_id) = filter_channel_step {
                plan.get_step_channel(step_id).filter_channel.as_mut().unwrap().num_receivers += 1;
                channel.filter_receiver = Some(plan.get_step_channel(step_id).filter_channel.as_mut().unwrap().filter_receiver.clone());
            }
            if is_filter_channel {
                channel.filter_channel = Some(FilterChannel::default());
            }
            let step = Intersect { ids_only: is_filter, channel };
            let step_id = plan.add_step(Box::new(step));
            let result_channels_from_prev_steps = queries
                .iter()
                .map(|x| {
                    // x.explain = request_header.explain;
                    let boost = merge_vec(&boost, &x.get_boost());
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

            step_id
        }
        SearchRequest::Search(part) => {
            // TODO Tokenize query according to field
            // part.terms = part.terms.iter().map(|el| util::normalize_text(el)).collect::<Vec<_>>();
            plan_creator_search_part(
                is_filter_channel,
                filter_channel_step,
                part,
                request_header,
                &mut boost,
                plan,
                parent_step_dependecy,
                depends_on_step,
                field_search_cache,
            )
        }
    }
}

fn plan_creator_search_part(
    is_filter_channel: bool,
    filter_channel_step: Option<usize>,
    request_part: &RequestSearchPart,
    request: &Request,
    boosts: &mut [RequestBoostPart],
    plan: &mut Plan,
    parent_step_dependecy: Option<usize>,
    depends_on_step: Option<usize>,
    field_search_cache: &mut FieldRequestCache,
) -> PlanStepId {
    let paths = util::get_steps_to_anchor(&request_part.path);
    let store_term_id_hits = request.why_found || request.text_locality;

    let val = field_search_cache.get_mut(request_part);

    let (field_search_step_id, field_search_step) = {
        if val.is_none() {
            panic!(
                "PlanCreator: Could not find request in field_search_cache.\nReq: {:#?}, \nCache: {:#?}",
                request_part,
                field_search_cache.keys()
            )
        }
        val.unwrap()
    };

    field_search_step.req.store_term_texts |= request.why_found;
    field_search_step.req.store_term_id_hits |= store_term_id_hits;
    field_search_step.channel.num_receivers += 1;
    let field_rx = field_search_step.channel.receiver_for_next_step.clone();

    //Check if is 1 to n field
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
                                                                       // STEP2: APPLY BOOST on anchor
            let token_to_anchor_rx = channel.receiver_for_next_step;
            let boost_vals_rx = boost_to_anchor_channel.receiver_for_next_step;
            let mut apply_boost_to_anchor_channel = PlanStepDataChannels::open_channel(1, vec![token_to_anchor_rx, boost_vals_rx]);

            // the last step gets set a filter channel to which he will send the result
            if is_filter_channel {
                apply_boost_to_anchor_channel.filter_channel = Some(FilterChannel::default());
            }
            let step = Box::new(ApplyAnchorBoost {
                trace_info: "ApplyAnchorBoost".to_string(),
                channel: apply_boost_to_anchor_channel,
                request: request_part.clone(),
                boost: boosto[0].clone(),
            });
            let step_id = plan.add_step(step);
            plan.add_dependency(step_id, boost_step_id);
            plan.add_dependency(step_id, token_to_anchor_step_id);
            if let Some(parent_step_dependecy) = parent_step_dependecy {
                plan.add_dependency(parent_step_dependecy, step_id);
            }

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
    id1
}

use rayon::prelude::*;

pub fn execute_steps(steps: Vec<Box<dyn PlanStepTrait>>, persistence: &Persistence) -> Result<(), VelociError> {
    let r: Result<Vec<_>, VelociError> = steps.into_par_iter().map(|step: Box<dyn PlanStepTrait>| step.execute_step(persistence)).collect();

    if let Err(err) = r {
        Err(err)
    } else {
        Ok(())
    }
}
