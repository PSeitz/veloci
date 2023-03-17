#![cfg_attr(feature = "cargo-clippy", allow(clippy::boxed_local))]
use crate::{
    error::*,
    persistence::{Persistence, *},
    plan_creator::{channel::*, execution_plan::*},
    search::{boost::*, *},
    util::StringAdd,
};
use std::sync::Arc;

use crate::steps::*;
use std::boxed::Box;

use super::*;
use itertools::Itertools;
use ordered_float::OrderedFloat;

#[derive(Clone, Debug)]
pub(crate) struct PlanStepFieldSearchToTokenIds {
    pub(crate) req: PlanRequestSearchPart,
    pub(crate) channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
pub(crate) struct ResolveTokenIdToAnchor {
    pub(crate) request: RequestSearchPart,
    pub(crate) channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
pub(crate) struct BoostToAnchor {
    #[allow(dead_code)]
    pub(crate) path: String,
    #[allow(dead_code)]
    pub(crate) trace_info: String,
    pub(crate) channel: PlanStepDataChannels,
    pub(crate) request: RequestSearchPart,
    pub(crate) boost: RequestBoostPart,
}
#[derive(Clone, Debug)]
pub(crate) struct ApplyAnchorBoost {
    #[allow(dead_code)]
    pub(crate) trace_info: String,
    pub(crate) channel: PlanStepDataChannels,
    #[allow(dead_code)]
    pub(crate) request: RequestSearchPart,
    pub(crate) boost: RequestBoostPart,
}
#[derive(Clone, Debug)]
pub(crate) struct BoostPlanStepFromBoostRequest {
    pub(crate) req: RequestBoostPart,
    pub(crate) channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
pub(crate) struct BoostAnchorFromPhraseResults {
    pub(crate) channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
pub(crate) struct PlanStepPhrasePairToAnchorId {
    pub(crate) req: RequestPhraseBoost,
    pub(crate) channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
pub(crate) struct Union {
    pub(crate) ids_only: bool,
    pub(crate) channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
pub(crate) struct Intersect {
    pub(crate) ids_only: bool,
    pub(crate) channel: PlanStepDataChannels,
}
#[derive(Clone, Debug)]
pub(crate) struct IntersectScoresWithIds {
    pub(crate) channel: PlanStepDataChannels,
}

impl std::fmt::Display for PlanStepFieldSearchToTokenIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "search {} {}", self.req.request.path, self.req.request.terms[0])?;
        Ok(())
    }
}
impl std::fmt::Display for ResolveTokenIdToAnchor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "token to anchor")?;
        Ok(())
    }
}
impl std::fmt::Display for BoostToAnchor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "BoostToAnchor {}", self.boost.path)?;
        Ok(())
    }
}
impl std::fmt::Display for ApplyAnchorBoost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "ApplyAnchorBoost",)?;
        Ok(())
    }
}
impl std::fmt::Display for BoostPlanStepFromBoostRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "BoostPlanStepFromBoostRequest")?;
        Ok(())
    }
}
impl std::fmt::Display for BoostAnchorFromPhraseResults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "BoostAnchorFromPhraseResults")?;
        Ok(())
    }
}
impl std::fmt::Display for PlanStepPhrasePairToAnchorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "PlanStepPhrasePairToAnchorId")?;
        Ok(())
    }
}
impl std::fmt::Display for Union {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Union")?;
        Ok(())
    }
}
impl std::fmt::Display for Intersect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Intersect")?;
        Ok(())
    }
}
impl std::fmt::Display for IntersectScoresWithIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "IntersectScoresWithIds")?;
        Ok(())
    }
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

/// Token to text ids (TEXT_IDS)
/// text ids to parent valueid (VALUE_IDS)
/// ValueIds to boost values (VALUE_IDS, BOOST_VALUES)
/// value ids to anchor (ANCHOR_IDS, ANCHOR_IDS)
impl PlanStepTrait for BoostToAnchor {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels {
        &mut self.channel
    }

    fn execute_step(self: Box<Self>, persistence: &Persistence) -> Result<(), VelociError> {
        debug_time!("BoostToAnchor {} {}", self.request.path, self.boost.path);
        let mut field_result = self.channel.input_prev_steps[0].recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?;

        //TODO EXPLAIN INFO NOT RESPECTED IN THIS METHOD
        resolve_token_hits_to_text_id_ids_only(persistence, &self.request, &mut field_result)?;

        //valueid to parent
        let text_index_ids_to_value_ids = self.request.path.add(TEXTINDEX).add(VALUE_ID_TO_PARENT);
        field_result = join_to_parent_ids(persistence, &field_result, &text_index_ids_to_value_ids, "boost: textindex to value id")?;

        let mut boost_field_path = (&self.boost.path).to_field_path();
        boost::get_boost_ids_and_resolve_to_anchor(persistence, &mut boost_field_path, &mut field_result)?;

        send_result_to_channel(field_result, &self.channel)?;
        drop_channel(self.channel);
        Ok(())
    }
}

/// Token to text ids (TEXT_IDS)
/// text ids to parent valueid (VALUE_IDS)
/// ValueIds to boost values (VALUE_IDS, BOOST_VALUES)
/// value ids to anchor (ANCHOR_IDS, ANCHOR_IDS)
impl PlanStepTrait for ApplyAnchorBoost {
    fn get_channel(&mut self) -> &mut PlanStepDataChannels {
        &mut self.channel
    }

    fn execute_step(self: Box<Self>, _persistence: &Persistence) -> Result<(), VelociError> {
        let mut field_result = self.channel.input_prev_steps[0].recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?;

        let boost_values = self.channel.input_prev_steps[1].recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?;

        apply_boost_values_anchor(&mut field_result, &self.boost, &mut boost_values.boost_ids.into_iter())?;

        send_result_to_channel(field_result, &self.channel)?;
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
        trace!("IntersectScoresWithIds scores_res {} ids_res {}", scores_res, ids_res);
        let res = intersect_score_hits_with_ids(scores_res, ids_res);
        send_result_to_channel(res, &self.channel)?;
        drop(self.channel.sender_to_next_steps);
        Ok(())
    }
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
            filter_channel.filter_sender.send(Arc::clone(&res)).map_err(|_| VelociError::PlanExecutionSendFailed)?;
        }
    }
    let mut data = vec![field_result]; //splat data to vec, first one is free
    for _ in 0..channel.num_receivers - 1 {
        let clone = data[0].clone();
        data.push(clone);
    }
    for el in data {
        channel.sender_to_next_steps.send(el).map_err(|_| VelociError::PlanExecutionSendFailed)?;
    }
    Ok(())
}

fn get_data(input_prev_steps: &[PlanDataReceiver]) -> Result<Vec<SearchFieldResult>, VelociError> {
    let mut dat = vec![];
    for el in input_prev_steps {
        dat.push(el.recv().map_err(|_| VelociError::PlanExecutionRecvFailed)?);
    }
    Ok(dat)
}
