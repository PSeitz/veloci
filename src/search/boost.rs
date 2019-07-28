
use super::*;

pub use self::search_field_result::*;
use std::{self, f32};

use fnv::{FnvHashMap};
use itertools::Itertools;


#[inline]
pub(crate) fn boost_text_locality_all(persistence: &Persistence, term_id_hits_in_field: &mut FnvHashMap<String, FnvHashMap<String, Vec<TermId>>>) -> Result<(Vec<Hit>), VelociError> {
    debug!("boost_text_locality_all {:?}", term_id_hits_in_field);
    info_time!("boost_text_locality_all");
    let mut boost_anchor: Vec<Hit> = vec![];

    let r: Result<Vec<_>, VelociError> = term_id_hits_in_field
        .into_par_iter()
        .map(|(path, term_with_ids)| boost_text_locality(persistence, path, term_with_ids))
        .collect();

    info_time!("collect sort_boost");
    let boosts = r?;
    let mergo = boosts.into_iter().kmerge_by(|a, b| a.id < b.id);
    for (id, group) in &mergo.group_by(|el| el.id) {
        let best_score = group.map(|el| el.score).max_by(|a, b| b.partial_cmp(&a).unwrap_or(Ordering::Equal)).unwrap();
        debug_assert!(best_score != std::f32::NAN);
        debug_assert!(best_score != std::f32::INFINITY);
        boost_anchor.push(Hit::new(id, best_score));
    }
    trace!("{:?}", boost_anchor);
    Ok(boost_anchor)
}

pub(crate) fn boost_text_locality(persistence: &Persistence, path: &str, search_term_to_text_ids: &mut FnvHashMap<String, Vec<TermId>>) -> Result<(Vec<Hit>), VelociError> {
    let mut boost_anchor = vec![];
    if search_term_to_text_ids.len() <= 1 {
        // No boost for single term hits
        return Ok(vec![]);
    }
    let token_to_text_id = persistence.get_valueid_to_parent(path.add(TOKENS_TO_TEXT_ID))?;
    let mut terms_text_ids: Vec<_> = vec![];
    let mut boost_text_ids = vec![];
    {
        trace_time!("text_locality_boost get and group text_ids");
        for text_ids in search_term_to_text_ids.values() {
            let mut text_ids = get_all_value_ids(&text_ids, token_to_text_id);
            text_ids.sort_unstable();
            terms_text_ids.push(text_ids);
        }
        let mergo = terms_text_ids.into_iter().kmerge_by(|a, b| a < b);
        for (id, group) in &mergo.group_by(|el| *el) {
            let num_hits_in_same_text = group.count();
            if num_hits_in_same_text > 1 {
                boost_text_ids.push((id, num_hits_in_same_text));
            }
        }
    }

    // text_ids are already anchor_ids === identity_column
    if persistence.metadata.columns.get(&extract_field_name(path)).map(|el| el.is_identity_column).unwrap_or(false) {
        boost_text_ids.sort_unstable_by_key(|el| el.0);
        for text_id in boost_text_ids {
            let num_hits_in_same_text = text_id.1;
            boost_anchor.push(Hit::new(text_id.0, 2. * num_hits_in_same_text as f32 * num_hits_in_same_text as f32));
        }
    } else {
        let text_id_to_anchor = persistence.get_valueid_to_parent(path.add(TEXT_ID_TO_ANCHOR))?;
        trace_time!("text_locality_boost text_ids to anchor");

        boost_text_ids.sort_unstable_by_key(|el| el.0);
        for text_id in boost_text_ids {
            let num_hits_in_same_text = text_id.1;
            for anchor_id in text_id_to_anchor.get_values_iter(u64::from(text_id.0)) {
                boost_anchor.push(Hit::new(anchor_id, 2. * num_hits_in_same_text as f32 * num_hits_in_same_text as f32));
            }
        }
    }

    boost_anchor.sort_unstable_by_key(|el| el.id);
    Ok(boost_anchor)
}



pub(crate) fn apply_boost_term(persistence: &Persistence, mut res: SearchFieldResult, boost_term: &[RequestSearchPart]) -> Result<SearchFieldResult, VelociError> {
    info_time!("boost_term");
    {
        persistence.term_boost_cache.write().get(boost_term); //poke
    }

    let mut from_cache = false;
    // Attentión - The read lock is still active in the else block therefore we need to create an extra scope to avoid deadlocks
    // This should be probably fixed sometime with better lifetime handling in rust
    {
        if let Some(data) = persistence.term_boost_cache.read().peek(boost_term) {
            // let mut boost_iter = data.hits_ids.iter().map(|el|el.clone());
            // res = apply_boost_from_iter(res, &mut boost_iter)
            info_time!("boost_term_from_cache");
            let mut boost_iter = data
                .iter()
                .map(|el| {
                    let boost_val: f32 = el.request.boost.map(|el| el.into_inner()).unwrap_or(2.0);
                    debug_assert!(boost_val != std::f32::NAN);
                    debug_assert!(boost_val != std::f32::INFINITY);
                    el.hits_ids.iter().map(move |id| Hit::new(*id, boost_val))
                })
                .kmerge_by(|a, b| a.id < b.id);

            // {
            //     let mut boost_iter_data:Vec<Hit> = data.iter()
            //     .map(|el| {
            //         let boost_val:f32 = el.request.boost.unwrap_or(2.0).clone();
            //         el.hits_ids.iter().map(move|id| Hit::new(*id, boost_val ))
            //     })
            //     .into_iter().kmerge_by(|a, b| a.id < b.id).collect();

            //     {
            //         let mut direct_data:Vec<f32> = vec![];
            //         for hit in boost_iter_data.iter() {
            //             if direct_data.len() <= hit.id as usize {
            //                 direct_data.resize(hit.id as usize + 1, 0.0);
            //             }
            //             direct_data[hit.id as usize] = hit.score;
            //         }
            //         info_time!("direct search boost");
            //         for hit in res.hits_scores.iter_mut(){
            //             if let Some(boost_hit) = direct_data.get(hit.id as usize) {
            //                 hit.score *= boost_hit;
            //             }
            //         }
            //     }

            //     {
            //         let my_boost = 2.0;
            //         let mut direct_data:FixedBitSet = {

            //             let mut ay = FixedBitSet::with_capacity(70000 as usize + 1);
            //             for hit in boost_iter_data.iter() {
            //                 let (_, id_in_bucket) = to_bucket_and_id(hit.id);
            //                 ay.insert(id_in_bucket as usize);
            //             }
            //             ay
            //         };
            //         info_time!("direct search bitset");
            //         for hit in res.hits_scores.iter_mut(){
            //             let (_, id_in_bucket) = to_bucket_and_id(hit.id);
            //             if direct_data.contains(id_in_bucket as usize) {
            //                 hit.score *= my_boost;
            //             }
            //         }
            //     }

            //     {
            //         info_time!("merge search boost");
            //         res = apply_boost_from_iter(res, &mut boost_iter_data.into_iter());
            //     }

            //     debug_time!("binary search".to_string());

            // }

            debug_time!("boost_hits_ids_vec_multi");
            res = apply_boost_from_iter(res, &mut boost_iter);

            from_cache = true;
        }
    }

    if !from_cache {
        let r: Result<Vec<_>, VelociError> = boost_term
            .to_vec()
            .into_par_iter()
            .map(|boost_term_req: RequestSearchPart| {
                let mut boost_term_req = PlanRequestSearchPart {
                    request: boost_term_req,
                    get_ids: true,
                    ..Default::default()
                };
                let mut result = search_field::get_term_ids_in_field(persistence, &mut boost_term_req)?;
                result = search_field::resolve_token_to_anchor(persistence, &boost_term_req.request, &None, &result)?;
                Ok(result)
            })
            .collect();
        let mut data = r?;
        res = boost_hits_ids_vec_multi(res, &mut data);
        {
            persistence.term_boost_cache.write().insert(boost_term.to_vec(), data);
        }
    }
    Ok(res)
}




pub(crate) fn apply_boost_from_iter(mut results: SearchFieldResult, mut boost_iter: &mut dyn Iterator<Item = Hit>) -> SearchFieldResult {
    let mut explain = FnvHashMap::default();
    mem::swap(&mut explain, &mut results.explain);
    let should_explain = results.request.explain;
    {
        let mut move_boost = |hit: &mut Hit, hit_curr: &mut Hit, boost_iter: &mut dyn Iterator<Item = Hit>| {
            //Forward the boost iterator and look for matches
            for b_hit in boost_iter {
                if b_hit.id > hit.id {
                    *hit_curr = b_hit.clone();
                    break;
                } else if b_hit.id == hit.id {
                    *hit_curr = b_hit.clone();
                    hit.score *= b_hit.score;
                    debug_assert!(hit.score != std::f32::NAN);
                    debug_assert!(hit.score != std::f32::INFINITY);
                    if should_explain {
                        let data = explain.entry(hit.id).or_insert_with(|| vec![]);
                        // data.push(format!("boost {:?}", b_hit.score));
                        data.push(Explain::Boost(b_hit.score));
                    }
                }
            }
        };

        if let Some(yep) = boost_iter.next() {
            let mut hit_curr = yep;
            for mut hit in &mut results.hits_scores {
                if hit_curr.id < hit.id {
                    move_boost(&mut hit, &mut hit_curr, &mut boost_iter);
                } else if hit_curr.id == hit.id {
                    hit.score *= hit_curr.score;
                    move_boost(&mut hit, &mut hit_curr, &mut boost_iter); // Possible multi boosts [id:0->2, id:0->4 ...]
                }
            }
        }
    }

    mem::swap(&mut explain, &mut results.explain);
    results
}


pub(crate) fn apply_boost_values_anchor(results: &mut SearchFieldResult, boost: &RequestBoostPart, mut boost_iter: &mut dyn Iterator<Item = Hit>) -> Result<(), VelociError> {
    let boost_param = boost.param.map(|el| el.into_inner()).unwrap_or(0.0);
    let expre = boost.expression.as_ref().map(|expression| ScoreExpression::new(expression.clone()));
    let mut explain = if results.request.explain {
        Some(&mut results.explain)
    }else{
        None
    };
    {
        if let Some(yep) = boost_iter.next() {
            let mut hit_curr = yep;
            for hit in &mut results.hits_scores {

                if hit_curr.id < hit.id {
                    for b_hit in &mut boost_iter {
                        if b_hit.id > hit.id {
                            hit_curr = b_hit.clone();
                            break;
                        } else if b_hit.id == hit.id {
                            hit_curr = b_hit.clone();
                            apply_boost(hit, b_hit.score, boost_param, &boost.boost_fun, &mut explain, &expre)?;
                        }
                    }
                } else if hit_curr.id == hit.id {
                    apply_boost(hit, hit_curr.score, boost_param, &boost.boost_fun, &mut explain, &expre)?;
                }
            }
        }
    }

    Ok(())
}

pub(crate)  fn apply_boost(hit:&mut Hit, boost_value:f32, boost_param:f32, boost_fun: &Option<BoostFunction>, explain: &mut Option<&mut FnvHashMap<u32, Vec<Explain>>>, expre: &Option<ScoreExpression>) -> Result<(), VelociError> {
    match boost_fun {
        Some(BoostFunction::Log10) => {
            // if hits.request.explain {
            //     let entry = hits.explain.entry(value_id).or_insert_with(|| vec![]);
            //     entry.push(Explain::Boost((boost_value as f32 + boost_param).log10()));
            // }
            if let Some(explain) = explain {
                let entry = explain.entry(hit.id).or_insert_with(|| vec![]);
                entry.push(Explain::Boost((boost_value + boost_param).log10()));
            }
            trace!(
                "Log10 boosting hit.id {:?} score {:?} to {:?} -- token_value {:?} boost_value {:?}",
                hit.id,
                hit.score,
                hit.score * boost_value + boost_param,
                boost_value,
                (boost_value + boost_param).log10(),
            );
            hit.score *= (boost_value + boost_param).log10();
        }
        Some(BoostFunction::Log2) => {
            trace!(
                "Log2 boosting hit.id {:?} hit.score {:?} to {:?} -- token_value {:?} boost_value {:?}",
                hit.id,
                hit.score,
                hit.score * boost_value + boost_param,
                boost_value,
                (boost_value + boost_param).log2(),
            );
            hit.score *= (boost_value + boost_param).log2();
        }
        Some(BoostFunction::Linear) => {
            trace!(
                "Linear boosting hit.id {:?} hit.score {:?} to {:?} -- token_value {:?} boost_value {:?}",
                hit.id,
                hit.score,
                hit.score + (boost_value + boost_param),
                boost_value,
                (boost_value + boost_param)
            );
            hit.score *= boost_value + boost_param;
        }
        Some(BoostFunction::Add) => {
            trace!(
                "boosting hit.id {:?} hit.score {:?} to {:?} -- token_value {:?} boost_value {:?}",
                hit.id,
                hit.score,
                hit.score + (boost_value + boost_param),
                boost_value,
                (boost_value + boost_param)
            );
            hit.score += boost_value + boost_param;
        }
        None => {}
    }
    if let Some(exp) = expre.as_ref() {
        let prev_score = hit.score;
        hit.score += exp.get_score(boost_value);
        trace!(
            "boost {:?} to {:?} with boost_fun({:?})={:?}",
            prev_score,
            hit.score,
            boost_value,
            exp.get_score(boost_value)
        );
    }

    debug_assert!(hit.score != std::f32::NAN);
    debug_assert!(hit.score != std::f32::INFINITY);
    if let Some(explain) = explain {
        let data = explain.entry(hit.id).or_insert_with(|| vec![]);
        // data.push(format!("boost {:?}", b_hit.score));
        data.push(Explain::Boost(hit.score));
    }

    Ok(())
}


/// applies the boost values from the boostparts to the result
pub(crate)  fn boost_hits_ids_vec_multi(mut results: SearchFieldResult, boost: &mut Vec<SearchFieldResult>) -> SearchFieldResult {
    {
        debug_time!("boost hits sort input");
        results.hits_scores.sort_unstable_by_key(|el| el.id); //TODO SORT NEEDED??
        for res in boost.iter_mut() {
            res.hits_scores.sort_unstable_by_key(|el| el.id);
            res.hits_ids.sort_unstable();
        }
    }

    let mut boost_iter = boost
        .iter()
        .map(|el| {
            let boost_val: f32 = el.request.boost.map(|el| el.into_inner()).unwrap_or(2.0);
            debug_assert!(boost_val != std::f32::NAN);
            debug_assert!(boost_val != std::f32::INFINITY);
            el.hits_ids.iter().map(move |id| Hit::new(*id, boost_val))
        })
        .kmerge_by(|a, b| a.id < b.id);

    debug_time!("boost_hits_ids_vec_multi");
    apply_boost_from_iter(results, &mut boost_iter)
}

#[test]
fn boost_intersect_hits_vec_test_multi() {
    let hits1 = vec![Hit::new(10, 20.0), Hit::new(0, 20.0), Hit::new(5, 20.0), Hit::new(60, 20.0)]; // unsorted
    let boost = vec![0, 3, 10, 10, 70];
    let boost2 = vec![10, 60];

    let mut boosts = vec![
        SearchFieldResult {
            hits_ids: boost,
            ..Default::default()
        },
        SearchFieldResult {
            hits_ids: boost2,
            ..Default::default()
        },
    ];

    let res = boost_hits_ids_vec_multi(
        SearchFieldResult {
            hits_scores: hits1,
            ..Default::default()
        },
        &mut boosts,
    );

    assert_eq!(res.hits_scores, vec![Hit::new(0, 40.0), Hit::new(5, 20.0), Hit::new(10, 160.0), Hit::new(60, 40.0)]);
}

pub(crate)  fn get_boost_ids_and_resolve_to_anchor(persistence: &Persistence, path: &str, hits: &mut SearchFieldResult) -> Result<(), VelociError> {
    let boost_path = path.add(BOOST_VALID_TO_VALUE);
    let boostkv_store = persistence.get_boost(&boost_path)?;

    // trace_index_id_to_parent(boostkv_store);

    for value_id in &mut hits.hits_ids {
        let val_opt = boostkv_store.get_value(u64::from(*value_id));

        if let Some(boost_value) = val_opt.as_ref() {
            hits.boost_ids.push(Hit::new(*value_id, *boost_value as f32));
        }
    }

    hits.hits_ids = vec![];

    // resolve to anchor
    let mut data = vec![];
    let kv_store = persistence.get_valueid_to_parent(&path.add(VALUE_ID_TO_ANCHOR))?; //TODO should be get_kv_store
    for boost_pair in &mut hits.boost_ids {
        let val_opt = kv_store.get_value(u64::from(boost_pair.id));

        if let Some(anchor_id) = val_opt.as_ref() {
            data.push(Hit::new(*anchor_id, boost_pair.score));
        }else{
            // can this happen: value_id without anchro id. I think not
        }
    }

    hits.boost_ids = data;

    Ok(())

}


pub(crate)  fn add_boost(persistence: &Persistence, boost: &RequestBoostPart, hits: &mut SearchFieldResult) -> Result<(), VelociError> {
    // let key = util::boost_path(&boost.path);
    let boost_path = boost.path.to_string() + BOOST_VALID_TO_VALUE;
    let boostkv_store = persistence.get_boost(&boost_path)?;
    let boost_param = boost.param.map(|el| el.into_inner()).unwrap_or(0.0);

    let expre = boost.expression.as_ref().map(|expression| ScoreExpression::new(expression.clone()));
    let default = vec![];
    let skip_when_score = boost
        .skip_when_score
        .as_ref()
        .map(|vecco| vecco.iter().map(|el| el.into_inner()).collect())
        .unwrap_or(default);

    let mut explain = if hits.request.explain {
        Some(&mut hits.explain)
    }else{
        None
    };
    for hit in &mut hits.hits_scores {
        if !skip_when_score.is_empty() && skip_when_score.iter().any(|x| (*x - hit.score).abs() < 0.00001) {
            // float comparisons should usually include a error margin
            continue;
        }
        // let value_id = &hit.id;
        // let score = &mut hit.score;
        // let ref vals_opt = boostkv_store.get(*value_id as usize);
        let val_opt = &boostkv_store.get_value(u64::from(hit.id));

        if let Some(boost_value) = val_opt.as_ref() {
            debug!("Found in boosting for value_id {:?}: {:?}", hit.id, val_opt);
            let boost_value = *boost_value as f32;

            apply_boost(hit, boost_value, boost_param, &boost.boost_fun, &mut explain, &expre)?;
            // match boost.boost_fun {
            //     Some(BoostFunction::Log10) => {
            //         if hits.request.explain {
            //             let entry = hits.explain.entry(*value_id).or_insert_with(|| vec![]);
            //             entry.push(Explain::Boost((boost_value as f32 + boost_param).log10()));
            //         }
            //         trace!(
            //             "Log10 boosting value_id {:?} score {:?} to {:?} -- token_value {:?} boost_value {:?}",
            //             *value_id,
            //             score,
            //             *score * boost_value as f32 + boost_param,
            //             boost_value,
            //             (boost_value as f32 + boost_param).log10(),
            //         );
            //         *score *= (boost_value as f32 + boost_param).log10();
            //     }
            //     Some(BoostFunction::Log2) => {
            //         trace!(
            //             "Log2 boosting value_id {:?} score {:?} to {:?} -- token_value {:?} boost_value {:?}",
            //             *value_id,
            //             score,
            //             *score * boost_value as f32 + boost_param,
            //             boost_value,
            //             (boost_value as f32 + boost_param).log2(),
            //         );
            //         *score *= (boost_value as f32 + boost_param).log2();
            //     }
            //     Some(BoostFunction::Linear) => {
            //         trace!(
            //             "Linear boosting value_id {:?} score {:?} to {:?} -- token_value {:?} boost_value {:?}",
            //             *value_id,
            //             score,
            //             *score + (boost_value as f32 + boost_param),
            //             boost_value,
            //             (boost_value as f32 + boost_param)
            //         );
            //         *score *= boost_value as f32 + boost_param;
            //     }
            //     Some(BoostFunction::Add) => {
            //         trace!(
            //             "boosting value_id {:?} score {:?} to {:?} -- token_value {:?} boost_value {:?}",
            //             *value_id,
            //             score,
            //             *score + (boost_value as f32 + boost_param),
            //             boost_value,
            //             (boost_value as f32 + boost_param)
            //         );
            //         *score += boost_value as f32 + boost_param;
            //     }
            //     None => {}
            // }
            // if let Some(exp) = expre.as_ref() {
            //     let prev_score = *score;
            //     *score += exp.get_score(boost_value as f32);
            //     trace!(
            //         "boost {:?} to {:?} with boost_fun({:?})={:?}",
            //         prev_score,
            //         score,
            //         boost_value,
            //         exp.get_score(boost_value as f32)
            //     );
            // }
        }

        debug_assert!(hit.score != std::f32::NAN);
        debug_assert!(hit.score != std::f32::INFINITY);
    }
    Ok(())
}
