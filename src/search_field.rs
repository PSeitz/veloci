use str;
use persistence::Persistence;
use persistence;
use search::RequestSearchPart;
use search::Request;
use search::SearchError;
use search;
use search::*;
use util::concat;
use std::cmp;
use std::cmp::Ordering;
use fnv::FnvHashMap;
use fnv::FnvHashSet;
use util;
use ordered_float::OrderedFloat;
// use hit_collector::HitCollector;
use itertools::Itertools;
#[allow(unused_imports)]
use fst::{IntoStreamer, Map, MapBuilder, Set};
#[allow(unused_imports)]
use fst_levenshtein::Levenshtein;
use fst::automaton::*;
use fst::raw::Fst;
use lev_automat::*;
use highlight_field::*;
use levenshtein_automaton::{Distance, LevenshteinAutomatonBuilder, DFA};
// use search::Hit;
use bit_vec::BitVec;

#[allow(unused_imports)]
use rayon::prelude::*;
use util::*;

#[allow(unused_imports)]
use trie::map;
use fixedbitset::FixedBitSet;

#[derive(Debug, Default)]
pub struct SearchFieldResult {
    pub hits_vec: Vec<search::Hit>,
    pub hits_ids: Vec<TermId>,
    pub terms: FnvHashMap<TermId, String>,
    pub highlight: FnvHashMap<TermId, String>,
    pub request: RequestSearchPart,
    /// store the term id hits field->Term->Hits, used for whyfound
    pub term_id_hits_in_field: FnvHashMap<String, FnvHashMap<String, Vec<TermId>>>,
}

impl SearchFieldResult {
    //Creates a new result, while keeping metadata for original hits
    pub fn new_from(other: &SearchFieldResult) -> Self {
        let mut res = SearchFieldResult::default();
        res.terms = other.terms.clone();
        res.highlight = other.highlight.clone();
        res.request = other.request.clone();
        res.term_id_hits_in_field = other.term_id_hits_in_field.clone();
        res
    }
}
// pub type TermScore = (TermId, Score);
pub type TermId = u32;
pub type Score = f32;

// fn get_default_score(term1: &str, term2: &str, prefix_matches: bool) -> f32 {
//     return get_default_score_for_distance(
//         distance(&term1.to_lowercase(), &term2.to_lowercase()) as u8,
//         prefix_matches,
//     );
//     // return 2.0/(distance(term1, term2) as f32 + 0.2 )
// }
fn get_default_score_for_distance(distance: u8, prefix_matches: bool) -> f32 {
    if prefix_matches {
        2.0 / ((distance as f32 + 1.0).log2() + 0.2)
    } else {
        2.0 / (distance as f32 + 0.2)
    }
}

#[inline]
pub fn ord_to_term(fst: &Fst, mut ord: u64, bytes: &mut Vec<u8>) -> bool {
    bytes.clear();
    let mut node = fst.root();
    while ord != 0 || !node.is_final() {
        let transition_opt = node.transitions().take_while(|transition| transition.out.value() <= ord).last();
        if let Some(transition) = transition_opt {
            ord -= transition.out.value();
            bytes.push(transition.inp);
            let new_node_addr = transition.addr;
            node = fst.node(new_node_addr);
        } else {
            return false;
        }
    }
    true
}

#[inline]
#[cfg_attr(feature = "flame_it", flame)]
fn get_text_lines<F>(persistence: &Persistence, options: &RequestSearchPart, mut fun: F) -> Result<(), SearchError>
where
    F: FnMut(String, u32),
{
    // let mut f = persistence.get_file_handle(&(options.path.to_string()+".fst"))?;
    // let mut buffer: Vec<u8> = Vec::new();
    // f.read_to_end(&mut buffer)?;
    // buffer.shrink_to_fit();
    // let map = try!(Map::from_bytes(buffer));
    // let map = persistence.get_fst(&options.path)?;

    let map = persistence
        .cache
        .fst
        .get(&options.path)
        .ok_or_else(|| SearchError::StringError(format!("fst not found loaded in cache {} ", options.path)))?;
    let lev = {
        debug_time!(format!("{} LevenshteinIC create", &options.path));
        LevenshteinIC::new(&options.terms[0], options.levenshtein_distance.unwrap_or(0))?
    };

    // let stream = map.search(lev).into_stream();
    let hits = if options.starts_with.unwrap_or(false) {
        let stream = map.search(lev.starts_with()).into_stream();
        stream.into_str_vec()?
    } else {
        let stream = map.search(lev).into_stream();
        stream.into_str_vec()?
    };
    // let hits = try!(stream.into_str_vec());
    // debug!("hitso {:?}", hits);

    for (term, id) in hits {
        fun(term, id as u32);
    }

    Ok(())
}

// #[derive(Debug)]
// struct TermnScore {
//     termId: TermId,
//     score: Score
// }

pub type SuggestFieldResult = Vec<(String, Score, TermId)>;

#[cfg_attr(feature = "flame_it", flame)]
fn get_text_score_id_from_result(suggest_text: bool, results: &[SearchFieldResult], skip: Option<usize>, top: Option<usize>) -> SuggestFieldResult {
    let mut suggest_result = results
        .iter()
        .flat_map(|res| {
            res.hits_vec.iter()// @Performance add only "top" elements ?
                .map(|term_n_score| {
                    let term = if suggest_text{
                        &res.terms[&term_n_score.id]
                    }else{
                        &res.highlight[&term_n_score.id]
                    };
                    (term.to_string(), term_n_score.score, term_n_score.id)
                })
                .collect::<SuggestFieldResult>()
        })
        .collect::<SuggestFieldResult>();
    suggest_result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
    search::apply_top_skip(&suggest_result, skip, top)
}
pub fn suggest_multi(persistence: &Persistence, req: Request) -> Result<SuggestFieldResult, SearchError> {
    info_time!("suggest time");
    let search_parts: Vec<RequestSearchPart> = req.suggest
        .ok_or_else(|| SearchError::StringError("only suggest allowed in suggest function".to_string()))?;
    // let mut search_results = vec![];
    let top = req.top;
    let skip = req.skip;
    let search_results: Result<Vec<_>, SearchError> = search_parts
        .into_par_iter()
        .map(|mut search_part| {
            search_part.return_term = Some(true);
            search_part.top = top;
            search_part.skip = skip;
            search_part.resolve_token_to_parent_hits = Some(false);
            get_hits_in_field(persistence, search_part, None)
        })
        .collect();
    // for mut search_part in search_parts {
    //     search_part.return_term = Some(true);
    //     search_part.top = Some(req.top);
    //     search_part.skip = Some(req.skip);
    //     search_part.resolve_token_to_parent_hits = Some(false);
    //     // search_part.term = util::normalize_text(&search_part.term);
    //     // search_part.terms = search_part
    //     //     .terms
    //     //     .iter()
    //     //     .map(|el| util::normalize_text(el))
    //     //     .collect::<Vec<_>>();
    //     // search_results.push(get_hits_in_field(persistence, &search_part, None)?);
    // }
    info_time!("suggest to vec/sort");
    Ok(get_text_score_id_from_result(true, &search_results?, req.skip, req.top))
}

// just adds sorting to search
pub fn suggest(persistence: &Persistence, options: &RequestSearchPart) -> Result<SuggestFieldResult, SearchError> {
    let mut req = Request {
        suggest: Some(vec![options.clone()]),
        ..Default::default()
    };
    req.top = options.top;
    req.skip = options.skip;
    // let options = vec![options.clone()];
    suggest_multi(persistence, req)
}

// just adds sorting to search
pub fn highlight(persistence: &Persistence, options: &mut RequestSearchPart) -> Result<SuggestFieldResult, SearchError> {
    options.terms = options.terms.iter().map(|el| util::normalize_text(el)).collect::<Vec<_>>();

    Ok(get_text_score_id_from_result(
        false,
        &[get_hits_in_field(persistence, options.clone(), None)?],
        options.skip,
        options.top,
    ))
}


#[cfg_attr(feature = "flame_it", flame)]
pub fn get_hits_in_field(persistence: &Persistence, options: RequestSearchPart, filter: Option<&FnvHashSet<u32>>) -> Result<SearchFieldResult, SearchError> {
    let mut options = options.clone();
    options.path = options.path.to_string() + ".textindex";

    if options.terms.len() == 1 {
        let mut hits = get_hits_in_field_one_term(persistence, &mut options, filter)?;
        hits.request = options;
        return Ok(hits);
    } else {
        // let mut all_hits: FnvHashMap<String, SearchFieldResult> = FnvHashMap::default();
        // let mut all_hits_results = vec![];
        // for term in &options.terms {
        //     let mut options = options.clone();
        //     options.terms = vec![term.to_string()];
        //     all_hits_results.push(get_term_ids_in_field(persistence, &mut options)?);
        //     // let hits: SearchFieldResult = get_hits_in_field_one_term(persistence, &mut options, filter)?;
        //     // all_hits.insert(term.to_string(), hits); // todo
        // }

        // get_boost_text_ids(persistence, &options, &all_hits_results)?;

        // if options.fast_field {
        //     for res in all_hits_results.iter_mut() {
        //         *res = resolve_token_to_anchor(persistence, &options, filter, &res)?;
        //     }
        // } else {
        //     // if options.resolve_token_to_parent_hits.unwrap_or(true) {
        //     //     resolve_token_hits(persistence, &options.path, &mut result, options, filter)?;
        //     // }
        // }

        // match options.term_operator {
        //     search::TermOperator::ALL => {
        //         return Ok(search::intersect_hits_vec(all_hits_results))
        //     },
        //     search::TermOperator::ANY => {
        //         return Ok(search::union_hits_vec(all_hits_results))
        //     },
        // }



    }

    Ok(SearchFieldResult::default())
}

fn get_boost_text_ids(persistence: &Persistence, options: &RequestSearchPart, all_hits_results: &Vec<SearchFieldResult>) -> Result<(), SearchError> {
// Boost tokens from same text_id
// Get text_ids for token_ids
    let token_to_text_id = persistence.get_valueid_to_parent(&concat(&options.path, ".tokens_to_parent"))?;
    let mut text_id_bvs = vec![];
    { //TEEEEEEEEEEEEEEEEEEEEEEEEEST
        info_time!(format!("{} WAAAA BITS SETZEN WAAA", &options.path));
        for token_hits in all_hits_results.iter() {
            let mut bv = BitVec::new();
            for hit in &token_hits.hits_vec {
                if let Some(text_ids) = token_to_text_id.get_values(hit.id as u64) {
                    for text_id in text_ids.iter() {
                        let id = *text_id as usize;
                        if bv.len() <= id + 1 {
                            bv.grow(id + 1, false);
                        }
                        bv.set(id, true);
                    }
                }
            }
            text_id_bvs.push(bv);
        }
    }

    Ok(())
}

#[cfg_attr(feature = "flame_it", flame)]
fn get_hits_in_field_one_term(
    persistence: &Persistence,
    options: &mut RequestSearchPart,
    filter: Option<&FnvHashSet<u32>>,
) -> Result<SearchFieldResult, SearchError> {
    debug_time!(format!("{} get_hits_in_field", &options.path));

    let mut result = get_term_ids_in_field(persistence, options)?;

    debug!("{:?} hits in textindex {:?}", result.hits_vec.len(), &options.path);
    trace!("hits in textindex: {:?}", result.hits_vec);

    if options.fast_field {
        result = resolve_token_to_anchor(persistence, options, filter, &result)?;
    } else {
        if options.resolve_token_to_parent_hits.unwrap_or(true) {
            resolve_token_hits(persistence, &options.path, &mut result, options, filter)?;
        }
    }

    Ok(result)
}


use std;

#[cfg_attr(feature = "flame_it", flame)]
fn get_term_ids_in_field(
    persistence: &Persistence,
    options: &mut RequestSearchPart
) -> Result<SearchFieldResult, SearchError> {

    let mut result = SearchFieldResult::default();
    //limit levenshtein distance to reasonable values
    let lower_term = options.terms[0].to_lowercase();
    options.levenshtein_distance.as_mut().map(|d| {
        *d = std::cmp::min(*d, lower_term.chars().count() as u32 - 1);
    });

    trace!("Will Check distance {:?}", options.levenshtein_distance.unwrap_or(0) != 0);
    trace!("Will Check starts_with {:?}", options.starts_with);

    //TODO Move to topn struct
    // let mut vec_hits: Vec<(u32, f32)> = vec![];
    let limit_result = options.top.is_some();
    let mut worst_score = std::f32::MIN;
    let top_n_search = if limit_result {
        (options.top.unwrap() + options.skip.unwrap_or(0)) as u32
    } else {
        std::u32::MAX
    };
    //TODO Move to topnstruct

    {
        debug_time!(format!("{} find token ids", &options.path));
        let lev_automaton_builder = LevenshteinAutomatonBuilder::new(options.levenshtein_distance.unwrap_or(0) as u8, true);

        let dfa = lev_automaton_builder.build_dfa(&lower_term);
        // let search_term_length = &lower_term.chars.count();
        let should_check_prefix_match = options.starts_with.unwrap_or(false) || options.levenshtein_distance.unwrap_or(0) != 0;

        // let exact_search = if options.exact.unwrap_or(false) {Some(options.term.to_string())} else {None};
        if options.ids_only {
            let teh_callback_id_only = |_line: String, line_pos: u32| {
                result.hits_ids.push(line_pos);
            };
            get_text_lines(persistence, options, teh_callback_id_only)?;
        } else {
            let teh_callback = |line: String, line_pos: u32| {
                // trace!("Checking {} with {}", line, term);

                let line_lower = line.to_lowercase();

                // In the case of levenshtein != 0 or starts_with, we want prefix_matches to have a score boost - so that "awe" scores better for awesome than aber
                let prefix_matches = if should_check_prefix_match && line_lower.starts_with(&lower_term) {
                    true
                } else {
                    false
                };

                //TODO: find term for multitoken
                let mut score = get_default_score_for_distance(distance_dfa(&line_lower, &dfa, &lower_term), prefix_matches);
                options.boost.map(|boost_val| score *= boost_val);

                if limit_result {
                    if score < worst_score {
                        // debug!("ABORT SCORE {:?}", score);
                        return;
                    }
                    if !result.hits_vec.is_empty() && (result.hits_vec.len() as u32 % (top_n_search * 5)) == 0 {
                        result
                            .hits_vec
                            .sort_unstable_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
                        result.hits_vec.truncate(top_n_search as usize);
                        worst_score = result.hits_vec.last().unwrap().score;
                        trace!("new worst {:?}", worst_score);
                    }

                    // vec_hits.push((line_pos, score));
                    result.hits_vec.push(Hit::new(line_pos, score));
                    debug!("Hit: {:?}\tid: {:?} score: {:?}", line, line_pos, score);

                    if options.return_term.unwrap_or(false) {
                        result.terms.insert(line_pos, line);
                    }
                    return;
                }
                debug!("Hit: {:?}\tid: {:?} score: {:?}", &line, line_pos, score);

                // result.hits.insert(line_pos, score);
                result.hits_vec.push(Hit::new(line_pos, score));

                if options.return_term.unwrap_or(false) {
                    result.terms.insert(line_pos, line);
                }
            };

            get_text_lines(persistence, options, teh_callback)?;
        }
    }

    {
        if limit_result {
            result
                .hits_vec
                .sort_unstable_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
            result.hits_vec.truncate(top_n_search as usize);
            // result.hits = result.hits_vec..into_iter().collect();
        }
    }

    // Store token_id hit for why_found
    if options.store_term_id_hits && !result.hits_vec.is_empty(){
        let mut map = FnvHashMap::default();
        map.insert(options.terms[0].clone(), result.hits_vec.iter().map(|el| el.id).collect()); // TODO Avoid copy? just store hit?
        result.term_id_hits_in_field.insert(options.path.to_string(), map);
    }

    if options.token_value.is_some() {
        debug!("Token Boosting: \n");
        search::add_boost(persistence, options.token_value.as_ref().unwrap(), &mut result)?;
    }

    Ok(result)

}

#[cfg_attr(feature = "flame_it", flame)]
fn resolve_token_to_anchor(
    persistence: &Persistence,
    options: &RequestSearchPart,
    filter: Option<&FnvHashSet<u32>>,
    result: &SearchFieldResult,
) -> Result<SearchFieldResult, SearchError> {

    let mut res = SearchFieldResult::new_from(&result);
    debug_time!(format!("{} fast_field", &options.path));
    let mut anchor_ids_hits = vec![];
    let token_kvdata = persistence.get_valueid_to_parent(&concat(&options.path, ".tokens.to_anchor_id_score"))?;
    {
        debug_time!(format!("{} tokens.to_anchor_id_score", &options.path));
        for hit in &result.hits_vec {
            // iterate over token hits
            if let Some(text_id_score) = token_kvdata.get_values(hit.id as u64) {
                trace_time!(format!("{} adding anchor hits for id {:?}", &options.path, hit.id));
                let mut curr_pos = unsafe_increase_len(&mut anchor_ids_hits, text_id_score.len() / 2);
                for (anchor_id, token_in_text_id_score) in text_id_score.iter().tuples().filter(|&(id, _)|!should_filter(&filter, &id))  {

                    let final_score = hit.score * (*token_in_text_id_score as f32 / 100.0); // TODO ADD LIMIT FOR TOP X
                    // trace!(
                    //     "anchor_id {:?} term_id {:?}, token_in_text_id_score {:?} score {:?} to final_score {:?}",
                    //     anchor_id,
                    //     hit.id,
                    //     token_in_text_id_score,
                    //     hit.score,
                    //     final_score
                    // );

                    // anchor_ids_hits.push(Hit::new(*anchor_id,final_score));
                    anchor_ids_hits[curr_pos] = search::Hit::new(*anchor_id, final_score);
                    curr_pos += 1;
                }
            }
        }
        debug!("{} found {:?} token in {:?} anchor_ids", &options.path, result.hits_vec.len(), anchor_ids_hits.len());
    }

    // { //TEEEEEEEEEEEEEEEEEEEEEEEEEST
    //     let mut the_bits = FixedBitSet::with_capacity(7000);
    //     info_time!(format!("{} WAAAA BITS SETZEN WAAA", &options.path));
    //     for hit in &result.hits_vec {
    //         // iterate over token hits
    //         if let Some(text_id_score) = token_kvdata.get_values(hit.id as u64) {
    //             //trace_time!(format!("{} adding anchor hits for id {:?}", &options.path, hit.id));
    //             for (text_id, _) in text_id_score.iter().tuples() {
    //                 let yep = *text_id as usize;
    //                 if the_bits.len() <= yep + 1{
    //                     the_bits.grow(yep + 1);
    //                 }
    //                 the_bits.insert(yep);
    //             }
    //         }
    //     }
    // }

    // let text_id_to_anchor = persistence.get_valueid_to_parent(&concat(&options.path, ".text_id_to_anchor"))?;
    // let mut anchor_hits = vec![];
    // {
    //     debug_time!(format!("{} .text_id_to_anchor", &options.path));
    //     //resolve text_ids with score to anchor
    //     for hit in anchor_ids_hits.iter() {
    //         if let Some(anchor_ids) = text_id_to_anchor.get_values(hit.id as u64) {
    //             let mut curr_pos = unsafe_increase_len(&mut anchor_hits, anchor_ids.len());
    //             for anchor_id in anchor_ids.into_iter().filter(|id|!should_filter(&filter, &id)) {
    //                 anchor_hits[curr_pos] = search::Hit::new(anchor_id, hit.score);
    //                 curr_pos += 1;
    //             }
    //         }
    //     }
    // }

    // debug!("found {:?} text_ids in {:?} anchors", anchor_ids_hits.len(), anchor_hits.len());

    // {
    //     //Collect hits from same anchor and sum boost
    //     let mut merged_fast_field_res = vec![];
    //     debug_time!(format!("{} sort and merge fast_field", &options.path));
    //     anchor_hits.sort_unstable_by(|a, b| b.id.partial_cmp(&a.id).unwrap_or(Ordering::Equal));
    //     for (text_id, group) in &anchor_hits.iter().group_by(|el| el.id) {
    //         merged_fast_field_res.push(search::Hit::new(text_id, group.map(|el| el.score).sum())) //Todo FixMe Perofrmance avoid copy inplace group by
    //     }
    //     anchor_hits = merged_fast_field_res;
    // }

    {
        debug_time!(format!("{} fast_field sort and dedup sum", &options.path));
        // anchor_ids_hits.sort_unstable_by(|a, b| b.id.partial_cmp(&a.id).unwrap_or(Ordering::Equal)); //TODO presort data in persistence, k_merge token_hits
        anchor_ids_hits.sort_unstable_by_key(|a| a.id); //TODO presort data in persistence, k_merge token_hits
        anchor_ids_hits.dedup_by( |a, b|{
            if a.id == b.id{
                b.score += a.score; // TODO: Check if b is always kept and a discarded in case of equality
                true
            }else{
                false
            }
        });
        // anchor_ids_hits.dedup_by_key(|b| b.id); // TODO FixMe Score
    }

    // IDS ONLY - scores müssen draußen bleiben
    let mut fast_field_res_ids = vec![];
    for id in &result.hits_ids {
        // iterate over token hits
        if let Some(anchor_id_score) = token_kvdata.get_values(*id as u64) {
            debug_time!(format!("{} adding {:?} anchor ids for id {:?}", &options.path, anchor_id_score.len(), id));
            let mut curr_pos = unsafe_increase_len(&mut fast_field_res_ids, anchor_id_score.len() / 2);
            for text_id in anchor_id_score.iter().step_by(2) {
                fast_field_res_ids[curr_pos] = *text_id;
                curr_pos += 1;
            }
        }
    }

    // //resolve text_ids to anchor
    // let mut fast_field_res_ids = vec![];
    // for id in text_ids_hit_ids {
    //     if let Some(anchor_ids) = text_id_to_anchor.get_values(id as u64) {
    //         let mut curr_pos = unsafe_increase_len(&mut fast_field_res_ids, anchor_ids.len());
    //         for anchor_id in anchor_ids.into_iter().filter(|id|!should_filter(&filter, &id)) {

    //             fast_field_res_ids[curr_pos] = anchor_id;
    //             curr_pos += 1;
    //         }
    //     }
    // }
    res.hits_ids = fast_field_res_ids;
    // // IDS ONLY - scores müssen draußen bleiben


    trace!("anchor id hits {:?}", anchor_ids_hits);
    res.hits_vec = anchor_ids_hits;


    Ok(res)
}


#[cfg_attr(feature = "flame_it", flame)]
pub fn get_text_for_ids(persistence: &Persistence, path: &str, ids: &[u32]) -> Vec<String> {
    // let mut faccess: persistence::FileSearch = persistence.get_file_search(path);
    // let offsets = persistence.get_offsets(path).unwrap();
    ids.iter().map(|id| get_text_for_id(persistence, path, *id)).collect()
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn get_text_for_id_disk(persistence: &Persistence, path: &str, id: u32) -> String {
    let mut faccess: persistence::FileSearch = persistence.get_file_search(path);
    let offsets = persistence.get_offsets(path).unwrap();
    faccess.get_text_for_id(id as usize, &**offsets)
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn get_text_for_id(persistence: &Persistence, path: &str, id: u32) -> String {
    let map = persistence.cache.fst.get(path).expect(&format!("fst not found loaded in cache {} ", path));

    let mut bytes = vec![];
    ord_to_term(map.as_fst(), id as u64, &mut bytes);
    str::from_utf8(&bytes).unwrap().to_string()
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn get_text_for_id_2(persistence: &Persistence, path: &str, id: u32, bytes: &mut Vec<u8>) {
    let map = persistence.cache.fst.get(path).expect(&format!("fst not found loaded in cache {} ", path));
    ord_to_term(map.as_fst(), id as u64, bytes);
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn get_id_text_map_for_ids(persistence: &Persistence, path: &str, ids: &[u32]) -> FnvHashMap<u32, String> {
    let map = persistence.cache.fst.get(path).expect(&format!("fst not found loaded in cache {} ", path));
    ids.iter()
        .map(|id| {
            let mut bytes = vec![];
            ord_to_term(map.as_fst(), *id as u64, &mut bytes);
            (*id, str::from_utf8(&bytes).unwrap().to_string())
        })
        .collect()
}

// #[cfg_attr(feature="flame_it", flame)]
// pub fn resolve_snippets(persistence: &Persistence, path: &str, result: &mut SearchFieldResult) -> Result<(), search::SearchError> {
//     let token_kvdata = persistence.get_valueid_to_parent(&concat(path, ".tokens"))?;
//     let mut value_id_to_token_hits: FnvHashMap<u32, Vec<u32>> = FnvHashMap::default();
//     //TODO snippety only for top x best scores?
//     for (token_id, _) in result.hits.iter() {
//         if let Some(parent_ids_for_token) = token_kvdata.get_values(*token_id as u64) {
//             for token_parentval_id in parent_ids_for_token {
//                 value_id_to_token_hits
//                     .entry(token_parentval_id)
//                     .or_insert(vec![])
//                     .push(*token_id);
//             }
//         }
//     }
// }

fn should_filter(filter: &Option<&FnvHashSet<u32>>, id: &u32) -> bool {
    filter.map(|filter| filter.contains(id)).unwrap_or(false)
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn resolve_token_hits(
    persistence: &Persistence,
    path: &str,
    result: &mut SearchFieldResult,
    options: &RequestSearchPart,
    filter: Option<&FnvHashSet<u32>>,
) -> Result<(), search::SearchError> {
    let has_tokens = persistence
        .meta_data
        .fulltext_indices
        .get(path)
        .map_or(false, |fulltext_info| fulltext_info.tokenize);
    debug!("has_tokens {:?} {:?} is_fast_field {}", path, has_tokens, options.fast_field);
    if !has_tokens && !options.fast_field {
        return Ok(());
    }

    let add_snippets = options.snippet.unwrap_or(false);

    debug_time!(format!("{} resolve_token_hits", path));
    let text_offsets = persistence
        .get_offsets(path)
        .expect(&format!("Could not find {:?} in index_64 cache", concat(path, ".offsets")));

    let token_path = concat(path, ".tokens_to_parent");

    let token_kvdata = persistence.get_valueid_to_parent(&token_path)?;
    debug!("Checking Tokens in {:?}", &token_path);
    persistence::trace_index_id_to_parent(token_kvdata);
    // trace!("All Tokens: {:?}", token_kvdata.get_values());

    // let mut token_hits:FnvHashMap<u32, f32> = FnvHashMap::default();
    let mut token_hits: Vec<(u32, f32, u32)> = vec![];
    // let mut anchor_hits = FnvHashMap::default();
    {
        //VEC VERSION
        debug_time!(format!("{} adding parent_id from tokens", token_path));
        for hit in &result.hits_vec {
            if let Some(parent_ids_for_token) = token_kvdata.get_values(hit.id as u64) {
                let token_text_length_offsets = text_offsets.get_mutliple_value(hit.id as usize..=hit.id as usize + 1).unwrap();
                let token_text_length = token_text_length_offsets[1] - token_text_length_offsets[0];

                token_hits.reserve(parent_ids_for_token.len());
                for token_parentval_id in parent_ids_for_token {
                    if should_filter(&filter, &token_parentval_id) {
                        continue;
                    }

                    if let Some(offsets) = text_offsets.get_mutliple_value(token_parentval_id as usize..=token_parentval_id as usize + 1) {
                        // TODO replace with different scoring algorithm, not just length
                        let parent_text_length = offsets[1] - offsets[0];
                        let adjusted_score = hit.score * (token_text_length as f32 / parent_text_length as f32);
                        trace!(
                            "value_id {:?} parent_l {:?}, token_l {:?} score {:?} to adjusted_score {:?}",
                            token_parentval_id,
                            parent_text_length,
                            token_text_length,
                            hit.score,
                            adjusted_score
                        );
                        token_hits.push((token_parentval_id, adjusted_score, hit.id));
                    }
                }
            }
        }

        result.hits_ids = result
            .hits_ids
            .iter()
            .flat_map(|id| token_kvdata.get_values(*id as u64))
            .flat_map(|el| el)
            .collect();
    }

    debug!("found {:?} token in {:?} texts", result.hits_vec.iter().count(), token_hits.iter().count());
    {
        debug_time!(format!("token_hits.sort_by {:?}", path));
        token_hits.sort_unstable_by(|a, b| a.0.cmp(&b.0)); // sort by parent id
    }
    debug_time!(format!("{} extend token_results", path));
    // hits.extend(token_hits);
    trace!("{} token_hits in textindex: {:?}", path, token_hits);
    if !token_hits.is_empty() {
        if add_snippets {
            result.hits_vec.clear(); //only document hits for highlightung
        }
        // token_hits.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal)); // sort by parent_id=value_id
        result.hits_vec.reserve(token_hits.len());

        for (parent_id, group) in &token_hits.iter().group_by(|el| el.0) {
            //Group by anchor
            let (mut t1, t2) = group.tee();
            let max_score = t1.max_by_key(|el| OrderedFloat(el.1.abs())).unwrap().1;

            result.hits_vec.push(Hit::new(parent_id, max_score));
            if add_snippets {
                //value_id_to_token_hits.insert(parent_id, t2.map(|el| el.2).collect_vec()); //TODO maybe store hits here, in case only best x are needed
                let snippet_config = options.snippet_info.as_ref().unwrap_or(&search::DEFAULT_SNIPPETINFO);
                let highlighted_document = highlight_document(persistence, path, parent_id as u64, &t2.map(|el| el.2).collect_vec(), snippet_config)?;
                if let Some(highlighted_document) = highlighted_document {
                    result.highlight.insert(parent_id, highlighted_document);
                }
            }
        }
    }
    trace!("{} hits with tokens: {:?}", path, result.hits_vec);
    // for hit in hits.iter() {
    //     trace!("NEW HITS {:?}", hit);
    // }
    Ok(())
}

// fn highlight_and_store(persistence: &Persistence, path: &str, valueid:u32, result: &mut FnvHashMap<TermId, String>, snippet_info: &SnippetInfo) -> Result<(), SearchError> {
//     let highlighted_document = highlight_document(persistence, path, valueid as u64, &t2.map(|el| el.2).collect_vec(), snippet_info)?;
//     result.insert(valueid, highlighted_document);
//     Ok(())
// }

fn distance_dfa(lower_hit: &str, dfa: &DFA, lower_term: &str) -> u8 {
    // let lower_hit = hit.to_lowercase();
    let mut state = dfa.initial_state();
    for &b in lower_hit.as_bytes() {
        state = dfa.transition(state, b);
    }

    match dfa.distance(state) {
        Distance::Exact(ok) => ok,
        Distance::AtLeast(_) => distance(lower_hit, lower_term),
    }
}
fn distance(s1: &str, s2: &str) -> u8 {
    let len_s1 = s1.chars().count();

    let mut column: Vec<u8> = Vec::with_capacity(len_s1 + 1);
    unsafe {
        column.set_len(len_s1 + 1);
    }
    for x in 0..len_s1 + 1 {
        column[x] = x as u8;
    }

    for (x, current_char2) in s2.chars().enumerate() {
        column[0] = x as u8 + 1;
        let mut lastdiag = x as u8;
        for (y, current_char1) in s1.chars().enumerate() {
            if current_char1 != current_char2 {
                lastdiag += 1
            }
            let olddiag = column[y + 1];
            column[y + 1] = cmp::min(column[y + 1] + 1, cmp::min(column[y] + 1, lastdiag));
            lastdiag = olddiag;
        }
    }
    column[len_s1]
}

// #[test]
// fn test_dfa() {
//     let lev_automaton_builder = LevenshteinAutomatonBuilder::new(2, true);

//     // We can now build an entire dfa.
//     let dfa = lev_automaton_builder.build_dfa("saucisson sec");

//     let mut state = dfa.initial_state();
//         for &b in "saucissonsec".as_bytes() {
//         state = dfa.transition(state, b);
//     }

//    assert_eq!(dfa.distance(state), Distance::Exact(1));
// }
