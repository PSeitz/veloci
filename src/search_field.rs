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

#[allow(unused_imports)]
use rayon::prelude::*;

#[allow(unused_imports)]
use trie::map;

#[derive(Debug, Default)]
pub struct SearchFieldResult {
    pub hits: FnvHashMap<TermId, f32>,
    pub hits_vec: Vec<search::Hit>,
    pub terms: FnvHashMap<TermId, String>,
    pub highlight: FnvHashMap<TermId, String>,
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
        return 2.0 / ((distance as f32 + 1.0).log2() + 0.2);
    } else {
        return 2.0 / (distance as f32 + 0.2);
    }
}

pub fn ord_to_term(fst: &Fst, mut ord: u64, bytes: &mut Vec<u8>) -> bool {
    bytes.clear();
    let mut node = fst.root();
    while ord != 0 || !node.is_final() {
        let transition_opt = node.transitions()
            .take_while(|transition| transition.out.value() <= ord)
            .last();
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

#[inline(always)]
#[flame]
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
        .ok_or(SearchError::StringError(format!(
            "fst not found loaded in cache {} ",
            options.path
        )))?;
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

#[flame]
fn get_text_score_id_from_result(suggest_text: bool, results: Vec<SearchFieldResult>, skip: Option<usize>, top: Option<usize>) -> SuggestFieldResult {
    let mut suggest_result = results
        .iter()
        .flat_map(|res| {
            res.hits_vec.iter()// @Performance add only "top" elements ?
                .map(|term_n_score| {
                    let term = if suggest_text{
                        res.terms.get(&term_n_score.id).unwrap()
                    }else{
                        res.highlight.get(&term_n_score.id).unwrap()
                    };
                    (term.to_string(), term_n_score.score, term_n_score.id)
                })
                .collect::<SuggestFieldResult>()
        })
        .collect::<SuggestFieldResult>();
    suggest_result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
    search::apply_top_skip(suggest_result, skip, top)
}
pub fn suggest_multi(persistence: &Persistence, req: Request) -> Result<SuggestFieldResult, SearchError> {
    info_time!("suggest time");
    let search_parts: Vec<RequestSearchPart> = req.suggest.ok_or(SearchError::StringError(
        "only suggest allowed in suggest function".to_string(),
    ))?;
    // let mut search_results = vec![];
    let top = req.top.clone();
    let skip = req.skip.clone();
    let search_results: Result<Vec<_>, SearchError> = search_parts
        .into_par_iter()
        .map(|ref mut search_part| {
            search_part.return_term = Some(true);
            search_part.top = top;
            search_part.skip = skip;
            search_part.resolve_token_to_parent_hits = Some(false);
            get_hits_in_field(persistence, &search_part, None)
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
    Ok(get_text_score_id_from_result(
        true,
        search_results?,
        req.skip,
        req.top,
    ))
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
    return suggest_multi(persistence, req);
}

// just adds sorting to search
pub fn highlight(persistence: &Persistence, options: &mut RequestSearchPart) -> Result<SuggestFieldResult, SearchError> {
    options.terms = options
        .terms
        .iter()
        .map(|el| util::normalize_text(el))
        .collect::<Vec<_>>();

    Ok(get_text_score_id_from_result(
        false,
        vec![get_hits_in_field(persistence, &options, None)?],
        options.skip,
        options.top,
    ))
}

#[flame]
pub fn get_hits_in_field(persistence: &Persistence, options: &RequestSearchPart, filter: Option<&FnvHashSet<u32>>) -> Result<SearchFieldResult, SearchError> {
    let mut options = options.clone();
    options.path = options.path.to_string() + ".textindex";

    if options.terms.len() == 1 {
        return get_hits_in_field_one_term(&persistence, &options, filter);
    } else {
        let mut all_hits: FnvHashMap<String, SearchFieldResult> = FnvHashMap::default();
        for term in &options.terms {
            let mut options = options.clone();
            options.terms = vec![term.to_string()];
            let hits: SearchFieldResult = get_hits_in_field_one_term(&persistence, &options, filter)?;
            all_hits.insert(term.to_string(), hits); // todo
        }
    }

    Ok(SearchFieldResult::default())
}
use std;
#[flame]
fn get_hits_in_field_one_term(persistence: &Persistence, options: &RequestSearchPart, filter: Option<&FnvHashSet<u32>>) -> Result<SearchFieldResult, SearchError> {
    debug_time!(format!("{} get_hits_in_field", &options.path));
    // let mut hits:FnvHashMap<u32, f32> = FnvHashMap::default();
    let mut result = SearchFieldResult::default();
    // let mut hits:Vec<(u32, f32)> = vec![];
    // let checks:Vec<Fn(&str) -> bool> = Vec::new();
    // options.first_char_exact_match = options.exact || options.levenshtein_distance == 0 || options.starts_with.is_some(); // TODO fix

    // if options.levenshtein_distance.unwrap_or(0) == 0 && !options.starts_with.unwrap_or(false) {
    //     options.exact = Some(true);
    // }

    // let term_chars = options.term.chars().collect::<Vec<char>>();
    // let start_char = if options.exact.unwrap_or(false) || options.levenshtein_distance.unwrap_or(0) == 0 || options.starts_with.unwrap_or(false) && term_chars.len() >= 2 {
    //     Some(term_chars[0].to_string() + &term_chars[1].to_string())
    // }
    // else if options.first_char_exact_match.unwrap_or(false) { Some(term_chars[0].to_string() )
    // }
    // else { None };
    // let start_char_val = start_char.as_ref().map(String::as_ref);

    trace!(
        "Will Check distance {:?}",
        options.levenshtein_distance.unwrap_or(0) != 0
    );
    // trace!("Will Check exact {:?}", options.exact);
    trace!("Will Check starts_with {:?}", options.starts_with);

    //TODO Move to topn struct
    // let mut vec_hits: Vec<(u32, f32)> = vec![];
    let limit_result = options.top.is_some();
    let mut worst_score = std::f32::MIN;
    let mut top_n_search = std::u32::MAX;
    if limit_result {
        top_n_search = (options.top.unwrap() + options.skip.unwrap_or(0)) as u32;
    }
    //TODO Move to topnstruct

    {
        debug_time!(format!("{} levenschwein", &options.path));
        let lev_automaton_builder = LevenshteinAutomatonBuilder::new(options.levenshtein_distance.unwrap_or(0) as u8, true);
        let lower_term = options.terms[0].to_lowercase();
        let dfa = lev_automaton_builder.build_dfa(&lower_term);
        // let search_term_length = &lower_term.chars.count();
        let should_check_prefix_match = options.starts_with.unwrap_or(false) || options.levenshtein_distance.unwrap_or(0) != 0;

        let teh_callback = |line: String, line_pos: u32| {
            // trace!("Checking {} with {}", line, term);

            let line_lower = line.to_lowercase();

            // In the case of levenshtein != 0 or starts_with, we want prefix_matches to have a score boost - so that "awe" scores better for awesome than aber
            let mut prefix_matches = false;
            if should_check_prefix_match && line_lower.starts_with(&lower_term) {
                prefix_matches = true;
            }

            // let distance = if options.levenshtein_distance.unwrap_or(0) != 0 {
            //     // Some(distance(&options.terms[0], &line))
            //     Some(distance_dfa(&line, &dfa))
            // } else {
            //     None
            // };
            //TODO: find term for multitoken

            let mut score = get_default_score_for_distance(distance_dfa(&line_lower, &dfa, &lower_term), prefix_matches);

            // let mut score = if options.levenshtein_distance.unwrap_or(0) != 0 {
            //     get_default_score_for_distance(distance_dfa(&line, &dfa, &lower_term), prefix_matches)
            // } else {
            //     get_default_score(&options.terms[0], &line, prefix_matches)
            //     // get_default_score_for_distance(0, prefix_matches)
            // };
            options.boost.map(|boost_val| score = score * boost_val); // @FixMe Move out of loop?
                                                                      // hits.insert(line_pos, score);
                                                                      // result.hits.push(Hit{id:line_pos, score:score});

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
        // let exact_search = if options.exact.unwrap_or(false) {Some(options.term.to_string())} else {None};
        get_text_lines(persistence, options, teh_callback)?;
    }

    {
        if limit_result {
            // println!("HITZZZ {:?}", result.hits_vec.);
            result
                .hits_vec
                .sort_unstable_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
            result.hits_vec.truncate(top_n_search as usize);
            // result.hits = result.hits_vec..into_iter().collect();
        }
    }

    debug!(
        "{:?} hits in textindex {:?}",
        result.hits_vec.len(),
        &options.path
    );
    trace!("hits in textindex: {:?}", result.hits_vec);

    if options.fast_field {
        //VEC VERSION
        debug_time!(format!("{} fast_field", &options.path));
        let mut fast_field_res = vec![];
        for hit in result.hits_vec.iter() {
            let token_kvdata = persistence.get_valueid_to_parent(&concat(&options.path, ".tokens.to_anchor"))?;

            token_kvdata.add_fast_field_hits(*hit, &mut fast_field_res, filter);

            // if let Some(anchor_score) = token_kvdata.get_values(hit.id as u64) {
            //     debug_time!(format!("{} adding stuff", &options.path));
            //     fast_field_res.reserve(1 + anchor_score.len() / 2);
            //     for (anchor_id, token_in_anchor_score) in anchor_score.iter().tuples() {
            //         if let Some(filter) = filter {
            //             if filter.contains(&anchor_id) {
            //                 continue;
            //             }
            //         }

            //         let final_score = hit.score * (*token_in_anchor_score as f32);
            //         trace!(
            //             "anchor_id {:?} term_id {:?}, token_in_anchor_score {:?} score {:?} to final_score {:?}",
            //             anchor_id,
            //             hit.id,
            //             token_in_anchor_score,
            //             hit.score,
            //             final_score
            //         );

            //         fast_field_res.push(Hit::new(*anchor_id,final_score));
            //     }
            // }
        }

        debug!(
            "found {:?} token in {:?} anchors",
            result.hits_vec.len(),
            fast_field_res.len()
        );

        {
            debug_time!(format!("{} fast_field sort and dedup", &options.path));
            fast_field_res.sort_unstable_by(|a, b| b.id.partial_cmp(&a.id).unwrap_or(Ordering::Equal)); //TODO presort data in persistence, k_merge token_hits
            fast_field_res.dedup_by_key(|b| b.id); // TODO FixMe Score
        }

        result.hits_vec = fast_field_res;

    // //HASHMAP VERSION
    // debug_time!(format!("{} fast_field", &options.path));
    // let mut fast_field_res = FnvHashMap::default();
    // for (term_id, score) in result.hits.iter() {
    //     let token_kvdata = persistence.get_valueid_to_parent(&concat(&options.path, ".tokens.to_anchor"))?;

    //     if let Some(anchor_score) = token_kvdata.get_values(*term_id as u64) {
    //         fast_field_res.reserve(anchor_score.len() / 2);
    //         for (anchor_id, token_in_anchor_score) in anchor_score.iter().tuples() {
    //             if let Some(filter) = filter {
    //                 if filter.contains(&anchor_id) {
    //                     continue;
    //                 }
    //             }

    //             let final_score = score * (*token_in_anchor_score as f32);
    //             trace!(
    //                 "anchor_id {:?} term_id {:?}, token_in_anchor_score {:?} score {:?} to final_score {:?}",
    //                 anchor_id,
    //                 term_id,
    //                 token_in_anchor_score,
    //                 score,
    //                 final_score
    //             );

    //             // anchor_hits.insert(*anchor_id as u32, score * (*token_in_anchor_score as f32));
    //             // fast_field_res.insert(*anchor_id as u32, final_score); //take max
    //             // let entry = fast_field_res.entry(*anchor_id as u32);
    //             fast_field_res
    //                 .entry(*anchor_id as u32)
    //                 .and_modify(|e| {
    //                     if *e < final_score {
    //                         *e = final_score;
    //                     }
    //                 })
    //                 .or_insert(final_score);
    //         }
    //     }
    // }

    // debug!(
    //     "found {:?} token in {:?} anchors",
    //     result.hits.len(),
    //     fast_field_res.len()
    // );

    // result.hits = fast_field_res;
    } else {
        if options.resolve_token_to_parent_hits.unwrap_or(true) {
            resolve_token_hits(persistence, &options.path, &mut result, options, filter)?;
        }
    }

    if options.token_value.is_some() {
        debug!("Token Boosting: \n");
        search::add_boost(
            persistence,
            options.token_value.as_ref().unwrap(),
            &mut result,
        )?;

        // for el in result.hits.iter_mut() {
        //     el.score = *hits.get(&el.id).unwrap();
        // }
    }

    Ok(result)
}

#[flame]
pub fn get_text_for_ids(persistence: &Persistence, path: &str, ids: &[u32]) -> Vec<String> {
    let mut faccess: persistence::FileSearch = persistence.get_file_search(path);
    let offsets = persistence.get_offsets(path).unwrap();
    ids.iter()
        .map(|id| faccess.get_text_for_id(*id as usize, &**offsets))
        .collect()
}

#[flame]
pub fn get_text_for_id_disk(persistence: &Persistence, path: &str, id: u32) -> String {
    let mut faccess: persistence::FileSearch = persistence.get_file_search(path);
    let offsets = persistence.get_offsets(path).unwrap();
    faccess.get_text_for_id(id as usize, &**offsets)
}

#[flame]
pub fn get_text_for_id(persistence: &Persistence, path: &str, id: u32) -> String {
    let map = persistence
        .cache
        .fst
        .get(path)
        .expect(&format!("fst not found loaded in cache {} ", path));

    let mut bytes = vec![];
    ord_to_term(map.as_fst(), id as u64, &mut bytes);
    str::from_utf8(&bytes).unwrap().to_string()
}

#[flame]
pub fn get_text_for_id_2(persistence: &Persistence, path: &str, id: u32, bytes: &mut Vec<u8>) {
    let map = persistence
        .cache
        .fst
        .get(path)
        .expect(&format!("fst not found loaded in cache {} ", path));
    ord_to_term(map.as_fst(), id as u64, bytes);
}

#[flame]
pub fn get_id_text_map_for_ids(persistence: &Persistence, path: &str, ids: &[u32]) -> FnvHashMap<u32, String> {
    let map = persistence
        .cache
        .fst
        .get(path)
        .expect(&format!("fst not found loaded in cache {} ", path));
    ids.iter()
        .map(|id| {
            let mut bytes = vec![];
            ord_to_term(map.as_fst(), *id as u64, &mut bytes);
            (*id, str::from_utf8(&bytes).unwrap().to_string())
        })
        .collect()
}

// #[flame]
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

#[flame]
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
    debug!(
        "has_tokens {:?} {:?} is_fast_field {}",
        path, has_tokens, options.fast_field
    );
    if !has_tokens && !options.fast_field {
        return Ok(());
    }

    let add_snippets = options.snippet.unwrap_or(false);

    debug_time!(format!("{} resolve_token_hits", path));
    let text_offsets = persistence.get_offsets(path).expect(&format!(
        "Could not find {:?} in index_64 cache",
        concat(path, ".offsets")
    ));

    let token_path = concat(path, ".tokens");

    let token_kvdata = persistence.get_valueid_to_parent(&token_path)?;
    debug!("Checking Tokens in {:?}", &token_path);
    persistence::trace_index_id_to_parent(token_kvdata);
    // trace!("All Tokens: {:?}", token_kvdata.get_values());

    // let token_kvdata = persistence.cache.index_id_to_parent.get(&key).expect(&format!("Could not find {:?} in index_id_to_parent cache", key));
    // let mut token_hits:FnvHashMap<u32, f32> = FnvHashMap::default();
    let mut token_hits: Vec<(u32, f32, u32)> = vec![];
    // let mut anchor_hits = FnvHashMap::default();
    {
        //VEC VERSION
        debug_time!(format!("{} adding parent_id from tokens", token_path));
        for hit in result.hits_vec.iter() {
            // let ref parent_ids_for_token_opt = token_kvdata.get(*value_id as usize);
            if let Some(parent_ids_for_token) = token_kvdata.get_values(hit.id as u64) {
                let token_text_length_offsets = text_offsets
                    .get_mutliple_value(hit.id as usize..=hit.id as usize + 1)
                    .unwrap();
                let token_text_length = token_text_length_offsets[1] - token_text_length_offsets[0];

                token_hits.reserve(parent_ids_for_token.len());
                for token_parentval_id in parent_ids_for_token {
                    if let Some(filter) = filter {
                        if filter.contains(&token_parentval_id) {
                            continue;
                        }
                    }

                    if let Some(offsets) = text_offsets.get_mutliple_value(token_parentval_id as usize..=token_parentval_id as usize + 1) {
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
        // //HASHMAP VERSION
        // debug_time!(format!("{} adding parent_id from tokens", token_path));
        // for (term_id, score) in result.hits.iter() {
        //     // let ref parent_ids_for_token_opt = token_kvdata.get(*value_id as usize);
        //     if let Some(parent_ids_for_token) = token_kvdata.get_values(*term_id as u64) {
        //         let token_text_length_offsets = text_offsets
        //             .get_mutliple_value(*term_id as usize..=*term_id as usize + 1)
        //             .unwrap();
        //         let token_text_length = token_text_length_offsets[1] - token_text_length_offsets[0];

        //         token_hits.reserve(parent_ids_for_token.len());
        //         for token_parentval_id in parent_ids_for_token {
        //             if let Some(filter) = filter {
        //                 if filter.contains(&token_parentval_id) {
        //                     continue;
        //                 }
        //             }

        //             if let Some(offsets) = text_offsets.get_mutliple_value(token_parentval_id as usize..=token_parentval_id as usize + 1) {
        //                 let parent_text_length = offsets[1] - offsets[0];
        //                 let adjusted_score = score * (token_text_length as f32 / parent_text_length as f32);
        //                 trace!(
        //                     "value_id {:?} parent_l {:?}, token_l {:?} score {:?} to adjusted_score {:?}",
        //                     token_parentval_id,
        //                     parent_text_length,
        //                     token_text_length,
        //                     score,
        //                     adjusted_score
        //                 );
        //                 token_hits.push((token_parentval_id, adjusted_score, *term_id));
        //             }
        //         }
        //     }
        // }
    }

    debug!(
        "found {:?} token in {:?} texts",
        result.hits_vec.iter().count(),
        token_hits.iter().count()
    );
    {
        // println!("{:?}", token_hits);
        debug_time!(format!("token_hits.sort_by {:?}", path));
        token_hits.sort_unstable_by(|a, b| a.0.cmp(&b.0)); // sort by parent id
    }
    debug_time!(format!("{} extend token_results", path));
    // hits.extend(token_hits);
    trace!("{} token_hits in textindex: {:?}", path, token_hits);
    if token_hits.len() > 0 {
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
                let snippet_config = options
                    .snippet_info
                    .as_ref()
                    .unwrap_or(&search::DEFAULT_SNIPPETINFO);
                let highlighted_document = highlight_document(
                    persistence,
                    path,
                    parent_id as u64,
                    &t2.map(|el| el.2).collect_vec(),
                    snippet_config,
                )?;
                result.highlight.insert(parent_id, highlighted_document);
            }
        }
    }
    trace!("{} hits with tokens: {:?}", path, result.hits_vec);
    // for hit in hits.iter() {
    //     trace!("NEW HITS {:?}", hit);
    // }
    Ok(())
}

fn distance_dfa(lower_hit: &str, dfa: &DFA, lower_term: &str) -> u8 {
    // let lower_hit = hit.to_lowercase();
    let mut state = dfa.initial_state();
    for &b in lower_hit.as_bytes() {
        state = dfa.transition(state, b);
    }

    match dfa.distance(state) {
        Distance::Exact(ok) => ok,
        Distance::AtLeast(_) => distance(&lower_hit, lower_term),
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
