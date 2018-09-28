use fnv::{FnvHashMap, FnvHashSet};
use fst::automaton::*;
use fst::raw::Fst;
use fst::IntoStreamer;
use highlight_field::*;
use itertools::Itertools;
use levenshtein_automata::{Distance, LevenshteinAutomatonBuilder, DFA};
use ordered_float::OrderedFloat;
use persistence;
use persistence::Persistence;
use persistence::*;
use search;
use search::*;
use std::cmp;
use std::cmp::Ordering;
use std::iter::FusedIterator;
use std::marker;
use std::sync::Arc;
use str;
use util;
use util::StringAdd;

use half::f16;
use rayon::prelude::*;
use std;
use std::ptr;

use execution_plan::*;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SearchFieldResult {
    pub explain: FnvHashMap<u32, Vec<Explain>>,
    pub hits_scores: Vec<search::Hit>,
    pub hits_ids: Vec<TermId>,
    pub terms: FnvHashMap<TermId, String>,
    pub highlight: FnvHashMap<TermId, String>,
    pub request: RequestSearchPart,
    pub phrase_boost: Option<RequestPhraseBoost>,
    /// store the term id hits field->Term->Hits, used for whyfound and term_locality_boost
    pub term_id_hits_in_field: FnvHashMap<String, FnvHashMap<String, Vec<TermId>>>,
    /// store the text of the term hit field->Terms, used for whyfound
    pub term_text_in_field: FnvHashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Explain {
    Boost(f32),
    MaxTokenToTextId(f32),
    TermToAnchor {
        term_score: f32,
        anchor_score: f32,
        final_score: f32,
        term_id: u32,
    },
    LevenshteinScore {
        score: f32,
        text_or_token_id: String,
        term_id: u32,
    },
    OrSumOverDistinctTerms(f32),
    NumDistintTermsBoost {
        distinct_boost: u32,
        new_score: u32,
    },
}

impl SearchFieldResult {
    pub(crate) fn iter(&self, term_id: u8, _field_id: u8) -> SearchFieldResultIterator {
        let begin = self.hits_scores.as_ptr();
        let end = unsafe { begin.offset(self.hits_scores.len() as isize) as *const search::Hit };

        SearchFieldResultIterator {
            _marker: marker::PhantomData,
            ptr: begin,
            end,
            term_id,
        }
    }

    //Creates a new result, while keeping metadata for original hits
    pub(crate) fn new_from(other: &SearchFieldResult) -> Self {
        let mut res = SearchFieldResult::default();
        res.terms = other.terms.clone();
        res.highlight = other.highlight.clone();
        res.request = other.request.clone();
        res.phrase_boost = other.phrase_boost.clone();
        res.term_id_hits_in_field = other.term_id_hits_in_field.clone();
        res.term_text_in_field = other.term_text_in_field.clone();
        res
    }
}

use test;
#[bench]
fn bench_search_field_iterator(b: &mut test::Bencher) {
    let mut res = SearchFieldResult::default();
    res.hits_scores = (0..6_000_000).map(|el| search::Hit::new(el, 1.0)).collect();
    b.iter(|| {
        let iter = res.iter(0, 1);
        iter.last().unwrap()
    })
}

#[derive(Debug, Clone)]
pub struct SearchFieldResultIterator<'a> {
    _marker: marker::PhantomData<&'a search::Hit>,
    ptr: *const search::Hit,
    end: *const search::Hit,
    term_id: u8,
    // field_id: u8,
}

impl<'a> Iterator for SearchFieldResultIterator<'a> {
    type Item = MiniHit;

    #[inline]
    fn count(self) -> usize {
        self.size_hint().0
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = unsafe { self.end.offset_from(self.ptr) as usize };
        (exact, Some(exact))
    }

    #[inline]
    fn next(&mut self) -> Option<MiniHit> {
        if self.ptr as *const _ == self.end {
            None
        } else {
            let old = self.ptr;
            self.ptr = unsafe { self.ptr.offset(1) };
            let hit = unsafe { ptr::read(old) };
            Some(MiniHit {
                id: hit.id,
                term_id: self.term_id,
                score: f16::from_f32(hit.score),
                // field_id: self.field_id,
            })
        }
    }
}

impl<'a> ExactSizeIterator for SearchFieldResultIterator<'a> {
    #[inline]
    fn len(&self) -> usize {
        unsafe { self.end.offset_from(self.ptr) as usize }
    }
}

impl<'a> FusedIterator for SearchFieldResultIterator<'a> {}

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
        2.0 / ((f32::from(distance) + 1.0).log2() + 0.2)
    } else {
        2.0 / (f32::from(distance) + 0.2)
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

    // let map = persistence.get_fst(&options.path)?;
    let map = persistence
        .indices
        .fst
        .get(&options.path)
        .ok_or_else(|| SearchError::StringError(format!("fst not found loaded in indices {} ", options.path)))?;
    let lev = {
        trace_time!("{} LevenshteinIC create", &options.path);
        let lev_automaton_builder = LevenshteinAutomatonBuilder::new(options.levenshtein_distance.unwrap_or(0) as u8, options.ignore_case.unwrap_or(true));
        lev_automaton_builder.build_dfa(&options.terms[0], true)
        // LevenshteinIC::new(&options.terms[0], options.levenshtein_distance.unwrap_or(0))?
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
            res.hits_scores
                .iter() // @Performance add only "top" elements ?
                .map(|term_n_score| {
                    let term = if suggest_text { &res.terms[&term_n_score.id] } else { &res.highlight[&term_n_score.id] };
                    (term.to_string(), term_n_score.score, term_n_score.id)
                }).collect::<SuggestFieldResult>()
        }).collect::<SuggestFieldResult>();

    //Merge same text
    if suggest_text {
        suggest_result.sort_unstable_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
        suggest_result.dedup_by(|a, b| {
            if a.0 == b.0 {
                if a.1 > b.1 {
                    b.1 = a.1;
                }
                true
            } else {
                false
            }
        });
    }

    suggest_result.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
    search::apply_top_skip(&suggest_result, skip, top)
}
pub fn suggest_multi(persistence: &Persistence, req: Request) -> Result<SuggestFieldResult, SearchError> {
    info_time!("suggest time");
    let search_parts: Vec<RequestSearchPart> = req
        .suggest
        .ok_or_else(|| SearchError::StringError("only suggest allowed in suggest function".to_string()))?;

    // let top = req.top;
    // let skip = req.skip;
    let search_results: Result<Vec<_>, SearchError> = search_parts
        .into_par_iter()
        .map(|search_part| {
            // if search_part.token_value.is_none() { //Apply top skip directly if there is no token_boosting, which alters the result afterwards.
            //     search_part.top = top;
            //     search_part.skip = skip;
            // }
            let mut search_part = PlanRequestSearchPart {
                request: search_part,
                get_scores: true,
                return_term: true,
                return_term_lowercase: true,
                ..Default::default()
            };
            get_term_ids_in_field(persistence, &mut search_part)
        }).collect();
    info_time!("suggest text_id result to vec/sort");
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

    let mut options = PlanRequestSearchPart {
        request: options.clone(),
        get_scores: true,
        ..Default::default()
    };

    let mut result = get_term_ids_in_field(persistence, &mut options)?;
    resolve_token_hits_to_text_id(persistence, &options.request, None, &mut result)?;
    Ok(get_text_score_id_from_result(false, &[result], options.request.skip, options.request.top))
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn get_anchor_for_phrases_in_search_results(
    persistence: &Persistence,
    path: &str,
    res1: SearchFieldResult,
    res2: SearchFieldResult,
) -> Result<(SearchFieldResult), SearchError> {
    let mut path = path.to_string();
    if !path.ends_with(TEXTINDEX) {
        path = path.add(TEXTINDEX);
    }
    if !path.ends_with(PHRASE_PAIR_TO_ANCHOR) {
        path = path.add(PHRASE_PAIR_TO_ANCHOR);
    }
    get_anchor_for_phrases_in_field(persistence, &path, &res1.hits_ids, &res2.hits_ids)
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn get_anchor_for_phrases_in_field(persistence: &Persistence, path: &str, term_id_pairs_1: &[u32], term_id_pairs_2: &[u32]) -> Result<(SearchFieldResult), SearchError> {
    let mut result = SearchFieldResult::default();
    let store = persistence.get_phrase_pair_to_anchor(path)?;
    for term_id_1 in term_id_pairs_1 {
        for term_id_2 in term_id_pairs_2 {
            if let Some(vals) = store.get_values((*term_id_1, *term_id_2)) {
                result.hits_ids.extend(vals);
            }
        }
    }
    result.hits_ids.sort_unstable();
    Ok(result)
}

// #[cfg_attr(feature = "flame_it", flame)]
// pub fn get_hits_in_field(persistence: &Persistence, options: &PlanRequestSearchPart, filter: Option<&FnvHashSet<u32>>) -> Result<SearchFieldResult, SearchError> {
//     let mut options = options.clone();

//     if !options.request.path.ends_with(TEXTINDEX){
//         options.request.path = options.request.path.add(TEXTINDEX);
//     }

//     if options.request.terms.len() == 1 {
//         let mut hits = get_hits_in_field_one_term(persistence, &mut options, filter)?;
//         hits.request = options.request;
//         return Ok(hits);
//     } else {
//         return Err(SearchError::StringError("multiple terms on field not supported".to_string()))
//     }
// }

// #[cfg_attr(feature = "flame_it", flame)]
// fn get_hits_in_field_one_term(persistence: &Persistence, options: &mut PlanRequestSearchPart, filter: Option<&FnvHashSet<u32>>) -> Result<SearchFieldResult, SearchError> {
//     debug_time!("{} get_hits_in_field", &options.request.path);

//     let mut result = get_term_ids_in_field(persistence, options)?;

//     debug!("{:?} hits in textindex {:?}", result.hits_scores.len(), &options.request.path);
//     trace!("hits in textindex: {:?}", result.hits_scores);

//     // resolve_token_hits_to_text_id(persistence, &options, filter, &mut result,)?;

//     Ok(result)
// }

#[cfg_attr(feature = "flame_it", flame)]
pub fn get_term_ids_in_field(persistence: &Persistence, options: &mut PlanRequestSearchPart) -> Result<SearchFieldResult, SearchError> {
    if !options.request.path.ends_with(TEXTINDEX) {
        options.request.path = options.request.path.add(TEXTINDEX);
    }
    let mut result = SearchFieldResult::default();
    result.request = options.request.clone();

    let lower_term = options.request.terms[0].to_lowercase();
    if let Some(d) = options.request.levenshtein_distance.as_mut() {
        *d = std::cmp::min(*d, lower_term.chars().count() as u32 - 1); //limit levenshtein distance to reasonable values
    }

    trace!("Will Check distance {:?}", options.request.levenshtein_distance.unwrap_or(0) != 0);
    trace!("Will Check starts_with {:?}", options.request.starts_with);

    // let mut vec_hits: Vec<(u32, f32)> = vec![];
    let limit_result = options.request.top.is_some();
    let mut worst_score = std::f32::MIN;
    let top_n_search = (options.request.top.unwrap_or(10) + options.request.skip.unwrap_or(0)) as u32;

    {
        debug_time!("{} find token ids", &options.request.path);
        let lev_automaton_builder = LevenshteinAutomatonBuilder::new(options.request.levenshtein_distance.unwrap_or(0) as u8, true);

        let dfa = lev_automaton_builder.build_dfa(&lower_term, false);
        // let search_term_length = &lower_term.chars.count();
        let should_check_prefix_match = options.request.starts_with.unwrap_or(false) || options.request.levenshtein_distance.unwrap_or(0) != 0;

        let teh_callback = |text_or_token: String, token_text_id: u32| {
            trace!("Checking {} with {}", text_or_token, text_or_token);

            if options.get_ids {
                result.hits_ids.push(token_text_id);
            }

            if options.get_scores {
                let line_lower = text_or_token.to_lowercase();

                // In the case of levenshtein != 0 or starts_with, we want prefix_matches to have a score boost - so that "awe" scores better for awesome than aber
                let prefix_matches = should_check_prefix_match && line_lower.starts_with(&lower_term);

                let score = get_default_score_for_distance(distance_dfa(&line_lower, &dfa, &lower_term), prefix_matches);
                // if let Some(boost_val) = options.request.boost {
                //     score *= boost_val
                // }

                if limit_result {
                    if score < worst_score {
                        // debug!("ABORT SCORE {:?}", score);
                        return;
                    }

                    if !result.hits_scores.is_empty() && result.hits_scores.len() as u32 == 200 + top_n_search {
                        // if !result.hits_scores.is_empty() && (result.hits_scores.len() as u32 % (top_n_search * 5)) == 0 {
                        result.hits_scores.sort_unstable_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
                        result.hits_scores.truncate(top_n_search as usize);
                        worst_score = result.hits_scores.last().unwrap().score;
                        trace!("new worst {:?}", worst_score);
                    }

                    search::check_apply_top_n_sort(
                        &mut result.hits_scores,
                        top_n_search,
                        &|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal),
                        &mut |the_worst: &Hit| worst_score = the_worst.score,
                    );
                }
                debug!("Hit: {:?}\tid: {:?} score: {:?}", &text_or_token, token_text_id, score);
                result.hits_scores.push(Hit::new(token_text_id, score));
                if options.request.explain {
                    // result.explain.insert(token_text_id, vec![format!("levenshtein score {:?} for {}", score, text_or_token)]);
                    result.explain.insert(
                        token_text_id,
                        vec![Explain::LevenshteinScore {
                            score: score,
                            term_id: token_text_id,
                            text_or_token_id: text_or_token.clone(),
                        }],
                    );
                }
            }

            if options.return_term || options.store_term_texts {
                if options.return_term_lowercase {
                    result.terms.insert(token_text_id, text_or_token.to_lowercase());
                } else {
                    result.terms.insert(token_text_id, text_or_token);
                }
            }
        };

        get_text_lines(persistence, &options.request, teh_callback)?;
    }

    if let Some(boost_val) = options.request.boost {
        let boost_val = boost_val.into_inner();
        for hit in &mut result.hits_scores {
            hit.score *= boost_val;
        }
    }

    if !result.hits_scores.is_empty() {
        info!("{:?}\thits for {:?} \t in {:?}", result.hits_scores.len(), options.request.terms[0], &options.request.path);
    }
    if !result.hits_ids.is_empty() {
        info!("{:?}\tids hits for {:?} \t in {:?}", result.hits_ids.len(), options.request.terms[0], &options.request.path);
    }

    if limit_result {
        result.hits_scores.sort_unstable_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
        result.hits_scores.truncate(top_n_search as usize);
    }

    // Store token_id hit for why_found or text locality
    if options.store_term_id_hits && !result.hits_scores.is_empty() {
        let mut map = FnvHashMap::default();
        map.insert(options.request.terms[0].clone(), result.hits_scores.iter().map(|el| el.id).collect());
        result.term_id_hits_in_field.insert(options.request.path.to_string(), map);
    }

    // Store token_id terms for why_found
    if options.store_term_texts && !result.terms.is_empty() {
        debug!("term_text_in_field {:?}", result.terms.values().cloned().collect::<Vec<_>>());
        result.term_text_in_field.insert(options.request.path.to_string(), result.terms.values().cloned().collect());
    }

    if let Some(ref mut token_boost) = options.request.token_value {
        debug!("Token Boosting: \n");
        token_boost.path = token_boost.path.add(TOKEN_VALUES);
        search::add_boost(persistence, token_boost, &mut result)?;
    }

    Ok(result)
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn resolve_token_to_anchor(
    persistence: &Persistence,
    options: &RequestSearchPart,
    // filter: Option<FnvHashSet<u32>>,
    filter: Option<Arc<FilterResult>>,
    result: &SearchFieldResult,
) -> Result<SearchFieldResult, SearchError> {
    let mut options = options.clone();
    if !options.path.ends_with(TEXTINDEX) {
        options.path = options.path.add(TEXTINDEX);
    }

    let mut res = SearchFieldResult::new_from(&result);
    debug_time!("{} fast_field", &options.path);
    let mut anchor_ids_hits = vec![];

    let token_to_anchor_score = persistence.get_token_to_anchor(&options.path)?;
    {
        debug_time!("{} tokens.to_anchor_id_score", &options.path);
        for hit in &result.hits_scores {
            let mut iter = token_to_anchor_score.get_score_iter(hit.id);
            anchor_ids_hits.reserve(iter.size_hint().1.unwrap());
            for el in iter {
                if should_filter(&filter, el.id) {
                    continue;
                }
                let final_score = hit.score * (el.score.to_f32() / 100.0);
                if options.explain {
                    let vecco = res.explain.entry(el.id).or_insert_with(|| vec![]);
                    // vecco.push(format!("term score {:?} * anchor score {:?} to {:?}", hit.score, el.score.to_f32() / 100.0, final_score));
                    vecco.push(Explain::TermToAnchor {
                        term_id: hit.id,
                        term_score: hit.score,
                        anchor_score: el.score.to_f32() / 100.0,
                        final_score,
                    });
                    if let Some(exp) = result.explain.get(&hit.id) {
                        vecco.extend_from_slice(exp);
                    }
                }
                anchor_ids_hits.push(search::Hit::new(el.id, final_score));
            }
        }

        if !result.hits_scores.is_empty() {
            debug!("{} found {:?} token in {:?} anchor_ids", &options.path, result.hits_scores.len(), anchor_ids_hits.len());
        }
    }

    {
        trace_time!("{} fast_field sort and dedup sum", &options.path);
        anchor_ids_hits.sort_unstable_by_key(|a| a.id);
        trace_time!("{} fast_field  dedup only", &options.path);
        anchor_ids_hits.dedup_by(|a, b| {
            if a.id == b.id {
                if a.score > b.score {
                    b.score = a.score; //a will be discarded, store max
                }
                true
            } else {
                false
            }
        });
    }

    // IDS ONLY - scores müssen draußen bleiben - This is used for boosting
    let mut fast_field_res_ids = vec![];
    {
        if !result.hits_ids.is_empty() {

            //TODO FIXME Important Note: In the Filter Case we currently only resolve TEXT_IDS to anchor. No Filter are possible on tokens. Fixme: Conflicts with token based boosting
            let text_id_to_anchor = persistence.get_valueid_to_parent(&options.path.add(TEXT_ID_TO_ANCHOR))?;

            debug_time!("{} tokens to anchor_id", &options.path);
            for id in &result.hits_ids {
                let mut iter = text_id_to_anchor.get_values_iter(u64::from(*id));
                fast_field_res_ids.reserve(iter.size_hint().1.unwrap());
                for anchor_id in iter {
                    //TODO ENABLE should_filter(&filter, anchor_id) ?
                    fast_field_res_ids.push(anchor_id);
                }

                // let mut iter = token_to_anchor_score.get_score_iter(*id);
                // fast_field_res_ids.reserve(iter.size_hint().1.unwrap() / 2);
                // for el in iter {
                //     //TODO ENABLE should_filter(&filter, el.id) ?
                //     fast_field_res_ids.push(el.id);
                // }
            }
        }
    }

    res.hits_ids = fast_field_res_ids;

    trace!("anchor id hits {:?}", anchor_ids_hits);
    res.hits_scores = anchor_ids_hits;

    Ok(res)
}

// #[cfg_attr(feature = "flame_it", flame)]
// fn get_text_for_ids(persistence: &Persistence, path: &str, ids: &[u32]) -> Vec<String> {
//     // let mut faccess: persistence::FileSearch = persistence.get_file_search(path);
//     // let offsets = persistence.get_offsets(path).unwrap();
//     ids.iter().map(|id| get_text_for_id(persistence, path, *id)).collect()
// }

// #[cfg_attr(feature = "flame_it", flame)]
// fn get_text_for_id_disk(persistence: &Persistence, path: &str, id: u32) -> String {
//     let mut faccess: persistence::FileSearch = persistence.get_file_search(path);
//     let offsets = persistence.get_offsets(path).unwrap();
//     faccess.get_text_for_id(id as usize, offsets)
// }

#[cfg_attr(feature = "flame_it", flame)]
pub fn get_text_for_id(persistence: &Persistence, path: &str, id: u32) -> String {
    let map = persistence.indices.fst.get(path).unwrap_or_else(|| panic!("fst not found loaded in indices {} ", path));

    let mut bytes = vec![];
    ord_to_term(map.as_fst(), u64::from(id), &mut bytes);
    unsafe { String::from_utf8_unchecked(bytes) }
}

// #[cfg_attr(feature = "flame_it", flame)]
// pub fn get_text_for_id_2(persistence: &Persistence, path: &str, id: u32, bytes: &mut Vec<u8>) {
//     let map = persistence
//         .indices
//         .fst
//         .get(path)
//         .unwrap_or_else(|| panic!("fst not found loaded in indices {} ", path));
//     ord_to_term(map.as_fst(), u64::from(id), bytes);
// }

#[cfg_attr(feature = "flame_it", flame)]
pub fn get_id_text_map_for_ids(persistence: &Persistence, path: &str, ids: &[u32]) -> FnvHashMap<u32, String> {
    let map = persistence.indices.fst.get(path).unwrap_or_else(|| panic!("fst not found loaded in indices {} ", path));
    ids.iter()
        .map(|id| {
            let mut bytes = vec![];
            ord_to_term(map.as_fst(), u64::from(*id), &mut bytes);
            (*id, str::from_utf8(&bytes).unwrap().to_string())
        }).collect()
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

#[inline]
fn should_filter(filter: &Option<Arc<FilterResult>>, id: u32) -> bool {
    filter
        .as_ref()
        .map(|filter| match **filter {
            FilterResult::Vec(_) => false,
            FilterResult::Set(ref filter) => !filter.contains(&id),
        }).unwrap_or(false)
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn resolve_token_hits_to_text_id(
    persistence: &Persistence,
    options: &RequestSearchPart,
    _filter: Option<FnvHashSet<u32>>,
    result: &mut SearchFieldResult,
) -> Result<(), search::SearchError> {
    let mut path = options.path.to_string();
    if !path.ends_with(TEXTINDEX) {
        path = path.add(TEXTINDEX);
    }
    let has_tokens = persistence
        .meta_data
        .fulltext_indices
        .get(&path)
        .map_or(false, |fulltext_info| fulltext_info.options.tokenize);
    debug!("has_tokens {:?} {:?}", path, has_tokens);
    if !has_tokens {
        return Ok(());
    }
    let add_snippets = options.snippet.unwrap_or(false);

    debug_time!("{} resolve_token_hits_to_text_id", path);

    let token_path = path.add(TOKENS_TO_TEXT_ID);
    let token_kvdata = persistence.get_valueid_to_parent(&token_path)?;
    debug!("Checking Tokens in {:?}", &token_path);
    persistence::trace_index_id_to_parent(token_kvdata);
    // trace!("All Tokens: {:?}", token_kvdata.get_values());

    // let mut token_hits:FnvHashMap<u32, f32> = FnvHashMap::default();
    let mut token_hits: Vec<(u32, f32, u32)> = vec![];
    // let mut anchor_hits = FnvHashMap::default();
    {
        debug_time!("{} adding parent_id from tokens", token_path);
        for hit in &result.hits_scores {
            if let Some(parent_ids_for_token) = token_kvdata.get_values(u64::from(hit.id)) {
                // let token_text_length_offsets = text_offsets.get_mutliple_value(hit.id as usize..=hit.id as usize + 1).unwrap();
                // let token_text_length = token_text_length_offsets[1] - token_text_length_offsets[0];

                token_hits.reserve(parent_ids_for_token.len());
                for token_parentval_id in parent_ids_for_token {
                    // if should_filter(&_filter, token_parentval_id) {
                    //     continue;
                    // }
                    token_hits.push((token_parentval_id, hit.score, hit.id)); //TODO ADD ANCHOR_SCORE IN THIS SEARCH
                }
            }
        }

        result.hits_ids = result.hits_ids.iter().flat_map(|id| token_kvdata.get_values(u64::from(*id))).flat_map(|el| el).collect();
    }

    debug!("found {:?} token in {:?} texts", result.hits_scores.iter().count(), token_hits.iter().count());
    {
        debug_time!("token_hits.sort_by {:?}", path);
        token_hits.sort_unstable_by(|a, b| a.0.cmp(&b.0)); // sort by parent id
    }
    debug_time!("{} extend token_results", path);
    // hits.extend(token_hits);
    trace!("{} token_hits in textindex: {:?}", path, token_hits);
    if !token_hits.is_empty() {
        if add_snippets {
            result.hits_scores.clear(); //only document hits for highlightung
        }
        // token_hits.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal)); // sort by parent_id=value_id
        result.hits_scores.reserve(token_hits.len());

        for (parent_id, group) in &token_hits.iter().group_by(|el| el.0) {
            //Group by anchor
            let (mut t1, t2) = group.tee();
            let max_score = t1.max_by_key(|el| OrderedFloat(el.1.abs())).unwrap().1;

            result.hits_scores.push(Hit::new(parent_id, max_score));

            if options.explain {
                // result.explain.insert(parent_id, vec![format!("max_score from token_hits score {:?}", max_score)]);
                result.explain.insert(parent_id, vec![Explain::MaxTokenToTextId(max_score)]);
            }
            if add_snippets {
                //value_id_to_token_hits.insert(parent_id, t2.map(|el| el.2).collect_vec()); //TODO maybe store hits here, in case only best x are needed
                let snippet_config = options.snippet_info.as_ref().unwrap_or(&search::DEFAULT_SNIPPETINFO);
                let highlighted_document = highlight_document(persistence, &path, u64::from(parent_id), &t2.map(|el| el.2).collect_vec(), snippet_config)?;
                if let Some(highlighted_document) = highlighted_document {
                    result.highlight.insert(parent_id, highlighted_document);
                }
            }
        }
    }
    trace!("{} hits with tokens: {:?}", path, result.hits_scores);
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

//TODO: FIXME This method can't compare string larger than u8 length
fn distance(s1: &str, s2: &str) -> u8 {
    trace_time!("distance {:?} {:?}", s1, s2);
    if s1.len() >= 255 || s2.len() >= 255 {
        return 255;
    }
    let len_s1 = s1.chars().count();

    let mut column: Vec<u8> = Vec::with_capacity(len_s1 + 1);
    unsafe {
        column.set_len(len_s1 + 1);
    }
    for (i, item) in column.iter_mut().enumerate().take(len_s1 + 1) {
        *item = i as u8;
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
