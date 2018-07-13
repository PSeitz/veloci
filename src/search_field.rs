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
use str;
use util;
use util::StringAdd;

use half::f16;
use rayon::prelude::*;
use std;
use std::ptr;

use execution_plan::*;

#[derive(Debug, Default)]
pub struct SearchFieldResult {
    pub hits_scores: Vec<search::Hit>,
    pub hits_ids: Vec<TermId>,
    pub terms: FnvHashMap<TermId, String>,
    pub highlight: FnvHashMap<TermId, String>,
    pub request: RequestSearchPart,
    /// store the term id hits field->Term->Hits, used for whyfound and term_locality_boost
    pub term_id_hits_in_field: FnvHashMap<String, FnvHashMap<String, Vec<TermId>>>,
    /// store the text of the term hit field->Terms, used for whyfound
    pub term_text_in_field: FnvHashMap<String, Vec<String>>,
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
            // field_id: field_id,
        }
    }

    // TODO AVOID COPY
    //Creates a new result, while keeping metadata for original hits
    pub(crate) fn new_from(other: &SearchFieldResult) -> Self {
        let mut res = SearchFieldResult::default();
        res.terms = other.terms.clone();
        res.highlight = other.highlight.clone();
        res.request = other.request.clone();
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
        let lev_automaton_builder = LevenshteinAutomatonBuilder::new(options.levenshtein_distance.unwrap_or(0) as u8, true);
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
            res.hits_scores.iter()// @Performance add only "top" elements ?
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
    let search_parts: Vec<RequestSearchPart> = req.suggest.ok_or_else(|| SearchError::StringError("only suggest allowed in suggest function".to_string()))?;
    // let mut search_results = vec![];
    let top = req.top;
    let skip = req.skip;
    let search_results: Result<Vec<_>, SearchError> = search_parts
        .into_par_iter()
        .map(|mut search_part| {
            search_part.top = top;
            search_part.skip = skip;
            // search_part.resolve_token_to_parent_hits = Some(false);
            let search_part = PlanRequestSearchPart{request:search_part, return_term: true, resolve_token_to_parent_hits: Some(false), ..Default::default()};
            get_hits_in_field(persistence, &search_part, None)
        })
        .collect();
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

    let options = PlanRequestSearchPart{request:options.clone(), ..Default::default()};

    Ok(get_text_score_id_from_result(
        false,
        &[get_hits_in_field(persistence, &options, None)?],
        options.request.skip,
        options.request.top,
    ))
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

#[cfg_attr(feature = "flame_it", flame)]
pub fn get_hits_in_field(persistence: &Persistence, options: &PlanRequestSearchPart, filter: Option<&FnvHashSet<u32>>) -> Result<SearchFieldResult, SearchError> {
    let mut options = options.clone();
    options.request.path = options.request.path.add(TEXTINDEX);

    if options.request.terms.len() == 1 {
        let mut hits = get_hits_in_field_one_term(persistence, &mut options, filter)?;
        hits.request = options.request;
        return Ok(hits);
    } else {
        return Err(SearchError::StringError("multiple terms on field not supported".to_string()))
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

}

#[cfg_attr(feature = "flame_it", flame)]
fn get_hits_in_field_one_term(persistence: &Persistence, options: &mut PlanRequestSearchPart, filter: Option<&FnvHashSet<u32>>) -> Result<SearchFieldResult, SearchError> {
    debug_time!("{} get_hits_in_field", &options.request.path);

    let mut result = get_term_ids_in_field(persistence, options)?;

    debug!("{:?} hits in textindex {:?}", result.hits_scores.len(), &options.request.path);
    trace!("hits in textindex: {:?}", result.hits_scores);

    if options.fast_field {
        result = resolve_token_to_anchor(persistence, &options.request, filter, &result)?;
    } else if options.resolve_token_to_parent_hits.unwrap_or(true) {
        resolve_token_hits(persistence, &options.request.path, &mut result, &options, filter)?;
    }

    Ok(result)
}

#[cfg_attr(feature = "flame_it", flame)]
fn get_term_ids_in_field(persistence: &Persistence, options: &mut PlanRequestSearchPart) -> Result<SearchFieldResult, SearchError> {
    let mut result = SearchFieldResult::default();

    let lower_term = options.request.terms[0].to_lowercase();
    if let Some(d) = options.request.levenshtein_distance.as_mut() {
        *d = std::cmp::min(*d, lower_term.chars().count() as u32 - 1); //limit levenshtein distance to reasonable values
    }

    trace!("Will Check distance {:?}", options.request.levenshtein_distance.unwrap_or(0) != 0);
    trace!("Will Check starts_with {:?}", options.request.starts_with);

    //TODO Move to topn struct
    // let mut vec_hits: Vec<(u32, f32)> = vec![];
    let limit_result = options.request.top.is_some();
    let mut worst_score = std::f32::MIN;
    let top_n_search = if limit_result {
        (options.request.top.unwrap() + options.request.skip.unwrap_or(0)) as u32
    } else {
        std::u32::MAX
    };
    //TODO Move to topnstruct

    {
        debug_time!("{} find token ids", &options.request.path);
        let lev_automaton_builder = LevenshteinAutomatonBuilder::new(options.request.levenshtein_distance.unwrap_or(0) as u8, true);

        let dfa = lev_automaton_builder.build_dfa(&lower_term, false);
        // let search_term_length = &lower_term.chars.count();
        let should_check_prefix_match = options.request.starts_with.unwrap_or(false) || options.request.levenshtein_distance.unwrap_or(0) != 0;

        // let exact_search = if options.request.exact.unwrap_or(false) {Some(options.request.term.to_string())} else {None};
        if options.ids_only {
            let teh_callback_id_only = |_line: String, token_text_id: u32| {
                result.hits_ids.push(token_text_id);
            };
            get_text_lines(persistence, &options.request, teh_callback_id_only)?;
        } else {
            let teh_callback = |line: String, token_text_id: u32| {
                // trace!("Checking {} with {}", line, term);

                let line_lower = line.to_lowercase();

                // In the case of levenshtein != 0 or starts_with, we want prefix_matches to have a score boost - so that "awe" scores better for awesome than aber
                let prefix_matches = should_check_prefix_match && line_lower.starts_with(&lower_term);

                //TODO: find term for multitoken
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
                debug!("Hit: {:?}\tid: {:?} score: {:?}", &line, token_text_id, score);
                result.hits_scores.push(Hit::new(token_text_id, score));

                if options.return_term || options.store_term_texts {
                    result.terms.insert(token_text_id, line);
                }
            };

            get_text_lines(persistence, &options.request, teh_callback)?;
        }
    }

    if let Some(boost_val) = options.request.boost {
        for hit in &mut result.hits_scores {
            hit.score *= boost_val;
        }
    }

    if !result.hits_scores.is_empty() {
        info!("{:?}\thits for {:?} \t in {:?}", result.hits_scores.len(), options.request.terms[0], &options.request.path);
    }

    if limit_result {
        result.hits_scores.sort_unstable_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
        result.hits_scores.truncate(top_n_search as usize);
    }

    // Store token_id hit for why_found or text locality
    if options.store_term_id_hits && !result.hits_scores.is_empty() {
        let mut map = FnvHashMap::default();
        map.insert(options.request.terms[0].clone(), result.hits_scores.iter().map(|el| el.id).collect()); // TODO Avoid copy? just store Hit?
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
fn resolve_token_to_anchor(
    persistence: &Persistence,
    options: &RequestSearchPart,
    filter: Option<&FnvHashSet<u32>>,
    result: &SearchFieldResult,
) -> Result<SearchFieldResult, SearchError> {
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
                if should_filter(filter, el.id) {
                    continue;
                }
                let final_score = hit.score * (el.score.to_f32() / 100.0); // TODO ADD LIMIT FOR TOP X
                anchor_ids_hits.push(search::Hit::new(el.id, final_score));
            }
        }

        debug!("{} found {:?} token in {:?} anchor_ids", &options.path, result.hits_scores.len(), anchor_ids_hits.len());
    }

    // {
    //     debug_time!("{} tokens.to_anchor_id_score", &options.path);
    //     let iterators:Vec<_> = result.hits_scores.iter().map(|hit|{
    //         let iter = token_to_anchor_score.get_score_iter(hit.id);
    //         anchor_ids_hits.reserve(iter.size_hint().1.unwrap());
    //         iter.map(move |el|{
    //             let final_score = hit.score * (el.score.to_f32() / 100.0);
    //             search::Hit::new(el.id, final_score)
    //         })
    //     }).collect();
    //     let mergo = iterators.into_iter().kmerge_by(|a, b| a.id < b.id);
    //     for (mut id, mut group) in &mergo.into_iter().group_by(|el| el.id) {
    //         let score = group.map(|el|el.score).sum();
    //         anchor_ids_hits.push(search::Hit::new(id, score));
    //     }
    // }
    // {
    //     let mut all_hits = vec![];
    //     debug_time!("{} tokens.to_anchor_id_score", &options.path);
    //     for hit in &result.hits_scores {
    //         if let Some(text_id_score) = token_to_anchor_score.get_scores(hit.id) {
    //             // trace_time!("{} adding anchor hits for id {:?}", &options.path, hit.id);
    //             // let mut curr_pos = unsafe_increase_len(&mut anchor_ids_hits, text_id_score.len());
    //             let mut token_hits = vec![];
    //             let mut curr_pos = unsafe_increase_len(&mut token_hits, text_id_score.len());
    //             for el in text_id_score {
    //                 if should_filter(&filter, el.id) {
    //                     continue;
    //                 }
    //                 let final_score = hit.score * (el.score.to_f32() / 100.0); // TODO ADD LIMIT FOR TOP X
    //                 token_hits[curr_pos] = search::Hit::new(el.id, final_score);
    //                 curr_pos += 1;
    //             }
    //             all_hits.push(token_hits)
    //         }
    //     }
    //     debug!("{} found {:?} token in {:?} anchor_ids", &options.path, result.hits_scores.len(), anchor_ids_hits.len() );

    //     debug_time!("{} KMERGO ", &options.path);

    //     let iterators: Vec<_> = all_hits
    //     .iter()
    //     .map(|res|res.iter())
    //     .collect();

    //     let mergo = iterators.into_iter().kmerge_by(|a, b| a.id < b.id);
    //     for (mut id, mut group) in &mergo.into_iter().group_by(|el| el.id) {
    //         let score = group.map(|el|el.score).sum();
    //         anchor_ids_hits.push(search::Hit::new(id, score));
    //     }

    // }

    // { //TEEEEEEEEEEEEEEEEEEEEEEEEEST
    //     let mut the_bits = FixedBitSet::with_capacity(7000);
    //     info_time!("{} WAAAA BITS SETZEN WAAA", &options.path);
    //     for hit in &result.hits_scores {
    //         // iterate over token hits
    //         if let Some(text_id_score) = token_kvdata.get_values(hit.id as u64) {
    //             //trace_time!("{} adding anchor hits for id {:?}", &options.path, hit.id);
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

    // debug!("found {:?} text_ids in {:?} anchors", anchor_ids_hits.len(), anchor_hits.len());
    // {
    //     //Collect hits from same anchor and sum boost
    //     let mut merged_fast_field_res = vec![];
    //     debug_time!("{} sort and merge fast_field", &options.path);
    //     anchor_hits.sort_unstable_by(|a, b| b.id.partial_cmp(&a.id).unwrap_or(Ordering::Equal));
    //     for (text_id, group) in &anchor_hits.iter().group_by(|el| el.id) {
    //         merged_fast_field_res.push(search::Hit::new(text_id, group.map(|el| el.score).sum())) //Todo FixMe Perofrmance avoid copy inplace group by
    //     }
    //     anchor_hits = merged_fast_field_res;
    // }

    {
        debug_time!("{} fast_field sort and dedup sum", &options.path);
        anchor_ids_hits.sort_unstable_by_key(|a| a.id);
        debug_time!("{} fast_field  dedup only", &options.path);
        anchor_ids_hits.dedup_by(|a, b| {
            if a.id == b.id {
                b.score += a.score; // TODO: Check if b is always kept and a discarded in case of equality
                true
            } else {
                false
            }
        });
        // anchor_ids_hits.dedup_by_key(|b| b.id); // TODO FixMe Score
    }

    // IDS ONLY - scores müssen draußen bleiben - This is used for boosting
    let mut fast_field_res_ids = vec![];
    {
        for id in &result.hits_ids {
            debug_time!("{} added anchor ids for id {:?}", &options.path, id);
            let mut iter = token_to_anchor_score.get_score_iter(*id);
            fast_field_res_ids.reserve(iter.size_hint().1.unwrap() / 2);
            for el in iter {
                //TODO ENABLE should_filter(&filter, el.id) ?
                fast_field_res_ids.push(el.id);
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

#[inline]
fn should_filter(filter: Option<&FnvHashSet<u32>>, id: u32) -> bool {
    filter.map(|filter| filter.contains(&id)).unwrap_or(false)
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn resolve_token_hits(
    persistence: &Persistence,
    path: &str,
    result: &mut SearchFieldResult,
    options: &PlanRequestSearchPart,
    filter: Option<&FnvHashSet<u32>>,
) -> Result<(), search::SearchError> {
    let has_tokens = persistence.meta_data.fulltext_indices.get(path).map_or(false, |fulltext_info| fulltext_info.tokenize);
    debug!("has_tokens {:?} {:?} is_fast_field {}", path, has_tokens, options.fast_field);
    if !has_tokens && !options.fast_field {
        return Ok(());
    }

    let add_snippets = options.request.snippet.unwrap_or(false);

    debug_time!("{} resolve_token_hits", path);

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
                    if should_filter(filter, token_parentval_id) {
                        continue;
                    }

                    token_hits.push((token_parentval_id, hit.score, hit.id)); //TODO ADD ANCHOR_SCORE IN THIS SEARCH

                    // if let Some(offsets) = text_offsets.get_mutliple_value(token_parentval_id as usize..=token_parentval_id as usize + 1) {
                    //     // TODO replace with different scoring algorithm, not just length
                    //     let parent_text_length = offsets[1] - offsets[0];
                    //     let adjusted_score = hit.score * (token_text_length as f32 / parent_text_length as f32);
                    //     trace!(
                    //         "value_id {:?} parent_l {:?}, token_l {:?} score {:?} to adjusted_score {:?}",
                    //         token_parentval_id,
                    //         parent_text_length,
                    //         token_text_length,
                    //         hit.score,
                    //         adjusted_score
                    //     );
                    //     token_hits.push((token_parentval_id, adjusted_score, hit.id));
                    // }
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
            if add_snippets {
                //value_id_to_token_hits.insert(parent_id, t2.map(|el| el.2).collect_vec()); //TODO maybe store hits here, in case only best x are needed
                let snippet_config = options.request.snippet_info.as_ref().unwrap_or(&search::DEFAULT_SNIPPETINFO);
                let highlighted_document = highlight_document(persistence, path, u64::from(parent_id), &t2.map(|el| el.2).collect_vec(), snippet_config)?;
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
fn distance(s1: &str, s2: &str) -> u8 {
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
