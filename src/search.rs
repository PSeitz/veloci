use std::cmp::Ordering;
use std::io;
use std::{self, cmp, f32, str};

use fnv::FnvHashMap;
use fnv::FnvHashSet;
use itertools::Itertools;
use serde_json;

use doc_loader::DocLoader;
use fst;
// use fst_levenshtein;
use persistence::Persistence;
use persistence::*;
use util;
use util::*;

use execution_plan::*;
use search_field::*;

use json_converter;

use crossbeam_channel;
use half::f16;
use rayon::prelude::*;

use highlight_field;
use search_field;

use expression::ScoreExpression;
use fnv;
// use ordered_float::*;
use persistence::*;
use std::fmt;

use ordered_float::OrderedFloat;

#[derive(Debug)]
enum SearchOperation {
    And,
    Or,
    Search
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct Request {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub or: Option<Vec<Request>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub and: Option<Vec<Request>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search: Option<RequestSearchPart>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggest: Option<Vec<RequestSearchPart>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<Vec<RequestBoostPart>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost_term: Option<Vec<RequestSearchPart>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<Vec<FacetRequest>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub select: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default = "default_top")]
    pub top: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default = "default_skip")]
    pub skip: Option<usize>,
    #[serde(skip_serializing_if = "skip_false")]
    #[serde(default)]
    pub why_found: bool,
    #[serde(skip_serializing_if = "skip_false")]
    #[serde(default)]
    pub text_locality: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
enum TextLocalitySetting {
    Enabled,
    Disabled,
    Fields(Vec<String>),
}

impl Default for TextLocalitySetting {
    fn default() -> TextLocalitySetting {
        TextLocalitySetting::Disabled
    }
}

pub fn skip_false(val: &bool) -> bool {
    !*val
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct FacetRequest {
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default = "default_top")]
    pub top: Option<usize>,
}

fn default_top() -> Option<usize> {
    Some(10)
}
fn default_skip() -> Option<usize> {
    None
}

pub struct RequestSearchPartCache {
    pub automaton: Option<Box<fst::Automaton<State = Option<usize>> + Send + Sync>>,
}


#[derive(Serialize, Deserialize, Default, Clone, Debug, Hash)]
pub struct RequestSearchPart {
    pub path: String,
    pub terms: Vec<String>, //TODO only first term used currently
    #[serde(default = "default_term_operator")]
    pub term_operator: TermOperator, //TODO unused currently

    #[serde(skip_serializing_if = "Option::is_none")]
    pub levenshtein_distance: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub starts_with: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_value: Option<RequestBoostPart>,

    /// boosts the search part with this value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<OrderedFloat<f32>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub top: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip: Option<usize>,

    /// return the snippet hit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet: Option<bool>,

    /// Override default SnippetInfo
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet_info: Option<SnippetInfo>,


}

//TODO: Change to faster eq maybe
impl PartialEq for RequestSearchPart {
    fn eq(&self, other: &RequestSearchPart) -> bool {
        format!("{:?}", self) == format!("{:?}", other)
    }
}
impl Eq for RequestSearchPart {}

impl PartialOrd for RequestSearchPart {
    fn partial_cmp(&self, other: &RequestSearchPart) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RequestSearchPart {
    fn cmp(&self, other: &RequestSearchPart) -> Ordering {
        format!("{:?}", self).cmp(&format!("{:?}", other))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Hash)]
pub struct SnippetInfo {
    #[serde(default = "default_num_words_around_snippet")]
    pub num_words_around_snippet: i64,
    #[serde(default = "default_snippet_start")]
    pub snippet_start_tag: String,
    #[serde(default = "default_snippet_end")]
    pub snippet_end_tag: String,
    #[serde(default = "default_snippet_connector")]
    pub snippet_connector: String,
    #[serde(default = "default_max_snippets")]
    pub max_snippets: u32,
}

fn default_num_words_around_snippet() -> i64 {
    5
}
fn default_snippet_start() -> String {
    "<b>".to_string()
}
fn default_snippet_end() -> String {
    "</b>".to_string()
}
fn default_snippet_connector() -> String {
    " ... ".to_string()
}
fn default_max_snippets() -> u32 {
    std::u32::MAX
}

lazy_static! {
    pub(crate) static ref DEFAULT_SNIPPETINFO: SnippetInfo = SnippetInfo {
        num_words_around_snippet: default_num_words_around_snippet(),
        snippet_start_tag: default_snippet_start(),
        snippet_end_tag: default_snippet_end(),
        snippet_connector: default_snippet_connector(),
        max_snippets: default_max_snippets(),
    };
}

fn default_term_operator() -> TermOperator {
    TermOperator::ALL
}

#[derive(Serialize, Deserialize, Clone, Debug, Hash)]
pub enum TermOperator {
    ALL,
    ANY,
}
impl Default for TermOperator {
    fn default() -> TermOperator {
        default_term_operator()
    }
}

#[derive(Serialize, Deserialize, Default, Clone, Debug, Hash)]
pub struct RequestBoostPart {
    pub path: String,
    pub boost_fun: Option<BoostFunction>,
    pub param: Option<OrderedFloat<f32>>,
    pub skip_when_score: Option<Vec<OrderedFloat<f32>>>,
    pub expression: Option<String>,
}

//TODO: Change to faster eq maybe
impl PartialEq for RequestBoostPart {
    fn eq(&self, other: &RequestBoostPart) -> bool {
        format!("{:?}", self) == format!("{:?}", other)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Hash)]
pub enum BoostFunction {
    Log10,
    Linear,
    Add,
}

impl Default for BoostFunction {
    fn default() -> BoostFunction {
        BoostFunction::Log10
    }
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct SearchResult {
    pub num_hits: u64,
    pub data: Vec<Hit>,
    pub ids: Vec<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<FnvHashMap<String, Vec<(String, usize)>>>,
    #[serde(skip_serializing_if = "FnvHashMap::is_empty")]
    pub why_found_info: FnvHashMap<u32, FnvHashMap<String, Vec<String>>>,
    #[serde(skip_serializing_if = "FnvHashMap::is_empty")]
    pub why_found_terms: FnvHashMap<String, Vec<String>>,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct SearchResultWithDoc {
    pub num_hits: u64,
    pub data: Vec<DocWithHit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<FnvHashMap<String, Vec<(String, usize)>>>,
}

impl SearchResultWithDoc {
    pub fn merge(&mut self, other: &SearchResultWithDoc) {
        self.num_hits += other.num_hits;
        self.data.extend(other.data.iter().cloned());
        // if let Some(mut facets) = self.facets {  //TODO FACETS MERGE
        //     // facets.extend()
        // }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DocWithHit {
    pub doc: serde_json::Value,
    pub hit: Hit,
    #[serde(skip_serializing_if = "FnvHashMap::is_empty")]
    pub why_found: FnvHashMap<String, Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Hit {
    pub id: u32,
    pub score: f32,
}

impl Hit {
    pub fn new(id: u32, score: f32) -> Hit {
        Hit { id, score }
    }
}

// #[cfg_attr(feature = "flame_it", flame)]
// fn hits_to_sorted_array(hits: FnvHashMap<u32, f32>) -> Vec<Hit> {
//     //TODO add top n sort
//     debug_time!("hits_to_sorted_array");
//     let mut res: Vec<Hit> = hits.iter().map(|(id, score)| Hit { id: *id, score: *score }).collect();
//     res.sort_unstable_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal)); //TODO Add sort by id when equal
//     res
// }

impl std::fmt::Display for DocWithHit {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "\n{}\t{}", self.hit.id, self.hit.score)?;
        write!(f, "\n{}", serde_json::to_string_pretty(&self.doc).unwrap())?;
        Ok(())
    }
}

fn highlight_on_original_document(doc: &str, why_found_terms: &FnvHashMap<String, FnvHashSet<String>>) -> FnvHashMap<String, Vec<String>> {
    let mut highlighted_texts: FnvHashMap<_, Vec<_>> = FnvHashMap::default();
    let stream = serde_json::Deserializer::from_str(&doc).into_iter::<serde_json::Value>();

    let mut id_holder = json_converter::IDHolder::new();

    {
        let mut cb_text = |_anchor_id: u32, value: &str, path: &str, _parent_val_id: u32| {
            if let Some(terms) = why_found_terms.get(path) {
                if let Some(highlighted) = highlight_field::highlight_text(value, &terms, &DEFAULT_SNIPPETINFO) {
                    let field_name = extract_field_name(path); // extract_field_name removes .textindex
                    let mut jepp = highlighted_texts.entry(field_name).or_default();
                    jepp.push(highlighted);
                }
            }
        };

        let mut callback_ids = |_anchor_id: u32, _path: &str, _value_id: u32, _parent_val_id: u32| {};

        json_converter::for_each_element(stream, &mut id_holder, &mut cb_text, &mut callback_ids);
    }
    highlighted_texts
}

// @FixMe Tests should use to_search_result
#[cfg_attr(feature = "flame_it", flame)]
pub fn to_documents(persistence: &Persistence, hits: &[Hit], select: &Option<Vec<String>>, result: &SearchResult) -> Vec<DocWithHit> {
    let tokens_set = {
        result
            .why_found_terms
            .iter()
            .map(|(path, terms)| {
                let tokens_set: FnvHashSet<String> = terms.iter().map(|el| el.to_string()).collect();
                (path.to_string(), tokens_set)
            })
            .collect()
    };

    hits.iter()
        .map(|hit| {
            if let Some(ref select) = select {
                return DocWithHit {
                    doc: read_data(persistence, hit.id, &select).unwrap(), // TODO validate fields
                    hit: hit.clone(),
                    why_found: result.why_found_info.get(&hit.id).cloned().unwrap_or_default(),
                };
            } else {
                let doc_str = DocLoader::get_doc(persistence, hit.id as usize).unwrap(); // TODO No unwrapo
                let ayse = highlight_on_original_document(&doc_str, &tokens_set);

                return DocWithHit {
                    doc: serde_json::from_str(&doc_str).unwrap(),
                    hit: hit.clone(),
                    why_found: ayse,
                };
            };
        })
        .collect::<Vec<_>>()
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn to_search_result(persistence: &Persistence, hits: SearchResult, select: &Option<Vec<String>>) -> SearchResultWithDoc {
    SearchResultWithDoc {
        data: to_documents(&persistence, &hits.data, &select, &hits),
        num_hits: hits.num_hits,
        facets: hits.facets,
    }
}

// pub fn get_search_result(persistence: &Persistence, request: Request) -> Result<SearchResultWithDoc, SearchError> {
//     let select = request.select.clone();
//     let res = search(request, &persistence)?;
//     Ok(to_search_result(&persistence, res, &select))
// }

// #[inline]
// fn to_bucket_and_id(value: u32) -> (u16, u16) {
//     ((value >> 16) as u16, value as u16)
// }

fn get_why_found(
    persistence: &Persistence,
    anchor_ids: &[u32],
    term_id_hits_in_field: &FnvHashMap<String, FnvHashMap<String, Vec<TermId>>>,
) -> Result<FnvHashMap<u32, FnvHashMap<String, Vec<String>>>, SearchError> {
    debug!("why_found info {:?}", term_id_hits_in_field);
    info_time!("why_found");
    let mut anchor_highlights: FnvHashMap<_, FnvHashMap<_, Vec<_>>> = FnvHashMap::default();

    for (path, term_with_ids) in term_id_hits_in_field.iter() {
        let field_name = &extract_field_name(path); // extract_field_name removes .textindex
        let mut paths = util::get_steps_to_anchor(field_name);

        let all_term_ids_hits_in_path = term_with_ids.iter().fold(vec![], |mut acc, (ref _term, ref hits)| {
            acc.extend(hits.iter());
            acc
        });

        if all_term_ids_hits_in_path.is_empty() {
            continue;
        }

        for anchor_id in anchor_ids {
            //debug_time!(format!("highlight anchor_id {:?}", anchor_id)); // TODO flip loops and trace time per anchor
            let ids = facet::join_anchor_to_leaf(persistence, &[*anchor_id], &paths)?;

            for value_id in ids {
                let path = paths.last().unwrap().to_string();
                let highlighted_document = highlight_field::highlight_document(persistence, &path, u64::from(value_id), &all_term_ids_hits_in_path, &DEFAULT_SNIPPETINFO).unwrap();
                if let Some(highlighted_document) = highlighted_document {
                    let jepp = anchor_highlights.entry(*anchor_id).or_default();
                    let mut field_highlights = jepp.entry(field_name.clone()).or_default();
                    field_highlights.push(highlighted_document);
                }
            }
        }
    }

    Ok(anchor_highlights)
}

#[inline]
fn boost_text_locality_all(persistence: &Persistence, term_id_hits_in_field: &mut FnvHashMap<String, FnvHashMap<String, Vec<TermId>>>) -> Result<(Vec<Hit>), SearchError> {
    debug!("boost_text_locality_all {:?}", term_id_hits_in_field);
    info_time!("boost_text_locality_all");
    let mut boost_anchor: Vec<Hit> = vec![];

    let r: Result<Vec<_>, SearchError> = term_id_hits_in_field
        .into_par_iter()
        .map(|(path, term_with_ids)| boost_text_locality(persistence, path, term_with_ids))
        .collect();

    info_time!("collect sort_boost");
    if r.is_err() {
        return Err(r.unwrap_err());
    } else {
        let boosts = r.unwrap();
        // for boost in boosts {
        //     boost_anchor.extend(boost.iter().cloned());
        // }

        let mergo = boosts.into_iter().kmerge_by(|a, b| a.id < b.id);
        for (mut id, mut group) in &mergo.into_iter().group_by(|el| el.id) {
            let best_score = group.map(|el| el.score).max_by(|a, b| b.partial_cmp(&a).unwrap_or(Ordering::Equal)).unwrap();
            boost_anchor.push(Hit::new(id, best_score));
        }
    }

    // info_time!("sort_boost");
    // boost_anchor.sort_unstable_by_key(|el| el.id); //TODO GROUP BY MAX instead sort dedup

    // boost_anchor.dedup_by( |a, b|{
    //     if a.id == b.id{
    //         if a.score > b.score {
    //             b.score = a.score; //TODO ADD TEST, KEEP ONLY BESTE SCORE VOMG RANK HER
    //         }
    //         true
    //     }else{
    //         false
    //     }
    // });

    Ok(boost_anchor)
}

fn boost_text_locality(persistence: &Persistence, path: &str, search_term_to_text_ids: &mut FnvHashMap<String, Vec<TermId>>) -> Result<(Vec<Hit>), SearchError> {
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
        for (mut id, mut group) in &mergo.into_iter().group_by(|el| *el) {
            let num_hits_in_same_text = group.count();
            if num_hits_in_same_text > 1 {
                boost_text_ids.push((id, num_hits_in_same_text));
            }
        }
    }
    let text_id_to_anchor = persistence.get_valueid_to_parent(path.add(TEXT_ID_TO_ANCHOR))?;
    trace_time!("text_locality_boost text_ids to anchor");

    boost_text_ids.sort_unstable_by_key(|el| el.0);
    for text_id in boost_text_ids {
        let num_hits_in_same_text = text_id.1;
        for anchor_id in text_id_to_anchor.get_values_iter(u64::from(text_id.0)) {
            boost_anchor.push(Hit::new(anchor_id, num_hits_in_same_text as f32 * num_hits_in_same_text as f32));
        }
    }
    boost_anchor.sort_unstable_by_key(|el| el.id);
    Ok(boost_anchor)
}

#[inline]
fn get_all_value_ids(ids: &[u32], token_to_text_id: &IndexIdToParent<Output = u32>) -> Vec<u32> {
    let mut text_ids: Vec<u32> = vec![];
    for id in ids {
        text_ids.extend(token_to_text_id.get_values_iter(u64::from(*id)))
        // if let Some(ids) = token_to_text_id.get_values(u64::from(*id)) {
        //     text_ids.extend(ids.iter()); // TODO move data, swap first
        // }
    }
    text_ids
}

#[inline]
fn sort_by_score_and_id(a: &Hit, b: &Hit) -> Ordering {
    let cmp = b.score.partial_cmp(&a.score);
    if cmp == Some(Ordering::Equal) {
        b.id.partial_cmp(&a.id).unwrap_or(Ordering::Equal)
    } else {
        cmp.unwrap()
    }
}

#[inline]
fn top_n_sort(data: Vec<Hit>, top_n: u32) -> Vec<Hit> {
    let mut worst_score = std::f32::MIN;

    let mut new_data: Vec<Hit> = Vec::with_capacity(top_n as usize * 5 + 1);
    for el in data {
        if el.score < worst_score {
            continue;
        }

        check_apply_top_n_sort(&mut new_data, top_n, &sort_by_score_and_id, &mut |the_worst: &Hit| worst_score = the_worst.score);

        new_data.push(el);
    }

    // Sort by score and anchor_id -- WITHOUT anchor_id SORTING SKIP MAY WORK NOT CORRECTLY FOR SAME SCORED ANCHOR_IDS
    new_data.sort_unstable_by(sort_by_score_and_id);
    new_data
}

#[inline]
pub(crate) fn check_apply_top_n_sort<T: std::fmt::Debug>(new_data: &mut Vec<T>, top_n: u32, sort_compare: &Fn(&T, &T) -> Ordering, new_worst: &mut FnMut(&T)) {
    if !new_data.is_empty() && new_data.len() as u32 == top_n + 200 {
        new_data.sort_unstable_by(sort_compare);
        new_data.truncate(top_n as usize);
        let new_worst_value = new_data.last().unwrap();
        trace!("new worst {:?}", new_worst_value);
        new_worst(new_worst_value);
        // worst_score = new_data.last().unwrap().score;
    }
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn search(mut request: Request, persistence: &Persistence) -> Result<SearchResult, SearchError> {
    info_time!("search");
    request.top = request.top.or(Some(10));
    request.skip = request.skip;

    let mut res = {
        info_time!("search terms");
        let mut plan = Plan::default();
        let plan_result = plan_creator(request.clone(), &mut plan);
        // info!("{:?}", plan);
        // info!("{:?}", serde_json::to_string_pretty(&plan).unwrap());
        // let yep = plan.get_output();

        // execute_steps(plan.steps, &persistence)?;
        // execute_step_in_parrael(steps, persistence).unwrap();
        for stepso in plan.get_ordered_steps() {
            execute_steps(stepso, &persistence)?;
        }
        // plan_result.0.execute_step(persistence)?;
        let mut res = plan_result.recv()?;
        drop(plan_result);
        res
    };

    let mut search_result = SearchResult { ..Default::default() };

    if let Some(boost_term) = request.boost_term {
        res = apply_boost_term(persistence, res, &boost_term)?;
    }

    if request.text_locality {
        info_time!("boost_text_locality_all");
        let boost_anchor = boost_text_locality_all(&persistence, &mut res.term_id_hits_in_field)?;
        res = apply_boost_from_iter(res, &mut boost_anchor.iter().cloned());
    }
    let term_id_hits_in_field = res.term_id_hits_in_field;
    search_result.why_found_terms = res.term_text_in_field;

    if let Some(facets_req) = request.facets {
        info_time!("all_facets {:?}", facets_req.iter().map(|el| el.field.clone()).collect::<Vec<_>>());

        let hit_ids: Vec<u32> = {
            // get sorted ids, for facets
            debug_time!("get_and_sort_for_factes");
            let mut hit_ids: Vec<u32> = res.hits_scores.iter().map(|el| el.id).collect();
            debug_time!("get_and_sort_for_factes sort only!!!");
            hit_ids.sort_unstable();
            hit_ids
        };

        search_result.facets = Some(
            facets_req
                .par_iter()
                .map(|facet_req| (facet_req.field.to_string(), facet::get_facet(persistence, facet_req, &hit_ids).unwrap()))
                .collect(),
        );
    }
    search_result.num_hits = res.hits_scores.len() as u64;
    {
        debug_time!("sort search by score");
        if let Some(top) = request.top {
            search_result.data = top_n_sort(res.hits_scores, top as u32 + request.skip.unwrap_or(0) as u32);
        } else {
            search_result.data = res.hits_scores;
            search_result.data.sort_unstable_by(sort_by_score_and_id);
        }
    }
    let topn_results = apply_top_skip(&search_result.data, request.skip, request.top);
    search_result.data = topn_results;

    if request.why_found && request.select.is_some() {
        // TODO WHY_FOUND, WHY_FOUND is done on the object, when all fields are returned
        let anchor_ids: Vec<u32> = search_result.data.iter().map(|el| el.id).collect();
        let why_found_info = get_why_found(&persistence, &anchor_ids, &term_id_hits_in_field)?;
        search_result.why_found_info = why_found_info;
    }
    Ok(search_result)
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn apply_boost_term(persistence: &Persistence, mut res: SearchFieldResult, boost_term: &[RequestSearchPart]) -> Result<SearchFieldResult, SearchError> {
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
            info_time!("boost_term_cache");
            let mut boost_iter = data
                .iter()
                .map(|el| {
                    let boost_val: f32 = el.request.boost.map(|el|el.into_inner()).unwrap_or(2.0);
                    el.hits_ids.iter().map(move |id| Hit::new(*id, boost_val))
                })
                .into_iter()
                .kmerge_by(|a, b| a.id < b.id);

            // {
            //     let mut boost_iter_data:Vec<Hit> = data.iter()
            //     .map(|el| {
            //         let boost_val:f32 = el.request.boost.unwrap_or(2.0).clone();
            //         el.hits_ids.iter().map(move|id| Hit::new(*id, boost_val ))
            //     })
            //     .into_iter().kmerge_by(|a, b| a.id < b.id).collect();

            //     {
            //         info_time!("binary search boost");
            //         let mut last_pos = 0;
            //         for hit in res.hits_scores.iter_mut(){
            //             match boost_iter_data[last_pos ..].binary_search_by_key(&hit.id, |&hit| hit.id) {
            //                 Ok(boost_hit) => {
            //                     hit.score *= boost_iter_data[boost_hit].score;
            //                     last_pos =boost_hit;
            //                 },
            //                 Err(pos) => { last_pos = pos;},
            //             }
            //         }
            //     }

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

            //     // { // Hashmap ist doof
            //     //     let mut boost_iter_data:FnvHashMap<u32, f32> = data.iter()
            //     //     .map(|el| {
            //     //         let boost_val:f32 = el.request.boost.unwrap_or(2.0).clone();
            //     //         el.hits_ids.iter().map(move|id| Hit::new(*id, boost_val ))
            //     //     })
            //     //     .into_iter().kmerge_by(|a, b| a.id < b.id).map(|hit| (hit.id, hit.score)).collect();

            //     //     info_time!("hashmap boost");
            //     //     for hit in res.hits_scores.iter_mut(){
            //     //         if let Some(boost_hit) = boost_iter_data.get(&hit.id) {
            //     //             hit.score *= boost_hit;
            //     //         }
            //     //     }
            //     // }

            //     {
            //         info_time!("merge search boost");
            //         res = apply_boost_from_iter(res, &mut boost_iter_data.into_iter());
            //     }

            //     debug_time!("binary search".to_string());

            // }

            debug_time!("boost_intersect_hits_vec_multi");
            res = apply_boost_from_iter(res, &mut boost_iter);

            from_cache = true;
        }
    }

    if !from_cache {
        let r: Result<Vec<_>, SearchError> = boost_term
            .to_vec()
            .into_par_iter()
            .map(|boost_term_req: RequestSearchPart| {
                // boost_term_req.ids_only = true;
                // boost_term_req.fast_field = true;
                let boost_term_req = PlanRequestSearchPart{request:boost_term_req, ids_only: true, ..Default::default()};
                let mut result = search_field::get_hits_in_field(persistence, &boost_term_req, None)?;
                result = search_field::resolve_token_to_anchor(persistence, &boost_term_req.request, None, &result)?;
                Ok(result)
                
            })
            .collect();
        let mut data = r?;
        res = boost_intersect_hits_vec_multi(res, &mut data);
        {
            persistence.term_boost_cache.write().insert(boost_term.to_vec(), data);
        }
    }
    Ok(res)
}

//TODO no copy
#[cfg_attr(feature = "flame_it", flame)]
pub fn apply_top_skip<T: Clone>(hits: &[T], skip: Option<usize>, top: Option<usize>) -> Vec<T> {
    let skip = skip.unwrap_or(0);
    if let Some(mut top) = top {
        top = cmp::min(top + skip, hits.len());
        hits[skip..top].to_vec()
    } else {
        hits[skip..].to_vec()
    }
}

use facet;

#[cfg_attr(feature = "flame_it", flame)]
pub fn get_shortest_result<T: std::iter::ExactSizeIterator>(results: &[T]) -> usize {
    let mut shortest = (0, std::u64::MAX);
    for (index, res) in results.iter().enumerate() {
        if (res.len() as u64) < shortest.1 {
            shortest = (index, res.len() as u64);
        }
    }
    shortest.0
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn get_longest_result<T: std::iter::ExactSizeIterator>(results: &[T]) -> usize {
    let mut longest = (0, std::u64::MIN);
    for (index, res) in results.iter().enumerate() {
        if (res.len() as u64) > longest.1 {
            longest = (index, res.len() as u64);
        }
    }
    longest.0
}

fn merge_term_id_hits(results: &mut Vec<SearchFieldResult>) -> FnvHashMap<String, FnvHashMap<String, Vec<TermId>>> {
    //attr -> term -> hits
    let mut term_id_hits_in_field: FnvHashMap<String, FnvHashMap<String, Vec<TermId>>> = FnvHashMap::default();
    for el in results.iter_mut() {
        for (attr, mut v) in el.term_id_hits_in_field.drain() {
            // term_id_hits_in_field.insert(attr, v);
            let attr_term_hits = term_id_hits_in_field.entry(attr).or_default();
            for (term, hits) in v.drain() {
                attr_term_hits.insert(term, hits);
            }
        }
    }
    debug!("term_id_hits_in_field {:?}", term_id_hits_in_field);
    term_id_hits_in_field
}
fn merge_term_id_texts(results: &mut Vec<SearchFieldResult>) -> FnvHashMap<String, Vec<String>> {
    //attr -> term_texts
    let mut term_text_in_field: FnvHashMap<String, Vec<String>> = FnvHashMap::default();
    for el in results.iter_mut() {
        for (attr, mut v) in el.term_text_in_field.drain() {
            let attr_term_hits = term_text_in_field.entry(attr).or_default();
            attr_term_hits.extend(v.iter().cloned());
        }
    }
    debug!("term_text_in_field {:?}", term_text_in_field);
    term_text_in_field
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn union_hits_vec(mut or_results: Vec<SearchFieldResult>) -> SearchFieldResult {
    if or_results.len() == 0 {
        return SearchFieldResult { ..Default::default() };
    }
    if or_results.len() == 1 {
        let res = or_results.swap_remove(0);
        return res;
    }
    let term_id_hits_in_field = { merge_term_id_hits(&mut or_results) };
    let term_text_in_field = { merge_term_id_texts(&mut or_results) };

    let index_longest: usize = get_longest_result(&or_results.iter().map(|el| el.hits_scores.iter()).collect::<Vec<_>>());

    let longest_len = or_results[index_longest].hits_scores.len() as f32;
    let len_total: usize = or_results.iter().map(|el| el.hits_scores.len()).sum();
    let sum_other_len = len_total as f32 - longest_len;

    // if longest_len as f32 * 0.05 > sum_other_len{ // TODO check best value
    //     let mut union_hits = or_results.swap_remove(index_longest).hits_scores;

    //INSERT SUPER SLOW
    // {
    //     debug_time!("union hits sort input".to_string());
    //     for res in or_results.iter_mut() {
    //         res.hits_scores.sort_unstable_by_key(|el| el.id);
    //         //TODO ALSO DEDUP???
    //     }
    // }

    // let iterators:Vec<_> = or_results.iter().map(|el| el.hits_scores.iter()).collect();
    // let mergo = iterators.into_iter().kmerge_by(|a, b| a.id < b.id);
    // debug_time!("union hits kmerge".to_string());

    // for (mut id, mut group) in &mergo.into_iter().group_by(|el| el.id)
    // {
    //     let sum_score = group.map(|a| a.score).sum(); // TODO same term = MAX, different terms = SUM
    //     let mkay = union_hits.binary_search_by_key(&id, |&a| a.id);
    //     match mkay {
    //         Ok(pos) => {
    //             union_hits[pos].score += sum_score;
    //         },
    //         Err(pos) => {
    //             union_hits.insert(pos, Hit::new(id,sum_score))
    //         },
    //     }
    // }

    //     {
    //         debug_time!("union hits append ".to_string());
    //         for mut res in or_results {
    //             union_hits.append(&mut res.hits_scores);
    //         }
    //     }

    //     debug_time!("union hits sort and dedup ".to_string());
    //     union_hits.sort_unstable_by_key(|el| el.id);
    //     let prev = union_hits.len();
    //     union_hits.dedup_by_key(|el| el.id); // TODO FixMe Score

    //     debug!("union hits merged from {} to {} hits", prev, union_hits.len() );

    //     SearchFieldResult {
    //         hits_scores: union_hits,
    //         ..Default::default()
    //     }
    // }else{

    {
        debug_time!("union hits sort input");
        for res in &mut or_results {
            res.hits_scores.sort_unstable_by_key(|el| el.id);
            //TODO ALSO DEDUP???
        }
    }

    let mut terms = or_results.iter().map(|res| res.request.terms[0].to_string()).collect::<Vec<_>>();
    terms.sort();
    terms.dedup();

    let mut fields = or_results.iter().map(|res| res.request.path.to_string()).collect::<Vec<_>>();
    fields.sort();
    fields.dedup();
    info!("or connect search terms {:?}", terms);

    let iterators: Vec<_> = or_results
        .iter()
        .map(|res| {
            let term_id = terms.iter().position(|ref x| x == &&res.request.terms[0]).unwrap() as u8;
            let field_id = fields.iter().position(|ref x| x == &&res.request.path).unwrap() as u8;
            // res.hits_scores.iter().map(move |el| (el.hit, f16::from_f32(el.score), term_id, field_id))

            res.iter(term_id, field_id)

            // res.hits_scores.iter().map(move |hit| MiniHit {
            //     id: hit.id,
            //     score: f16::from_f32(hit.score),
            //     term_id: term_id,
            //     field_id: field_id,
            // })
        })
        .collect();

    let mut union_hits = Vec::with_capacity(longest_len as usize + sum_other_len as usize / 2);
    // let mergo = iterators.into_iter().kmerge_by(|a, b| a.1.id < b.1.id);
    let mergo = iterators.into_iter().kmerge_by(|a, b| a.id < b.id);

    // let mergo = kmerge_by::kmerge_by(iterators.into_iter(), |a, b| a.id < b.id);
    // let mergo = kmerge_by::kmerge_by(iterators.into_iter(), |a, b| a.id < b.id);

    debug_time!("union hits kmerge");

    // for (mut id, mut group) in &mergo.into_iter().group_by(|el| el.0.id) {
    //     let sum_score = group.map(|a| a.0.score).sum(); // TODO same term = MAX, different terms = SUM
    //     union_hits.push(Hit::new(id, sum_score));
    // }

    // let mut field_id_hits = 0;
    for (mut id, mut group) in &mergo.into_iter().group_by(|el| el.id) {
        let mut term_id_hits = 0;
        let mut sum_score = 0.;
        // let mut num_hits:u8 = 0;
        for el in group {
            // num_hits +=1;
            // set_bit_at(&mut field_id_hits, el.field_id);
            set_bit_at(&mut term_id_hits, el.term_id);
            sum_score += el.score.to_f32();
        }

        // if num_hits <= 3{
        //     continue;
        // }

        // if num_hits != 1 {
        let num_distinct_terms = term_id_hits.count_ones() as f32;
        sum_score = sum_score * num_distinct_terms * num_distinct_terms;
        // };
        //let num_distinct_terms = term_id_hits.count_ones() as f32;
        // let num_fields = field_id_hits.count_ones() as f32;
        // let field_locality_boost = num_hits as f32 / num_fields;
        // sum_score = sum_score * num_distinct_terms * num_distinct_terms * field_locality_boost;

        // let mut sum_score = group.map(|a| a.score).sum();
        union_hits.push(Hit::new(id, sum_score));
        // term_id_hits = 0;
        // field_id_hits = 0;
    }

    // debug!("union hits merged from {} to {} hits", prev, union_hits.len() );
    SearchFieldResult {
        term_id_hits_in_field,
        term_text_in_field,
        hits_scores: union_hits,
        ..Default::default()
    }
    // }
}

#[derive(Debug, Clone)]
pub struct MiniHit {
    pub id: u32,
    pub score: f16,
    pub term_id: u8,
    // pub field_id: u8,
}

#[test]
fn union_hits_vec_test() {
    let hits1 = vec![Hit::new(10, 20.0), Hit::new(0, 10.0), Hit::new(5, 20.0)]; // unsorted
    let hits2 = vec![Hit::new(0, 20.0), Hit::new(3, 20.0), Hit::new(10, 30.0), Hit::new(20, 30.0)];

    let yop = vec![
        SearchFieldResult {
            request: RequestSearchPart {
                terms: vec!["a".to_string()],
                ..Default::default()
            },
            hits_scores: hits1,
            ..Default::default()
        },
        SearchFieldResult {
            request: RequestSearchPart {
                terms: vec!["b".to_string()],
                ..Default::default()
            },
            hits_scores: hits2,
            ..Default::default()
        },
    ];

    let res = union_hits_vec(yop);

    assert_eq!(
        res.hits_scores,
        vec![Hit::new(0, 120.0), Hit::new(3, 20.0), Hit::new(5, 20.0), Hit::new(10, 200.0), Hit::new(20, 30.0)]
    );
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn intersect_hits_vec(mut and_results: Vec<SearchFieldResult>) -> SearchFieldResult {
    if and_results.len() == 0 {
        return SearchFieldResult { ..Default::default() };
    }
    if and_results.len() == 1 {
        let res = and_results.swap_remove(0);
        return res;
    }
    let term_id_hits_in_field = { merge_term_id_hits(&mut and_results) };
    let term_text_in_field = { merge_term_id_texts(&mut and_results) };

    let index_shortest = get_shortest_result(&and_results.iter().map(|el| el.hits_scores.iter()).collect::<Vec<_>>());

    for res in &mut and_results {
        res.hits_scores.sort_unstable_by_key(|el| el.id); //TODO ALSO DEDUP???
    }
    let mut shortest_result = and_results.swap_remove(index_shortest).hits_scores;

    // let mut iterators = &and_results.iter().map(|el| el.hits_scores.iter()).collect::<Vec<_>>();

    let mut iterators_and_current = and_results
        .iter_mut()
        .map(|el| {
            let mut iterator = el.hits_scores.iter();
            let current = iterator.next();
            (iterator, current)
        })
        .filter(|el| el.1.is_some())
        .map(|el| (el.0, el.1.unwrap()))
        .collect::<Vec<_>>();

    let mut intersected_hits = Vec::with_capacity(shortest_result.len());
    for current_el in &mut shortest_result {
        let current_id = current_el.id;
        let current_score = current_el.score;

        if iterators_and_current.iter_mut().all(|ref mut iter_n_current| {
            if (iter_n_current.1).id == current_id {
                return true;
            }
            let iter = &mut iter_n_current.0;
            while let Some(el) = iter.next() {
                let id = el.id;
                iter_n_current.1 = el;
                if id > current_id {
                    return false;
                }
                if id == current_id {
                    return true;
                }
            }
            false
        }) {
            let mut score = iterators_and_current.iter().map(|el| (el.1).score).sum();
            score += current_score; //TODO SCORE Max oder Sum FOR AND
            intersected_hits.push(Hit::new(current_id, score));
        }
    }
    // all_results
    SearchFieldResult {
        term_id_hits_in_field,
        term_text_in_field,
        hits_scores: intersected_hits,
        ..Default::default()
    }
}

#[test]
fn intersect_hits_vec_test() {
    let hits1 = vec![Hit::new(10, 20.0), Hit::new(0, 20.0), Hit::new(5, 20.0)]; // unsorted
    let hits2 = vec![Hit::new(0, 20.0), Hit::new(3, 20.0), Hit::new(10, 30.0), Hit::new(20, 30.0)];

    let yop = vec![
        SearchFieldResult {
            hits_scores: hits1,
            ..Default::default()
        },
        SearchFieldResult {
            hits_scores: hits2,
            ..Default::default()
        },
    ];

    let res = intersect_hits_vec(yop);

    assert_eq!(res.hits_scores, vec![Hit::new(0, 40.0), Hit::new(10, 50.0)]);
}

// #[cfg_attr(feature = "flame_it", flame)]
// fn boost_intersect_hits_vec(mut results: SearchFieldResult, mut boost: SearchFieldResult) -> SearchFieldResult {
//     results.hits_scores.sort_unstable_by_key(|el| el.id); //TODO SORT NEEDED??
//     boost.hits_scores.sort_unstable_by_key(|el| el.id); //TODO SORT NEEDED??

//     let mut boost_iter = boost.hits_scores.into_iter();
//     apply_boost_from_iter(results, &mut boost_iter) // TODO FIXME
// }

fn apply_boost_from_iter(mut results: SearchFieldResult, mut boost_iter: &mut Iterator<Item = Hit>) -> SearchFieldResult {
    let move_boost = |hit: &mut Hit, hit_curr: &mut Hit, boost_iter: &mut Iterator<Item = Hit>| {
        //Forward the boost iterator and look for matches
        for b_hit in boost_iter {
            if b_hit.id > hit.id {
                *hit_curr = b_hit.clone(); //TODO LOW maybe change data pointed to by hit_curr
                break;
            } else if b_hit.id == hit.id {
                *hit_curr = b_hit.clone();
                hit.score *= b_hit.score;
                break;
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

    results
}

#[cfg_attr(feature = "flame_it", flame)]
fn boost_intersect_hits_vec_multi(mut results: SearchFieldResult, boost: &mut Vec<SearchFieldResult>) -> SearchFieldResult {
    {
        debug_time!("boost hits sort input");
        results.hits_scores.sort_unstable_by_key(|el| el.id); //TODO SORT NEEDED??
        for res in boost.iter_mut() {
            res.hits_scores.sort_unstable_by_key(|el| el.id);
            res.hits_ids.sort_unstable();
        }
    }
    // let mut boosts =
    let mut boost_iter = boost
        .iter()
        .map(|el| {
            let boost_val: f32 = el.request.boost.map(|el|el.into_inner()).unwrap_or(2.0);
            el.hits_ids.iter().map(move |id| Hit::new(*id, boost_val)) //TODO create version for hits_scores
        })
        .into_iter()
        .kmerge_by(|a, b| a.id < b.id);

    debug_time!("boost_intersect_hits_vec_multi");
    apply_boost_from_iter(results, &mut boost_iter)
}

#[test]
fn boost_intersect_hits_vec_test_multi() {
    let hits1 = vec![Hit::new(10, 20.0), Hit::new(0, 20.0), Hit::new(5, 20.0), Hit::new(60, 20.0)]; // unsorted
    let boost = vec![0, 3, 10, 70];
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

    let res = boost_intersect_hits_vec_multi(
        SearchFieldResult {
            hits_scores: hits1,
            ..Default::default()
        },
        &mut boosts,
    );

    assert_eq!(res.hits_scores, vec![Hit::new(0, 40.0), Hit::new(5, 20.0), Hit::new(10, 80.0), Hit::new(60, 40.0)]);
}

// #[test]
// fn boost_intersect_hits_vec_test() {
//     let hits1 = vec![Hit::new(10, 20.0), Hit::new(0, 20.0), Hit::new(5, 20.0)]; // unsorted
//     let boost = vec![Hit::new(0, 20.0), Hit::new(3, 20.0), Hit::new(10, 30.0), Hit::new(20, 30.0)];

//     let res = boost_intersect_hits_vec(
//         SearchFieldResult {
//             hits_scores: hits1,
//             ..Default::default()
//         },
//         SearchFieldResult {
//             hits_scores: boost,
//             ..Default::default()
//         },
//     );

//     assert_eq!(res.hits_scores, vec![Hit::new(0, 400.0), Hit::new(5, 20.0), Hit::new(10, 600.0)]);
// }

#[cfg(test)]
mod bench_intersect {
    use super::*;
    use test;
    #[bench]
    fn bench_boost_intersect_hits_vec_multi(b: &mut test::Bencher) {
        let hits1: Vec<Hit> = (0..4_000_00).map(|i| Hit::new(i * 5 as u32, 2.2 as f32)).collect();
        let hits2: Vec<Hit> = (0..40_000).map(|i| Hit::new(i * 3 as u32, 2.2 as f32)).collect();

        b.iter(|| {
            boost_intersect_hits_vec_multi(
                SearchFieldResult {
                    hits_scores: hits1.clone(),
                    ..Default::default()
                },
                &mut vec![SearchFieldResult {
                    hits_scores: hits2.clone(),
                    ..Default::default()
                }],
            )
        })
    }
}

// #[bench]
// fn bench_intersect_hits_vec(b: &mut test::Bencher) {
//     let hits1 = (0..4_000_00).map(|i|(i*5, 2.2)).collect();
//     let hits2 = (0..40_000).map(|i|(i*3, 2.2)).collect();

//     let yop = vec![
//         SearchFieldResult {
//             hits_scores: hits1,
//             ..Default::default()
//         },
//         SearchFieldResult {
//             hits_scores: hits2,
//             ..Default::default()
//         },
//     ];

//     b.iter(|| intersect_hits_vec())
// }

#[cfg_attr(feature = "flame_it", flame)]
pub fn add_boost(persistence: &Persistence, boost: &RequestBoostPart, hits: &mut SearchFieldResult) -> Result<(), SearchError> {
    // let key = util::boost_path(&boost.path);
    let boost_path = boost.path.to_string() + BOOST_VALID_TO_VALUE;
    let boostkv_store = persistence.get_boost(&boost_path)?;
    let boost_param = boost.param.map(|el|el.into_inner()).unwrap_or(0.0);

    let expre = boost.expression.as_ref().map(|expression| ScoreExpression::new(expression.clone()));
    let default = vec![];
    let skip_when_score = boost.skip_when_score.as_ref().map(|vecco|vecco.iter().map(|el|el.into_inner()).collect()).unwrap_or(default);
    for hit in &mut hits.hits_scores {
        if !skip_when_score.is_empty() && skip_when_score.iter().any(|x| (*x - hit.score).abs() < 0.00001) {
            // float comparisons should usually include a error margin
            continue;
        }
        let value_id = &hit.id;
        let mut score = &mut hit.score;
        // let ref vals_opt = boostkv_store.get(*value_id as usize);
        let val_opt = &boostkv_store.get_value(u64::from(*value_id));

        if let Some(boost_value) = val_opt.as_ref() {
            debug!("Found in boosting for value_id {:?}: {:?}", value_id, val_opt);
            let boost_value = *boost_value;
            match boost.boost_fun {
                Some(BoostFunction::Log10) => {
                    let prev_score = *score;
                    *score += (boost_value as f32 + boost_param).log10(); // @Temporary // @Hack // @Cleanup // @FixMe
                    trace!(
                        "boosting value_id {:?} score {:?} with token_value {:?} boost_value {:?} to {:?}",
                        *value_id,
                        prev_score,
                        boost_value,
                        (boost_value as f32 + boost_param).log10(),
                        *score
                    );
                }
                Some(BoostFunction::Linear) => {
                    *score *= boost_value as f32 + boost_param; // @Temporary // @Hack // @Cleanup // @FixMe
                }
                Some(BoostFunction::Add) => {
                    trace!(
                        "boosting value_id {:?} score {:?} with token_value {:?} boost_value {:?} to {:?}",
                        *value_id,
                        score,
                        boost_value,
                        (boost_value as f32 + boost_param),
                        *score + (boost_value as f32 + boost_param)
                    );
                    *score += boost_value as f32 + boost_param;
                }
                None => {}
            }
            if let Some(exp) = expre.as_ref() {
                let prev_score = *score;
                *score += exp.get_score(boost_value as f32);
                trace!(
                    "boost {:?} to {:?} with boost_fun({:?})={:?}",
                    prev_score,
                    score,
                    boost_value,
                    exp.get_score(boost_value as f32)
                );
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
pub enum SearchError {
    Io(io::Error),
    StringError(String),
    MetaData(serde_json::Error),
    Utf8Error(std::str::Utf8Error),
    FstError(fst::Error),
    // FstLevenShtein(fst_levenshtein::Error),
    CrossBeamError(crossbeam_channel::SendError<std::collections::HashMap<u32, f32, std::hash::BuildHasherDefault<fnv::FnvHasher>>>),
    CrossBeamError2(Box<crossbeam_channel::SendError<SearchFieldResult>>),
    CrossBeamErrorReceive(crossbeam_channel::RecvError),
    TooManyStates,
}
// Automatic Conversion
impl From<io::Error> for SearchError {
    fn from(err: io::Error) -> SearchError {
        SearchError::Io(err)
    }
}
impl From<serde_json::Error> for SearchError {
    fn from(err: serde_json::Error) -> SearchError {
        SearchError::MetaData(err)
    }
}
impl From<std::str::Utf8Error> for SearchError {
    fn from(err: std::str::Utf8Error) -> SearchError {
        SearchError::Utf8Error(err)
    }
}
impl From<fst::Error> for SearchError {
    fn from(err: fst::Error) -> SearchError {
        SearchError::FstError(err)
    }
}
// impl From<fst_levenshtein::Error> for SearchError {
//     fn from(err: fst_levenshtein::Error) -> SearchError {
//         SearchError::FstLevenShtein(err)
//     }
// }
impl From<crossbeam_channel::SendError<std::collections::HashMap<u32, f32, std::hash::BuildHasherDefault<fnv::FnvHasher>>>> for SearchError {
    fn from(err: crossbeam_channel::SendError<std::collections::HashMap<u32, f32, std::hash::BuildHasherDefault<fnv::FnvHasher>>>) -> SearchError {
        SearchError::CrossBeamError(err)
    }
}
impl From<crossbeam_channel::SendError<SearchFieldResult>> for SearchError {
    fn from(err: crossbeam_channel::SendError<SearchFieldResult>) -> SearchError {
        SearchError::CrossBeamError2(Box::new(err))
    }
}
impl From<crossbeam_channel::RecvError> for SearchError {
    fn from(err: crossbeam_channel::RecvError) -> SearchError {
        SearchError::CrossBeamErrorReceive(err)
    }
}

impl From<String> for SearchError {
    fn from(err: String) -> SearchError {
        SearchError::StringError(err)
    }
}

impl<'a> From<&'a str> for SearchError {
    fn from(err: &'a str) -> SearchError {
        SearchError::StringError(err.to_string())
    }
}

pub use std::error::Error;

impl fmt::Display for SearchError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "\n{}", self)?;
        Ok(())
    }
}

impl Error for SearchError {
    fn cause(&self) -> Option<&Error> {
        None
    }

    fn description(&self) -> &str {
        "self.error.description()"
    }
}

#[inline]
fn join_and_get_text_for_ids(persistence: &Persistence, id: u32, prop: &str) -> Result<Option<String>, SearchError> {
    let text_value_id_opt = join_for_1_to_1(persistence, id, &prop.add(TEXTINDEX).add(PARENT_TO_VALUE_ID))?;
    Ok(text_value_id_opt.map(|text_value_id| get_text_for_id(persistence, &prop.add(TEXTINDEX), text_value_id)))
}

pub fn read_data(persistence: &Persistence, id: u32, fields: &[String]) -> Result<serde_json::Value, SearchError> {
    // let all_steps: FnvHashMap<String, Vec<String>> = fields.iter().map(|field| (field.clone(), util::get_steps_to_anchor(&field))).collect();
    // let all_steps: Vec<Vec<String>> = fields.iter().map(|field| util::get_steps_to_anchor(&field)).collect();
    // let paths = util::get_steps_to_anchor(&request.path);
    // let tree = to_node_tree(all_steps);
    let tree = get_read_tree_from_fields(persistence, fields);
    read_tree(persistence, id, &tree)
    // Ok(serde_json::to_string_pretty(&dat).unwrap())
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn read_tree(persistence: &Persistence, id: u32, tree: &NodeTree) -> Result<serde_json::Value, SearchError> {
    let mut json = json!({});
    match *tree {
        NodeTree::Map(ref map) => {
            for (prop, sub_tree) in map.iter() {
                let current_path = prop.add(PARENT_TO_VALUE_ID);
                let is_array = prop.ends_with("[]");
                match *sub_tree {
                    NodeTree::IsLeaf => {
                        if is_array {
                            if let Some(sub_ids) = join_for_1_to_n(persistence, id, &current_path)? {
                                let mut sub_data = vec![];
                                for sub_id in sub_ids {
                                    if let Some(texto) = join_and_get_text_for_ids(persistence, sub_id, prop)? {
                                        sub_data.push(json!(texto));
                                    }
                                }
                                json[extract_prop_name(prop)] = json!(sub_data);
                            }
                        } else if let Some(texto) = join_and_get_text_for_ids(persistence, id, prop)? {
                            json[extract_prop_name(prop)] = json!(texto);
                        }
                    }
                    NodeTree::Map(ref _next) => {
                        if !persistence.has_index(&current_path) {
                            // Special case a node without information an object in object e.g. there is no information 1:n to store
                            json[extract_prop_name(prop)] = read_tree(persistence, id, &sub_tree)?;
                        } else if let Some(sub_ids) = join_for_1_to_n(persistence, id, &current_path)? {
                            if is_array {
                                let mut sub_data = vec![];
                                for sub_id in sub_ids {
                                    sub_data.push(read_tree(persistence, sub_id, &sub_tree)?);
                                }
                                json[extract_prop_name(prop)] = json!(sub_data);
                            } else if let Some(sub_id) = sub_ids.get(0) {
                                json[extract_prop_name(prop)] = read_tree(persistence, *sub_id, &sub_tree)?;
                            }
                        }
                    }
                }
            }
        }
        NodeTree::IsLeaf => {}
    }

    Ok(json)
}

//TODO CHECK FIELD VALIDTY
pub fn get_read_tree_from_fields(_persistence: &Persistence, fields: &[String]) -> util::NodeTree {
    let all_steps: Vec<Vec<String>> = fields.iter().map(|field| util::get_all_steps_to_anchor(&field)).collect();
    to_node_tree(all_steps)
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn join_to_parent_with_score(persistence: &Persistence, input: &SearchFieldResult, path: &str, _trace_time_info: &str) -> Result<SearchFieldResult, SearchError> {
    let mut total_values = 0;
    let num_hits = input.hits_scores.len();

    let mut hits = Vec::with_capacity(num_hits);
    let kv_store = persistence.get_valueid_to_parent(path)?;

    for hit in &input.hits_scores {
        let mut score = hit.score;
        if let Some(values) = kv_store.get_values(u64::from(hit.id)).as_ref() {
            total_values += values.len();
            hits.reserve(values.len());
            // trace!("value_id: {:?} values: {:?} ", value_id, values);
            for parent_val_id in values {
                hits.push(Hit::new(*parent_val_id, score));
            }
        }
    }
    hits.sort_unstable_by_key(|a| a.id);
    hits.dedup_by(|a, b| {
        if a.id == b.id {
            b.score = b.score.max(a.score);
            true
        } else {
            false
        }
    });

    debug!("{:?} hits hit {:?} distinct ({:?} total ) in column {:?}", num_hits, hits.len(), total_values, path);
    let mut res = SearchFieldResult::new_from(&input);
    res.hits_scores = hits;
    Ok(res)
}

// #[cfg_attr(feature = "flame_it", flame)]
// pub(crate) fn join_for_read(persistence: &Persistence, input: Vec<u32>, path: &str) -> Result<FnvHashMap<u32, Vec<u32>>, SearchError> {
//     let mut hits: FnvHashMap<u32, Vec<u32>> = FnvHashMap::default();
//     let kv_store = persistence.get_valueid_to_parent(path)?;
//     // debug_time!("term hits hit to column");
//     debug_time!(format!("{:?} ", path));
//     for value_id in input {
//         let values = &kv_store.get_values(u64::from(value_id));
//         if let Some(values) = values.as_ref() {
//             hits.reserve(values.len());
//             hits.insert(value_id, values.clone());
//         }
//     }
//     debug!("hits hit {:?} distinct in column {:?}", hits.len(), path);

//     Ok(hits)
// }

#[cfg_attr(feature = "flame_it", flame)]
pub fn join_for_1_to_1(persistence: &Persistence, value_id: u32, path: &str) -> Result<std::option::Option<u32>, SearchError> {
    let kv_store = persistence.get_valueid_to_parent(path)?;
    // trace!("path {:?} id {:?} resulto {:?}", path, value_id, kv_store.get_value(value_id as u64));
    Ok(kv_store.get_value(u64::from(value_id)))
}
#[cfg_attr(feature = "flame_it", flame)]
pub fn join_for_1_to_n(persistence: &Persistence, value_id: u32, path: &str) -> Result<Option<Vec<u32>>, SearchError> {
    let kv_store = persistence.get_valueid_to_parent(path)?;
    Ok(kv_store.get_values(u64::from(value_id)))
}
