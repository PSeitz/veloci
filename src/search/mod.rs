pub(crate) mod boost;
pub mod search_field;
pub mod search_field_result;
mod set_op;
pub mod stopwords;

pub(crate) use self::boost::*;
pub use self::{search_field::*, search_field_result::*, set_op::*};
use super::highlight_field;
use crate::{
    error::VelociError,
    execution_plan::*,
    expression::ScoreExpression,
    facet,
    util::{self, *},
};
use json_converter;

use std::{
    self,
    cmp::{self, Ordering},
    f32, mem, str, u32,
};

use crate::persistence::{Persistence, *};
use doc_store::DocLoader;
use fnv::{FnvHashMap, FnvHashSet};
use ordered_float::OrderedFloat;
use rayon::prelude::*;
use serde_json;

// #[derive(Serialize, Deserialize, Clone, Debug)]
// pub enum SearchOperation {
//     And(Vec<SearchOperation>),
//     Or(Vec<SearchOperation>),
//     Search(RequestSearchPart),
// }

// impl Default for SearchOperation {
//     fn default() -> SearchOperation {
//         SearchOperation::Search(Default::default())
//     }
// }

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct Request {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// or/and/search and suggest are mutually exclusive
    pub or: Option<Vec<Request>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// or/and/search and suggest are mutually exclusive
    pub and: Option<Vec<Request>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// or/and/search and suggest are mutually exclusive
    pub search: Option<RequestSearchPart>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// or/and/search and suggest are mutually exclusive
    pub suggest: Option<Vec<RequestSearchPart>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<Vec<RequestBoostPart>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost_term: Option<Vec<RequestSearchPart>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<Vec<FacetRequest>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// list of requests tuples to phrase boost
    pub phrase_boosts: Option<Vec<RequestPhraseBoost>>,

    /// only return selected fields
    pub select: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// filter does not affect the score, it just filters the result
    pub filter: Option<Box<Request>>,
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
    #[serde(skip_serializing_if = "skip_false")]
    #[serde(default)]
    pub explain: bool,
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

#[derive(Serialize, Deserialize, Default, Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct RequestSearchPart {
    pub path: String,
    pub terms: Vec<String>, //TODO only first term used currently

    #[serde(skip_serializing)]
    #[serde(default)]
    pub explain: bool,

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

    /// default is true
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ignore_case: Option<bool>,

    /// return the snippet hit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet: Option<bool>,

    /// Override default SnippetInfo
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet_info: Option<SnippetInfo>,
}

impl Ord for RequestSearchPart {
    fn cmp(&self, other: &RequestSearchPart) -> Ordering {
        format!("{:?}", self).cmp(&format!("{:?}", other))
    }
}

#[derive(Serialize, Deserialize, Default, Clone, Debug, Hash, PartialEq, Eq)]
pub struct RequestPhraseBoost {
    pub search1: RequestSearchPart,
    pub search2: RequestSearchPart,
}

#[derive(Serialize, Deserialize, Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
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

#[derive(Serialize, Deserialize, Default, Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct RequestBoostPart {
    pub path: String,
    pub boost_fun: Option<BoostFunction>,
    pub param: Option<OrderedFloat<f32>>,
    pub skip_when_score: Option<Vec<OrderedFloat<f32>>>,
    pub expression: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub enum BoostFunction {
    Log2,
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
    pub explain: FnvHashMap<u32, Vec<Explain>>,
    #[serde(skip_serializing_if = "FnvHashMap::is_empty")]
    pub why_found_info: FnvHashMap<u32, FnvHashMap<String, Vec<String>>>,
    #[serde(skip_serializing_if = "FnvHashMap::is_empty")]
    pub why_found_terms: FnvHashMap<String, Vec<String>>,
}

// #[derive(Serialize, Deserialize, Default, Clone, Debug)]
// pub struct FilterResult {
//     pub hits_vec: Vec<TermId>,
//     pub hits_ids: FnvHashSet<TermId>,
// }

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum FilterResult {
    Vec(Vec<TermId>),
    Set(FnvHashSet<TermId>),
}

impl FilterResult {
    pub fn from_result(res: &[TermId]) -> FilterResult {
        if res.len() > 100_000 {
            FilterResult::Vec(res.to_vec())
        } else {
            let mut filter = FnvHashSet::with_capacity_and_hasher(100_000, Default::default());
            for id in res {
                filter.insert(*id);
            }
            FilterResult::Set(filter)
            // FilterResult::Set(res.iter().collect())
        }
    }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain: Option<Vec<Explain>>,
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

//
// fn hits_to_sorted_array(hits: FnvHashMap<u32, f32>) -> Vec<Hit> {
//     debug_time!("hits_to_sorted_array");
//     let mut res: Vec<Hit> = hits.iter().map(|(id, score)| Hit { id: *id, score: *score }).collect();
//     res.sort_unstable_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal)); //TODO Add sort by id when equal
//     res
// }

impl std::fmt::Display for DocWithHit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
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
        let mut cb_text = |_anchor_id: u32, value: &str, path: &str, _parent_val_id: u32| -> Result<(), serde_json::error::Error> {
            let path = path.to_string() + TEXTINDEX;
            if let Some(terms) = why_found_terms.get(&path) {
                if let Some(highlighted) = highlight_field::highlight_text(value, &terms, &DEFAULT_SNIPPETINFO) {
                    let field_name = extract_field_name(&path); // extract_field_name removes .textindex
                    let jepp = highlighted_texts.entry(field_name).or_default();
                    jepp.push(highlighted);
                }
            }
            Ok(())
        };

        let mut callback_ids = |_anchor_id: u32, _path: &str, _value_id: u32, _parent_val_id: u32| -> Result<(), serde_json::error::Error> { Ok(()) };

        json_converter::for_each_element(stream, &mut id_holder, &mut cb_text, &mut callback_ids).unwrap(); // unwrap is ok here
    }
    highlighted_texts
}

// @FixMe Tests should use to_search_result

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
                    explain: result.explain.get(&hit.id).cloned(),
                    why_found: result.why_found_info.get(&hit.id).cloned().unwrap_or_default(),
                };
            } else {
                let offsets = persistence.indices.doc_offsets.as_ref().unwrap();
                let f = persistence.get_file_handle("data").unwrap(); // TODO No unwrapo
                let doc_str = DocLoader::get_doc(f, offsets, hit.id as usize).unwrap(); // TODO No unwrapo
                                                                                        // let doc_str = DocLoader::get_doc(persistence, hit.id as usize).unwrap(); // TODO No unwrapo
                let ayse = highlight_on_original_document(&doc_str, &tokens_set);

                return DocWithHit {
                    doc: serde_json::from_str(&doc_str).unwrap(),
                    hit: hit.clone(),
                    explain: result.explain.get(&hit.id).cloned(),
                    why_found: ayse,
                };
            };
        })
        .collect::<Vec<_>>()
}

pub fn to_search_result(persistence: &Persistence, hits: SearchResult, select: &Option<Vec<String>>) -> SearchResultWithDoc {
    SearchResultWithDoc {
        data: to_documents(&persistence, &hits.data, &select, &hits),
        num_hits: hits.num_hits,
        facets: hits.facets,
    }
}

// pub fn get_search_result(persistence: &Persistence, request: Request) -> Result<SearchResultWithDoc, VelociError> {
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
) -> Result<FnvHashMap<u32, FnvHashMap<String, Vec<String>>>, VelociError> {
    debug!("why_found info {:?}", term_id_hits_in_field);
    info_time!("why_found");
    let mut anchor_highlights: FnvHashMap<_, FnvHashMap<_, Vec<_>>> = FnvHashMap::default();

    for (path, term_with_ids) in term_id_hits_in_field.iter() {
        let field_name = &extract_field_name(path); // extract_field_name removes .textindex
        let paths = util::get_steps_to_anchor(field_name);

        let all_term_ids_hits_in_path = term_with_ids.iter().fold(vec![], |mut acc, (ref _term, ref hits)| {
            acc.extend(hits.iter());
            acc
        });

        if all_term_ids_hits_in_path.is_empty() {
            continue;
        }

        for anchor_id in anchor_ids {
            let ids = facet::join_anchor_to_leaf(persistence, &[*anchor_id], &paths)?;

            for value_id in ids {
                let path = paths.last().unwrap().to_string();
                let highlighted_document = highlight_field::highlight_document(persistence, &path, u64::from(value_id), &all_term_ids_hits_in_path, &DEFAULT_SNIPPETINFO).unwrap();
                if let Some(highlighted_document) = highlighted_document {
                    let jepp = anchor_highlights.entry(*anchor_id).or_default();
                    let field_highlights = jepp.entry(field_name.clone()).or_default();
                    field_highlights.push(highlighted_document);
                }
            }
        }
    }

    Ok(anchor_highlights)
}

#[inline]
fn get_all_value_ids(ids: &[u32], token_to_text_id: &dyn IndexIdToParent<Output = u32>) -> Vec<u32> {
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
pub fn sort_by_score_and_id(a: &Hit, b: &Hit) -> Ordering {
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
pub(crate) fn check_apply_top_n_sort<T: std::fmt::Debug>(new_data: &mut Vec<T>, top_n: u32, sort_compare: &dyn Fn(&T, &T) -> Ordering, new_worst: &mut dyn FnMut(&T)) {
    if !new_data.is_empty() && new_data.len() as u32 == top_n + 200 {
        new_data.sort_unstable_by(sort_compare);
        new_data.truncate(top_n as usize);
        let new_worst_value = new_data.last().unwrap();
        trace!("new worst {:?}", new_worst_value);
        new_worst(new_worst_value);
        // worst_score = new_data.last().unwrap().score;
    }
}

pub fn search(mut request: Request, persistence: &Persistence) -> Result<SearchResult, VelociError> {
    info_time!("search");
    request.top = request.top.or(Some(10));
    request.skip = request.skip;

    let mut res = {
        info_time!("search terms");
        let mut plan = Plan::default();
        plan_creator(request.clone(), &mut plan);
        // info!("{:?}", plan);
        // info!("{:?}", serde_json::to_string_pretty(&plan).unwrap());
        // let yep = plan.get_output();

        // execute_steps(plan.steps, &persistence)?;
        // execute_step_in_parrael(steps, persistence).unwrap();
        let plan_result = plan.plan_result.as_ref().unwrap().clone();
        for stepso in plan.get_ordered_steps() {
            execute_steps(stepso, &persistence)?;
        }
        let res = plan_result.recv().unwrap();
        drop(plan_result);
        res
    };

    let mut search_result = SearchResult { ..Default::default() };
    search_result.explain = res.explain.clone();

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
    // let topn_results = apply_top_skip(&search_result.data, request.skip, request.top);
    // search_result.data = topn_results;
    apply_top_skip(&mut search_result.data, request.skip, request.top);

    if request.why_found && request.select.is_some() {
        let anchor_ids: Vec<u32> = search_result.data.iter().map(|el| el.id).collect();
        let why_found_info = get_why_found(&persistence, &anchor_ids, &term_id_hits_in_field)?;
        search_result.why_found_info = why_found_info;
    }
    Ok(search_result)
}

pub fn apply_top_skip<T: Clone>(hits: &mut Vec<T>, skip: Option<usize>, top: Option<usize>) {
    if let Some(mut skip) = skip {
        skip = cmp::min(skip, hits.len());
        hits.drain(..skip);
    }
    if let Some(mut top) = top {
        top = cmp::min(top, hits.len());
        hits.drain(top..);
    }
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

//     b.iter(|| intersect_hits_score())
// }

#[inline]
fn join_and_get_text_for_ids(persistence: &Persistence, id: u32, prop: &str) -> Result<Option<String>, VelociError> {
    // TODO CHECK field_name exists previously
    let field_name = prop.add(TEXTINDEX);
    let text_value_id_opt = join_for_1_to_1(persistence, id, &field_name.add(PARENT_TO_VALUE_ID))?;
    if let Some(text_value_id) = text_value_id_opt {
        let text = if text_value_id >= persistence.metadata.columns[prop].textindex_metadata.num_text_ids as u32 {
            let text_id_to_token_ids = persistence.get_valueid_to_parent(field_name.add(TEXT_ID_TO_TOKEN_IDS))?;
            let vals = text_id_to_token_ids.get_values(u64::from(text_value_id));
            if let Some(vals) = vals {
                vals.iter()
                    .map(|token_id| get_text_for_id(persistence, &field_name, *token_id))
                    .collect::<Vec<_>>()
                    .concat()
            } else {
                return Err(VelociError::MissingTextId {
                    text_value_id,
                    field_name: field_name.add(TEXT_ID_TO_TOKEN_IDS),
                });
            }
        } else {
            get_text_for_id(persistence, &field_name, text_value_id)
        };

        Ok(Some(text))
    } else {
        Ok(None)
    }
}

pub fn read_data(persistence: &Persistence, id: u32, fields: &[String]) -> Result<serde_json::Value, VelociError> {
    let tree = get_read_tree_from_fields(persistence, fields);
    read_tree(persistence, id, &tree)
}

pub fn read_tree(persistence: &Persistence, id: u32, tree: &NodeTree) -> Result<serde_json::Value, VelociError> {
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
pub fn get_read_tree_from_fields(persistence: &Persistence, fields: &[String]) -> util::NodeTree {
    let all_steps: Vec<Vec<String>> = fields
        .iter()
        .filter(|path| persistence.has_index(&path.add(TEXTINDEX).add(PARENT_TO_VALUE_ID)))
        .map(|field| util::get_all_steps_to_anchor(&field))
        .collect();
    to_node_tree(all_steps)
}

pub fn join_to_parent_with_score(persistence: &Persistence, input: &SearchFieldResult, path: &str, _trace_time_info: &str) -> Result<SearchFieldResult, VelociError> {
    let mut total_values = 0;
    let num_hits = input.hits_scores.len();

    let mut hits = Vec::with_capacity(num_hits);
    let kv_store = persistence.get_valueid_to_parent(path)?;

    let should_explain = input.request.explain;
    // let explain = input.explain;
    let mut explain_hits: FnvHashMap<u32, Vec<Explain>> = FnvHashMap::default();

    for hit in &input.hits_scores {
        let score = hit.score;
        if let Some(values) = kv_store.get_values(u64::from(hit.id)).as_ref() {
            total_values += values.len();
            hits.reserve(values.len());
            // trace!("value_id: {:?} values: {:?} ", value_id, values);
            for parent_val_id in values {
                hits.push(Hit::new(*parent_val_id, score));

                if should_explain {
                    let expains = input.explain.get(&hit.id).unwrap_or_else(|| panic!("could not find explain for id {:?}", hit.id));
                    explain_hits.entry(*parent_val_id).or_insert_with(|| expains.clone());
                }
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
    res.explain = explain_hits;
    Ok(res)
}

pub fn join_to_parent_ids(persistence: &Persistence, input: &SearchFieldResult, path: &str, _trace_time_info: &str) -> Result<SearchFieldResult, VelociError> {
    let mut total_values = 0;
    let num_hits = input.hits_ids.len();

    let mut hits = Vec::with_capacity(num_hits);
    let kv_store = persistence.get_valueid_to_parent(path)?;

    let should_explain = input.request.explain;

    let mut explain_hits: FnvHashMap<u32, Vec<Explain>> = FnvHashMap::default();

    for id in &input.hits_ids {
        if let Some(values) = kv_store.get_values(u64::from(*id)).as_ref() {
            total_values += values.len();
            hits.reserve(values.len());
            // trace!("value_id: {:?} values: {:?} ", value_id, values);
            for parent_val_id in values {
                hits.push(*parent_val_id);

                if should_explain {
                    let expains = input.explain.get(&*id).unwrap_or_else(|| panic!("could not find explain for id {:?}", *id));
                    explain_hits.entry(*parent_val_id).or_insert_with(|| expains.clone());
                }
            }
        }
    }
    hits.sort_unstable();
    hits.dedup();

    debug!("{:?} hits hit {:?} distinct ({:?} total ) in column {:?}", num_hits, hits.len(), total_values, path);
    let mut res = SearchFieldResult::new_from(&input);
    res.hits_ids = hits;
    res.explain = explain_hits;
    Ok(res)
}

//
// pub(crate) fn join_for_read(persistence: &Persistence, input: Vec<u32>, path: &str) -> Result<FnvHashMap<u32, Vec<u32>>, VelociError> {
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

pub fn join_for_1_to_1(persistence: &Persistence, value_id: u32, path: &str) -> Result<std::option::Option<u32>, VelociError> {
    let kv_store = persistence.get_valueid_to_parent(path)?;
    // trace!("path {:?} id {:?} resulto {:?}", path, value_id, kv_store.get_value(value_id as u64));
    Ok(kv_store.get_value(u64::from(value_id)))
}

pub fn join_for_1_to_n(persistence: &Persistence, value_id: u32, path: &str) -> Result<Option<Vec<u32>>, VelociError> {
    let kv_store = persistence.get_valueid_to_parent(path)?;
    Ok(kv_store.get_values(u64::from(value_id)))
}
