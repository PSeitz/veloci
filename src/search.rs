#[allow(unused_imports)]
use std::io::{self, BufRead};
#[allow(unused_imports)]
use std::path::Path;
use std::cmp;

use std;
#[allow(unused_imports)]
use std::{str, thread, f32};
#[allow(unused_imports)]
use std::sync::mpsc::sync_channel;

#[allow(unused_imports)]
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::cmp::Ordering;

#[allow(unused_imports)]
use fnv::FnvHashMap;

use serde_json;
#[allow(unused_imports)]
use std::time::Duration;
#[allow(unused_imports)]
use itertools::Itertools;


// use search_field;
use persistence::Persistence;
use doc_loader::DocLoader;
use util;
use util::concat;
use fst;
use fst_levenshtein;

#[allow(unused_imports)]
use execution_plan;
use execution_plan::*;
// use execution_plan::execute_plan;

#[allow(unused_imports)]
use rayon::prelude::*;
#[allow(unused_imports)]
use crossbeam_channel;
#[allow(unused_imports)]
use std::sync::Mutex;

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct Request {
    pub or: Option<Vec<Request>>,
    pub and: Option<Vec<Request>>,
    pub search: Option<RequestSearchPart>,
    pub suggest: Option<Vec<RequestSearchPart>>,
    pub boost: Option<Vec<RequestBoostPart>>,
    #[serde(default = "default_top")] pub top: usize,
    #[serde(default = "default_skip")] pub skip: usize,
}

fn default_top() -> usize {
    10
}
fn default_skip() -> usize {
    0
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct RequestSearchPart {
    pub path: String,
    pub terms: Vec<String>,
    #[serde(default = "default_term_operator")] pub term_operator: TermOperator,
    pub levenshtein_distance: Option<u32>,
    pub starts_with: Option<bool>,
    pub return_term: Option<bool>,
    pub snippet: Option<bool>,
    pub token_value: Option<RequestBoostPart>,
    // pub exact: Option<bool>,
    // pub first_char_exact_match: Option<bool>,
    /// boosts the search part with this value
    pub boost: Option<f32>,
    #[serde(default = "default_resolve_token_to_parent_hits")] pub resolve_token_to_parent_hits: Option<bool>,
    pub top: Option<usize>,
    pub skip: Option<usize>,
    pub snippet_info: Option<SnippetInfo>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SnippetInfo {
    #[serde(default = "default_num_words_around_snippet")]
    pub num_words_around_snippet: i64,
    #[serde(default = "default_snippet_start")]
    pub snippet_start_tag:String,
    #[serde(default = "default_snippet_end")]
    pub snippet_end_tag:String,
    #[serde(default = "default_snippet_connector")]
    pub snippet_connector:String,
    #[serde(default = "default_max_snippets")]
    pub max_snippets:u32
}
fn default_num_words_around_snippet() -> i64 { 5 }
fn default_snippet_start() -> String {"<b>".to_string() }
fn default_snippet_end() -> String {"</b>".to_string() }
fn default_snippet_connector() -> String {" ... ".to_string() }
fn default_max_snippets() -> u32 {std::u32::MAX }

lazy_static! {
    pub static ref DEFAULT_SNIPPETINFO: SnippetInfo = SnippetInfo{
        num_words_around_snippet :  default_num_words_around_snippet(),
        snippet_start_tag: default_snippet_start(),
        snippet_end_tag: default_snippet_end(),
        snippet_connector: default_snippet_connector(),
        max_snippets: default_max_snippets(),
    };
}

fn default_resolve_token_to_parent_hits() -> Option<bool> {
    Some(true)
}

fn default_term_operator() -> TermOperator {
    TermOperator::ALL
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum TermOperator {
    ALL,
    ANY,
}
impl Default for TermOperator {
    fn default() -> TermOperator {
        default_term_operator()
    }
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct RequestBoostPart {
    pub path:            String,
    pub boost_fun:       Option<BoostFunction>,
    pub param:           Option<f32>,
    pub skip_when_score: Option<Vec<f32>>,
    pub expression:      Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum BoostFunction {
    Log10,
    Linear,
    Add,
}

// #[derive(Debug)]
// struct ScoreTrace {
//     HashMap: <TermId, (ScoreSource, f32, )>
// }

// #[derive(Debug)]
// struct ScoreSource {
//     source: Vec<ScoreTrace>,
//     f32: score
// }

impl Default for BoostFunction {
    fn default() -> BoostFunction {
        BoostFunction::Log10
    }
}

// pub enum CheckOperators {
//     All,
//     One
// }
// impl Default for CheckOperators {
//     fn default() -> CheckOperators { CheckOperators::All }
// }


#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Hit {
    pub id:    u32,
    pub score: f32,
}

#[flame]
fn hits_to_sorted_array(hits: FnvHashMap<u32, f32>) -> Vec<Hit> {
    debug_time!("hits_to_array_sort");
    let mut res: Vec<Hit> = hits.iter().map(|(id, score)| Hit { id:    *id, score: *score }).collect();
    res.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal)); // Add sort by id
    res
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DocWithHit {
    pub doc: serde_json::Value,
    pub hit: Hit,
}


impl std::fmt::Display for DocWithHit {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "\n{}\t{}", self.hit.id, self.hit.score)?;
        write!(f, "\n{}", serde_json::to_string_pretty(&self.doc).unwrap())?;
        Ok(())
    }
}

// @FixMe Tests should use to_search_result
#[flame]
pub fn to_documents(persistence: &Persistence, hits: &Vec<Hit>) -> Vec<DocWithHit> {
    // DocLoader::load(persistence);
    hits.iter()
        .map(|ref hit| {
            let doc = DocLoader::get_doc(persistence, hit.id as usize).unwrap();
            DocWithHit { doc: serde_json::from_str(&doc).unwrap(), hit: *hit.clone() }
        })
        .collect::<Vec<_>>()
}

#[flame]
pub fn to_search_result(persistence: &Persistence, hits: &SearchResult) -> SearchResultWithDoc {
    SearchResultWithDoc {
        data:     to_documents(&persistence, &hits.data),
        num_hits: hits.num_hits,
    }
}

#[flame]
pub fn apply_top_skip<T: Clone>(hits: Vec<T>, skip: usize, mut top: usize) -> Vec<T> {
    top = cmp::min(top + skip, hits.len());
    hits[skip..top].to_vec()
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct SearchResult {
    pub num_hits: u64,
    pub data:     Vec<Hit>,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct SearchResultWithDoc {
    pub num_hits: u64,
    pub data:     Vec<DocWithHit>,
}

#[flame]
pub fn search_query(request: &str, persistence: &Persistence, top: Option<usize>, skip: Option<usize>, levenshtein: Option<usize>) -> Request {
    // let req = persistence.meta_data.fulltext_indices.key

    let terms = request.split(" ").collect::<Vec<&str>>();

    info_time!("generating search query");
    let parts: Vec<Request> = persistence.meta_data.fulltext_indices.keys().flat_map(|field| {
        let field_name:String = field.chars().take(field.chars().count()-10).into_iter().collect();

        let levenshtein_distance = levenshtein.unwrap_or(1);

        let requests:Vec<Request> = terms.iter().map(|term| {
            let part = RequestSearchPart {
                path: field_name.to_string(),
                terms: vec![term.to_string()],
                levenshtein_distance: Some(levenshtein_distance as u32),
                resolve_token_to_parent_hits: Some(true),
                ..Default::default()
            };
            Request {search: Some(part), ..Default::default() }

        }).collect();

        requests

    }).collect();

    Request {
        or: Some(parts),
        top: top.unwrap_or(10),
        skip: skip.unwrap_or(0),
        ..Default::default()
    }
}

#[flame]
pub fn search(request: Request, persistence: &Persistence) -> Result<SearchResult, SearchError> {
    info_time!("search");
    let skip = request.skip;
    let top = request.top;

    let plan = plan_creator(request);
    let yep = plan.get_output();
    plan.execute_step(persistence)?;
    // execute_step(plan, persistence)?;
    let res = yep.recv()?;

    // let res = search_unrolled(&persistence, request)?;
    // println!("{:?}", res);
    // let res = hits_to_array_iter(res.iter());
    // let res = hits_to_sorted_array(res);

    let mut search_result = SearchResult { num_hits: 0, data:     vec![] };
    search_result.data = hits_to_sorted_array(res.hits);
    search_result.num_hits = search_result.data.len() as u64;
    search_result.data = apply_top_skip(search_result.data, skip, top);
    Ok(search_result)
}

#[flame]
pub fn get_shortest_result<T: std::iter::ExactSizeIterator>(results: &Vec<T>) -> usize {
    let mut shortest = (0, std::u64::MAX);
    for (index, res) in results.iter().enumerate() {
        if (res.len() as u64) < shortest.1 {
            shortest = (index, res.len() as u64);
        }
    }
    shortest.0
}

// #[flame]
// pub fn search_unrolled(persistence: &Persistence, request: Request) -> Result<FnvHashMap<u32, f32>, SearchError> {
//     debug_time!("search_unrolled");

//     if let Some(or) = request.or {

//         let vec:Vec<FnvHashMap<u32, f32>> = or.par_iter().map(|x| -> FnvHashMap<u32, f32> {
//             search_unrolled(persistence, x.clone()).unwrap()
//         }).collect();

//         debug_time!("search_unrolled_collect_ors");
//         Ok(union_hits(vec))
//         // Ok(or.iter().fold(FnvHashMap::default(), |mut acc, x| -> FnvHashMap<u32, f32> {
//         //     acc.extend(&search_unrolled(persistence, x.clone()).unwrap());
//         //     acc
//         // }))

//     } else if let Some(ands) = request.and {
//         let mut and_results: Vec<FnvHashMap<u32, f32>> = ands.par_iter().map(|x| search_unrolled(persistence, x.clone()).unwrap()).collect(); // @Hack  unwrap forward errors

//         debug_time!("and algorithm");
//         Ok(intersect_hits(and_results))
//         // for res in &and_results {
//         //     all_results.extend(res); // merge all results
//         // }

//     } else if request.search.is_some() {
//         Ok(search_raw(persistence, request.search.unwrap(), request.boost)?)
//     } else {
//         Ok(FnvHashMap::default())
//     }
// }

// pub fn union_hits(vec:Vec<FnvHashMap<u32, f32>>) -> FnvHashMap<u32, f32> {
//     vec.iter().fold(FnvHashMap::default(), |mut acc, x| -> FnvHashMap<u32, f32> {
//         acc.extend(x);
//         acc
//     })
// }

use search_field::*;


pub fn union_hits(vec:Vec<SearchFieldResult>) -> SearchFieldResult {

    let mut result = SearchFieldResult::default();

    result.hits = vec.iter().fold(FnvHashMap::default(), |mut acc, x| -> FnvHashMap<u32, f32> {
        acc.extend(&x.hits);
        acc
    });

    result
}

// pub fn intersect_hits(mut and_results:Vec<FnvHashMap<u32, f32>>) -> FnvHashMap<u32, f32> {
//     let mut all_results: FnvHashMap<u32, f32> = FnvHashMap::default();
//     let index_shortest = get_shortest_result(&and_results.iter().map(|el| el.iter()).collect());

//     let shortest_result = and_results.swap_remove(index_shortest);
//     for (k, v) in shortest_result {
//         if and_results.iter().all(|ref x| x.contains_key(&k)) {
//             all_results.insert(k, v);
//         }
//     }
//     all_results
// }

pub fn intersect_hits(mut and_results:Vec<SearchFieldResult>) -> SearchFieldResult {
    let mut all_results: FnvHashMap<u32, f32> = FnvHashMap::default();
    let index_shortest = get_shortest_result(&and_results.iter().map(|el| el.hits.iter()).collect());

    let shortest_result = and_results.swap_remove(index_shortest).hits;
    for (k, v) in shortest_result {
        if and_results.iter().all(|ref x| x.hits.contains_key(&k)) { // if all hits contain this key
            all_results.insert(k, v);
        }
    }
    // all_results
    SearchFieldResult{hits:all_results, ..Default::default()}
}

use expression::ScoreExpression;

#[allow(dead_code)]
#[derive(Debug)]
struct BoostIter {
    // iterHashmap: IterMut<K, V> (&'a K, &'a mut V)
}

#[flame]
pub fn add_boost(persistence: &Persistence, boost: &RequestBoostPart, hits: &mut SearchFieldResult) -> Result<(), SearchError> {
    // let key = util::boost_path(&boost.path);
    let boost_path = boost.path.to_string() + ".boost_valid_to_value";
    let boostkv_store = persistence.get_boost(&boost_path)?;
    // let boostkv_store = persistence.cache.index_id_to_parent.get(&key).expect(&format!("Could not find {:?} in index_id_to_parent cache", key));
    let boost_param = boost.param.unwrap_or(0.0);

    let expre = boost.expression.as_ref().map(|expression| ScoreExpression::new(expression.clone()));
    let default = vec![];
    let skip_when_score = boost.skip_when_score.as_ref().unwrap_or(&default);
    for (value_id, score) in hits.hits.iter_mut() {
        if skip_when_score.len() > 0 && skip_when_score.iter().find(|x| *x == score).is_some() {
            continue;
        }
        // let ref vals_opt = boostkv_store.get(*value_id as usize);
        let ref vals_opt = boostkv_store.get_values(*value_id as u64);
        debug!("Found in boosting for value_id {:?}: {:?}", value_id, vals_opt);
        vals_opt.as_ref().map(|values| {
            if values.len() > 0 {
                let boost_value = values[0]; // @Temporary // @Hack this should not be an array for this case
                match boost.boost_fun {
                    Some(BoostFunction::Log10) => {
                        debug!(
                            "boosting value_id {:?} score {:?} with token_value {:?} boost_value {:?} to {:?}",
                            *value_id,
                            score,
                            boost_value,
                            (boost_value as f32 + boost_param).log10(),
                            *score + (boost_value as f32 + boost_param).log10()
                        );
                        *score += (boost_value as f32 + boost_param).log10(); // @Temporary // @Hack // @Cleanup // @FixMe
                    }
                    Some(BoostFunction::Linear) => {
                        *score *= boost_value as f32 + boost_param; // @Temporary // @Hack // @Cleanup // @FixMe
                    }
                    Some(BoostFunction::Add) => {
                        debug!(
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
                expre.as_ref().map(|exp| {
                    debug!("expression to {:?} with boost_value {:?}", exp.get_score(boost_value as f32), boost_value);
                    *score += exp.get_score(boost_value as f32)
                });
            }
        });
    }
    Ok(())
}


use fnv;

#[derive(Debug)]
pub enum SearchError {
    Io(io::Error),
    StringError(String),
    MetaData(serde_json::Error),
    Utf8Error(std::str::Utf8Error),
    FstError(fst::Error),
    FstLevenShtein(fst_levenshtein::Error),
    CrossBeamError(crossbeam_channel::SendError<std::collections::HashMap<u32, f32, std::hash::BuildHasherDefault<fnv::FnvHasher>>>),
    CrossBeamError2(crossbeam_channel::SendError<SearchFieldResult>),
    CrossBeamErrorReceive(crossbeam_channel::RecvError),
    TooManyStates
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
impl From<fst_levenshtein::Error> for SearchError {
    fn from(err: fst_levenshtein::Error) -> SearchError {
        SearchError::FstLevenShtein(err)
    }
}
impl From<crossbeam_channel::SendError<std::collections::HashMap<u32, f32, std::hash::BuildHasherDefault<fnv::FnvHasher>>>> for SearchError {
    fn from(err: crossbeam_channel::SendError<std::collections::HashMap<u32, f32, std::hash::BuildHasherDefault<fnv::FnvHasher>>>) -> SearchError {
        SearchError::CrossBeamError(err)
    }
}
impl From<crossbeam_channel::SendError<SearchFieldResult>> for SearchError {
    fn from(err: crossbeam_channel::SendError<SearchFieldResult>) -> SearchError {
        SearchError::CrossBeamError2(err)
    }
}
impl From<crossbeam_channel::RecvError> for SearchError {
    fn from(err: crossbeam_channel::RecvError) -> SearchError {
        SearchError::CrossBeamErrorReceive(err)
    }
}

use std::fmt;
pub use std::error::Error;

impl fmt::Display for SearchError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "\n{}", self)?;
        Ok(())
    }
}

impl Error for SearchError {
    fn description(&self) -> &str {
        "self.error.description()"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}


use util::*;


pub fn read_data_single(persistence: &Persistence, id: u32, field: String) -> Result<String, SearchError> {
    let steps = util::get_steps_to_anchor(&field);

    let mut data = vec![id];
    let mut result = json!({});

    for path in steps.iter() {
        result[path.clone()] = json!([]);
        let dat:FnvHashMap<u32, Vec<u32>> = join_for_read(persistence, data, &concat(path, ".parentToValueId"))?;
        data = dat.get(&id).expect(&format!("Could not find id {:?} in  {:?} {:?}", id, path, dat)).clone();
    }

    let texto = get_id_text_map_for_ids(persistence, steps.last().unwrap(), &data);
    println!("{:?}", texto);
    Ok(serde_json::to_string_pretty(&result).unwrap())
    // "".to_string()
}

pub fn read_tree(persistence: &Persistence, id: u32, tree: NodeTree) -> Result<serde_json::Value, SearchError> {
    let mut json = json!({});

    for (prop, sub_tree) in tree.next.iter() {
        if sub_tree.is_leaf {
            let text_value_id_opt = join_for_1_to_1(persistence, id, &concat(&prop, ".parentToValueId"))?;
            if let Some(text_value_id) = text_value_id_opt {
                let texto = get_text_for_id(persistence, &prop, text_value_id);
                json[extract_prop_name(prop)] = json!(texto);
            }
        }else{
            if let Some(sub_ids) = join_for_1_to_n(persistence, id, &concat(&prop, ".parentToValueId"))? {
                let is_flat = sub_tree.next.len()==1 && sub_tree.next.keys().nth(0).unwrap().ends_with("[].textindex");
                if is_flat{
                    let flat_prop = sub_tree.next.keys().nth(0).unwrap();
                    //text_id for value_ids
                    let text_ids:Vec<u32> = sub_ids.iter().flat_map(|id| join_for_1_to_1(persistence, *id, &concat(&flat_prop, ".parentToValueId")).unwrap()).collect();
                    let texto = get_text_for_ids(persistence, flat_prop, &text_ids);
                    json[extract_prop_name(prop)] = json!(texto);
                }else{
                    let is_array =  prop.ends_with("[]");
                    if is_array {
                        let mut sub_data = vec![];
                        for sub_id in sub_ids {
                            sub_data.push(read_tree(persistence, sub_id, sub_tree.clone())?);
                        }
                        json[extract_prop_name(prop)] = json!(sub_data);

                    }else if let Some(sub_id) = sub_ids.get(0) {
                        // println!("KEIN ARRAY {:?}", sub_tree.clone());
                        json[extract_prop_name(prop)] = read_tree(persistence, *sub_id, sub_tree.clone())?;
                    }
                }
            }
        }
    }
    Ok(json)

}



pub fn read_data(persistence: &Persistence, id: u32, fields: Vec<String>) -> Result<String, SearchError> {

    // let all_steps: FnvHashMap<String, Vec<String>> = fields.iter().map(|field| (field.clone(), util::get_steps_to_anchor(&field))).collect();
    let all_steps: Vec<Vec<String>> = fields.iter().map(|field| util::get_steps_to_anchor(&field)).collect();
    println!("{:?}", all_steps);
    // let paths = util::get_steps_to_anchor(&request.path);

    let tree = to_node_tree(all_steps);

    let dat = read_tree(persistence, id, tree)?;
    Ok(serde_json::to_string_pretty(&dat).unwrap())
}


#[flame]
pub fn join_to_parent_with_score(persistence: &Persistence, input: SearchFieldResult, path: &str, trace_time_info: &str) -> Result<SearchFieldResult, SearchError>
{
    let mut total_values = 0;
    let mut hits: FnvHashMap<u32, f32> = FnvHashMap::default();
    let hits_iter = input.hits.into_iter();
    let num_hits = hits_iter.size_hint().1.unwrap_or(0);
    hits.reserve(num_hits);
    let kv_store = persistence.get_valueid_to_parent(path)?;
    // debug_time!("term hits hit to column");
    debug_time!(format!("{:?} {:?}", path, trace_time_info));
    for (term_id, score) in hits_iter {
        let ref values = kv_store.get_values(term_id as u64);
        values.as_ref().map(|values| {
            total_values += values.len();
            hits.reserve(values.len());
            // trace!("value_id: {:?} values: {:?} ", value_id, values);
            for parent_val_id in values {
                // @Temporary
                match hits.entry(*parent_val_id as u32) {
                    Vacant(entry) => {
                        trace!("value_id: {:?} to parent: {:?} score {:?}", term_id, parent_val_id, score);
                        entry.insert(score);
                    }
                    Occupied(entry) => if *entry.get() < score {
                        trace!("value_id: {:?} to parent: {:?} score: {:?}", term_id, parent_val_id, score.max(*entry.get()));
                        *entry.into_mut() = score.max(*entry.get());
                    },
                }
            }
        });
    }
    debug!("{:?} hits hit {:?} distinct ({:?} total ) in column {:?}", num_hits, hits.len(), total_values, path);

    // debug!("{:?} hits in next_level_hits {:?}", next_level_hits.len(), &concat(path_name, ".valueIdToParent"));

    // trace!("next_level_hits from {:?}: {:?}", &concat(path_name, ".valueIdToParent"), hits);
    // debug!("{:?} hits in next_level_hits {:?}", hits.len(), &concat(path_name, ".valueIdToParent"));

    Ok(SearchFieldResult{hits:hits, ..Default::default()})
}

#[flame]
pub fn join_for_read(persistence: &Persistence, input: Vec<u32>, path: &str) -> Result<FnvHashMap<u32, Vec<u32>>, SearchError>
{
    let mut hits: FnvHashMap<u32, Vec<u32>> = FnvHashMap::default();
    let kv_store = persistence.get_valueid_to_parent(path)?;
    // debug_time!("term hits hit to column");
    debug_time!(format!("{:?} ", path));
    for value_id in input {
        let ref values = kv_store.get_values(value_id as u64);
        values.as_ref().map(|values| {
            hits.reserve(values.len());
            hits.insert(value_id, values.clone());
        });
    }
    debug!("hits hit {:?} distinct in column {:?}", hits.len(), path);

    Ok(hits)
}
#[flame]
pub fn join_for_1_to_1(persistence: &Persistence, value_id: u32, path: &str) -> Result<std::option::Option<u32>, SearchError>
{
    let kv_store = persistence.get_valueid_to_parent(path)?;
    Ok(kv_store.get_value(value_id as u64))
}
#[flame]
pub fn join_for_1_to_n(persistence: &Persistence, value_id: u32, path: &str) -> Result<Option<Vec<u32>>, SearchError>
{
    let kv_store = persistence.get_valueid_to_parent(path)?;
    Ok(kv_store.get_values(value_id as u64))
}



// #[flame]
// fn join_to_parent<I>(persistence: &Persistence, input: I, path: &str, trace_time_info: &str) -> FnvHashMap<u32, f32>
//     where
//     I: IntoIterator<Item = (u32, f32)> ,
// {
//     let mut total_values = 0;
//     let mut hits: FnvHashMap<u32, f32> = FnvHashMap::default();
//     let hits_iter = input.into_iter();
//     let num_hits = hits_iter.size_hint().1.unwrap_or(0);
//     hits.reserve(num_hits);
//     let kv_store = persistence.get_valueid_to_parent(&concat(&path, ".valueIdToParent"));
//     // debug_time!("term hits hit to column");
//     debug_time!(format!("{:?} {:?}", path, trace_time_info));
//     for (term_id, score) in hits_iter {
//         let ref values = kv_store.get_values(term_id as u64);
//         values.as_ref().map(|values| {
//             total_values += values.len();
//             hits.reserve(values.len());
//             // trace!("value_id: {:?} values: {:?} ", value_id, values);
//             for parent_val_id in values {
//                 // @Temporary
//                 match hits.entry(*parent_val_id as u32) {
//                     Vacant(entry) => {
//                         trace!("value_id: {:?} to parent: {:?} score {:?}", term_id, parent_val_id, score);
//                         entry.insert(score);
//                     }
//                     Occupied(entry) => if *entry.get() < score {
//                         trace!("value_id: {:?} to parent: {:?} score: {:?}", term_id, parent_val_id, score.max(*entry.get()));
//                         *entry.into_mut() = score.max(*entry.get());
//                     },
//                 }
//             }
//         });
//     }
//     debug!("{:?} hits hit {:?} distinct ({:?} total ) in column {:?}", num_hits, hits.len(), total_values, path);

//     // debug!("{:?} hits in next_level_hits {:?}", next_level_hits.len(), &concat(path_name, ".valueIdToParent"));

//     // trace!("next_level_hits from {:?}: {:?}", &concat(path_name, ".valueIdToParent"), hits);
//     // debug!("{:?} hits in next_level_hits {:?}", hits.len(), &concat(path_name, ".valueIdToParent"));

//     hits
// }


// #[flame]
// pub fn search_raw(
//     persistence: &Persistence, mut request: RequestSearchPart, boost: Option<Vec<RequestBoostPart>>
// ) -> Result<FnvHashMap<u32, f32>, SearchError> {
//     // request.term = util::normalize_text(&request.term);
//     request.terms = request.terms.iter().map(|el| util::normalize_text(el)).collect::<Vec<_>>();
//     debug_time!("search and join to anchor");

//     let step = plan_creator_search_part(request.clone(), boost);

//     let yep = step.get_output();

//     execute_step(step, persistence)?;
//     let hits = yep.recv().unwrap();
//     Ok(hits)
// }

// pub fn test_levenshtein(term:&str, max_distance:u32) -> Result<(Vec<String>), io::Error> {

//     use std::time::SystemTime;

//     let mut f = try!(File::open("de_full_2.txt"));
//     let mut s = String::new();
//     try!(f.read_to_string(&mut s));

//     let now = SystemTime::now();

//     let lines = s.lines();
//     let mut hits = vec![];
//     for line in lines{
//         let distance = distance(term, line);
//         if distance < max_distance {
//             hits.push(line.to_string())
//         }
//     }

//     let ms = match now.elapsed() {
//         Ok(elapsed) => {(elapsed.as_secs() as f64) * 1_000.0 + (elapsed.subsec_nanos() as f64 / 1000_000.0)}
//         Err(_e) => {-1.0}
//     };

//     let lines_checked = s.lines().count() as f64;
//     println!("levenshtein ms: {}", ms);
//     println!("Lines : {}", lines_checked );
//     let ms_per_1000 = ((ms as f64) / lines_checked) * 1000.0;
//     println!("ms per 1000 lookups: {}", ms_per_1000);
//     Ok((hits))

// }
