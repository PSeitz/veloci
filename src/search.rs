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

use search_field;
use persistence::Persistence;
use doc_loader::DocLoader;
use util;
use util::concat;
use fst;

use rayon::prelude::*;
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
        TermOperator::ALL
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
pub fn to_documents(persistence: &Persistence, hits: &Vec<Hit>) -> Vec<DocWithHit> {
    // DocLoader::load(persistence);
    hits.iter()
        .map(|ref hit| {
            let doc = DocLoader::get_doc(persistence, hit.id as usize).unwrap();
            DocWithHit { doc: serde_json::from_str(&doc).unwrap(), hit: *hit.clone() }
        })
        .collect::<Vec<_>>()
}

pub fn to_search_result(persistence: &Persistence, hits: &SearchResult) -> SearchResultWithDoc {
    SearchResultWithDoc {
        data:     to_documents(&persistence, &hits.data),
        num_hits: hits.num_hits,
    }
}


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

pub fn search_query(request: &str, persistence: &Persistence, top: Option<usize>, skip: Option<usize>) -> Request {
    // let req = persistence.meta_data.fulltext_indices.key
    info_time!("generating search query");
    let parts: Vec<Request> = persistence.meta_data.fulltext_indices.keys().map(|field| {
        let field_name:String = field.chars().take(field.chars().count()-10).into_iter().collect();

        let part = RequestSearchPart {
            path: field_name.to_string(),
            terms: vec![request.to_string()],
            levenshtein_distance: Some(1),
            resolve_token_to_parent_hits: Some(true),
            ..Default::default()
        };

        Request {search: Some(part), ..Default::default() }
    }).collect();

    Request {
        or: Some(parts),
        top: top.unwrap_or(10),
        skip: skip.unwrap_or(0),
        ..Default::default()
    }
}

pub fn search(request: Request, persistence: &Persistence) -> Result<SearchResult, SearchError> {
    info_time!("search");
    let skip = request.skip;
    let top = request.top;
    let res = search_unrolled(&persistence, request)?;
    // println!("{:?}", res);
    // let res = hits_to_array_iter(res.iter());
    // let res = hits_to_sorted_array(res);

    let mut search_result = SearchResult { num_hits: 0, data:     vec![] };
    search_result.data = hits_to_sorted_array(res);
    search_result.num_hits = search_result.data.len() as u64;
    search_result.data = apply_top_skip(search_result.data, skip, top);
    Ok(search_result)
}

pub fn get_shortest_result<T: std::iter::ExactSizeIterator>(results: &Vec<T>) -> usize {
    let mut shortest = (0, std::u64::MAX);
    for (index, res) in results.iter().enumerate() {
        if (res.len() as u64) < shortest.1 {
            shortest = (index, res.len() as u64);
        }
    }
    shortest.0
}

pub fn search_unrolled(persistence: &Persistence, request: Request) -> Result<FnvHashMap<u32, f32>, SearchError> {
    debug_time!("search_unrolled");

    if let Some(or) = request.or {

        let vec:Vec<FnvHashMap<u32, f32>> = or.par_iter().map(|x| -> FnvHashMap<u32, f32> {
            search_unrolled(persistence, x.clone()).unwrap()
        }).collect();

        debug_time!("search_unrolled_collect_ors");
        Ok(vec.iter().fold(FnvHashMap::default(), |mut acc, x| -> FnvHashMap<u32, f32> {
            acc.extend(x);
            acc
        }))

        // Ok(or.iter().fold(FnvHashMap::default(), |mut acc, x| -> FnvHashMap<u32, f32> {
        //     acc.extend(&search_unrolled(persistence, x.clone()).unwrap());
        //     acc
        // }))


    } else if let Some(ands) = request.and {
        let mut and_results: Vec<FnvHashMap<u32, f32>> = ands.par_iter().map(|x| search_unrolled(persistence, x.clone()).unwrap()).collect(); // @Hack  unwrap forward errors

        debug_time!("and algorithm");
        let mut all_results: FnvHashMap<u32, f32> = FnvHashMap::default();
        let index_shortest = get_shortest_result(&and_results.iter().map(|el| el.iter()).collect());

        let shortest_result = and_results.swap_remove(index_shortest);
        for (k, v) in shortest_result {
            if and_results.iter().all(|ref x| x.contains_key(&k)) {
                all_results.insert(k, v);
            }
        }
        // for res in &and_results {
        //     all_results.extend(res); // merge all results
        // }

        Ok(all_results)
    } else if request.search.is_some() {
        Ok(search_raw(persistence, request.search.unwrap(), request.boost)?)
    } else {
        Ok(FnvHashMap::default())
    }
}

use expression::ScoreExpression;

#[allow(dead_code)]
#[derive(Debug)]
struct BoostIter {
    // iterHashmap: IterMut<K, V> (&'a K, &'a mut V)
}

pub fn add_boost(persistence: &Persistence, boost: &RequestBoostPart, hits: &mut FnvHashMap<u32, f32>) {
    // let key = util::boost_path(&boost.path);
    let boost_path = boost.path.to_string() + ".boost_valid_to_value";
    let boostkv_store = persistence.get_boost(&boost_path);
    // let boostkv_store = persistence.cache.index_id_to_parent.get(&key).expect(&format!("Could not find {:?} in index_id_to_parent cache", key));
    let boost_param = boost.param.unwrap_or(0.0);

    let expre = boost.expression.as_ref().map(|expression| ScoreExpression::new(expression.clone()));
    let default = vec![];
    let skip_when_score = boost.skip_when_score.as_ref().unwrap_or(&default);
    for (value_id, score) in hits.iter_mut() {
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
}





#[derive(Debug)]
pub enum SearchError {
    Io(io::Error),
    MetaData(serde_json::Error),
    Utf8Error(std::str::Utf8Error),
    FstError(fst::Error),
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

fn check_apply_boost(persistence: &Persistence, boost: &RequestBoostPart, path_name: &str, hits: &mut FnvHashMap<u32, f32>) -> bool {
    let will_apply_boost = boost.path.starts_with(path_name);
    if will_apply_boost {
        info!("will_apply_boost: boost.path {:?} path_name {:?}", boost.path, path_name);
        add_boost(persistence, boost, hits);
    }
    !will_apply_boost
}

pub fn search_raw(
    persistence: &Persistence, mut request: RequestSearchPart, mut boost: Option<Vec<RequestBoostPart>>
) -> Result<FnvHashMap<u32, f32>, SearchError> {
    // request.term = util::normalize_text(&request.term);
    request.terms = request.terms.iter().map(|el| util::normalize_text(el)).collect::<Vec<_>>();
    debug_time!("search and join to anchor");
    let field_result = search_field::get_hits_in_field(persistence, &mut request)?;

    let num_term_hits = field_result.hits.len();
    if num_term_hits == 0 {
        return Ok(FnvHashMap::default());
    };
    let mut next_level_hits: FnvHashMap<u32, f32> = FnvHashMap::default();
    let mut hits: FnvHashMap<u32, f32> = FnvHashMap::default();
    // let mut next_level_hits:Vec<(u32, f32)> = vec![];
    // let mut hits:Vec<(u32, f32)> = vec![];

    let paths = util::get_steps_to_anchor(&request.path);
//    if let Some(last_path) = paths.last_mut() {
//        *last_path = last_path.clone() + ".textindex";
//    }
    // text to "rows"
    // let path_name = util::get_file_path_name(paths.last().unwrap(), true);
    // let key = util::concat_tuple(&path_name, ".valueIdToParent.valIds", ".valueIdToParent.mainIds");
    // let kv_store = persistence.get_valueid_to_parent(&key);
    let kv_store = persistence.get_valueid_to_parent(&concat(&paths.last().unwrap(), ".valueIdToParent"));
    let mut total_values = 0;
    {
        hits.reserve(field_result.hits.len());
        debug_time!("term hits hit to column");
        for (term_id, score) in field_result.hits {
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
    }
    debug!("{:?} term hits hit {:?} distinct ({:?} total ) in column {:?}", num_term_hits, hits.len(), total_values, paths.last().unwrap());


    info!("Joining {:?} hits from {:?} for {:?}", hits.len(), paths, &request.terms);
    for i in (0..paths.len() - 1).rev() {
        // let is_text_index = i == (paths.len() - 1);
        // let path_name = util::get_file_path_name(&paths[i], is_text_index);
        let path_name = &paths[i];

        if boost.is_some() {
            boost.as_mut().unwrap().retain(|boost| check_apply_boost(persistence, boost, &path_name, &mut hits));
        }

        // let key = util::concat_tuple(&path_name, ".valueIdToParent.valIds", ".valueIdToParent.mainIds");
        debug_time!("Joining to anchor");
        let kv_store = persistence.get_valueid_to_parent(&concat(path_name, ".valueIdToParent"));
        // let kv_store = persistence.cache.index_id_to_parent.get(&key).expect(&format!("Could not find {:?} in index_id_to_parent cache", key));
        debug_time!("Adding all values");
        next_level_hits.reserve(hits.len());
        for (value_id, score) in hits.iter() {
            // kv_store.add_values(*value_id, &cache_lock, *score, &mut next_level_hits);
            // let ref values = kv_store[*value_id as usize];
            let ref values = kv_store.get_values(*value_id as u64);
            values.as_ref().map(|values| {
                next_level_hits.reserve(values.len());
                // trace!("value_id: {:?} values: {:?} ", value_id, values);
                for parent_val_id in values {
                    // @Temporary
                    match next_level_hits.entry(*parent_val_id as u32) {
                        Vacant(entry) => {
                            trace!("value_id: {:?} to parent: {:?} score {:?} --new insert", value_id, parent_val_id, score);
                            entry.insert(*score);
                        }
                        Occupied(entry) => if *entry.get() < *score {
                            trace!("value_id: {:?} to parent: {:?} score: {:?} --update", value_id, parent_val_id, score.max(*entry.get()));
                            *entry.into_mut() = *score;
                        },
                    }
                }
                // for parent_val_id in values {    // @Temporary
                //     next_level_hits.place_back() <- (parent_val_id, *score);
                //     // next_level_hits.push((parent_val_id, *score));
                // }
            });



            // for parent_val_id in values {
            //     let hit = next_level_hits.get(parent_val_id as u64);
            //     if  hit.map_or(true, |el| el == f32::NEG_INFINITY) {
            //         next_level_hits.insert(parent_val_id as u64, score);
            //     }else{
            //         next_level_hits.insert(parent_val_id as u64, score);
            //     }
            // }
        }

        // next_level_hits.sort_by(|a, b| a.0.cmp(&b.0));
        trace!("next_level_hits from {:?}: {:?}", &concat(path_name, ".valueIdToParent"), next_level_hits);
        debug!("{:?} hits in next_level_hits {:?}", next_level_hits.len(), &concat(path_name, ".valueIdToParent"));

        // debug_time!("sort and dedup");
        // next_level_hits.sort_by(|a, b| a.0.cmp(&b.0));
        // next_level_hits.dedup_by_key(|i| i.0);
        // hits.clear();
        // debug_time!("insert to next level");
        // hits.reserve(next_level_hits.len());
        // for el in &next_level_hits {
        //     hits.insert(el.0, el.1);
        // }
        // next_level_hits.clear();

        // hits.extend(next_level_hits.iter());
        hits = next_level_hits;
        next_level_hits = FnvHashMap::default();
    }

    if boost.is_some() {
        //remaining boosts
        boost.as_mut().unwrap().retain(|boost| check_apply_boost(persistence, boost, "", &mut hits));
    }

    Ok(hits)
}


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
