use std::io;
use std::io::Write;
use std::fs::File;
use std::{self, str};

use fnv::FnvHashMap;
use fnv::FnvHashSet;
use fst::{self, MapBuilder};
use itertools::Itertools;
use log;
use rayon::prelude::*;
use serde_json::{self, Value};
use serde_json::{Deserializer, StreamDeserializer};
use std::io::BufRead;
use json_converter;
use persistence;
use persistence::{IndexIdToParent, LoadingType, Persistence};
use persistence_data_indirect::*;
use persistence_score::token_to_anchor_score_vint::*;
use search;
use search_field;
use tokenizer::*;
use util::*;
use util::{self, concat};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum CreateIndex {
    FulltextInfo(Fulltext),
    BoostInfo(Boost),
    FacetInfo(FacetIndex),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FacetIndex {
    facet: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Fulltext {
    fulltext: String,
    options: Option<FulltextIndexOptions>,
    loading_type: Option<LoadingType>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Boost {
    boost: String,
    options: BoostIndexOptions,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenValuesConfig {
    path: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct FulltextIndexOptions {
    pub tokenize: bool,
    pub add_normal_values: Option<bool>,
    pub stopwords: Option<FnvHashSet<String>>,
}

impl FulltextIndexOptions {
    #[allow(dead_code)]
    fn new_without_tokenize() -> FulltextIndexOptions {
        FulltextIndexOptions {
            tokenize: true,
            stopwords: None,
            add_normal_values: Some(true),
        }
    }

    fn new_with_tokenize() -> FulltextIndexOptions {
        FulltextIndexOptions {
            tokenize: true,
            stopwords: None,
            add_normal_values: Some(true),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BoostIndexOptions {
    boost_type: String, // type:
}

#[derive(Debug, Default)]
pub struct TermInfo {
    pub id: u32,
    pub num_occurences: u32,
}

impl TermInfo {
    #[inline]
    pub fn new(id: u32) -> TermInfo {
        TermInfo { id: id, num_occurences: 0 }
    }
}

// #[inline]
// pub fn set_ids(terms: &mut TermMap) {
//     let mut v: Vec<_> = terms
//         .keys()
//         .map(|el| el.as_str() as *const str) //#borrow
//         .collect();
//     v.sort_unstable_by_key(|term| unsafe {
//         std::mem::transmute::<*const str, &str>(*term) //#borrow
//     });
//     for (i, term) in v.iter().enumerate() {
//         let term = unsafe {
//             std::mem::transmute::<*const str, &str>(*term) //#borrow this is only done to trick the borrow checker for performance reasons
//         };
//         if let Some(term_info) = terms.get_mut(term) {
//             term_info.id = i as u32;
//         }
//     }
// }
#[inline]
pub fn set_ids(terms: &mut TermMap) {
    let mut v: Vec<_> = terms
        .keys()
        .map(|el| el.as_str() as *const str) //#borrow
        .collect();
    v.sort_unstable_by_key(|term| unsafe {
        std::mem::transmute::<*const str, &str>(*term) //#borrow
    });
    for (i, term) in v.iter().enumerate() {
        let term = unsafe {
            std::mem::transmute::<*const str, &str>(*term) //#borrow this is only done to trick the borrow checker for performance reasons
        };
        if let Some(term_info) = terms.get_mut(term) {
            term_info.id = i as u32;
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ValIdPair {
    pub valid: u32,
    pub parent_val_id: u32,
}

impl ValIdPair {
    #[inline]
    pub fn new(valid: u32, parent_val_id: u32) -> ValIdPair {
        ValIdPair {
            valid: valid,
            parent_val_id: parent_val_id,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ValIdPairToken {
    pub token_or_text_id: u32,
    pub anchor_id: u32,
    pub token_pos: u32,
    pub num_occurences: u32,
    pub num_tokens_in_text: u32,
}

#[derive(Debug, Default, Clone)]
pub struct TokenToAnchorScore {
    pub valid: u32,
    pub anchor_id: u32,
    pub score: u32,
}

pub trait KeyValuePair {
    fn get_key(&self) -> u32;
    fn set_key(&mut self, id: u32);
    fn get_value(&self) -> u32;
    fn set_value(&mut self, id: u32);
}

impl KeyValuePair for ValIdPair {
    #[inline]
    fn get_key(&self) -> u32 {
        self.valid
    }
    #[inline]
    fn set_key(&mut self, id: u32) {
        self.valid = id;
    }
    #[inline]
    fn get_value(&self) -> u32 {
        self.parent_val_id
    }
    #[inline]
    fn set_value(&mut self, id: u32) {
        self.parent_val_id = id;
    }
}
impl KeyValuePair for ValIdPairToken {
    #[inline]
    fn get_key(&self) -> u32 {
        self.token_or_text_id
    }
    #[inline]
    fn set_key(&mut self, id: u32) {
        self.token_or_text_id = id;
    }
    #[inline]
    fn get_value(&self) -> u32 {
        self.anchor_id
    }
    #[inline]
    fn set_value(&mut self, id: u32) {
        self.anchor_id = id;
    }
}
impl KeyValuePair for ValIdToValue {
    #[inline]
    fn get_key(&self) -> u32 {
        self.valid
    }
    #[inline]
    fn set_key(&mut self, id: u32) {
        self.valid = id;
    }
    #[inline]
    fn get_value(&self) -> u32 {
        self.value
    }
    #[inline]
    fn set_value(&mut self, id: u32) {
        self.value = id;
    }
}

/// Used for boost
/// e.g. boost value 5000 for id 5
/// 5 -> 5000
#[derive(Debug, Clone)]
pub struct ValIdToValue {
    pub valid: u32,
    pub value: u32,
}

impl std::fmt::Display for ValIdPair {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "\n{}\t{}", self.valid, self.parent_val_id)?;
        Ok(())
    }
}

// impl<ValIdPair> fmt::Display for Vec<ValIdPair> {
//     fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
//         write!(f, "(a, b)",)
//         Ok(())
//     }
// }

#[allow(dead_code)]
fn print_vec(vec: &Vec<ValIdPair>, valid_header: &str, parentid_header: &str) -> String {
    format!("{}\t{}", valid_header, parentid_header)
        + &vec.iter()
            .map(|el| format!("\n{}\t{}", el.valid, el.parent_val_id))
            .collect::<Vec<_>>()
            .join("")
}

#[allow(dead_code)]
fn print_index_id_to_parent(vec: &IndexIdToMultipleParentIndirect<u32>, valid_header: &str, parentid_header: &str) -> String {
    let keys = vec.get_keys();
    format!("{}\t{}", valid_header, parentid_header)
        + &keys.iter()
            .map(|key| format!("\n{}\t{:?}", key, vec.get_values(*key as u64)))
            .collect::<Vec<_>>()
            .join("")
}

fn store_full_text_info_and_set_ids(
    persistence: &Persistence,
    all_terms: &mut TermMap,
    path: &str,
    options: &FulltextIndexOptions,
    id_lists: &mut FnvHashMap<String, persistence::IDList>,
    fulltext_indices: &mut FnvHashMap<String, FulltextIndexOptions>,
) -> Result<(), io::Error> {
    debug_time!(format!("store_fst strings and string offsets {:?}", path));
    // let mut sorted_terms: Vec<&String> = all_terms.keys().collect::<Vec<&String>>();
    // sorted_terms.sort_unstable();

    let mut sorted_terms: Vec<_> = all_terms
        .keys()
        .map(|el| el.as_str() as *const str) //#borrow
        .collect();
    sorted_terms.sort_unstable_by_key(|term| unsafe {
        std::mem::transmute::<*const str, &str>(*term) //#borrow
    });
    for (i, term) in sorted_terms.iter().enumerate() {
        let term = unsafe {
            std::mem::transmute::<*const str, &str>(*term) //#borrow this is only done to trick the borrow checker for performance reasons
        };
        if let Some(term_info) = all_terms.get_mut(term) {
            term_info.id = i as u32;
        }
    }

    let offsets = get_string_offsets(&sorted_terms); // TODO REPLACE OFFSET STUFF IN search field with something else
    let (id_list_path, id_list) = persistence.write_offset(&persistence::vec_to_bytes_u64(&offsets), &offsets, &concat(path, ".offsets"))?;
    id_lists.insert(id_list_path, id_list);

    store_fst(persistence, sorted_terms, &all_terms, &path).expect("Could not store fst");
    fulltext_indices.insert(path.to_string(), options.clone());
    Ok(())
}

// fn store_fst(persistence: &mut Persistence, all_terms: &TermMap, sorted_terms: Vec<&String>, path: &str) -> Result<(), fst::Error> {
fn store_fst(persistence: &Persistence, sorted_terms: Vec<*const str>, _all_terms: &TermMap, path: &str) -> Result<(), fst::Error> {
    debug_time!(format!("store_fst {:?}", path));
    let wtr = persistence.get_buffered_writer(&concat(path, ".fst"))?;
    // Create a builder that can be used to insert new key-value pairs.
    let mut build = MapBuilder::new(wtr)?;

    for (term_id, term) in sorted_terms.iter().enumerate() {
        build
            .insert(unsafe { std::mem::transmute::<*const str, &str>(*term) }, term_id as u64)
            .expect("could not insert into fst");
    }

    // for (term_id, term) in sorted_terms.iter().enumerate() {
    //     let term_info = all_terms.get(unsafe { std::mem::transmute::<*const str, &str>(*term) }).unwrap();
    //     let term_id_and_num_occurences: u64 = term_id as u64 | ((term_info.num_occurences as u64) << 32);
    //     build
    //         .insert(unsafe { std::mem::transmute::<*const str, &str>(*term) }, term_id_and_num_occurences)
    //         .expect("could not insert into fst");
    // }


    build.finish()?;

    Ok(())
}

use term_hashmap;

type TermMap = term_hashmap::HashMap<TermInfo>;

#[inline]
fn add_count_text(terms: &mut TermMap, text: &str) {

    let stat = terms.entry(text).or_insert_with(||TermInfo::default());
    stat.num_occurences += 1;

    // if !terms.contains_key(text) {
    //     terms.insert(text.to_string(), TermInfo::default());
    // }
    // let stat = terms.get_mut(text).unwrap();
    // stat.num_occurences += 1;
}

#[inline]
fn add_text<T: Tokenizer>(text: &str, terms: &mut TermMap, options: &FulltextIndexOptions, tokenizer: &T) {
    trace!("text: {:?}", text);
    if options.stopwords.as_ref().map(|el| el.contains(text)).unwrap_or(false) {
        return;
    }

    add_count_text(terms, text);

    //Add lowercase version for search
    // {
    //     let stat = terms.entry(text.to_lowercase().trim().to_string()).or_insert(TermInfo::default());
    //     stat.num_occurences += 1;
    // }

    if options.tokenize && tokenizer.has_tokens(&text) {
        tokenizer.get_tokens(&text, &mut |token: &str, _is_seperator: bool| {
            if options.stopwords.as_ref().map(|el| el.contains(token)).unwrap_or(false) {
                return;
            }
            add_count_text(terms, token);
            // //Add lowercase version for non seperators
            // if !is_seperator{
            //     let stat = terms.entry(token_str.to_lowercase().trim().to_string()).or_insert(TermInfo::default());
            //     stat.num_occurences += 1;
            // }
        });
    }
}


#[inline]
// *mut FnvHashMap here or the borrow checker will complain, because the return apparently expands the scope of the mutable ownership to the complete function(?)
fn get_or_insert_prefer_get<'a, T, F>(map: *mut FnvHashMap<String, T>, key: &str, constructor: &F) -> &'a mut T
where
    F: Fn() -> T,
{
    unsafe
    {
        if let Some(e) = (*map).get_mut(key) {
            return e;
        }

        (*map).insert(key.to_string(), constructor());
        (*map).get_mut(key).unwrap()
    }
}

fn calculate_token_score_in_doc(tokens_to_anchor_id: &mut Vec<ValIdPairToken>) -> Vec<TokenToAnchorScore> {
    // TokenToAnchorScore {
    //     pub valid: u32,
    //     pub anchor_id: u32,
    //     pub score: u32
    // }

    // Sort by anchor, tokenid
    tokens_to_anchor_id.sort_unstable_by(|a, b| {
        let sort_anch = a.anchor_id.cmp(&b.anchor_id);
        if sort_anch == std::cmp::Ordering::Equal {
            let sort_valid = a.token_or_text_id.cmp(&b.token_or_text_id);
            if sort_valid == std::cmp::Ordering::Equal {
                a.token_pos.cmp(&b.token_pos)
            } else {
                sort_valid
            }
        } else {
            sort_anch
        }
    }); // sort by parent id

    // Sort by anchor, tokenid
    // tokens_to_anchor_id.sort_by(|a, b| {
    //     let sort_anch = a.anchor_id.cmp(&b.anchor_id);
    //     if sort_anch == std::cmp::Ordering::Equal {
    //         a.token_or_text_id.cmp(&b.token_or_text_id)
    //     } else {
    //         sort_anch
    //     }
    // }); // sort by parent id

    let mut dat = Vec::with_capacity(tokens_to_anchor_id.len());
    for (_, mut group) in &tokens_to_anchor_id.into_iter().group_by(|el| (el.anchor_id, el.token_or_text_id)) {
        let first = group.next().unwrap();
        let best_pos = first.token_pos;
        let num_occurences = first.num_occurences;
        // let mut avg_pos = best_pos;
        // let mut num_occurences_in_doc = 1;

        let mut is_exact = first.num_tokens_in_text == 1 && first.token_pos == 0;

        // let mut exact_match_boost = 1;
        // if first.num_tokens_in_text == 1 && first.token_pos == 0 {
        //     exact_match_boost = 2
        // }

        // for el in group {
        //     num_occurences = el.num_occurences;
        //     // avg_pos = avg_pos + (el.token_pos - avg_pos) / num_occurences_in_doc;
        //     // num_occurences_in_doc += 1;
        // }

        // let mut score = ((20 / (best_pos + 2)) + num_occurences_in_doc.log10() ) / first.num_occurences;
        // let mut score = 2000 / (best_pos + 10);
        // score *= exact_match_boost;
        // score = (score as f32 / (num_occurences as f32 + 10.).log10()) as u32; //+10 so 1 is bigger than 1


        let score = calculate_token_score_for_entry(best_pos, num_occurences, is_exact);

        // trace!("best_pos {:?}",best_pos);
        // trace!("num_occurences_in_doc {:?}",num_occurences_in_doc);
        // trace!("first.num_occurences {:?}",first.num_occurences);
        // trace!("scorescore {:?}",score);

        dat.push(TokenToAnchorScore {
            valid: first.token_or_text_id,
            anchor_id: first.anchor_id,
            score,
        });
    }

    dat
}

fn calculate_token_score_for_entry(token_best_pos: u32, num_occurences: u32, is_exact: bool) -> u32{
    let mut score = if is_exact {
        400
    }else{
        2000 / (token_best_pos + 10)
    };
    score = (score as f32 / (num_occurences as f32 + 10.).log10()) as u32; //+10 so log() is bigger than 1
    score
}

#[derive(Debug, Default)]
pub struct CreateCache {
    term_data: AllTermsAndDocumentBuilder,
}

#[derive(Debug, Default)]
pub struct AllTermsAndDocumentBuilder {
    offsets: Vec<u64>,
    current_offset: usize,
    id_holder: json_converter::IDHolder,
    terms_in_path: FnvHashMap<String, TermMap>,
}

pub fn get_allterms_per_path<I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>>(
    stream: I,
    // persistence: &mut Persistence,
    fulltext_info_for_path: &FnvHashMap<String, Fulltext>,
    data: &mut AllTermsAndDocumentBuilder,
) -> Result<(), io::Error> {
    info_time!("get_allterms_per_path");

    let mut opt = json_converter::ForEachOpt {};

    let tokenizer = SimpleTokenizerCharsIterateGroupTokens {};
    let default_fulltext_options = FulltextIndexOptions::new_with_tokenize();

    let mut id_holder = json_converter::IDHolder::new();
    {
        let mut cb_text = |_anchor_id: u32, value: &str, path: &str, _parent_val_id: u32| {
            let options: &FulltextIndexOptions = fulltext_info_for_path
                .get(path)
                .and_then(|el| el.options.as_ref())
                .unwrap_or(&default_fulltext_options);

            let mut terms = get_or_insert_prefer_get(&mut data.terms_in_path as *mut FnvHashMap<_, _>, path, &|| TermMap::default());

            add_text(value, &mut terms, &options, &tokenizer);
        };
        let mut callback_ids = |_anchor_id: u32, _path: &str, _value_id: u32, _parent_val_id: u32| {};

        json_converter::for_each_element(stream, &mut id_holder, &mut opt, &mut cb_text, &mut callback_ids);
    }

    std::mem::swap(&mut data.id_holder, &mut id_holder);

    Ok(())
}

#[derive(Debug, Default, Clone)]
struct PathData {
    tokens_to_text_id: Vec<ValIdPair>,
    // the_terms: Vec<(String, usize)>,
    tokens_to_anchor_id: Vec<ValIdPairToken>,
    token_to_anchor_id_score: Vec<TokenToAnchorScore>,
    text_id_to_token_ids: IndexIdToMultipleParentIndirect<u32>,
    text_id_to_parent: Vec<ValIdPair>,
    text_id_to_anchor: Vec<ValIdPair>,
    anchor_to_text_id: Option<Vec<ValIdPair>>,
    boost: Option<Vec<ValIdToValue>>,
    max_valid: u32,
    max_parentid: u32,
}

// #[allow(dead_code)]
// fn check_similarity(data: &FnvHashMap<String, TermMap>) {
//     let mut map: FnvHashMap<String, FnvHashMap<String, (f32, f32)>> = FnvHashMap::default();

//     info_time!("check_similarity");
//     for (path, terms) in data {
//         let num_terms = terms.len();
//         for (path_comp, terms_comp) in data.iter().filter(|&(path_comp, _)| path_comp != path) {
//             let num_similar = terms.keys().filter(|term| terms_comp.contains_key(term.as_str())).count();
//             let similiarity = num_similar as f32 / num_terms as f32;
//             //println!("Similiarity {:?} {:?} {:?}", path, path_comp, num_similar as f32 / num_terms as f32);
//             if map.contains_key(path_comp) {
//                 let aha = map.get_mut(path_comp).unwrap().get_mut(path).unwrap();
//                 aha.1 = similiarity;
//             // map.get_mut(path_comp).1 = num_similar as f32 / num_terms as f32
//             } else {
//                 let entry = map.entry(path.to_string()).or_insert(FnvHashMap::default());
//                 entry.insert(path_comp.to_string(), (similiarity, 0.));
//             }
//         }
//     }

//     for (path, sub) in map {
//         for (path2, data) in sub {
//             if data.0 > 0.1 {
//                 println!("{} {} {} {}", path, path2, data.0, data.1);
//             }
//         }
//     }
// }

fn replace_term_ids<T: KeyValuePair>(yep: &mut Vec<T>, index: &Vec<u32>) {
    for el in yep.iter_mut() {
        let val_id = el.get_key() as usize;
        el.set_key(index[val_id]);
    }
}

#[test]
fn replace_term_ids_test() {
    let mut yep = vec![];
    yep.push(ValIdPair::new(1 as u32, 2 as u32));
    replace_term_ids(&mut yep, &vec![10, 10]);
    assert_eq!(yep, vec![ValIdPair::new(10 as u32, 2 as u32)]);
}

fn is_1_to_n(path: &str) -> bool {
    path.contains("[]")
}

fn parse_json_and_prepare_indices<I>(
    stream1: I,
    _persistence: &Persistence,
    fulltext_info_for_path: &FnvHashMap<String, Fulltext>,
    boost_info_for_path: &FnvHashMap<String, Boost>,
    facet_index: &FnvHashSet<String>,
    create_cache: &mut CreateCache,
) -> Result<(FnvHashMap<String, PathData>, FnvHashMap<String, Vec<ValIdPair>>), io::Error>
where
    I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>,
{
    let mut path_data: FnvHashMap<String, PathData> = FnvHashMap::default();

    let mut id_holder = json_converter::IDHolder::new();
    let mut tuples_to_parent_in_path: FnvHashMap<String, Vec<ValIdPair>> = FnvHashMap::default();

    let default_fulltext_options = FulltextIndexOptions::new_with_tokenize();

    let tokenizer = SimpleTokenizerCharsIterateGroupTokens {};

    {
        info_time!("build path data");
        let mut cb_text = |anchor_id: u32, value: &str, path: &str, parent_val_id: u32| {
            let data = get_or_insert_prefer_get(&mut path_data as *mut FnvHashMap<_, _>, path, &|| {
                let boost_info_data = if boost_info_for_path.contains_key(path) { Some(vec![]) } else { None };
                let anchor_to_text_id = if facet_index.contains(path) && is_1_to_n(path) { Some(vec![]) } else { None }; //Create facet index only for 1:N
                PathData {
                    anchor_to_text_id: anchor_to_text_id,
                    boost: boost_info_data,
                    ..Default::default()
                }
            });

            let all_terms = create_cache.term_data.terms_in_path.get(path).unwrap();
            let options: &FulltextIndexOptions = fulltext_info_for_path
                .get(path)
                .and_then(|el| el.options.as_ref())
                .unwrap_or(&default_fulltext_options);

            if options.stopwords.as_ref().map(|el| el.contains(value)).unwrap_or(false) {
                return;
            }

            // let fst = persistence.get_fst(path).unwrap();
            let text_info = all_terms.get(value).expect("did not found term");

            data.text_id_to_parent.push(ValIdPair::new(text_info.id as u32, parent_val_id));
            data.text_id_to_anchor.push(ValIdPair::new(text_info.id as u32, anchor_id));
            data.anchor_to_text_id
                .as_mut()
                .map(|el| el.push(ValIdPair::new(anchor_id, text_info.id as u32)));
            data.boost.as_mut().map(|el| {
                // if options.boost_type == "int" {
                let my_int = value.parse::<u32>().expect(&format!("Expected an int value but got {:?}", value));
                el.push(ValIdToValue {
                    valid: parent_val_id,
                    value: my_int,
                });
                // } // TODO More cases
            });
            trace!("Found id {:?} for {:?}", text_info, value);

            // tokens_to_anchor_id.push(ValIdPairToken {
            //     token_or_text_id: text_info.id as u32,
            //     num_occurences: text_info.num_occurences as u32,
            //     anchor_id: anchor_id,
            //     token_pos: 0,
            //     num_tokens_in_text: 1,
            // });

            let score = calculate_token_score_for_entry(0, text_info.num_occurences, true);
            data.token_to_anchor_id_score.push(TokenToAnchorScore {
                valid: text_info.id,
                anchor_id: anchor_id,
                score,
            });

            // let temp_id = data.the_terms.len();
            // data.the_terms.push((value.to_string(), temp_id));
            if options.tokenize && tokenizer.has_tokens(value) {
                let mut current_token_pos = 0;
                let mut tokens_ids = Vec::with_capacity(5);
                let mut tokens_to_anchor_id = Vec::with_capacity(10);

                tokenizer.get_tokens(value, &mut |token: &str, _is_seperator: bool| {
                    if options.stopwords.as_ref().map(|el| el.contains(token)).unwrap_or(false) {
                        return; //TODO FIXEME return here also prevents proper recreation of text with tokens
                    }

                    // let temp_id = data.the_terms.len();
                    // data.the_terms.push((token.to_string(), temp_id));

                    let token_info = all_terms.get(token).expect("did not found token");
                    trace!("Adding to tokens_ids {:?} : {:?}", token, token_info);

                    // data.text_id_to_token_ids.push(ValIdPair::new(text_info.id as u32, token_info.id as u32));
                    tokens_ids.push(token_info.id as u32);
                    data.tokens_to_text_id.push(ValIdPair::new(token_info.id as u32, text_info.id as u32));
                    tokens_to_anchor_id.push(ValIdPairToken {
                        token_or_text_id: token_info.id as u32,
                        num_occurences: token_info.num_occurences as u32,
                        anchor_id: anchor_id,
                        token_pos: current_token_pos as u32,
                        num_tokens_in_text: 0,
                    });
                    current_token_pos += 1;
                });

                //add num tokens info
                for mut el in tokens_to_anchor_id.iter_mut() {
                    el.num_tokens_in_text = current_token_pos;
                }

                if data.text_id_to_token_ids.get_values(text_info.id as u64).is_none() {
                    // store relation value_id -> text_ids only once
                    trace!("Adding for {:?} {:?} token_ids {:?}", value, text_info.id, tokens_ids);
                    data.text_id_to_token_ids.set(text_info.id, tokens_ids);
                }
                let token_to_anchor_id_scores = calculate_token_score_in_doc(&mut tokens_to_anchor_id);
                data.token_to_anchor_id_score.extend(token_to_anchor_id_scores);
            }
            // data.tokens_to_anchor_id.extend(tokens_to_anchor_id);

            // let token_to_anchor_id_scores = calculate_token_score_in_doc(&mut tokens_to_anchor_id);
            // data.token_to_anchor_id_score.extend(token_to_anchor_id_scores);
        };

        let mut callback_ids = |_anchor_id: u32, path: &str, value_id: u32, parent_val_id: u32| {
            let tuples = get_or_insert_prefer_get(&mut tuples_to_parent_in_path as *mut FnvHashMap<_, _>, path, &|| vec![]);

            tuples.push(ValIdPair::new(value_id, parent_val_id));
        };

        json_converter::for_each_element(stream1, &mut id_holder, &mut json_converter::ForEachOpt {}, &mut cb_text, &mut callback_ids);

        // json_converter::for_each_element(stream2, &mut id_holder, &mut opt, &mut cb_text, &mut callback_ids);
    }

    std::mem::swap(&mut create_cache.term_data.id_holder, &mut id_holder);


    for (_key, data) in path_data.iter_mut() {
        data.tokens_to_text_id.shrink_to_fit();
        data.tokens_to_anchor_id.shrink_to_fit();
        data.token_to_anchor_id_score.shrink_to_fit();
        data.text_id_to_parent.shrink_to_fit();
        data.text_id_to_anchor.shrink_to_fit();

        data.text_id_to_token_ids.start_pos.shrink_to_fit();
        data.text_id_to_token_ids.data.shrink_to_fit();

        if let Some(ref mut anchor_to_text_id) = data.anchor_to_text_id {
            anchor_to_text_id.shrink_to_fit();
        }

        if let Some(ref mut tuples) = data.boost {
            tuples.shrink_to_fit();
        }
    }

    for (_key, data) in tuples_to_parent_in_path.iter_mut() {
        data.shrink_to_fit();
    }

    Ok((path_data, tuples_to_parent_in_path))
}

fn write_docs<K, S: AsRef<str>>(persistence: &mut Persistence, create_cache: &mut CreateCache, stream3: K) -> Result<(), CreateError>
where
    K: Iterator<Item = S>,
{
    info_time!("write_docs");
    let mut file_out = persistence.get_buffered_writer("data")?;
    let mut offsets = vec![];
    let mut current_offset = create_cache.term_data.current_offset;
    for doc in stream3 {
        file_out.write_all(&doc.as_ref().as_bytes()).unwrap();
        file_out.write_all(b"\n").unwrap();
        offsets.push(current_offset as u64);
        current_offset += doc.as_ref().len();
        current_offset += 1;
    }
    offsets.push(current_offset as u64);
    create_cache.term_data.offsets.extend(offsets);
    create_cache.term_data.current_offset = current_offset;
    let (id_list_path, id_list_meta_data) = persistence.write_offset(
        &persistence::vec_to_bytes_u64(&create_cache.term_data.offsets),
        &create_cache.term_data.offsets,
        &"data.offsets",
    )?;
    persistence.meta_data.id_lists.insert(id_list_path, id_list_meta_data);
    Ok(())
}

fn trace_indices(path_data: &FnvHashMap<String, PathData>) {
    for (path, data) in path_data {
        let path = &path;
        trace!("{}\n{}", &concat(path, ".tokens"), print_vec(&data.tokens_to_text_id, "token_id", "parent_id"));

        trace!(
            "{}\n{}",
            &concat(path, ".text_id_to_token_ids"),
            print_index_id_to_parent(&data.text_id_to_token_ids, "value_id", "token_id")
        );

        trace!(
            "{}\n{}",
            &concat(path, ".valueIdToParent"),
            print_vec(&data.text_id_to_parent, &path, "parentid")
        );

        trace!(
            "{}\n{}",
            &concat(path, ".text_id_to_anchor"),
            print_vec(&data.text_id_to_anchor, "anchor_id", "anchor_id")
        );
    }
}

use persistence_data::*;

fn add_index(path: String,
    tuples: &mut Vec<ValIdPair>,
    is_always_1_to_1: bool,
    sort_and_dedup: bool,
    indices: &mut IndicesFromRawData,
    loading_type: LoadingType,)
{
    if is_always_1_to_1 {
        let store = valid_pair_to_direct_index(tuples);
        indices.direct_indices.push((path, store, loading_type));
    } else {
        let store = valid_pair_to_indirect_index(tuples, sort_and_dedup);
        indices.indirect_indices.push((path, store, loading_type));
    }
}

#[derive(Debug, Default)]
struct IndicesFromRawData {
    direct_indices: Vec<(String, IndexIdToOneParent<u32>, LoadingType)>,
    indirect_indices: Vec<(String, IndexIdToMultipleParentIndirect<u32>, LoadingType)>,
    boost_indices: Vec<(String, IndexIdToOneParent<u32>)>,
    anchor_score_indices: Vec<(String, TokenToAnchorScoreVintIM)>,
}

fn free_vec<T>(vecco: &mut Vec<T>) {
    vecco.clear();
    vecco.shrink_to_fit();
}

fn convert_raw_path_data_to_indices(
    path_data: &mut FnvHashMap<String, PathData>,
    tuples_to_parent_in_path: &mut FnvHashMap<String, Vec<ValIdPair>>,
    facet_index: &FnvHashSet<String> ) -> IndicesFromRawData
{
    info_time!("convert_raw_path_data_to_indices");
    let mut indices = IndicesFromRawData::default();
    let is_text_id_to_parent = |path: &str| path.ends_with(".textindex");

    let indices_vec: Vec<_> = path_data
        .into_par_iter()
        .map(|(path, data)| {
            let mut indices = IndicesFromRawData::default();

            let path = &path;
            add_index(concat(path, ".tokens_to_text_id"), &mut data.tokens_to_text_id, false, true, &mut indices, LoadingType::Disk);
            free_vec(&mut data.tokens_to_text_id);

            let mut token_to_anchor_id_score_vint_index = TokenToAnchorScoreVintIM::default();
            // let mut token_to_anchor_id_scores = calculate_token_score_in_doc(&mut data.tokens_to_anchor_id);
            // token_to_anchor_id_scores.sort_unstable_by_key(|a| a.valid);
            // for (token_id, mut group) in &token_to_anchor_id_scores.into_iter().group_by(|el| (el.valid)) {
            //     let mut group:Vec<TokenToAnchorScore> = group.collect();
            //     group.sort_unstable_by_key(|a| a.anchor_id);
            //     let mut groupo: Vec<u32> = vec![];
            //     for el in group {
            //         groupo.push(el.anchor_id);
            //         groupo.push(el.score);
            //     }
            //     token_to_anchor_id_score_vint_index.set_scores(token_id, groupo);
            // }
            data.token_to_anchor_id_score.sort_unstable_by_key(|a| a.valid);
            let mut groupo: Vec<u32> = vec![];
            for (token_id, mut group) in &data.token_to_anchor_id_score.iter().group_by(|el| el.valid) {
                let mut group:Vec<&TokenToAnchorScore> = group.collect();
                group.sort_unstable_by_key(|a| a.anchor_id);
                group.dedup_by_key(|a| a.anchor_id);
                groupo.clear();
                for el in group {
                    groupo.push(el.anchor_id);
                    groupo.push(el.score);
                }
                token_to_anchor_id_score_vint_index.set_scores(token_id, &mut groupo);
            }
            free_vec(&mut data.token_to_anchor_id_score);
            indices.anchor_score_indices.push((concat(path, ".to_anchor_id_score"), token_to_anchor_id_score_vint_index));

            let sort_and_dedup = false;
            let mut index = IndexIdToMultipleParentIndirect::default();
            std::mem::swap(&mut index, &mut data.text_id_to_token_ids);
            indices.indirect_indices.push((concat(path, ".text_id_to_token_ids"), index, LoadingType::Disk));

            let is_alway_1_to_1 = !is_text_id_to_parent(path); // valueIdToParent relation is always 1 to 1, expect for text_ids, which can have multiple parents

            add_index(concat(path, ".valueIdToParent"), &mut data.text_id_to_parent, is_alway_1_to_1, sort_and_dedup, &mut indices, LoadingType::Disk);
            //Flip values
            for el in data.text_id_to_parent.iter_mut() {
                std::mem::swap(&mut el.parent_val_id, &mut el.valid);
            }

            let loading_type = if facet_index.contains(&path.to_string()) && !is_1_to_n(path) {
                LoadingType::InMemoryUnCompressed
            } else {
                LoadingType::Disk
            };

            add_index(concat(path, ".parentToValueId"), &mut data.text_id_to_parent, !is_1_to_n(path), sort_and_dedup, &mut indices, loading_type);
            free_vec(&mut data.text_id_to_parent);
            add_index(concat(path, ".text_id_to_anchor"), &mut data.text_id_to_anchor, false, sort_and_dedup, &mut indices, LoadingType::Disk);
            free_vec(&mut data.text_id_to_anchor);

            if let Some(ref mut anchor_to_text_id) = data.anchor_to_text_id {
                add_index(concat(path, ".anchor_to_text_id"), anchor_to_text_id, false, sort_and_dedup, &mut indices, LoadingType::InMemoryUnCompressed);
                free_vec(anchor_to_text_id);
            }

            if let Some(ref mut tuples) = data.boost {
                let store = valid_pair_to_direct_index(tuples);
                indices.boost_indices.push((concat(&extract_field_name(path), ".boost_valid_to_value"), store));
                free_vec(tuples);
            }

            indices
    }).collect();

    for mut indice in indices_vec{
        indices.direct_indices.append(&mut indice.direct_indices);
        indices.indirect_indices.append(&mut indice.indirect_indices);
        indices.boost_indices.append(&mut indice.boost_indices);
        indices.anchor_score_indices.append(&mut indice.anchor_score_indices);
    }

    let indices_vec_2: Vec<_> = tuples_to_parent_in_path
        .into_par_iter()
        .map(|(path, mut data)| {
            let mut indices = IndicesFromRawData::default();

            let is_alway_1_to_1 = !is_text_id_to_parent(path);

            let path = &path;
            add_index(concat(path, ".valueIdToParent"), &mut data, is_alway_1_to_1, false, &mut indices, LoadingType::Disk);
            //Flip values
            for el in data.iter_mut() {
                std::mem::swap(&mut el.parent_val_id, &mut el.valid);
            }
            add_index(concat(path, ".parentToValueId"), &mut data, !is_1_to_n(path), false, &mut indices, LoadingType::Disk);
            free_vec(&mut data);

            indices
    }).collect();

    for mut indice in indices_vec_2{
        indices.direct_indices.append(&mut indice.direct_indices);
        indices.indirect_indices.append(&mut indice.indirect_indices);
        indices.boost_indices.append(&mut indice.boost_indices);
        indices.anchor_score_indices.append(&mut indice.anchor_score_indices);
    }

    indices

}

pub fn create_fulltext_index<'a, I, J, K, S: AsRef<str>>(
    stream1: I,
    stream2: J,
    stream3: K,
    mut persistence: &mut Persistence,
    indices_json: &Vec<CreateIndex>,
    create_cache: &mut CreateCache,
    load_persistence: bool,
) -> Result<(), io::Error>
where
    I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>,
    J: Iterator<Item = Result<serde_json::Value, serde_json::Error>>,
    K: Iterator<Item = S>,
{
    let fulltext_info_for_path: FnvHashMap<String, Fulltext> = indices_json
        .iter()
        .flat_map(|index| match index {
            &CreateIndex::FulltextInfo(ref fulltext_info) => Some((fulltext_info.fulltext.to_string() + ".textindex", (*fulltext_info).clone())),
            _ => None,
        })
        .collect();

    let boost_info_for_path: FnvHashMap<String, Boost> = indices_json
        .iter()
        .flat_map(|index| match index {
            &CreateIndex::BoostInfo(ref boost_info) => Some((boost_info.boost.to_string() + ".textindex", (*boost_info).clone())),
            _ => None,
        })
        .collect();

    let facet_index: FnvHashSet<String> = indices_json
        .iter()
        .flat_map(|index| match index {
            &CreateIndex::FacetInfo(ref el) => Some(el.facet.to_string() + ".textindex"),
            _ => None,
        })
        .collect();

    write_docs(&mut persistence, create_cache, stream3);
    get_allterms_per_path(stream1, &fulltext_info_for_path, &mut create_cache.term_data)?;

    let default_fulltext_options = FulltextIndexOptions::new_with_tokenize();
    {
        info_time!("set term ids and write fst");
        let reso: Result<Vec<_>, io::Error> = create_cache
            .term_data
            .terms_in_path
            .par_iter_mut()
            .map(|(path, mut terms)| {
                let mut id_lists = FnvHashMap::default();
                let mut fulltext_indices = FnvHashMap::default();
                let options: &FulltextIndexOptions = fulltext_info_for_path
                    .get(path)
                    .and_then(|el| el.options.as_ref())
                    .unwrap_or(&default_fulltext_options);
                store_full_text_info_and_set_ids(&persistence, &mut terms, &path, &options, &mut id_lists, &mut fulltext_indices)?;
                Ok((id_lists, fulltext_indices))
            })
            .collect();
        for (id_lists, fulltext_indices) in reso? {
            persistence.meta_data.id_lists.extend(id_lists);
            persistence.meta_data.fulltext_indices.extend(fulltext_indices);
        }
        persistence.load_all_fst();
    }

    // check_similarity(&data.terms_in_path);
    info_time!("create and (write) fulltext_index");
    trace!("all_terms {:?}", create_cache.term_data.terms_in_path);

    let (mut path_data, mut tuples_to_parent_in_path) =
        parse_json_and_prepare_indices(stream2, &persistence, &fulltext_info_for_path, &boost_info_for_path, &facet_index, create_cache)?;

    if log_enabled!(log::Level::Trace) {
        trace_indices(&path_data)
    }

    let indices = convert_raw_path_data_to_indices(&mut path_data, &mut tuples_to_parent_in_path, &facet_index);
    if persistence.persistence_type == persistence::PersistenceType::Persistent {
        info_time!("write indices");
        let mut key_value_stores = vec![];
        let mut anchor_score_stores = vec![];
        let mut boost_stores = vec![];

        for ind_index in indices.indirect_indices.iter() {
            key_value_stores.push(persistence.write_indirect_index(&ind_index.1, &ind_index.0, ind_index.2.clone())?);
        }
        for direct_index in indices.direct_indices.iter() {
            key_value_stores.push(persistence.write_direct_index(&direct_index.1, direct_index.0.to_string(), direct_index.2.clone())?);
        }
        for direct_index in indices.anchor_score_indices.iter() {
            anchor_score_stores.push(persistence.write_score_index_vint(&direct_index.1, &direct_index.0, LoadingType::Disk)?);
        }
        for direct_index in indices.boost_indices.iter() {
            boost_stores.push(persistence.write_direct_index(&direct_index.1, &direct_index.0, LoadingType::Disk)?);
        }
        persistence.meta_data.key_value_stores.extend(key_value_stores);
        persistence.meta_data.anchor_score_stores.extend(anchor_score_stores);
        persistence.meta_data.boost_stores.extend(boost_stores);
    }

    // load the converted indices, without writing them
    if load_persistence {
        // persistence.load_from_disk();

        persistence.load_all_id_lists();

        for index in indices.indirect_indices {
            persistence.indices.key_value_stores.insert(index.0, Box::new(index.1));
        }
        for index in indices.direct_indices {
            persistence.indices.key_value_stores.insert(index.0, Box::new(index.1));
        }
        for index in indices.anchor_score_indices {
            persistence.indices.token_to_anchor_to_score.insert(index.0, Box::new(index.1));
        }
        for index in indices.boost_indices {
            persistence.indices.boost_valueid_to_value.insert(index.0, Box::new(index.1));
        }
    }

    //TEST FST AS ID MAPPER
    // let mut all_ids_as_str: TermMap = FnvHashMap::default();
    // for pair in &tuples {
    //     let padding = 10;
    //     all_ids_as_str.insert(format!("{:0padding$}", pair.valid, padding = padding), TermInfo::new(pair.parent_val_id)); // COMPRESSION 50-90%
    // }
    // store_fst(persistence, &all_ids_as_str, &concat(&path_name, ".valueIdToParent.fst")).expect("Could not store fst");
    //TEST FST AS ID MAPPER

    Ok(())
}

fn get_string_offsets(terms: &Vec<*const str>) -> Vec<u64> {
    let mut offsets = vec![];
    let mut offset = 0;
    for term in terms {
        offsets.push(offset as u64);
        offset += unsafe { std::mem::transmute::<*const str, &str>(*term) }.len() + 1; // 1 for linevreak
    }
    offsets.push(offset as u64);
    offsets
}
#[derive(Debug, Clone)]
struct CharData {
    suffix: String,
    line_num: u64,
    byte_offset_start: u64,
}

impl PartialEq for CharData {
    fn eq(&self, other: &CharData) -> bool {
        self.suffix == other.suffix
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct TokenValueData {
    text: String,
    value: Option<u32>,
}

pub fn add_token_values_to_tokens(persistence: &mut Persistence, data_str: &str, config: &str) -> Result<(), search::SearchError> {
    let data: Vec<TokenValueData> = serde_json::from_str(data_str).unwrap();
    let config: TokenValuesConfig = serde_json::from_str(config).unwrap();

    let mut options: search::RequestSearchPart = search::RequestSearchPart {
        path: config.path.clone(),
        levenshtein_distance: Some(0),
        resolve_token_to_parent_hits: Some(false),

        ..Default::default()
    };

    let is_text_index = true;
    let path_name = util::get_file_path_name(&config.path, is_text_index);
    let mut tuples: Vec<ValIdToValue> = vec![];

    for el in data {
        if el.value.is_none() {
            continue;
        }
        options.terms = vec![el.text];
        options.terms = options.terms.iter().map(|el| util::normalize_text(el)).collect::<Vec<_>>();

        let hits = search_field::get_hits_in_field(persistence, options.clone(), None)?;
        if hits.hits_vec.len() == 1 {
            tuples.push(ValIdToValue {
                valid: hits.hits_vec.iter().nth(0).unwrap().id,
                value: el.value.unwrap(),
            });
        }
    }

    let store = valid_pair_to_direct_index(&mut tuples);
    let path = concat(&path_name, ".tokenValues.boost_valid_to_value");
    let meta_data = persistence.write_direct_index(&store, &path, LoadingType::Disk)?;
    persistence.meta_data.boost_stores.push(meta_data);
    persistence.write_meta_data()?;
    persistence.indices.boost_valueid_to_value.insert(path.to_string(), Box::new(store));
    Ok(())
}

pub fn create_indices_from_str(
    persistence: &mut Persistence,
    data_str: &str,
    indices: &str,
    create_cache: Option<CreateCache>,
    load_persistence: bool,
) -> Result<(CreateCache), CreateError> {
    let stream1 = Deserializer::from_str(&data_str).into_iter::<Value>(); //TODO Performance: Use custom line break deserializer to get string and json at the same time
    let stream2 = Deserializer::from_str(&data_str).into_iter::<Value>();
    create_indices_from_streams(persistence, stream1, stream2, data_str.lines(), indices, create_cache, load_persistence)
}
pub fn create_indices_from_file(
    persistence: &mut Persistence,
    data_path: &str,
    indices: &str,
    create_cache: Option<CreateCache>,
    load_persistence: bool,
) -> Result<(CreateCache), CreateError> {

    let stream1 = std::io::BufReader::new(File::open(data_path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()));
    let stream2 = std::io::BufReader::new(File::open(data_path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()));
    let stream3 = std::io::BufReader::new(File::open(data_path).unwrap()).lines().map(|line| line.unwrap());
    // let stream1 = std::io::BufReader::new(File::open(data_path).unwrap()).lines().map(|line|serde_json::from_str(&line.unwrap()));

    // let stream1 = Deserializer::from_str(&data_str).into_iter::<Value>(); //TODO Performance: Use custom line break deserializer to get string and json at the same time
    // let stream2 = Deserializer::from_str(&data_str).into_iter::<Value>();
    create_indices_from_streams(persistence, stream1, stream2, stream3, indices, create_cache, load_persistence)
}

pub fn create_indices_from_streams<'a, I, J, K, S: AsRef<str>>(
    mut persistence: &mut Persistence,
    stream1: I,
    stream2: J,
    stream3: K,
    indices: &str,
    create_cache: Option<CreateCache>,
    load_persistence: bool,
) -> Result<(CreateCache), CreateError>
where
    I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>,
    J: Iterator<Item = Result<serde_json::Value, serde_json::Error>>,
    K: Iterator<Item = S>,
{
    info_time!(format!("total time create_indices for {:?}", persistence.db));

    let indices_json: Vec<CreateIndex> = serde_json::from_str(indices).unwrap();
    let mut create_cache = create_cache.unwrap_or_else(|| CreateCache::default());
    create_fulltext_index(stream1, stream2, stream3, &mut persistence, &indices_json, &mut create_cache, load_persistence)?;

    info_time!(format!("write json and metadata {:?}", persistence.db));

    persistence.write_meta_data()?;

    Ok(create_cache)
}

#[derive(Debug)]
pub enum CreateError {
    Io(io::Error),
    InvalidJson(serde_json::Error),
    Utf8Error(std::str::Utf8Error),
}

impl From<io::Error> for CreateError {
    fn from(err: io::Error) -> CreateError {
        CreateError::Io(err)
    }
}
impl From<serde_json::Error> for CreateError {
    fn from(err: serde_json::Error) -> CreateError {
        CreateError::InvalidJson(err)
    }
}
impl From<std::str::Utf8Error> for CreateError {
    fn from(err: std::str::Utf8Error) -> CreateError {
        CreateError::Utf8Error(err)
    }
}

// #[cfg(test)]
// mod test {
//     use create;
//     use serde_json;
//     use serde_json::Value;

//     #[test]
//     fn test_ewwwwwwwq() {

//         let opt: create::FulltextIndexOptions = serde_json::from_str(r#"{"tokenize":true, "stopwords": []}"#).unwrap();
//         // let opt = create::FulltextIndexOptions{
//         //     tokenize: true,
//         //     stopwords: vec![]
//         // };

//         let dat2 = r#" [{ "name": "John Doe", "age": 43 }, { "name": "Jaa", "age": 43 }] "#;
//         let data: Value = serde_json::from_str(dat2).unwrap();
//         let res = create::create_fulltext_index(&data, "name", opt);
//         let deserialized: create::BoostIndexOptions = serde_json::from_str(r#"{"boost_type":"int"}"#).unwrap();

//         assert_eq!("Hello", "Hello");

//         let service: create::CreateIndex = serde_json::from_str(r#"{"boost_type":"int"}"#).unwrap();

//     }
// }
