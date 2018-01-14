use util::{self, concat};
use fnv::FnvHashMap;
use fnv::FnvHashSet;

use serde_json::{self, Value};

use json_converter;

use std::{self, str};
use std::io;

use persistence::{LoadingType, Persistence};

use create_from_json;
use log;

#[allow(unused_imports)]
use fst::{self, IntoStreamer, MapBuilder, Set};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum CreateIndex {
    FulltextInfo(Fulltext),
    BoostInfo(Boost),
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
    fn new_with_tokenize() -> FulltextIndexOptions {
        FulltextIndexOptions {
            tokenize: true,
            stopwords: None,
            add_normal_values: Some(true),
        }
    }
    #[allow(dead_code)]
    fn new_without_tokenize() -> FulltextIndexOptions {
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
    pub fn new(id: u32) -> TermInfo {
        TermInfo {
            id: id,
            num_occurences: 0,
        }
    }
}

pub fn set_ids(terms: &mut FnvHashMap<String, TermInfo>) {
    let mut v: Vec<String> = terms
        .keys()
        // .collect::<Vec<&String>>()
        // .iter()
        .map(|el| (*el).clone())
        .collect();
    v.sort();
    for (i, term) in v.iter().enumerate() {
        // terms.get_mut(term)
        if let Some(term_info) = terms.get_mut(&term.clone()) {
            term_info.id = i as u32;
        }
    }
}

pub trait GetValueId {
    fn get_value_id(&self) -> u32;
}

#[derive(Debug)]
pub struct ValIdPair {
    pub valid: u32,
    pub parent_val_id: u32,
}

impl GetValueId for ValIdPair {
    fn get_value_id(&self) -> u32 {
        self.valid
    }
}

/// Used for boost
/// e.g. boost value 5000 for id 5
/// 5 -> 5000
#[derive(Debug)]
pub struct ValIdToValue {
    pub valid: u32,
    pub value: u32,
}

impl GetValueId for ValIdToValue {
    fn get_value_id(&self) -> u32 {
        self.valid
    }
}

impl std::fmt::Display for ValIdPair {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "\n{}\t{}", self.valid, self.parent_val_id)?;
        Ok(())
    }
}

// use std::fmt;
// use std::fmt::{Display, Formatter, Error};

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

use persistence;

fn store_full_text_info(persistence: &mut Persistence, all_terms: FnvHashMap<String, TermInfo>, path: &str, options: &FulltextIndexOptions) -> Result<(), io::Error> {
    info_time!(format!("store_fst strings and string offsets {:?}", path));
    let mut sorted_terms: Vec<&String> = all_terms.keys().collect::<Vec<&String>>();
    sorted_terms.sort();

    persistence.write_data(
        path,
        sorted_terms
            .iter()
            .fold(String::new(), |acc, line| acc + line + "\n")
            .as_bytes(),
    )?;
    let offsets = get_string_offsets(sorted_terms);

    persistence.write_index(
        &persistence::vec_to_bytes_u64(&offsets),
        &offsets,
        &concat(&path, ".offsets"),
    )?; // String byte offsets
        // persistence.write_index(&all_terms.iter().map(|ref el| el.len() as u32).collect::<Vec<_>>(), &concat(path, ".length"))?;
    store_fst(persistence, &all_terms, path).expect("Could not store fst"); // @FixMe handle result
                                                                            // create_char_offsets(&all_terms, &concat(&path, ""), &mut persistence)?;
    persistence
        .meta_data
        .fulltext_indices
        .insert(path.to_string(), options.clone());
    Ok(())
}

fn store_fst(persistence: &mut Persistence, all_terms: &FnvHashMap<String, TermInfo>, path: &str) -> Result<(), fst::Error> {
    debug_time!(format!("store_fst {:?}", path));
    let wtr = persistence.get_buffered_writer(&concat(&path, ".fst"))?;
    // Create a builder that can be used to insert new key-value pairs.
    let mut build = MapBuilder::new(wtr)?;

    let mut v: Vec<&String> = all_terms.keys().collect::<Vec<&String>>();
    v.sort();
    for term in v.iter() {
        let term_info = all_terms.get(term.clone()).expect("wtf");
        build
            .insert(term, term_info.id as u64)
            .expect("could not insert into fst");
    }
    // for (term, term_info) in all_terms.iter() {
    //     build.insert(term, term_info.id as u64).unwrap();
    // }
    // Finish construction of the map and flush its contents to disk.
    build.finish()?;

    Ok(())
}
#[allow(unused_imports)]
use sled;
#[allow(unused_imports)]
use byteorder::{LittleEndian, WriteBytesExt};

use tokenizer::*;

fn add_count_text(terms: &mut FnvHashMap<String, TermInfo>, text: &str) {
    if !terms.contains_key(text){
        terms.insert(text.to_string(), TermInfo::default());
    }
    let stat = terms.get_mut(text).unwrap();
    stat.num_occurences += 1;
}

fn add_text<T: Tokenizer>(text: &str, terms: &mut FnvHashMap<String, TermInfo>, options: &FulltextIndexOptions, tokenizer: &T) {
    trace!("text: {:?}", text);
    if options
        .stopwords
        .as_ref()
        .map(|el| el.contains(text))
        .unwrap_or(false)
    {
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
            // let token_str = token.to_string();
            if options
                .stopwords
                .as_ref()
                .map(|el| el.contains(token))
                .unwrap_or(false)
            {
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

pub fn get_allterms(data: &Value, fulltext_info_for_path: &FnvHashMap<String, Fulltext>) -> FnvHashMap<String, FnvHashMap<String, TermInfo>> {
    info_time!("get_allterms dictionary");
    let mut terms_in_path: FnvHashMap<String, FnvHashMap<String, TermInfo>> = FnvHashMap::default();

    let mut opt = json_converter::ForEachOpt {};
    let mut id_holder = json_converter::IDHolder::new();

    let num_elements = if let Some(arr) = data.as_array() {arr.len() } else{ 1 };

    let tokenizer = SimpleTokenizerCharsIterateGroupTokens {};
    let default_fulltext_options = FulltextIndexOptions::new_with_tokenize();

    {
        let mut cb_text = |value: &str, path: &str, _parent_val_id: u32| {
            let options: &FulltextIndexOptions = fulltext_info_for_path
                .get(path)
                .and_then(|el| el.options.as_ref())
                .unwrap_or(&default_fulltext_options);

            if !terms_in_path.contains_key(path){
                terms_in_path.insert(path.to_string(), FnvHashMap::with_capacity_and_hasher(num_elements, Default::default()));
            }
            let mut terms = terms_in_path.get_mut(path).unwrap();

            // let mut terms = terms_in_path
            //     .entry(path.to_string())
            //     .or_insert(FnvHashMap::with_capacity_and_hasher(num_elements, Default::default()));
            // let normalized_text = util::normalize_text(value);
            add_text(value, &mut terms, &options, &tokenizer);
            // if options.add_normal_values.unwrap_or(true){
            //     add_text(value.to_string(), &mut terms, &options);
            // }
        };

        let mut callback_ids = |_path: &str, _value_id: u32, _parent_val_id: u32| {};

        json_converter::for_each_element(
            &data,
            &mut id_holder,
            &mut opt,
            &mut cb_text,
            &mut callback_ids,
        );
    }

    {
        for mut terms in terms_in_path.values_mut() {
            set_ids(&mut terms);
        }
    }
    terms_in_path

    // let mut v: Vec<String> = terms.into_iter().collect::<Vec<String>>();
    // v.sort();
    // v
}

// fn get_tokens<T: Tokenizer>(x: T) {
//     unimplemented!();
// }

pub fn create_fulltext_index(data: &Value, mut persistence: &mut Persistence, indices_json: &Vec<CreateIndex>) -> Result<(), io::Error> {
    // let data: Value = serde_json::from_str(data_str).unwrap();

    let num_elements = if let Some(arr) = data.as_array() {arr.len() } else{ 1 };

    let fulltext_info_for_path: FnvHashMap<String, Fulltext> = indices_json
        .iter()
        .flat_map(|index| match index {
            &CreateIndex::FulltextInfo(ref el) => Some(el),
            &CreateIndex::BoostInfo(_) => None,
        })
        .map(|fulltext_info| {
            (
                fulltext_info.fulltext.to_string() + ".textindex",
                (*fulltext_info).clone(),
            )
        })
        .collect();

    let all_terms_in_path = get_allterms(&data, &fulltext_info_for_path);
    info_time!("create_fulltext_index");
    trace!("all_terms {:?}", all_terms_in_path);

    let mut opt = json_converter::ForEachOpt {};
    let mut id_holder = json_converter::IDHolder::new();

    let mut tokens_in_path: FnvHashMap<String, Vec<ValIdPair>> = FnvHashMap::default();
    let mut value_id_to_token_ids_in_path: FnvHashMap<String, Vec<ValIdPair>> = FnvHashMap::default();
    let mut tuples_to_parent_in_path: FnvHashMap<String, Vec<ValIdPair>> = FnvHashMap::default(); // tuples to anchor are normalized for searching, here the real texts are used to recreated data
                                                                                                  // let mut text_tuples_to_leaf_in_path:FnvHashMap<String, Vec<ValIdPair>> = FnvHashMap::default(); // text tuples to leaf are used for reading values, here the real texts are used to recreated data
    let mut text_tuples_to_parent_in_path: FnvHashMap<String, Vec<ValIdPair>> = FnvHashMap::default();

    let default_fulltext_options = FulltextIndexOptions::new_with_tokenize();

    let tokenizer = SimpleTokenizerCharsIterateGroupTokens {};
    {
        info_time!(format!("extract text and ids"));
        let mut cb_text = |value: &str, path: &str, parent_val_id: u32| {
            // let value = value.to_string();
            let tokens_to_parent = tokens_in_path.entry(path.to_string()).or_insert(Vec::with_capacity(num_elements));
            let tuples = text_tuples_to_parent_in_path
                .entry(path.to_string())
                .or_insert(Vec::with_capacity(num_elements));
            // let tuples_to_leaf = text_tuples_to_leaf_in_path.entry(path.to_string()).or_insert(vec![]);
            let all_terms = all_terms_in_path.get(path).unwrap();

            let options: &FulltextIndexOptions = fulltext_info_for_path
                .get(path)
                .and_then(|el| el.options.as_ref())
                .unwrap_or(&default_fulltext_options);

            // let options = FulltextIndexOptions::new_with_tokenize(); // TODO @FixMe
            // let normalized_text = util::normalize_text(value);
            if options
                .stopwords
                .as_ref()
                .map(|el| el.contains(value))
                .unwrap_or(false)
            {
                return;
            }

            // //Lower case to search
            // let search_text_id = all_terms.get(&value.to_lowercase()).expect("did not found term").id;
            // tuples.push(ValIdPair { valid:         search_text_id as u32, parent_val_id: parent_val_id });
            // trace!("Found id {:?} for {:?}", search_text_id, value);

            let original_text_id = all_terms.get(value).expect("did not found term").id;
            tuples.push(ValIdPair {
                valid: original_text_id as u32,
                parent_val_id: parent_val_id,
            });
            trace!("Found id {:?} for {:?}", original_text_id, value);

            if options.tokenize && tokenizer.has_tokens(value) {
                let value_id_to_token_ids = value_id_to_token_ids_in_path
                    .entry(path.to_string())
                    .or_insert(vec![]);
                tokenizer.get_tokens(value, &mut |token: &str, _is_seperator: bool| {
                    // let token_str = token.to_string();
                    if options
                        .stopwords
                        .as_ref()
                        .map(|el| el.contains(&token.to_string()))
                        .unwrap_or(false)
                    {
                        return;
                    }

                    // let normalized_id = if is_seperator{
                    //     all_terms.get(&token_str).expect("did not found token").id
                    // }else{
                    //     all_terms.get(&token_str.to_lowercase()).expect("did not found token").id
                    // };

                    // terms.insert(token.to_string());
                    let original_token_val_id = all_terms.get(token).expect("did not found token").id;
                    trace!("Adding to tokens {:?} : {:?}", token, original_token_val_id);
                    // value_id_to_token_ids.push(ValIdPair { valid: search_text_id as u32, parent_val_id: original_token_val_id as u32 }); //ADD search_text_id ????
                    value_id_to_token_ids.push(ValIdPair {
                        valid: original_text_id as u32,
                        parent_val_id: original_token_val_id as u32,
                    });
                    tokens_to_parent.push(ValIdPair {
                        valid: original_token_val_id as u32,
                        parent_val_id: original_text_id as u32,
                    });
                });
            }
        };

        let mut callback_ids = |path: &str, value_id: u32, parent_val_id: u32| {
            let tuples = tuples_to_parent_in_path
                .entry(path.to_string())
                .or_insert(Vec::with_capacity(num_elements));
            tuples.push(ValIdPair {
                valid: value_id,
                parent_val_id: parent_val_id,
            });
        };

        json_converter::for_each_element(
            &data,
            &mut id_holder,
            &mut opt,
            &mut cb_text,
            &mut callback_ids,
        );
    }

    {
        for (path, mut tuples) in tuples_to_parent_in_path
            .iter_mut()
            .chain(text_tuples_to_parent_in_path.iter_mut())
        {
            persistence.write_tuple_pair(&mut tuples, &concat(&path, ".valueIdToParent"))?;
            if log_enabled!(log::Level::Trace) {
                trace!(
                    "{}\n{}",
                    &concat(&path, ".valueIdToParent"),
                    print_vec(&tuples, &path, "parentid")
                );
            }

            //Flip values
            for el in tuples.iter_mut() {
                std::mem::swap(&mut el.parent_val_id, &mut el.valid);
            }
            persistence.write_tuple_pair(&mut tuples, &concat(&path, ".parentToValueId"))?;
            if log_enabled!(log::Level::Trace) {
                trace!(
                    "{}\n{}",
                    &concat(&path, ".parentToValueId"),
                    print_vec(&tuples, &path, "value_id")
                );
            }
        }

        for (path, mut tokens) in tokens_in_path.iter_mut() {
            persistence.write_tuple_pair_dedup(&mut tokens, &concat(&path, ".tokens"), true)?;
        }

        for (path, mut tokens) in value_id_to_token_ids_in_path.iter_mut() {
            persistence.write_tuple_pair(&mut tokens, &concat(&path, ".value_id_to_token_ids"))?;
            trace!(
                "{}\n{}",
                &concat(&path, ".value_id_to_token_ids"),
                print_vec(&tokens, "value_id", "token_id")
            );
        }

        for (path, all_terms) in all_terms_in_path {
            let options: &FulltextIndexOptions = fulltext_info_for_path
                .get(&path)
                .and_then(|el| el.options.as_ref())
                .unwrap_or(&default_fulltext_options);
            store_full_text_info(&mut persistence, all_terms, &path, &options)?;
        }
    }

    // let path_name = util::get_file_path_name(&paths[i], is_text_index);
    // persistence.write_tuple_pair(&mut tuples, &concat(&path_name, ".valueIdToParent"))?;

    //TEST FST AS ID MAPPER
    // let mut all_ids_as_str: FnvHashMap<String, TermInfo> = FnvHashMap::default();
    // for pair in &tuples {
    //     let padding = 10;
    //     all_ids_as_str.insert(format!("{:0padding$}", pair.valid, padding = padding), TermInfo::new(pair.parent_val_id)); // COMPRESSION 50-90%
    // }
    // store_fst(persistence, &all_ids_as_str, &concat(&path_name, ".valueIdToParent.fst")).expect("Could not store fst");
    //TEST FST AS ID MAPPER

    // if is_text_index && options.tokenize {
    //     persistence.write_tuple_pair(&mut tokens, &concat(&path_name, ".tokens"))?;
    //     trace!("{}\n{}",&concat(&path_name, ".tokens"), print_vec(&tokens, &concat(&path_name, ".tokenid"), &concat(&path_name, ".valueid")));
    // }

    // store_full_text_info(&mut persistence, all_terms, path, &options)?;

    Ok(())
}

fn get_string_offsets(data: Vec<&String>) -> Vec<u64> {
    let mut offsets = vec![];
    let mut offset = 0;
    for el in data {
        offsets.push(offset as u64);
        offset += el.len() + 1; // 1 for linevreak
    }
    offsets.push(offset as u64);
    offsets
}

fn create_boost_index(data: &Value, path: &str, options: BoostIndexOptions, persistence: &mut Persistence) -> Result<(), io::Error> {
    info_time!("create_boost_index");

    let mut opt = create_from_json::ForEachOpt {
        parent_pos_in_path: 0,
        current_parent_id_counter: 0,
        value_id_counter: 0,
    };

    let mut tuples: Vec<ValIdToValue> = vec![];
    {
        let mut callback = |value: &str, value_id: u32, _parent_val_id: u32| {
            if options.boost_type == "int" {
                let my_int = value.parse::<u32>().expect("Expected an int value");
                tuples.push(ValIdToValue {
                    valid: value_id,
                    value: my_int,
                });
            } // TODO More cases
        };
        create_from_json::for_each_element_in_path(&data, &mut opt, &path, &mut callback);
    }

    persistence.write_boost_tuple_pair(&mut tuples, path)?;

    Ok(())
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

use search;
use search_field;
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
        options.terms = options
            .terms
            .iter()
            .map(|el| util::normalize_text(el))
            .collect::<Vec<_>>();

        let hits = search_field::get_hits_in_field(persistence, &options)?;
        if hits.hits.len() == 1 {
            tuples.push(ValIdToValue {
                valid: *hits.hits.iter().nth(0).unwrap().0,
                value: el.value.unwrap(),
            });
        }
    }
    persistence.write_boost_tuple_pair(&mut tuples, &concat(&path_name, ".tokenValues"))?;
    persistence.write_meta_data()?;
    Ok(())
}

pub fn create_indices_json(folder: &str, data: &Value, indices: &str) -> Result<(), CreateError> {
    info_time!(format!("total time create_indices for {:?}", folder));

    let indices_json: Vec<CreateIndex> = serde_json::from_str(indices).unwrap();
    let mut persistence = Persistence::create(folder.to_string())?;
    create_fulltext_index(data, &mut persistence, &indices_json)?;
    for el in indices_json {
        match el {
            CreateIndex::FulltextInfo(_) => {}
            CreateIndex::BoostInfo(boost) => create_boost_index(data, &boost.boost, boost.options, &mut persistence)?,
        }
    }

    info_time!(format!("write json and metadata {:?}", folder));
    if let &Some(arr) = &data.as_array() {
        persistence.write_json_to_disk(arr, "data")?;
    } else {
        persistence.write_json_to_disk(&vec![data.clone()], "data")?;
    }

    persistence.write_meta_data()?;

    Ok(())
}

pub fn create_indices(folder: &str, data_str: &str, indices: &str) -> Result<(), CreateError> {
    let data: Value = serde_json::from_str(data_str).unwrap();
    create_indices_json(folder, &data, indices)
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
