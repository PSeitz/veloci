
#[allow(unused_imports)]
use std::io::{self, BufRead};
#[allow(unused_imports)]
use std::time::Duration;

#[allow(unused_imports)]
use std::thread;
#[allow(unused_imports)]
use std::sync::mpsc::sync_channel;

#[allow(unused_imports)]
use std::io::SeekFrom;
use util;
use util::concat;
#[allow(unused_imports)]
use fnv::FnvHashSet;
use fnv::FnvHashMap;
#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::cmp::Ordering;

use serde_json;
use serde_json::Value;

#[allow(unused_imports)]
use std::fs;
#[allow(unused_imports)]
use std::env;

#[allow(unused_imports)]
use std::io::prelude::*;

use std::str;
use persistence::Persistence;
use persistence::LoadingType;

use std;
use std::time::Instant;
use csv;

#[allow(unused_imports)]
use fst::{self, IntoStreamer, Levenshtein, Set, MapBuilder};

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum CreateIndex {
    FulltextInfo(Fulltext),
    BoostInfo(Boost)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Fulltext {
    fulltext: String,
    options: Option<FulltextIndexOptions>,
    attr_pos:Option<usize>,
    loading_type: Option<LoadingType>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Boost {
    boost: String,
    options: BoostIndexOptions
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TokenValuesConfig {
    path: String
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct FulltextIndexOptions {
    pub tokenize: bool,
    pub stopwords: Option<Vec<String>>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BoostIndexOptions {
    boost_type:String
    // type:
}

struct ForEachOpt {
    parent_pos_in_path: u32,
    current_parent_id_counter: u32,
    value_id_counter: u32
}

fn convert_to_string(value: &Value) -> String {
    match value {
        &Value::String(ref s) => s.as_str().to_string(),
        &Value::Number(ref i) if i.is_u64() => i.as_u64().unwrap().to_string(),
        &Value::Number(ref i) if i.is_f64() => i.as_f64().unwrap().to_string(),
        &Value::Bool(ref i) => i.to_string(),
        _ => "".to_string(),
    }
}

fn walk<F>(mut current_el: &Value, start_pos: u32, opt: &mut ForEachOpt, paths:&Vec<&str>, cb: &mut F)
where F: FnMut(&str, u32, u32) {

    for i in start_pos..(paths.len() as u32) {
        let is_last_path = i == paths.len() as u32-1;
        let is_parent_path_pos = i == opt.parent_pos_in_path && i!=0;
        let comp = paths[i as usize];
        // println!("MOVE TO NEXT");
        // println!("{:?}", comp);
        // println!("{:?}", current_el.to_string());
        // println!("{:?}", current_el.get(comp));
        if !current_el.get(comp).is_some() {break;}
        let next_el = &current_el[comp];
        // println!("{:?}", next_el);
        if next_el.is_array(){
            let current_el_arr = next_el.as_array().unwrap();
            if is_last_path{
                for el in current_el_arr {
                    if !el.is_null() {
                        cb(&convert_to_string(&el), opt.value_id_counter, opt.current_parent_id_counter);
                        opt.value_id_counter+=1;
                    }
                }
            }else{
                let next_level = i+1;
                for subarr_el in current_el_arr {
                    walk(subarr_el, next_level, opt, paths, cb);
                    if is_parent_path_pos {opt.current_parent_id_counter += 1;}
                }
            }
        }else{
            if is_last_path{
                if !next_el.is_null() {
                    cb(&convert_to_string(&next_el), opt.value_id_counter, opt.current_parent_id_counter);
                    opt.value_id_counter+=1;
                }
            }
        }
        current_el = next_el

    }
}

fn for_each_element_in_path<F>(data: &Value, opt: &mut ForEachOpt, path2:&str, cb: &mut F)
where F: FnMut(&str, u32, u32) { // value, value_id, parent_val_id   // TODO ADD Template for Value

    let path = util::remove_array_marker(path2);
    let paths = path.split(".").collect::<Vec<_>>();

    if data.is_array(){
        // let startMainId = parent_pos_in_path == 0 ? current_parent_id_counter : 0
        for el in data.as_array().unwrap() {
            walk(el, 0, opt, &paths, cb);
            if opt.parent_pos_in_path == 0 {opt.current_parent_id_counter += 1;}
        }
    }else{
        walk(data, 0, opt, &paths, cb);
    }
}

#[derive(Debug, Default)]
pub struct TermInfo {
    pub id: u32,
    pub num_occurences: u32
}

pub fn get_allterms(data:&Value, path:&str, options:&FulltextIndexOptions) -> FnvHashMap<String, TermInfo>{

    let mut terms:FnvHashMap<String, TermInfo> = FnvHashMap::default();

    let mut opt = ForEachOpt {
        parent_pos_in_path: 0,
        current_parent_id_counter: 0,
        value_id_counter: 0
    };

    for_each_element_in_path(&data, &mut opt, &path,  &mut |value: &str, _value_id: u32, _parent_val_id: u32| {
        let normalized_text = util::normalize_text(value);
        trace!("normalized_text: {:?}", normalized_text);
        if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&normalized_text) {
            return;
        }

        // if stopwords.map_or(false, |ref v| v.contains(&value)){
        //     return;
        // }

        {
            let stat = terms.entry(normalized_text.clone()).or_insert(TermInfo::default());
            stat.num_occurences += 1;
        }

        if options.tokenize && normalized_text.split(" ").count() > 1 {
            for token in normalized_text.split(" ") {
                let token_str = token.to_string();
                if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&token_str) { continue; }
                // terms.insert(token_str);
                let stat = terms.entry(token_str.clone()).or_insert(TermInfo::default());
                stat.num_occurences += 1;
            }
        }
    });

    set_ids(&mut terms);
    terms

    // let mut v: Vec<String> = terms.into_iter().collect::<Vec<String>>();
    // v.sort();
    // v
}

fn get_allterms_csv(csv_path:&str, attr_pos:usize, options:&FulltextIndexOptions) -> FnvHashMap<String, TermInfo>{
    // char escapeChar = 'a';
    // MATNR, ISMTITLE, ISMORIGTITLE, ISMSUBTITLE1, ISMSUBTITLE2, ISMSUBTITLE3, ISMARTIST, ISMLANGUAGES, ISMPUBLDATE, EAN11, ISMORIDCODE
    info_time!("get_allterms_csv total");
    let mut terms:FnvHashMap<String, TermInfo> = FnvHashMap::default();
    let mut rdr = csv::Reader::from_file(csv_path).unwrap().has_headers(false).escape(Some(b'\\'));
    for record in rdr.decode() {
        let els:Vec<Option<String>> = record.unwrap();
        if els[attr_pos].is_none() { continue;}
        let normalized_text = util::normalize_text(els[attr_pos].as_ref().unwrap());

        if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&normalized_text) { continue; }
        // terms.insert(els[attr_pos].as_ref().unwrap().clone());
        // terms.insert(normalized_text.clone());
        {
            let stat = terms.entry(normalized_text.clone()).or_insert(TermInfo::default());
            stat.num_occurences += 1;
        }
        if options.tokenize && normalized_text.split(" ").count() > 1 {
            for token in normalized_text.split(" ") {
                let token_str = token.to_string();
                if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&token_str) { continue; }
                // terms.insert(token_str);
                let stat = terms.entry(token_str.clone()).or_insert(TermInfo::default());
                stat.num_occurences += 1;
            }
        }

    }
    info_time!("get_allterms_csv sort");
    set_ids(&mut terms);
    terms
}

fn set_ids(terms: &mut FnvHashMap<String, TermInfo>) {
    let mut v: Vec<String> = terms.keys().collect::<Vec<&String>>().iter().map(|el| (*el).clone()).collect();
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
    pub parent_val_id:u32
}

impl GetValueId for ValIdPair {
    fn get_value_id(&self) -> u32{ self.valid }
}

/// Used for boost
/// e.g. boost value 5000 for id 5
/// 5 -> 5000
#[derive(Debug)]
pub struct ValIdToValue {
    pub valid:u32,
    pub value:u32
}

impl GetValueId for ValIdToValue {
    fn get_value_id(&self) -> u32{ self.valid }
}

impl std::fmt::Display for ValIdPair {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "\n{}\t{}", self.valid, self.parent_val_id )?;
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

fn print_vec(vec: &Vec<ValIdPair>) -> String{
    String::from("valid\tparent_val_id") + &vec
        .iter().map(|el| format!("\n{}\t{}", el.valid, el.parent_val_id))
        .collect::<Vec<_>>()
        .join("")
}



pub fn create_fulltext_index_csv(csv_path: &str, attr_name:&str, attr_pos: usize , options:FulltextIndexOptions, mut persistence: &mut Persistence) -> Result<(), io::Error> {
    let now = Instant::now();
    let all_terms = get_allterms_csv(csv_path, attr_pos, &options);
    println!("all_terms {} {}ms" , csv_path, (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));

    let mut tuples:Vec<ValIdPair> = vec![];
    let mut tokens:Vec<ValIdPair> = vec![];
    let mut row: i64 = -1;

    let mut rdr = csv::Reader::from_file(csv_path).unwrap().has_headers(false).escape(Some(b'\\'));
    for record in rdr.decode() {
        row+=1;
        let els:Vec<Option<String>> = record.unwrap();
        if els[attr_pos].is_none() { continue;}
        let normalized_text = util::normalize_text(els[attr_pos].as_ref().unwrap());
        if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&normalized_text) { continue; }

        // let val_id = all_terms.binary_search(&normalized_text).unwrap();
        let val_id = all_terms.get(&normalized_text).unwrap().id;
        tuples.push(ValIdPair{valid:val_id as u32, parent_val_id:row as u32});
        trace!("Found id {:?} for {:?}", val_id, normalized_text);
        if options.tokenize && normalized_text.split(" ").count() > 1 {
            for token in normalized_text.split(" ") {
                let token_str = token.to_string();
                if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&token_str) { continue; }
                // let tolen_val_id = all_terms.binary_search(&token_str).unwrap();
                let tolen_val_id = all_terms.get(&token_str).unwrap().id;
                trace!("Adding to tokens {:?} : {:?}", token, tolen_val_id);
                tokens.push(ValIdPair{valid:tolen_val_id as u32, parent_val_id:val_id as u32});
            }
        }
    }

    let is_text_index = true;
    let path_name = util::get_path_name(attr_name, is_text_index);
    persistence.write_tuple_pair(&mut tuples, &concat(&path_name, ".valueIdToParent"))?;

    if tokens.len() > 0 {
        persistence.write_tuple_pair(&mut tokens, &concat(&path_name, ".tokens"))?;
    }

    store_full_text_info(&mut persistence, all_terms, &attr_name, &options)?;

    println!("createIndexComplete {} {}ms" , attr_name, (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));

    Ok(())
}
use persistence;

fn store_full_text_info(mut persistence: &mut Persistence, all_terms: FnvHashMap<String, TermInfo>, path:&str, options:&FulltextIndexOptions) -> Result<(), io::Error> {
    let mut sorted_terms: Vec<&String> = all_terms.keys().collect::<Vec<&String>>();
    sorted_terms.sort();
    let offsets = get_string_offsets(sorted_terms);
    persistence.write_index(&persistence::vec_to_bytes_u64(&offsets), &offsets, &concat(&path, ".offsets"))?; // String byte offsets
    // persistence.write_data(path, all_terms.join("\n").as_bytes())?;
    // persistence.write_index(&all_terms.iter().map(|ref el| el.len() as u32).collect::<Vec<_>>(), &concat(path, ".length"))?;
    store_fst(persistence, &all_terms, path).expect("Could not store fst"); // @FixMe handle result
    // create_char_offsets(&all_terms, &concat(&path, ""), &mut persistence)?;
    persistence.meta_data.fulltext_indices.insert(path.to_string(), options.clone());
    Ok(())
}

fn store_fst(persistence: &mut Persistence,all_terms: &FnvHashMap<String, TermInfo>, path:&str) -> Result<(), fst::Error> {
    info_time!("store_fst");
    let wtr = persistence.get_buffered_writer(&concat(&path, ".fst"))?;
    // Create a builder that can be used to insert new key-value pairs.
    let mut build = MapBuilder::new(wtr)?;

    let mut v: Vec<&String> = all_terms.keys().collect::<Vec<&String>>();
    v.sort();
    for term in v.iter() {
        let term_info = all_terms.get(term.clone()).expect("wtf");
        build.insert(term, term_info.id as u64).expect("could not insert");
    }
    // for (term, term_info) in all_terms.iter() {
    //     build.insert(term, term_info.id as u64).unwrap();
    // }
    // Finish construction of the map and flush its contents to disk.
    build.finish()?;

    Ok(())
}

pub fn create_fulltext_index(data: &Value, path:&str, options:FulltextIndexOptions,mut persistence: &mut Persistence) -> Result<(), io::Error> {
    let now = Instant::now();

    // let data: Value = serde_json::from_str(data_str).unwrap();
    let all_terms = get_allterms(&data, path, &options);
    println!("all_terms {} {}ms" , path, (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));
    trace!("all_terms {:?}", all_terms);
    let paths = util::get_steps_to_anchor(path);
    info!("paths: {:?}", paths);
    for i in 0..paths.len() {

        let level = util::get_level(&paths[i]);
        let mut tuples:Vec<ValIdPair> = vec![];
        let mut tokens:Vec<ValIdPair> = vec![];

        let is_text_index = i == (paths.len() -1);

        let mut opt = ForEachOpt {
            parent_pos_in_path: if level>0 {level-1} else {0},
            current_parent_id_counter: 0,
            value_id_counter: 0
        };

        if is_text_index {
            for_each_element_in_path(&data, &mut opt, &paths[i], &mut |value: &str, value_id: u32, _parent_val_id: u32| {
                let normalized_text = util::normalize_text(value);
                if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&normalized_text) { return; }

                let val_id = all_terms.get(&normalized_text).expect("did not found term").id;
                tuples.push(ValIdPair{valid:val_id as u32, parent_val_id:value_id});
                trace!("Found id {:?} for {:?}", val_id, normalized_text);
                // println!("normalized_text.split {:?}", normalized_text.split(" "));
                if options.tokenize && normalized_text.split(" ").count() > 1 {
                    for token in normalized_text.split(" ") {
                        let token_str = token.to_string();
                        if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&token_str) { continue; }
                        // terms.insert(token.to_string());
                        let tolen_val_id = all_terms.get(&token_str).expect("did not found token").id;
                        trace!("Adding to tokens {:?} : {:?}", token, tolen_val_id);
                        tokens.push(ValIdPair{valid:tolen_val_id as u32, parent_val_id:val_id as u32});
                    }
                }
            });
        }else{
            let mut callback = |_value: &str, value_id: u32, parent_val_id: u32| {
                tuples.push(ValIdPair{valid:value_id, parent_val_id:parent_val_id});
            };
            for_each_element_in_path(&data, &mut opt, &paths[i], &mut callback);
        }

        let path_name = util::get_path_name(&paths[i], is_text_index);
        persistence.write_tuple_pair(&mut tuples, &concat(&path_name, ".valueIdToParent"))?;

        if tokens.len() > 0 {
            persistence.write_tuple_pair(&mut tokens, &concat(&path_name, ".tokens"))?;
        }

    }

    println!("createIndex {} {}ms" , path, (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));

    store_full_text_info(&mut persistence, all_terms, path, &options)?;

    println!("createIndexComplete {} {}ms" , path, (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));
    Ok(())

}


fn get_string_offsets(data:Vec<&String>) -> Vec<u64> {
    let mut offsets = vec![];
    let mut offset = 0;
    for el in data {
        offsets.push(offset as u64);
        offset += el.len() + 1; // 1 for linevreak
    }
    offsets.push(offset as u64);
    offsets
}

fn create_boost_index(data: &Value, path:&str, options:BoostIndexOptions,mut persistence: &mut Persistence) -> Result<(), io::Error> {
    let now = Instant::now();
    let mut opt = ForEachOpt {
        parent_pos_in_path: 0,
        current_parent_id_counter: 0,
        value_id_counter: 0
    };

    let mut tuples:Vec<ValIdToValue> = vec![];
    {
        let mut callback = |value: &str, value_id: u32, _parent_val_id: u32| {
            if options.boost_type == "int" {
                let my_int = value.parse::<u32>().unwrap();
                tuples.push(ValIdToValue{valid:value_id, value:my_int});
            } // TODO More cases
        };
        for_each_element_in_path(&data, &mut opt, &path, &mut callback);
    }

    persistence.write_boost_tuple_pair(&mut tuples, path)?;

    println!("create_boost_index {} {}ms" , path, (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));
    Ok(())

}

#[derive(Debug, Clone)]
struct CharData {
    suffix:String,
    line_num: u64,
    byte_offset_start: u64
}

impl PartialEq for CharData {
    fn eq(&self, other: &CharData) -> bool {
        self.suffix == other.suffix
    }
}


#[derive(Debug, Clone)]
struct CharDataComplete {
    suffix:String,
    line_num: u64,
    byte_offset_start: u64,
    byte_offset_end: u64
}

fn print_vec_chardata(vec: &Vec<CharDataComplete>) -> String{
    String::from(format!("\nchar\toffset_start\toffset_end\tline_offset")) + &vec
        .iter().map(|el| format!("\n{:3}\t{:10}\t{:10}\t{:10}", el.suffix, el.byte_offset_start, el.byte_offset_end, el.line_num))
        .collect::<Vec<_>>()
        .join("")
}


// pub fn create_char_offsets(data:&Vec<String>, path:&str,mut persistence: &mut Persistence) -> Result<(), io::Error> {
//     let now = Instant::now();
//     let mut char_offsets:Vec<CharData> = vec![];

//     let mut current_byte_offset = 0;
//     let mut line_num = 0;
//     for text in data {
//         let mut chars = text.chars();
//         let char1 = chars.next().map_or("".to_string(), |c| c.to_string());
//         let char12 = char1.clone() + &chars.next().map_or("".to_string(), |c| c.to_string());

//         if char_offsets.binary_search_by(|ref x| x.suffix.cmp(&char1)).is_err(){
//             char_offsets.push(CharData{suffix:char1, byte_offset_start:current_byte_offset, line_num:line_num});
//         }

//         if char_offsets.binary_search_by(|ref x| x.suffix.cmp(&char12)).is_err() {
//             char_offsets.push(CharData{suffix:char12, byte_offset_start:current_byte_offset, line_num:line_num});
//         }

//         current_byte_offset += text.len() as u64 + 1;
//         line_num+=1;
//     }
//     let mut char_offsets_complete:Vec<CharDataComplete> = vec![];

//     for (i,ref mut char_offset) in char_offsets.iter().enumerate() {
//         let forward_look_next_el = char_offsets.iter().skip(i+1).find(|&r| r.suffix.len() == char_offset.suffix.len());
//         // println!("{:?}", forward_look_next_el);
//         let byte_offset_end = forward_look_next_el.map_or(current_byte_offset, |v| v.byte_offset_start-1);
//         char_offsets_complete.push(CharDataComplete{
//             suffix:char_offset.suffix.to_string(),
//             line_num:char_offset.line_num,
//             byte_offset_start:char_offset.byte_offset_start,
//             byte_offset_end:byte_offset_end});
//     }

//     trace!("{}", print_vec_chardata(&char_offsets_complete));


//     // path!PWN test macro
//     persistence.write_index(&char_offsets_complete.iter().map(|ref el| el.byte_offset_start).collect::<Vec<_>>(), &(path.to_string()+".char_offsets.byteOffsetsStart"))?;
//     persistence.write_index(&char_offsets_complete.iter().map(|ref el| el.byte_offset_end  ).collect::<Vec<_>>(), &(path.to_string()+".char_offsets.byteOffsetsEnd"))?;
//     persistence.write_index(&char_offsets_complete.iter().map(|ref el| el.line_num         ).collect::<Vec<_>>(), &(path.to_string()+".char_offsets.lineOffset"))?;

//     persistence.write_data(&(path.to_string()+".char_offsets.chars"), &char_offsets_complete.iter().map(|ref el| el.suffix.to_string()).collect::<Vec<_>>().join("\n").as_bytes())?;

//     info!("create_char_offsets_complete {} {}ms" , path, (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));
//     Ok(())
// }

#[derive(Serialize, Deserialize, Debug)]
struct TokenValueData {
    text: String,
    value: Option<u32>
}

use search;
use search_field;
pub fn add_token_values_to_tokens(persistence:&mut Persistence, data_str:&str, config:&str) -> Result<(), search::SearchError> {
    let data: Vec<TokenValueData> = serde_json::from_str(data_str).unwrap();
    let config: TokenValuesConfig = serde_json::from_str(config).unwrap();


    let mut options: search::RequestSearchPart = search::RequestSearchPart{
        path: config.path.clone(),
        levenshtein_distance: Some(0),
        resolve_token_to_parent_hits: Some(false),
        .. Default::default()
    };

    let is_text_index = true;
    let path_name = util::get_path_name(&config.path, is_text_index);
    let mut tuples:Vec<ValIdToValue> = vec![];

    for el in data {
        if el.value.is_none() {
            continue;
        }
        options.term = util::normalize_text(&el.text);

        let hits = search_field::get_hits_in_field(persistence, &options)?;
        if hits.hits.len() == 1 {
            tuples.push(ValIdToValue{valid:hits.hits[0].0, value:el.value.unwrap()});
        }
    }
    persistence.write_boost_tuple_pair(&mut tuples, &concat(&path_name, ".tokenValues"))?;
    persistence.write_meta_data()?;
    Ok(())
}


pub fn create_indices(folder:&str, data_str:&str, indices:&str) -> Result<(), CreateError>{

    let data: Value = serde_json::from_str(data_str).unwrap();

    let indices_json:Vec<CreateIndex> = serde_json::from_str(indices).unwrap();
    let mut persistence = Persistence::create(folder.to_string())?;
    for el in indices_json {
        match el {
            CreateIndex::FulltextInfo(full_text) => create_fulltext_index(&data, &full_text.fulltext, full_text.options.unwrap_or(Default::default()), &mut persistence)?,
            CreateIndex::BoostInfo(boost) => create_boost_index(&data, &boost.boost, boost.options, &mut persistence)?
        }
    }

    persistence.write_json_to_disk(&data.as_array().unwrap(), "data")?;
    persistence.write_meta_data()?;

    Ok(())
}

#[derive(Debug)]
pub enum CreateError{
    Io(io::Error),
    InvalidJson(serde_json::Error),
    Utf8Error(std::str::Utf8Error)
}

impl From<io::Error> for CreateError {fn from(err: io::Error) -> CreateError {CreateError::Io(err) } }
impl From<serde_json::Error> for CreateError {fn from(err: serde_json::Error) -> CreateError {CreateError::InvalidJson(err) } }
impl From<std::str::Utf8Error> for CreateError {fn from(err: std::str::Utf8Error) -> CreateError {CreateError::Utf8Error(err) } }

pub fn create_indices_csv(folder:&str, csv_path: &str, indices:&str) -> Result<(), CreateError>{

    // let indices_json:Result<Vec<CreateIndex>> = serde_json::from_str(indices);
    // println!("{:?}", indices_json);
    let indices_json:Vec<CreateIndex> = serde_json::from_str(indices)?;
    let mut persistence = Persistence::create(folder.to_string())?;
    for el in indices_json {
        match el {
            CreateIndex::FulltextInfo(full_text)/*{ fulltext: path, options, attr_pos : _ }*/ => create_fulltext_index_csv(csv_path, &full_text.fulltext, full_text.attr_pos.unwrap(), full_text.options.unwrap_or(Default::default()), &mut persistence)?,
            CreateIndex::BoostInfo(boost) => {} // @Temporary // @FixMe
        }
    }

    let json = create_json_from_c_s_v(csv_path);
    persistence.write_json_to_disk(&json, "data")?;

    persistence.write_meta_data()?;

    Ok(())
}


fn create_json_from_c_s_v(csv_path: &str) -> Vec<Value> {
    let mut res = vec![];
    // let mut row: i64 = -1;

    let mut rdr = csv::Reader::from_file(csv_path).unwrap().has_headers(false).escape(Some(b'\\'));
    for record in rdr.decode() {
        // row+=1;
        let els:Vec<Option<String>> = record.unwrap();
        let mut map = FnvHashMap::default();
        // if els[attr_pos].is_none() { continue;}

        map.insert("MATNR".to_string(), els[0].clone().unwrap());
        let v: Value = serde_json::from_str(&serde_json::to_string(&map).unwrap()).unwrap();
        res.push(v);

    }
    res
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
//         println!("{:?}", res);
//         let deserialized: create::BoostIndexOptions = serde_json::from_str(r#"{"boost_type":"int"}"#).unwrap();

//         assert_eq!("Hello", "Hello");

//         let service: create::CreateIndex = serde_json::from_str(r#"{"boost_type":"int"}"#).unwrap();
//         println!("service: {:?}", service);



//     }
// }

