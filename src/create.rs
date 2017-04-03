
use std::fs::File;
use std::io::prelude::*;
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
use util::get_file_path;
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
#[allow(unused_imports)]

use std::str;
use persistence;

// #[derive(Serialize, Deserialize, Debug, Default)]
// pub struct IndexKeyValueMetaData {
//     path1: IDList,
//     path2: IDList,
//     size: u64
// }
// #[derive(Serialize, Deserialize, Debug, Default)]
// pub struct CharOffsetMetaData {
//     path: String,
// }



#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum CreateIndex {
    Fulltext { fulltext: String, options: Option<FulltextIndexOptions>, attr_pos:Option<usize>},
    Boost { boost: String, options: BoostIndexOptions },
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct FulltextIndexOptions {
    tokenize: bool,
    stopwords: Option<Vec<String>>
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



pub fn get_allterms(data:&Value, path:&str, options:&FulltextIndexOptions) -> Vec<String>{

    let mut terms:FnvHashSet<String> = FnvHashSet::default();

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
        terms.insert(normalized_text.clone());
        if options.tokenize && normalized_text.split(" ").count() > 1 {
            for token in normalized_text.split(" ") {
                let token_str = token.to_string();
                if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&token_str) { continue; }
                terms.insert(token_str);
            }
        }
    });

    let mut v: Vec<String> = terms.into_iter().collect::<Vec<String>>();
    v.sort();
    v
}


#[derive(Debug)]
struct ValIdPair {
    valid: u32,
    parent_val_id:u32
}

impl std::fmt::Display for ValIdPair {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "\n{}\t{}", self.valid, self.parent_val_id )?;
        Ok(())
    }
}

use std;
// use std::fmt;
// use std::fmt::{Display, Formatter, Error};

// impl<ValIdPair> fmt::Display for Vec<ValIdPair> {
//     fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
//         write!(f, "(a, b)",)
//         Ok(())
//     }
// }

// #[derive(Debug)]
// struct ValIdPairVec<'a>(& 'a Vec<ValIdPair>);

// impl std::fmt::Display<'a> for ValIdPairVec<'a> {
//     fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
//         write!(f, "\nvalid\tparent_val_id")?;
//         for el in &self.0{
//             write!(f, "\n{}\t{}", el.valid, el.parent_val_id )?;
//         }
//         Ok(())
//     }
// }

//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "({}, {})", self.x, self.y)
//     }
// }

fn print_vec(vec: &Vec<ValIdPair>) -> String{
    String::from("valid\tparent_val_id") + &vec
        .iter().map(|el| format!("\n{}\t{}", el.valid, el.parent_val_id))
        .collect::<Vec<_>>()
        .join("")
}

use std::time::Instant;


fn get_allterms_csv(csv_path:&str, attr_pos:usize, options:&FulltextIndexOptions) -> Vec<String>{
    // char escapeChar = 'a';
    // MATNR, ISMTITLE, ISMORIGTITLE, ISMSUBTITLE1, ISMSUBTITLE2, ISMSUBTITLE3, ISMARTIST, ISMLANGUAGES, ISMPUBLDATE, EAN11, ISMORIDCODE
    let total_time = util::MeasureTime::new("total_time", util::MeasureTimeLogLevel::Debug);
    let mut terms:FnvHashSet<String> = FnvHashSet::default();
    let mut rdr = csv::Reader::from_file(csv_path).unwrap().has_headers(false).escape(Some(b'\\'));
    for record in rdr.decode() {
        let els:Vec<Option<String>> = record.unwrap();
        if els[attr_pos].is_none() { continue;}
        let normalized_text = util::normalize_text(els[attr_pos].as_ref().unwrap());

        if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&normalized_text) { continue; }
        // terms.insert(els[attr_pos].as_ref().unwrap().clone());
        terms.insert(normalized_text.clone());
        if options.tokenize && normalized_text.split(" ").count() > 1 {
            for token in normalized_text.split(" ") {
                let token_str = token.to_string();
                if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&token_str) { continue; }
                terms.insert(token_str);
            }
        }

    }
    let my_time = util::MeasureTime::new("Sort Time", util::MeasureTimeLogLevel::Debug);
    let mut v: Vec<String> = terms.into_iter().collect::<Vec<String>>();
    v.sort();
    v
}


use csv;
pub fn create_fulltext_index_csv(csv_path: &str, folder: &str, attr_name:&str, attr_pos: usize ,options:FulltextIndexOptions, mut meta_data: &mut persistence::MetaData) -> Result<(), io::Error> {
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

        let val_id = all_terms.binary_search(&normalized_text).unwrap();
        tuples.push(ValIdPair{valid:val_id as u32, parent_val_id:row as u32});
        trace!("Found id {:?} for {:?}", val_id, normalized_text);
        if options.tokenize && normalized_text.split(" ").count() > 1 {
            for token in normalized_text.split(" ") {
                let token_str = token.to_string();
                if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&token_str) { continue; }
                let tolen_val_id = all_terms.binary_search(&token_str).unwrap();
                trace!("Adding to tokens {:?} : {:?}", token, tolen_val_id);
                tokens.push(ValIdPair{valid:tolen_val_id as u32, parent_val_id:val_id as u32});
            }
        }
    }

    let is_text_index = true;
    tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
    let path_name = util::get_path_name(attr_name, is_text_index);
    trace!("\nValueIdToParent {:?}: {}", path_name, print_vec(&tuples));
    persistence::write_index(&tuples.iter().map(|ref el| el.valid      ).collect::<Vec<_>>(),   &get_file_path(folder, &path_name, ".valueIdToParent.valIds"), &mut meta_data)?;
    persistence::write_index(&tuples.iter().map(|ref el| el.parent_val_id).collect::<Vec<_>>(), &get_file_path(folder, &path_name, ".valueIdToParent.mainIds"), &mut meta_data)?;

    if tokens.len() > 0 {
        tokens.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
        trace!("\nTokens {:?}: {}", &path_name, print_vec(&tokens));
        persistence::write_index(&tokens.iter().map(|ref el| el.valid      ).collect::<Vec<_>>(),  &get_file_path(folder, &path_name, ".tokens.tokenValIds"), &mut meta_data)?;
        persistence::write_index(&tokens.iter().map(|ref el| el.parent_val_id).collect::<Vec<_>>(), &get_file_path(folder, &path_name, ".tokens.parentValId"), &mut meta_data)?;
    }

    persistence::write_index64(&get_string_offsets(&all_terms), &get_file_path(folder, &attr_name, ".offsets"), &mut meta_data)?; // String offsets
    File::create(&get_file_path(folder, &attr_name, ""))?.write_all(all_terms.join("\n").as_bytes())?;
    persistence::write_index(&all_terms.iter().map(|ref el| el.len() as u32).collect::<Vec<_>>(), &get_file_path(folder, attr_name, ".length"), &mut meta_data)?;
    create_char_offsets(all_terms, &get_file_path(folder, &attr_name, ""), &mut meta_data)?;

    println!("createIndexComplete {} {}ms" , attr_name, (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));


    Ok(())
}



pub fn create_fulltext_index(data: &Value, folder: &str, path:&str, options:FulltextIndexOptions,mut meta_data: &mut persistence::MetaData) -> Result<(), io::Error> {
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

                let val_id = all_terms.binary_search(&normalized_text).unwrap();
                tuples.push(ValIdPair{valid:val_id as u32, parent_val_id:value_id});
                trace!("Found id {:?} for {:?}", val_id, normalized_text);
                // println!("normalized_text.split {:?}", normalized_text.split(" "));
                if options.tokenize && normalized_text.split(" ").count() > 1 {
                    for token in normalized_text.split(" ") {
                        let token_str = token.to_string();
                        if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&token_str) { continue; }
                        // terms.insert(token.to_string());
                        let tolen_val_id = all_terms.binary_search(&token_str).unwrap();
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

        tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
        let path_name = util::get_path_name(&paths[i], is_text_index);
        trace!("\nValueIdToParent {:?}: {}", path_name, print_vec(&tuples));
        persistence::write_index(&tuples.iter().map(|ref el| el.valid      ).collect::<Vec<_>>(),   &get_file_path(folder, &path_name, ".valueIdToParent.valIds"), &mut meta_data)?;
        persistence::write_index(&tuples.iter().map(|ref el| el.parent_val_id).collect::<Vec<_>>(), &get_file_path(folder, &path_name, ".valueIdToParent.mainIds"), &mut meta_data)?;


        if tokens.len() > 0 {
            tokens.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
            trace!("\nTokens {:?}: {}", &path_name, print_vec(&tokens));
            persistence::write_index(&tokens.iter().map(|ref el| el.valid      ).collect::<Vec<_>>(),  &get_file_path(folder, &path_name, ".tokens.tokenValIds"), &mut meta_data)?;
            persistence::write_index(&tokens.iter().map(|ref el| el.parent_val_id).collect::<Vec<_>>(), &get_file_path(folder, &path_name, ".tokens.parentValId"), &mut meta_data)?;
        }

    }

    println!("createIndex {} {}ms" , path, (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));

    // println!("{:?}", all_terms);
    // println!("{:?}", all_terms.join("\n"));
    persistence::write_index64(&get_string_offsets(&all_terms), &get_file_path(folder, &path, ".offsets"), &mut meta_data)?; // String offsets
    File::create(&get_file_path(folder, &path, ""))?.write_all(all_terms.join("\n").as_bytes())?;
    persistence::write_index(&all_terms.iter().map(|ref el| el.len() as u32).collect::<Vec<_>>(), &get_file_path(folder, path, ".length"), &mut meta_data)?;
    create_char_offsets(all_terms, &get_file_path(folder, &path, ""), &mut meta_data)?;

    println!("createIndexComplete {} {}ms" , path, (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));
    Ok(())

}


fn get_string_offsets(data:&Vec<String>) -> Vec<u64> {
    let mut offsets = vec![];
    let mut offset = 0;
    for el in data {
        offsets.push(offset as u64);
        offset += el.len() + 1; // 1 for linevreak
    }
    offsets.push(offset as u64);
    offsets
}

fn create_boost_index(data: &Value, folder: &str, path:&str, options:BoostIndexOptions,mut meta_data: &mut persistence::MetaData) -> Result<(), io::Error> {
    let now = Instant::now();
    let mut opt = ForEachOpt {
        parent_pos_in_path: 0,
        current_parent_id_counter: 0,
        value_id_counter: 0
    };

    let mut tuples:Vec<ValIdPair> = vec![];
    {
        let mut callback = |value: &str, _value_id: u32, parent_val_id: u32| {
            if options.boost_type == "int" {
                let my_int = value.parse::<u32>().unwrap();
                tuples.push(ValIdPair{valid:my_int, parent_val_id:parent_val_id});
            } // TODO More cases
        };
        for_each_element_in_path(&data, &mut opt, &path, &mut callback);
    }
    tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));

    persistence::write_index(&tuples.iter().map(|ref el| el.parent_val_id).collect::<Vec<_>>(),&get_file_path(folder, path, ".boost.subObjId"), &mut meta_data)?;
    persistence::write_index(&tuples.iter().map(|ref el| el.valid      ).collect::<Vec<_>>(),  &get_file_path(folder, path, ".boost.value"), &mut meta_data)?;
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


pub fn create_char_offsets(data:Vec<String>, path:&str,mut meta_data: &mut persistence::MetaData) -> Result<(), io::Error> {
    let now = Instant::now();
    let mut char_offsets:Vec<CharData> = vec![];

    let mut current_byte_offset = 0;
    let mut line_num = 0;
    for text in data {
        let mut chars = text.chars();
        let char1 = chars.next().map_or("".to_string(), |c| c.to_string());
        let char12 = char1.clone() + &chars.next().map_or("".to_string(), |c| c.to_string());

        if char_offsets.binary_search_by(|ref x| x.suffix.cmp(&char1)).is_err(){
            char_offsets.push(CharData{suffix:char1, byte_offset_start:current_byte_offset, line_num:line_num});
        }

        if char_offsets.binary_search_by(|ref x| x.suffix.cmp(&char12)).is_err() {
            char_offsets.push(CharData{suffix:char12, byte_offset_start:current_byte_offset, line_num:line_num});
        }

        current_byte_offset += text.len() as u64 + 1;
        line_num+=1;
    }
    let mut char_offsets_complete:Vec<CharDataComplete> = vec![];

    for (i,ref mut char_offset) in char_offsets.iter().enumerate() {
        let forward_look_next_el = char_offsets.iter().skip(i+1).find(|&r| r.suffix.len() == char_offset.suffix.len());
        // println!("{:?}", forward_look_next_el);
        let byte_offset_end = forward_look_next_el.map_or(current_byte_offset, |v| v.byte_offset_start-1);
        char_offsets_complete.push(CharDataComplete{
            suffix:char_offset.suffix.to_string(),
            line_num:char_offset.line_num,
            byte_offset_start:char_offset.byte_offset_start,
            byte_offset_end:byte_offset_end});
    }

    trace!("{}", print_vec_chardata(&char_offsets_complete));


    // path!PWN test macro
    persistence::write_index64(&char_offsets_complete.iter().map(|ref el| el.byte_offset_start).collect::<Vec<_>>(), &(path.to_string()+".char_offsets.byteOffsetsStart"), &mut meta_data)?;
    persistence::write_index64(&char_offsets_complete.iter().map(|ref el| el.byte_offset_end  ).collect::<Vec<_>>(), &(path.to_string()+".char_offsets.byteOffsetsEnd"), &mut meta_data)?;
    persistence::write_index64(&char_offsets_complete.iter().map(|ref el| el.line_num         ).collect::<Vec<_>>(), &(path.to_string()+".char_offsets.lineOffset"), &mut meta_data)?;


    File::create(&(path.to_string()+".char_offsets.chars"))?.write_all(&char_offsets_complete.iter().map(|ref el| el.suffix.to_string()).collect::<Vec<_>>().join("\n").as_bytes())?;
    info!("create_char_offsets_complete {} {}ms" , path, (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));
    Ok(())
}

pub fn create_indices(folder:&str, data_str:&str, indices:&str) -> Result<(), CreateError>{

    fs::create_dir_all(folder)?;
    let data: Value = serde_json::from_str(data_str).unwrap();

    let indices_json:Vec<CreateIndex> = serde_json::from_str(indices).unwrap();
    let mut meta_data = persistence::MetaData {id_lists: FnvHashMap::default()};
    for el in indices_json {
        match el {
            CreateIndex::Fulltext{ fulltext: path, options, attr_pos } => create_fulltext_index(&data, &folder, &path, options.unwrap_or(Default::default()), &mut meta_data)?,
            CreateIndex::Boost{ boost: path, options } => create_boost_index(&data, &folder, &path, options, &mut meta_data)?
        }
    }

    write_json_to_disk(&data.as_array().unwrap(), folder, "data", &mut meta_data)?;

    let meta_data_str = serde_json::to_string_pretty(&meta_data).unwrap();
    let mut buffer = File::create(&get_file_path(folder, "metaData", ""))?;
    buffer.write_all(&meta_data_str.as_bytes())?;

    Ok(())
}

#[derive(Debug)]
pub enum CreateError{
    Io(io::Error),
    InvalidJson(serde_json::Error),
    Utf8Error(std::str::Utf8Error)
}

impl From<io::Error> for CreateError { // Automatic Conversion
    fn from(err: io::Error) -> CreateError {
        CreateError::Io(err)
    }
}

impl From<serde_json::Error> for CreateError { // Automatic Conversion
    fn from(err: serde_json::Error) -> CreateError {
        CreateError::InvalidJson(err)
    }
}

impl From<std::str::Utf8Error> for CreateError { // Automatic Conversion
    fn from(err: std::str::Utf8Error) -> CreateError {
        CreateError::Utf8Error(err)
    }
}

pub fn create_indices_csv(folder:&str, csv_path: &str, indices:&str) -> Result<(), CreateError>{

    fs::create_dir_all(folder)?;
    // let indices_json:Result<Vec<CreateIndex>> = serde_json::from_str(indices);
    // println!("{:?}", indices_json);
    let indices_json:Vec<CreateIndex> = serde_json::from_str(indices)?;
    let mut meta_data = persistence::MetaData {id_lists: FnvHashMap::default()};
    for el in indices_json {
        match el {
            CreateIndex::Fulltext{ fulltext: attr_name, options, attr_pos } =>{
                create_fulltext_index_csv(csv_path, &folder, &attr_name, attr_pos.unwrap(), options.unwrap_or(Default::default()), &mut meta_data)?
             },
            CreateIndex::Boost{ boost: path, options } => {} // @Temporary
        }
    }

    let json = create_json_from_c_s_v(csv_path);
    write_json_to_disk(&json, folder, "data", &mut meta_data)?;

    let meta_data_str = serde_json::to_string_pretty(&meta_data).unwrap();
    let mut buffer = File::create(&get_file_path(folder, "metaData", ""))?;
    buffer.write_all(&meta_data_str.as_bytes())?;

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



fn write_json_to_disk(arro: &Vec<Value>, folder: &str, path:&str,mut meta_data: &mut persistence::MetaData) -> Result<(), io::Error> {
    let mut offsets = vec![];
    let mut buffer = File::create(&get_file_path(folder, &path, ""))?;
    let mut current_offset = 0;
    // let arro = data.as_array().unwrap();
    for el in arro {
        let el_str = el.to_string().into_bytes();
        buffer.write_all(&el_str)?;
        offsets.push(current_offset as u64);
        current_offset += el_str.len();
    }
    // println!("json offsets: {:?}", offsets);
    persistence::write_index64(&offsets, &get_file_path(folder, &path, ".offsets"), &mut meta_data)?;
    Ok(())
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

