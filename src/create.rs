
use std::fs::File;
use std::io::prelude::*;
#[allow(unused_imports)]
use std::io::{self, BufRead};
#[allow(unused_imports)]
use std::time::Duration;

#[allow(unused_imports)]
use futures_cpupool::CpuPool;

#[allow(unused_imports)]
use futures::{Poll, Future, Sink, executor};
#[allow(unused_imports)]
use futures::future::{ok, err};
#[allow(unused_imports)]
use futures::stream::{iter, Peekable, BoxStream, Stream};
#[allow(unused_imports)]
use futures::sync::{oneshot, mpsc};
#[allow(unused_imports)]
use std::thread;
#[allow(unused_imports)]
use std::sync::mpsc::sync_channel;

#[allow(unused_imports)]
use std::io::SeekFrom;
use util;
use fnv::FnvHashSet;

#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::cmp::Ordering;

use serde_json;
use serde_json::Value;


pub struct CreateIndexOptions<'a> {
    tokenize: bool,
    stopwords: Vec<&'a str>
}

struct ForEachOpt {
    parent_pos_in_path: u32,
    current_parent_id_counter: u32,
    value_id_counter: u32
}

fn walk<F>(current_el: &Value, start_pos: u32, opt: &mut ForEachOpt, paths:&Vec<&str>, cb: &mut F)
where F: FnMut(&str, u32, u32) {

    for i in start_pos..(paths.len() as u32) {
        let is_last_path = i == paths.len() as u32-1;
        let is_parent_path_pos = i == opt.parent_pos_in_path && i!=0;
        let comp = paths[i as usize];
        if !current_el.get(comp).is_some() {break;}
        let next_el = &current_el[comp];

        if next_el.is_array(){
            let current_el_arr = next_el.as_array().unwrap();
            if is_last_path{
                for el in current_el_arr {
                    cb(el.as_str().unwrap(), opt.value_id_counter, opt.current_parent_id_counter);
                    opt.value_id_counter+=1;
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
                cb(next_el.as_str().unwrap(), opt.value_id_counter, opt.current_parent_id_counter);
                opt.value_id_counter+=1;
            }
        }

    }
}


fn for_each_element_in_path<F>(data: &Value, opt: &mut ForEachOpt, path2:&str, cb: &mut F)
where F: FnMut(&str, u32, u32) { // value, value_id, parent_val_id   // TODO ADD Template for Value

    let path = util::remove_array_marker(path2);
    let paths = path.split(".").collect::<Vec<_>>();
    println!("JAAAA:: {:?}", paths);

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



pub fn get_allterms(data:&Value, path:&str, options:&CreateIndexOptions) -> Vec<String>{

    let mut terms:FnvHashSet<String> = FnvHashSet::default();

    let mut opt = ForEachOpt {
        parent_pos_in_path: 0,
        current_parent_id_counter: 0,
        value_id_counter: 0
    };

    for_each_element_in_path(&data, &mut opt, &path,  &mut |value: &str, _value_id: u32, _parent_val_id: u32| {
        let normalized_text = util::normalize_text(value);
        if options.stopwords.contains(&(&normalized_text as &str)) {
            return;
        }

        // if stopwords.map_or(false, |ref v| v.contains(&value)){
        //     return;
        // }
        terms.insert(normalized_text.clone());
        if options.tokenize && normalized_text.split(" ").count() > 1 {
            for token in normalized_text.split(" ") {
                if options.stopwords.contains(&token) { continue; }
                terms.insert(token.to_string());
            }
        }
    });

    let mut v: Vec<String> = terms.into_iter().collect::<Vec<String>>();
    v.sort();
    v
}


// #[derive(Debug)]
struct ValIdPair {
    valid: u32,
    parent_val_id:u32
}


pub fn create_fulltext_index(data_str:&str, path:&str, options:CreateIndexOptions) -> Result<(), io::Error> {

    let data: Value = serde_json::from_str(data_str).unwrap();
    let all_terms = get_allterms(&data, path, &options);

    let paths = util::get_steps_to_anchor(path);

    for i in 0..(paths.len() - 1) {

        let level = util::get_level(&paths[i]);
        let mut tuples:Vec<ValIdPair> = vec![];
        let mut tokens:Vec<ValIdPair> = vec![];

        let is_text_index = i == (paths.len() -1);

        let mut opt = ForEachOpt {
            parent_pos_in_path: level-1,
            current_parent_id_counter: 0,
            value_id_counter: 0
        };

        if is_text_index {
            for_each_element_in_path(&data, &mut opt, &paths[i], &mut |value: &str, value_id: u32, _parent_val_id: u32| {
                let normalized_text = util::normalize_text(value);
                if options.stopwords.contains(&(&normalized_text as &str)) { return; }
                // if isInStopWords(normalized_text, options) continue/return

                let val_id = all_terms.binary_search(&value.to_string()).unwrap();
                tuples.push(ValIdPair{valid:val_id as u32, parent_val_id:value_id});
                if options.tokenize && normalized_text.split(" ").count() > 1 {
                    for token in normalized_text.split(" ") {
                        if options.stopwords.contains(&token) { continue; }
                        // terms.insert(token.to_string());
                        let val_id = all_terms.binary_search(&token.to_string()).unwrap();
                        tokens.push(ValIdPair{valid:val_id as u32, parent_val_id:value_id});
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
        util::write_index(&tuples.iter().map(|ref el| el.valid      ).collect::<Vec<_>>(), &(path_name.to_string()+".valueIdToParent.val_ids"))?;
        util::write_index(&tuples.iter().map(|ref el| el.parent_val_id).collect::<Vec<_>>(), &(path_name.to_string()+".valueIdToParent.mainIds"))?;

        if tokens.len() > 0 {
            tokens.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
            util::write_index(&tokens.iter().map(|ref el| el.valid      ).collect::<Vec<_>>(), &(path.to_string()+".tokens.tokenValIds"))?;
            util::write_index(&tokens.iter().map(|ref el| el.parent_val_id).collect::<Vec<_>>(), &(path.to_string()+".tokens.parent_val_id"))?;
        }

    }

    File::create(path)?.write_all(all_terms.join("\n").as_bytes())?;
    util::write_index(&all_terms.iter().map(|ref el| el.len() as u32).collect::<Vec<_>>(), &(path.to_string()+".length"))?;
    create_char_offsets(all_terms, path)?;
    Ok(())

}

#[derive(Debug, Clone)]
struct CharData {
    suffix:String,
    line_num: u32,
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
    line_num: u32,
    byte_offset_start: u64,
    byte_offset_end: u64
}

pub fn create_char_offsets(data:Vec<String>, path:&str) -> Result<(), io::Error> {

    let mut char_offsets:Vec<CharData> = vec![];

    let mut current_byte_offset = 0;
    let mut line_num = 0;
    for text in data {
        let char1 = text.chars().nth(0).map_or("".to_string(), |c| c.to_string());
        let char12 = char1.clone() + &text.chars().nth(1).map_or("".to_string(), |c| c.to_string());

        if !char_offsets.iter().any(|ref x| x.suffix == char1) {
            char_offsets.push(CharData{suffix:char1, byte_offset_start:current_byte_offset, line_num:line_num});
        }

        if !char_offsets.iter().any(|ref x| x.suffix == char12) {
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

    util::write_index64(&char_offsets_complete.iter().map(|ref el| el.byte_offset_start).collect::<Vec<_>>(), &(path.to_string()+".char_offsets.byteOffsetsStart"))?;
    util::write_index64(&char_offsets_complete.iter().map(|ref el| el.byte_offset_end  ).collect::<Vec<_>>(), &(path.to_string()+".char_offsets.byteOffsetsEnd"))?;
    util::write_index(&char_offsets_complete.iter().map(|ref el| el.line_num         ).collect::<Vec<_>>(), &(path.to_string()+".char_offsets.lineOffset"))?;

    File::create(path)?.write_all(&char_offsets_complete.iter().map(|ref el| el.suffix.to_string()).collect::<Vec<_>>().join("\n").as_bytes())?;

    Ok(())
}



#[cfg(test)]
mod test {
    use create;
    #[test]
    fn test_eq() {
        let opt = create::CreateIndexOptions{
            tokenize: true,
            stopwords: vec![]
        };

        let dat2 = r#" [{ "name": "John Doe", "age": 43 }, { "name": "Jaa", "age": 43 }] "#;

        create::create_fulltext_index(dat2, "name", opt);
        assert_eq!("Hello", "Hello");

    }
}