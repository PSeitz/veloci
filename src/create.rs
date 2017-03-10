
use std::fs::File;
use std::io::prelude::*;
#[allow(unused_imports)]
use std::io::{self, BufRead};
// use std::io::Error;
#[allow(unused_imports)]
use std::path::Path;
#[allow(unused_imports)]
use std::char;
#[allow(unused_imports)]
use std::cmp;
#[allow(unused_imports)]
use std::mem;
#[allow(unused_imports)]
use std::time::Duration;

#[allow(unused_imports)]
use futures_cpupool::CpuPool;
#[allow(unused_imports)]
use tokio_timer::Timer;

#[allow(unused_imports)]
use futures::{Poll, Future, Sink};
#[allow(unused_imports)]
use futures::executor;
#[allow(unused_imports)]
use futures::future::{ok, err};
#[allow(unused_imports)]
use futures::stream::{iter, Peekable, BoxStream, channel, Stream};
#[allow(unused_imports)]
use futures::sync::oneshot;
#[allow(unused_imports)]
use futures::sync::mpsc;
#[allow(unused_imports)]
use std::str;
#[allow(unused_imports)]
use std::thread;
#[allow(unused_imports)]
use std::fmt;
#[allow(unused_imports)]
use std::sync::mpsc::sync_channel;
#[allow(unused_imports)]
use std::fs;

#[allow(unused_imports)]
use std::io::SeekFrom;
#[allow(unused_imports)]
use std::collections::HashMap;
use util;
#[allow(unused_imports)]
use std::collections::hash_map::Entry;
use fnv::FnvHashMap;

use fnv::FnvHashSet;

#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::cmp::Ordering;

//-----
use serde_json;
use serde_json::Value;


pub struct CreateIndexOptions<'a> {
    tokenize: bool,
    stopwords: Vec<&'a str>
}

struct ForEachOpt {
    parent_pos_in_path: u32,
    current_parent_id_counter: u32,
    value_id_counter: u32,
    path: String,
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
        value_id_counter: 0,
        path: path.to_string(),
    };

    for_each_element_in_path(&data, &mut opt, &path,  &mut |value: &str, value_id: u32, parent_val_id: u32| {
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


// fn getValueID(data, value){
//     return binarySearch(data, value)
// }

// fn isInStopWords(term:&str, stopwords:&Vec<&str>) -> bool{
//     stopwords.contains(&term)
//     // return stopwords.indexOf(term) >= 0
// }

// #[derive(Debug)]
struct Tuple {
    valid: u32,
    parent_val_id:u32
}



pub fn createFulltextIndex(data_str:&str, path:&str, options:CreateIndexOptions) -> Result<(), io::Error> {

    // let dat2 = r#" { "name": "John Doe", "age": 43, ... } "#;
    let data: Value = serde_json::from_str(data_str).unwrap();

    let all_terms = get_allterms(&data, path, &options);

    let paths = util::getStepsToAnchor(path);

    for i in 0..(paths.len() - 1) {

        let level = util::getLevel(&paths[i]);
        let mut tuples:Vec<Tuple> = vec![];
        let mut tokens:Vec<Tuple> = vec![];

        let isTextIndex = (i == (paths.len() -1));

        let mut opt = ForEachOpt {
            parent_pos_in_path: level-1,
            current_parent_id_counter: 0,
            value_id_counter: 0,
            path: paths[i].clone(),
        };

        if isTextIndex {
            for_each_element_in_path(&data, &mut opt, &paths[i], &mut |value: &str, value_id: u32, parent_val_id: u32| {
                let normalized_text = util::normalize_text(value);
                // if isInStopWords(normalized_text, options) continue/return

                let valId = all_terms.binary_search(&value.to_string()).unwrap();
                tuples.push(Tuple{valid:valId as u32, parent_val_id:value_id});
                if (options.tokenize && normalized_text.split(" ").count() > 1) {
                    for token in normalized_text.split(" ") {
                        if options.stopwords.contains(&token) { continue; }
                        // terms.insert(token.to_string());
                        let valId = all_terms.binary_search(&token.to_string()).unwrap();
                        tokens.push(Tuple{valid:valId as u32, parent_val_id:value_id});
                    }
                }

            });
        }else{
            let mut callback = |value: &str, value_id: u32, parent_val_id: u32| {
                tuples.push(Tuple{valid:value_id, parent_val_id:parent_val_id});
            };

            for_each_element_in_path(&data, &mut opt, &paths[i], &mut callback);

        }

        tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
        let pathName = util::getPathName(&paths[i], isTextIndex);
        util::write_index(&tuples.iter().map(|ref el| el.valid      ).collect::<Vec<_>>(), &(pathName.to_string()+".valueIdToParent.valIds"));
        util::write_index(&tuples.iter().map(|ref el| el.parent_val_id).collect::<Vec<_>>(), &(pathName.to_string()+".valueIdToParent.mainIds"));

        if (tokens.len() > 0) {
            tokens.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
            util::write_index(&tokens.iter().map(|ref el| el.valid      ).collect::<Vec<_>>(), &(path.to_string()+".tokens.tokenValIds"));
            util::write_index(&tokens.iter().map(|ref el| el.parent_val_id).collect::<Vec<_>>(), &(path.to_string()+".tokens.parent_val_id"));
        }

    }

    File::create(path)?.write_all(all_terms.join("\n").as_bytes());
    util::write_index(&all_terms.iter().map(|ref el| el.len() as u32).collect::<Vec<_>>(), &(path.to_string()+".length"));
    creatCharOffsets(all_terms, path);
    Ok(())

}

struct CharWithOffset {
    char: String,
    byte_offset_start: usize
}

struct CharData {
    line_num: usize,
    byte_offset_start: usize
}

struct CharDataComplete {
    suffix:String,
    line_num: usize,
    byte_offset_start: usize,
    byteOffsetEnd: usize
}

pub fn creatCharOffsets(data:Vec<String>, path:&str){

    // let mut terms:FnvHashSet<String> = FnvHashSet::default();
    let mut charToOffset:FnvHashMap<String, CharData> = FnvHashMap::default();

    let mut currentByteOffset = 0;
    let mut line_num = 0;
    for text in data {
        let char1 = text.chars().nth(0).map_or("".to_string(), |c| c.to_string());
        let char12 = char1.clone() + &text.chars().nth(1).map_or("".to_string(), |c| c.to_string());

        if !charToOffset.contains_key(&char1) {
            charToOffset.insert(char1, CharData{byte_offset_start:currentByteOffset, line_num:line_num});
        }

        if !charToOffset.contains_key(&char12) {
            charToOffset.insert(char12, CharData{byte_offset_start:currentByteOffset, line_num:line_num});
        }

        currentByteOffset += text.len() + 1;
        line_num+=1;
    }

    let mut charOffsets:Vec<CharDataComplete> = charToOffset.iter().map(|(suffix, charData)| 
        CharDataComplete{suffix:suffix.to_string(), line_num:charData.line_num, byte_offset_start:charData.byte_offset_start, byteOffsetEnd:0}
    ).collect();
    charOffsets.sort_by(|a, b| a.suffix.partial_cmp(&b.suffix).unwrap_or(Ordering::Equal));

    // for ref mut charOffset in charOffsets {
    //     charOffset.byteOffsetEnd = 10;

    // }


    for (i,ref mut charOffset) in charOffsets.iter().enumerate() {
        let forwardLookNextEl = charOffsets.iter().skip(i).find(|&r| r.suffix.len() == charOffset.suffix.len());

        // let el = forwardLookIter.unwrap();
        // charOffset.byteOffsetEnd = 10;

    }

       // to_string() == "two"

    // res

    // let offsets = []

    // let currentSingleChar, currentSecondChar
    // let byteOffset = 0, line_num = 0, currentChar, currentTwoChar
    // rl.on('line', (line) => {
    //     let firstCharOfLine = line.charAt(0)
    //     let firstTwoCharOfLine = line.charAt(0) + line.charAt(1)
    //     if(currentChar != firstCharOfLine){
    //         currentChar = firstCharOfLine
    //         if(currentSingleChar) currentSingleChar.byteOffsetEnd = byteOffset
    //         currentSingleChar = {char: currentChar, byte_offset_start:byteOffset, lineOffset:line_num}
    //         offsets.push(currentSingleChar)
    //         console.log(`${currentChar} ${byteOffset} ${line_num}`)
    //     }
    //     if(currentTwoChar != firstTwoCharOfLine){
    //         currentTwoChar = firstTwoCharOfLine
    //         if(currentSecondChar) currentSecondChar.byteOffsetEnd = byteOffset
    //         currentSecondChar = {char: currentTwoChar, byte_offset_start:byteOffset, lineOffset:line_num}
    //         offsets.push(currentSecondChar)
    //         console.log(`${currentTwoChar} ${byteOffset} ${line_num}`)
    //     }
    //     byteOffset+= Buffer.byteLength(line, 'utf8') + 1 // linebreak = 1
    //     line_num++
    // }).on('close', () => {
    //     if(currentSingleChar) currentSingleChar.byteOffsetEnd = byteOffset
    //     if(currentSecondChar) currentSecondChar.byteOffsetEnd = byteOffset
    //     writeFileSync(path+'.charOffsets.chars', JSON.stringify(offsets.map(offset=>offset.char)))
    //     writeFileSync(path+'.charOffsets.byteOffsetsStart',     new Buffer(new Uint32Array(offsets.map(offset=>offset.byte_offset_start)).buffer))
    //     writeFileSync(path+'.charOffsets.byteOffsetsEnd',  new Buffer(new Uint32Array(offsets.map(offset=>offset.byteOffsetEnd)).buffer))
    //     writeFileSync(path+'.charOffsets.lineOffset',  new Buffer(new Uint32Array(offsets.map(offset=>offset.lineOffset)).buffer))
    //     resolve()
    // })
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

        let dat2 = r#" [{ "name": "John Doe", "age": 43 }] "#;

        let res = create::createFulltextIndex(dat2, "name", opt);
        assert_eq!("Hello", "Hello");

    }
}