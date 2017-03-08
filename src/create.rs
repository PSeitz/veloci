
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufRead};
// use std::io::Error;
use std::path::Path;
use std::char;
use std::cmp;
use std::mem;
use std::time::Duration;

use futures_cpupool::CpuPool;
use tokio_timer::Timer;

use futures::{Poll, Future, Sink};
use futures::executor;
use futures::future::{ok, err};
use futures::stream::{iter, Peekable, BoxStream, channel, Stream};
use futures::sync::oneshot;
use futures::sync::mpsc;
use std::str;
use std::thread;
use std::fmt;
use std::sync::mpsc::sync_channel;
use std::fs;

// use std::os::windows::fs::FileExt;
use std::io::SeekFrom;
use std::collections::HashMap;
use util;
use std::collections::hash_map::Entry;
use fnv::FnvHashMap;


//-----
use serde_json;
use serde_json::Value;


pub struct CreateIndexOptions<'a> {
    tokenize: bool,
    stopwords: Vec<&'a str>
}

struct ForEachOpt {
    parentPosInPath: u32,
    currentParentIdCounter: u32,
    valueIdCounter: u32,
    path: String,
}

fn walk<F>(currentEl: &Value, startPos: u32, opt: &mut ForEachOpt, paths:&Vec<&str>, cb: &mut F)
where F: FnMut(&str, u32, u32) {

    for i in startPos..(paths.len() as u32) {
        let isLastPath = i == paths.len() as u32-1;
        let isParentPathPos = (i == opt.parentPosInPath && i!=0);
        let mut comp = paths[i as usize];
        if !currentEl.get(comp).is_some() {break;}
        let nextEl = &currentEl[comp];

        if nextEl.is_array(){
            let currentElArr = nextEl.as_array().unwrap();
            if isLastPath{
                for el in currentElArr {
                    cb(el.as_str().unwrap(), opt.valueIdCounter, opt.currentParentIdCounter);
                    opt.valueIdCounter+=1;
                }
            }else{
                let nextLevel = i+1;
                for subarrEl in currentElArr {
                    walk(subarrEl, nextLevel, opt, paths, cb);
                    if (isParentPathPos){opt.currentParentIdCounter += 1;}
                }
            }
        }else{
            if isLastPath{
                cb(nextEl.as_str().unwrap(), opt.valueIdCounter, opt.currentParentIdCounter);
                opt.valueIdCounter+=1;
            }
        }

    }
}


fn forEachElementInPath<F>(data: &Value, opt: &mut ForEachOpt, path2:&str, cb: &mut F)
where F: FnMut(&str, u32, u32) { // value, valueId, parentValId   // TODO ADD Template for Value

    let path = util::removeArrayMarker(path2);
    let paths = path.split(".").collect::<Vec<_>>();
    println!("JAAAA:: {:?}", paths);
    

    if data.is_array(){
        // let startMainId = parentPosInPath == 0 ? currentParentIdCounter : 0
        for el in data.as_array().unwrap() {
            walk(el, 0, opt, &paths, cb);
            if (opt.parentPosInPath == 0) {opt.currentParentIdCounter += 1;}
        }
    }else{
        walk(data, 0, opt, &paths, cb);
    }
}

use fnv::FnvHashSet;

pub fn getAllterms(data:&Value, path:&str, options:&CreateIndexOptions) -> Vec<String>{

    let mut terms:FnvHashSet<String> = FnvHashSet::default();

    let mut opt = ForEachOpt {
        parentPosInPath: 0,
        currentParentIdCounter: 0,
        valueIdCounter: 0,
        path: path.to_string(),
    };


    forEachElementInPath(&data, &mut opt, &path,  &mut |value: &str, valueId: u32, parentValId: u32| {
        let normalizedText = util::normalizeText(value);
        if options.stopwords.contains(&(&normalizedText as &str)) {
            return;
        }

        // if stopwords.map_or(false, |ref v| v.contains(&value)){
        //     return;
        // }
        terms.insert(normalizedText.clone());
        if (options.tokenize && normalizedText.split(" ").count() > 1) {
            for token in normalizedText.split(" ") {
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
    parentValId:u32
}

use std::sync::{Arc, Mutex};
use std::cmp::Ordering;

pub fn createFulltextIndex(dataStr:&str, path:&str, options:CreateIndexOptions) -> Result<(), io::Error> {

    // let dat2 = r#" { "name": "John Doe", "age": 43, ... } "#;
    let data: Value = serde_json::from_str(dataStr).unwrap();

    let allTerms = getAllterms(&data, path, &options);

    let paths = util::getStepsToAnchor(path);

    for i in 0..(paths.len() - 1) {

        let level = util::getLevel(&paths[i]);
        let mut tuples:Vec<Tuple> = vec![];
        let mut tokens:Vec<Tuple> = vec![];

        let isTextIndex = (i == (paths.len() -1));

        let mut opt = ForEachOpt {
            parentPosInPath: level-1,
            currentParentIdCounter: 0,
            valueIdCounter: 0,
            path: paths[i].clone(),
        };

        if isTextIndex {
            forEachElementInPath(&data, &mut opt, &paths[i], &mut |value: &str, valueId: u32, parentValId: u32| {
                let normalizedText = util::normalizeText(value);
                // if isInStopWords(normalizedText, options) continue/return

                let valId = allTerms.binary_search(&value.to_string()).unwrap();
                tuples.push(Tuple{valid:valId as u32, parentValId:valueId});
                if (options.tokenize && normalizedText.split(" ").count() > 1) {
                    for token in normalizedText.split(" ") {
                        if options.stopwords.contains(&token) { continue; }
                        // terms.insert(token.to_string());
                        let valId = allTerms.binary_search(&token.to_string()).unwrap();
                        tokens.push(Tuple{valid:valId as u32, parentValId:valueId});
                    }
                }

            });
        }else{
            let mut callback = |value: &str, valueId: u32, parentValId: u32| {
                tuples.push(Tuple{valid:valueId, parentValId:parentValId});
            };

            forEachElementInPath(&data, &mut opt, &paths[i], &mut callback);

        }

        tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
        let pathName = util::getPathName(&paths[i], isTextIndex);
        util::write_index(&tuples.iter().map(|ref el| el.valid      ).collect::<Vec<_>>(), &(pathName.to_string()+".valueIdToParent.valIds"));
        util::write_index(&tuples.iter().map(|ref el| el.parentValId).collect::<Vec<_>>(), &(pathName.to_string()+".valueIdToParent.mainIds"));

        if (tokens.len() > 0) {
            tokens.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
            util::write_index(&tokens.iter().map(|ref el| el.valid      ).collect::<Vec<_>>(), &(path.to_string()+".tokens.tokenValIds"));
            util::write_index(&tokens.iter().map(|ref el| el.parentValId).collect::<Vec<_>>(), &(path.to_string()+".tokens.parentValId"));
        }

    }

    File::create(path)?.write_all(allTerms.join("\n").as_bytes());
    util::write_index(&allTerms.iter().map(|ref el| el.len() as u32).collect::<Vec<_>>(), &(path.to_string()+".length"));
    creatCharOffsets(allTerms, path);
    Ok(())

}

struct CharWithOffset {
    char: String,
    byteOffsetStart: usize
}

struct CharData {
    lineNum: usize,
    byteOffsetStart: usize
}

struct CharDataComplete {
    suffix:String,
    lineNum: usize,
    byteOffsetStart: usize,
    byteOffsetEnd: usize
}

pub fn creatCharOffsets(data:Vec<String>, path:&str){

    // let mut terms:FnvHashSet<String> = FnvHashSet::default();
    let mut charToOffset:FnvHashMap<String, CharData> = FnvHashMap::default();

    let mut currentByteOffset = 0;
    let mut lineNum = 0;
    for text in data {
        let char1 = text.chars().nth(0).map_or("".to_string(), |c| c.to_string());
        let char12 = char1.clone() + &text.chars().nth(1).map_or("".to_string(), |c| c.to_string());

        if !charToOffset.contains_key(&char1) {
            charToOffset.insert(char1, CharData{byteOffsetStart:currentByteOffset, lineNum:lineNum});
        }

        if !charToOffset.contains_key(&char12) {
            charToOffset.insert(char12, CharData{byteOffsetStart:currentByteOffset, lineNum:lineNum});
        }

        currentByteOffset += text.len() + 1;
        lineNum+=1;
    }

    let mut charOffsets:Vec<CharDataComplete> = charToOffset.iter().map(|(suffix, charData)| 
        CharDataComplete{suffix:suffix.to_string(), lineNum:charData.lineNum, byteOffsetStart:charData.byteOffsetStart, byteOffsetEnd:0}
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
    // let byteOffset = 0, lineNum = 0, currentChar, currentTwoChar
    // rl.on('line', (line) => {
    //     let firstCharOfLine = line.charAt(0)
    //     let firstTwoCharOfLine = line.charAt(0) + line.charAt(1)
    //     if(currentChar != firstCharOfLine){
    //         currentChar = firstCharOfLine
    //         if(currentSingleChar) currentSingleChar.byteOffsetEnd = byteOffset
    //         currentSingleChar = {char: currentChar, byteOffsetStart:byteOffset, lineOffset:lineNum}
    //         offsets.push(currentSingleChar)
    //         console.log(`${currentChar} ${byteOffset} ${lineNum}`)
    //     }
    //     if(currentTwoChar != firstTwoCharOfLine){
    //         currentTwoChar = firstTwoCharOfLine
    //         if(currentSecondChar) currentSecondChar.byteOffsetEnd = byteOffset
    //         currentSecondChar = {char: currentTwoChar, byteOffsetStart:byteOffset, lineOffset:lineNum}
    //         offsets.push(currentSecondChar)
    //         console.log(`${currentTwoChar} ${byteOffset} ${lineNum}`)
    //     }
    //     byteOffset+= Buffer.byteLength(line, 'utf8') + 1 // linebreak = 1
    //     lineNum++
    // }).on('close', () => {
    //     if(currentSingleChar) currentSingleChar.byteOffsetEnd = byteOffset
    //     if(currentSecondChar) currentSecondChar.byteOffsetEnd = byteOffset
    //     writeFileSync(path+'.charOffsets.chars', JSON.stringify(offsets.map(offset=>offset.char)))
    //     writeFileSync(path+'.charOffsets.byteOffsetsStart',     new Buffer(new Uint32Array(offsets.map(offset=>offset.byteOffsetStart)).buffer))
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