
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

pub struct CreateIndexOptions {
    tokenize: bool,
    firstCharExactMatch: bool
}

struct ForEachOpt {
    parentPosInPath: u32,
    currentParentIdCounter: u32,
    valueIdCounter: u32,
    path: String,
}

fn walk<F>(currentEl: &Value, startPos: u32, opt: &mut ForEachOpt, paths:&Vec<&str>, cb: &mut F)
where F: FnMut(&str, u32, u32) {

    for i in startPos..(paths.len() as u32 - 1) {
        let isLastPath = i == paths.len() as u32-1;
        let isParentPathPos = (i == opt.parentPosInPath && i!=0);
        let mut comp = paths[i as usize];

        if !currentEl.get(comp).is_some() {break;}

        if currentEl.is_array(){
            let currentElArr = currentEl.as_array().unwrap();
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
                cb(currentEl.as_str().unwrap(), opt.valueIdCounter, opt.currentParentIdCounter);
                opt.valueIdCounter+=1;
            }
        }

    }
}

fn forEachElementInPath<F>(data: &Value, opt: &mut ForEachOpt, path2:&str, cb: &mut F)
where F: FnMut(&str, u32, u32) { // value, valueId, parentValId   // TODO ADD Template for Value

    let path = util::removeArrayMarker(path2);
    let paths = path.split(".").collect::<Vec<_>>();
    
    walk(data, 0, opt, &paths, cb);
}

pub fn getAllterms(data:&Value, path:&str, options:CreateIndexOptions) -> Vec<String>{

    vec![]
}


// fn getValueID(data, value){
//     return binarySearch(data, value)
// }

fn isInStopWords(term:&str, stopwords:&Vec<&str>){
    // return stopwords.indexOf(term) >= 0
}

// #[derive(Debug)]
struct Tuple {
    valid: u32,
    parentValId:u32
}

use std::sync::{Arc, Mutex};
use std::cmp::Ordering;

pub fn createFulltextIndex(dataStr:String, path:&str, options:CreateIndexOptions){

    // let dat2 = r#" { "name": "John Doe", "age": 43, ... } "#;
    let data: Value = serde_json::from_str(&dataStr).unwrap();

    let allTerms = getAllterms(&data, path, options);

    let paths = util::getStepsToAnchor(path);

    let lastPath = (paths.iter().last().unwrap()).clone();


    for i in 0..(paths.len() - 1) {

        let level = util::getLevel(&paths[i]);
        let mut tuples:Vec<Tuple> = vec![];

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

                let valId = allTerms.binary_search(&value.to_owned()).unwrap();
                tuples.push(Tuple{valid:valId as u32, parentValId:valueId});
                // if (options.tokenize && normalizedText.split(' ').length > 1) 
                //     forEachToken(normalizedText, token => {if(!isInStopWords(normalizedText, options)) tokens.push([getValueID(allTerms, token), valId])})

            });
        }else{
            let mut callback = |value: &str, valueId: u32, parentValId: u32| {
                tuples.push(Tuple{valid:valueId, parentValId:parentValId});
            };

            forEachElementInPath(&data, &mut opt, &paths[i], &mut callback);

        }

        tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
        let pathName = util::getPathName(&paths[i], isTextIndex);
        util::write_index(&tuples.iter().map(|ref el| el.valid).collect::<Vec<_>>(), &(pathName.clone()+".valueIdToParent.valIds"));
        util::write_index(&tuples.iter().map(|ref el| el.parentValId).collect::<Vec<_>>(), &(pathName+".valueIdToParent.mainIds"));

    }

}