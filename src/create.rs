
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


fn forEachElementInPath<F>(data2: Value, path2:&str, mut fun: F)
where F: FnMut(&Value, u32, u32) { // value, mainId, subObjId

    let path = util::removeArrayMarker(path2);
    let paths = path.split(".").collect::<Vec<_>>();
    
    let lastPath = paths.last().unwrap();

    let mut valueId = 0; // TODO get Current valueId
    if data2.is_array(){
        let data = data2.as_array().unwrap();

        let mainId = 0; // TODO get Current MainID

        for entry in data {
            let mut currentEl = entry;

            for mut i in 0..(paths.len()-1) {
                let mut comp = paths[i];
                if !currentEl.get(comp).is_some() {break;}
                currentEl = &currentEl[comp];
                if currentEl.is_array(){
                    let currentElArr = currentEl.as_array().unwrap();
                    if lastPath == &comp{
                        for el in currentElArr {
                            fun(el, mainId, valueId);
                        }
                    }else{
                        i+=1;
                        comp = paths[i];
                        for subarrEl in currentElArr {
                            if lastPath == &comp && subarrEl.get(comp).is_some(){
                                fun(&subarrEl[comp], mainId, valueId);
                            }else{
                                // throw new Error('level 3 not supported')
                            }
                            valueId+=1;
                        }
                    }
                }else{
                    if lastPath == &comp{
                        fun(currentEl, mainId, valueId);
                    }
                }
            }
        }

    }
}

pub fn getAllterms(data:String,  path:&str, options:CreateIndexOptions){


}


pub fn createFulltextIndex(data:String, path:&str, options:CreateIndexOptions){

    // let dat2 = r#" { "name": "John Doe", "age": 43, ... } "#;
    let v: Value = serde_json::from_str(&data).unwrap();

    let allTerms = getAllterms(data, path, options);

    let paths = util::getStepsToAnchor(path);

    let lastPath = (paths.iter().last().unwrap()).clone();

    for pathToAnchor in paths {
        let level = util::getLevel(&pathToAnchor);
        // let tuples = vec![];

        let isLast = pathToAnchor == lastPath;

    }

}