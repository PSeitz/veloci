#![feature(test)]
#![feature(collection_placement)]
#![feature(placement_in_syntax)]
#![feature(box_syntax, box_patterns)]
#![cfg_attr(feature= "unstable", feature(alloc, heap_api, repr_simd))]

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate serde_json;

extern crate serde;
extern crate rand;
// extern crate tokio_timer;
extern crate regex;
extern crate fnv;
extern crate fst;

#[macro_use] extern crate log;
extern crate env_logger;

// extern crate abomonation;
extern crate csv;

extern crate test;

extern crate bit_set;
extern crate bit_vec;

extern crate num;

extern crate hyper;
extern crate iron;
extern crate bodyparser;
extern crate router;
extern crate time;
extern crate snap;

extern crate bincode;

#[macro_use]
extern crate measure_time;

extern crate heapsize;

extern crate byteorder;

// use fst::{IntoStreamer, Streamer, Levenshtein, Set, MapBuilder};
#[allow(unused_imports)]
use fst::{IntoStreamer, Levenshtein, Set, MapBuilder};
use std::fs::File;
use std::io::prelude::*;
#[allow(unused_imports)]
use std::io::{self, BufRead};
#[allow(unused_imports)]
use fnv::FnvHashSet;
#[allow(unused_imports)]
use std::collections::HashSet;
#[allow(unused_imports)]
use std::collections::HashMap;
#[allow(unused_imports)]
use fnv::FnvHashMap;

use std::time::Instant;

#[macro_use] extern crate lazy_static;


// extern crate rustc_serialize;

#[macro_use]
pub mod util;
pub mod search;
pub mod create;
pub mod doc_loader;
pub mod persistence;
pub mod persistence_data;
pub mod search_field;
pub mod expression;
pub mod bucket_list;
mod server;

#[cfg(test)]
mod tests;

use std::str;

#[allow(dead_code)]
fn create_thalia_index() {
    // let all_terms = loadcsv("./data.csv", 0);
    // println!("{:?}", all_terms.len());

    // File::create("MATNR").unwrap().write_all(all_terms.join("\n").as_bytes()).unwrap();
    let indices = r#"
    [
        { "fulltext":"MATNR", "attr_pos" : 0 },
        { "fulltext":"ISMTITLE", "attr_pos" : 1, "options":{"tokenize":true}},
        { "fulltext":"ISMORIGTITLE", "attr_pos" : 2, "options":{"tokenize":true}},
        { "fulltext":"ISMSUBTITLE1", "attr_pos" : 3, "options":{"tokenize":true}},
        { "fulltext":"ISMSUBTITLE2", "attr_pos" : 4, "options":{"tokenize":true}},
        { "fulltext":"ISMSUBTITLE3", "attr_pos" : 5, "options":{"tokenize":true}},
        { "fulltext":"ISMARTIST", "attr_pos" : 6, "options":{"tokenize":true}},
        { "fulltext":"ISMLANGUAGES", "attr_pos" : 7},
        { "fulltext":"ISMPUBLDATE", "attr_pos" : 8},
        { "fulltext":"EAN11", "attr_pos" : 9},
        { "fulltext":"ISMORIDCODE", "attr_pos" : 10}
    ]
    "#;

    // let indices = r#"
    // [
    //     { "fulltext":"MATNR", "attr_pos" : 0 },
    //     { "fulltext":"ISMTITLE", "attr_pos" : 1, "options":{"tokenize":true}}
    // ]
    // "#;

    // let indices = r#"
    // [
    //     { "fulltext":"MATNR", "attr_pos" : 0 , "options":{"tokenize":true}},
    //     { "fulltext":"ISMTITLE", "attr_pos" : 1, "options":{"tokenize":true}}
    // ]
    // "#;
    println!("{:?}", create::create_indices_csv("csv_test", "./data.csv", indices));
}

fn main() {
    env_logger::init().unwrap();

    // create_thalia_index();
    // println!("{:?}",create_jmdict_index());

    // {
    //     let my_time = util::MeasureTime::new("jmdict load time", util::MeasureTimeLogLevel::Print);
    //     let mut _pers:persistence::Persistence = persistence::Persistence::load("jmdict".to_string()).expect("could not load jmdict");
    // }

    // let mut _pers:persistence::Persistence = persistence::Persistence::load("csv_test".to_string()).expect("could not load persistence");
    // doc_loader::DocLoader::load(&mut pers);
    // search::to_documents(&pers, &vec!(search::Hit{id:0, score:0.5}));

    // println!("_pers {:?}mb", _pers.heap_size_of_children()/1_000_000);
    // _pers.print_heap_sizes();

    // {
    //     let my_time = util::MeasureTime::new("binary_search total");
    //     let mut faccess = FileAccess::new("jmdict/meanings.ger[].text");
    //     let result = faccess.binary_search("haus");
    //     let result = faccess.binary_search("genau");
    //     let result = faccess.binary_search("achtung");
    //     // println!("{:?}", result);
    // }

    // println!("{:?}",test_build_f_s_t());
    // println!("{:?}",testfst("anschauen", 2));
    // println!("{:?}",search::test_levenshtein("anschauen", 2));


    // let _ = env_logger::init();
    // let req = json!({
    //     "search": {
    //         "term":"haus",
    //         "path": "meanings.ger[].text",
    //         "levenshtein_distance": 0,
    //         "firstCharExactMatch":true
    //     }
    // });

    // let requesto: search::Request = serde_json::from_str(&req.to_string()).unwrap();
    // let my_time = util::MeasureTime::new("Search");
    // let hits = search::search("jmdict", requesto, 0, 10).unwrap();

    // let requesto2: search::Request = serde_json::from_str(&req.to_string()).unwrap();
    // let hits2 = search::search("jmdict", requesto2, 0, 10).unwrap();

    // let docs = search::to_documents(&hits, "jmdict");

    // println!("{:?}", hits);




    // let doc_loader = doc_loader::DocLoader::new("jmdict", "data");
    // let now = Instant::now();
    // println!("{:?}", doc_loader.get_doc(1000).unwrap());
    // println!("Load Time: {}", (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));

    // println!("{:?}",test_build_fst());

    server::start_server();

}
// { "fulltext":"meanings.ger[]", "options":{"tokenize":true, "stopwords": ["stopword"]} }

#[allow(dead_code)]
fn create_jmdict_index() -> Result<(), io::Error> {
    let indices = r#"
    [
    {
        "boost": "commonness",
        "options": { "boost_type": "int" }
    },
    { "fulltext": "kanji[].text" },
    { "fulltext": "kana[].text" },
    {
        "fulltext": "meanings.ger[].text",
        "options": { "tokenize": true  }
    },
    {
        "boost": "meanings.ger[].rank",
        "options": { "boost_type": "int" }
    },
    {
        "fulltext": "meanings.eng[]",
        "options": { "tokenize": true  }
    },
    {
        "boost": "kanji[].commonness",
        "options": { "boost_type": "int" }
    },
    {
        "boost": "kana[].commonness",
        "options": { "boost_type": "int" }
    }
    ]
    "#;
    let mut f = File::open("jmdict.json")?;
    let mut s = String::new();
    f.read_to_string(&mut s)?;
    println!("{:?}", create::create_indices("jmdict", &s,  indices));
    Ok(())
}


#[allow(dead_code)]
pub fn testfst(term:&str, max_distance:u32) -> Result<(Vec<String>), fst::Error> {

    let mut f = try!(File::open("de_full_2.txt"));
    let mut s = String::new();
    try!(f.read_to_string(&mut s));
    let lines = s.lines().collect::<Vec<&str>>();
    // lines.sort();

    println!("{:?}", lines.len());


    // A convenient way to create sets in memory.
    // let set = try!(Set::from_iter(lines));

    let keys = vec!["寿司は焦げられない"];
    let set = try!(Set::from_iter(keys));

    let now = Instant::now();

    let lev = try!(Levenshtein::new(term, max_distance));
    let stream = set.search(lev).into_stream();
    let hits = try!(stream.into_strs());

    println!("fst ms: {}", (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));

    // assert_eq!(hits, vec!["fo", "fob", "foo", "food"]);

    Ok((hits))
}

// fn split_at_first()  {

//     lines.sort();
//     let firsts = lines.into_iter().map(|line: &str| {
//         let splits = line.split(" ").collect::<Vec<&str>>();
//         splits[0].to_string()

//     }).collect::<Vec<String>>();
//     File::create("de_full_2.txt")?.write_all(firsts.join("\n").as_bytes());
// }

#[allow(dead_code)]
fn test_build_fst() -> Result<(), fst::Error> {
    let now = Instant::now();

    let mut f = File::open("de_full_2.txt")?;
    let mut s = String::new();
    f.read_to_string(&mut s)?;
    let lines = s.lines().collect::<Vec<&str>>();
    println!("lines: {:?}", lines.len());


    let wtr = io::BufWriter::new(File::create("map.fst")?);
    // Create a builder that can be used to insert new key-value pairs.
    let mut build = MapBuilder::new(wtr)?;

    let mut i = 0;
    for line in lines {
        build.insert(line, i).unwrap();
        i += 1;
    }

    // println!("mapsize: {:?}", build.len());
    // println!("lines: {:?}", lines.len());
    // println(dupl_terms_checker.len())
    // Finish construction of the map and flush its contents to disk.
    build.finish()?;

    println!("test_build_fst ms: {}", (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));


    Ok(())
}

// use std::collections::BTreeMap;
// use fst::raw::{Builder, Fst, Output};

// #[cfg(test)]
// mod testero {

// use test::Bencher;
// use super::*;

//     #[bench]
//     fn bench_teh_stuff_btree(b: &mut Bencher) {
//         let mut map = BTreeMap::new();
//         for n in 1..15555555 {
//             map.insert(n, n * 30);
//         }
//         b.iter(|| {
//             // let mut hits = vec![];
//             // for i in 100000..200000 {
//             //     hits.push(map.get(&(i*50)));
//             // }
//             map.get(&1_234_567);
//             map.get(&60_000_000);
//             map.get(&12_345_678);
//             map.get(&80_345_678);
//             map.get(&123_456_789);
//         });
//     }

//     #[bench]
//     fn bench_teh_stuff_hashmap(b: &mut Bencher) {
//         let mut map = FnvHashMap::default();
//         for n in 1..15555555 {
//             map.insert(n, n * 30);
//         }
//         b.iter(|| {
//             // let mut hits = vec![];
//             // for i in 100000..200000 {
//             //     hits.push(map.get(&(i*50)));
//             // }
//             map.get(&1_234_567);
//             map.get(&60_000_000);
//             map.get(&12_345_678);
//             map.get(&80_345_678);
//             map.get(&123_456_789);
//         });
//     }

//     #[bench]
//     fn bench_teh_stuff_fst(b: &mut Bencher) {
//         let wtr = io::BufWriter::new(File::create("bencho.fst").unwrap());
//         let mut build2 = MapBuilder::new(wtr).unwrap();
//         let mut ids = vec![];
//         let mut build = Builder::memory();
//         for n in 1..15555555 {
//             // map.insert(n, n);
//             let raw_bytes : [u8; 8] = unsafe {std::mem::transmute(n as u64)};
//             // build.insert(raw_bytes, n).unwrap();
//             // build.insert(n.to_string(), n).unwrap();
//             // ids.push(format!("{:09}", n));
//             // ids.push(n.to_string());
//             ids.push((raw_bytes, n * 30));
//         }
//         // ids.sort();
//         ids.sort_by_key(|&(k, ref v)| k);
//         for (ref el, ref v) in ids {
//             build.insert(el.clone(), *v  ).unwrap();
//             build2.insert(el.clone(), *v ).unwrap();
//         }
//         let fst_bytes = build.into_inner().unwrap();
//         build2.finish().unwrap();
//         // Create an Fst that we can query.
//         let fst = Fst::from_bytes(fst_bytes).unwrap();
//         let raw_bytes1 : [u8; 8] = unsafe {std::mem::transmute(1_234_567 as u64)};
//         let raw_bytes2 : [u8; 8] = unsafe {std::mem::transmute(60_000_000 as u64)};
//         let raw_bytes3 : [u8; 8] = unsafe {std::mem::transmute(12_345_678 as u64)};
//         let raw_bytes4 : [u8; 8] = unsafe {std::mem::transmute(80_345_678 as u64)};
//         let raw_bytes5 : [u8; 8] = unsafe {std::mem::transmute(123_456_789 as u64)};
//         b.iter(|| {
//             // for i in 100000..200000 {
//             //     // fst.get(format!("{:09}", i*50));
//             //     // fst.get((i*50).to_string());
//             //     let raw_bytes : [u8; 8] = unsafe {std::mem::transmute((i*50) as u32)};
//             //     fst.get(raw_bytes);
//             // }
//             fst.get(raw_bytes1);
//             fst.get(raw_bytes2);
//             fst.get(raw_bytes3);
//             fst.get(raw_bytes4);
//             fst.get(raw_bytes5);
//         });
//     }

//     #[bench]
//     fn bench_teh_stuff_fst_string_based(b: &mut Bencher) {
//         let mut ids = vec![];
//         let mut build = Builder::memory();
//         for n in 1..15555555 {
//             ids.push((format!("{:09}", n), n * 30));
//         }
//         ids.sort_by_key(|&(ref k, ref v)| k.clone());
//         for (ref el, ref v) in ids {
//             build.insert(el.clone(),  *v ).unwrap();
//         }
//         let fst_bytes = build.into_inner().unwrap();
//         // Create an Fst that we can query.
//         let fst = Fst::from_bytes(fst_bytes).unwrap();
//         b.iter(|| {
//             // for i in 100000..200000 {
//             //     // fst.get(format!("{:09}", i*50));
//             //     // fst.get((i*50).to_string());
//             //     let raw_bytes : [u8; 8] = unsafe {std::mem::transmute((i*50) as u32)};
//             //     fst.get(raw_bytes);
//             // }
//             fst.get(format!("{:09}", 1_234_567));
//             fst.get(format!("{:09}", 60_000_000));
//             fst.get(format!("{:09}", 12_345_678));
//             fst.get(format!("{:09}", 80_345_678));
//             fst.get(format!("{:09}", 123_456_789));
//         });
//     }

//     #[bench]
//     fn bench_teh_stuff_vec(b: &mut Bencher) {
//         let mut vec = vec![];
//         vec.resize(15555555, 0);
//         for n in 1..15555555 {
//             // vec.insert(n, n * 30);
//             vec[n] = n * 30;
//         }
//         b.iter(|| {
//             // let mut hits = vec![];
//             // for i in 100000..200000 {
//             //     hits.push(vec.get(i*50));
//             // }
//             vec.get(1_234_567);
//             vec.get(60_000_000);
//             vec.get(12_345_678);
//             vec.get(80_345_678);
//             vec.get(123_456_789);
//         });
//     }

//     #[bench]
//     fn bench_teh_stuff_vec_binary_search(b: &mut Bencher) {
//         let mut vec1 = vec![];
//         let mut vec2 = vec![];
//         // vec.resize(15555555, 0);
//         for n in 1..15555555 {
//             vec1.push(n);
//             vec2.push(n * 30);
//         }
//         b.iter(|| {
//             // let mut hits = vec![];
//             // for i in 100000..200000 {
//             //     hits.push(vec.get(i*50));
//             // }
//             match vec1.binary_search(&1_234_567) {
//                 Ok(mut pos) => {
//                     vec2[pos];
//                 },Err(_) => {},
//             }
//             match vec1.binary_search(&60_000_000) {
//                 Ok(mut pos) => {
//                     vec2[pos];
//                 },Err(_) => {},
//             }
//             match vec1.binary_search(&12_345_678) {
//                 Ok(mut pos) => {
//                     vec2[pos];
//                 },Err(_) => {},
//             }
//             match vec1.binary_search(&80_345_678) {
//                 Ok(mut pos) => {
//                     vec2[pos];
//                 },Err(_) => {},
//             }
//             match vec1.binary_search(&123_456_789) {
//                 Ok(mut pos) => {
//                     vec2[pos];
//                 },Err(_) => {},
//             }
//             // vec1.binary_search(&1_234_567);
//             // vec1.binary_search(&60_000_000);
//             // vec1.binary_search(&12_345_678);
//             // vec1.binary_search(&80_345_678);
//             // vec1.binary_search(&123_456_789);
//         });
//     }

// }

// #[test]
// fn it_works() {

//     assert_eq!(util::normalize_text("Hello"), "hello");
//     assert_eq!(util::normalize_text("(Hello)"), "hello");
//     assert_eq!(util::normalize_text("\"H,ell-;o"), "hello");
//     assert_eq!(util::normalize_text("Hello(f)"), "hello");
//     assert_eq!(util::normalize_text("Hello(2)"), "hello");

//     assert_eq!(util::normalize_text("majestätisches Aussehen (n)"), "majestätisches aussehen");

//     assert_eq!(util::remove_array_marker("Hello[]"), "hello");
//     assert_eq!(util::remove_array_marker("Hello[].ja"), "hello.ja");

// }
