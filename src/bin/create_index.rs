#![recursion_limit = "128"]

#[macro_use]
extern crate log;

extern crate env_logger;
extern crate flexi_logger;
extern crate fst;
// extern crate fst_levenshtein;
#[macro_use]
extern crate measure_time;
extern crate rayon;
extern crate search_lib;
#[macro_use]
extern crate serde_json;

#[allow(unused_imports)]
use fst::{IntoStreamer, MapBuilder, Set};
// use fst_levenshtein::Levenshtein;
use serde_json::{Deserializer, Value};
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::str;

#[allow(unused_imports)]
use rayon::prelude::*;

fn main() {
    // env_logger::init().unwrap();
    search_lib::trace::enable_log();
    std::env::args().nth(1).expect("require command line parameter");

    for jeppo in std::env::args().skip(1) {
        match jeppo.as_ref() {
            "healthcare" => println!("{:?}", create_healtcare()),
            "jmdict" => println!("{:?}", create_jmdict_index()),
            "jmdict_shards" => println!("{:?}", create_jmdict_index_shards()),
            "gutenberg" => println!("{:?}", create_book_index()),
            "single_data" => println!("{:?}", create_single_data_index()),
            // "thalia" => println!("{:?}", create_thalia_index()),
            "thalia_big" => println!("{:?}", create_thalia_index_big()),
            "thalia_shards" => println!("{:?}", create_thalia_index_shards()),
            _ => {}
        };
    }

    // create_thalia_index();
    // {
    //     let my_time = util::MeasureTime::new("jmdict load time", util::MeasureTimeLogLevel::Print);
    //     let mut _pers:search_lib::persistence::Persistence = search_lib::persistence::Persistence::load("jmdict".to_string()).expect("could not load jmdict");
    // }

    // let doc_loader = doc_loader::DocLoader::new("jmdict", "data");
    // let now = Instant::now();
    // server::start_server();
}

static TAHLIA_INDICES: &str = r#"
[
    { "facet": "ISMLANGUAGES" },
    { "facet": "ISMARTIST" },
    { "facet": "GENRE" },
    { "facet": "VERLAG[]" },
    { "fulltext":"ISMTITLE",     "options":{"tokenize":true}  },
    { "fulltext":"ISMORIGTITLE", "options":{"tokenize":true}  },
    { "fulltext":"ISMSUBTITLE1", "options":{"tokenize":true}  },
    { "fulltext":"ISMSUBTITLE2", "options":{"tokenize":true}  },
    { "fulltext":"ISMSUBTITLE3", "options":{"tokenize":true}  },
    { "fulltext":"ISMARTIST",    "options":{"tokenize":true}  },
    { "fulltext":"ISMMEDIATYPE", "options":{"tokenize":false} },
    { "fulltext":"MEINS",        "options":{"tokenize":false} },
    { "fulltext":"MTART",        "options":{"tokenize":false} },
    { "fulltext":"MATNR",        "options":{"tokenize":false} },
    { "fulltext":"ZZSW2_KEY",    "options":{"tokenize":false} },
    { "fulltext":"ZZONLINE_KAT", "options":{"tokenize":false} },
    { "fulltext":"ZZONLINE_KAT", "options":{"tokenize":false} },
    { "fulltext":"ZZMYINFOARTNR","options":{"tokenize":false} },
    { "fulltext":"ISMLANGUAGES", "options":{"tokenize":false} },
    { "fulltext":"ISMPUBLDATE",  "options":{"tokenize":false} },
    { "fulltext":"EAN11",        "options":{"tokenize":false} },
    { "fulltext":"ISMORIDCODE",  "options":{"tokenize":false} }
]
"#;

// static TAHLIA_INDICES_NEW_CONCEPT: &str = r#"
// {
//     "facets": ["VERLAG[]"],
//     "fulltext" : {
//         "MATNR"        : {},
//         "ISMTITLE"     : {"options":{"tokenize":true}  },
//         "ISMORIGTITLE" : {"options":{"tokenize":true}  },
//         "ISMSUBTITLE1" : {"options":{"tokenize":true}  },
//         "ISMSUBTITLE2" : {"options":{"tokenize":true}  },
//         "ISMSUBTITLE3" : {"options":{"tokenize":true}  },
//         "ISMARTIST"    : {"options":{"tokenize":true}  },
//         "ISMLANGUAGES" : {"options":{"tokenize":false} },
//         "ISMPUBLDATE"  : {"options":{"tokenize":false} },
//         "EAN11"        : {"options":{"tokenize":false} },
//         "ISMORIDCODE"  : {"options":{"tokenize":false} }
//     }
// }

// "#;

// #[allow(dead_code)]
// fn create_thalia_index() {
//     // let all_terms = loadcsv("./data.csv", 0);

//     let headers = vec![
//         "MATNR".to_string(),
//         "ISMTITLE".to_string(),
//         "ISMORIGTITLE".to_string(),
//         "ISMSUBTITLE1".to_string(),
//         "ISMSUBTITLE2".to_string(),
//         "ISMSUBTITLE3".to_string(),
//         "ISMARTIST".to_string(),
//         "ISMLANGUAGES".to_string(),
//         "ISMPUBLDATE".to_string(),
//         "EAN11".to_string(),
//         "ISMORIDCODE".to_string(),
//     ];

//     let json = search_lib::create_from_csv::convert_to_json("./data 2.csv", headers);

//     File::create("thalia.json")
//         .unwrap()
//         .write_all(serde_json::to_string_pretty(&json).unwrap().as_bytes())
//         .unwrap();

//     println!("{:?}", search_lib::create::create_indices_from_json("thalia", &json, TAHLIA_INDICES));
//     // File::create("MATNR").unwrap().write_all(all_terms.join("\n").as_bytes()).unwrap();

//     // println!("{:?}", search_lib::create::create_indices_csv("csv_test", "./data.csv", TAHLIA_INDICES));
// }

#[allow(dead_code)]
fn create_thalia_index_big() -> Result<(), io::Error> {
    let mut f = File::open("data")?;
    let mut json = String::new();
    f.read_to_string(&mut json)?;

    search_lib::create::create_indices_from_str(
        &mut search_lib::persistence::Persistence::create("thalia_new".to_string()).unwrap(),
        &json,
        TAHLIA_INDICES,
        None,
    ).unwrap();
    // File::create("MATNR").unwrap().write_all(all_terms.join("\n").as_bytes()).unwrap();

    Ok(())
}

#[allow(dead_code)]
fn create_thalia_index_shards() -> Result<(), io::Error> {
    (0..167).into_iter().for_each(|i: i32| {
        let shard_num = i.to_string();
        let path = "data_split_500/data_".to_owned() + &shard_num;
        // println!("{:?}", &path);
        let mut f = File::open(&path).unwrap();
        let mut json = String::new();
        f.read_to_string(&mut json).unwrap();
        let path = "thalia_split_500/thalia_".to_owned() + &shard_num;
        search_lib::create::create_indices_from_str(
            &mut search_lib::persistence::Persistence::create(path.to_string()).unwrap(),
            &json,
            TAHLIA_INDICES,
            None,
        ).unwrap();
        println!("created shard num {:?}", &shard_num);
    });

    Ok(())
}

#[allow(dead_code)]
fn create_jmdict_index_shards() -> Result<(), io::Error> {
    let indices = r#"
    [
    {
        "boost": "commonness",
        "options": { "boost_type": "int" }
    },
    { "fulltext": "kanji[].text", "options":{"tokenize":false} },
    { "fulltext": "kanji[].conjugated[].form", "options":{"tokenize":false} },
    { "fulltext": "kana[].text" , "options":{"tokenize":false} },
    { "fulltext": "kana[].conjugated[].form" , "options":{"tokenize":false} },
    { "fulltext": "kana[].romaji" , "options":{"tokenize":true} },
    { "fulltext": "meanings.ger[].text", "options": { "tokenize": true } },
    { "fulltext": "meanings.eng[]", "options": { "tokenize": true } },
    { "fulltext": "pos", "options": { "tokenize": false } },
    {
        "boost": "meanings.ger[].rank",
        "options": { "boost_type": "int" }
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

    let mut s = String::new();
    let mut f = File::open("jmdict.json")?;
    f.read_to_string(&mut s)?;
    let v: Value = serde_json::from_str(&s)?;

    print_time!("jmdict_index_shards");
    let mut jmdict_shards = search_lib::shards::Shards::new("jmdict_shards".to_string());
    if let Some(arr) = v.as_array() {
        for el in arr.iter() {
            jmdict_shards.insert(el.to_string(), indices);
        }
    }

    // let docs = Deserializer::from_reader(f).into_iter::<Value>();
    // for doc in docs {
    //     jmdict_shards.insert(doc?.to_string(), indices);
    // }

    // let doc = r#"
    //     {
    //         "pos": [
    //           "v5t"
    //         ],
    //         "misc": [],
    //         "kanji": [{"text": "持つ", "ent_seq": "1315720", "commonness": 1620, "readings": ["もつ"] } ],
    //         "kana": [{"text": "もつ", "ent_seq": "1315720", "romaji": "Motsu", "commonness": 40 } ],
    //         "meanings": {
    //           "eng": ["hold (in one's hand)", "take", "carry", "possess", "have", "own", "maintain", "keep", "last", "be durable", "survive", "take charge of", "be in charge of"],
    //           "ger": [
    //             {"text": "haben", "rank": 1 },
    //             {"text": "besitzen"},
    //             {"text": "innehaben"},
    //             {"text": "verfügen"},
    //             {"text": "in der Hand halten", "rank": 2 },
    //             {"text": "festhalten"},
    //             {"text": "halten"},
    //             {"text": "hegen", "rank": 3 },
    //             {"text": "pflegen"},
    //             {"text": "unterstützen"},
    //             {"text": "tragen", "rank": 4 },
    //             {"text": "bei sich haben"},
    //             {"text": "auf sich nehmen", "rank": 5 },
    //             {"text": "übernehmen"},
    //             {"text": "verantwortlich sein"},
    //             {"text": "bekleiden"},
    //             {"text": "halten", "rank": 6 },
    //             {"text": "dauern"},
    //             {"text": "währen"},
    //             {"text": "überstehen"},
    //             {"text": "überleben"}
    //           ]
    //         },
    //         "ent_seq": "1315720",
    //         "commonness": 1660
    //     }
    // "#;
    // jmdict_shards.insert(doc.to_string(), indices);
    // jmdict_shards.insert(doc.to_string(), indices);
    // jmdict_shards.insert(doc.to_string(), indices);
    Ok(())
}

#[allow(dead_code)]
fn create_jmdict_index() -> Result<(), io::Error> {
    let indices = r#"
    [
    {
        "boost": "commonness",
        "options": { "boost_type": "int" }
    },
    { "fulltext": "kanji[].text", "options":{"tokenize":false} },
    { "fulltext": "kanji[].conjugated[].form", "options":{"tokenize":false} },
    { "fulltext": "kana[].text" , "options":{"tokenize":false} },
    { "fulltext": "kana[].conjugated[].form" , "options":{"tokenize":false} },
    { "fulltext": "kana[].romaji" , "options":{"tokenize":true} },
    { "fulltext": "meanings.ger[].text", "options": { "tokenize": true } },
    { "fulltext": "meanings.eng[]", "options": { "tokenize": true } },
    { "fulltext": "pos", "options": { "tokenize": false } },
    {
        "boost": "meanings.ger[].rank",
        "options": { "boost_type": "int" }
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
    search_lib::create::create_indices_from_str(
        &mut search_lib::persistence::Persistence::create("jmdict".to_string()).unwrap(),
        &s,
        indices,
        None,
    ).unwrap();
    Ok(())
}

#[allow(dead_code)]
fn create_single_data_index() -> Result<(), io::Error> {
    info_time!("create_single_data_index");
    let indices = "[]";

    for i in 0..1 {
        let books = (0..10000)
            .map(|_el| {
                json!({
                "commonness": 3103,
                "ent_seq": "1259290",
                "kana": [
                    {
                        "commonness": 35,
                        "ent_seq": "1259290",
                        "romaji": "Miru",
                        "text": "みる"
                    }
                ],
                "kanji": [
                    {
                        "commonness": 3068,
                        "conjugated": [
                            {"form": "見ないでください", "name": "negative request"},
                            {"form": "見ませんでした", "name": "past polite negative"},
                            {"form": "見なかった", "name": "past negative"},
                            {"form": "見させられる", "name": "causative passive"},
                            {"form": "見るでしょう", "name": "polite presumptive"},
                            {"form": "見たげる", "name": "simplified te ageru"},
                            {"form": "見ている", "name": "te iru"},
                            {"form": "見てある", "name": "te aru"},
                            {"form": "見ません", "name": "present polite negative"},
                            {"form": "見ておる", "name": "te oru"},
                            {"form": "見ておく", "name": "te oku"},
                            {"form": "見たがる", "name": "other's desire"},
                            {"form": "見ました", "name": "past polite"},
                            {"form": "見るらしい", "name": "apparently the case"},
                            {"form": "見るだろう", "name": "plain presumptive"},
                            {"form": "見たろう", "name": "past presumptive"},
                            {"form": "見とる", "name": "simplified te oru"},
                            {"form": "見るそう", "name": "claimed to be the case"},
                            {"form": "見ない", "name": "present negative"},
                            {"form": "見たい", "name": "desire"},
                            {"form": "見ます", "name": "present polite"},
                            {"form": "見てる", "name": "simplified te iru"},
                            {"form": "見たり", "name": "representative"},
                            {"form": "見そう", "name": "looks to be the case"},
                            {"form": "見かた", "name": "way of doing"},
                            {"form": "見させる", "name": "causative"},
                            {"form": "見られる", "name": "passive"},
                            {"form": "見とく", "name": "simplified te oku"},
                            {"form": "見", "name": "short potential"},
                            {"form": "見れば", "name": "hypothetical"},
                            {"form": "見よう", "name": "pseudo futurum"},
                            {"form": "見て", "name": "te form"},
                            {"form": "見るな", "name": "negative imperative"},
                            {"form": "見-", "name": "conjunctive"},
                            {"form": "見た", "name": "past"},
                            {"form": "見ろ", "name": "commanding"}
                        ],
                        "ent_seq": "1259290",
                        "readings": [
                            "みる"
                        ],
                        "text": "見る"
                    },
                    {
                        "commonness": 0,
                        "conjugated": [
                            {"form": "観ないでください", "name": "negative request"},
                            {"form": "観ませんでした", "name": "past polite negative"},
                            {"form": "観なかった", "name": "past negative"},
                            {"form": "観させられる", "name": "causative passive"},
                            {"form": "観るでしょう", "name": "polite presumptive"},
                            {"form": "観たげる", "name": "simplified te ageru"},
                            {"form": "観ている", "name": "te iru"},
                            {"form": "観てある", "name": "te aru"},
                            {"form": "観ません", "name": "present polite negative"},
                            {"form": "観ておる", "name": "te oru"},
                            {"form": "観ておく", "name": "te oku"},
                            {"form": "観たがる", "name": "other's desire"},
                            {"form": "観ました", "name": "past polite"},
                            {"form": "観るらしい", "name": "apparently the case"},
                            {"form": "観るだろう", "name": "plain presumptive"},
                            {"form": "観たろう", "name": "past presumptive"},
                            {"form": "観とる", "name": "simplified te oru"},
                            {"form": "観るそう", "name": "claimed to be the case"},
                            {"form": "観ない", "name": "present negative"},
                            {"form": "観たい", "name": "desire"},
                            {"form": "観ます", "name": "present polite"},
                            {"form": "観てる", "name": "simplified te iru"},
                            {"form": "観たり", "name": "representative"},
                            {"form": "観そう", "name": "looks to be the case"},
                            {"form": "観かた", "name": "way of doing"},
                            {"form": "観させる", "name": "causative"},
                            {"form": "観られる", "name": "passive"},
                            {"form": "観とく", "name": "simplified te oku"},
                            {"form": "観", "name": "short potential"},
                            {"form": "観れば", "name": "hypothetical"},
                            {"form": "観よう", "name": "pseudo futurum"},
                            {"form": "観て", "name": "te form"},
                            {"form": "観るな", "name": "negative imperative"},
                            {"form": "観-", "name": "conjunctive"},
                            {"form": "観た", "name": "past"},
                            {"form": "観ろ", "name": "commanding"}
                        ],
                        "ent_seq": "1259290",
                        "readings": ["みる"],
                        "text": "観る"
                    },
                    {
                        "commonness": 0,
                        "conjugated": [
                            {"form": "視ないでください", "name": "negative request"},
                            {"form": "視ませんでした", "name": "past polite negative"},
                            {"form": "視なかった", "name": "past negative"},
                            {"form": "視させられる", "name": "causative passive"},
                            {"form": "視るでしょう", "name": "polite presumptive"},
                            {"form": "視たげる", "name": "simplified te ageru"},
                            {"form": "視ている", "name": "te iru"},
                            {"form": "視てある", "name": "te aru"},
                            {"form": "視ません", "name": "present polite negative"},
                            {"form": "視ておる", "name": "te oru"},
                            {"form": "視ておく", "name": "te oku"},
                            {"form": "視たがる", "name": "other's desire"},
                            {"form": "視ました", "name": "past polite"},
                            {"form": "視るらしい", "name": "apparently the case"},
                            {"form": "視るだろう", "name": "plain presumptive"},
                            {"form": "視たろう", "name": "past presumptive"},
                            {"form": "視とる", "name": "simplified te oru"},
                            {"form": "視るそう", "name": "claimed to be the case"},
                            {"form": "視ない", "name": "present negative"},
                            {"form": "視たい", "name": "desire"},
                            {"form": "視ます", "name": "present polite"},
                            {"form": "視てる", "name": "simplified te iru"},
                            {"form": "視たり", "name": "representative"},
                            {"form": "視そう", "name": "looks to be the case"},
                            {"form": "視かた", "name": "way of doing"},
                            {"form": "視させる", "name": "causative"},
                            {"form": "視られる", "name": "passive"},
                            {"form": "視とく", "name": "simplified te oku"},
                            {"form": "視", "name": "short potential"},
                            {"form": "視れば", "name": "hypothetical"},
                            {"form": "視よう", "name": "pseudo futurum"},
                            {"form": "視て", "name": "te form"},
                            {"form": "視るな", "name": "negative imperative"},
                            {"form": "視-", "name": "conjunctive"},
                            {"form": "視た", "name": "past"},
                            {"form": "視ろ", "name": "commanding"}
                        ],
                        "ent_seq": "1259290",
                        "readings": ["みる"],
                        "text": "視る"
                    }
                ],
                "meanings": {
                    "eng": ["see", "look", "watch", "view", "observe", "look over", "look on", "assess", "examine", "judge", "look after", "keep an eye on", "take care of", "view (e.g. flowers, movie)", "try", "try out", "see that...", "find that..."],
                    "ger": [
                        {"text": "betrachten"}, {"text": "anschauen"}, {"rank": 1, "text": "sehen"},
                        {"text": "schauen"}, {"text": "gucken"}, {"text": "(norddt.) kucken"}, {"rank": 2, "text": "anschauen"},
                        {"text": "ansehen"}, {"text": "anblicken"}, {"rank": 3, "text": "lesen"},
                        {"text": "durchsehen"}, {"rank": 4, "text": "erblicken"},
                        {"text": "bemerken"}, {"rank": 5, "text": "betrachten"},
                        {"text": "beobachten"}, {"rank": 6, "text": "betrachten und inhaltlich verstehen"},
                        {"text": "mit ansehen"}, {"text": "zusehen"}, {"text": "zuschauen"}, {"text": "anstarren"}, {"text": "besichtigen"}, {"rank": 1, "text": "ansehen als …"},
                        {"rank": 1, "text": "überprüfen"},
                        {"text": "nachschlagen"}, {"rank": 1, "text": "beurteilen"},
                        {"text": "bewerten"}, {"rank": 1, "text": "auf etw. achten"},
                        {"text": "sich um etw. kümmern"}, {"rank": 1, "text": "betreuen"},
                        {"text": "sich um jmdn. kümmern"}, {"rank": 1, "text": "erfahren"},
                        {"text": "eine Erfahrung machen"}, {"rank": 1, "text": "verwirklichen"},
                        {"rank": 1, "text": "treffen (bes. jmdn. des anderen Geschlechtes)"},
                        {"text": "Umgang haben"}, {"rank": 1, "text": "als Ehepaar leben"},
                        {"rank": 1, "text": "(nach te-Form) versuchen"}
                    ]
                },
                "misc": [],
                "pos": [
                    "v1",
                    "vt"
                ]
            })
            })
            .collect::<Vec<_>>();

        let mut persistence =
            search_lib::persistence::Persistence::create_type("single_data".to_string(), search_lib::persistence::PersistenceType::Persistent).unwrap();
        search_lib::create::create_indices_from_str(&mut persistence, &serde_json::to_string_pretty(&books).unwrap(), indices, None).unwrap();
    }

    Ok(())
}

#[allow(dead_code)]
fn create_book_index() -> Result<(), io::Error> {
    let indices = "[]";
    let mut f = File::open("1342-0.txt")?;
    let mut s = String::new();
    f.read_to_string(&mut s)?;

    let books = (0..100).map(|_el| json!({"title":"PRIDE AND PREJUDICE", "content":s})).collect::<Vec<_>>();

    search_lib::create::create_indices_from_str(
        &mut search_lib::persistence::Persistence::create("gutenberg".to_string()).unwrap(),
        &serde_json::to_string_pretty(&books).unwrap(),
        indices,
        None,
    ).unwrap();
    Ok(())
}

// #[allow(dead_code)]
// pub fn testfst(term: &str, max_distance: u32) -> Result<(Vec<String>), fst_levenshtein::Error> {
//     let mut f = File::open("de_full_2.txt").unwrap();
//     let mut s = String::new();
//     f.read_to_string(&mut s).unwrap();
//     let lines = s.lines().collect::<Vec<&str>>();
//     // lines.sort();

//     println!("{:?}", lines.len());
//     // A convenient way to create sets in memory.
//     // let set = try!(Set::from_iter(lines));

//     let keys = vec!["寿司は焦げられない"];
//     let set = Set::from_iter(keys).unwrap();

//     let now = Instant::now();

//     let lev = Levenshtein::new(term, max_distance).unwrap();
//     let stream = set.search(lev).into_stream();
//     let hits = stream.into_strs().unwrap();

//     println!("fst ms: {}", (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));

//     // assert_eq!(hits, vec!["fo", "fob", "foo", "food"]);

//     Ok((hits))
// }

// fn split_at_first()  {

//     lines.sort();
//     let firsts = lines.into_iter().map(|line: &str| {
//         let splits = line.split(" ").collect::<Vec<&str>>();
//         splits[0].to_string()

//     }).collect::<Vec<String>>();
//     File::create("de_full_2.txt")?.write_all(firsts.join("\n").as_bytes());
// }

// #[allow(dead_code)]
// fn test_build_fst() -> Result<(), fst::Error> {
//     let now = Instant::now();

//     let mut f = File::open("de_full_2.txt")?;
//     let mut s = String::new();
//     f.read_to_string(&mut s)?;
//     let lines = s.lines().collect::<Vec<&str>>();

//     let wtr = io::BufWriter::new(File::create("map.fst")?);
//     // Create a builder that can be used to insert new key-value pairs.
//     let mut build = MapBuilder::new(wtr)?;

//     let mut i = 0;
//     for line in lines {
//         build.insert(line, i).unwrap();
//         i += 1;
//     }
//     // Finish construction of the map and flush its contents to disk.
//     build.finish()?;

//     println!(
//         "test_build_fst ms: {}",
//         (now.elapsed().as_secs() as f64 * 1_000.0) + (f64::from(now.elapsed().subsec_nanos()) / 1000_000.0)
//     );

//     Ok(())
// }

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

fn create_healtcare() -> Result<(), io::Error> {
    let indices = r#"
    [
        {"fulltext": "diagnosticreport[].result[].reference", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].result[].display", "options":{"tokenize":true}},
        {"fulltext": "address[].country", "options":{"tokenize":true}},
        {"fulltext": "address[].city", "options":{"tokenize":true}},
        {"fulltext": "address[].postalCode", "options":{"tokenize":true}},
        {"fulltext": "address[].state", "options":{"tokenize":true}},
        {"fulltext": "patientname[].given[]", "options":{"tokenize":true}},
        {"fulltext": "condition[].code.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "condition[].code.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "condition[].code.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component.code.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component.code.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component.code.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "patientname[].prefix[]", "options":{"tokenize":true}},
        {"fulltext": "procedure[].code.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "procedure[].code.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "procedure[].code.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueCodeableConcept.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueCodeableConcept.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueCodeableConcept.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "communication.language.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "communication.language.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "communication.language.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].verificationStatus", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].allergyintoleranceType", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].patient_reference", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "condition[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "condition[].subject_reference", "options":{"tokenize":true}},
        {"fulltext": "condition[].id", "options":{"tokenize":true}},
        {"fulltext": "condition[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "encounter[].period_end", "options":{"tokenize":true}},
        {"fulltext": "encounter[].serviceProvider_reference", "options":{"tokenize":true}},
        {"fulltext": "encounter[].period_start", "options":{"tokenize":true}},
        {"fulltext": "encounter[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "encounter[].id", "options":{"tokenize":true}},
        {"fulltext": "encounter[].patient_reference", "options":{"tokenize":true}},
        {"fulltext": "encounter[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "encounter[].status", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueQuantity_unit", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].encounter_reference", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].code_text", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueQuantity_code", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueQuantity_system", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].effectiveDateTime", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].subject_reference", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueQuantity_value", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].id", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].issued", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueCodeableConcept_text", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].status", "options":{"tokenize":true}},
        {"fulltext": "procedure[].extension.valueCodeableConcept.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "procedure[].extension.valueCodeableConcept.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "procedure[].extension.valueCodeableConcept.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "careplanCategory.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "careplanCategory.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "careplanCategory.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "encounter[].reason.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "encounter[].reason.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "encounter[].reason.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "maritalStatus.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "maritalStatus.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "address[].extension.extension[].valueDecimal", "options":{"tokenize":true}},
        {"fulltext": "address[].extension.extension[].url", "options":{"tokenize":true}},
        {"fulltext": "immunization.vaccineCode.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "immunization.vaccineCode.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "immunization.vaccineCode.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dispenseRequest_expectedSupplyDuration_system", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dispenseRequest_numberOfRepeatsAllowed", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dispenseRequest_quantity_unit", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dispenseRequest_expectedSupplyDuration_unit", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dispenseRequest_expectedSupplyDuration_value", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dispenseRequest_quantity_value", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].patient_reference", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dispenseRequest_expectedSupplyDuration_code", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].category[].text", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].code.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].code.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].code.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].effectiveDateTime", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].subject_reference", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].encounter_reference", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].id", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].issued", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].status", "options":{"tokenize":true}},
        {"fulltext": "patientname[].suffix[]", "options":{"tokenize":true}},
        {"fulltext": "type.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "type.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "type.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].additionalInstructions.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].additionalInstructions.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].additionalInstructions.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "patientname[].use", "options":{"tokenize":true}},
        {"fulltext": "patientname[].family", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component[].valueQuantity_system", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component[].valueQuantity_unit", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component[].valueQuantity_value", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component[].code_text", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component[].valueQuantity_code", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].gender", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].type_text", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].maritalStatus_text", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].birthDate", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].context_reference", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].period_end", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].period_start", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].multipleBirthInteger", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].organizationname", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].deceasedDateTime", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].subject_reference", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].id", "options":{"tokenize":true}},
        {"fulltext": "healthcare[]._id", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].status", "options":{"tokenize":true}},
        {"fulltext": "suggest[].input", "options":{"tokenize":true}},
        {"fulltext": "suggest[].weight", "options":{"tokenize":true}},
        {"fulltext": "address[].line[]", "options":{"tokenize":true}},
        {"fulltext": "procedure[].extension[].url", "options":{"tokenize":true}},
        {"fulltext": "procedure[].extension[].valueCodeableConcept_text", "options":{"tokenize":true}},
        {"fulltext": "addresses[].reference", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].code.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].code.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].code.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].performer[].display", "options":{"tokenize":true}},
        {"fulltext": "procedure[].reasonReference_reference", "options":{"tokenize":true}},
        {"fulltext": "procedure[].performedPeriod_end", "options":{"tokenize":true}},
        {"fulltext": "procedure[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "procedure[].subject_reference", "options":{"tokenize":true}},
        {"fulltext": "procedure[].encounter_reference", "options":{"tokenize":true}},
        {"fulltext": "procedure[].performedDateTime", "options":{"tokenize":true}},
        {"fulltext": "procedure[].performedPeriod_start", "options":{"tokenize":true}},
        {"fulltext": "procedure[].code_text", "options":{"tokenize":true}},
        {"fulltext": "procedure[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "procedure[].status", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].medicationCodeableConcept.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].medicationCodeableConcept.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].medicationCodeableConcept.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].category.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].category.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "activity[].detail_status", "options":{"tokenize":true}},
        {"fulltext": "address[].extension[].url", "options":{"tokenize":true}},
        {"fulltext": "goal[].reference", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].allergyintoleranceCategory[]", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].code.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].code.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].code.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "immunization[].date", "options":{"tokenize":true}},
        {"fulltext": "immunization[].primarySource", "options":{"tokenize":true}},
        {"fulltext": "immunization[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "immunization[].vaccineCode_text", "options":{"tokenize":true}},
        {"fulltext": "immunization[].encounter_reference", "options":{"tokenize":true}},
        {"fulltext": "immunization[].patient_reference", "options":{"tokenize":true}},
        {"fulltext": "immunization[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "immunization[].status", "options":{"tokenize":true}},
        {"fulltext": "immunization[].wasNotGiven", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].doseQuantity_value", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].sequence", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].timing_repeat_period", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].timing_repeat_periodUnit", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].timing_repeat_frequency", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].asNeededBoolean", "options":{"tokenize":true}},
        {"fulltext": "activity[].detail.code.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "activity[].detail.code.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "activity[].detail.code.coding[].display", "options":{"tokenize":true}}
    ]
    "#;

    let mut f = File::open("healthcare.json")?;
    let mut s = String::new();
    f.read_to_string(&mut s)?;
    search_lib::create::create_indices_from_str(
        &mut search_lib::persistence::Persistence::create("healthcare".to_string()).unwrap(),
        &s,
        indices,
        None,
    ).unwrap();
    Ok(())
}
