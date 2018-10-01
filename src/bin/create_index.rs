#![recursion_limit = "128"]
#[macro_use]
extern crate log;

extern crate flexi_logger;
extern crate fst;
// extern crate fst_levenshtein;

#[macro_use]
extern crate measure_time;
extern crate rayon;
extern crate search_lib;
#[macro_use]
extern crate serde_json;

#[cfg(feature = "enable_cpuprofiler")]
extern crate cpuprofiler;

#[allow(unused_imports)]
use fst::{IntoStreamer, MapBuilder, Set};
// use fst_levenshtein::Levenshtein;
// use serde_json::{Deserializer, Value};
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::str;

#[allow(unused_imports)]
use rayon::prelude::*;

fn main() {
    search_lib::trace::enable_log();
    std::env::args().nth(1).expect("require command line parameter");

    for jeppo in std::env::args().skip(1) {
        match jeppo.as_ref() {
            // "healthcare" => println!("{:?}", create_healtcare()),
            "jmdict" => println!("{:?}", create_jmdict_index()),
            "jmdict_shards" => println!("{:?}", create_jmdict_index_shards()),
            "gutenberg" => println!("{:?}", create_book_index()),
            "single_data" => println!("{:?}", create_single_data_index()),
            // "thalia" => println!("{:?}", create_thalia_index()),
            "thalia_big" => println!("{:?}", create_thalia_index_big()),
            "thalia_alt" => println!("{:?}", create_thalia_index_alt()),
            "thalia_shards" => println!("{:?}", create_thalia_index_shards()),
            _ => {}
        };
    }

}

static TAHLIA_INDICES: &str = r#"
{
    "GENRE":        { "facet":true  },
    "VERLAG[]":     { "facet":true  },
    "ISMTITLE":     { "fulltext":{"tokenize":true}  },
    "ISMORIGTITLE": { "fulltext":{"tokenize":true}  },
    "ISMSUBTITLE1": { "fulltext":{"tokenize":true}  },
    "ISMSUBTITLE2": { "fulltext":{"tokenize":true}  },
    "ISMSUBTITLE3": { "fulltext":{"tokenize":true}  },
    "ISMARTIST":    { "fulltext":{"tokenize":true}, "facet":true  },
    "ISMMEDIATYPE": { "fulltext":{"tokenize":false} },
    "MEINS":        { "fulltext":{"tokenize":false} },
    "MTART":        { "fulltext":{"tokenize":false} },
    "MATNR":        { "fulltext":{"tokenize":false} },
    "ZZSW2_KEY":    { "fulltext":{"tokenize":false} },
    "ZZONLINE_KAT": { "fulltext":{"tokenize":false} },
    "ZZMYINFOARTNR":{ "fulltext":{"tokenize":false} },
    "ISMLANGUAGES": { "fulltext":{"tokenize":false}, "facet":true },
    "ISMPUBLDATE":  { "fulltext":{"tokenize":false} },
    "EAN11":        { "fulltext":{"tokenize":false} },
    "ISMORIDCODE":  { "fulltext":{"tokenize":false} }
}
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

#[cfg(not(enable_cpuprofiler))]
fn start_profiler(_: &str) {}
#[cfg(not(enable_cpuprofiler))]
fn stop_profiler() {}

#[cfg(feature = "enable_cpuprofiler")]
fn start_profiler(name: &str) {
    use cpuprofiler::PROFILER;
    PROFILER.lock().unwrap().start(name).unwrap();
}

#[cfg(feature = "enable_cpuprofiler")]
fn stop_profiler() {
    use cpuprofiler::PROFILER;
    PROFILER.lock().unwrap().stop().unwrap();
}

#[allow(dead_code)]
fn create_thalia_index_big() -> Result<(), io::Error> {
    // let mut f = File::open("data")?;
    // let mut json = String::new();
    // f.read_to_string(&mut json)?;

    start_profiler("./my-prof.profile");

    search_lib::create::create_indices_from_file(
        &mut search_lib::persistence::Persistence::create("thalia_new".to_string()).unwrap(),
        "data",
        TAHLIA_INDICES,
        None,
        false,
    ).unwrap();

    stop_profiler();
    // PROFILER.lock().unwrap().stop().unwrap();
    // search_lib::create::create_indices_from_str(
    //     &mut search_lib::persistence::Persistence::create("thalia_new".to_string()).unwrap(),
    //     &json,
    //     TAHLIA_INDICES,
    //     None,
    //     false,
    // ).unwrap();
    // File::create("MATNR").unwrap().write_all(all_terms.join("\n").as_bytes()).unwrap();

    Ok(())
}

#[allow(dead_code)]
fn create_thalia_index_alt() -> Result<(), io::Error> {
    start_profiler("./my-prof.profile");

    search_lib::create::create_indices_from_file(
        &mut search_lib::persistence::Persistence::create("thalia_alt".to_string()).unwrap(),
        "ESaaS_DATA_ESA020_20180713_Z06ST_MARA_COLUMNS_1_to_5000.txt",
        "[]",
        None,
        false,
    ).unwrap();

    stop_profiler();
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
            false,
        ).unwrap();
        println!("created shard num {:?}", &shard_num);
    });

    Ok(())
}

#[allow(dead_code)]
fn create_jmdict_index_shards() -> Result<(), io::Error> {
    // let mut _s = String::new();
    // let f = File::open("jmdict_split.json")?;

    // print_time!("jmdict_index_shards");

    // let mut num = 0;
    // for line in std::io::BufReader::new(&f).lines() {
    //     if num % 100 == 0 {
    //         println!("{:?}", num);
    //     }
    //     num+=1;
    //     jmdict_shards.insert(&line?, JMDICT_INDICES);
    // }

    let threshold_bytes: usize = std::env::args().nth(2).expect("require command line parameter").parse().unwrap();
    let mut jmdict_shards = search_lib::shards::Shards::new("jmdict_shards".to_string());

    let start = std::time::Instant::now();
    let mut lines = String::new();
    let mut total_bytes = 0;
    for line in std::io::BufReader::new(File::open("jmdict_split.json")?).lines().take(threshold_bytes) {
        let line = line?;
        lines += &line;
        lines += "\n";

        total_bytes += line.len();
        if lines.len() > threshold_bytes {
            jmdict_shards.insert(&lines, JMDICT_INDICES).unwrap();
            lines.clear();
        }
    }

    // println!("num kbytes {:?}", lines.len()/1000);

    // let mut persistence = search_lib::persistence::Persistence::create_type("single_data".to_string(), search_lib::persistence::PersistenceType::Persistent).unwrap();
    //     search_lib::create::create_indices_from_str(&mut persistence, &lines, JMDICT_INDICES, None, false).unwrap();

    let time_in_ms = (start.elapsed().as_secs() as f64 * 1_000.0) + (f64::from(start.elapsed().subsec_nanos()) / 1_000_000.0);

    let mbs = total_bytes as f64 / 1_000_000.;
    println!("total_bytes {:?}", total_bytes);
    println!("time_in_s {:?}", time_in_ms / 1_000.);
    println!("MB/s {:?}", mbs / (time_in_ms as f64 / 1000.));

    Ok(())
}

const JMDICT_INDICES: &str = r#"
{

    "commonness":               { "boost":{ "boost_type": "int" }},
    "meanings.ger[].rank":      { "boost":{ "boost_type": "int" }},
    "kanji[].commonness":       { "boost":{ "boost_type": "int" }},
    "kana[].commonness":        { "boost":{ "boost_type": "int" }},
    "kanji[].text":             { "fulltext":{"tokenize":false} },
    "kanji[].conjugated[].form":{ "fulltext":{"tokenize":false} },
    "kana[].text" :             { "fulltext":{"tokenize":false} },
    "kana[].conjugated[].form" :{ "fulltext":{"tokenize":false} },
    "kana[].romaji" :           { "fulltext":{"tokenize":true} },
    "meanings.ger[].text":      { "fulltext":{"tokenize":true} },
    "meanings.eng[]":           { "fulltext":{"tokenize":true} },
    "pos":                      { "fulltext":{"tokenize":false} }
}
"#;

#[allow(dead_code)]
fn create_jmdict_index() -> Result<(), io::Error> {
    // PROFILER.lock().unwrap().start("./my-prof.profile").unwrap();
    // let stream1 = Deserializer::from_reader(std::io::BufReader::new(&f)).into_iter::<Value>();
    // let stream2 = Deserializer::from_reader(std::io::BufReader::new(&f2)).into_iter::<Value>();

    // let stream1 = std::io::BufReader::new(File::open("jmdict_split.json")?)
    //     .lines()
    //     .map(|line| serde_json::from_str(&line.unwrap()));
    // let stream2 = std::io::BufReader::new(File::open("jmdict_split.json")?)
    //     .lines()
    //     .map(|line| serde_json::from_str(&line.unwrap()));
    // let stream3 = std::io::BufReader::new(File::open("jmdict_split.json")?).lines().map(|line| line.unwrap());

    // let stream2 = Deserializer::from_reader(File::open("jmdict_split.json")?).into_iter::<Value>();

    search_lib::create::create_indices_from_file(
        &mut search_lib::persistence::Persistence::create("jmdict".to_string()).unwrap(),
        "jmdict_split.json",
        JMDICT_INDICES,
        None,
        false,
    ).unwrap();

    // let mut f = File::open("jmdict.json")?;
    // let mut s = String::new();
    // f.read_to_string(&mut s)?;

    // search_lib::create::create_indices_from_str(
    //     &mut search_lib::persistence::Persistence::create("jmdict".to_string()).unwrap(),
    //     &s,
    //     indices,
    //     None,
    //     false,
    // ).unwrap();
    // PROFILER.lock().unwrap().stop().unwrap();
    Ok(())
}

#[allow(dead_code)]
fn create_single_data_index() -> Result<(), io::Error> {
    info_time!("create_single_data_index");

    let num: usize = std::env::args().nth(2).expect("require command line parameter").parse().unwrap();

    let mut lines = String::new();
    for line in std::io::BufReader::new(File::open("jmdict_split.json")?).lines().take(num) {
        lines += &line?;
        lines += "\n";
    }

    println!("num kbytes {:?}", lines.len() / 1000);

    let start = std::time::Instant::now();

    let mut persistence = search_lib::persistence::Persistence::create_type("single_data".to_string(), search_lib::persistence::PersistenceType::Persistent).unwrap();
    search_lib::create::create_indices_from_str(&mut persistence, &lines, JMDICT_INDICES, None, false).unwrap();

    let time_in_ms = (start.elapsed().as_secs() as f64 * 1_000.0) + (f64::from(start.elapsed().subsec_nanos()) / 1_000_000_f64);

    let mbs = lines.len() as f64 / 1_000_000_f64;
    println!("MB/s {:?}", mbs / (time_in_ms as f64 / 1000_f64));

    Ok(())
}

#[allow(dead_code)]
fn create_single_data_index_() -> Result<(), io::Error> {
    info_time!("create_single_data_index");
    let indices = "[]";

    for _i in 0..1 {
        let books = (0..1000)
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

        let mut persistence = search_lib::persistence::Persistence::create_type("single_data".to_string(), search_lib::persistence::PersistenceType::Transient).unwrap();
        search_lib::create::create_indices_from_str(&mut persistence, &serde_json::to_string_pretty(&books).unwrap(), indices, None, false).unwrap();
    }

    Ok(())
}

#[allow(dead_code)]
fn create_book_index() -> Result<(), io::Error> {
    let indices = "[]";
    let mut f = File::open("1342-0.txt")?;
    let mut s = String::new();
    f.read_to_string(&mut s)?;

    let doc = json!({"title":"PRIDE AND PREJUDICE", "content":s});
    let mut books = doc.to_string() + "\n";

    for _ in 0..7 {
        let double = books.to_string();
        books += &double;
    }

    // let books = (0..100).map(|_el| doc_str).collect::<Vec<_>>();

    search_lib::create::create_indices_from_str(
        &mut search_lib::persistence::Persistence::create("gutenberg".to_string()).unwrap(),
        &serde_json::to_string_pretty(&books).unwrap(),
        indices,
        None,
        false,
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
