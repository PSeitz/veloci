#![recursion_limit = "128"]

#[macro_use]
extern crate measure_time;

use search_lib;
#[macro_use]
extern crate serde_json;

#[cfg(feature = "enable_cpuprofiler")]
extern crate cpuprofiler;

use std::{
    fs::File,
    io::{self, prelude::*},
    str,
};

#[allow(unused_imports)]
use rayon::prelude::*;

fn main() {
    search_lib::trace::enable_log();
    std::env::args().nth(1).expect("require command line parameter");

    for jeppo in std::env::args().skip(1) {
        match jeppo.as_ref() {
            "jmdict" => info!("{:?}", create_jmdict_index()),
            // "jmdict_shards" => info!("{:?}", create_jmdict_index_shards()),
            "gutenberg" => info!("{:?}", create_book_index()),
            _ => {}
        };
    }
}

// #[allow(dead_code)]
// fn create_jmdict_index_shards() -> Result<(), io::Error> {
//     let threshold_bytes: usize = std::env::args().nth(2).expect("require command line parameter").parse().unwrap();
//     let mut jmdict_shards = search_lib::shards::Shards::new("jmdict_shards".to_string());

//     let start = std::time::Instant::now();
//     let mut lines = String::new();
//     let mut total_bytes = 0;
//     for line in std::io::BufReader::new(File::open("jmdict_split.json")?).lines().take(threshold_bytes) {
//         let line = line?;
//         lines += &line;
//         lines += "\n";

//         total_bytes += line.len();
//         if lines.len() > threshold_bytes {
//             jmdict_shards.insert(&lines, JMDICT_INDICES).unwrap();
//             lines.clear();
//         }
//     }

//     let time_in_ms = (start.elapsed().as_secs() as f64 * 1_000.0) + (f64::from(start.elapsed().subsec_nanos()) / 1_000_000.0);

//     let mbs = total_bytes as f64 / 1_000_000.;
//     info!("total_bytes {:?}", total_bytes);
//     info!("time_in_s {:?}", time_in_ms / 1_000.);
//     info!("MB/s {:?}", mbs / (time_in_ms as f64 / 1000.));

//     Ok(())
// }

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

    search_lib::create::create_indices_from_file(
        &mut search_lib::persistence::Persistence::create("jmdict".to_string()).unwrap(),
        "jmdict_split.json",
        JMDICT_INDICES,
        None,
        false,
    )
    .unwrap();

    // PROFILER.lock().unwrap().stop().unwrap();
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

    search_lib::create::create_indices_from_str(
        &mut search_lib::persistence::Persistence::create("gutenberg".to_string()).unwrap(),
        &serde_json::to_string_pretty(&books).unwrap(),
        indices,
        None,
        false,
    )
    .unwrap();
    Ok(())
}
