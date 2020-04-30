#![recursion_limit = "128"]
#[macro_use]
extern crate criterion;

#[macro_use]
extern crate serde_json;

use criterion::Criterion;
use json_converter::IDProvider;

use json_converter::{for_each_element, for_each_elemento, for_each_text, for_each_texto, IDHolder};

fn get_test_json() -> serde_json::Value {
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
                    {"form": "見なかった", "name": "past negative"}
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
                    {"form": "観よう", "name": "pseudo futurum"},
                    {"form": "観て", "name": "te form"},
                    {"form": "観るな", "name": "negative imperative"},
                    {"form": "観-", "name": "conjunctive"},
                    {"form": "観た", "name": "past"},
                    {"form": "観ろ", "name": "commanding"}
                ],
                "ent_seq": "1259290",
                "readings": [
                    "みる"
                ],
                "text": "観る"
            },
            {
                "commonness": 0,
                "conjugated": [
                    {"form": "視ろ", "name": "commanding"}
                ],
                "ent_seq": "1259290",
                "readings": [
                    "みる"
                ],
                "text": "視る"
            }
        ],
        "meanings": {
            "eng": ["see", "look", "watch", "view", "observe", "look over", "look on", "assess", "examine", "judge", "look after", "keep an eye on", "take care of", "view (e.g. flowers, movie)", "try", "try out", "see that...", "find that..."],
            "ger": [
                {"text": "betrachten"}, {"text": "anschauen"}, {"rank": 1, "text": "sehen"},
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
}

fn get_json_test_data_line_seperated() -> String {
    let json_values: Vec<serde_json::Value> = (0..500).map(|_| get_test_json()).collect();

    let mut json_string_line_seperated = String::new();
    for val in json_values {
        json_string_line_seperated.push_str(&serde_json::to_string(&val).unwrap());
        json_string_line_seperated.push_str("\n");
    }

    json_string_line_seperated
}

fn criterion_benchmark(c: &mut Criterion) {
    // let json_string_line_seperated = get_json_test_data_line_seperated();

    c.bench_function("walk json", move |b| {
        b.iter(|| {
            let mut cb_text = |_anchor_id: u32, _value: &str, _path: &str, _parent_val_id: u32| -> Result<(), serde_json::Error> {
                // println!("TEXT: path {} value {} parent_val_id {}",path, value, parent_val_id);
                Ok(())
            };
            let mut cb_ids = |_anchor_id: u32, _path: &str, _val_id: u32, _parent_val_id: u32| -> Result<(), serde_json::Error> {
                // println!("IDS: path {} val_id {} parent_val_id {}",path, val_id, parent_val_id);
                Ok(())
            };

            let mut id_provider = IDHolder::new();

            let test_json = get_test_json();
            let mut path = String::with_capacity(25);
            for _ in 0..500 {
                let root_id = id_provider.get_id("");
                for_each_elemento(&test_json, root_id, &mut id_provider, root_id, &mut path, "", &mut cb_text, &mut cb_ids).unwrap();
                path.clear();
            }

            // let stream = json_string_line_seperated.lines().map(|line| serde_json::from_str(&line));
            // // let stream = Deserializer::from_str(&data_str).into_iter::<Value>();
            // for_each_element(stream, &mut id_holder, &mut cb_text, &mut callback_ids).unwrap();
        })
    });

    // let _json_string_line_seperated = get_json_test_data_line_seperated();

    c.bench_function("walk json text", move |b| {
        b.iter(|| {
            let mut cb_text = |_value: &str, _path: &str| -> Result<(), serde_json::Error> {
                // println!("TEXT: path {} value {} parent_val_id {}",path, value, parent_val_id);
                Ok(())
            };

            let test_json = get_test_json();
            let mut path = String::with_capacity(25);
            for _ in 0..500 {
                for_each_texto(&test_json, &mut path, "", &mut cb_text).unwrap();
                path.clear();
            }

            // let stream = json_string_line_seperated.lines().map(|line| serde_json::from_str(&line));
            // for_each_text(stream, &mut cb_text).unwrap();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
