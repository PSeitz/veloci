#![recursion_limit = "128"]
#[macro_use]
extern crate criterion;
extern crate json_converter;
#[macro_use]
extern crate serde_json;

use criterion::Criterion;

use json_converter::for_each_element;
use json_converter::IDHolder;
use serde_json::{Deserializer, Value};

fn criterion_benchmark(c: &mut Criterion) {
    let long_string: Vec<serde_json::Value> = (0..1000)
        .map(|_| {
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
        })
        .collect();

    let mut id_holder = IDHolder::new();

    let data = json!(long_string);
    let data_str = serde_json::to_string(&data).unwrap();

    c.bench_function("walk json", move |b| {
        b.iter(|| {
            let mut cb_text = |_anchor_id: u32, _value: &str, _path: &str, _parent_val_id: u32| {
                // println!("TEXT: path {} value {} parent_val_id {}",path, value, parent_val_id);
            };
            let mut callback_ids = |_anchor_id: u32, _path: &str, _val_id: u32, _parent_val_id: u32| {
                // println!("IDS: path {} val_id {} parent_val_id {}",path, val_id, parent_val_id);
            };

            let stream = Deserializer::from_str(&data_str).into_iter::<Value>();
            for_each_element(stream, &mut id_holder, &mut cb_text, &mut callback_ids);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
