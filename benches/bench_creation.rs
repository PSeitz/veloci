#![feature(test)]

#[macro_use]
extern crate criterion;
extern crate veloci;
#[macro_use]
extern crate serde_json;
extern crate test;

#[cfg(test)]
mod bench_creation {

    use criterion::Criterion;

    use serde_json;
    use serde_json::Value;
    use std::fs::File;
    use std::io::prelude::*;
    use veloci::*;

    use test;
    #[bench]
    fn name(b: &mut test::Bencher) {
        let indices = r#"
        [
            {
                "boost": "commonness",
                "options": { "boost_type": "f32" }
            },
            { "fulltext": "kanji[].text", "options":{"tokenize":false} },
            { "fulltext": "kanji[].conjugated[].form", "options":{"tokenize":false} },
            { "fulltext": "kana[].text" , "options":{"tokenize":false} },
            { "fulltext": "kana[].conjugated[].form" , "options":{"tokenize":false} },
            { "fulltext": "kana[].romaji" , "options":{"tokenize":true} },
            {
                "fulltext": "meanings.ger[].text",
                "options": { "tokenize": true }
            },
            {
                "fulltext": "meanings.eng[]",
                "options": { "tokenize": true }
            },
            {
                "fulltext": "pos",
                "options": { "tokenize": false }
            },
            {
                "boost": "meanings.ger[].rank",
                "options": { "boost_type": "f32" }
            },
            {
                "boost": "kanji[].commonness",
                "options": { "boost_type": "f32" }
            },
            {
                "boost": "kana[].commonness",
                "options": { "boost_type": "f32" }
            }
        ]
        "#;
        let mut f = File::open("create_performance_big.json").unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();
        let test_data: Value = serde_json::from_str(&s).unwrap();
        // let test_data = get_test_data();

        b.iter(|| {
            create::create_indices_json("create_perf", &test_data, indices);
        })
    }
}

// fn criterion_benchmark(c: &mut Criterion) {

//     let indices = r#"
//     [
//         {
//             "boost": "commonness",
//             "options": { "boost_type": "f32" }
//         },
//         { "fulltext": "kanji[].text", "options":{"tokenize":false} },
//         { "fulltext": "kanji[].conjugated[].form", "options":{"tokenize":false} },
//         { "fulltext": "kana[].text" , "options":{"tokenize":false} },
//         { "fulltext": "kana[].conjugated[].form" , "options":{"tokenize":false} },
//         { "fulltext": "kana[].romaji" , "options":{"tokenize":true} },
//         {
//             "fulltext": "meanings.ger[].text",
//             "options": { "tokenize": true }
//         },
//         {
//             "fulltext": "meanings.eng[]",
//             "options": { "tokenize": true }
//         },
//         {
//             "fulltext": "pos",
//             "options": { "tokenize": false }
//         },
//         {
//             "boost": "meanings.ger[].rank",
//             "options": { "boost_type": "f32" }
//         },
//         {
//             "boost": "kanji[].commonness",
//             "options": { "boost_type": "f32" }
//         },
//         {
//             "boost": "kana[].commonness",
//             "options": { "boost_type": "f32" }
//         }
//     ]
//     "#;

//     let mut f = File::open("create_performance_big.json").unwrap();
//     let mut s = String::new();
//     f.read_to_string(&mut s).unwrap();
//     let test_data: Value = serde_json::from_str(&s).unwrap();
//     // let test_data = get_test_data();

//     Criterion::default()
//         .bench_function("create indices performance", |b| b.iter(|| {
//             create::create_indices_json("create_perf", &test_data, indices);
//         }));
// }

// criterion_group!(benches, criterion_benchmark);
// criterion_main!(benches);

pub fn get_test_data() -> serde_json::Value {
    json!([
        {
            "commonness": 123456,
            "ent_seq": "99999"
        },
        {
            "nofulltext":"my tokens",
            "commonness": 20,
            "tags": ["nice", "cool"],
            "kanji": [
                { "text": "偉容", "commonness": 0},
                { "text": "威容","commonness": 5}
            ],
            "kana": [
                {
                    "text": "いよう",
                    "romaji": "Iyou",
                    "commonness": 5
                }
            ],
            "meanings": {
                "eng" : ["dignity", "majestic appearance", "will test"],
                "ger": ["majestätischer Anblick (m)", "majestätisches Aussehen (n)", "Majestät (f)"]
            },
            "ent_seq": "1587680"
        },
        {
            "commonness": 20,
            "tags": ["nice", "cool"],
            "kanji": [
                { "text": "意欲", "commonness": 40},
                { "text": "意慾", "commonness": 0}
            ],
            "kana": [
                {
                    "text": "いよく",
                    "romaji": "Iyoku",
                    "commonness": 40
                }
            ],
            "meanings": {
                "eng" : ["will", "desire", "urge", "having a long torso"],
                "ger": ["Wollen (n)", "Wille (m)", "Begeisterung (f)", "begeistern"]
            },
            "ent_seq": "1587690"
        },
        {
            "id": 1234566,
            "gender": "male",
            "tags": ["awesome", "cool"],
            "birthDate": "1960-08-19",
            "address": [
                {
                    "line": ["nuts strees"]
                },
                {
                    "line": ["asdf"]
                }
            ],
            "commonness": 500,
            "kanji": [
                { "text": "意慾", "commonness": 20}
            ],
            "field1" : [{"text":"awesome", "rank":1}],
            "kana": [
                {
                    "text": "いよく"
                }
            ],
            "meanings": {
                "eng" : ["test1"],
                "ger": ["der test", "das ist ein guter Treffer"]
            },
            "ent_seq": "1587700"
        },
        {
            "id": 123456,
            "gender": "female",
            "birthDate": "1950-08-19",
            "address": [
                {
                    "line": [
                        "71955 Ilene Brook"
                    ]
                }
            ],
            "commonness": 551,
            "kanji": [
                {
                    "text": "何の",
                    "commonness": 526
                }
            ],
            "field1" : [{"text":"awesome"}, {"text":"nixhit"}],
            "kana": [
                {
                    "text": "どの",
                    "romaji": "Dono",
                    "commonness": 25
                }
            ],
            "meanings": {
                "ger": ["welch", "guter nicht Treffer", "alle meine Words"]
            },
            "ent_seq": "1920240",
            "mylongtext": "Prolog:\nthis is a story of a guy who went out to rule the world, but then died. the end"
        },
        {
            "pos": [
                "adj-i"
            ],
            "commonness": 1,
            "misc": [],
            "kanji": [
                {
                    "text": "柔らかい",
                    "commonness": 57
                }
            ],
            "kana": [
                {
                    "text": "やわらかい",
                    "romaji": "Yawarakai",
                    "commonness": 30
                }
            ],
            "meanings": {
                "ger": [
                    "(1) weich",
                    "stopword"
                ]
            },
            "ent_seq": "1605630"
        }
    ])
}
