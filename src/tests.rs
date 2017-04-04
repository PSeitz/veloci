#[cfg(test)]
mod tests {
    extern crate env_logger;

    #[allow(unused_imports)]
    use doc_loader;
    #[allow(unused_imports)]
    use util;
    #[allow(unused_imports)]
    use persistence;
    #[allow(unused_imports)]
    use util::normalize_text;
    #[allow(unused_imports)]
    use create;
    #[allow(unused_imports)]
    use search;
    #[allow(unused_imports)]
    use serde_json;
    #[allow(unused_imports)]
    use serde_json::Value;
    use std::fs::File;
    use std::fs;
    use std::io::prelude::*;


    static TEST_DATA:&str = r#"[
        {
            "commonness": 20,
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
                "ger": ["Wollen (n)", "Wille (m)", "Begeisterung (f)"]
            },
            "ent_seq": "1587690"
        },
        {
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
                "ger": ["der test"]
            },
            "ent_seq": "1587700"
        },
        {
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
                "ger": [
                    "welch"
                ]
            },
            "ent_seq": "1920240"
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
    ]"#;

    static TEST_FOLDER:&str = "mochaTest";

    // #[test]
    // fn it_super_duper_works() {
    //     assert_eq!(normalize_text("Hello"), "Hello");
    // }

    // #[test]
    // fn creates_da_indexo() {
    //     assert_eq!(normalize_text("Hello"), "Hello");
    // }


    
    #[test]#[ignore]
    fn test_paths() {
        let paths = util::get_steps_to_anchor("meanings.ger[]");
        println!("{:?}", paths);
        
    }

    // #[test]#[ignore]
    // fn test_write_index() {
    //     let ele:Vec<u32> = vec![3, 3, 3, 7];
    //     println!("{:?}", persistence::write_index(&ele, "testbug"));
    //     let ele2 = persistence::load_index("testbug").unwrap();
    //     println!("{:?}", ele2);
    //     assert_eq!(ele, ele2);
    //     println!("{:?}", fs::remove_file("testbug"));
    // }

    // #[test]#[ignore]
    // fn test_write_index_64() {
    //     let ele:Vec<u64> = vec![3_000_000_000_000, 3, 3, 7];
    //     println!("{:?}", persistence::write_index64(&ele, "test64"));
    //     let ele2 = persistence::load_index_64("test64").unwrap();
    //     println!("{:?}", ele2);
    //     assert_eq!(ele, ele2);
    //     println!("{:?}", fs::remove_file("test64"));
    // }

    #[test]#[ignore]
    fn test_binary_search() {
        let x = vec![1,2,3,6,7,8];
        let u =  x.binary_search(&4).unwrap_err();;
        println!("{:?}", u);
        let value = match x.binary_search(&4) { Ok(value) => value,Err(value) => value};
        println!("mjjaaa {}", value);
    }

    #[test]
    fn test_json_request() {
        warn!("can log from the test too");
        let requesto: search::Request = serde_json::from_str(r#"{"search":{"path":"asdf", "term": "asdf", "levenshtein_distance":1}}"#).unwrap();
        println!("mjjaaa {:?}", requesto);
        assert_eq!(requesto.search.unwrap().levenshtein_distance, Some(1));
    }

    #[test]
    fn create_indices_1() {
        let _ = env_logger::init();
        {

            let indices = r#"
            [
                { "boost":"commonness" , "options":{"boost_type":"int"}},
                { "fulltext":"ent_seq" },
                { "boost":"field1[].rank" , "options":{"boost_type":"int"}},
                { "fulltext":"field1[].text" },
                { "fulltext":"kanji[].text" },
                { "fulltext":"meanings.ger[]", "options":{"tokenize":true, "stopwords": ["stopword"]} },
                { "fulltext":"meanings.eng[]", "options":{"tokenize":true} },
                { "boost":"kanji[].commonness" , "options":{"boost_type":"int"}},
                { "boost":"kana[].commonness", "options":{"boost_type":"int"} }
            ]
            "#;
            // let indices = r#"
            // [
            //     { "fulltext":"meanings.ger[]", "options":{"tokenize":true, "stopwords": ["stopword"]} }
            // ]
            // "#;
            println!("{:?}", create::create_indices(TEST_FOLDER, TEST_DATA, indices));

            let meta_data = persistence::MetaData::new(TEST_FOLDER);
            println!("{:?}", persistence::load_all(&meta_data));

        }

    //     assert_eq!(normalize_text("Hello"), "Hello");
    // }

    // #[test]
    // fn should_search_tokenized_and_levensthein() {

    // fn search_test(req: Value) -> Vec<search::Hit> {
    //     let requesto: search::Request = serde_json::from_str(&req.to_string()).unwrap();
    //     search::search(TEST_FOLDER, requesto, 0, 10).unwrap()
    // }

    fn search_test_to_doc(req: Value) -> Result<Vec<search::DocWithHit>, search::SearchError>  {
        let requesto: search::Request = serde_json::from_str(&req.to_string()).unwrap();
        let hits = search::search(TEST_FOLDER, requesto, 0, 10)?;
        Ok(search::to_documents(&hits, TEST_FOLDER))
    }

        {
            let req = json!({
                "search": {
                    "term":"majestätischer",
                    "path": "meanings.ger[]",
                    "levenshtein_distance": 1,
                    "firstCharExactMatch": true
                }
            });

            let hits = search_test_to_doc(req).unwrap();
            assert_eq!(hits.len(), 1);
        }

        { // should search without firstCharExactMatch
            let req = json!({
                "search": {
                    "term":"najestätischer",
                    "path": "meanings.ger[]",
                    "levenshtein_distance": 1
                }
            });
            let hits = search_test_to_doc(req).unwrap();

            // println!("hits {:?}", hits);
            assert_eq!(hits.len(), 1);
        }

        { // 'should prefer exact matches to tokenmatches'

            let req = json!({
                "search": {
                    "term":"will",
                    "path": "meanings.eng[]",
                    "levenshtein_distance": 1
                }
            });
            let wa = search_test_to_doc(req).unwrap();

            // serde_json::from_str(data_str).unwrap();
            // println!("wa {}", wa[0]);
            let doc1:Value = serde_json::from_str(&wa[0].doc).unwrap();
            assert_eq!(doc1["meanings"]["eng"][0], "will");
        }

        { // 'should search word non tokenized'
            let req = json!({
                "search": {
                    "term":"偉容",
                    "path": "kanji[].text",
                    "levenshtein_distance": 0,
                    "firstCharExactMatch":true
                }
            });

            let hits = search_test_to_doc(req);
            assert_eq!(hits.unwrap().len(), 1);
        }

        { // 'should search on non subobject'
            let req = json!({
                "search": {
                    "term":"1587690",
                    "path": "ent_seq",
                    "levenshtein_distance": 0,
                    "firstCharExactMatch":true
                }
            });

            let hits = search_test_to_doc(req);
            assert_eq!(hits.unwrap().len(), 1);
        }

        { // 'AND connect hits same field'
            let req = json!({
                "and":[
                    {"search": {
                        "term":"aussehen",
                        "path": "meanings.ger[]",
                        "levenshtein_distance":0
                    }},
                    {"search": {
                        "term":"majestätisches",
                        "path": "meanings.ger[]",
                        "levenshtein_distance":0
                    }}
                ]
            });

            let hits = search_test_to_doc(req);
            assert_eq!(hits.unwrap().len(), 1);
        }

        { // AND connect hits different fields
            let req = json!({
                "and":[
                    {"search": {
                        "term":"majestät",
                        "path": "meanings.ger[]",
                        "levenshtein_distance":0
                    }},
                    {"search": {
                        "term":"majestic",
                        "path": "meanings.eng[]",
                        "levenshtein_distance":0
                    }}
                ]
            });

            let hits = search_test_to_doc(req);
            assert_eq!(hits.unwrap().len(), 1);
        }

        { // AND connect hits different fields - no hit
            let req = json!({
                "and":[
                    {"search": {
                        "term":"majestät",
                        "path": "meanings.ger[]",
                        "levenshtein_distance":0
                    }},
                    {"search": {
                        "term":"urge",
                        "path": "meanings.eng[]",
                        "levenshtein_distance":0
                    }}
                ]
            });

            let hits = search_test_to_doc(req);
            assert_eq!(hits.unwrap().len(), 0);
        }

        { // OR connect hits
            let _ = env_logger::init();
            let req = json!({
                "or":[
                    {"search": {
                        "term":"majestät",
                        "path": "meanings.ger[]",
                        "levenshtein_distance":0
                    }},
                    {"search": {
                        "term":"urge",
                        "path": "meanings.eng[]",
                        "levenshtein_distance":0
                    }}
                ]
            });

            let hits = search_test_to_doc(req);
            assert_eq!(hits.unwrap().len(), 2);
        }

    }

    // #[test]
    // fn checked_was_abgehst_22() {
        
    //     let small_test_json:&str = r#"[
    //         {
    //             "meanings": {
    //                 "eng" : ["dignity", "majestic appearance", "will test"],
    //                 "ger": ["majestätischer Anblick (m)", "stopword", "majestätisches Aussehen (n)", "Majestät (f)"]
    //             },
    //             "ent_seq": "1587680"
    //         }
    //     ]"#;

    //     let indices = r#"
    //     [
    //         { "fulltext":"meanings.ger[]", "options":{"tokenize":true, "stopwords": ["stopword"]} }
    //     ]
    //     "#;

    //     println!("{:?}", create::create_indices("rightTerms", small_test_json, indices));

    //     assert_eq!(normalize_text("Hello"), "Hello");

    //     let mut f = File::open("meanings.ger[]").unwrap();
    //     let mut s = String::new();
    //     f.read_to_string(&mut s).unwrap();

    //     let lines = s.lines().collect::<Vec<_>>();
    //     println!("{:?}", lines);
    //     let text = vec!["Anblick", "Aussehen", "Majestät", "majestätischer", "majestätischer Anblick", "majestätisches", "majestätisches Aussehen"];
    //     assert_eq!(lines, text);

    // }

    #[test]
    fn create_and_delete_file_in_subfolder() {
        fs::create_dir_all("subFolder1").unwrap();
        let some_terms = vec!["yep, yep"];
        File::create("subFolder1/test1").unwrap().write_all(some_terms.join("\n").as_bytes()).unwrap();
        assert_eq!("lines", "lines");
        println!("{:?}", fs::remove_dir_all("subFolder1"));
    }



}




// #[cfg(test)]
// mod test {
//     use create;
//     use serde_json;
//     use serde_json::Value;

//     #[test]
//     fn test_ewwwwwwwq() {

//         let opt: create::FulltextIndexOptions = serde_json::from_str(r#"{"tokenize":true, "stopwords": []}"#).unwrap();
//         // let opt = create::FulltextIndexOptions{
//         //     tokenize: true,
//         //     stopwords: vec![]
//         // };

//         let dat2 = r#" [{ "name": "John Doe", "age": 43 }, { "name": "Jaa", "age": 43 }] "#;
//         let data: Value = serde_json::from_str(dat2).unwrap();
//         let res = create::create_fulltext_index(&data, "name", opt);
//         println!("{:?}", res);
//         let deserialized: create::BoostIndexOptions = serde_json::from_str(r#"{"boost_type":"int"}"#).unwrap();

//         assert_eq!("Hello", "Hello");

//         let service: create::CreateIndex = serde_json::from_str(r#"{"boost_type":"int"}"#).unwrap();
//         println!("service: {:?}", service);

//     }
// }
