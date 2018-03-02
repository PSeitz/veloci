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
    use search_field;
    use search;
    #[allow(unused_imports)]
    use serde_json;
    #[allow(unused_imports)]
    use serde_json::Value;
    use std::fs::File;
    use std::fs;
    use std::io::prelude::*;
    use trace;
    use parking_lot::RwLock;
    use chashmap::CHashMap;
    // use fnv::FnvHashMap;

    use facet;
    pub fn get_test_data() -> String {
        json!([
            {
                "commonness": 123456,
                "ent_seq": "99999",
                "tags": ["nice", "cool"]
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
                    "eng" : ["karlo","dignity", "majestic appearance", "will test"],
                    "ger": ["majestätischer Anblick (m)", "majestätisches Aussehen (n)", "Majestät (f)"]
                },
                "ent_seq": "1587680"
            },
            {
                "commonness": 20,
                "tags": ["nice"],
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
                    "eng" : ["will", "desire", "der große karl",  "urge", "having a long torso"],
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
                "tags": ["nice", "cool"],
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
                "tags": ["nice", "cool"],
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
            },
            {
                "meanings": {
                    "ger": [
                        "(1) 2 3 super nice weich" // add wich with no commonness
                    ]
                },
                "ent_seq": "9555"
            },
            {
                "sub_level": [{"text":"Prolog:\nthis is story of a guy who went out to rule the world, but then died. the end"}],
                "commonness": 515151,
                "ent_seq": "25",
                "tags": ["nice", "cool"]
            },
            {
                "commonness": 30,
                "ent_seq": "26",
                "tags": ["nice", "coolo"]
            },
            {
                "commonness": 20,
                "ent_seq": "27",
                "tags": ["Eis", "cool"]
            },
            {
                "commonness": 20,
                "ent_seq": "28",
                "tags": ["nice", "cool"]
            }
        ]).to_string()
    }

    static TOKEN_VALUE: &str = r#"[
        {
            "text": "Begeisterung",
            "value": 20
        }
    ]"#;

    static TEST_FOLDER: &str = "mochaTest";
    static INDEX_CREATED: RwLock<bool> = RwLock::new(false);
    lazy_static! {
        static ref PERSISTENCES: CHashMap<String, persistence::Persistence> = {
            CHashMap::default()
        };
    }

    #[test]
    fn test_paths() {
        let paths = util::get_steps_to_anchor("meanings.ger[]");
        println!("NAAA {:?}", paths);
    }

    #[test]
    #[ignore]
    fn test_binary_search() {
        let x = vec![1, 2, 3, 6, 7, 8];
        let u = x.binary_search(&4).unwrap_err();
        println!("{:?}", u);
        let value = match x.binary_search(&4) {
            Ok(value) => value,
            Err(value) => value,
        };
        println!("mjjaaa {}", value);
    }

    #[test]
    fn test_json_request() {
        let requesto: search::Request = serde_json::from_str(r#"{"search":{"path":"asdf", "terms":[ "asdf"], "levenshtein_distance":1}}"#).unwrap();
        println!("mjjaaa {:?}", requesto);
        assert_eq!(requesto.search.unwrap().levenshtein_distance, Some(1));
    }

    fn search_testo_to_doc(req: Value) -> search::SearchResultWithDoc {
        search_testo_to_doco(req).expect("search error")
    }

    fn search_testo_to_doco(req: Value) -> Result<search::SearchResultWithDoc, search::SearchError> {
        let pers = PERSISTENCES.get(&"default".to_string()).expect("Can't find loaded persistence");
        Ok(search::to_search_result(&pers, search_testo_to_hitso(req)?, None))
    }

    fn search_testo_to_hitso(req: Value) -> Result<search::SearchResult, search::SearchError> {
        let pers = PERSISTENCES.get(&"default".to_string()).expect("Can't find loaded persistence");
        let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
        let hits = search::search(requesto, &pers)?;
        Ok(hits)
    }

    describe! search_test {
        before_each {

            let mut INDEX_CREATEDO = INDEX_CREATED.write();
            {

                if !*INDEX_CREATEDO {
                    trace::enable_log();

                    // Start up a test.
                    let indices = r#"
                    [
                        { "facet":"tags[]"},
                        { "boost":"commonness" , "options":{"boost_type":"int"}},
                        { "fulltext":"ent_seq" },
                        { "boost":"field1[].rank" , "options":{"boost_type":"int"}},
                        { "fulltext":"field1[].text" },
                        { "fulltext":"kanji[].text" },
                        { "fulltext":"meanings.ger[]", "options":{"tokenize":true, "stopwords": ["stopword"]} },
                        { "fulltext":"meanings.eng[]", "options":{"tokenize":true} },
                        { "fulltext":"nofulltext", "options":{"tokenize":false} },
                        { "fulltext":"address[].line[]", "options":{"tokenize":true} },
                        { "boost":"kanji[].commonness" , "options":{"boost_type":"int"}},
                        { "boost":"kana[].commonness", "options":{"boost_type":"int"} }
                    ]
                    "#;
                    // let indices = r#"
                    // [
                    //     { "fulltext":"address[].line[]", "options":{"tokenize":true} }
                    // ]
                    // "#;
                    println!("{:?}", create::create_indices(TEST_FOLDER, &get_test_data(), indices));

                    {
                        let mut pers = persistence::Persistence::load(TEST_FOLDER.to_string()).expect("Could not load persistence");
                        // let mut pers = persistence::Persistence::load(TEST_FOLDER.to_string()).expect("Could not load persistence");
                        let config = json!({
                            "path": "meanings.ger[]"
                        });
                        create::add_token_values_to_tokens(&mut pers, TOKEN_VALUE, &config.to_string()).expect("Could not add token values");

                    }

                    PERSISTENCES.insert("default".to_string(), persistence::Persistence::load(TEST_FOLDER.to_string()).expect("could not load persistence"));

                    *INDEX_CREATEDO = true;
                }
            }
        }


        it "simple_search"{
            let req = json!({
                "search": {
                    "terms":["urge"],
                    "path": "meanings.eng[]"
                }
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0].doc["ent_seq"], "1587690");
        }

        it "two tokens hit the same anchor" {
            let req = json!({
                "search": {
                    "terms":["majestätischer"],
                    "path": "meanings.ger[]",
                    "levenshtein_distance": 1
                }
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0].doc["ent_seq"], "1587680");
        }

       it "deep structured objects" {

           let req = json!({
               "search": {
                   "terms":["brook"],
                   "path": "address[].line[]",
                   "levenshtein_distance": 1
               }
           });

           let hits = search_testo_to_doc(req).data;
           assert_eq!(hits.len(), 1);
           assert_eq!(hits[0].doc["id"], 123456);
       }


        it "should search without firstCharExactMatch"{
            let req = json!({
                "search": {
                    "terms":["najestätischer"],
                    "path": "meanings.ger[]",
                    "levenshtein_distance": 1
                }
            });
            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0].doc["ent_seq"], "1587680");
        }

        it "should prefer exact matches to tokenmatches'"{

            let req = json!({
                "search": {
                    "terms":["will"],
                    "path": "meanings.eng[]",
                    "levenshtein_distance": 1
                }
            });
            let wa = search_testo_to_doc(req).data;
            assert_eq!(wa[0].doc["meanings"]["eng"][0], "will");
        }

        it "should prefer exact tokenmatches to fuzzy text hits'"{

            let req = json!({
                "search": {
                    "terms":["karl"],
                    "path": "meanings.eng[]",
                    "levenshtein_distance": 1
                }
            });
            let wa = search_testo_to_doc(req).data;
            assert_eq!(wa[0].doc["meanings"]["eng"][0], "will");
        }

        it "should search word non tokenized'"{
            let req = json!({
                "search": {
                    "terms":["偉容"],
                    "path": "kanji[].text"
                }
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0].doc["ent_seq"], "1587680");
        }

        it "should check disabled tokenization"{
            let req = json!({
                "search": {
                    "terms":["tokens"],
                    "path": "nofulltext"
                }
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits.len(), 0);
        }

        it "should search on non subobject'"{
            let req = json!({
                "search": {
                    "terms":["1587690"],
                    "path": "ent_seq"
                }
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits.len(), 1);
        }

        it "AND connect hits same field"{
            let req = json!({
                "and":[
                    {"search": {"terms":["aussehen"],       "path": "meanings.ger[]"}},
                    {"search": {"terms":["majestätisches"], "path": "meanings.ger[]"}}
                ]
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0].doc["ent_seq"], "1587680");
        }

        it "AND connect hits different fields"{
            let req = json!({
                "and":[
                    {"search": {"terms":["majestät"], "path": "meanings.ger[]"}},
                    {"search": {"terms":["majestic"], "path": "meanings.eng[]"}}
                ]
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits.len(), 1);
        }

        it "AND connect hits different fields - no hit"{
            let req = json!({
                "and":[
                    {"search": {
                        "terms":["majestät"],
                        "path": "meanings.ger[]"
                    }},
                    {"search": {
                        "terms":["urge"],
                        "path": "meanings.eng[]"
                    }}
                ]
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits.len(), 0);
        }

        it "OR connect hits"{
            let req = json!({
                "or":[
                    {"search": {
                        "terms":["majestät"],
                        "path": "meanings.ger[]"
                    }},
                    {"search": {
                        "terms":["urge"],
                        "path": "meanings.eng[]"
                    }}
                ]
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits.len(), 2);
        }

        it "should find 2 values from token"{
            let req = json!({
                "search": {
                    "terms":["意慾"],
                    "path": "kanji[].text"
                }
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits.len(), 2);
        }
        it "should search and boosto"{
            let req = json!({
                "search": {
                    "terms":["意慾"],
                    "path": "kanji[].text"
                },
                "boost" : [{
                    "path":"kanji[].commonness",
                    "boost_fun": "Log10",
                    "param": 1
                }]
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits.len(), 2);
        }

        it "should search and double boost"{
            let req = json!({
                "search": {
                    "terms":["awesome"],
                    "path": "field1[].text"
                },
                "boost" : [{
                    "path":"commonness",
                    "boost_fun": "Log10",
                    "param": 1
                },
                {
                    "path":"field1[].rank",
                    "expression": "10 / $SCORE",
                    "skip_when_score" : [0]
                }]
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits.len(), 2);
        }

        it "should search and boost anchor"{
            let req = json!({
                "search": {
                    "terms":["意慾"],
                    "path": "kanji[].text",
                    "levenshtein_distance": 0,
                    "firstCharExactMatch":true
                },
                "boost" : [{
                    "path":"commonness",
                    "boost_fun": "Log10",
                    "param": 1
                }]
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits[0].doc["commonness"], 500);
        }

        // it('should suggest', function() {
        //     return searchindex.suggest({path:'meanings.ger[]', term:'majes'}).then(res => {
        //         // console.log(JSON.stringify(res, null, 2))
        //         return Object.keys(res)
        //     })
        //     .should.eventually.have.length(5)
        // })


        it "should use search on field for suggest without sorting etc."{
            let req = json!({
                "terms":["majes"],
                "path": "meanings.ger[]",
                "levenshtein_distance": 0,
                "starts_with":true,
                "return_term":true
            });
            let requesto: search::RequestSearchPart = serde_json::from_str(&req.to_string()).expect("Can't parse json");
            let mut pers = PERSISTENCES.get(&"default".to_string()).unwrap();
            let results = search_field::get_hits_in_field(&mut pers, requesto, None).unwrap();
            let mut all_terms = results.terms.values().collect::<Vec<&String>>();
            all_terms.sort();
            // assert_eq!(all_terms, ["majestät", "majestätischer", "majestätischer anblick", "majestätisches", "majestätisches aussehen"]);
            assert_eq!(all_terms, ["Majestät", "Majestät (f)", "majestätischer", "majestätischer Anblick (m)", "majestätisches", "majestätisches Aussehen (n)"]);
        }

        //TODO ENBALE
        // it "should load the text for ids"{
        //     let pers = PERSISTENCES.get(&"default".to_string()).unwrap();
        //     let mut faccess:persistence::FileSearch = pers.get_file_search("meanings.ger[].textindex");

        //     assert_eq!(faccess.get_text_for_id(11, pers.get_offsets("meanings.ger[].textindex").unwrap()), "Majestät" );
        //     assert_eq!(faccess.get_text_for_id(12, pers.get_offsets("meanings.ger[].textindex").unwrap()), "Majestät (f)" );
        //     assert_eq!(faccess.get_text_for_id(13, pers.get_offsets("meanings.ger[].textindex").unwrap()), "Treffer" );

        // }

        // it "should highlight ids"{
        //     let mut pers = PERSISTENCES.get(&"default".to_string()).unwrap();


        //     let inf = search::SnippetInfo{
        //         num_words_around_snippet :  4,
        //         max_snippets :  40,
        //         snippet_start_tag: "<b>".to_string(),
        //         snippet_end_tag: "</b>".to_string(),
        //         snippet_connector: " ... ".to_string(),
        //     };
        //     let results = search_field::highlight_document(&mut pers, "mylongtext.textindex", 13, &[9], &inf).unwrap();
        //     assert_eq!(results, "this is a <b>story</b> of a guy who  ... " );

        // }

        it "should highlight on field"{
            let req = json!({
                "terms":["story"],
                "path": "mylongtext",
                "levenshtein_distance": 0,
                "starts_with":true,
                "snippet":true,
                "top":10,
                "skip":0
            });
            let mut requesto: search::RequestSearchPart = serde_json::from_str(&req.to_string()).expect("Can't parse json");
            let mut pers = PERSISTENCES.get(&"default".to_string()).unwrap();
            let results = search_field::highlight(&mut pers, &mut requesto).unwrap();
            assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["Prolog:\nthis is a <b>story</b> of a guy who went ... "]);
        }

        it "should highlight on sub_level field"{
            let req = json!({
                "terms":["story"],
                "path": "sub_level[].text",
                "levenshtein_distance": 0,
                "starts_with":true,
                "snippet":true,
                "top":10,
                "skip":0
            });
            let mut requesto: search::RequestSearchPart = serde_json::from_str(&req.to_string()).expect("Can't parse json");
            let mut pers = PERSISTENCES.get(&"default".to_string()).unwrap();
            let results = search_field::highlight(&mut pers, &mut requesto).unwrap();
            assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["Prolog:\nthis is <b>story</b> of a guy who went ... "]);
        }

        it "real suggest with score"{
            let req = json!({
                "terms":["majes"],
                "path": "meanings.ger[]",
                "levenshtein_distance": 0,
                "starts_with":true,
                "top":10,
                "skip":0
            });
            let requesto: search::RequestSearchPart = serde_json::from_str(&req.to_string()).expect("Can't parse json");
            let mut pers = PERSISTENCES.get(&"default".to_string()).unwrap();
            let results = search_field::suggest(&mut pers, &requesto).unwrap();
            // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["majestät", "majestätischer", "majestätisches", "majestätischer anblick", "majestätisches aussehen"]);
            assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["Majestät", "Majestät (f)", "majestätischer", "majestätisches", "majestätischer Anblick (m)", "majestätisches Aussehen (n)"]);
        }

        it "multi real suggest with score"{

            let req = json!({
                "suggest" : [
                    {"terms":["will"], "path": "meanings.ger[]", "levenshtein_distance": 0, "starts_with":true},
                    {"terms":["will"], "path": "meanings.eng[]", "levenshtein_distance": 0, "starts_with":true}
                ],
                "top":10,
                "skip":0
            });

            let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
            let mut pers = PERSISTENCES.get(&"default".to_string()).unwrap();
            let results = search_field::suggest_multi(&mut pers, requesto).unwrap();
            // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["will", "wille", "will test"]);
            assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["will", "Wille", "Wille (m)", "will test"]);
        }


        it "real suggest with boosting score of 'Begeisterung' and token value"{
            let req = json!({
                "terms":["begeist"],
                "path": "meanings.ger[]",
                "levenshtein_distance": 0,
                "starts_with":true,
                "token_value": {
                    "path":"meanings.ger[].textindex.tokenValues",
                    "boost_fun":"Log10",
                    "param": 1
                },
                "top":10,
                "skip":0
            });
            let requesto: search::RequestSearchPart = serde_json::from_str(&req.to_string()).expect("Can't parse json");
            let mut pers = PERSISTENCES.get(&"default".to_string()).unwrap();
            let results = search_field::suggest(&mut pers, &requesto).unwrap();
            // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["begeisterung", "begeistern"]);
            assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["Begeisterung", "begeistern", "Begeisterung (f)"]);
        }

        // it "should or connect the checks"{
        //     let req = json!({
        //         "search": {
        //             "terms":["having a long]",
        //             "path": "meanings.eng[]",
        //             "levenshtein_distance": 1,
        //             "firstCharExactMatch":true,
        //             startsWith:true,
        //             operator:"some"
        //         }]
        //     });
        //     let hits = search_testo_to_doc(req).data;
        //     assert_eq!(hits.len(), 1);
        // }

        it "should rank exact matches pretty good"{
            let req = json!({
                "search": {
                    "terms":["weich"], // hits welche and weich
                    "path": "meanings.ger[]",
                    "levenshtein_distance": 1,
                    "firstCharExactMatch":true
                },
                "boost" : [{
                    "path":"commonness",
                    "boost_fun": "Log10",
                    "param": 1
                }]
            });

            let hits = search_testo_to_doc(req).data;
            println!("{:?}", hits);
            assert_eq!(hits[0].doc["meanings"]["ger"][0], "(1) weich");
        }

        it "should boost terms"{
            let req = json!({
                "search": {
                    "terms":["weich"],
                    "path": "meanings.ger[]",
                    "levenshtein_distance": 1,
                    "firstCharExactMatch":true
                },
                "boost_term":[{
                    "terms":["9555"],
                    "path": "ent_seq",
                    "boost": 5.0
                }]
            });

            let hits = search_testo_to_doc(req).data;
            println!("{:?}", hits);
            assert_eq!(hits[0].doc["meanings"]["ger"][0], "(1) 2 3 super nice weich");
        }

        it "OR connect hits but boost one term"{
            let req = json!({
                "or":[
                    {"search": {"terms":["majestät (f)"], "path": "meanings.ger[]", "boost": 2}},
                    {"search": {"terms":["urge"], "path": "meanings.eng[]"}}
                ]
            });

            let hits = search_testo_to_doc(req).data;
            println!("{:?}", hits);
            assert_eq!(hits.len(), 2);
            assert_eq!(hits[0].doc["meanings"]["ger"][0], "majestätischer Anblick (m)");
        }

        it "search and get facet"{
            let req = json!({
                "search": {"terms":["will"], "path": "meanings.eng[]"},
                "facets": [{"field":"tags[]"}, {"field":"commonness"}]
            });

            let hits = search_testo_to_doc(req);
            assert_eq!(hits.data.len(), 2);
            let facets = hits.facets.unwrap();
            assert_eq!(facets.get("tags[]").unwrap(), &vec![("nice".to_string(), 2), ("cool".to_string(), 1)] );
            assert_eq!(facets.get("commonness").unwrap(), &vec![("20".to_string(), 2)] );
        }

        // it "majestät"{
        //     let pers = PERSISTENCES.get(&"default".to_string()).unwrap();
        //     let req = json!({
        //         "search": {"terms":["majestät"], "path": "meanings.ger[]"}
        //     });
        //     let hits = search_testo_to_hitso(req).unwrap();
        //     println!("{:?}", hits);
        //     assert_eq!(hits.data.len(), 1);
        // }

        it "read object only partly"{
            let pers = PERSISTENCES.get(&"default".to_string()).unwrap();
            let yay = search::read_data(&pers, 3, &vec!["commonness".to_string(),
                                                        "ent_seq".to_string(),
                                                        "meanings.ger[]".to_string(),
                                                        "kana[].text".to_string(),
                                                        "kana[].commonness".to_string(),
                                                        "kana[].romaji".to_string(),
                                                        "address[].line[]".to_string()]).unwrap();
            assert_eq!(yay, json!({
                "address": [
                    {"line": ["nuts strees"] },
                    {"line": ["asdf"] }
                ],
                "commonness": "500",
                "ent_seq": "1587700",
                "meanings": {
                    "ger": ["der test", "das ist ein guter Treffer"]
                },
                "kana": [{"text": "いよく"} ]
            }));

        }

        it "should skip existing fields which are not existent in the object - None values "{
            let pers = PERSISTENCES.get(&"default".to_string()).unwrap();
            //Check None values
            let yay = search::read_data(&pers, 3, &vec!["mylongtext".to_string()]).unwrap();
            assert_eq!(yay, json!({}));
        }

        it "read recreate complete object with read"{
            let pers = PERSISTENCES.get(&"default".to_string()).unwrap();
            let all_props = pers.get_all_properties();
            let yay2 = search::read_data(&pers, 3, &all_props).unwrap();

            assert_eq!(yay2, json!({ //TODO FIX INTEGER TO STRING
                "id": "1234566",
                "gender": "male",
                "tags": ["awesome", "cool"],
                "birthDate": "1960-08-19",
                "address": [{"line": ["nuts strees"] }, {"line": ["asdf"] } ],
                "commonness": "500",
                "kanji": [{ "text": "意慾", "commonness": "20"} ],
                "field1" : [{"text":"awesome", "rank":"1"}],
                "kana": [{"text": "いよく"} ],
                "meanings": {"eng" : ["test1"], "ger": ["der test", "das ist ein guter Treffer"] },
                "ent_seq": "1587700"
            }));

        }

        it "facet"{
            let pers = PERSISTENCES.get(&"default".to_string()).unwrap();
            let yep = facet::get_facet(&pers, &search::FacetRequest{field:"tags[]".to_string(), top:Some(10)}, &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
            assert_eq!(yep.unwrap(), vec![("nice".to_string(), 8), ("cool".to_string(), 8), ("awesome".to_string(), 1), ("coolo".to_string(), 1), ("Eis".to_string(), 1)] );
        }

        //MUTLI TERMS

        // { // multi terms attribute ALL
        //     let req = json!({
        //         "or":[{"search": {"terms":["alle","Words"], "path": "meanings.ger[]", "term_operator": "ALL"}} ]
        //     });

        //     let hits = search_test_to_doc(req, &mut pers);
        //     assert_eq!(hits[0].doc["meanings"]["ger"][2], "alle meine Words");
        // }

        // { // multi terms attribute ALL
        //     let req = json!({
        //         "or":[{"search": {"terms":["alle","Words", "TRIFFTNICHT"], "path": "meanings.ger[]", "term_operator": "ANY"}} ]
        //     });

        //     let hits = search_test_to_doc(req, &mut pers);
        //     assert_eq!(hits[0].doc["meanings"]["ger"][2], "alle meine Words");
        // }

        // { // terms
        //     let req = json!({
        //         "or":[
        //             {"search": {"terms":["guter","Treffer"], "path": "meanings.ger[]"}}
        //         ]
        //     });

        //     let hits = search_test_to_doc(req, &mut pers);
        //     println!("{:?}", hits);
        //     // assert_eq!(hits.as_ref().unwrap().len(), 2);
        //     assert_eq!(hits[0].doc["meanings"]["ger"][1], "das ist ein guter Treffer");
        // }

    }

    // fn load_test_data() -> &'static persistence::Persistence  {
    //     let persistences = PERSISTENCES.read().unwrap();
    //     persistences.get(&"default".to_string()).unwrap()
    // }

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
