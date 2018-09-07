#![feature(plugin)]
#![cfg_attr(test, plugin(stainless))]
#![recursion_limit = "128"]

#[macro_use]
extern crate lazy_static;
extern crate search_lib;
#[macro_use]
extern crate serde_json;

use search_lib::create;
use search_lib::facet;
use search_lib::persistence;
use search_lib::query_generator;
use search_lib::search;
use search_lib::search_field;
use search_lib::trace;
use serde_json::Value;

fn search_testo_to_doc(req: Value) -> search::SearchResultWithDoc {
    search_testo_to_doco(req).expect("search error")
}

fn search_testo_to_doco_qp(qp: query_generator::SearchQueryGeneratorParameters) -> search::SearchResultWithDoc {
    let pers = &TEST_PERSISTENCE;
    let requesto = query_generator::search_query(&pers, qp);
    // println!("{}", serde_json::to_string_pretty(&requesto).unwrap());
    search::to_search_result(&pers, search_testo_to_hitso(requesto.clone()).expect("search error"), &requesto.select)
}

fn search_testo_to_doco(req: Value) -> Result<search::SearchResultWithDoc, search::SearchError> {
    let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    search_testo_to_doco_req(requesto, &TEST_PERSISTENCE)
}

fn search_testo_to_doco_req(requesto: search::Request, pers: &persistence::Persistence) -> Result<search::SearchResultWithDoc, search::SearchError> {
    Ok(search::to_search_result(&pers, search_testo_to_hitso(requesto.clone())?, &requesto.select))
}

fn search_testo_to_hitso(requesto: search::Request) -> Result<search::SearchResult, search::SearchError> {
    let pers = &TEST_PERSISTENCE;
    let hits = search::search(requesto, &pers)?;
    Ok(hits)
}

static TEST_FOLDER: &str = "mochaTest";
lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = {
        trace::enable_log();

        let indices = r#"
        [
            { "facet":"tags[]"},
            { "facet":"commonness"},
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

        let data = get_test_data();

        let mut persistence_type = persistence::PersistenceType::Transient;
        if let Some(val) = std::env::var_os("PersistenceType") {
            if val.clone().into_string().unwrap() == "Transient" {
                persistence_type = persistence::PersistenceType::Transient;
            } else if val.clone().into_string().unwrap() == "Persistent" {
                persistence_type = persistence::PersistenceType::Persistent;
            } else {
                panic!("env PersistenceType needs to be Transient or Persistent");
            }
        }

        let mut pers = persistence::Persistence::create_type(TEST_FOLDER.to_string(), persistence_type.clone()).unwrap();

        let mut out: Vec<u8> = vec![];
        search_lib::create::convert_any_json_data_to_line_delimited(data.to_string().as_bytes(), &mut out).unwrap();
        println!("{:?}", create::create_indices_from_str(&mut pers, std::str::from_utf8(&out).unwrap(), indices, None, true));

        {
            let config = json!({
                "path": "meanings.ger[]"
            });
            create::add_token_values_to_tokens(&mut pers, r#"[{"text": "Begeisterung", "value": 20 } ]"#, &config.to_string()).expect("Could not add token values");
        }

        if persistence_type == persistence::PersistenceType::Persistent {
            pers = persistence::Persistence::load(TEST_FOLDER.to_string()).expect("Could not load persistence");
        }
        pers
    };
}

pub fn get_test_data() -> Value {
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
                "eng" : ["karlo","dignity", "majestic appearance", "will testo"],
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
                "eng" : ["will", "urge", "having a long torso"],
                "ger": ["Wollen (n)", "Wille (m)", "Begeisterung (f)", "begeistern"]
            },
            "ent_seq": "1587690"
        },
        {
            "meanings": {
                "eng" : ["karl der große"],
            }
        },
        {
            "id": 1234566,
            "gender": "male",
            "tags": ["awesome", "cool"],
            "sinlge_value_multi": ["wert"],
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
                "ger": ["welch", "guter nicht Treffer", "alle meine Words", "text", "localität"]
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
            "meanings": {
                "ger": ["text localität", "alle meine Words"]
            },
            "ent_seq": "1000"
        },
        {
            "sub_level": [{"text":"Prolog:\nthis is story of a guy who went out to rule the world, but then died. the end"}],
            "commonness": 515151,
            "ent_seq": "25",
            "tags": ["nice", "cool"]
        },
        {
            "title": "Die Erbin die Sünde",
            "type": "taschenbuch"
        },
        {
            "title": "Die Erbin",
            "type": "taschenbuch"
        },
        {
            "commonness": 30,
            "float_value": 5.123,
            "ent_seq": "26",
            "tags": ["nice", "coolo"]
        },
        {
            "commonness": 20,
            "ent_seq": "27",
            "my_bool" : true,
            "tags": ["Eis", "cool"]
        },
        {
            "commonness": 20,
            "ent_seq": "28",
            "tags": ["nice", "cool"]
        }
    ])
}

#[test]
#[ignore]
fn test_binary_search() {
    let x = vec![1, 2, 3, 6, 7, 8];
    let _value = match x.binary_search(&4) {
        Ok(value) => value,
        Err(value) => value,
    };
}

#[test]
fn test_json_request() {
    let requesto: search::Request = serde_json::from_str(r#"{"search":{"path":"asdf", "terms":[ "asdf"], "levenshtein_distance":1}}"#).unwrap();
    assert_eq!(requesto.search.unwrap().levenshtein_distance, Some(1));
}

fn to_vec(data: &[&'static str]) -> Vec<String> {
    data.iter().map(|el| el.to_string()).collect()
}

describe! search_test {


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
        assert_eq!(hits[0].doc["commonness"], 20);
        assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
    }

    it "simple_search explained"{
        let req = json!({
            "search": {
                "terms":["urge"],
                "path": "meanings.eng[]",
                "explain":true
            }
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc["ent_seq"], "1587690");
        assert_eq!(hits[0].doc["commonness"], 20);
        assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
        // assert_eq!(hits[0].explain, Some(to_vec(&["term score 10.0 * anchor score 3.68 to 36.8", "levenshtein score 10.0 for urge"])));
        assert_eq!(hits[0].explain.as_ref().unwrap().len(), 2);
    }

    it "or_query_explained"{

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
            ],
            "explain":true
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].doc["ent_seq"], "1587690");
        // assert_eq!(hits[0].explain, Some(to_vec(&["or sum_over_distinct_terms 36.8125", "term score 10.0 * anchor score 3.68 to 36.8", "levenshtein score 10.0 for urge"])));
        assert_eq!(hits[0].explain.as_ref().unwrap().len(), 3);

    }

    it "simple_search_querygenerator_explained"{
        let mut params = query_generator::SearchQueryGeneratorParameters::default();
        params.explain = Some(true);
        params.search_term = "urge".to_string();

        let hits = search_testo_to_doco_qp(params).data;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc["ent_seq"], "1587690");
        assert_eq!(hits[0].doc["commonness"], 20);
        assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
        // assert_eq!(hits[0].explain, Some(to_vec(&["or sum_over_distinct_terms 36.8125", "term score 10.0 * anchor score 3.68 to 36.8", "levenshtein score 10.0 for urge"])));
        assert_eq!(hits[0].explain.as_ref().unwrap().len(), 3);
    }

    it "simple_search_querygenerator_OR_connect_explained"{
        let mut params = query_generator::SearchQueryGeneratorParameters::default();
        params.explain = Some(true);
        params.search_term = "urge OR いよく".to_string();

        let hits = search_testo_to_doco_qp(params).data;
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0].doc["ent_seq"], "1587690");
        assert_eq!(hits[0].doc["commonness"], 20);
        assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
        // assert_eq!(hits[0].explain, Some(vec!["or sum_over_distinct_terms 452.375".to_string(), "term score 15.0 * anchor score 3.7 to 55.5".to_string(), "term score 15.0 * anchor score 3.84 to 57.6".to_string()]));
        // assert_eq!(hits[0].explain, None);
        assert_eq!(hits[0].explain.as_ref().unwrap().len(), 5);

    }

    it "test_float"{
        let req = json!({
            "search": {
                "terms":["5.123"],
                "path": "float_value"
            }
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc["float_value"], 5.123);
    }

    it "test_bool"{
        let req = json!({
            "search": {
                "terms":["true"],
                "path": "my_bool"
            }
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc["my_bool"], true);
    }

    it "should return an error when trying to query an invalid field"{
        let req = json!({
            "search": {
                "terms":["test"],
                "path": "notexisting"
            }
        });
        let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
        let hits = search_testo_to_hitso(requesto);

        match hits {
            Err(search::SearchError::StringError(el)) => {assert_eq!(el, "fst not found loaded in indices notexisting.textindex ".to_string())},
            _ => panic!("YESO"),
        }

    }

    it "simple_search querygenerator"{
        let mut params = query_generator::SearchQueryGeneratorParameters::default();
        params.search_term="urge".to_string();

        let hits = search_testo_to_doco_qp(params).data;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc["ent_seq"], "1587690");
        assert_eq!(hits[0].doc["commonness"], 20);
        assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
    }

    it "simple_search_querygenerator_OR_connect"{
        let mut params = query_generator::SearchQueryGeneratorParameters::default();
        params.search_term="urge OR いよく".to_string();

        let hits = search_testo_to_doco_qp(params).data;
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0].doc["ent_seq"], "1587690");
        assert_eq!(hits[0].doc["commonness"], 20);
        assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
    }

    it "simple_search querygenerator and"{
        let mut params = query_generator::SearchQueryGeneratorParameters::default();
        params.search_term="urge AND いよく".to_string();

        let hits = search_testo_to_doco_qp(params).data;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc["ent_seq"], "1587690");
        assert_eq!(hits[0].doc["commonness"], 20);
        assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
    }
    it "simple_search querygenerator and emtpy stopword list"{
        let mut params = query_generator::SearchQueryGeneratorParameters::default();
        params.stopword_lists = Some(vec![]);
        params.search_term="urge AND いよく".to_string();

        let hits = search_testo_to_doco_qp(params).data;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc["ent_seq"], "1587690");
        assert_eq!(hits[0].doc["commonness"], 20);
        assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
    }
    it "simple_search querygenerator and stopword list"{
        let mut params = query_generator::SearchQueryGeneratorParameters::default();
        params.stopword_lists = Some(vec!["en".to_string()]);
        params.search_term="urge AND いよく".to_string();

        let hits = search_testo_to_doco_qp(params).data;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc["ent_seq"], "1587690");
        assert_eq!(hits[0].doc["commonness"], 20);
        assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
    }
    it "simple_search querygenerator and no hit"{
        let mut params = query_generator::SearchQueryGeneratorParameters::default();
        params.search_term="urge AND いよく AND awesome".to_string();

        let hits = search_testo_to_doco_qp(params).data;
        assert_eq!(hits.len(), 0);
    }

    it "select single field"{
        let req = json!({
            "search": {
                "terms":["urge"],
                "path": "meanings.eng[]"
            },
            "select": ["ent_seq", "tags[]"]
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc["ent_seq"], "1587690");
        assert_eq!(hits[0].doc.get("commonness"), None); // didn't select
        assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));

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

    it "should_prefer_exact_tokenmatches_to_fuzzy_text_hits"{

        let req = json!({
            "search": {
                "terms":["karl"],
                "path": "meanings.eng[]",
                "levenshtein_distance": 1
            },
            "explain":true
        });
        let wa = search_testo_to_doc(req).data;
        println!("{}", serde_json::to_string_pretty(&wa).unwrap());
        assert_eq!(wa[0].doc["meanings"]["eng"][0], "karl der große"); // should hit karl, not karlo
    }

    it "should_prefer_short_results"{
        let mut params = query_generator::SearchQueryGeneratorParameters::default();
        params.phrase_pairs = Some(true);
        params.explain = Some(true);
        params.search_term = "die erbin taschenbuch".to_string();
        let hits = search_testo_to_doco_qp(params).data;
        assert_eq!(hits[0].doc["title"], "Die Erbin");
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
        assert_eq!(hits[0].doc["ent_seq"], "1587680");
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

    it "AND connect hits different fields - same text 'alle meine words' appears again"{
        let req = json!({
            "and":[
                {"search": {
                    "terms":["words"],
                    "path": "meanings.ger[]"
                }},
                {"search": {
                    "terms":["1000"],
                    "path": "ent_seq"
                }}
            ]
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc["ent_seq"], "1000");
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
        assert_eq!(hits[0].doc["ent_seq"], "1587690");
        assert_eq!(hits.len(), 2);
    }

    it "simple_search_and_connect_hits_with_filter"{
        let req = json!({
            "search": {
                "terms":["urge"],
                "path": "meanings.eng[]"
            },
            "filter":{
                "search": {
                    "terms":["1587690"],
                    "path": "ent_seq"
                }
            }
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits.len(), 1);
    }

    it "OR_connect_hits_with_filter"{
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
            ],
            "filter":{
                "search": {
                    "terms":["1587690"],
                    "path": "ent_seq"
                }
            }
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits.len(), 1);
    }

    it "OR_connect_hits_with_filter_reuse_query"{
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
            ],
            "filter":{
                "search": {
                    "terms":["urge"],
                    "path": "meanings.eng[]"
                }
            }
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits.len(), 1);
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

    it "should_search_and_boosto"{
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

    it "should or connect search and boost anchor"{
        let req = json!({
            "or":[
                {
                    "search": {
                        "terms":["awesome"],
                        "path": "field1[].text"
                    },
                    "boost" : [{
                        "path":"field1[].rank",
                        "boost_fun": "Log10",
                        "param": 1
                    }]
                },
                {
                    "search": {
                        "terms":["urge"],
                        "path": "meanings.eng[]"
                    },
                    "boost" : [{
                        "path":"commonness",
                        "boost_fun": "Log10",
                        "param": 1
                    }]
                }
            ]
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits[0].doc["commonness"], 20);
    }

    it "should_or_connect_same_search"{
        let req = json!({
            "or":[
                {
                    "search": {
                        "terms":["awesome"],
                        "path": "field1[].text"
                    }
                },
                {
                    "search": {
                        "terms":["awesome"],
                        "path": "field1[].text"
                    }
                }
            ]
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits[0].doc["commonness"], 551);
        assert_eq!(hits.len(), 2);
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
        });
        let requesto: search::RequestSearchPart = serde_json::from_str(&req.to_string()).expect("Can't parse json");
        let mut pers = &TEST_PERSISTENCE;
        use search_lib::execution_plan::PlanRequestSearchPart;
        let mut requesto = PlanRequestSearchPart{request:requesto, return_term:true, ..Default::default()};
        // let results = search_field::get_hits_in_field(&mut pers, &requesto, None).unwrap();
        // get_term_ids_in_field(persistence, &mut search_part)
        let results = search_field::get_term_ids_in_field(&mut pers, &mut requesto).unwrap();

        let mut all_terms = results.terms.values().collect::<Vec<&String>>();
        all_terms.sort();
        // assert_eq!(all_terms, ["majestät", "majestätischer", "majestätischer anblick", "majestätisches", "majestätisches aussehen"]);
        assert_eq!(all_terms, ["Majestät", "Majestät (f)", "majestätischer", "majestätischer Anblick (m)", "majestätisches", "majestätisches Aussehen (n)"]);
    }

    //TODO ENBALE
    // it "should load the text for ids"{
    //     let pers = &TEST_PERSISTENCE;
    //     let mut faccess:persistence::FileSearch = pers.get_file_search("meanings.ger[].textindex");

    //     assert_eq!(faccess.get_text_for_id(11, pers.get_offsets("meanings.ger[].textindex").unwrap()), "Majestät" );
    //     assert_eq!(faccess.get_text_for_id(12, pers.get_offsets("meanings.ger[].textindex").unwrap()), "Majestät (f)" );
    //     assert_eq!(faccess.get_text_for_id(13, pers.get_offsets("meanings.ger[].textindex").unwrap()), "Treffer" );

    // }

    // it "should highlight ids"{
    //     let mut pers = &TEST_PERSISTENCE;


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
        let mut pers = &TEST_PERSISTENCE;
        let results = search_field::highlight(&mut pers, &mut requesto).unwrap();
        assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["Prolog:\nthis is a <b>story</b> of a guy who went ... "]);
    }

    it "should select on long text"{

        let req = json!({
            "search": {
                "terms":["story"],
                "path": "mylongtext"
            },
            "select": ["mylongtext"]
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc["mylongtext"], json!("Prolog:\nthis is a story of a guy who went out to rule the world, but then died. the end".to_string()));

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
        let mut pers = &TEST_PERSISTENCE;
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
        let mut pers = &TEST_PERSISTENCE;
        let results = search_field::suggest(&mut pers, &requesto).unwrap();
        // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["majestät", "majestätischer", "majestätisches", "majestätischer anblick", "majestätisches aussehen"]);
        // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["Majestät", "Majestät (f)", "majestätischer", "majestätisches", "majestätischer Anblick (m)", "majestätisches Aussehen (n)"]);
        assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["majestät", "majestät (f)", "majestätisches", "majestätischer", "majestätischer anblick (m)", "majestätisches aussehen (n)"]);
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
        let mut pers = &TEST_PERSISTENCE;
        let results = search_field::suggest_multi(&mut pers, requesto).unwrap();
        // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["will", "wille", "will testo"]);
        // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["will", "Wille", "Wille (m)", "will testo"]);
        assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["will", "wille", "wille (m)", "will testo"]);
    }


    it "real suggest with boosting score of 'Begeisterung' and token value"{
        let req = json!({
            "terms":["begeist"],
            "path": "meanings.ger[]",
            "levenshtein_distance": 0,
            "starts_with":true,
            "token_value": {
                "path":"meanings.ger[].textindex",
                "boost_fun":"Log10",
                "param": 1
            },
            "top":10,
            "skip":0
        });
        let requesto: search::RequestSearchPart = serde_json::from_str(&req.to_string()).expect("Can't parse json");
        let mut pers = &TEST_PERSISTENCE;
        let results = search_field::suggest(&mut pers, &requesto).unwrap();
        // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["begeisterung", "begeistern"]);
        // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["Begeisterung", "begeistern", "Begeisterung (f)"]);
        assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["begeisterung", "begeistern", "begeisterung (f)"]);
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
        assert_eq!(hits[0].doc["meanings"]["ger"][0], "(1) weich");
    }

    it "should boost terms and from cache"{
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

        let hits = search_testo_to_doc(req.clone()).data;
        assert_eq!(hits[0].doc["meanings"]["ger"][0], "(1) 2 3 super nice weich");

        //using boost cache here
        let hits = search_testo_to_doc(req.clone()).data;
        assert_eq!(hits[0].doc["meanings"]["ger"][0], "(1) 2 3 super nice weich");
        let hits = search_testo_to_doc(req.clone()).data;
        assert_eq!(hits[0].doc["meanings"]["ger"][0], "(1) 2 3 super nice weich");
    }

    it "should_add_why_found_terms"{
        let req = json!({
            "search": {
                "terms":["weich"],
                "path": "meanings.ger[]",
                "levenshtein_distance": 1,
                "firstCharExactMatch":true
            },
            "why_found":true,
            "explain": true
        });

        let hits = search_testo_to_doc(req).data;
        println!("{}", serde_json::to_string_pretty(&hits).unwrap());
        assert_eq!(hits[0].doc["meanings"]["ger"][0], "(1) weich");
    }

    it "OR connect hits but boost one term"{
        let req = json!({
            "or":[
                {"search": {"terms":["majestät (f)"], "path": "meanings.ger[]", "boost": 2}},
                {"search": {"terms":["urge"], "path": "meanings.eng[]"}}
            ]
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].doc["meanings"]["ger"][0], "majestätischer Anblick (m)");
    }

    it "get_bytes_indexed"{
        let pers = &TEST_PERSISTENCE;
        assert_eq!(pers.get_bytes_indexed().unwrap(), 2570);
    }

    it "boost_text_localitaet"{
        let req = json!({
            "or":[
                {"search": {"terms":["text"],      "path": "meanings.ger[]"}, "text_locality": true},
                {"search": {"terms":["localität"], "path": "meanings.ger[]"}, "text_locality": true},
            ],
            "text_locality": true,
            "explain": true
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits[0].doc["meanings"]["ger"][0], "text localität");
    }

    it "search_and_get_facet_with_facet_index"{
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

    it "search and get facet without facet index"{ // meanings.eng[] hat no facet index and is a 1-n facet
        let req = json!({
            "search": {"terms":["test"], "path": "meanings.ger[]"},
            "facets": [{"field":"meanings.eng[]"}]
        });

        let hits = search_testo_to_doc(req);
        assert_eq!(hits.data.len(), 1);
        let facets = hits.facets.unwrap();
        assert_eq!(facets.get("meanings.eng[]").unwrap(), &vec![("test1".to_string(), 1)] );
    }

    it "read object only partly"{
        let pers = &TEST_PERSISTENCE;
        let yay = search::read_data(&pers, 4, &vec!["commonness".to_string(),
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
        let pers = &TEST_PERSISTENCE;
        //Check None values
        let yay = search::read_data(&pers, 3, &vec!["mylongtext".to_string()]).unwrap();
        assert_eq!(yay, json!({}));
    }

    it "read recreate complete object with read"{
        let pers = &TEST_PERSISTENCE;
        let all_props = pers.meta_data.get_all_fields();
        let yay2 = search::read_data(&pers, 4, &all_props).unwrap();

        assert_eq!(yay2, json!({ //TODO FIX INTEGER TO STRING
            "id": "1234566",
            "gender": "male",
            "tags": ["awesome", "cool"],
            "sinlge_value_multi": ["wert"],
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
        let pers = &TEST_PERSISTENCE;
        let yep = facet::get_facet(&pers, &search::FacetRequest{field:"tags[]".to_string(), top:Some(10)}, &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]);
        assert_eq!(yep.unwrap(), vec![("nice".to_string(), 8), ("cool".to_string(), 8), ("awesome".to_string(), 1), ("coolo".to_string(), 1), ("Eis".to_string(), 1)] );
    }

    //MUTLI TERMS

    // it "should handle multi terms and connected"{ // multi terms attribute ALL
    //     let req = json!({
    //         "or":[{"search": {"terms":["alle","Words"], "path": "meanings.ger[]", "term_operator": "ALL"}} ]
    //     });

    //     let hits = search_testo_to_doc(req);
    //     assert_eq!(hits.data[0].doc["meanings"]["ger"][2], "alle meine Words");
    // }

    // it "should handle multi terms or connected"{ // multi terms attribute ALL
    //     let req = json!({
    //         "or":[{"search": {"terms":["alle","Words", "TRIFFTNICHTS"], "path": "meanings.ger[]", "term_operator": "ANY"}} ]
    //     });

    //     let hits = search_testo_to_doc(req);
    //     assert_eq!(hits.data[0].doc["meanings"]["ger"][2], "alle meine Words");
    // }

    // it "should handle multi terms or connected n hits"{ // multi terms attribute ALL
    //     let req = json!({
    //         "or":[{"search": {"terms":["alle","Words", "TRIFFTNICHTS"], "path": "meanings.ger[]", "term_operator": "ALL"}} ]
    //     });

    //     let hits = search_testo_to_doc(req);
    //     assert_eq!(hits.data.len(), 0);
    // }

    // { // terms
    //     let req = json!({
    //         "or":[
    //             {"search": {"terms":["guter","Treffer"], "path": "meanings.ger[]"}}
    //         ]
    //     });

    //     let hits = search_test_to_doc(req, &mut pers);
    //     // assert_eq!(hits.as_ref().unwrap().len(), 2);
    //     assert_eq!(hits[0].doc["meanings"]["ger"][1], "das ist ein guter Treffer");
    // }

}

// #[test]
// fn checked_was_abgehst_22() {
//     let small_test_json:&str = r#"[
//         {
//             "meanings": {
//                 "eng" : ["dignity", "majestic appearance", "will testo"],
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

//     assert_eq!(normalize_text("Hello"), "Hello");

//     let mut f = File::open("meanings.ger[]").unwrap();
//     let mut s = String::new();
//     f.read_to_string(&mut s).unwrap();

//     let lines = s.lines().collect::<Vec<_>>();
//     let text = vec!["Anblick", "Aussehen", "Majestät", "majestätischer", "majestätischer Anblick", "majestätisches", "majestätisches Aussehen"];
//     assert_eq!(lines, text);

// }

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
//         let deserialized: create::BoostIndexOptions = serde_json::from_str(r#"{"boost_type":"int"}"#).unwrap();

//         assert_eq!("Hello", "Hello");

//         let service: create::CreateIndex = serde_json::from_str(r#"{"boost_type":"int"}"#).unwrap();

//     }
// }
