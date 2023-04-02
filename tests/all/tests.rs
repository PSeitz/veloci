use serde_json::Value;
use veloci::*;

use super::common;

static TEST_FOLDER: &str = "test_all";
lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = {
        let indices = r#"
        ["*GLOBAL*"]
            features = ["All"]
        ["commonness"]
            facet = true
        ["commonness".boost]
            boost_type = "f32"
        ["ent_seq".fulltext]
            tokenize = true
        ["nofulltext".fulltext]
            tokenize = false
        ["tags[]"]
            facet = true
        ["field1[].rank".boost]
            boost_type = "f32"
        ["field1[].text"]
            tokenize = true
        ["kanji[].text"]
            tokenize = true
        ["meanings.ger[]"]
            stopwords = ["stopword"]
            fulltext.tokenize = true
        ["meanings.eng[]".fulltext]
            tokenize = true
        ["kanji[].commonness".boost]
            boost_type = "f32"
        ["kana[].commonness".boost]
            boost_type = "f32"
        "#;

        let token_values = Some((r#"[{"text": "Begeisterung", "value": 20 } ]"#.to_string(), json!({"path": "meanings.ger[]"})));

        common::create_test_persistence(TEST_FOLDER, indices, get_test_data().to_string().as_bytes(), token_values)
    };
}

pub fn get_test_data() -> Value {
    json!([
        {
            "ignore_field":"",
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
            "tags": ["nice", "cool", "Prolog:\nthis is a story of a guy who went out to rule the world, but then died. the end"],
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
            "title": "COllectif",
            "meanings": {"ger": ["boostemich"] }
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

// TODO enable
// #[test]
// fn test_json_request() {
//     let requesto: search::Request = serde_json::from_str(r#"{"search":{"path":"asdf", "terms":[ "asdf"], "levenshtein_distance":1}}"#).unwrap();
//     assert_eq!(requesto.search.unwrap().get_options().levenshtein_distance, Some(1));
// }

#[test]
fn test_create_index_from_file() {
    let mut pers = persistence::Persistence::create_type("test_files/test_from_file".to_string(), persistence::PersistenceType::Persistent).unwrap();
    create::create_indices_from_file(&mut pers, "test_files/test_data.json", "", false).unwrap();
}

#[test]
fn simple_search() {
    let req = json!({
        "search": {
            "terms":["urge"],
            "path": "meanings.eng[]"
        }
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1587690");
    assert_eq!(hits[0].doc["commonness"], 20);
    assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
}

#[test]
fn return_execution_time() {
    let req = json!({
        "search": {
            "terms":["urge"],
            "path": "meanings.eng[]"
        }
    });

    let res = search_request_json_to_doc!(req);
    assert_gt!(res.execution_time_ns, 1);
}

// #[test]
// fn levenshtein_lowercase_regression() {
//     let req = json!({
//         "search": {
//             "terms":["COllectif"],
//             "path": "title",
//             "levenshtein_distance": 2
//         }
//     });

//     let hits = search_request_json_to_doc!(req).data;
//     assert_eq!(hits.len(), 1);
//     assert_eq!(hits[0].doc["title"], "COllectif");
// }

#[test]
fn simple_search_skip_far() {
    let req = json!({
        "search_req": {
            "search": {
                "terms":["urge"],
                "path": "meanings.eng[]"
            }
        },
        "skip": 1000
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 0);
}

#[test]
fn simple_search_case_sensitive() {
    let req = json!({
        "search": {
            "ignore_case": true,
            "terms":["Urge"],
            "path": "meanings.eng[]"
        }
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);

    let req = json!({
        "search": {
            "ignore_case": false,
            "terms":["Urge"],
            "path": "meanings.eng[]"
        }
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 0);
}

#[test]
fn simple_search_explained() {
    let req = json!({
        "search": {
            "terms":["urge"],
            "path": "meanings.eng[]",
            "options": {"explain":true}
        }
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1587690");
    assert_eq!(hits[0].doc["commonness"], 20);
    assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
    // assert_eq!(hits[0].explain, Some(to_vec(&["term score 10.0 * anchor score 3.68 to 36.8", "levenshtein score 10.0 for urge"])));
    assert_eq!(hits[0].explain.as_ref().unwrap().len(), 2);
}

#[test]
fn or_query_explained() {
    let req = json!({
        "search_req": {
            "or":{
                "queries": [
                    {"search": {
                        "terms":["majestät"],
                        "path": "meanings.ger[]"
                    }},
                    {"search": {
                        "terms":["urge"],
                        "path": "meanings.eng[]"
                    }}
                ]
            }
        },
        "explain":true
    });
    println!("yo");
    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].doc["ent_seq"], "1587690");
    // assert_eq!(hits[0].explain, Some(to_vec(&["or sum_over_distinct_terms 36.8125", "term score 10.0 * anchor score 3.68 to 36.8", "levenshtein score 10.0 for urge"])));
    assert_eq!(hits[0].explain.as_ref().unwrap().len(), 5);
}

#[test]
fn test_float() {
    let req = json!({
        "search": {
            "terms":["5.123"],
            "path": "float_value"
        }
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["float_value"], 5.123);
}

#[test]
fn test_bool() {
    let req = json!({
        "search": {
            "terms":["true"],
            "path": "my_bool"
        }
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["my_bool"], true);
}

#[test]
fn should_return_an_error_when_trying_to_query_an_invalid_field() {
    let req = json!({
        "search": {
            "terms":["test"],
            "path": "notexisting"
        }
    });
    let search_request: search::SearchRequest = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    let requesto = search::Request {
        search_req: Some(search_request),
        ..Default::default()
    };
    let hits = search_to_hits!(requesto);
    assert_eq!(format!("{}", hits.unwrap_err()), "field does not exist notexisting.textindex (fst not found)".to_string())
}

#[test]
fn select_fields() {
    let req = json!({
        "search_req": { "search": {
            "terms":["urge"],
            "path": "meanings.eng[]"
        }},
        "select": ["ent_seq", "tags[]"]
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1587690");
    assert_eq!(hits[0].doc.get("commonness"), None); // didn't select
    assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
}

#[test]
fn two_tokens_h_test_fn_the_same_anchor() {
    let req = json!({
        "search": {
            "terms":["majestätischer"],
            "path": "meanings.ger[]",
            "levenshtein_distance": 1
        }
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1587680");
}

#[test]
fn deep_structured_objects() {
    let req = json!({
        "search": {
            "terms":["brook"],
            "path": "address[].line[]",
            "levenshtein_distance": 1
        }
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["id"], 123456);
}

#[test]
fn should_search_without_first_char_exact_match() {
    let req = json!({
        "search": {
            "terms":["najestätischer"],
            "path": "meanings.ger[]",
            "levenshtein_distance": 1
        }
    });
    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1587680");
}

#[test]
fn should_prefer_exact_matches_to_tokenmatches() {
    let req = json!({
        "search": {
            "terms":["will"],
            "path": "meanings.eng[]",
            "levenshtein_distance": 1
        }
    });
    let wa = search_request_json_to_doc!(req).data;
    assert_eq!(wa[0].doc["meanings"]["eng"][0], "will");
}

#[test]
fn test_prefer_exact_match_over_multi_hit() {
    let dir = "test_boost_simple";
    // Exact match is more important than many non exact hits
    let test_data = r#"
{ "definition": ["home"], "traditional": "家" }
{ "definition": ["to live at home", "to stay at home", "home (schooling etc)", "le home", "ok home", "so much home"], "traditional": "居家"}
    "#;
    let indices = r#""#;
    let pers: persistence::Persistence = common::create_test_persistence(dir, indices, test_data.as_bytes(), None);

    let req = json!({
        "search_req": { "search": {
            "terms":["home"],
            "path": "definition[]",
            "levenshtein_distance": 0,
            "firstCharExactMatch":true
        }}
    });

    let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    let hits = search::to_search_result(&pers, search::search(requesto.clone(), &pers).expect("search error"), &requesto.select).data;

    assert_eq!(hits[0].doc["traditional"], "家");
    assert_eq!(hits[1].doc["traditional"], "居家");
}

#[test]
fn test_exact_match_with_boost() {
    let dir = "test_boost_simple";
    // Exact match is more important than many non exact hits
    let test_data = r#"
{ "definition": ["home", "family"], "traditional": "家", "commonness": 5.5318 }
{ "definition": ["place to return to", "home", "final destination", "ending"], "traditional": "歸宿", "commonness": 3.1294}
    "#;
    let indices = r#"
    [commonness.boost]
    boost_type = 'f32'
    "#;
    let pers: persistence::Persistence = common::create_test_persistence(dir, indices, test_data.as_bytes(), None);

    let req = json!({
        "search_req": { "search": {
            "terms":["home"],
            "path": "definition[]",
            "levenshtein_distance": 0,
            "firstCharExactMatch":true
        }},
        "boost" : [{
            "path":"commonness",
            "boost_fun": "Log10",
            "param": 1
        }]
    });

    let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    let hits = search::to_search_result(&pers, search::search(requesto.clone(), &pers).expect("search error"), &requesto.select).data;

    assert_eq!(hits[0].doc["traditional"], "家");
    assert_eq!(hits[1].doc["traditional"], "歸宿");
}

#[test]
fn should_prefer_exact_tokenmatches_to_fuzzy_text_hits() {
    let req = json!({
        "search_req": { "search": {
            "terms":["karl"],
            "path": "meanings.eng[]",
            "levenshtein_distance": 1
        }},
        "explain":true
    });
    let wa = search_testo_to_doc!(req).data;
    println!("{}", serde_json::to_string_pretty(&wa).unwrap());
    assert_eq!(wa[0].doc["meanings"]["eng"][0], "karl der große"); // should hit karl, not karlo
}

#[test]
fn should_prefer_short_results() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.phrase_pairs = Some(true);
    params.explain = Some(true);
    params.search_term = "die erbin taschenbuch".to_string();
    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits[0].doc["title"], "Die Erbin");
}

#[test]
fn should_search_word_non_tokenized() {
    let req = json!({
        "search": {
            "terms":["偉容"],
            "path": "kanji[].text"
        }
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1587680");
}

#[test]
fn should_check_disabled_tokenization() {
    let req = json!({
        "search": {
            "terms":["tokens"],
            "path": "nofulltext"
        }
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 0);
}

#[test]
fn should_search_on_non_subobject() {
    let req = json!({
        "search": {
            "terms":["1587690"],
            "path": "ent_seq"
        }
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
}

#[test]
fn and_connect_hits_same_field() {
    let req = json!({
        "and":{
            "queries":[
            {"search": {"terms":["aussehen"],       "path": "meanings.ger[]"}},
            {"search": {"terms":["majestätisches"], "path": "meanings.ger[]"}}
        ]}
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1587680");
}

#[test]
fn and_connect_hits_different_fields() {
    let req = json!({
        "and":{
            "queries":[
            {"search": {"terms":["majestät"], "path": "meanings.ger[]"}},
            {"search": {"terms":["majestic"], "path": "meanings.eng[]"}}
        ]}
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1587680");
}

#[test]
fn and_connect_hits_different_fields_no_hit() {
    let req = json!({
        "and":{
            "queries":[
            {"search": {
                "terms":["majestät"],
                "path": "meanings.ger[]"
            }},
            {"search": {
                "terms":["urge"],
                "path": "meanings.eng[]"
            }}
        ]}
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 0);
}

#[test]
fn and_connect_hits_different_fields_same_text_alle_meine_words_appears_again() {
    let req = json!({
        "and":{
            "queries":[
            {"search": {
                "terms":["words"],
                "path": "meanings.ger[]"
            }},
            {"search": {
                "terms":["1000"],
                "path": "ent_seq"
            }}
        ]}
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1000");
}

#[test]
fn or_connect_hits_with_top() {
    let req = json!({
        "search_req": {
            "or":{"queries":[
                {"search": {
                    "terms":["majestät"],
                    "path": "meanings.ger[]"
                }},
                {"search": {
                    "terms":["urge"],
                    "path": "meanings.eng[]"
                }}
            ]}
        },
        "top":1
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].doc["ent_seq"], "1587690");
    assert_eq!(hits.len(), 1);
}

#[test]
fn or_connect_hits() {
    let req = json!({
        "or":{"queries":[
            {"search": {
                "terms":["majestät"],
                "path": "meanings.ger[]"
            }},
            {"search": {
                "terms":["urge"],
                "path": "meanings.eng[]"
            }}
        ]}
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits[0].doc["ent_seq"], "1587690");
    assert_eq!(hits.len(), 2);
}

#[test]
fn simple_search_and_connect_hits_with_filter() {
    let req = json!({
        "search_req": { "search": {
            "terms":["urge"],
            "path": "meanings.eng[]"
        }},
        "filter":{
            "search": {
                "terms":["1587690"],
                "path": "ent_seq"
            }
        }
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
}

#[test]
fn or_connect_hits_with_filter() {
    let req = json!({
        "search_req": {
            "or":{
                "queries":[
                    {"search": {
                        "terms":["majestät"],
                        "path": "meanings.ger[]"
                    }},
                    {"search": {
                        "terms":["urge"],
                        "path": "meanings.eng[]"
                    }}
                ]
            }
        },
        "filter":{
            "search": {
                "terms":["1587690"],
                "path": "ent_seq"
            }
        }
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
}

#[test]
fn or_connect_hits_with_filter_reuse_query() {
    let req = json!({
        "search_req": {"or":{ "queries": [
            {"search": {
                "terms":["majestät"],
                "path": "meanings.ger[]"
            }},
            {"search": {
                "terms":["urge"],
                "path": "meanings.eng[]"
            }}
        ]}},
        "filter":{
            "search": {
                "terms":["urge"],
                "path": "meanings.eng[]"
            }
        }
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
}

#[test]
fn should_find_2_values_from_token() {
    let req = json!({
        "search_req": { "search": {
            "terms":["意慾"],
            "path": "kanji[].text"
        }}
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 2);
}

#[test]
fn should_search_and_boosto() {
    let req = json!({
        "search_req": { "search": {
            "terms":["意慾"],
            "path": "kanji[].text"
        }},
        "boost" : [{
            "path":"kanji[].commonness",
            "boost_fun": "Log10",
            "param": 1
        }]
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 2);
}

#[test]
fn should_search_and_double_boost() {
    let req = json!({
        "search_req": { "search": {
            "terms":["awesome"],
            "path": "field1[].text"
        }},
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

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 2);
}

#[test]
fn should_search_and_boost_anchor() {
    let req = json!({
        "search_req": { "search": {
            "terms":["意慾"],
            "path": "kanji[].text",
            "levenshtein_distance": 0,
            "firstCharExactMatch":true
        }},
        "boost" : [{
            "path":"commonness",
            "boost_fun": "Log10",
            "param": 1
        }]
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].doc["commonness"], 500);
}

#[test]
fn should_or_connect_search_and_boost_anchor() {
    let req = json!({
        "or":{
            "queries": [
            {
                "search": {
                    "terms":["awesome"],
                    "path": "field1[].text",
                    "options": {"boost" : [{
                        "path":"field1[].rank",
                        "boost_fun": "Log10",
                        "param": 1
                    }]}
                }
            },
            {
                "search": {
                    "terms":["urge"],
                    "path": "meanings.eng[]",
                    "options": {
                        "boost" : [{
                        "path":"commonness",
                        "boost_fun": "Log10",
                        "param": 1
                    }]}
                }
            }
        ]}
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits[0].doc["commonness"], 20);
}

#[test]
fn should_or_connect_same_search() {
    let req = json!({
        "or":{
            "queries": [
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
        ]}
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits[0].doc["commonness"], 551);
    assert_eq!(hits.len(), 2);
}

#[test]
fn should_use_search_on_field_for_suggest_without_sorting_etc() {
    let req = json!({
        "terms":["majes"],
        "path": "meanings.ger[]",
        "levenshtein_distance": 0,
        "starts_with":true,
    });
    let requesto: search::RequestSearchPart = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    let pers = &TEST_PERSISTENCE;
    use veloci::plan_creator::execution_plan::PlanRequestSearchPart;
    let mut requesto = PlanRequestSearchPart {
        request: requesto,
        return_term: true,
        ..Default::default()
    };
    let results = search_field::get_term_ids_in_field(pers, &mut requesto).unwrap();

    let mut all_terms = results.terms.values().collect::<Vec<&String>>();
    all_terms.sort();
    // assert_eq!(all_terms, ["majestät", "majestätischer", "majestätischer anblick", "majestätisches", "majestätisches aussehen"]);
    assert_eq!(
        all_terms,
        [
            "Majestät",
            "Majestät (f)",
            "majestätischer",
            "majestätischer Anblick (m)",
            "majestätisches",
            "majestätisches Aussehen (n)"
        ]
    );
}

// #[test]
// fn should_highlight_ids(){
//     let mut pers = &TEST_PERSISTENCE;
//     let inf = search::SnippetInfo{
//         num_words_around_snippet :  4,
//         max_snippets :  40,
//         snippet_start_tag: "<b>".to_string(),
//         snippet_end_tag: "</b>".to_string(),
//         snippet_connector: " ... ".to_string(),
//     };
//     let results = search_field::highlight_document(&pers, "mylongtext.textindex", 13, &[9], &inf).unwrap();
//     assert_eq!(results, "this is a <b>story</b> of a guy who  ... " );

// }

#[test]
fn should_highlight_on_field() {
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
    let pers = &TEST_PERSISTENCE;
    let results = search_field::highlight(pers, &mut requesto).unwrap();
    assert_eq!(
        results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(),
        ["Prolog:\nthis is a <b>story</b> of a guy who went ... "]
    );
}

#[test]
fn should_highlight_on_1_n_field() {
    let req = json!({
        "terms":["story"],
        "path": "tags[]",
        "levenshtein_distance": 0,
        "starts_with":true,
        "snippet":true,
        "top":10,
        "skip":0
    });
    let mut requesto: search::RequestSearchPart = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    let pers = &TEST_PERSISTENCE;
    let results = search_field::highlight(pers, &mut requesto).unwrap();
    assert_eq!(
        results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(),
        ["Prolog:\nthis is a <b>story</b> of a guy who went ... "]
    );
}

#[test]
fn should_select_on_long_text() {
    let req = json!({
        "search_req": { "search": {
            "terms":["story"],
            "path": "mylongtext"
        }},
        "select": ["mylongtext"]
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].doc["mylongtext"],
        json!("Prolog:\nthis is a story of a guy who went out to rule the world, but then died. the end".to_string())
    );
}

#[test]
fn should_highlight_on_sub_level_field() {
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
    let pers = &TEST_PERSISTENCE;
    let results = search_field::highlight(pers, &mut requesto).unwrap();
    assert_eq!(
        results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(),
        ["Prolog:\nthis is <b>story</b> of a guy who went ... "]
    );
}

#[test]
fn real_suggest_with_score() {
    let req = json!({
        "terms":["majes"],
        "path": "meanings.ger[]",
        "levenshtein_distance": 0,
        "starts_with":true,
        "top":10,
        "skip":0
    });
    let requesto: search::RequestSearchPart = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    let pers = &TEST_PERSISTENCE;
    let results = search_field::suggest(pers, &requesto).unwrap();
    // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["majestät", "majestätischer", "majestätisches", "majestätischer anblick", "majestätisches aussehen"]);
    // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["Majestät", "Majestät (f)", "majestätischer", "majestätisches", "majestätischer Anblick (m)", "majestätisches Aussehen (n)"]);
    assert_eq!(
        results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(),
        [
            "majestät",
            "majestät (f)",
            "majestätisches",
            "majestätischer",
            "majestätischer anblick (m)",
            "majestätisches aussehen (n)"
        ]
    );
}

#[test]
fn multi_real_suggest_with_score() {
    let req = json!({
        "suggest" : [
            {"terms":["will"], "path": "meanings.ger[]", "levenshtein_distance": 0, "starts_with":true},
            {"terms":["will"], "path": "meanings.eng[]", "levenshtein_distance": 0, "starts_with":true}
        ],
        "top":10,
        "skip":0
    });

    let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    let pers = &TEST_PERSISTENCE;
    let results = search_field::suggest_multi(pers, requesto).unwrap();
    // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["will", "wille", "will testo"]);
    // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["will", "Wille", "Wille (m)", "will testo"]);
    assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["will", "wille", "wille (m)", "will testo"]);
}

#[test]
fn real_suggest_with_boosting_score_of_begeisterung_and_token_value() {
    let req = json!({
        "terms":["begeist"],
        "path": "meanings.ger[]",
        "levenshtein_distance": 0,
        "starts_with":true,
        "token_value": {
            "path":"meanings.ger[]",
            "boost_fun":"Log10",
            "param": 1
        },
        "top":10,
        "skip":0
    });
    let requesto: search::RequestSearchPart = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    let pers = &TEST_PERSISTENCE;
    let results = search_field::suggest(pers, &requesto).unwrap();
    // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["begeisterung", "begeistern"]);
    // assert_eq!(results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(), ["Begeisterung", "begeistern", "Begeisterung (f)"]);
    assert_eq!(
        results.iter().map(|el| el.0.clone()).collect::<Vec<String>>(),
        ["begeisterung", "begeistern", "begeisterung (f)"]
    );
}

#[test]
fn should_rank_boost_on_anchor_higher_search_on_anchor() {
    let hits_boosted = search_testo_to_doc!(json!({
        "search_req": { "search": {
            "terms":["COllectif"],
            "path": "title"
        }},
        "boost" : [{
            "path":"commonness",
            "boost_fun": "Log2",
            "param": 2
        }]
    }))
    .data;
    let hits_unboosted = search_request_json_to_doc!(json!({
        "search": {
            "terms":["COllectif"],
            "path": "title"
        }
    }))
    .data;

    assert_gt!(hits_boosted[0].hit.score, hits_unboosted[0].hit.score);
}

#[test]
fn should_rank_boost_on_anchor_higher_search_on_1_n() {
    let hits_boosted = search_testo_to_doc!(json!({
        "search_req": {"search": {
            "terms":["boostemich"],
            "path": "meanings.ger[]"
        }},
        "boost" : [{
            "path":"commonness",
            "boost_fun": "Log2",
            "param": 2
        }]
    }))
    .data;
    let hits_unboosted = search_testo_to_doc!(json!({
        "search_req": {"search": {
            "terms":["boostemich"],
            "path": "meanings.ger[]"
        }}
    }))
    .data;

    assert_gt!(hits_boosted[0].hit.score, hits_unboosted[0].hit.score);
}

#[test]
fn should_check_explain_plan_contents() {
    let req = json!({
        "search_req": {"search": {
            "terms":["weich"], // hits welche and weich
            "path": "meanings.ger[]",
            "levenshtein_distance": 1,
            "firstCharExactMatch":true
        }},
        "boost" : [{
            "path":"commonness",
            "boost_fun": "Log2",
            "param": 2
        }]
    });

    let explain = search_testo_to_explain!(req).to_lowercase();
    assert_contains!(explain, "weich"); // include the term
    assert_contains!(explain, "meanings.ger[]"); // include the field
    assert_contains!(explain, "boost");
}

#[test]
fn should_boost_terms_and_from_cache() {
    let req = json!({
        "search_req": {"search": {
            "terms":["weich"],
            "path": "meanings.ger[]",
            "levenshtein_distance": 1,
            "firstCharExactMatch":true
        }},
        "boost_term":[{
            "terms":["9555"],
            "path": "ent_seq",
            "boost": 5.0
        }]
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].doc["meanings"]["ger"][0], "(1) 2 3 super nice weich");

    //using boost cache here
    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].doc["meanings"]["ger"][0], "(1) 2 3 super nice weich");
    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].doc["meanings"]["ger"][0], "(1) 2 3 super nice weich");
}

#[test]
fn should_add_why_found_terms() {
    let req = json!({
        "search_req": {"search": {
            "terms":["weich"],
            "path": "meanings.ger[]",
            "levenshtein_distance": 1,
            "firstCharExactMatch":true
        }},
        "why_found":true,
        "explain": true
    });

    let hits = search_testo_to_doc!(req).data;
    println!("{}", serde_json::to_string_pretty(&hits).unwrap());
    assert_eq!(hits[0].doc["meanings"]["ger"][0], "(1) weich");
}

#[test]
fn or_connect_hits_but_boost_one_term() {
    let req = json!({
        "search_req": {"or":{"queries":[
            {"search": {"terms":["majestät (f)"], "path": "meanings.ger[]", "boost": 2}},
            {"search": {"terms":["urge"], "path": "meanings.eng[]"}}
        ]}}
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].doc["meanings"]["ger"][0], "majestätischer Anblick (m)");
}

#[test]
fn get_bytes_indexed() {
    let pers = &TEST_PERSISTENCE;
    assert_gt!(pers.get_bytes_indexed(), 2685);
}

#[test]
fn boost_text_localitaet() {
    let req = json!({
        "search_req": {
            "or":{
                "queries":[
                    {"search": {"terms":["text"],      "path": "meanings.ger[]"}},
                    {"search": {"terms":["localität"], "path": "meanings.ger[]"}},
                ]
            }
        },
        "text_locality": true,
        "explain": true
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].doc["meanings"]["ger"][0], "text localität");
}

#[test]
fn read_object_only_partly() {
    let pers = &TEST_PERSISTENCE;
    let yay = search::read_data(
        pers,
        4,
        &[
            "commonness".to_string(),
            "ent_seq".to_string(),
            "meanings.ger[]".to_string(),
            "kana[].text".to_string(),
            "kana[].commonness".to_string(),
            "kana[].romaji".to_string(),
            "address[].line[]".to_string(),
        ],
    )
    .unwrap();
    assert_eq!(
        yay,
        json!({
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
        })
    );
}

#[test]
fn should_skip_existing_fields_which_are_not_existent_in_the_object_none_values() {
    let pers = &TEST_PERSISTENCE;
    //Check None values
    let yay = search::read_data(pers, 3, &["mylongtext".to_string()]).unwrap();
    assert_eq!(yay, json!({}));
}

#[test]
fn read_recreate_complete_object_with_read() {
    let pers = &TEST_PERSISTENCE;
    let all_props = pers.metadata.get_all_fields();
    let yay2 = search::read_data(pers, 4, &all_props).unwrap();

    assert_eq!(
        yay2,
        json!({ //TODO FIX INTEGER TO STRING
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
        })
    );
}
