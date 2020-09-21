extern crate more_asserts;

use serde_json::Value;
use veloci::*;

use super::common;

static TEST_FOLDER: &str = "mochaTest";
lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = {
        let indices = r#"
        ["*GLOBAL*"]
            features = ["All"]
        ["commonness"]
            facet = true
        ["commonness".boost]
            boost_type = "int"
        ["ent_seq".fulltext]
            tokenize = true
        ["nofulltext".fulltext]
            tokenize = false
        ["tags[]"]
            facet = true
        ["field1[].rank".boost]
            boost_type = "int"
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
            boost_type = "int"
        ["kana[].commonness".boost]
            boost_type = "int"
        "#;

        let token_values = Some((r#"[{"text": "Begeisterung", "value": 20 } ]"#.to_string(), json!({"path": "meanings.ger[]"})));

        common::create_test_persistence(TEST_FOLDER, indices, get_test_data().to_string().as_bytes(), token_values)
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
            "ent_seq": "1337",
            "commonness": 20,
            "tags": ["nice", "cool", "ent_seq:99999"], // ent_seq:99999 to test no_attributes
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
                "eng" : ["will testo"],
                "ger": ["majestätischer Anblick (m)", "majestätisches Aussehen (n)", "Majestät (f)"]
            }
        },
        {
            "ent_seq": "1587690",
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
            }
        },
        {
            "id": 1234566,
            "tags": ["awesome", "cool"],
            "commonness": 500,
            "kanji": [
                { "text": "意慾", "commonness": 20}
            ],
            "kana": [
                {
                    "text": "いよく"
                }
            ],
            "ent_seq": "1587700"
        },
        {
            "commonness": 515151,
            "ent_seq": "25",
            "tags": ["nice", "cool"]
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

#[test]
fn simple_search_querygenerator_explained() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.explain = Some(true);
    params.search_term = "urge".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1587690");
    assert_eq!(hits[0].doc["commonness"], 20);
    assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
    // assert_eq!(hits[0].explain, Some(to_vec(&["or sum_over_distinct_terms 36.8125", "term score 10.0 * anchor score 3.68 to 36.8", "levenshtein score 10.0 for urge"])));
    println!("{:?}", hits[0].explain);
    assert_eq!(hits[0].explain.as_ref().unwrap().len(), 5);
}

#[test]
fn simple_search_querygenerator_or_connect_explained() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.explain = Some(true);
    params.search_term = "urge OR いよく".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0].doc["ent_seq"], "1587690");
    assert_eq!(hits[0].doc["commonness"], 20);
    assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
    // assert_eq!(hits[0].explain, Some(vec!["or sum_over_distinct_terms 452.375".to_string(), "term score 15.0 * anchor score 3.7 to 55.5".to_string(), "term score 15.0 * anchor score 3.84 to 57.6".to_string()]));
    // assert_eq!(hits[0].explain, None);
    assert_eq!(hits[0].explain.as_ref().unwrap().len(), 7);
}
#[test]
fn simple_search_querygenerator() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "urge".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1587690");
    assert_eq!(hits[0].doc["commonness"], 20);
    assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
}

#[test]
fn attributed_search() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    // attributed search, it will search for "99999" on field ent_seq
    params.search_term = "ent_seq:99999".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "99999");
}
#[test]
fn disabled_attributed_search() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "ent_seq:99999".to_string();
    // disabling attributed search, that means, it will search for "ent_seq:99999" on all fields
    params.parser_options = Some(custom_parser::Options{no_attributes: true, ..Default::default()});

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1337");
}

#[test]
fn simple_search_querygenerator_or_connect() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "urge OR いよく".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0].doc["ent_seq"], "1587690");
    assert_eq!(hits[0].doc["commonness"], 20);
    assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
}

#[test]
fn simple_search_querygenerator_and() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "urge AND いよく".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1587690");
    assert_eq!(hits[0].doc["commonness"], 20);
    assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
}
#[test]
fn simple_search_querygenerator_and_emtpy_stopword_list() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.stopword_lists = Some(vec![]);
    params.search_term = "urge AND いよく".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1587690");
    assert_eq!(hits[0].doc["commonness"], 20);
    assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
}
#[test]
fn simple_search_querygenerator_and_stopword_list() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.stopword_lists = Some(vec!["en".to_string()]);
    params.search_term = "urge AND いよく".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1587690");
    assert_eq!(hits[0].doc["commonness"], 20);
    assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
}

#[test]
fn simple_search_querygenerator_and_stopword_list_from_json() {
    let params = json!({
        "stopword_lists": ["en"],
        "search_term": "urge AND いよく"
    });

    let params: query_generator::SearchQueryGeneratorParameters = serde_json::from_str(&params.to_string()).expect("Can't parse json");

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["ent_seq"], "1587690");
    assert_eq!(hits[0].doc["commonness"], 20);
    assert_eq!(hits[0].doc["tags"], json!(["nice".to_string()]));
}

#[test]
fn complex_search_querygenerator_from_json() {
    let params = json!({
        "search_term": "will",
        "top": 10,
        "facets": ["commonness","kanji[].commonness"],
        "levenshtein": 0,
        "boost_fields": {"meanings.eng[]": 1.5}
    });

    let params: query_generator::SearchQueryGeneratorParameters = serde_json::from_str(&params.to_string()).expect("Can't parse json");
    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].doc["meanings"]["eng"][0], "will");

    let params = json!({
        "search_term": "will",
        "top": 10,
        "facets": ["commonness","kanji[].commonness"],
        "levenshtein": 0,
        "boost_fields": {"meanings.eng[]": 1.5},
        "boost_terms": {"meanings.ger[]:majestätisches Aussehen (n)": 20.0}
    });

    let params: query_generator::SearchQueryGeneratorParameters = serde_json::from_str(&params.to_string()).expect("Can't parse json");
    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].doc["meanings"]["eng"][0], "will testo");
}

#[test]
fn simple_search_querygenerator_and_no_hit() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "urge AND いよく AND awesome".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 0);
}

#[test]
fn simple_search_wildcard_starts_with() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "awes*".to_string();

    let hits = search_testo_to_doco_qp!(params.clone()).data;
    assert_eq!(hits.len(), 1);

    params.search_term = "いよ*".to_string();
    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 3);
}

#[test]
fn no_matching_fields_from_field_list() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "awes*".to_string();
    params.fields = Some(vec!["notexistingfield".to_string()]);

    let requesto = query_generator::search_query(&TEST_PERSISTENCE, params);
    assert_eq!(requesto.is_err(), true);
    assert_contains!(requesto.unwrap_err().to_string(), "All fields filtered");
}

#[test]
fn no_matching_fields_from_query() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "notexistingfield:awes*".to_string();

    let requesto = query_generator::search_query(&TEST_PERSISTENCE, params);
    assert_eq!(requesto.is_err(), true);
    assert_contains!(requesto.unwrap_err().to_string(), "Field notexistingfield not found in");
}

// #[test]
// fn no_matching_fields_in_facet() {
//     let params = json!({
//         "search_term": "will",
//         "facets": ["tagso[]"],
//         "boost_fields": {"meanings.eng[]": 1.5}
//     });

//     let params: query_generator::SearchQueryGeneratorParameters = serde_json::from_str(&params.to_string()).expect("Can't parse json");
//     let requesto = query_generator::search_query(&TEST_PERSISTENCE, params);

//     assert_eq!(requesto.is_err(), true);
//     assert_contains!(requesto.unwrap_err().to_string(), "Field tagso[] not found in");

// }

// // TODO tags[] is configured as a field, but no data is provided, it should still be in the get_all_search_field_names list
// #[test]
// fn no_matching_fields_in_facet_todo() {
//     let params = json!({
//         "search_term": "will",
//         "facets": ["tags[]"],
//         "boost_fields": {"meanings.eng[]": 1.5}
//     });

//     let params: query_generator::SearchQueryGeneratorParameters = serde_json::from_str(&params.to_string()).expect("Can't parse json");
//     let requesto = query_generator::search_query(&TEST_PERSISTENCE, params);

//     assert_eq!(requesto.is_err(), true);
//     assert_contains!(requesto.unwrap_err().to_string(), "Field tags[] not found in");

// }

//TODO validate boost_fields

// TODO FIXME
// #[test]
// fn simple_search_querygenerator_suggest() {
//     let query = query_generator::suggest_query(
//         "Begeisteru",
//         &TEST_PERSISTENCE,
//         Some(10),
//         None,
//         Some(1),
//         &Some(vec!["meanings.ger[]".to_string()]),
//         None,
//     ).unwrap();

//     let hits = search_to_hits!(query).unwrap();
//     assert_eq!(hits.data.len(), 2);
// }
