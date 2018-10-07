#![recursion_limit = "128"]

#[macro_use]
extern crate lazy_static;
extern crate search_lib;
#[macro_use]
extern crate serde_json;

use search_lib::*;
use serde_json::Value;

#[macro_use]
mod common;

pub fn get_test_data() -> Value {
    json!([
        {
            "title": "die erbin"
        },
        {
            "title": "erbin",
            "tags": ["die", "erbin"]
        },
        {
            "tags": ["greg tagebuch 05"]
        },
        {
            "tags": ["greg tagebuch", "05"]
        },
        {
            "title": "greg tagebuch",
            "tags": ["greg tagebuch", "05"]
        }
    ])
}

static TEST_FOLDER: &str = "mochaTest_phrase";

lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = {
        let indices = r#"{ "title":{"fulltext":{"tokenize":true} }} "#;
        common::create_test_persistence(TEST_FOLDER, indices, &get_test_data().to_string().as_bytes(), None)
    };
}

#[test]
fn should_boost_phrase() {
    let req = json!({
        "search": {"terms":["erbin"], "path": "title"},
        "phrase_boosts": [{
            "path":"title",
            "search1":{"terms":["die"], "path": "title"},
            "search2":{"terms":["erbin"], "path": "title"}
        }]
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].doc["title"], "die erbin");
}

#[test]
fn should_boost_phrase_search_multifield() {
    let req = json!({
        "or":[
            {"search": {"terms":["die"], "path": "title" }},
            {"search": {"terms":["erbin"], "path": "title" }},
            {"search": {"terms":["die"], "path": "tags[]" }},
            {"search": {"terms":["erbin"], "path": "tags[]" }}
        ],
        "phrase_boosts": [{
            "path":"title",
            "search1":{"terms":["die"], "path": "title" },
            "search2":{"terms":["erbin"], "path": "title" }
        },{
            "path":"tags[]",
            "search1":{"terms":["die"], "path": "tags[]" },
            "search2":{"terms":["erbin"], "path": "tags[]" }
        }]
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].doc["title"], "die erbin");
}

#[test]
fn should_and_boost_phrase_search() {
    let req = json!({
        "and":[
            {"search": {"terms":["die"], "path": "title" }},
            {"search": {"terms":["erbin"], "path": "title" }}
        ],
        "phrase_boosts": [{
            "path":"title",
            "search1":{"terms":["die"], "path": "title" },
            "search2":{"terms":["erbin"], "path": "title" }
        }]
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].doc["title"], "die erbin");
}

#[test]
fn should_and_boost_phrase_a_n_d_query_generator() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "die AND erbin".to_string();
    params.phrase_pairs = Some(true);
    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits[0].doc["title"], "die erbin");
}

#[test]
fn should_and_boost_phrase_query_generator_and_explain() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "die erbin".to_string();
    params.phrase_pairs = Some(true);
    params.explain = Some(true);
    let hits = search_testo_to_doco_qp!(params).data;
    println!("{:?}", hits);
    assert_eq!(hits[0].doc["title"], "die erbin");
}

#[test]
fn should_and_boost_phrase_o_r_query_generator() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "die erbin".to_string();
    params.phrase_pairs = Some(true);
    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits[0].doc["title"], "die erbin");
}

#[test]
fn should_double_boost_from_multiphrases() {

    // This query will hit ["greg tagebuch", "05"] from different texts, boosting only for greg tagebuch
    let req_with_single_phrase = json!({
        "or":[
            {"search": {"terms":["greg"], "path": "tags[]" }},
            {"search": {"terms":["tagebuch"], "path": "tags[]" }},
            {"search": {"terms":["05"], "path": "tags[]" }}
        ],
        "phrase_boosts": [{
            "path":"tags[]",
            "search1":{"terms":["greg"], "path": "tags[]" },
            "search2":{"terms":["tagebuch"], "path": "tags[]" }
        }]
    });

    let hits = search_testo_to_doc!(req_with_single_phrase).data;
    assert_eq!(hits[0].doc["tags"][0], "greg tagebuch");
    assert_eq!(hits[0].doc["tags"][1], "05");

    // This query will hit ["greg tagebuch 05"]
    let req_with_multi_phrase = json!({
        "or":[
            {"search": {"terms":["greg"], "path": "tags[]" }},
            {"search": {"terms":["tagebuch"], "path": "tags[]" }},
            {"search": {"terms":["05"], "path": "tags[]" }}
        ],
        "phrase_boosts": [{
            "path":"tags[]",
            "search1":{"terms":["greg"], "path": "tags[]" },
            "search2":{"terms":["tagebuch"], "path": "tags[]" }
        },{
            "path":"tags[]",
            "search1":{"terms":["tagebuch"], "path": "tags[]" },
            "search2":{"terms":["05"], "path": "tags[]" }
        }]
    });

    let hits = search_testo_to_doc!(req_with_multi_phrase).data;
    assert_eq!(hits[0].doc["tags"][0], "greg tagebuch 05");
}
#[test]
fn should_double_boost_from_multiphrases_a_n_d_searchterms() {
    let req_with_single_phrase = json!({
        "and":[
            {"search": {"terms":["greg"], "path": "tags[]" }},
            {"search": {"terms":["tagebuch"], "path": "tags[]" }},
            {"search": {"terms":["05"], "path": "tags[]" }}
        ],
        "phrase_boosts": [{
            "path":"tags[]",
            "search1":{"terms":["greg"], "path": "tags[]" },
            "search2":{"terms":["tagebuch"], "path": "tags[]" }
        }]
    });

    let hits = search_testo_to_doc!(req_with_single_phrase).data;
    assert_eq!(hits[0].doc["tags"][0], "greg tagebuch");

    let req_with_multi_phrase = json!({
        "and":[
            {"search": {"terms":["greg"], "path": "tags[]" }},
            {"search": {"terms":["tagebuch"], "path": "tags[]" }},
            {"search": {"terms":["05"], "path": "tags[]" }}
        ],
        "phrase_boosts": [{
            "path":"tags[]",
            "search1":{"terms":["greg"], "path": "tags[]" },
            "search2":{"terms":["tagebuch"], "path": "tags[]" }
        },{
            "path":"tags[]",
            "search1":{"terms":["tagebuch"], "path": "tags[]" },
            "search2":{"terms":["05"], "path": "tags[]" }
        }]
    });

    let hits = search_testo_to_doc!(req_with_multi_phrase).data;
    assert_eq!(hits[0].doc["tags"][0], "greg tagebuch 05");
}
#[test]
fn should_prefer_different_phrases_from_same_phrase_multiple_times() {
    let req_with_single_phrase = json!({
        "or":[
            {"search": {"terms":["greg"], "path": "tags[]" }},
            {"search": {"terms":["tagebuch"], "path": "tags[]" }},
            {"search": {"terms":["05"], "path": "tags[]" }},
            {"search": {"terms":["greg"], "path": "title" }},
            {"search": {"terms":["tagebuch"], "path": "title" }},
            {"search": {"terms":["05"], "path": "title" }}
        ],
        "phrase_boosts": [{
                "path":"tags[]",
                "search1":{"terms":["greg"], "path": "tags[]" },
                "search2":{"terms":["tagebuch"], "path": "tags[]" }
            },
            {
                "path":"title",
                "search1":{"terms":["greg"], "path": "title" },
                "search2":{"terms":["tagebuch"], "path": "title" }
            },
            {
                "path":"tags[]",
                "search1":{"terms":["tagebuch"], "path": "tags[]" },
                "search2":{"terms":["05"], "path": "tags[]" }
            },
            {
                "path":"title",
                "search1":{"terms":["tagebuch"], "path": "title" },
                "search2":{"terms":["05"], "path": "title" }
            }
        ]
    });

    let hits = search_testo_to_doc!(req_with_single_phrase).data;
    assert_eq!(hits[0].doc["tags"][0], "greg tagebuch 05");
}
