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
            "title": "greg tagebuch 05",
        },
        {
            "title": "and some some text 05 this is not relevant let tagebuch greg",
        },
        {
            "title": "greg tagebuch"
        }
    ])
}

static TEST_FOLDER: &str = "mochaTest_score";

lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = {
        let indices = r#"{ "title":{"fulltext":{"tokenize":true} }} "#;
        common::create_test_persistence(TEST_FOLDER, indices, &get_test_data().to_string().as_bytes(), None)
    };
}

#[test]
fn check_score_regarding_to_length() {
    let req_with_single_phrase = json!({
        "or":[
            {"search": {"terms":["greg"], "path": "title" }},
            {"search": {"terms":["tagebuch"], "path": "title" }},
            {"search": {"terms":["05"], "path": "title" }}
        ],
        "phrase_boosts": [{
            "path":"title",
            "search1":{"terms":["greg"], "path": "title" },
            "search2":{"terms":["tagebuch"], "path": "title" }
        }]
    });

    let res = search_testo_to_doc!(req_with_single_phrase);
    assert_eq!(res.data[0].doc["title"], "greg tagebuch 05"); //hits 3 tokens and phrases
    assert_eq!(res.data[1].doc["title"], "greg tagebuch"); //hits 2 tokens and phrases
    assert_eq!(res.data[2].doc["title"], "and some some text 05 this is not relevant let tagebuch greg"); //hits 3 tokens but no phrases
}
