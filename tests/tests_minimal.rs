#![recursion_limit = "128"]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate serde_json;

use search_lib::*;
use serde_json::Value;

#[macro_use]
mod common;

static TEST_FOLDER: &str = "mochaTest";
lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = { common::create_test_persistence(TEST_FOLDER, "{}", &get_test_data().to_string().as_bytes(), None) };
}

pub fn get_test_data() -> Value {
    //both fields are identity_columns
    json!([
        {
            "field": "test",
            "field2": "test2",
        }
    ])
}

#[test]
fn test_minimal() {
    let req = json!({
        "search": {
            "terms":["test"],
            "path": "field"
        }
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["field"], "test");
}

#[test]
fn test_minimal_with_filter_identity_column_test() {
    let req = json!({
        "search": {
            "terms":["test"],
            "path": "field"
        },
        "filter":{
            "search": {
                "terms":["test"],
                "path": "field"
            }
        }
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 1);

    // panic!("{}", serde_json::to_string_pretty(&TEST_PERSISTENCE.metadata.columns).unwrap());
    assert_eq!(TEST_PERSISTENCE.metadata.columns.get("field").expect("field.textindex not found").is_identity_column, true);
    assert_eq!(hits[0].doc["field"], "test");
}

#[test]
fn test_minimal_or() {
    let req = json!({
        "or":[
        {
            "search": {
                "terms":["test"],
                "path": "field",
            }
        },{
            "search": {
                "terms":["test2"],
                "path": "field",
            }
        }]
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["field"], "test");
}
