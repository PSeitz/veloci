use serde_json::Value;
use veloci::{search::*, *};

use super::common;

static TEST_FOLDER: &str = "test_minimal";
lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = common::create_test_persistence(TEST_FOLDER, "{}", get_test_data().to_string().as_bytes(), None);
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

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["field"], "test");
}

#[test]
fn test_minimal_with_filter_identity_column_test() {
    let req = json!({
        "search_req": {
            "search": {
                "terms":["test"],
                "path": "field"
            }
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
    assert!(TEST_PERSISTENCE.metadata.columns.get("field").expect("field.textindex not found").is_anchor_identity_column);
    assert_eq!(hits[0].doc["field"], "test");
}

#[test]
fn test_minimal_or_json() {
    let req = json!({
        "or":{
            "queries": [
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
                }
            ]}
    });

    let hits = search_request_json_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["field"], "test");
}

#[test]
fn test_minimal_or_object() {
    let req = SearchRequest::Or(SearchTree {
        queries: vec![
            SearchRequest::Search(RequestSearchPart {
                terms: vec!["test".to_string()],
                path: "field".to_string(),
                ..Default::default()
            }),
            SearchRequest::Search(RequestSearchPart {
                terms: vec!["test2".to_string()],
                path: "field".to_string(),
                ..Default::default()
            }),
        ],
        ..Default::default()
    });

    let hits = search_request_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["field"], "test");
}
