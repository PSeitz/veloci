extern crate more_asserts;

use serde_json::Value;
use veloci::{
    search::{RequestSearchPart, SearchRequest},
    *,
};

use super::common;

static TEST_FOLDER: &str = "codeTest";
lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = {
        let indices = r#"
        ["*GLOBAL*"]
            features = ["All"]
        ["filepath"]
            tokenize = true
            tokenize_on_chars = ['/', '\']
        ["filename"]
            tokenize = true
        ["line"]
            tokenize = true
        ["line_number"]
            boost_type = "int"
        "#;

        common::create_test_persistence(TEST_FOLDER, indices, get_test_data().to_string().as_bytes(), None)
    };
}

pub fn get_test_data() -> Value {
    json!([
        {
            "line_number": 1,
            "line": "function myfun(param1: Type1)",
            "filename": "cool.ts",
            "filepath": "all/the/path"
        }
    ])
}

#[test]
fn regex_search_request_api() {
    let req = SearchRequest::Search(RequestSearchPart {
        terms: vec![".*myfun.*type1.*".to_string()],
        path: "line".to_string(),
        is_regex: true,
        ignore_case: Some(true),
        ..Default::default()
    });

    let hits = search_request_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["line"], "function myfun(param1: Type1)");
}

#[test]
fn regex_search_request_api_case_sensitive() {
    // case sensitive no hit
    let req = SearchRequest::Search(RequestSearchPart {
        terms: vec![".*myfun.*type1.*".to_string()],
        path: "line".to_string(),
        is_regex: true,
        ignore_case: Some(false),
        ..Default::default()
    });

    let hits = search_request_to_doc!(req).data;
    assert_eq!(hits.len(), 0);
}

#[test]
fn pattern_code_search_query_generator() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "*myfun*Type1*".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["line"], "function myfun(param1: Type1)");
}

#[test]
fn pattern_code_search_allows_ignore_case_query_generator() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "*myfun*type1*".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["line"], "function myfun(param1: Type1)");
}

#[test]
fn pattern_code_search_no_fuzzy_query_generator() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "*myfun*type2*".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 0);
}

#[test]
fn token_code_search_query_generator() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "myfun".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
}

// pasting code will most certainly conflict with the query parser used in the query_generator
// it's possible to disable them for this purpose
#[test]
fn token_code_search_disable_parser_query_generator() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.parser_options = Some(query_parser::Options {
        no_parentheses: true,
        no_attributes: true,
        no_levensthein: true,
    });
    params.search_term = "*myfun(param1: Type1)*".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
}

// pasting code will most certainly conflict with the query parser, in this case we can quote the query
#[test]
fn token_code_phrase_pattern_query_generator() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "\"*myfun(param1: Type1)*\"".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
}
