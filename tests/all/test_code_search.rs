extern crate more_asserts;

use serde_json::Value;
use veloci::*;

use super::common;

static TEST_FOLDER: &str = "codeTest";
lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = {
        let indices = r#"
        ["*GLOBAL*"]
            features = ["All"]
        ["filepath"]
            tokenize = true
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
fn pattern_code_search() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "*myfun*Type1*".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["line"], "function myfun(param1: Type1)");
}

#[test]
fn pattern_code_search_allows_ignore_case() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "*myfun*type1*".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc["line"], "function myfun(param1: Type1)");
}

#[test]
fn pattern_code_search_no_fuzzy() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "*myfun*type2*".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 0);
}

#[test]
fn token_code_search() {

    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "myfun".to_string();

    let hits = search_testo_to_doco_qp!(params).data;
    assert_eq!(hits.len(), 1);

}
