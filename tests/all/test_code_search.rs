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
            seperators = ["\", "/"]
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
            "line_number": 123456,
            "line": "function myfun(param1: Type1)",
            "filename": "cool.ts"
            "filepath": "all/the/path"
        }
    ])
}

#[test]
fn simple_code_search() {

}
