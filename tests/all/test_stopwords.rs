use super::common;
use serde_json::Value;
use veloci::*;

// #[macro_use]
// mod common;

pub fn get_test_data() -> Value {
    json!([
        {
            "title": "die erbin"
        },
        {
            "title": "erbin"
        },
        {
            "title": "der die erbin"
        },
        {
            "title": "asdf die erbin",
            "subtitles": ["die erbin"]
        }
    ])
}

static TEST_FOLDER: &str = "test_stopwords";

lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = {
        let indices = r#"{ "title":{"features": ["Search","PhraseBoost","BoostTextLocality"], "fulltext":{"tokenize":true}}, "subtitles[]":{"features": ["Search","PhraseBoost","BoostTextLocality"], "fulltext":{"tokenize":true} }} "#;
        common::create_test_persistence(TEST_FOLDER, indices, &get_test_data().to_string().as_bytes(), None)
    };
}

// TODO add tests ..
