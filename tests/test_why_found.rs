#![recursion_limit = "128"]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate serde_json;

use veloci::*;
use serde_json::Value;

#[macro_use]
mod common;

pub fn get_test_data() -> Value {
    json!([
        {
            "richtig": "schön super",
            "viele": ["nette", "leute"]
        },
        {
            "custom_tokenized": "test§_ cool _",
            "richtig": "hajoe genau"
        },
        {
            "richtig": "shön",
            "custom_tokenized": "<<cool>>",
            "viele": ["treffers", "und so", "super treffers", "ein längerer Text, um zu checken, dass da nicht umsortiert wird"] //same text "super treffers" multiple times
        },
        {
            "buch": "Taschenbuch (kartoniert)",
            "viele": ["super treffers"] //same text "super treffers" multiple times
        }
    ])
}

static TEST_FOLDER: &str = "mochaTest_wf";

lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = {
        let indices = r#"
        ["*GLOBAL*"]
        features = ['All']
        [richtig.fulltext]
        tokenize = true
        [custom_tokenized.fulltext]
            tokenize = true
            tokenize_on_chars = ['§', '<']
        "#;
        common::create_test_persistence(TEST_FOLDER, indices, &get_test_data().to_string().as_bytes(), None)
    };
}

#[test]
fn get_number_of_docs() {
    let pers = &TEST_PERSISTENCE;
    assert_eq!(pers.get_number_of_documents(), 4);
}

#[test]
fn should_highlight_properly_with_custom_tokenized() {
    let req = json!({
        "search": {
            "terms":["test"],
            "path": "custom_tokenized",
        },
        "why_found":true
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].why_found["custom_tokenized"], vec!["<b>test</b>§_ cool _"]);

    let req = json!({
        "search": {
            "terms":["§"],
            "path": "custom_tokenized",
        },
        "why_found":true
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].why_found["custom_tokenized"], vec!["test<b>§</b>_ cool _"]);

    let req = json!({
        "search": {
            "terms":["_ cool _"],
            "path": "custom_tokenized",
        },
        "why_found":true
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].why_found["custom_tokenized"], vec!["test§<b>_ cool _</b>"]);

    let req = json!({
        "search": {
            "terms":["<<"],
            "path": "custom_tokenized",
        },
        "why_found":true
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].why_found["custom_tokenized"], vec!["<b><<</b>cool>>"]);
}

//TODO FIXME BUG: THIS API SHOULD TOKENIZE THE TERM appropriately. CURRENTLY NO TOKENIZING IS DONE, AND THEREFORE NOTHING IS HIT
// #[test]
// fn should_highlight_properly_when_complete_text_is_hit() {

//     let req = json!({
//         "search": {
//             "terms":["<<cool"],
//             "path": "custom_tokenized",
//         },
//         "why_found":true
//     });
//     let hits = search_testo_to_doc!(req).data;
//     assert_eq!(hits[0].why_found["custom_tokenized"], vec!["<b><<cool</b>>>"]);

// }

//TODO ADD TEST FOR WITHOUT WHY_FOUND
#[test]
fn should_highlight_properly_when_complete_text_is_hit() {
    let req = json!({
        "search": {
            "terms":["<<cool>>"],
            "path": "custom_tokenized",
        },
        "why_found":true
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].why_found["custom_tokenized"], vec!["<b><<cool>></b>"]);
}

#[test]
fn should_not_hit_because_in_custom_tokenizer_space_is_not_a_seperator() {
    let req = json!({
        "search": {
            "terms":["cool"],
            "path": "custom_tokenized",
        },
        "why_found":true
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 0);
}

#[test]
fn should_add_why_found_terms_highlight_tokens_and_also_text_ids() {
    let req = json!({
        "search": {
            "terms":["schön"],
            "path": "richtig",
            "levenshtein_distance": 1
        },
        "why_found":true
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].why_found["richtig"], vec!["<b>schön</b> super"]);
    assert_eq!(hits[1].why_found["richtig"], vec!["<b>shön</b>"]);
}

#[test]
fn should_add_why_found_from_1_n_terms_highlight_tokens_and_also_text_ids() {
    let req = json!({
        "search": {
            "terms":["treffers"],
            "path": "viele[]",
            "levenshtein_distance": 1
        },
        "why_found":true
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].why_found["viele[]"], vec!["<b>treffers</b>", "super <b>treffers</b>"]);
}

#[test]
fn should_add_why_found_from_1_n_terms_because_when_select_is_used_a_different_why_found_strategy_is_used() {
    let req = json!({
        "search": {
            "terms":["umsortiert"],
            "path": "viele[]",
            "levenshtein_distance": 0
        },
        "why_found":true,
        "select": ["richtig"]
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].doc["richtig"], "shön");
    assert_eq!(
        hits[0].why_found["viele[]"],
        vec![" ... zu checken, dass da nicht <b>umsortiert</b> wird"] // TODO FIXME 1. Should not behave differently, why_found with select
    );
    let req = json!({
        "search": {
            "terms":["umsortiert"],
            "path": "viele[]",
            "levenshtein_distance": 0
        },
        "why_found":true
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].doc["richtig"], "shön");
    assert_eq!(
        hits[0].why_found["viele[]"],
        vec![" ... zu checken, dass da nicht <b>umsortiert</b> wird"] // TODO FIXME 2. Should not behave differently, why found on doc
    );
}

#[test]
fn should_add_highlight_taschenbuch() {
    let req = json!({
        "search": {
            "terms":["Taschenbuch"],
            "path": "buch",
            "levenshtein_distance": 1
        },
        "why_found":true
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].why_found["buch"], vec!["<b>Taschenbuch</b> (kartoniert)"]);
}

#[test]
fn should_add_highlight_multi_terms() {
    let req = json!({
        "or":[
        {
            "search": {
                "terms":["Taschenbuch"],
                "path": "buch",
                "levenshtein_distance": 1
            },
            "why_found":true
        },{
            "search": {
                "terms":["kartoniert"],
                "path": "buch",
                "levenshtein_distance": 1
            },
            "why_found":true
        }],
        "why_found":true
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].why_found["buch"], vec!["<b>Taschenbuch</b> (<b>kartoniert</b>)"]);
}
