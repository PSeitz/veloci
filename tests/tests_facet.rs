#![recursion_limit = "256"]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate serde_json;

use search_lib::*;
use serde_json::Value;

#[macro_use]
mod common;

static TEST_FOLDER: &str = "facetTest";
lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = {
        let indices = r#"
        {
            "tags[]":{"facet":true, "features":["Facets"]},
            "commonness":{"facet":true}
        }
        "#;

        common::create_test_persistence(TEST_FOLDER, indices, get_test_data().to_string().as_bytes(), None)
    };
}

pub fn get_test_data() -> Value {
    json!([
        {
            "commonness": 20,
            "tags": ["nice", "cool"],
            "meanings": {
                "eng" : ["karlo","dignity", "majestic appearance", "will testo"],
                "ger": ["majestätischer Anblick (m)", "majestätisches Aussehen (n)", "Majestät (f)"]
            }
        },
        {
            "commonness": 20,
            "tags": ["nice"],
            "meanings": {
                "eng" : ["will", "urge", "having a long torso"],
                "ger": ["Wollen (n)", "Wille (m)", "Begeisterung (f)", "begeistern"]
            }
        },
        {
            "commonness": 123456,
            "tags": ["nice", "cool"]
        },
        {
            "meanings": {
                "eng" : ["test1"],
                "ger": ["der test", "das ist ein guter Treffer"]
            }
        },
        {
            "commonness": 20,
            "tags": ["Eis", "cool"]
        }
    ])
}

#[test]
fn search_and_get_facet_with_facet_index() {
    let req = json!({
        "search": {"terms":["will"], "path": "meanings.eng[]"},
        "facets": [{"field":"tags[]"}, {"field":"commonness"}]
    });

    let hits = search_testo_to_doc!(req);
    assert_eq!(hits.data.len(), 2);
    let facets = hits.facets.unwrap();
    assert_eq!(facets.get("tags[]").unwrap(), &vec![("nice".to_string(), 2), ("cool".to_string(), 1)]);
    assert_eq!(facets.get("commonness").unwrap(), &vec![("20".to_string(), 2)]);
}

#[test]
fn search_and_get_facet_without_facet_index() {
    // meanings.eng[] hat no facet index and is a 1-n facet
    let req = json!({
        "search": {"terms":["test"], "path": "meanings.ger[]"},
        "facets": [{"field":"meanings.eng[]"}]
    });

    let hits = search_testo_to_doc!(req);
    assert_eq!(hits.data.len(), 1);
    let facets = hits.facets.unwrap();
    assert_eq!(facets.get("meanings.eng[]").unwrap(), &vec![("test1".to_string(), 1)]);
}

#[test]
fn facets() {
    let pers = &TEST_PERSISTENCE;
    let mut yep = facet::get_facet(
        &pers,
        &search::FacetRequest {
            field: "tags[]".to_string(),
            top: Some(10),
        },
        &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    )
    .unwrap();

    yep.sort_by(|a, b| format!("{:?}{:?}", b.1, b.0).cmp(&format!("{:?}{:?}", a.1, a.0)));
    assert_eq!(
        vec![
            ("nice".to_string(), 3),
            ("cool".to_string(), 3),
            ("Eis".to_string(), 1),
        ],
        yep
    );
}
