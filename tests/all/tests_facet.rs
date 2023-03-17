use serde_json::Value;
use veloci::*;

use super::common;

static TEST_FOLDER: &str = "facetTest";
lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = {
        let indices = r#"
        ["*GLOBAL*"]
        features = ['All']

        ["tags[]"]
        facet = true
        features = ['Facets']

        [commonness]
        facet = true

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
        "search_req": {"search": {"terms":["will"], "path": "meanings.eng[]"}},
        "facets": [{"field":"tags[]"}, {"field":"commonness"}]
    });

    let hits = search_testo_to_doc!(req);
    assert_eq!(hits.data.len(), 2);
    let facets = hits.facets.unwrap();
    assert_eq!(facets.get("tags[]").unwrap(), &vec![("nice".to_string(), 2), ("cool".to_string(), 1)]);
    assert_eq!(facets.get("commonness").unwrap(), &vec![("20".to_string(), 2)]);
}

#[test]
fn search_query_params_and_get_facet_with_facet_index() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "will".to_string();
    params.facets = Some(vec!["tags[]".to_string(), "commonness".to_string()]);
    params.fields = Some(vec!["meanings.eng[]".to_string()]);

    let hits = search_testo_to_doco_qp!(params);
    assert_eq!(hits.data.len(), 2);
    let facets = hits.facets.unwrap();
    assert_eq!(facets.get("tags[]").unwrap(), &vec![("nice".to_string(), 2), ("cool".to_string(), 1)]);
    assert_eq!(facets.get("commonness").unwrap(), &vec![("20".to_string(), 2)]);
}

#[test]
fn search_and_get_facet_without_facet_index() {
    // meanings.eng[] hat no facet index and is a 1-n facet
    let req = json!({
        "search_req": {"search": {"terms":["test"], "path": "meanings.ger[]"}},
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
        pers,
        &search::FacetRequest {
            field: "tags[]".to_string(),
            top: Some(10),
        },
        &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    )
    .unwrap();

    yep.sort_by(|a, b| format!("{:?}{:?}", b.1, b.0).cmp(&format!("{:?}{:?}", a.1, a.0)));
    assert_eq!(vec![("nice".to_string(), 3), ("cool".to_string(), 3), ("Eis".to_string(), 1),], yep);
}
