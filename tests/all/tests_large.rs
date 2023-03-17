use veloci::*;

use super::common;
static TEST_FOLDER: &str = "mochaTest_large";

lazy_static! {
    static ref TEST_PERSISTENCE:persistence::Persistence = {
        // Start up a test.
        let indices = r#"
        ["*GLOBAL*"]
        features = [
            'All',
        ]

        ["tags[]"]
        facet = true
        "#;

        let mut data:String = r#"
            {
                "category": "superb",
                "tags": ["nice", "cool"]
            }
            {
                "category": "awesomo",
                "tags": ["is", "cool"]
            }
        "#.repeat(300);

        data += r#"{
            "text": "a long text with more than 64 characters so that the option do_not_store_text_longer_than is active. then the whole text won't be store in the fst, only its tokens"
        }"#;

        common::create_test_persistence_with_logging(TEST_FOLDER, indices, data.as_bytes(), None, false)

    };
}

use super::*;
#[test]
fn simple_search() {
    let req = json!({
        "search": {
            "terms":["superb"],
            "path": "category"
        }
    });
    assert_eq!(search_request_json_to_doc!(req).num_hits, 300);
}

#[test]
fn select_on_large_text() {
    let req = json!({
        "search_req": {
            "search": {
                "terms":["long"],
                "path": "text"
            }
        },
        "select": ["text"]
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].doc["text"],
        "a long text with more than 64 characters so that the option do_not_store_text_longer_than is active. then the whole text won't be store in the fst, only its tokens"
    );
    assert_eq!(hits[0].doc.get("category"), None); // didn't select
}

#[test]
fn and_search() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "superb AND cool".to_string();

    let res = search_testo_to_doco_qp!(params);
    assert_eq!(res.num_hits, 300);
}

#[test]
fn or_search() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "superb OR awesome".to_string();

    let res = search_testo_to_doco_qp!(params);
    assert_eq!(res.num_hits, 600);
}

#[test]
fn search_and_filter() {
    let mut params = query_generator::SearchQueryGeneratorParameters::default();
    params.search_term = "superb".to_string();
    params.filter = Some("nice AND superb".to_string());

    let res = search_testo_to_doco_qp!(params);
    assert_eq!(res.num_hits, 300);
}

#[test]
fn search_and_get_facet_with_facet_index() {
    let req = json!({
        "search_req": {"search": {"terms":["superb"], "path": "category"}},
        "facets": [{"field":"tags[]"}]
    });

    let hits = search_testo_to_doc!(req);
    let facets = hits.facets.unwrap();
    let mut yep = facets.get("tags[]").unwrap().clone();
    yep.sort_by(|a, b| format!("{:?}{:?}", b.1, b.0).cmp(&format!("{:?}{:?}", a.1, a.0)));
    assert_eq!(yep, vec![("nice".to_string(), 300), ("cool".to_string(), 300)]);
}
