#![recursion_limit = "128"]

#[macro_use]
extern crate lazy_static;


#[macro_use]
extern crate serde_json;

use search_lib::*;

#[macro_use]
mod common;

static TEST_FOLDER: &str = "mochaTest_large";

lazy_static! {
    static ref TEST_PERSISTENCE:persistence::Persistence = {
        // Start up a test.
        let indices = r#"
        { "tags[]":{"facet":true}}
        "#;

        let mut data:Vec<u8> = vec![];
        for _ in 0..300 {

            let el = r#"{
                "category": "superb",
                "tags": ["nice", "cool"]
            }"#;

            data.extend(el.as_bytes());

            let el = r#"{
                "category": "awesomo",
                "tags": ["is", "cool"]
            }"#;

            data.extend(el.as_bytes());
        }

        common::create_test_persistence(TEST_FOLDER, indices, &data, None)

    };
}

mod tests_large {
    use super::*;
    #[test]
    fn simple_search() {
        let req = json!({
            "search": {
                "terms":["superb"],
                "path": "category"
            }
        });
        assert_eq!(search_testo_to_doc!(req).num_hits, 300);
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
            "search": {"terms":["superb"], "path": "category"},
            "facets": [{"field":"tags[]"}]
        });

        let hits = search_testo_to_doc!(req);
        let facets = hits.facets.unwrap();
        let mut yep = facets.get("tags[]").unwrap().clone();
        yep.sort_by(|a, b| format!("{:?}{:?}", b.1, b.0).cmp(&format!("{:?}{:?}", a.1, a.0)));
        assert_eq!(yep, vec![("nice".to_string(), 300), ("cool".to_string(), 300)]);
    }

}
