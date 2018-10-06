#![recursion_limit = "128"]

#[macro_use]
extern crate lazy_static;
extern crate search_lib;

#[macro_use]
extern crate serde_json;

use search_lib::*;
use serde_json::Value;

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

        let mut persistence_type = persistence::PersistenceType::Transient;
        if let Some(val) = std::env::var_os("PersistenceType") {
            if val.clone().into_string().unwrap() ==  "Transient"{
                persistence_type = persistence::PersistenceType::Transient;
            }else if val.clone().into_string().unwrap() ==  "Persistent"{
                persistence_type = persistence::PersistenceType::Persistent;
            }else{
                panic!("env PersistenceType needs to be Transient or Persistent");
            }
        }

        let mut pers = persistence::Persistence::create_type(TEST_FOLDER.to_string(), persistence_type.clone()).unwrap();

        let mut out:Vec<u8> = vec![];
        search_lib::create::convert_any_json_data_to_line_delimited(&data as &[u8], &mut out).unwrap();

        println!("{:?}", create::create_indices_from_str(&mut pers, std::str::from_utf8(&out).unwrap(), indices, None, true));

        if persistence_type == persistence::PersistenceType::Persistent {
            pers = persistence::Persistence::load(TEST_FOLDER.to_string()).expect("Could not load persistence");
        }

        pers
    };
}

fn search_testo_to_doc(req: Value) -> search::SearchResultWithDoc {
    search_testo_to_doco(req).expect("search error")
}

fn search_testo_to_doco_qp(qp: query_generator::SearchQueryGeneratorParameters) -> search::SearchResultWithDoc {
    let pers = &TEST_PERSISTENCE;
    let requesto = query_generator::search_query(&pers, qp).unwrap();
    search::to_search_result(&pers, search_testo_to_hitso(requesto.clone()).expect("search error"), &requesto.select)
}


fn search_testo_to_doco(req: Value) -> Result<search::SearchResultWithDoc, search::SearchError> {
    let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    search_testo_to_doco_req(requesto)
}

fn search_testo_to_doco_req(requesto: search::Request) -> Result<search::SearchResultWithDoc, search::SearchError> {
    let pers = &TEST_PERSISTENCE;
    Ok(search::to_search_result(&pers, search_testo_to_hitso(requesto.clone())?, &requesto.select))
}

fn search_testo_to_hitso(requesto: search::Request) -> Result<search::SearchResult, search::SearchError> {
    let pers = &TEST_PERSISTENCE;
    let hits = search::search(requesto, &pers)?;
    Ok(hits)
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
        assert_eq!(search_testo_to_doc(req).num_hits, 300);
    }

    #[test]
    fn and_search() {
        let mut params = query_generator::SearchQueryGeneratorParameters::default();
        params.search_term = "superb AND cool".to_string();

        let res = search_testo_to_doco_qp(params);
        assert_eq!(res.num_hits, 300);
    }

    #[test]
    fn or_search() {
        let mut params = query_generator::SearchQueryGeneratorParameters::default();
        params.search_term = "superb OR awesome".to_string();

        let res = search_testo_to_doco_qp(params);
        assert_eq!(res.num_hits, 600);
    }
    #[test]
    fn search_and_filter() {
        let mut params = query_generator::SearchQueryGeneratorParameters::default();
        params.search_term = "superb".to_string();
        params.filter = Some("nice AND superb".to_string());

        let res = search_testo_to_doco_qp(params);
        assert_eq!(res.num_hits, 300);
    }

    #[test]
    fn search_and_get_facet_with_facet_index() {
        let req = json!({
            "search": {"terms":["superb"], "path": "category"},
            "facets": [{"field":"tags[]"}]
        });

        let hits = search_testo_to_doc(req);
        let facets = hits.facets.unwrap();
        let mut yep = facets.get("tags[]").unwrap().clone();
        yep.sort_by(|a, b| format!("{:?}{:?}", b.1, b.0).cmp(&format!("{:?}{:?}", a.1, a.0)));
        assert_eq!(yep, vec![("nice".to_string(), 300), ("cool".to_string(), 300)]);
    }

}
