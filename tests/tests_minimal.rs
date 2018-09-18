#![feature(plugin)]
#![cfg_attr(test, plugin(stainless))]
#![recursion_limit = "128"]

#[macro_use]
extern crate lazy_static;
extern crate search_lib;
#[macro_use]
extern crate serde_json;

use search_lib::create;
use search_lib::facet;
use search_lib::persistence;
use search_lib::query_generator;
use search_lib::search;
use search_lib::search_field;
use search_lib::trace;
use serde_json::Value;

fn search_testo_to_doc(req: Value) -> search::SearchResultWithDoc {
    search_testo_to_doco(req).expect("search error")
}

fn search_testo_to_doco(req: Value) -> Result<search::SearchResultWithDoc, search::SearchError> {
    let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    search_testo_to_doco_req(requesto, &TEST_PERSISTENCE)
}

fn search_testo_to_doco_req(requesto: search::Request, pers: &persistence::Persistence) -> Result<search::SearchResultWithDoc, search::SearchError> {
    Ok(search::to_search_result(&pers, search_testo_to_hitso(requesto.clone())?, &requesto.select))
}

fn search_testo_to_hitso(requesto: search::Request) -> Result<search::SearchResult, search::SearchError> {
    let pers = &TEST_PERSISTENCE;
    let hits = search::search(requesto, &pers)?;
    Ok(hits)
}

static TEST_FOLDER: &str = "mochaTest";
lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = {
        trace::enable_log();

        let data = get_test_data();

        let mut persistence_type = persistence::PersistenceType::Transient;
        if let Some(val) = std::env::var_os("PersistenceType") {
            if val.clone().into_string().unwrap() == "Transient" {
                persistence_type = persistence::PersistenceType::Transient;
            } else if val.clone().into_string().unwrap() == "Persistent" {
                persistence_type = persistence::PersistenceType::Persistent;
            } else {
                panic!("env PersistenceType needs to be Transient or Persistent");
            }
        }

        let mut pers = persistence::Persistence::create_type(TEST_FOLDER.to_string(), persistence_type.clone()).unwrap();

        let mut out: Vec<u8> = vec![];
        search_lib::create::convert_any_json_data_to_line_delimited(data.to_string().as_bytes(), &mut out).unwrap();
        println!("{:?}", create::create_indices_from_str(&mut pers, std::str::from_utf8(&out).unwrap(), "{}", None, true));

        if persistence_type == persistence::PersistenceType::Persistent {
            pers = persistence::Persistence::load(TEST_FOLDER.to_string()).expect("Could not load persistence");
        }
        pers
    };
}

pub fn get_test_data() -> Value {
    json!([
        {
            "field": "test",
            "field2": "test2",
        }
    ])
}

describe! search_test {

    it "test_minimal"{
        let req = json!({
            "search": {
                "terms":["test"],
                "path": "field"
            }
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc["field"], "test");
    }

    it "test_minimal_or"{
        let req = json!({
            "or":[
            {
                "search": {
                    "terms":["test"],
                    "path": "field",
                }
            },{
                "search": {
                    "terms":["test2"],
                    "path": "field",
                }
            }]
        });

        let hits = search_testo_to_doc(req).data;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].doc["field"], "test");
    }

}
