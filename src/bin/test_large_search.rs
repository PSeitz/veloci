#![recursion_limit = "128"]

#[macro_use]
extern crate log;

extern crate env_logger;
extern crate flexi_logger;
extern crate fst;
// extern crate fst_levenshtein;
// extern crate cpuprofiler;
#[macro_use]
extern crate measure_time;
extern crate rayon;
extern crate search_lib;
#[macro_use]
extern crate serde_json;

#[allow(unused_imports)]
use fst::{IntoStreamer, MapBuilder, Set};
// use fst_levenshtein::Levenshtein;
// use serde_json::{Deserializer, Value};
use search_lib::*;
use std::str;

#[allow(unused_imports)]
use rayon::prelude::*;

static TEST_FOLDER: &str = "bench_taschenbuch";

fn load_persistence_disk() -> persistence::Persistence {
    use std::path::Path;
    if Path::new(TEST_FOLDER).exists() {
        return persistence::Persistence::load(TEST_FOLDER.to_string()).expect("Could not load persistence");
    }
    let object = r#"{"type":"taschenbuch","title":"mein buch"}"#.to_owned() + "\n";
    let mut data = String::new();
    for _ in 0..6_000_000 {
        data += &object;
    }
    let mut pers = persistence::Persistence::create_type(TEST_FOLDER.to_string(), persistence::PersistenceType::Persistent).unwrap();
    println!("{:?}", create::create_indices_from_str(&mut pers, &data, "[]", None, true));

    // env::set_var("LoadingType", "Disk");
    // persistence::Persistence::load(TEST_FOLDER.to_string()).expect("Could not load persistence")

    pers
}

fn main() {
    search_lib::trace::enable_log();
    let pers = load_persistence_disk();
    info_time!("wo");
    let _results = search_freestyle("taschenbuch", &pers);
    // println!("{:?}", results[0]);
}

fn search_freestyle(term: &str, pers: &persistence::Persistence) -> Vec<search::DocWithHit> {
    let yop = query_generator::SearchQueryGeneratorParameters {
        search_term: term.to_string(),
        ..Default::default()
    };
    let requesto = query_generator::search_query(pers, yop);
    let hits = search::search(requesto, pers).unwrap();
    search::to_documents(pers, &hits.data, &None, &hits)
}
