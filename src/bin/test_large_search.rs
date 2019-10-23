#![recursion_limit = "128"]

// extern crate fst_levenshtein;
// extern crate cpuprofiler;
#[macro_use]
extern crate measure_time;

use veloci;

#[allow(unused_imports)]
#[macro_use]
extern crate serde_json;

#[allow(unused_imports)]
use fst::{IntoStreamer, MapBuilder, Set};
// use fst_levenshtein::Levenshtein;
// use serde_json::{Deserializer, Value};
use std::str;
use veloci::*;

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
    println!("{:?}", create::create_indices_from_str(&mut pers, &data, "{}", true));

    // env::set_var("LoadingType", "Disk");
    // persistence::Persistence::load(TEST_FOLDER.to_string()).expect("Could not load persistence")

    pers
}

fn main() {
    veloci::trace::enable_log();
    let pers = load_persistence_disk();
    info_time!("wo");
    let _results = search_freestyle("taschenbuch", &pers);
}

fn search_freestyle(term: &str, pers: &persistence::Persistence) -> Vec<search::DocWithHit> {
    let yop = query_generator::SearchQueryGeneratorParameters {
        search_term: term.to_string(),
        ..Default::default()
    };
    let requesto = query_generator::search_query(pers, yop).unwrap();
    let hits = search::search(requesto, pers).unwrap();
    search::to_documents(pers, &hits.data, &None, &hits)
}
