#![feature(collection_placement)]
#![feature(placement_in_syntax)]
#![feature(test)]

// #[macro_use]
// extern crate serde_derive;
#[macro_use]
extern crate log;

extern crate flexi_logger;

#[macro_use]
extern crate serde_json;

extern crate rand;
extern crate veloci;
extern crate serde;
extern crate test;

#[macro_use]
extern crate criterion;

use criterion::Criterion;
use veloci::*;
static TEST_FOLDER: &str = "bench_taschenbuch";


fn load_persistence_disk() -> persistence::Persistence {

    use std::path::Path;
    if Path::new(TEST_FOLDER).exists(){
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

fn search_freestyle(term: &str, pers: &persistence::Persistence) -> Vec<search::DocWithHit> {
    let yop = query_generator::SearchQueryGeneratorParameters {
        search_term: term.to_string(),
        ..Default::default()
    };
    let requesto = query_generator::search_query(pers, yop);
    let hits = search::search(requesto, pers).unwrap();
    search::to_documents(pers, &hits.data, None, &hits)
}

fn searches(c: &mut Criterion) {
    // veloci::trace::enable_log();
    let pers = load_persistence_disk();

    // c.bench_function("jmdict_search_anschauen", |b| b.iter(|| search("anschauen", &pers, 1)));

    // c.bench_function("jmdict_search_haus", |b| b.iter(|| search("haus", &pers, 1)));
    c.bench_function("jmdict_search_taschenbuch", move |b| b.iter(|| search_freestyle("taschenbuch", &pers)));

}

criterion_group!(benches, searches);
criterion_main!(benches);

