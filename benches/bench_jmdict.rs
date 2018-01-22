#![feature(test)]

// #[macro_use]
// extern crate serde_derive;

#[macro_use]
extern crate serde_json;

extern crate rand;
extern crate search_lib;
extern crate serde;
extern crate test;

#[macro_use]
extern crate criterion;

// use rand::distributions::{IndependentSample, Range};
use criterion::Criterion;
use search_lib::*;
use search_lib::search::*;
static TEST_FOLDER: &str = "jmdict";

use std::env;



fn load_persistence_im() -> persistence::Persistence {
    env::set_var("LoadingType", "InMemory");
    persistence::Persistence::load(TEST_FOLDER.to_string()).expect("Could not load persistence")
}

fn load_persistence_disk() -> persistence::Persistence {
    env::set_var("LoadingType", "Disk");
    persistence::Persistence::load(TEST_FOLDER.to_string()).expect("Could not load persistence")
}

// fn load_gutenberg_persistence() -> persistence::Persistence {
//     persistence::Persistence::load("gutenberg".to_string()).expect("Could not load persistence")
// }

#[cfg(test)]
mod bench_jmdict {
    extern crate env_logger;
    extern crate rand;

    // fn search_testo_to_doc(req: Value) -> Result<Vec<search::DocWithHit>, search::SearchError> {
    //     let persistences = PERSISTENCES.read().unwrap();
    //     let pers = persistences.get(&"default".to_string()).unwrap();
    //     let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    //     let hits = search::search(requesto, pers)?;
    //     Ok(search::to_documents(pers, &hits.data))
    // }

    #[bench]
    fn highlight_in_book(b: &mut Bencher) {
        let pers = load_gutenberg_persistence();
        b.iter(|| highlight("pride", "content", &pers));
    }


    // #[bench]
    // fn get_text_ids_fst(b: &mut Bencher) {
    //     let mut rng = rand::thread_rng();
    //     let between = Range::new(0, 7000);
    //     let pers = load_gutenberg_persistence();
    //     b.iter(|| {
    //         search_field::get_text_for_id(
    //             &pers,
    //             "content.textindex",
    //             between.ind_sample(&mut rng) as u32,
    //         )
    //     });
    // }

    // #[bench]
    // fn get_text_ids_fst_cache(b: &mut Bencher) {
    //     let mut rng = rand::thread_rng();
    //     let between = Range::new(0, 7000);
    //     let pers = load_gutenberg_persistence();
    //     let mut bytes = vec![];
    //     b.iter(|| {
    //         search_field::get_text_for_id_2(
    //             &pers,
    //             "content.textindex",
    //             between.ind_sample(&mut rng) as u32,
    //             &mut bytes,
    //         )
    //     });
    // }

    // #[bench]
    // fn get_text_ids_disk(b: &mut Bencher) {
    //     let mut rng = rand::thread_rng();
    //     let between = Range::new(0, 7000);
    //     let pers = load_gutenberg_persistence();
    //     b.iter(|| {
    //         search_field::get_text_for_id_disk(
    //             &pers,
    //             "content.textindex",
    //             between.ind_sample(&mut rng) as u32,
    //         )
    //     });
    // }

    // #[bench]
    // fn get_text_ids_cache_fst_cache_bytes(b: &mut Bencher) {
    //     let mut rng = rand::thread_rng();
    //     let between = Range::new(0, 7000);
    //     let pers = load_gutenberg_persistence();

    //     let map = pers.cache.fst.get("content.textindex").unwrap();
    //     let mut bytes = vec![];
    //     b.iter(|| {
    //         search_field::ord_to_term(
    //             map.as_fst(),
    //             between.ind_sample(&mut rng) as u64,
    //             &mut bytes,
    //         )
    //     });
    // }

    // #[test]
    // fn highlight_in_book_yeah() {
    //     let pers = load_gutenberg_persistence();
    //     assert_eq!(highlight("pride", "content", &pers)[0].0, "QUAARK");
    // }

}

    fn get_request(term: &str, levenshtein_distance: u32) -> search::Request {
        let query = json!({
            "or": [
                {
                    "search": {
                        "terms": vec![term.to_string()],
                        "path": "kanji[].text",
                        "levenshtein_distance": levenshtein_distance,
                        "starts_with": true
                    },
                    "boost": [
                        {
                            "path": "commonness",
                            "boost_fun": "Log10",
                            "param": 1
                        },
                        {
                            "path": "kanji[].commonness",
                            "boost_fun": "Log10",
                            "param": 1
                        }
                    ]
                },
                {
                    "search": {
                        "terms": vec![term.to_string()],
                        "path": "kana[].text",
                        "levenshtein_distance": levenshtein_distance,
                        "starts_with": true
                    },
                    "boost": [
                        {
                            "path": "commonness",
                            "boost_fun": "Log10",
                            "param": 1
                        },
                        {
                            "path": "kana[].commonness",
                            "boost_fun": "Log10",
                            "param": 1
                        }
                    ]
                },
                {
                    "search": {
                        "terms": vec![term.to_string()],
                        "path": "kana[].text",
                        "levenshtein_distance": levenshtein_distance,
                        "starts_with": true
                    },
                    "boost": [
                        {
                            "path": "commonness",
                            "boost_fun": "Log10",
                            "param": 1
                        },
                        {
                            "path": "kana[].commonness",
                            "boost_fun": "Log10",
                            "param": 1
                        }
                    ]
                },
                {
                    "search": {
                        "terms": vec![term.to_string()],
                        "path": "meanings.ger[].text",
                        "levenshtein_distance": levenshtein_distance
                    },
                    "boost": [
                        {
                            "path": "commonness",
                            "boost_fun": "Log10",
                            "param": 0
                        },
                        {
                            "path": "meanings.ger[].rank",
                            "expression": "10 / $SCORE"
                        }
                    ]
                },
                {
                    "search": {
                        "terms": vec![term.to_string()],
                        "path": "meanings.eng[]",
                        "levenshtein_distance": levenshtein_distance
                    },
                    "boost": [
                        {
                            "path": "commonness",
                            "boost_fun": "Log10",
                            "param": 1
                        }
                    ]
                }
            ],
            "top": 10,
            "skip": 0
        });

        let requesto: search::Request = serde_json::from_str(&query.to_string()).expect("Can't parse json");
        requesto
    }

    fn search(term: &str, pers: &persistence::Persistence, levenshtein_distance: u32) -> Vec<search::DocWithHit> {
        let requesto = get_request(term, levenshtein_distance);
        let hits = search::search(requesto, &pers).unwrap();
        search::to_documents(&pers, &hits.data)
    }
    fn search_with_facets(term: &str, pers: &persistence::Persistence, levenshtein_distance: u32, facets: Vec<FacetRequest>) -> Vec<search::DocWithHit> {
        let mut requesto = get_request(term, levenshtein_distance);
        requesto.facets = Some(facets);
        let hits = search::search(requesto, &pers).unwrap();
        search::to_documents(&pers, &hits.data)
    }

    fn suggest(term: &str, path: &str, pers: &persistence::Persistence) -> search_field::SuggestFieldResult {
        let req = json!({
            "terms":[term],
            "path": path,
            "levenshtein_distance": 0,
            "starts_with":true,
            "top":10,
            "skip":0
        });
        let requesto: search::RequestSearchPart = serde_json::from_str(&req.to_string()).expect("Can't parse json");
        search_field::suggest(&pers, &requesto).unwrap()
    }

    fn highlight(term: &str, path: &str, pers: &persistence::Persistence) -> search_field::SuggestFieldResult {
        let req = json!({
            "terms":[term],
            "path": path,
            "levenshtein_distance": 0,
            "starts_with":true,
            "snippet":true,
            "top":10,
            "skip":0
        });
        let mut requesto: search::RequestSearchPart = serde_json::from_str(&req.to_string()).expect("Can't parse json");
        search_field::highlight(&pers, &mut requesto).unwrap()
    }

// fn get_text_ids_cache_fst_cache_bytes(c: &mut Criterion) {
//     let mut rng = rand::thread_rng();
//     let between = Range::new(0, 7000);
//     let pers = load_gutenberg_persistence();

//     let map = pers.cache.fst.get("content.textindex").unwrap();
//     let mut bytes = vec![];
//     Criterion::default()
//         .bench_function("get_text_ids_cache_fst_cache_bytes", |b| b.iter(|| {
//             search_field::ord_to_term(
//                 map.as_fst(),
//                 4350 as u64,
//                 &mut bytes,
//             )
//         }));

//     let mut bytes = vec![];

//     Criterion::default()
//     .bench_function("get_text_ids_disk", |b| b.iter(|| {
//         search_field::get_text_for_id_2(
//             &pers,
//             "content.textindex",
//             4350 as u32,
//             &mut bytes,
//         )
//     }));

//     Criterion::default()
//     .bench_function("get_text_ids_fst_cache", |b| b.iter(|| {
//         search_field::get_text_for_id_2(
//             &pers,
//             "content.textindex",
//             4350 as u32,
//             &mut bytes,
//         )
//     }));

//     Criterion::default()
//     .bench_function("get_text_ids_fst", |b| b.iter(|| {
//         search_field::get_text_for_id(
//             &pers,
//             "content.textindex",
//             4350 as u32,
//         )
//     }));
// }


fn searches(c: &mut Criterion) {
    let pers = load_persistence_disk();
    let pers_im = load_persistence_im();

    c.bench_function("jmdict_search_anschauen", |b|
        b.iter(|| search("anschauen", &pers, 1))
    );

    c.bench_function("jmdict_search_haus", |b|
        b.iter(|| search("haus", &pers, 1))
    );

    c.bench_function("jmdict_search_japanese", |b|
        b.iter(|| search("家", &pers, 0))
    );

    // let facets: Vec<FacetRequest> = vec![FacetRequest{field:"commonness".to_string(), .. Default::default()}];

    let req = json!({
        "search": {
            "terms": ["the"],
            "path": "meanings.eng[]",
            "levenshtein_distance":0
        },
        "top": 10,
        "skip": 0,
        "facets": [ {"field":"commonness"}]
    });

    let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    c.bench_function("jmdict_search_with_facets", |b|
        b.iter(|| {
            // search_with_facets("the", &pers, 0, facets.clone())
            search::search(requesto.clone(), &pers)
        })
    );

    c.bench_function("jmdict_search_with_facets_im", |b|
        b.iter(|| {
            // search_with_facets("the", &pers, 0, facets.clone())
            search::search(requesto.clone(), &pers_im)
        })
    );

    c.bench_function("jmdict_suggest_an", |b|
        b.iter(|| suggest("an", "meanings.ger[].text", &pers))
    );

    c.bench_function("jmdict_suggest_a", |b|
        b.iter(|| suggest("a", "meanings.ger[].text", &pers))
    );

    c.bench_function("jmdict_suggest_kana_a", |b|
        b.iter(|| suggest("あ", "kana[].text", &pers))
    );
}

criterion_group!(benches, searches);
criterion_main!(benches);

// fn main() {
//     unimplemented!();
// }
