#![feature(test)]

// #[macro_use]
// extern crate serde_derive;

#[macro_use]
extern crate serde_json;

#[macro_use]
extern crate criterion;

use criterion::Criterion;
// use rand::distributions::Range;
use veloci::{doc_store::*, search::*, *};
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

    // fn search_testo_to_doc(req: Value) -> Result<Vec<search::DocWithHit>, VelociError> {
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
    let hits = search::search(requesto, pers).unwrap();
    search::to_documents(pers, &hits.data, &None, &hits)
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
    search_field::suggest(pers, &requesto).unwrap()
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
    search_field::highlight(pers, &mut requesto).unwrap()
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

fn searches(_c: &mut Criterion) {
    let _pers = load_persistence_disk();
    let _pers_im = load_persistence_im();

    // c.bench_function("jmdict_search_anschauen", |b| b.iter(|| search("anschauen", &pers, 1)));

    // c.bench_function("jmdict_search_haus", |b| b.iter(|| search("haus", &pers, 1)));

    // c.bench_function("jmdict_search_freestyle_haus", |b| b.iter(|| search_freestyle("haus", &pers)));

    // c.bench_function("jmdict_search_in_a_hurry", |b| b.iter(|| search_freestyle("in a hurry", &pers)));

    // c.bench_function("jmdict_search_japanese", |b| b.iter(|| search("家", &pers, 0)));

    // // let facets: Vec<FacetRequest> = vec![FacetRequest{field:"commonness".to_string(), .. Default::default()}];

    // let req = json!({
    //     "search": {
    //         "terms": ["the"],
    //         "path": "meanings.eng[]",
    //         "levenshtein_distance":0
    //     },
    //     "top": 10,
    //     "skip": 0,
    //     "facets": [ {"field":"commonness"}]
    // });

    // let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    // c.bench_function("jmdict_search_facets", |b| b.iter(|| search::search(requesto.clone(), &pers)));

    // c.bench_function("jmdict_search_facets_im", |b| b.iter(|| search::search(requesto.clone(), &pers_im)));

    // c.bench_function("jmdict_suggest_an", |b| b.iter(|| suggest("an", "meanings.ger[].text", &pers)));

    // let mut rng = rand::thread_rng();
    // let between = Range::new(0, 166600);
    // let fields = pers.get_all_fields();
    // let tree = get_read_tree_from_fields(&pers, &fields);
    // let single_tree = get_read_tree_from_fields(&pers, &vec!["ent_seq".to_string()]);

    // c.bench_function("load_documents_direct_large", |b| b.iter(|| DocLoader::get_doc(&pers, 166600 as usize)));

    // c.bench_function("load_documents_tree_large", |b| b.iter(|| search::read_tree(&pers, 166600, &tree)));

    // c.bench_function("load_documents_direct_random", |b| {
    //     b.iter(|| DocLoader::get_doc(&pers, between.ind_sample(&mut rng) as u32 as usize))
    // });

    // c.bench_function("load_documents_cache:tree_random", |b| {
    //     b.iter(|| search::read_tree(&pers, between.ind_sample(&mut rng) as u32, &tree))
    // });

    // c.bench_function("load_documents_new_tree_random", |b| {
    //     b.iter(|| {
    //         let fields = pers.get_all_fields();
    //         let tree = get_read_tree_from_fields(&pers, &fields);
    //         search::read_tree(&pers, between.ind_sample(&mut rng) as u32, &tree)
    //     })
    // });

    // c.bench_function("load_documents_tree_random_single_field", |b| {
    //     b.iter(|| search::read_tree(&pers, between.ind_sample(&mut rng) as u32, &single_tree))
    // });

    // c.bench_function("jmdict_suggest_a", |b| b.iter(|| suggest("a", "meanings.ger[].text", &pers)));

    // c.bench_function("jmdict_suggest_kana_a", |b| b.iter(|| suggest("あ", "kana[].text", &pers)));

    // c.bench_function("jmdict_suggest_kana_a", |b| b.iter(|| suggest("あ", "kana[].text", &pers)));
}

criterion_group!(benches, searches);
criterion_main!(benches);

// fn main() {
//     unimplemented!();
// }
