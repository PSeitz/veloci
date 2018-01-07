#![feature(test)]

// #[macro_use]
// extern crate serde_derive;

#[macro_use]
extern crate serde_json;

extern crate rand;
extern crate serde;
extern crate search_lib;
extern crate test;

#[cfg(test)]
mod bench_jmdict {
    extern crate env_logger;
    extern crate rand;
    
    
    // use search_lib::*;
    use search_lib::persistence;
    use search_lib::search;
    use search_lib::search_field;
    use serde_json;

    use test::Bencher;

    static TEST_FOLDER: &str = "jmdict";

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

    // fn search_testo_to_doc(req: Value) -> Result<Vec<search::DocWithHit>, search::SearchError> {
    //     let persistences = PERSISTENCES.read().unwrap();
    //     let pers = persistences.get(&"default".to_string()).unwrap();
    //     let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    //     let hits = search::search(requesto, pers)?;
    //     Ok(search::to_documents(pers, &hits.data))
    // }

    fn load_persistence() -> persistence::Persistence {
        persistence::Persistence::load(TEST_FOLDER.to_string()).expect("Could not load persistence")
    }

    fn load_gutenberg_persistence() -> persistence::Persistence {
        persistence::Persistence::load("gutenberg".to_string()).expect("Could not load persistence")
    }

    #[bench]
    fn search_anschauen(b: &mut Bencher) {
        let pers = load_persistence();

        b.iter(|| search("anschauen", &pers, 1));
    }

    #[bench]
    fn search_haus(b: &mut Bencher) {
        let pers = load_persistence();

        b.iter(|| search("haus", &pers, 1));
    }

    #[bench]
    fn search_japanese(b: &mut Bencher) {
        let pers = load_persistence();

        b.iter(|| search("家", &pers, 0));
    }

    #[bench]
    fn suggest_an(b: &mut Bencher) {
        let pers = load_persistence();
        b.iter(|| suggest("an", "meanings.ger[].text", &pers));
    }

    #[bench]
    fn suggest_a(b: &mut Bencher) {
        let pers = load_persistence();
        b.iter(|| suggest("a", "meanings.ger[].text", &pers));
    }

    #[bench]
    fn suggest_kana_a(b: &mut Bencher) {
        let pers = load_persistence();
        b.iter(|| suggest("あ", "kana[].text", &pers));
    }


    #[bench]
    fn highlight_in_book(b: &mut Bencher) {
        let pers = load_gutenberg_persistence();
        b.iter(|| highlight("pride", "content", &pers));
    }

    use rand::distributions::{IndependentSample, Range};

    #[bench]
    fn get_text_ids_fst(b: &mut Bencher) {
        let mut rng = rand::thread_rng();
        let between = Range::new(0, 7000);
        let pers = load_gutenberg_persistence();
        b.iter(|| search_field::get_text_for_id(&pers, "content.textindex", between.ind_sample(&mut rng) as u32 ));
    }

    #[bench]
    fn get_text_ids_fst_cache(b: &mut Bencher) {
        let mut rng = rand::thread_rng();
        let between = Range::new(0, 7000);
        let pers = load_gutenberg_persistence();
        let mut bytes = vec![];
        b.iter(|| search_field::get_text_for_id_2(&pers, "content.textindex", between.ind_sample(&mut rng) as u32,&mut bytes ));
    }

    #[bench]
    fn get_text_ids_disk(b: &mut Bencher) {
        let mut rng = rand::thread_rng();
        let between = Range::new(0, 7000);
        let pers = load_gutenberg_persistence();
        b.iter(|| search_field::get_text_for_id_disk(&pers, "content.textindex", between.ind_sample(&mut rng) as u32 ));
    }

    #[bench]
    fn get_text_ids_cache_fst_cache_bytes(b: &mut Bencher) {
        let mut rng = rand::thread_rng();
        let between = Range::new(0, 7000);
        let pers = load_gutenberg_persistence();

        let map = pers.cache.fst.get("content.textindex").unwrap();
        let mut bytes = vec![];
        b.iter(|| search_field::ord_to_term(map.as_fst(), between.ind_sample(&mut rng) as u64, &mut bytes));
    }

    // #[test]
    // fn highlight_in_book_yeah() {
    //     let pers = load_gutenberg_persistence();
    //     assert_eq!(highlight("pride", "content", &pers)[0].0, "QUAARK");
    // }


}


fn main() {
    unimplemented!();
}