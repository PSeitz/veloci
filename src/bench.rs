#[cfg(test)]
mod bench {
    extern crate env_logger;

    use persistence;
    use search;
    use search_field;
    use serde_json;

    use test::Bencher;

    static TEST_FOLDER: &str = "jmdict";

    fn get_request(term: &str) -> search::Request {
        let query = json!({
            "or": [
                {
                    "search": {
                        "terms": vec![term.to_string()],
                        "path": "kanji[].text",
                        "levenshtein_distance": 0,
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
                        "levenshtein_distance": 0,
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
                        "levenshtein_distance": 0,
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
                        "levenshtein_distance": 1
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
                        "levenshtein_distance": 1
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


    fn search(term: &str, pers: &persistence::Persistence) -> Vec<search::DocWithHit> {
        let requesto = get_request(term);
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

    // fn search_testo_to_doc(req: Value) -> Result<Vec<search::DocWithHit>, search::SearchError> {
    //     let persistences = PERSISTENCES.read().unwrap();
    //     let pers = persistences.get(&"default".to_string()).unwrap();
    //     let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    //     let hits = search::search(requesto, pers)?;
    //     Ok(search::to_documents(pers, &hits.data))
    // }

    fn load_persistence() -> persistence::Persistence{
        persistence::Persistence::load(TEST_FOLDER.to_string()).expect("Could not load persistence")
    }

    #[bench]
    fn search_anschauen(b: &mut Bencher) {
        let pers = load_persistence();

        b.iter(|| { search("anschauen", &pers) });
    }

    #[bench]
    fn search_haus(b: &mut Bencher) {
        let pers = load_persistence();

        b.iter(|| { search("haus", &pers) });
    }

    #[bench]
    fn search_japanese(b: &mut Bencher) {
        let pers = load_persistence();

        b.iter(|| { search("家", &pers) });
    }

    #[bench]
    fn suggest_an(b: &mut Bencher) {
        let pers = load_persistence();
        b.iter(||  { suggest("an", "meanings.ger[].text", &pers) });
    }

    #[bench]
    fn suggest_a(b: &mut Bencher) {
        let pers = load_persistence();
        b.iter(||  { suggest("a", "meanings.ger[].text", &pers) });
    }

    #[bench]
    fn suggest_kana_a(b: &mut Bencher) {
        let pers = load_persistence();
        b.iter(||  { suggest("あ", "kana[].text", &pers) });
    }


}

