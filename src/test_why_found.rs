#[cfg(test)]
mod tests {
    extern crate env_logger;

    use persistence;
    use create;
    #[allow(unused_imports)]
    use search_field;
    use search;
    #[allow(unused_imports)]
    use serde_json;
    #[allow(unused_imports)]
    use serde_json::Value;
    use trace;
    use parking_lot::RwLock;
    use chashmap::CHashMap;

    pub fn get_test_data() -> String {
        json!([
            {
                "richtig": "schön super",
                "viele": ["nette", "leute"]
            },
            {
                "richtig": "hajoe genau"
            },
            {
                "richtig": "shön",
                "viele": ["treffers", "und so", "super treffers"] //same text "super treffers" multiple times
            },
            {
                "buch": "Taschenbuch (kartoniert)",
                "viele": ["super treffers"] //same text "super treffers" multiple times
            }
        ]).to_string()
    }

    static TEST_FOLDER: &str = "mochaTest_wf";
    static INDEX_CREATED: RwLock<bool> = RwLock::new(false);
    lazy_static! {
        static ref PERSISTENCES: CHashMap<String, persistence::Persistence> = {
            CHashMap::default()
        };
    }

    fn search_testo_to_doc(req: Value) -> search::SearchResultWithDoc {
        let pers = PERSISTENCES.get(&"default".to_string()).expect("Can't find loaded persistence");
        search::to_search_result(&pers, search_testo_to_hitso(req).expect("search error"), None)
    }

    fn search_testo_to_hitso(req: Value) -> Result<search::SearchResult, search::SearchError> {
        let pers = PERSISTENCES.get(&"default".to_string()).expect("Can't find loaded persistence");
        let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
        let hits = search::search(requesto, &pers)?;
        Ok(hits)
    }

    describe! search_test {
        before_each {

            let mut INDEX_CREATEDO = INDEX_CREATED.write();
            {

                if !*INDEX_CREATEDO {
                    trace::enable_log();
                    let indices = r#"[{ "fulltext":"richtig", "options":{"tokenize":true} } ] "#;
                    println!("{:?}", create::create_indices(TEST_FOLDER, &get_test_data(), indices));

                    {
                        let mut pers = persistence::Persistence::load(TEST_FOLDER.to_string()).expect("Could not load persistence");
                    }
                    PERSISTENCES.insert("default".to_string(), persistence::Persistence::load(TEST_FOLDER.to_string()).expect("could not load persistence"));

                    *INDEX_CREATEDO = true;
                }
            }
        }


        it "should add why found terms highlight tokens and also text_ids"{
            let req = json!({
                "search": {
                    "terms":["schön"],
                    "path": "richtig",
                    "levenshtein_distance": 1
                },
                "why_found":true
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits[0].why_found["richtig"], vec!["<b>schön</b> super"]);
            assert_eq!(hits[1].why_found["richtig"], vec!["<b>shön</b>"]);
        }

        it "should add why found from 1:n terms, highlight tokens and also text_ids"{
            let req = json!({
                "search": {
                    "terms":["treffers"],
                    "path": "viele[]",
                    "levenshtein_distance": 1
                },
                "why_found":true
            });

            let hits = search_testo_to_doc(req).data;
            println!("{:?}", hits);
            assert_eq!(hits[0].why_found["viele[]"], vec!["<b>treffers</b>", "super <b>treffers</b>"]);
        }

        it "should add highlight taschenbuch"{
            let req = json!({
                "search": {
                    "terms":["Taschenbuch"],
                    "path": "buch",
                    "levenshtein_distance": 1
                },
                "why_found":true
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits[0].why_found["buch"], vec!["<b>Taschenbuch</b> (kartoniert)"]);
        }

    }


}
