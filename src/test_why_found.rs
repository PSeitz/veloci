#[cfg(test)]
mod tests {
    extern crate env_logger;

    use chashmap::CHashMap;
    use create;
    use parking_lot::RwLock;
    use persistence;
    use search;
    use serde_json;
    use serde_json::Value;
    use trace;

    pub fn get_test_data() -> Value {
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
                "viele": ["treffers", "und so", "super treffers", "ein längerer Text, um zu checken, dass da nicht umsortiert wird"] //same text "super treffers" multiple times
            },
            {
                "buch": "Taschenbuch (kartoniert)",
                "viele": ["super treffers"] //same text "super treffers" multiple times
            }
        ])
    }

    static TEST_FOLDER: &str = "mochaTest_wf";
    static INDEX_CREATED: RwLock<bool> = RwLock::new(false);
    lazy_static! {
        static ref PERSISTENCES: CHashMap<String, persistence::Persistence> = { CHashMap::default() };
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
                    let mut persistence = persistence::Persistence::create(TEST_FOLDER.to_string()).unwrap();

                    let data = get_test_data();
                    if let Some(arr) = data.as_array() {
                        // arr.map(|el| el.to_string()+"\n").collect();
                        let docs_line_separated = arr.iter().fold(String::with_capacity(100), |acc, el| acc + &el.to_string()+"\n");
                        println!("{:?}", create::create_indices_from_str(&mut persistence, &docs_line_separated, indices, None, false));
                    }

                    let pers = persistence::Persistence::load(TEST_FOLDER.to_string()).expect("Could not load persistence");

                    PERSISTENCES.insert("default".to_string(), pers);

                    *INDEX_CREATEDO = true;
                }
            }
        }


        it "get_number_of_docs"{
            let pers = PERSISTENCES.get(&"default".to_string()).expect("Can't find loaded persistence");
            assert_eq!(pers.get_number_of_documents().unwrap(), 4);
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

        it "should add why found from 1:n terms - highlight tokens and also text_ids"{
            let req = json!({
                "search": {
                    "terms":["treffers"],
                    "path": "viele[]",
                    "levenshtein_distance": 1
                },
                "why_found":true
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits[0].why_found["viele[]"], vec!["<b>treffers</b>", "super <b>treffers</b>"]);
        }

        it "should add why found from 1:n terms - when select is used a different why_found strategy is used"{
            let req = json!({
                "search": {
                    "terms":["umsortiert"],
                    "path": "viele[]",
                    "levenshtein_distance": 0
                },
                "why_found":true,
                "select": ["richtig"]
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits[0].doc["richtig"], "shön");
            assert_eq!(hits[0].why_found["viele[]"], vec!["ein längerer Text, um zu checken, dass da nicht <b>umsortiert</b> wird"]);
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

        it "should add highlight multi terms"{
            let req = json!({
                "or":[
                {
                    "search": {
                        "terms":["Taschenbuch"],
                        "path": "buch",
                        "levenshtein_distance": 1
                    },
                    "why_found":true
                },{
                    "search": {
                        "terms":["kartoniert"],
                        "path": "buch",
                        "levenshtein_distance": 1
                    },
                    "why_found":true
                }],
                "why_found":true
            });

            let hits = search_testo_to_doc(req).data;
            assert_eq!(hits[0].why_found["buch"], vec!["<b>Taschenbuch</b> (<b>kartoniert</b>)"]);
        }

    }

}
