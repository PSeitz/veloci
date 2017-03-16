
#[cfg(test)]
mod tests {

    static TEST_DATA:&str = r#"[
        {
            "commonness": 20,
            "kanji": [
                { "text": "偉容", "commonness": 0},
                { "text": "威容","commonness": 5}
            ],
            "kana": [
                {
                    "text": "いよう",
                    "romaji": "Iyou",
                    "commonness": 5
                }
            ],
            "meanings": {
                "eng" : ["dignity", "majestic appearance", "will test"],
                "ger": ["majestätischer Anblick (m)", "majestätisches Aussehen (n)", "Majestät (f)"]
            },
            "ent_seq": "1587680"
        },
        {
            "commonness": 20,
            "kanji": [
                { "text": "意欲", "commonness": 40},
                { "text": "意慾", "commonness": 0}
            ],
            "kana": [
                {
                    "text": "いよく",
                    "romaji": "Iyoku",
                    "commonness": 40
                }
            ],
            "meanings": {
                "eng" : ["will", "desire", "urge", "having a long torso"],
                "ger": ["Wollen (n)", "Wille (m)", "Begeisterung (f)"]
            },
            "ent_seq": "1587690"
        },
        {
            "commonness": 500,
            "kanji": [
                { "text": "意慾", "commonness": 20}
            ],
            "field1" : [{"text":"awesome", "rank":1}],
            "kana": [
                {
                    "text": "いよく"
                }
            ],
            "meanings": {
                "eng" : ["test1"],
                "ger": ["der test"]
            },
            "ent_seq": "1587700"
        },
        {
            "commonness": 551,
            "kanji": [
                {
                    "text": "何の",
                    "commonness": 526
                }
            ],
            "field1" : [{"text":"awesome"}, {"text":"nixhit"}],
            "kana": [
                {
                    "text": "どの",
                    "romaji": "Dono",
                    "commonness": 25
                }
            ],
            "meanings": {
                "ger": [
                    "welch"
                ]
            },
            "ent_seq": "1920240"
        },
        {
            "pos": [
                "adj-i"
            ],
            "commonness": 1,
            "misc": [],
            "kanji": [
                {
                    "text": "柔らかい",
                    "commonness": 57
                }
            ],
            "kana": [
                {
                    "text": "やわらかい",
                    "romaji": "Yawarakai",
                    "commonness": 30
                }
            ],
            "meanings": {
                "ger": [
                    "(1) weich",
                    "stopword"
                ]
            },
            "ent_seq": "1605630"
        }
    ]"#;

    use util;
    use util::normalize_text;
    use create;
    use serde_json;

    // #[test]
    // fn it_super_duper_works() {
    //     assert_eq!(normalize_text("Hello"), "Hello");
    // }

    // #[test]
    // fn creates_da_indexo() {
    //     assert_eq!(normalize_text("Hello"), "Hello");
    // }

     #[test]
    fn test_write_index() {
        let ele:Vec<u32> = vec![3, 3, 3, 7];
        println!("{:?}", util::write_index(&ele, "testbug"));
        let ele2 = util::load_index("testbug").unwrap();
        println!("{:?}", ele2);
        assert_eq!(ele, ele2);
    }

     #[test]
    fn test_write_index_64() {
        let ele:Vec<u64> = vec![3_000_000_000_000, 3, 3, 7];
        println!("{:?}", util::write_index64(&ele, "test64"));
        let ele2 = util::load_index64("test64").unwrap();
        println!("{:?}", ele2);
        assert_eq!(ele, ele2);
    }

    #[test]
    fn checked_was_abgeht() {
        
        let ele = vec![3, 3, 3, 7];
        println!("{:?}", util::write_index(&ele, "testbug2"));

        // let opt: create::FulltextIndexOptions = serde_json::from_str(r#"{"tokenize":true, "stopwords": []}"#).unwrap();
        // let opt = create::FulltextIndexOptions{
        //     tokenize: true,
        //     stopwords: vec![]
        // };
        // assert_eq!(normalize_text("Hello"), "Hello");
        // let dat2 = r#" [{ "name": "John Doe", "age": 43 }, { "name": "Jaa", "age": 43 }] "#;
        // let data: Value = serde_json::from_str(dat2).unwrap();
        // let res = create::create_fulltext_index(&data, "name", opt);
        // println!("{:?}", res);
        // let deserialized: create::BoostIndexOptions = serde_json::from_str(r#"{"boost_type":"int"}"#).unwrap();
        // println!("{:?}", deserialized);

        let service: create::CreateIndex = serde_json::from_str(r#"{"boost":"kanji[].commonness", "options":{"boost_type":"int"}}"#).unwrap();
        println!("service: {:?}", service);

        let indices = r#"
        [
            { "boost":"commonness" , "options":{"boost_type":"int"}}
        ]
        "#;

        println!("{:?}", create::create_indices("mochaTest", TEST_DATA, indices));

        assert_eq!(normalize_text("Hello"), "Hello");
    }


}




// #[cfg(test)]
// mod test {
//     use create;
//     use serde_json;
//     use serde_json::Value;

//     #[test]
//     fn test_ewwwwwwwq() {

//         let opt: create::FulltextIndexOptions = serde_json::from_str(r#"{"tokenize":true, "stopwords": []}"#).unwrap();
//         // let opt = create::FulltextIndexOptions{
//         //     tokenize: true,
//         //     stopwords: vec![]
//         // };

//         let dat2 = r#" [{ "name": "John Doe", "age": 43 }, { "name": "Jaa", "age": 43 }] "#;
//         let data: Value = serde_json::from_str(dat2).unwrap();
//         let res = create::create_fulltext_index(&data, "name", opt);
//         println!("{:?}", res);
//         let deserialized: create::BoostIndexOptions = serde_json::from_str(r#"{"boost_type":"int"}"#).unwrap();

//         assert_eq!("Hello", "Hello");

//         let service: create::CreateIndex = serde_json::from_str(r#"{"boost_type":"int"}"#).unwrap();
//         println!("service: {:?}", service);

//     }
// }
