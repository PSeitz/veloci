use serde_json::Value;
use veloci::*;

use super::common;

pub fn get_test_data() -> Value {
    json!([
        {
            "id": 1,
            "order": 500,
            "title": "greg tagebuch 05"
        },
        {
            "id": 2,
            "order": 20,
            "title": "and some some text 05 this is not relevant let tagebuch greg"
        },
        {
            "id": 3,
            "order": 1000,
            "title": "greg tagebuch"
        },
        {
            "id": 4,
            "commonness": 41,
            "meanings": {
              "ger": [
                {
                  "text": "Fernsehen-Schauen (n)",
                  "boost" : 20
                }
              ]
            }
        },
        {
            "id": 5,
            "commonness": 551,
            "meanings": {"ger": ["welch"] }
        },
        {
            "id": 6,
            "commonness": 2,
            "meanings": {"ger": ["weich"] }
        }
    ])
}

static TEST_FOLDER: &str = "test_score";

lazy_static! {
    static ref TEST_PERSISTENCE: persistence::Persistence = {
        let indices = r#"
        [title.fulltext]
        tokenize = true
        ["meanings.ger[].boost".boost]
        boost_type = 'f32'
        ["meanings.ger[].text".fulltext]
        tokenize = true
        [commonness.boost]
        boost_type = 'f32'
        [order.boost]
        boost_type = 'f32'
        "#;
        common::create_test_persistence(TEST_FOLDER, indices, get_test_data().to_string().as_bytes(), None)
    };
}

#[test]
fn test_boost_simple() {
    let dir = "test_boost_simple";
    let test_data = r#"
{ "commonness": 10, "name": "product" }
{ "commonness": 99, "name": "product" }
{ "commonness": 33, "name": "product" }
    "#;
    let indices = r#"
        [name]
        tokenize = true
        [commonness.boost]
        boost_type = 'f32'
        "#;
    let pers: persistence::Persistence = common::create_test_persistence(dir, indices, test_data.as_bytes(), None);

    let req = json!({
        "search_req": { "search": {
            "terms":["product"],
            "path": "name",
            "levenshtein_distance": 0,
            "firstCharExactMatch":true
        }},
        "boost" : [{
            "path":"commonness",
            "boost_fun": "Log10",
            "param": 1
        }]
    });

    let requesto: search::Request = serde_json::from_str(&req.to_string()).expect("Can't parse json");
    let hits = search::to_search_result(&pers, search::search(requesto.clone(), &pers).expect("search error"), &requesto.select).data;

    assert_eq!(hits[0].doc["commonness"], 99);
    assert_eq!(hits[1].doc["commonness"], 33);
    assert_eq!(hits[2].doc["commonness"], 10);
}

#[test]
fn check_score_regarding_to_length() {
    let req_with_single_phrase = json!({
        "search_req": { "or":  { "queries": [
            {"search": {"terms":["greg"], "path": "title" }},
            {"search": {"terms":["tagebuch"], "path": "title" }},
            {"search": {"terms":["05"], "path": "title" }}
        ]}},
        "phrase_boosts": [{
            "path":"title",
            "search1":{"terms":["greg"], "path": "title" },
            "search2":{"terms":["tagebuch"], "path": "title" }
        }]
    });

    let res = search_testo_to_doc!(req_with_single_phrase);
    assert_eq!(res.data[0].doc["title"], "greg tagebuch 05"); //hits 3 tokens and phrases
    assert_eq!(res.data[1].doc["title"], "greg tagebuch"); //hits 2 tokens and phrases
    assert_eq!(res.data[2].doc["title"], "and some some text 05 this is not relevant let tagebuch greg");
    //hits 3 tokens but no phrases
}

// #[test]
// fn check_score_sub_token() {
//     let params = serde_json:: json!({
//     "search_term": "schauen",
//         "top": 3,
//         "skip": 0,
//         "explain": true,
//         "why_found": true,
//         "boost_queries": [
//           {
//             "path": "commonness",
//             "boost_fun": "Log10",
//             "param": 10
//           }
//         ],
//         "boost_fields": {
//             "meanings.ger[].text": 2.0
//         }
//     });

//     let params = serde_json::from_value(params).unwrap();
//     let res = search_testo_to_doco_qp!(params).data;
//     println!("{:?}", res);
//     assert_eq!(res[0].hit.score, 10.0); //hits 3 tokens and phrases
// //     assert_eq!(res.data[1].doc["title"], "greg tagebuch"); //hits 2 tokens and phrases
// //     assert_eq!(res.data[2].doc["title"], "and some some text 05 this is not relevant let tagebuch greg"); //hits 3 tokens but no phrases
// }

#[test]
fn check_score_boost_relative_field() {
    let params = serde_json:: json!({
        "search_term": "schauen",
        "fields": ["meanings.ger[].text"],
        "top": 3,
        "skip": 0,
        "why_found": true,
        "boost_queries": [
          {
            "path": "meanings.ger[].boost",
            "boost_fun": "Log10",
            "param": 10
          }
        ],
        "boost_fields": {
            "meanings.ger[].text": 2.0
        }
    });

    let params = serde_json::from_value(params).unwrap();
    let res = search_testo_to_doco_qp!(params).data;
    // println!("{:?}", res);
    assert_gt!(res[0].hit.score, 40.0);
    // assert_eq!(res[0].hit.score, 10.0); //hits 3 tokens and phrases
    //     assert_eq!(res.data[1].doc["title"], "greg tagebuch"); //hits 2 tokens and phrases
    //     assert_eq!(res.data[2].doc["title"], "and some some text 05 this is not relevant let tagebuch greg"); //hits 3 tokens but no phrases
}

#[test]
fn check_score_boost_add_value_from_field() {
    let params = serde_json::from_value(json!({
        "search_term": "weich",
        "fields": ["meanings.ger[]"],
        "levenshtein": 0,
        "boost_queries": [
          {
            "path": "commonness",
            "boost_fun": "Add"
          }
        ]
    }))
    .unwrap();
    let res_boosted = search_testo_to_doco_qp!(params).data;

    let params = serde_json::from_value(json!({
        "search_term": "weich",
        "levenshtein": 0,
        "fields": ["meanings.ger[]"]
    }))
    .unwrap();
    let res_unboosted = search_testo_to_doco_qp!(params).data;

    assert_eq!(res_unboosted[0].hit.score + 2.0, res_boosted[0].hit.score);
}

#[test]
fn check_score_boost_multiply_value_from_field() {
    let params = serde_json::from_value(json!({
        "search_term": "weich",
        "fields": ["meanings.ger[]"],
        "levenshtein": 0,
        "boost_queries": [
          {
            "path": "commonness",
            "boost_fun": "Multiply"
          }
        ]
    }))
    .unwrap();
    let res_boosted = search_testo_to_doco_qp!(params).data;

    let params = serde_json::from_value(json!({
        "search_term": "weich",
        "levenshtein": 0,
        "fields": ["meanings.ger[]"]
    }))
    .unwrap();
    let res_unboosted = search_testo_to_doco_qp!(params).data;

    assert_eq!(res_unboosted[0].hit.score * 2.0, res_boosted[0].hit.score);
}

#[test]
fn should_rank_exact_matches_pretty_good() {
    // TODO test with exact TOKEN score like: (1)weich
    let req = json!({
        "search_req": { "search": {
            "terms":["weich"], // hits welche and weich, welche has more boost
            "path": "meanings.ger[]",
            "levenshtein_distance": 1,
            "explain": true,
            "firstCharExactMatch":true
        }},
        "boost" : [{
            "path":"commonness",
            "boost_fun": "Log2",
            "param": 2
        }]
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].doc["meanings"]["ger"][0], "weich");
}

#[test]
fn test_order_by_field_via_replace_boost() {
    let req = json!({
        "search_req": { "search": {
            "terms":[".*"], // hit all
            "path": "title",
            "is_regex": true
        }},
        "boost" : [{
            "path":"order",
            "boost_fun": "Replace"
        }]
    });

    let hits = search_testo_to_doc!(req).data;
    assert_eq!(hits[0].doc["id"], 3);
    assert_eq!(hits[1].doc["id"], 1);
    assert_eq!(hits[2].doc["id"], 2);
}

// #[test]
// fn check_score_boost() {
//     let params = serde_json:: json!({
//     "search_term": "schauen",
//         "top": 3,
//         "skip": 0,
//         "explain": true,
//         "why_found": true,
//         "boost_queries": [
//           {
//             "path": "commonness",
//             "boost_fun": "Log10",
//             "param": 10
//           },
//           {
//             "path": "meanings.ger[].boost",
//             "boost_fun": "Log10",
//             "param": 10
//           }
//         ],
//         "boost_fields": {
//             "meanings.ger[].text": 2.0
//         }
//     });

//     let params = serde_json::from_value(params).unwrap();
//     let res = search_testo_to_doco_qp!(params).data;
//     println!("{:?}", res);
//     // assert_eq!(res[0].hit.score, 10.0); //hits 3 tokens and phrases
// //     assert_eq!(res.data[1].doc["title"], "greg tagebuch"); //hits 2 tokens and phrases
// //     assert_eq!(res.data[2].doc["title"], "and some some text 05 this is not relevant let tagebuch greg"); //hits 3 tokens but no phrases
// }
