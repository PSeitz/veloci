#[cfg(test)]
mod test {
    use super::super::rocket;
    use rocket::local::Client;

    use search_lib::*;
    use rocket::{
        http::{ContentType, Status},
    };

    #[macro_export]
    macro_rules! assert_contains {
        ($left:expr, $right:expr) => {{
            let (left, right) = (&($left), &($right));
            if !(left.contains(right)) {
                panic!("assertion failed: `(left does not contain right)`\n  left: `{:?}`,\n right: `{:?}`", left, right);
            }
        }};
    }


    static TEST_DATA: &str=r#"{"text": "hi there", "name": "fred", "boost": "me"}"#;

    fn create_db() {

        use std::sync::Once;

        static START: Once = Once::new();

        const CONFIG: &str = r#"
        ["*GLOBAL*"]
            features = ["All"]
        "#;
        START.call_once(|| {
             let mut pers = persistence::Persistence::create_type("test_rocket".to_string(), persistence::PersistenceType::Persistent).unwrap();
            println!("{:?}", create::create_indices_from_str(&mut pers, TEST_DATA, CONFIG, true));
        });

    }

    #[test]
    fn get_version() {
        let client = Client::new(rocket()).expect("valid rocket instance");
        let response = client.get("/version").dispatch();
        assert_eq!(response.status(), Status::Ok);
    }

    #[test]
    fn get_request() {
        create_db();

        let client = Client::new(rocket()).expect("valid rocket instance");
        let mut response = client.get("/test_rocket/search?query=fred&top=10&boost_fields=name-%3E2.5&boost_terms=boost:me-%3E2.0").dispatch(); // -> == -%3E, url escaping is needed here for some reason
        assert_eq!(response.status(), Status::Ok);
        assert_contains!(response.body_string().unwrap(), "name");
    }

    #[test]
    fn get_suggest() {
        create_db();
        let client = Client::new(rocket()).expect("valid rocket instance");

        let mut response = client.get("/test_rocket/suggest?query=fr&top=10").dispatch();
        assert_eq!(response.status(), Status::Ok);
        assert_contains!(response.body_string().unwrap(), "fred");
    }

    #[test]
    fn post_request() {
        create_db();
        let client = Client::new(rocket()).expect("valid rocket instance");
        let mut response = client.post("/test_rocket/search_query_params")
            .body(r#"{
"search_term": "fred",
    "top": 3,
    "skip": 0,
    "select":"name",
    "boost_fields": {"name":2.50},
    "boost_terms": {"boost:me":2.0},
    "why_found": true
}"#)
            .header(ContentType::JSON)
            .dispatch();
        assert_eq!(response.status(), Status::Ok);
        assert_contains!(response.body_string().unwrap(), "name");
    }

        #[test]
    fn post_request_invalid() {
        create_db();
        let client = Client::new(rocket()).expect("valid rocket instance");
        let mut response = client.post("/test_rocket/search_query_params")
            .body(r#"{
"search_term": "fred",
    "top": 3,
    "skip": 0,
    "select":"name",
    "fields": ["invalid"],
    "boost_terms": {"boost:me":2.0},
    "why_found": true
}"#)
            .header(ContentType::JSON)
            .dispatch();
        assert_eq!(response.status(), Status::BadRequest);
        assert_contains!(response.body_string().unwrap(), "Did not find any fields for");
    }

    #[test]
    fn post_request_explain_plan() {
        create_db();
        let client = Client::new(rocket()).expect("valid rocket instance");
        let mut response = client.post("/test_rocket/search_query_params/explain_plan")
            .body(r#"{
"search_term": "fred",
    "top": 3,
    "skip": 0,
    "why_found": true
}"#)
            .header(ContentType::JSON)
            .dispatch();
        assert_eq!(response.status(), Status::Ok);
        let resp = response.body_string().unwrap();
        assert_contains!(resp, "name");
        assert_contains!(resp, "fred");
        assert_contains!(resp, "search");
    }

    #[test]
    fn post_suggest() {
        create_db();
        let client = Client::new(rocket()).expect("valid rocket instance");
        let mut response = client.post("/test_rocket/suggest")
            .body(r#"{
                "suggest":[
                {
                  "terms": ["fre"],
                  "path": "name",
                  "starts_with": true
                }
                ]
            }"#)
            .header(ContentType::JSON)
            .dispatch();
        assert_eq!(response.status(), Status::Ok);
        assert_contains!(response.body_string().unwrap(), "fred");
    }

    #[test]
    fn get_doc_id() {
        create_db();
        let client = Client::new(rocket()).expect("valid rocket instance");
        let mut response = client.get("/test_rocket/_id/0")
            .dispatch();
        assert_eq!(response.status(), Status::Ok);
        let ret = response.body_string().unwrap();
        assert_contains!(ret, r#""name":"fred""#);
        assert_contains!(ret, r#""text":"hi there""#);
    }
    #[test]
    fn get_doc_id_tree() {
        create_db();
        let client = Client::new(rocket()).expect("valid rocket instance");
        let mut response = client.get("/test_rocket/_idtree/0")
            .dispatch();
        assert_eq!(response.status(), Status::Ok);
        let ret = response.body_string().unwrap();
        assert_contains!(ret, r#""name":"fred""#);
        assert_contains!(ret, r#""text":"hi there""#);
    }
    #[test]
    fn test_inspect_data() {
        create_db();
        let client = Client::new(rocket()).expect("valid rocket instance");
        let mut response = client.get("/test_rocket/inspect/boost.textindex.parent_to_value_id/0")
            .dispatch();
        assert_eq!(response.status(), Status::Ok);
        let ret = response.body_string().unwrap();
        assert_contains!(ret, r#"[0]"#);
    }
}