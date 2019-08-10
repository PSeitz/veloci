#[cfg(test)]
mod test {
    use super::super::rocket;
    use rocket::local::Client;
    use rocket::http::Status;

    static test_data: &str=r#"{"text": "hi there", "name": "fred"}"#;

    #[test]
    fn create_db() {
        let client = Client::new(rocket()).expect("valid rocket instance");
         client.post("/test")
            .body("field=value&otherField=123")
            .header(ContentType::Form);
        assert_eq!(response.status(), Status::Ok);
        // assert_eq!(response.body_string(), Some("Hello, world!".into()));
    }

    #[test]
    fn hello_world() {
        let client = Client::new(rocket()).expect("valid rocket instance");
        let mut response = client.get("/version").dispatch();
        assert_eq!(response.status(), Status::Ok);
        // assert_eq!(response.body_string(), Some("Hello, world!".into()));
    }
}