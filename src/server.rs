use search;
use search_field;
// use create;
// use doc_loader;
// use persistence;
use persistence::Persistence;
use iron::prelude::*;
use iron::{BeforeMiddleware, AfterMiddleware, typemap};
use time::precise_time_ns;
use router::Router;
use bodyparser;
use serde_json;

use iron::{headers, status};
use iron::modifiers::Header;
use persistence;

#[allow(unused_imports)]
use std::collections::HashMap;
#[allow(unused_imports)]
use fnv::FnvHashMap;
use std::sync::RwLock;

struct ResponseTime;

impl typemap::Key for ResponseTime { type Value = u64; }

impl BeforeMiddleware for ResponseTime {
    fn before(&self, req: &mut Request) -> IronResult<()> {
        req.extensions.insert::<ResponseTime>(precise_time_ns());
        Ok(())
    }
}

impl AfterMiddleware for ResponseTime {
    fn after(&self, req: &mut Request, res: Response) -> IronResult<Response> {
        let delta = precise_time_ns() - *req.extensions.get::<ResponseTime>().unwrap();
        println!("Request took: {} ms", (delta as f64) / 1000000.0);
        Ok(res)
    }
}

// fn hello_world(_: &mut Request) -> IronResult<Response> {
//     Ok(Response::with((iron::status::Ok, "Hello World")))
// }
// const MAX_BODY_LENGTH: usize = 1024 * 1024 * 10;

lazy_static! {
    // static ref CSV_PERSISTENCE: Persistence = {
    //     persistence::Persistence::load("csv_test".to_string()).expect("could not load persistence")
    // };
    // static ref JMDICT_PERSISTENCE: Persistence = {
    //     persistence::Persistence::load("jmdict".to_string()).expect("could not load persistence")
    // };

    static ref PERSISTENCES: RwLock<FnvHashMap<String, Persistence>> = {
        RwLock::new(FnvHashMap::default())
    };

    // static ref HASHMAP: Mutex<FnvHashMap<String, Persistence>> = {
    //     let m = FnvHashMap::default();
    //     Mutex::new(m)
    // };
}

// fn main() {
//     start_server("jmdict".to_string());
// }




pub fn start_server(database: String) {

    {
        let mut persistences = PERSISTENCES.write().unwrap();
        persistences.insert("default".to_string(), persistence::Persistence::load(database.clone()).expect("could not load persistence"));
    }
    // PERSISTENCES.write().unwrap()

    // &JMDICT_PERSISTENCE.print_heap_sizes();
    let mut router = Router::new();                     // Alternative syntax:
    router.get("/", handler, "index");                  // let router = router!(index: get "/" => handler,
    router.get("/:query", handler, "query");            //                      query: get "/:query" => handler);
    router.post("/search", search_handler, "search");
    router.post("/suggest", suggest_handler, "suggest");

    // let mut pers = Persistence::load("csv_test".to_string()).expect("Could not load persistence");

    Iron::new(router).http("0.0.0.0:3000").unwrap();

    fn handler(req: &mut Request) -> IronResult<Response> {
        let ref query = req.extensions.get::<Router>().unwrap().find("query").unwrap_or("/");
        Ok(Response::with((status::Ok, *query)))
    }

    fn search_handler(req: &mut Request) -> IronResult<Response> {
        // let ref query = req.extensions.get::<Router>().unwrap().find("query").unwrap_or("/");
        // Ok(Response::with(status::Ok))
        // Ok(Response::with((status::Ok, "*query")))
        let struct_body = req.get::<bodyparser::Struct<search::Request>>();
        match struct_body {
            Ok(Some(struct_body)) => {
                println!("Parsed body:\n{:?}", struct_body);


                // let pers:persistence::Persistence = persistence::Persistence::load("csv_test".to_string()).expect("could not load persistence");
                info_time!("search total");
                let persistences = PERSISTENCES.read().unwrap();
                let persistence = persistences.get(&"default".to_string()).unwrap();

                println!("Searching ... ");
                let hits = search::search(struct_body, &persistence).unwrap();
                println!("Loading Documents... ");
                let doc = search::to_documents(&persistence, &hits);
                println!("Returning ... ");
                Ok(Response::with((status::Ok, Header(headers::ContentType::json()), serde_json::to_string(&doc).unwrap())))
            },
            Ok(None) => {
                println!("No body");
                Ok(Response::with((status::Ok, "No body")))
            },
            Err(err) => {
                println!("Error: {:?}", err);
                Ok(Response::with((status::Ok, err.to_string())))
            }
        }
    }

    fn suggest_handler(req: &mut Request) -> IronResult<Response> {
        let struct_body = req.get::<bodyparser::Struct<search::Request>>();
        match struct_body {
            Ok(Some(struct_body)) => {
                println!("Parsed body:\n{:?}", struct_body);

                info_time!("search total");
                let persistences = PERSISTENCES.read().unwrap();
                let persistence = persistences.get(&"default".to_string()).unwrap();

                println!("Suggesting ... ");
                let hits = search_field::suggest_multi(&persistence, struct_body).unwrap();
                println!("Returning ... ");
                Ok(Response::with((status::Ok, Header(headers::ContentType::json()), serde_json::to_string(&hits).unwrap())))
            },
            Ok(None) => {
                println!("No body");
                Ok(Response::with((status::Ok, "No body")))
            },
            Err(err) => {
                println!("Error: {:?}", err);
                Ok(Response::with((status::Ok, err.to_string())))
            }
        }
    }
}
