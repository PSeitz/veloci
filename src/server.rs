// use util;
use search;
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
    static ref JMDICT_PERSISTENCE: Persistence = {
        persistence::Persistence::load("jmdict".to_string()).expect("could not load persistence")
    };
}


pub fn start_server() {
    &JMDICT_PERSISTENCE.print_heap_sizes();
    let mut router = Router::new();                     // Alternative syntax:
    router.get("/", handler, "index");                  // let router = router!(index: get "/" => handler,
    router.get("/:query", handler, "query");            //                      query: get "/:query" => handler);
    router.post("/search", post_handler, "search");

    // let mut pers = Persistence::load("csv_test".to_string()).expect("Could not load persistence");

    Iron::new(router).http("0.0.0.0:3000").unwrap();

    fn handler(req: &mut Request) -> IronResult<Response> {
        let ref query = req.extensions.get::<Router>().unwrap().find("query").unwrap_or("/");
        Ok(Response::with((status::Ok, *query)))
    }

    fn post_handler(req: &mut Request) -> IronResult<Response> {

        let struct_body = req.get::<bodyparser::Struct<search::Request>>();
        match struct_body {
            Ok(Some(struct_body)) => {
                println!("Parsed body:\n{:?}", struct_body);

                // let mut pers = match try!(req.get::<Persistence>()) {
                //     Some(pers) => pers,
                //     None => {
                //         let _pers:persistence::Persistence = persistence::Persistence::load("csv_test".to_string()).expect("could not load persistence");
                //         req.set::<Persistence>(_pers);
                //         // Aaa("".to_owned())
                //         _pers
                //     }
                // };

                // req.get::<Persistence>()

                // let pers:persistence::Persistence = persistence::Persistence::load("csv_test".to_string()).expect("could not load persistence");
                info_time!("search total");

                let hits = search::search(struct_body, 0, 10, &JMDICT_PERSISTENCE).unwrap();
                let doc = search::to_documents(&JMDICT_PERSISTENCE, &hits);
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

        // let ref query = req.extensions.get::<Router>().unwrap().find("query").unwrap_or("/");
        // Ok(Response::with(status::Ok))
        // Ok(Response::with((status::Ok, "*query")))
    }

    // let mut chain = Chain::new(hello_world);
    // chain.link_before(ResponseTime);
    // chain.link_after(ResponseTime);
    // Iron::new(chain).http("localhost:3000").unwrap();
}