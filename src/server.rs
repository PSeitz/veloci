 #![feature(underscore_lifetimes)]

extern crate bodyparser;
extern crate flexi_logger;
extern crate env_logger;
extern crate fnv;
extern crate hyper;
extern crate iron;
extern crate urlencoded;
extern crate iron_cors;
extern crate router;
extern crate serde_json;
extern crate snap;
extern crate time;
extern crate chashmap;
extern crate multipart;

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate measure_time;
extern crate search_lib;
extern crate iron_compress;

use iron_compress::GzipWriter;

use chashmap::CHashMap;


use search_lib::search;
use search_lib::search_field;
// use create;
// use doc_loader;
// use persistence;
use search_lib::persistence::Persistence;
use iron::prelude::*;
use iron::{typemap, AfterMiddleware, BeforeMiddleware, Chain, Iron, IronResult, Request, Response};
use iron_cors::CorsMiddleware;
use iron::{headers, status};
use iron::modifiers::Header;
use urlencoded::UrlEncodedQuery;

use multipart::server::{Multipart, Entries, SaveResult, SavedFile};
use iron::mime::{TopLevel, SubLevel};


use time::precise_time_ns;
use router::Router;

use search_lib::persistence;

#[allow(unused_imports)]
use std::collections::HashMap;
#[allow(unused_imports)]
use fnv::FnvHashMap;
// use std::sync::RwLock;

use std::fs::File;
use std::io::prelude::*;

struct ResponseTime;

fn main() {
    // env_logger::init().unwrap();
    search_lib::trace::enable_log();
    // start_server("jmdict".to_string());
    start_server();
}

impl typemap::Key for ResponseTime {
    type Value = u64;
}

impl BeforeMiddleware for ResponseTime {
    fn before(&self, req: &mut Request) -> IronResult<()> {
        req.extensions.insert::<ResponseTime>(precise_time_ns());
        Ok(())
    }
}

impl AfterMiddleware for ResponseTime {
    fn after(&self, req: &mut Request, res: Response) -> IronResult<Response> {
        let delta = precise_time_ns() - *req.extensions.get::<ResponseTime>().unwrap();
        info!("Request took: {} ms", (delta as f64) / 1000000.0);
        Ok(res)
    }
}

// fn hello_world(_: &mut Request) -> IronResult<Response> {
//     Ok(Response::with((iron::status::Ok, "Hello World")))
// }
// const MAX_BODY_LENGTH: usize = 1024 * 1024 * 10;



// static STATIC: CHashMap<String, Persistence> = CHashMap::new();

lazy_static! {
    // static ref CSV_PERSISTENCE: Persistence = {
    //     persistence::Persistence::load("csv_test".to_string()).expect("could not load persistence")
    // };
    // static ref JMDICT_PERSISTENCE: Persistence = {
    //     persistence::Persistence::load("jmdict".to_string()).expect("could not load persistence")
    // };

    static ref PERSISTENCES: CHashMap<String, Persistence> = {
        CHashMap::default()
    };

    // static ref HASHMAP: Mutex<FnvHashMap<String, Persistence>> = {
    //     let m = FnvHashMap::default();
    //     Mutex::new(m)
    // };
}

fn ensure_database(database: &String) {
    if !PERSISTENCES.contains_key(database) {
        PERSISTENCES.insert(database.clone(), persistence::Persistence::load(database.clone()).expect("could not load persistence"));
    }
}

pub fn start_server() {

    // ensure_database(&database);
    // PERSISTENCES.write()

    // &JMDICT_PERSISTENCE.print_heap_sizes();
    let mut router = Router::new(); // Alternative syntax:
    router.get("/", handler, "index"); // let router = router!(index: get "/" => handler,
    router.get("/:query", handler, "query"); //                      query: get "/:query" => handler);
    router.post("/:database/search", search_handler, "search");
    router.get("/:database/search", search_get_handler, "search_get");
    router.post("/:database/suggest", suggest_handler, "suggest");
    router.post("/:database/highlight", highlight_handler, "highlight");
    // let mut pers = Persistence::load("csv_test".to_string()).expect("Could not load persistence");

    router.post("/data/:database", handlero, "handlero");

    // Initialize middleware
    let cors_middleware = CorsMiddleware::with_allow_any();
    // Setup chain with middleware
    let mut chain = Chain::new(router);
    chain.link_around(cors_middleware);

    use std::env;

    let port = env::var("SERVER_PORT").unwrap_or("3000".to_string());
    let ip = env::var("SERVER_IP").unwrap_or("0.0.0.0".to_string());

    let combined = format!("{}:{}", ip, port);
    println!("Start server {:?}", combined);
    Iron::new(chain).http(combined).unwrap();

    fn handler(req: &mut Request) -> IronResult<Response> {
        let ref query = req.extensions.get::<Router>().unwrap().find("query").unwrap_or("/");
        Ok(Response::with((status::Ok, *query)))
    }

    fn search_get_handler(req: &mut Request) -> IronResult<Response> {
        info_time!("search request total");
        let database = req.extensions.get::<Router>().unwrap().find("database").expect("could not find collection name in url").to_string();
        ensure_database(&database);
        
        // Extract the decoded data as hashmap, using the UrlEncodedQuery plugin.
        match req.get_ref::<UrlEncodedQuery>() {
            Ok(ref hashmap) => {

                info!("Parsed GET request query string:\n {:?}", hashmap);
                let ref query = hashmap.get("query").expect("not query parameter found").iter().nth(0).unwrap();
                let ref top =   hashmap.get("top").map(|el|el.iter().nth(0).unwrap().parse::<usize>().unwrap());
                let ref skip =  hashmap.get("skip").map(|el|el.iter().nth(0).unwrap().parse::<usize>().unwrap());
                let ref levenshtein =  hashmap.get("levenshtein").map(|el|el.iter().nth(0).unwrap().parse::<usize>().unwrap());

                info!("query {:?} top {:?} skip {:?}", query, top, skip);
                // let persistences = PERSISTENCES.read();
                let persistence = PERSISTENCES.get(&database).unwrap();

                let request = search::search_query(query.clone(), &persistence, top.clone(), skip.clone(), levenshtein.clone());
                search_in_persistence(&persistence, request)
            },
            Err(ref e) => Err(IronError::new(StringError(e.to_string()), status::BadRequest))
        }

    }

    use std::error::Error;
    use std::fmt::{self, Debug};
    #[derive(Debug)]
    struct StringError(String);

    impl fmt::Display for StringError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            Debug::fmt(self, f)
        }
    }

    impl Error for StringError {
        fn description(&self) -> &str { &*self.0 }
    }


    fn search_in_persistence(persistence: &Persistence, request: search_lib::search::Request) -> IronResult<Response> {
        info!("Searching ... ");
        let hits = {
            info_time!("Searching ... ");
            search::search(request, &persistence).unwrap()
        };
        info!("Loading Documents... ");
        let doc = {
            info_time!("Loading Documents...  ");
            search::to_search_result(&persistence, &hits)
        };

        info!("Returning ... ");

        Ok(Response::with((status::Ok, Header(headers::ContentType::json()), GzipWriter(serde_json::to_string(&doc).unwrap().as_bytes()))))
    }

    fn search_handler(req: &mut Request) -> IronResult<Response> {
        let database = req.extensions.get::<Router>().unwrap().find("database").expect("could not find collection name in url").to_string();
        ensure_database(&database);
        // let ref query = req.extensions.get::<Router>().unwrap().find("query").unwrap_or("/");
        // Ok(Response::with(status::Ok))
        // Ok(Response::with((status::Ok, "*query")))
        let struct_body = req.get::<bodyparser::Struct<search::Request>>();
        match struct_body {
            Ok(Some(struct_body)) => {
                info!("Parsed body:\n{:?}", struct_body);
                info_time!("search total");

                // let persistences = PERSISTENCES.read();
                let persistence = PERSISTENCES.get(&database).unwrap();
                search_in_persistence(&persistence, struct_body)
            }
            Ok(None) => {
                info!("No body");
                Ok(Response::with((status::Ok, "No body")))
            }
            Err(err) => {
                info!("Error: {:?}", err);
                Ok(Response::with((status::Ok, err.to_string())))
            }
        }
    }

    fn suggest_handler(req: &mut Request) -> IronResult<Response> {
        let database = req.extensions.get::<Router>().unwrap().find("database").expect("could not find collection name in url").to_string();
        ensure_database(&database);
        let struct_body = req.get::<bodyparser::Struct<search::Request>>();
        match struct_body {
            Ok(Some(struct_body)) => {
                info!("Parsed body:\n{:?}", struct_body);

                info_time!("search total");
                let persistence = PERSISTENCES.get(&database).unwrap();

                info!("Suggesting ... ");
                let hits = search_field::suggest_multi(&persistence, struct_body).unwrap();
                info!("Returning ... ");
                Ok(Response::with((status::Ok, Header(headers::ContentType::json()), serde_json::to_string(&hits).unwrap())))
            }
            Ok(None) => {
                info!("No body");
                Ok(Response::with((status::Ok, "No body")))
            }
            Err(err) => {
                info!("Error: {:?}", err);
                Ok(Response::with((status::Ok, err.to_string())))
            }
        }
    }

    fn highlight_handler(req: &mut Request) -> IronResult<Response> {
        let database = req.extensions.get::<Router>().unwrap().find("database").expect("could not find collection name in url").to_string();
        ensure_database(&database);
        let struct_body = req.get::<bodyparser::Struct<search::RequestSearchPart>>();
        match struct_body {
            Ok(Some(mut struct_body)) => {
                info!("Parsed body:\n{:?}", struct_body);

                info_time!("search total");
                let persistence = PERSISTENCES.get(&database).unwrap();

                info!("highlighting ... ");
                let hits = search_field::highlight(&persistence, &mut struct_body).unwrap();
                info!("Returning ... ");
                Ok(Response::with((status::Ok, Header(headers::ContentType::json()), serde_json::to_string(&hits).unwrap())))
            }
            Ok(None) => {
                info!("No body");
                Ok(Response::with((status::Ok, "No body")))
            }
            Err(err) => {
                info!("Error: {:?}", err);
                Ok(Response::with((status::Ok, err.to_string())))
            }
        }
    }


    // create stuff

    fn handlero(req: &mut Request) -> IronResult<Response> {
        
        println!("getting 1 request");
        let header = req.headers.get::<headers::ContentType>().expect("no content type set").clone();

        println!("header: {:?}", *header);
        match *header {
            iron::mime::Mime(TopLevel::Application, SubLevel::Json, _) => Ok(Response::with((status::BadRequest, "error"))),
            iron::mime::Mime(TopLevel::Application, iron::mime::SubLevel::WwwFormUrlEncoded, _) |
            iron::mime::Mime(TopLevel::Multipart, iron::mime::SubLevel::FormData, _) => handle_multipart(req),
            _ => {
                let error = format!("content type has to be {:?}/{:?} or {:?}/{:?} but got {:?}", TopLevel::Application, SubLevel::Json,TopLevel::Multipart, iron::mime::SubLevel::FormData, *header );
                println!("Error: {:?}", error);
                Ok(Response::with((status::BadRequest, error)))
            }
        }
    }

    // fn proxy_handler(req: &mut Request) -> IronResult<Response> {
    //     println!("getting 1 request");
    //     let header = req.headers.get::<headers::ContentType>().expect("no content type set").clone();

    //     println!("header: {:?}", *header);
    //     match *header {
    //         iron::mime::Mime(TopLevel::Application, SubLevel::Json, _) => return Ok(Response::with((status::BadRequest, error)),
    //         iron::mime::Mime(TopLevel::Application, iron::mime::SubLevel::WwwFormUrlEncoded, _) |
    //         iron::mime::Mime(TopLevel::Multipart, iron::mime::SubLevel::FormData, _) => return handle_multipart(req),
    //         _ => {
    //             let error = format!("content type has to be {:?}/{:?} or {:?}/{:?} but got {:?}", TopLevel::Application, SubLevel::Json,TopLevel::Multipart, iron::mime::SubLevel::FormData, *header );
    //             println!("Error: {:?}", error);
    //             return Ok(Response::with((status::BadRequest, error)))
    //         }
    //     }
    // }

    fn handle_multipart(req: &mut Request) -> IronResult<Response> {
        let database = req.extensions.get::<Router>().unwrap().find("database").expect("could not find collection name in url").to_string();
        match Multipart::from_request(req) {
            Ok(mut multipart) => {
                // Fetching all data and processing it.
                // save().temp() reads the request fully, parsing all fields and saving all files
                // in a new temporary directory under the OS temporary directory.
                match multipart.save().temp() {
                    SaveResult::Full(entries) =>  process_entries(entries, database),
                    SaveResult::Partial(_, reason) => {Ok(Response::with((status::BadRequest, format!("error reading request: {}", reason.unwrap_err())))) }
                    SaveResult::Error(error) => Ok(Response::with((status::BadRequest, format!("error reading request: {}", error))))
                }
            }
            Err(err) => {
                println!("Error: {:?}", err);
                Ok(Response::with((status::BadRequest, "The request is not multipart?")))
            }
        }
    }


    fn process_entries(entries: Entries, database:String) -> IronResult<Response> {

        if entries.files.len() != 1 {return Ok(Response::with((status::BadRequest, format!("only single file uploads supported, but got {} entries", entries.files.len())))); }

        let entry = entries.files.iter().last().unwrap();
        println!("Field {:?} has {} files:", entry.0, entry.1.len());
        if entry.1.len() != 1 {return Ok(Response::with((status::BadRequest, "only single file uploads supported" ))); }
        let contents = get_multipart_file_contents(&entry.1.iter().last().unwrap())?;
        let data: serde_json::Value = serde_json::from_str(&contents).expect("InvalidJson");

        // Start up a test.
        let indices = r#"[] "#;
        // let indices = r#"
        // [
        //     { "fulltext":"address[].line[]", "options":{"tokenize":true} }
        // ]
        // "#;



        println!("{:?}", search_lib::create::create_indices(&database, &contents, indices));

        // {
        //     let mut pers = persistence::Persistence::load(database.to_string()).expect("Could not load persistence");
        //     // let mut pers = persistence::Persistence::load(database.to_string()).expect("Could not load persistence");
        //     let config = json!({
        //         "path": "meanings.ger[]"
        //     });
        //     create::add_token_values_to_tokens(&mut pers, TOKEN_VALUE, &config.to_string()).expect("Could not add token values");
        // }
        // PERSISTENCES.insert("default".to_string(), persistence::Persistence::load(database.to_string()).expect("could not load persistence"));



        Ok(Response::with((status::Ok, "schema::convert_to_schema(&data).unwrap()" )))
        // Ok(Response::with((status::Ok, schema::convert_to_schema(&data).unwrap())))

    }

    fn get_multipart_file_contents(saved_file: &SavedFile) -> IronResult<(String)> {
        let mut file = match File::open(&saved_file.path) {
            Ok(file) => file,
            Err(error) => {return Err(IronError::new(error, (status::InternalServerError, "Server couldn't open saved file"))) } };

        let mut contents = String::new();
        if let Err(error) = file.read_to_string(&mut contents) {
            return Err(IronError::new(error, (status::BadRequest, "The file was not a text")));
        }
        println!("File {:?} ({:?}):", saved_file.filename, saved_file.content_type);
        Ok((contents))
    }




}
