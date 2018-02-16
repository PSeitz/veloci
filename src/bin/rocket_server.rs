#![feature(plugin, decl_macro, custom_derive)]
#![plugin(rocket_codegen)]
#![feature(plugin, custom_attribute)]
#![feature(underscore_lifetimes)]

extern crate rocket;
extern crate rocket_contrib;

extern crate bodyparser;
extern crate chashmap;
extern crate env_logger;
extern crate flexi_logger;
extern crate fnv;
extern crate hyper;
extern crate iron;
extern crate iron_cors;
extern crate multipart;
extern crate router;
extern crate serde;
#[macro_use]
extern crate serde_json;
extern crate snap;
extern crate time;
extern crate urlencoded;

extern crate iron_compress;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate measure_time;
extern crate search_lib;
extern crate flate2;
#[allow(unused_imports)]
use rocket::{Request, State, Data, Response};
use rocket::fairing;
// use rocket::fairing::{AdHoc, Fairing, Info, Kind};
// use rocket::http::{Method, ContentType, Status};
#[allow(unused_imports)]
use rocket_contrib::{Json, Value};

use search_lib::search;
use search_lib::search_field;
use search_lib::persistence::Persistence;
use search_lib::persistence;


#[allow(unused_imports)]
use fnv::FnvHashMap;
use chashmap::CHashMap;

#[allow(unused_imports)]
use std::collections::HashMap;
// use std::fs::File;
// use std::io::prelude::*;

// use flate2::Compression;
use flate2::read::GzEncoder;

lazy_static! {
    static ref PERSISTENCES: CHashMap<String, Persistence> = {
        CHashMap::default()
    };
}

pub struct Gzip;

impl fairing::Fairing for Gzip {
    fn info(&self) -> fairing::Info {
        fairing::Info {
            name: "Gzip compression",
            kind: fairing::Kind::Response,
        }
    }

    fn on_response(&self, request: &Request, response: &mut Response) {
        use flate2::{Compression};
        use std::io::{Cursor, Read};
        let headers = request.headers();
        if headers
            .get("Accept-Encoding")
            .any(|e| e.to_lowercase().contains("gzip"))
        {
            response.body_bytes().and_then(|body| {
                let mut gz = GzEncoder::new(&body[..], Compression::default());
                let mut buf = Vec::with_capacity(body.len());
                gz.read_to_end(&mut buf).map(|_| {
                    response.set_sized_body(Cursor::new(buf));
                    response.set_raw_header("Content-Encoding", "gzip");
                })
                .map_err(|e| eprintln!("{}", e)).ok()
            });
        }
    }
}


#[derive(FromForm)]
struct QueryParams {
    query: String,
    top: Option<usize>,
    skip: Option<usize>,
    levenshtein: Option<usize>,
    fields: Option<String>,
    facets: Option<String>,
    facetlimit: Option<usize>,
    boost_fields: Option<String>,
    operator: Option<String>,
}

fn query_param_to_vec(name: Option<String>) -> Option<Vec<String>> {
    name.map(|el| el.split(",").map(|f| f.to_string()).collect())
}

fn ensure_database(database: &String) {
    if !PERSISTENCES.contains_key(database) {
        PERSISTENCES.insert(
            database.clone(),
            persistence::Persistence::load(database.clone()).expect("could not load persistence"),
        );
    }
}

#[get("/version")]
fn version() -> String {
    "0.5".to_string()
}

fn search_in_persistence(persistence: &Persistence, request: search_lib::search::Request, _enable_flame: bool) -> Result<SearchResultWithDoc, SearchError> {
    // info!("Searching ... ");
    let hits = {
        info_time!("Searching ... ");
        let res = search::search(request, &persistence)?;
        // if let Some(ref err) = res.as_ref().err() {
        //     // unimplemented!();
        //     return serde_json::to_string(&json!({"Error": format!("{:?}", err)})).unwrap();
        // }
        // res.unwrap()
        // search::search(request, &persistence).unwrap()
    };
    info!("Loading Documents... ");
    let doc = {
        info_time!("Loading Documents...  ");
        search::to_search_result(&persistence, hits)
    };
    debug!("Returning ... ");
    doc

}


fn excute_suggest(persistence: &Persistence, struct_body: search::Request, _flame: bool) -> String {
    info_time!("search total");
    info!("Suggesting ... ");
    let hits = search_field::suggest_multi(&persistence, struct_body).unwrap();
    debug!("Returning ... ");

    serde_json::to_string(&hits).unwrap()
}

#[post("/<database>/search", format = "application/json", data = "<request>")]
fn search_post(database: String, request: Json<search::Request>) -> String {
    ensure_database(&database);
    let persistence = PERSISTENCES.get(&database).unwrap();

    // search_in_persistence(&persistence, request.0, false)

    "".to_string()
}

#[get("/<database>/search?<params>")]
fn search_get(database: String, params: QueryParams) -> Result<search::SearchResultWithDoc, search::SearchError> {
    ensure_database(&database);
    let persistence = PERSISTENCES.get(&database).unwrap();

    let facets: Option<Vec<String>> = query_param_to_vec(params.facets);
    let fields: Option<Vec<String>> = query_param_to_vec(params.fields);
    let boost_fields: HashMap<String, f32> = query_param_to_vec(params.boost_fields).map(|mkay| {
                mkay.into_iter()
                    .map(|el| {
                        let field_n_boost = el.split("->").collect::<Vec<&str>>();
                        (field_n_boost[0].to_string(), field_n_boost[1].parse::<f32>().unwrap())
                    })
                    .collect()
            })
            .unwrap_or(HashMap::default());

    let request = search::search_query(
        &params.query,
        &persistence,
        params.top,
        params.skip,
        params.operator,
        params.levenshtein,
        params.facetlimit,
        facets,
        fields,
        boost_fields,
    );

    debug!("{}", serde_json::to_string(&request).unwrap());
    // Json(From::from(search_in_persistence(&persistence, request, false)))
    // Json(From::from(search_in_persistence(&persistence, request, false)))
    search_in_persistence(&persistence, request, false)

}

#[post("/<database>/suggest", format = "application/json", data = "<request>")]
fn suggest_post(database: String, request: Json<search::Request>) -> String {
    ensure_database(&database);
    let persistence = PERSISTENCES.get(&database).unwrap();
    excute_suggest(&persistence, request.0, false)
}

#[get("/<database>/suggest?<params>", format = "application/json")]
fn suggest_get(database: String, params: QueryParams) -> String {
    ensure_database(&database);
    let persistence = PERSISTENCES.get(&database).unwrap();

    let fields: Option<Vec<String>> = query_param_to_vec(params.fields);

    let request = search::suggest_query(
        &params.query,
        &persistence,
        params.top,
        params.skip,
        params.levenshtein,
        fields,
    );

    debug!("{}", serde_json::to_string(&request).unwrap());
    excute_suggest(&persistence, request, false)

}

#[post("/<database>/highlight", format = "application/json", data = "<request>")]
fn highlight_post(database: String, mut request: Json<search::RequestSearchPart>) -> String {
    ensure_database(&database);
    let persistence = PERSISTENCES.get(&database).unwrap();
    let hits = search_field::highlight(&persistence, &mut request).unwrap();
    serde_json::to_string(&hits).unwrap()
}

fn main() {
    search_lib::trace::enable_log();
    rocket::ignite()
        .mount("/", routes![version, search_get, search_post, suggest_get, suggest_post, highlight_post])
        .attach(Gzip)
        .launch();
}


