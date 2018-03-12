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
extern crate flate2;
extern crate search_lib;
#[allow(unused_imports)]
use rocket::{Data, Request, State};
#[allow(unused_imports)]
use rocket::response::{self, Responder, Response, ResponseBuilder};
use rocket::fairing;
// use rocket::fairing::{AdHoc, Fairing, Info, Kind};
#[allow(unused_imports)]
use rocket::http::{ContentType, Method, Status};
#[allow(unused_imports)]
use rocket_contrib::{Json, Value};

use search_lib::doc_loader::*;
use search_lib::search;
use search_lib::query_generator;
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
#[allow(unused_imports)]
use std::io::{BufReader, Cursor};

lazy_static! {
    static ref PERSISTENCES: CHashMap<String, Persistence> = {
        CHashMap::default()
    };
}

#[derive(Debug)]
struct SearchResult(search::SearchResultWithDoc);

#[derive(Debug)]
struct SuggestResult(search_field::SuggestFieldResult);

impl<'r> Responder<'r> for SearchResult {
    fn respond_to(self, _req: &Request) -> response::Result<'r> {
        Response::build()
            .header(ContentType::JSON)
            .sized_body(Cursor::new(serde_json::to_string(&self.0).unwrap()))
            .ok()
    }
}

impl<'r> Responder<'r> for SuggestResult {
    fn respond_to(self, _req: &Request) -> response::Result<'r> {
        Response::build()
            .header(ContentType::JSON)
            .sized_body(Cursor::new(serde_json::to_string(&self.0).unwrap()))
            .ok()
    }
}

#[derive(FromForm)]
struct QueryParams { //TODO serialize directly into SearchQueryGeneratorParameters
    query: String,
    top: Option<usize>,
    skip: Option<usize>,
    levenshtein: Option<usize>,
    levenshtein_auto_limit: Option<usize>,
    fields: Option<String>,
    facets: Option<String>,
    facetlimit: Option<usize>,
    boost_fields: Option<String>,
    boost_terms: Option<String>,
    operator: Option<String>,
    select: Option<String>,
    why_found: Option<String>,
}

fn query_param_to_vec(name: Option<String>) -> Option<Vec<String>> {
    // TODO Replace with FromForm ? directly in QueryParams
    name.map(|el| el.split(',').map(|f| f.to_string()).collect())
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

fn search_in_persistence(persistence: &Persistence, request: search_lib::search::Request, _enable_flame: bool) -> Result<SearchResult, search::SearchError> {
    // info!("Searching ... ");
    let select = request.select.clone();
    let hits = {
        info_time!("Searching ... ");
        search::search(request, &persistence)?
    };
    info!("Loading Documents... ");
    let doc = {
        info_time!("Loading Documents...  ");
        SearchResult(search::to_search_result(&persistence, hits, select))
    };
    debug!("Returning ... ");
    Ok(doc)
}

fn excute_suggest(persistence: &Persistence, struct_body: search::Request, _flame: bool) -> Result<SuggestResult, search::SearchError> {
    info_time!("search total");
    info!("Suggesting ... ");
    let hits = search_field::suggest_multi(persistence, struct_body)?;
    debug!("Returning ... ");
    Ok(SuggestResult(hits))
}

#[post("/<database>/search", format = "application/json", data = "<request>")]
fn search_post(database: String, request: Json<search::Request>) -> Result<SearchResult, search::SearchError> {
    ensure_database(&database);
    let persistence = PERSISTENCES.get(&database).unwrap();

    search_in_persistence(&persistence, request.0, false)
}

#[get("/<database>/_idtree/<id>")]
fn get_doc_for_id_tree(database: String, id: u32) -> Json<Value> {
    let persistence = PERSISTENCES.get(&database).unwrap();
    let fields = persistence.get_all_properties();
    let tree = search::get_read_tree_from_fields(&persistence, &fields);

    Json(search::read_tree(&persistence, id, &tree).unwrap())
}

#[get("/<database>/_id/<id>")]
fn get_doc_for_id_direct(database: String, id: u32) -> Json<Value> {
    // let persistence = PERSISTENCES.get(&database).unwrap();
    // let fields = persistence.get_all_properties();
    // let tree = search::get_read_tree_from_fields(&persistence, &fields);
    ensure_database(&database);
    let persistence = PERSISTENCES.get(&database).unwrap();
    Json(serde_json::from_str(&DocLoader::get_doc(&persistence, id as usize).unwrap()).unwrap())
}
// #[get("/<database>/<id>")]
// fn get_doc_for_id(database: String, id: u32) -> Result<serde_json::Value, search::SearchError> {
//     let persistence = PERSISTENCES.get(&database).unwrap();
//     let fields = persistence.get_all_properties();
//     let tree = search::get_read_tree_from_fields(&persistence, &fields);

//     search::read_tree(&persistence, 25000, &tree)

// }

#[get("/<database>/search?<params>")]
fn search_get(database: String, params: QueryParams) -> Result<SearchResult, search::SearchError> {
    ensure_database(&database);
    let persistence = PERSISTENCES.get(&database).unwrap();

    let facets: Option<Vec<String>> = query_param_to_vec(params.facets);
    let fields: Option<Vec<String>> = query_param_to_vec(params.fields);
    let boost_fields: HashMap<String, f32> = query_param_to_vec(params.boost_fields)
        .map(|mkay| {
            mkay.into_iter()
                .map(|el| {
                    let field_n_boost = el.split("->").collect::<Vec<&str>>();
                    (field_n_boost[0].to_string(), field_n_boost[1].parse::<f32>().unwrap())
                })
                .collect()
        })
        .unwrap_or(HashMap::default());

    let boost_terms: HashMap<String, f32> = query_param_to_vec(params.boost_terms)
        .map(|mkay| {
            mkay.into_iter()
                .map(|el| {
                    let field_n_boost = el.split("->").collect::<Vec<&str>>();
                    (
                        field_n_boost[0].to_string(),
                        field_n_boost.get(1).map(|el| el.parse::<f32>().unwrap()).unwrap_or(2.0),
                    )
                })
                .collect()
        })
        .unwrap_or(HashMap::default());

    let q_params = query_generator::SearchQueryGeneratorParameters {
        search_term : params.query.to_string(),
        top: params.top,
        skip: params.skip,
        operator : params.operator,
        levenshtein: params.levenshtein,
        levenshtein_auto_limit: params.levenshtein_auto_limit,
        facetlimit: params.facetlimit,
        why_found : params.why_found.map(|el| el == "true" || el == "TRUE" || el == "True"),
        facets : facets,
        fields : fields,
        boost_fields : boost_fields,
        boost_terms : boost_terms,
    };

    let mut request = query_generator::search_query(
        &persistence,
        q_params,
    );

    request.select = query_param_to_vec(params.select);

    debug!("{}", serde_json::to_string(&request).unwrap());
    search_in_persistence(&persistence, request, false)
}

#[post("/<database>/suggest", format = "application/json", data = "<request>")]
fn suggest_post(database: String, request: Json<search::Request>) -> Result<SuggestResult, search::SearchError> {
    ensure_database(&database);
    let persistence = PERSISTENCES.get(&database).unwrap();
    excute_suggest(&persistence, request.0, false)
}

#[get("/<database>/inspect/<path>/<id>")]
fn inspect_data(database: String, path: String, id: u64) -> Result<String, search::SearchError> {
    ensure_database(&database);
    let persistence = PERSISTENCES.get(&database).unwrap();
    // persistence.get(path)
    let data = persistence.get_valueid_to_parent(&path)?;
    Ok(serde_json::to_string(&data.get_values(id)).unwrap())
}

#[get("/<database>/suggest?<params>", format = "application/json")]
fn suggest_get(database: String, params: QueryParams) -> Result<SuggestResult, search::SearchError> {
    ensure_database(&database);
    let persistence = PERSISTENCES.get(&database).unwrap();

    let fields: Option<Vec<String>> = query_param_to_vec(params.fields);

    let request = query_generator::suggest_query(
        &params.query,
        &persistence,
        params.top,
        params.skip,
        params.levenshtein,
        fields,
        params.levenshtein_auto_limit,
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

    for preload_db in std::env::args().skip(1) {
        ensure_database(&preload_db);
    }
    println!("Starting Server...");
    rocket::ignite()
        // .mount("/", routes![version, get_doc_for_id_direct, get_doc_for_id_tree, search_get, search_post, suggest_get, suggest_post, highlight_post])
        .mount("/", routes![version, get_doc_for_id_direct, get_doc_for_id_tree, search_get, search_post, suggest_get, suggest_post, highlight_post, inspect_data])
        .attach(Gzip)
        .launch();
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
        use flate2::Compression;
        use std::io::{Cursor, Read};
        let headers = request.headers();
        if headers.get("Accept-Encoding").any(|e| e.to_lowercase().contains("gzip")) {
            response.body_bytes().and_then(|body| {
                let mut gz = GzEncoder::new(&body[..], Compression::default());
                let mut buf = Vec::with_capacity(body.len());
                gz.read_to_end(&mut buf)
                    .map(|_| {
                        response.set_sized_body(Cursor::new(buf));
                        response.set_raw_header("Content-Encoding", "gzip");
                    })
                    .map_err(|e| eprintln!("{}", e))
                    .ok()
            });
        }
    }
}
