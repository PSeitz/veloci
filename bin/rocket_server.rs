#![feature(plugin, decl_macro, custom_derive)]
#![plugin(rocket_codegen)]
#![feature(plugin, custom_attribute)]
#![feature(type_ascription)]

extern crate rocket;
extern crate rocket_contrib;
extern crate rocket_cors;

extern crate chashmap;
extern crate env_logger;
extern crate fnv;
extern crate serde;
#[macro_use]
extern crate serde_json;
// extern crate snap;

// extern crate time;
// extern crate bodyparser;
// extern crate urlencoded;

extern crate flate2;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate measure_time;
extern crate multipart;
extern crate search_lib;

use rocket::fairing;
use rocket::http::Method;
use rocket::response::{self, Responder, Response};
use rocket::Request;

use rocket_contrib::{Json, Value};
use rocket_cors::{AllowedHeaders, AllowedOrigins};

use multipart::server::save::Entries;
use multipart::server::save::SaveResult::*;
use multipart::server::Multipart;

use rocket::http::{ContentType, Status};
use rocket::Data;
// use rocket::response::Stream;
use rocket::response::status::Custom;

use search_lib::doc_loader::*;
use search_lib::persistence;
use search_lib::persistence::Persistence;
use search_lib::query_generator;
use search_lib::search;
use search_lib::search_field;
use search_lib::shards::Shards;

use chashmap::CHashMap;

use std::collections::HashMap;

use flate2::read::GzEncoder;
use std::io::Cursor;

lazy_static! {
    static ref PERSISTENCES: CHashMap<String, Persistence> = { CHashMap::default() };
    static ref SHARDS: CHashMap<String, Shards> = { CHashMap::default() };
}

#[derive(Debug)]
struct SearchResult(search::SearchResultWithDoc);
struct SearchErroro(search::SearchError);

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
impl<'r> Responder<'r> for SearchErroro {
    fn respond_to(self, _req: &Request) -> response::Result<'r> {
        let formatted_error: String = format!("{:?}", &self.0);
        Response::build()
            .header(ContentType::JSON)
            .sized_body(Cursor::new(serde_json::to_string(&json!({ "error": formatted_error })).unwrap()))
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
struct QueryParams {
    //TODO serialize directly into SearchQueryGeneratorParameters
    query: String,
    top: Option<usize>,
    skip: Option<usize>,
    levenshtein: Option<usize>,
    levenshtein_auto_limit: Option<usize>,
    fields: Option<String>,
    facets: Option<String>,
    stopword_lists: Option<String>,
    facetlimit: Option<usize>,
    boost_fields: Option<String>,
    boost_terms: Option<String>,
    operator: Option<String>,
    select: Option<String>,
    why_found: Option<String>,
    boost_queries: Option<String>,
    phrase_pairs: Option<String>,
    text_locality: Option<String>,
}

// struct MyParam {
//     key: String,
//     value: usize
// }

// use rocket::request::FromParam;
// use rocket::http::RawStr;

// impl FromParam for MyParam {
//     type Error = RawStr;

//     fn from_param(param: RawStr) -> Result<Self, Self::Error> {
//         let (key, val_str) = match param.find(':') {
//             Some(i) if i > 0 => (&param[..i], &param[(i + 1)..]),
//             _ => return Err(param)
//         };

//         if !key.chars().all(|c| (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
//             return Err(param);
//         }

//         val_str.parse().map(|value| {
//             MyParam {
//                 key: key,
//                 value: value
//             }
//         }).map_err(|_| param)
//     }
// }

// #[get("/test")]
// fn test() -> String {
//     "0.5".to_string()
// }

fn query_param_to_vec(name: Option<String>) -> Option<Vec<String>> {
    // TODO Replace with FromForm ? directly in QueryParams
    name.map(|el| el.split(',').map(|f| f.to_string()).collect())
}

fn ensure_database(database: &String) -> Result<(), search::SearchError> {
    if !PERSISTENCES.contains_key(database) {
        PERSISTENCES.insert(database.clone(), persistence::Persistence::load(database.clone())?);
    }
    Ok(())
}

fn ensure_shard(database: &String) -> Result<(), search::SearchError> {
    if !SHARDS.contains_key(database) {
        SHARDS.insert(database.clone(), Shards::load(database.clone())?);
        // SHARDS.insert(database.clone(), Shards::load(database.clone(), 1)?);
    }
    Ok(())
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
        SearchResult(search::to_search_result(&persistence, hits, &select))
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
    ensure_database(&database)?;
    let persistence = PERSISTENCES.get(&database).unwrap();

    search_in_persistence(&persistence, request.0, false)
}

#[get("/<database>/_idtree/<id>")]
fn get_doc_for_id_tree(database: String, id: u32) -> Json<Value> {
    let persistence = PERSISTENCES.get(&database).unwrap();
    let all_fields = persistence.get_all_fields();
    let tree = search::get_read_tree_from_fields(&persistence, &all_fields);

    Json(search::read_tree(&persistence, id, &tree).unwrap())
}

#[get("/<database>/_id/<id>")]
fn get_doc_for_id_direct(database: String, id: u32) -> Json<Value> {
    // let persistence = PERSISTENCES.get(&database).unwrap();
    // let fields = persistence.get_all_fields();
    // let tree = search::get_read_tree_from_fields(&persistence, &fields);
    ensure_database(&database).unwrap();
    let persistence = PERSISTENCES.get(&database).unwrap();
    Json(serde_json::from_str(&DocLoader::get_doc(&persistence, id as usize).unwrap()).unwrap())
}

// #[get("/<database>/<id>")]
// fn get_doc_for_id(database: String, id: u32) -> Result<serde_json::Value, search::SearchError> {
//     let persistence = PERSISTENCES.get(&database).unwrap();
//     let fields = persistence.get_all_fields();
//     let tree = search::get_read_tree_from_fields(&persistence, &fields);
//     search::read_tree(&persistence, 25000, &tree)
// }

#[get("/<database>/search?<params>")]
fn search_get(database: String, params: Result<QueryParams, rocket::Error>) -> Result<SearchResult, Custom<String>> {
    let params = params.map_err(|err| Custom(Status::BadRequest, format!("{:?}", err)))?;
    ensure_database(&database).map_err(search_error_to_rocket_error)?;
    let persistence = PERSISTENCES.get(&database).unwrap();

    let facets: Option<Vec<String>> = query_param_to_vec(params.facets);
    let stopword_lists: Option<Vec<String>> = query_param_to_vec(params.stopword_lists);
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
                    (field_n_boost[0].to_string(), field_n_boost.get(1).map(|el| el.parse::<f32>().unwrap()).unwrap_or(2.0))
                })
                .collect()
        })
        .unwrap_or(HashMap::default());

    let mut q_params = query_generator::SearchQueryGeneratorParameters {
        search_term: params.query.to_string(),
        top: params.top,
        skip: params.skip,
        operator: params.operator,
        levenshtein: params.levenshtein,
        levenshtein_auto_limit: params.levenshtein_auto_limit,
        facetlimit: params.facetlimit,
        why_found: params.why_found.map(|el| el.to_lowercase() == "true"),
        phrase_pairs: params.phrase_pairs.map(|el| el.to_lowercase() == "true"),
        text_locality: params.text_locality.map(|el| el.to_lowercase() == "true"),
        facets: facets,
        stopword_lists,
        fields: fields,
        boost_fields: boost_fields,
        boost_terms: boost_terms,
        boost_queries: None,
    };

    if let Some(el) = params.boost_queries {
        q_params.boost_queries = serde_json::from_str(&el).map_err(|_err| Custom(Status::BadRequest, "wrong format boost_queries".to_string()))?;
        println!("{:?}", q_params.boost_queries);
    }

    let mut request = query_generator::search_query(&persistence, q_params);

    request.select = query_param_to_vec(params.select);

    debug!("{}", serde_json::to_string(&request).unwrap());
    search_in_persistence(&persistence, request, false).map_err(search_error_to_rocket_error)
}

#[get("/<database>/search_shard?<params>")]
fn search_get_shard(database: String, params: QueryParams) -> Result<SearchResult, search::SearchError> {
    ensure_shard(&database)?;
    let shard = SHARDS.get(&database).unwrap();

    let facets: Option<Vec<String>> = query_param_to_vec(params.facets);
    let stopword_lists: Option<Vec<String>> = query_param_to_vec(params.stopword_lists);
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
                    (field_n_boost[0].to_string(), field_n_boost.get(1).map(|el| el.parse::<f32>().unwrap()).unwrap_or(2.0))
                })
                .collect()
        })
        .unwrap_or(HashMap::default());

    let q_params = query_generator::SearchQueryGeneratorParameters {
        search_term: params.query.to_string(),
        top: params.top,
        skip: params.skip,
        operator: params.operator,
        levenshtein: params.levenshtein,
        levenshtein_auto_limit: params.levenshtein_auto_limit,
        facetlimit: params.facetlimit,
        why_found: params.why_found.map(|el| el.to_lowercase() == "true"),
        phrase_pairs: params.phrase_pairs.map(|el| el.to_lowercase() == "true"),
        text_locality: params.text_locality.map(|el| el.to_lowercase() == "true"),
        facets: facets,
        fields: fields,
        stopword_lists,
        boost_fields: boost_fields,
        boost_terms: boost_terms,
        boost_queries: None,
    };

    //TODO enable
    // if let Some(el) = params.boost_queries {
    //     q_params.boost_queries = serde_json::from_str(&el).map_err(|_err| Custom(Status::BadRequest, "wrong format boost_queries".to_string()) )?;
    // }

    Ok(SearchResult(shard.search_all_shards_from_qp(&q_params, &query_param_to_vec(params.select))?))
}

// ******************************************** UPLOAD UPLOAD ********************************************
// use std::io;
#[post("/<database>", data = "<data>")]
// signature requires the request to have a `Content-Type`
fn multipart_upload(database: String, cont_type: &ContentType, data: Data) -> Result<String, Custom<String>> {
    // this and the next check can be implemented as a request guard but it seems like just
    // more boilerplate than necessary
    if !cont_type.is_form_data() {
        return Err(Custom(Status::BadRequest, "Content-Type not multipart/form-data".into()));
    }

    let (_, boundary) = cont_type
        .params()
        .find(|&(k, _)| k == "boundary")
        .ok_or_else(|| Custom(Status::BadRequest, "`Content-Type: multipart/form-data` boundary param not provided".into()))?;

    let resp = process_upload(boundary, data).map_err(search_error_to_rocket_error)?;

    search_lib::create::create_indices_from_str(
        &mut search_lib::persistence::Persistence::create(database.to_string()).unwrap(),
        &resp.0,
        &resp.1.unwrap_or("[]".to_string()),
        None,
        false,
    ).unwrap();
    Ok(format!("created {:?}", &database))
}

fn search_error_to_rocket_error(err: search::SearchError) -> Custom<String> {
    match err {
        search::SearchError::StringError(msg) => Custom(Status::BadRequest, msg),
        _ => Custom(Status::InternalServerError, format!("SearchError: {:?}", err)),
    }
}

fn process_upload(boundary: &str, data: Data) -> Result<(String, Option<String>), search::SearchError> {
    // let mut out = Vec::new();

    // saves all fields, any field longer than 10kB goes to a temporary directory
    // Entries could implement FromData though that would give zero control over
    // how the files are saved; Multipart would be a good impl candidate though
    match Multipart::with_body(data.open(), boundary).save().size_limit(500_000_000).temp() {
        Full(entries) => process_entries(entries),
        Partial(partial, reason) => {
            error!("Request partially processed: {:?}", reason);
            // writeln!(out, "Request partially processed: {:?}", reason)?;
            // if let Some(field) = partial.partial {
            //     writeln!(out, "Stopped on field: {:?}", field.source.headers)?;
            // }

            process_entries(partial.entries)
        }
        Error(e) => return Err(search_lib::search::SearchError::Io(e)),
    }

    // Ok(out)
}
use std::io::prelude::*;
// having a streaming output would be nice; there's one for returning a `Read` impl
// but not one that you can `write()` to
fn process_entries(entries: Entries) -> Result<(String, Option<String>), search::SearchError> {
    if entries.fields_count() == 2 {
        let mut config = String::new();
        entries
            .fields
            .get(&"config".to_string())
            .ok_or_else(|| search::SearchError::StringError(format!("expecting content field, but got {:?}", entries.fields.keys().collect::<Vec<_>>())))?[0]
            .data
            .readable()?
            .read_to_string(&mut config)?;

        let data_reader = entries
            .fields
            .get(&"data".to_string())
            .ok_or_else(|| search::SearchError::StringError(format!("expecting data field, but got {:?}", entries.fields.keys().collect::<Vec<_>>())))?[0]
            .data
            .readable()?;

        let mut data: Vec<u8> = vec![];
        search_lib::create::convert_any_json_data_to_line_delimited(data_reader, &mut data)?;
        return Ok((unsafe { String::from_utf8_unchecked(data) }, Some(config)));
    }

    let mut data: Vec<u8> = vec![];
    let data_reader = entries
        .fields
        .get(&"data".to_string())
        .ok_or_else(|| search::SearchError::StringError(format!("expecting data field, but got {:?}", entries.fields.keys().collect::<Vec<_>>())))?[0]
        .data
        .readable()?;
    search_lib::create::convert_any_json_data_to_line_delimited(data_reader, &mut data)?;
    Ok((unsafe { String::from_utf8_unchecked(data) }, None))
}

#[post("/<database>", data = "<data>")]
fn create_db(database: String, data: rocket::data::Data) -> Result<String, search::SearchError> {
    if PERSISTENCES.contains_key(&database) {
        //TODO @BUG @FixMe ERROR OWASP
        PERSISTENCES.remove(&database);
    }

    let mut out: Vec<u8> = vec![];
    search_lib::create::convert_any_json_data_to_line_delimited(data.open(), &mut out).unwrap();

    search_lib::create::create_indices_from_str(
        &mut search_lib::persistence::Persistence::create(database).unwrap(),
        unsafe { std::str::from_utf8_unchecked(&out) },
        "[]",
        None,
        false,
    ).unwrap();
    Ok("created".to_string())
}

// ******************************************** UPLOAD UPLOAD ********************************************

#[delete("/<database>")]
fn delete_db(database: String) -> Result<String, search::SearchError> {
    if PERSISTENCES.contains_key(&database) {
        PERSISTENCES.remove(&database);
    }
    use std::path::Path;
    if Path::new(&database).exists() {
        std::fs::remove_dir_all(&database)?; //TODO @BUG @FixMe ERROR OWASP
    }

    Ok("deleted".to_string())
}

#[post("/<database>/suggest", format = "application/json", data = "<request>")]
fn suggest_post(database: String, request: Json<search::Request>) -> Json<Value> {
    ensure_database(&database).unwrap();
    let persistence = PERSISTENCES.get(&database).unwrap();
    let hits = search_field::suggest_multi(&persistence, request.0).unwrap();
    Json(serde_json::from_str(&serde_json::to_string(&hits).unwrap()).unwrap())
}

#[get("/<database>/inspect/<path>/<id>")]
fn inspect_data(database: String, path: String, id: u64) -> Result<String, search::SearchError> {
    ensure_database(&database)?;
    let persistence = PERSISTENCES.get(&database).unwrap();
    // persistence.get(path)
    let data = persistence.get_valueid_to_parent(&path)?;
    Ok(serde_json::to_string(&data.get_values(id)).unwrap())
}

#[get("/<database>/suggest?<params>", format = "application/json")]
fn suggest_get(database: String, params: QueryParams) -> Result<SuggestResult, search::SearchError> {
    ensure_database(&database)?;
    let persistence = PERSISTENCES.get(&database).unwrap();

    let fields: Option<Vec<String>> = query_param_to_vec(params.fields);

    let request = query_generator::suggest_query(
        &params.query,
        &persistence,
        params.top,
        params.skip,
        params.levenshtein,
        &fields,
        params.levenshtein_auto_limit,
    );

    debug!("{}", serde_json::to_string(&request).unwrap());
    excute_suggest(&persistence, request, false)
}

#[post("/<database>/highlight", format = "application/json", data = "<request>")]
fn highlight_post(database: String, mut request: Json<search::RequestSearchPart>) -> String {
    ensure_database(&database).unwrap();
    let persistence = PERSISTENCES.get(&database).unwrap();
    let hits = search_field::highlight(&persistence, &mut request).unwrap();
    serde_json::to_string(&hits).unwrap()
}

fn main() {
    search_lib::trace::enable_log();

    for preload_db in std::env::args().skip(1) {
        ensure_database(&preload_db).unwrap();
    }
    // for preload_db in std::env::args().skip(1) {
    //     ensure_shard(&preload_db).unwrap();
    // }

    let cors_options = rocket_cors::Cors {
        allowed_origins: AllowedOrigins::all(),
        allowed_methods: vec![Method::Get, Method::Post].into_iter().map(From::from).collect(),
        allowed_headers: AllowedHeaders::all(),
        allow_credentials: true,
        ..Default::default()
    };

    println!("Starting Server...");
    rocket::ignite()
        // .mount("/", routes![version, get_doc_for_id_direct, get_doc_for_id_tree, search_get, search_post, suggest_get, suggest_post, highlight_post])
        .mount("/", routes![version, delete_db, multipart_upload, get_doc_for_id_direct, get_doc_for_id_tree, search_get, search_post, suggest_get, search_get_shard, suggest_post, highlight_post, inspect_data])
        .attach(Gzip)
        .attach(cors_options)
        .launch();
}

pub struct Gzip;
impl fairing::Fairing for Gzip {
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

    fn info(&self) -> fairing::Info {
        fairing::Info {
            name: "Gzip compression",
            kind: fairing::Kind::Response,
        }
    }
}
