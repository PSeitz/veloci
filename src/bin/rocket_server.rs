#![feature(plugin, decl_macro, custom_derive)]
#![plugin(rocket_codegen)]
#![feature(plugin, custom_attribute)]
#![feature(underscore_lifetimes)]

extern crate rocket;

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
// #[macro_use]
extern crate serde_json;
extern crate snap;
extern crate time;
extern crate urlencoded;

extern crate iron_compress;
#[macro_use]
extern crate lazy_static;
// #[macro_use]
extern crate log;
extern crate measure_time;
extern crate search_lib;


use search_lib::search;
use search_lib::search_field;
use search_lib::persistence::Persistence;
use search_lib::persistence;


#[allow(unused_imports)]
use fnv::FnvHashMap;
use chashmap::CHashMap;

#[allow(unused_imports)]
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;

lazy_static! {

    static ref PERSISTENCES: CHashMap<String, Persistence> = {
        CHashMap::default()
    };

}

#[derive(FromForm)]
struct QueryParams {
    q: Option<String>,
    skip: Option<u32>
}

fn ensure_database(database: &String) {
    if !PERSISTENCES.contains_key(database) {
        PERSISTENCES.insert(
            database.clone(),
            persistence::Persistence::load(database.clone()).expect("could not load persistence"),
        );
    }
}

#[get("/<name>/<age>")]
fn hello(name: String, age: u8) -> String {
    format!("Hello, {} year old named {}!", age, name)
}

#[get("/<database>/search?<params>")]
fn search_get(database: String, params: QueryParams) -> String {

	let persistence = PERSISTENCES.get(&database).unwrap();

    format!("Hello, {}!", database)
}

fn main() {
	search_lib::trace::enable_log();
    rocket::ignite().mount("/hello", routes![hello]).launch();
}
