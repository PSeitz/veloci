#![feature(plugin, decl_macro)]
#![plugin(rocket_codegen)]
#![feature(plugin, custom_attribute)]
#![plugin(flamer)]
#![feature(underscore_lifetimes)]

extern crate rocket;


extern crate bodyparser;
extern crate chashmap;
extern crate env_logger;
extern crate flame;
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


// use search_lib::search;
// use search_lib::search_field;
// use search_lib::persistence::Persistence;
// use search_lib::persistence;

// use time::precise_time_ns;

// #[allow(unused_imports)]
// use fnv::FnvHashMap;
// use chashmap::CHashMap;

// #[allow(unused_imports)]
// use std::collections::HashMap;
// use std::fs::File;
// use std::io::prelude::*;


#[get("/<name>/<age>")]
fn hello(name: String, age: u8) -> String {
    format!("Hello, {} year old named {}!", age, name)
}

fn main() {
    rocket::ignite().mount("/hello", routes![hello]).launch();
}