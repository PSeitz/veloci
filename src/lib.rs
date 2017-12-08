#![feature(plugin)]
#![cfg_attr(test, plugin(stainless))]
#![feature(test)]
#![feature(placement_in_syntax)]
#![feature(box_syntax, box_patterns)]
#![cfg_attr(feature = "unstable", feature(alloc, heap_api, repr_simd))]

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate serde_json;

extern crate rand;
extern crate serde;
// extern crate tokio_timer;
extern crate fnv;
extern crate fst;
extern crate regex;
extern crate chrono;
extern crate rayon;

extern crate flexi_logger;
// extern crate env_logger;
#[macro_use]
extern crate log;

// extern crate abomonation;
extern crate csv;

extern crate test;

extern crate bit_set;
extern crate bit_vec;

extern crate num;

extern crate bodyparser;
extern crate hyper;
extern crate iron;
extern crate iron_cors;
extern crate router;
extern crate snap;
extern crate time;

extern crate bincode;

#[macro_use]
extern crate measure_time;

extern crate heapsize;

extern crate byteorder;

extern crate sled;
extern crate json_converter;

// use fst::{IntoStreamer, Streamer, Levenshtein, Set, MapBuilder};
#[allow(unused_imports)]
use fst::{IntoStreamer, Levenshtein, MapBuilder, Set};
#[allow(unused_imports)]
use std::io::{self, BufRead};
#[allow(unused_imports)]
use fnv::FnvHashSet;
#[allow(unused_imports)]
use std::collections::HashSet;
#[allow(unused_imports)]
use std::collections::HashMap;
#[allow(unused_imports)]
use fnv::FnvHashMap;

#[macro_use]
extern crate lazy_static;

#[macro_use]
pub mod util;
pub mod search;
pub mod create;
pub mod create_from_json;
pub mod doc_loader;
pub mod persistence;
pub mod persistence_data;
pub mod search_field;
pub mod expression;
pub mod bucket_list;
pub mod hit_collector;
pub mod trace;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod bench;

use std::str;
