#![feature(plugin)]
#![cfg_attr(test, plugin(stainless))]

#![feature(test)]

#![feature(placement_in_syntax)]
#![feature(box_syntax, box_patterns)]
#![cfg_attr(feature= "unstable", feature(alloc, heap_api, repr_simd))]

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate serde_json;

extern crate serde;
extern crate rand;
// extern crate tokio_timer;
extern crate regex;
extern crate fnv;
extern crate fst;

#[macro_use] extern crate log;
extern crate env_logger;

// extern crate abomonation;
extern crate csv;

extern crate test;

extern crate bit_set;
extern crate bit_vec;

extern crate num;

extern crate hyper;
extern crate iron;
extern crate iron_cors;
extern crate bodyparser;
extern crate router;
extern crate time;
extern crate snap;

extern crate bincode;

#[macro_use]
extern crate measure_time;

extern crate heapsize;

extern crate byteorder;

extern crate sled;

// use fst::{IntoStreamer, Streamer, Levenshtein, Set, MapBuilder};
#[allow(unused_imports)]
use fst::{IntoStreamer, Levenshtein, Set, MapBuilder};
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

#[macro_use] extern crate lazy_static;

#[macro_use]
pub mod util;
pub mod search;
pub mod create;
pub mod doc_loader;
pub mod persistence;
pub mod persistence_data;
pub mod search_field;
pub mod expression;
pub mod bucket_list;
pub mod server;

#[cfg(test)]
mod tests;

use std::str;
