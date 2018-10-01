#![feature(drain_filter)]
#![feature(align_offset)]
#![feature(ptr_offset_from)]
#![feature(step_trait)]
#![feature(specialization)]
#![feature(test)]
#![cfg_attr(feature = "unstable", feature(alloc, heap_api, repr_simd))]
#![cfg_attr(feature = "flame_it", feature(plugin, custom_attribute))]
#![cfg_attr(feature = "flame_it", plugin(flamer))]
#![feature(core_intrinsics)]

extern crate crossbeam;
extern crate crossbeam_channel;
extern crate crossbeam_utils;

#[cfg(feature = "flame_it")]
extern crate flame;

extern crate memmap;

#[cfg(feature = "enable_cpuprofiler")]
extern crate cpuprofiler;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate serde_json;

extern crate vint;

extern crate serde;

extern crate fnv;
extern crate fst;

extern crate rayon;
extern crate regex;
// extern crate uuid;

#[macro_use]
#[allow(unused_imports)]
extern crate dump;

extern crate flexi_logger;
#[allow(unused_imports)]
#[macro_use]
extern crate log;

extern crate itertools;

extern crate lru_time_cache;
extern crate num;
extern crate parking_lot;

#[macro_use]
extern crate prettytable;

// extern crate bincode;
extern crate levenshtein_automata;
// extern crate snap;

extern crate heapsize;
#[macro_use]
extern crate heapsize_derive;
#[macro_use]
extern crate measure_time;

extern crate byteorder;

extern crate buffered_index_writer;
extern crate json_converter;
extern crate ordered_float;
extern crate parser;

extern crate colored;

extern crate half;
#[macro_use]
extern crate lazy_static;

extern crate chrono;
extern crate fixedbitset;
// extern crate lru_cache;
extern crate term_hashmap;
extern crate doc_store as other_doc_store;


#[macro_use]
pub mod util;
pub mod stopwords;
#[macro_use]
pub mod type_info;
pub mod create;
// pub mod create_from_csv;
pub mod doc_store;
pub mod execution_plan;
pub mod expression;
pub mod facet;
pub mod highlight_field;
pub mod persistence;
pub mod persistence_data;
pub mod persistence_data_binary_search;
pub mod persistence_data_indirect;
pub mod persistence_score;
pub mod query_generator;
pub mod search;
pub mod search_field;
pub mod shards;
pub mod tokenizer;
pub mod trace;

#[cfg(test)]
extern crate chashmap;

#[cfg(test)]
extern crate test;

#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate tempfile;

// #[cfg(test)]
// mod test_why_found;
// #[cfg(test)]
// mod tests;

use std::str;
