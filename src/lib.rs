#![feature(option_filter)]
#![feature(extern_prelude)]
#![feature(align_offset)]
#![feature(offset_to)]
#![feature(ptr_offset_from)]
#![feature(pointer_methods)]
#![feature(core_intrinsics)]
#![feature(splice)]
#![feature(entry_and_modify)]
#![recursion_limit = "128"]
#![feature(iterator_step_by)]
#![feature(step_trait)]
#![feature(specialization)]
#![feature(inclusive_range)]
#![feature(inclusive_range_methods)]
#![feature(plugin)]
#![cfg_attr(test, plugin(stainless))]
#![feature(test)]
#![feature(placement_in_syntax)]
#![feature(use_extern_macros)]
#![feature(box_syntax, box_patterns)]
#![cfg_attr(feature = "unstable", feature(alloc, heap_api, repr_simd))]
#![cfg_attr(feature = "flame_it", feature(plugin, custom_attribute))]
#![cfg_attr(feature = "flame_it", plugin(flamer))]

extern crate crossbeam_channel;
extern crate crossbeam_utils;

#[cfg(feature = "flame_it")]
extern crate flame;

extern crate memmap;

// extern crate fxhash;

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
extern crate uuid;

#[macro_use]
extern crate dump;

extern crate flexi_logger;
#[macro_use]
extern crate log;

extern crate test;

extern crate chashmap;
extern crate itertools;

extern crate lru_time_cache;
extern crate num;
extern crate parking_lot;

#[macro_use]
extern crate prettytable;

extern crate bincode;
extern crate levenshtein_automata;
extern crate snap;

extern crate heapsize;
#[macro_use]
extern crate heapsize_derive;
#[macro_use]
extern crate measure_time;

extern crate byteorder;

extern crate buffered_index_writer;
extern crate json_converter;
extern crate ordered_float;

extern crate colored;

// #[cfg(feature = "mayda")]
// extern crate mayda;

extern crate half;
#[macro_use]
extern crate lazy_static;

extern crate chrono;
extern crate term_hashmap;
// extern crate time;
// extern crate bit_set;
// extern crate bit_vec;
// extern crate sled;
// extern crate lz4;
// extern crate utf8_ranges;
extern crate btree;
extern crate fixedbitset;
extern crate lru_cache;
// extern crate fst_levenshtein;

#[macro_use]
pub mod util;
#[macro_use]
pub mod type_info;
pub mod bucket_list;
pub mod create;
// pub mod create_from_csv;
pub mod create_from_json;
pub mod doc_loader;
pub mod execution_plan;
pub mod expression;
pub mod facet;
pub mod highlight_field;
pub mod persistence;
pub mod persistence_data;
pub mod persistence_data_indirect;
pub mod persistence_score;
pub mod query_generator;
pub mod search;
pub mod search_field;
pub mod shards;
pub mod tokenizer;
pub mod trace;

#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate tempfile;

#[cfg(test)]
mod test_why_found;
#[cfg(test)]
mod tests;

use std::str;
