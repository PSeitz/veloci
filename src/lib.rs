#![feature(option_filter)]
#![feature(pointer_methods)]
#![feature(splice)]
#![feature(entry_and_modify)]
#![recursion_limit = "128"]
#![feature(iterator_step_by)]
#![feature(step_trait)]
#![feature(specialization)]
#![feature(dotdoteq_in_patterns)]
#![feature(inclusive_range)]
#![feature(inclusive_range_syntax)]
#![feature(conservative_impl_trait)]
#![feature(plugin)]
#![cfg_attr(test, plugin(stainless))]
#![feature(test)]
#![feature(placement_in_syntax)]
#![feature(box_syntax, box_patterns)]
#![cfg_attr(feature = "unstable", feature(alloc, heap_api, repr_simd))]
#![cfg_attr(feature = "flame_it", feature(plugin, custom_attribute))]
#![cfg_attr(feature = "flame_it", plugin(flamer))]

extern crate crossbeam_channel;
extern crate crossbeam_utils;

#[cfg(feature = "flame_it")]
extern crate flame;

extern crate memmap;

extern crate faster;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate serde_json;

extern crate rand;
extern crate serde;
// extern crate tokio_timer;
extern crate chrono;
extern crate fnv;
extern crate fst;
extern crate fst_levenshtein;
extern crate rayon;
extern crate regex;

extern crate flexi_logger;
// extern crate env_logger;
#[macro_use]
extern crate log;

// extern crate abomonation;
extern crate csv;

extern crate test;

extern crate bit_set;
extern crate bit_vec;

extern crate chashmap;
extern crate itertools;
extern crate lru_cache;
extern crate lru_time_cache;
extern crate num;
extern crate parking_lot;
extern crate trie;

#[macro_use]
extern crate prettytable;

extern crate levenshtein_automaton;
extern crate snap;
extern crate time;

extern crate bincode;

#[macro_use]
extern crate measure_time;

extern crate heapsize;
#[macro_use]
extern crate heapsize_derive;

extern crate byteorder;
extern crate fixedbitset;

extern crate json_converter;
extern crate ordered_float;
extern crate sled;

extern crate colored;
extern crate mayda;
extern crate utf8_ranges;

#[macro_use]
extern crate lazy_static;

#[macro_use]
pub mod util;
pub mod search;
pub mod create;
pub mod query_generator;
pub mod create_from_json;
pub mod create_from_csv;
pub mod doc_loader;
pub mod persistence;
pub mod persistence_data_indirect;
pub mod persistence_data;
pub mod search_field;
pub mod expression;
pub mod bucket_list;
pub mod hit_collector;
pub mod trace;
pub mod tokenizer;
pub mod execution_plan;
pub mod lev_automat;
pub mod highlight_field;
pub mod facet;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod test_why_found;

use std::str;
