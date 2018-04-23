#![feature(option_filter)]
#![feature(align_offset)]
#![feature(offset_to)]
#![feature(pointer_methods)]
#![feature(core_intrinsics)]
#![feature(splice)]
#![feature(entry_and_modify)]
#![recursion_limit = "128"]
#![feature(iterator_step_by)]
#![feature(step_trait)]
#![feature(specialization)]
#![feature(inclusive_range)]
#![feature(inclusive_range_fields)]
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

// extern crate faster;
extern crate lz4;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate serde_json;

extern crate vint;

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
extern crate half;

#[macro_use]
pub mod util;
#[macro_use]
pub mod type_info;
pub mod bucket_list;
pub mod create;
pub mod create_from_csv;
pub mod create_from_json;
pub mod doc_loader;
pub mod execution_plan;
pub mod expression;
pub mod facet;
pub mod highlight_field;
pub mod kmerge_by;
pub mod lev_automat;
pub mod persistence;
pub mod persistence_data;
pub mod persistence_data_indirect;
pub mod persistence_score;
pub mod query_generator;
pub mod search;
pub mod search_field;
pub mod tokenizer;
pub mod trace;

#[cfg(test)]
extern crate tempfile;

#[cfg(test)]
mod test_why_found;
#[cfg(test)]
mod tests;

use std::str;
