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

#[cfg(feature = "enable_cpuprofiler")]
extern crate cpuprofiler;
#[cfg(feature = "flame_it")]
extern crate flame;

extern crate byteorder;
extern crate chrono;
extern crate colored;
extern crate crossbeam_channel;
#[macro_use]
#[allow(unused_imports)]
extern crate dump;
extern crate fixedbitset;
extern crate flexi_logger;
extern crate fnv;
extern crate fst;
extern crate half;
extern crate heapsize;
#[macro_use]
extern crate heapsize_derive;
extern crate itertools;
#[macro_use]
extern crate lazy_static;
extern crate log;
extern crate lru_time_cache;
extern crate memmap;
extern crate num;
extern crate ordered_float;
extern crate parking_lot;
#[macro_use]
extern crate prettytable;
extern crate rayon;
extern crate regex;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;

extern crate buffered_index_writer;
pub extern crate doc_store;
extern crate json_converter;
extern crate levenshtein_automata;
#[macro_use]
extern crate measure_time;
extern crate parser;
extern crate term_hashmap;
extern crate vint;

#[macro_use]
pub mod util;
// pub mod stopwords;
#[macro_use]
pub mod type_info;
pub mod create;
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
pub mod shards;
pub mod tokenizer;
pub mod trace;

pub use self::search::search_field;
pub use self::search::search_field_result;

#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate tempfile;
#[cfg(test)]
extern crate test;
