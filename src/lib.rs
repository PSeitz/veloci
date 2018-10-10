#![cfg_attr(feature = "cargo-clippy", allow(implicit_hasher))]
#![cfg_attr(feature = "cargo-clippy", allow(too_many_arguments))]
#![feature(tool_lints)]
#![feature(drain_filter)]
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
#[macro_use]
#[allow(unused_imports)]
extern crate dump;
#[macro_use]
extern crate heapsize_derive;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prettytable;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
pub use doc_store;

#[macro_use]
extern crate measure_time;
#[macro_use]
extern crate failure;

#[macro_use]
pub mod util;
#[macro_use]
pub mod type_info;
pub mod create;
pub mod error;
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
extern crate test;
