#![warn(missing_debug_implementations, rust_2018_idioms, trivial_casts, trivial_numeric_casts)]
#![cfg_attr(feature = "cargo-clippy", allow(clippy::implicit_hasher))]
#![cfg_attr(feature = "cargo-clippy", allow(clippy::too_many_arguments))]
#![feature(drain_filter)]
#![feature(min_specialization)]
#![feature(test)]
#![cfg_attr(feature = "unstable", feature(alloc, heap_api, repr_simd))]
#![feature(core_intrinsics)]

#[cfg(feature = "enable_cpuprofiler")]
extern crate cpuprofiler;
#[macro_use]
#[allow(unused_imports)]
extern crate dump;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prettytable;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;

#[macro_use]
extern crate measure_time;
#[macro_use]
extern crate failure;

pub use query_parser;
pub use doc_store;
#[macro_use]
pub mod util;
#[macro_use]
pub mod type_info;

#[cfg(feature = "create")]
pub mod create;

pub mod error;
pub mod expression;
pub mod facet;
pub mod highlight_field;
pub mod indices;
pub mod metadata;
pub mod persistence;
pub mod plan_creator;
pub mod query_generator;
pub mod search;
pub mod shards;
pub mod steps;
pub mod tokenizer;
pub mod trace;

pub use self::search::{result, search_field};

#[cfg(test)]
extern crate test;
