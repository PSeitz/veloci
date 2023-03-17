#![warn(missing_debug_implementations, rust_2018_idioms, trivial_casts, trivial_numeric_casts)]
#![cfg_attr(feature = "cargo-clippy", allow(clippy::too_many_arguments))]
#![cfg_attr(feature = "cargo-clippy", allow(clippy::field_reassign_with_default))]
#![cfg_attr(feature = "cargo-clippy", allow(clippy::comparison_chain))]
#![cfg_attr(feature = "cargo-clippy", allow(clippy::large_enum_variant))]
#![cfg_attr(all(feature = "unstable", test), feature(test))]

#[cfg(all(test, feature = "unstable"))]
extern crate test;

#[cfg(feature = "enable_cpuprofiler")]
extern crate cpuprofiler;
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

pub use doc_store;
pub use query_parser;
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
