#![warn(missing_debug_implementations, rust_2018_idioms, trivial_casts, trivial_numeric_casts)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::field_reassign_with_default)]
#![allow(clippy::comparison_chain)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::borrowed_box)]
#![allow(clippy::let_and_return)]
#![cfg_attr(all(feature = "unstable", test), feature(test))]

#[cfg(all(test, feature = "unstable"))]
extern crate test;

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

pub use doc_store;
pub use query_parser;
#[macro_use]
pub mod util;
#[macro_use]
pub mod type_info;

#[cfg(feature = "create")]
pub mod create;

#[allow(dead_code)]
pub mod directory;
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
pub mod steps;
pub mod tokenizer;
pub mod trace;

pub use self::search::{result, search_field};
