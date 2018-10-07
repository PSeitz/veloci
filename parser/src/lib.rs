#![cfg_attr(feature = "cargo-clippy", feature(tool_lints))]
#![cfg_attr(feature = "cargo-clippy", allow(clippy::unneeded_field_pattern))]
#![recursion_limit = "80"]
extern crate combine;
pub mod query_parser;

#[derive(Debug)]
pub enum Ast {
    Filter(String),
    Or(Vec<Ast>),
    And(Vec<Ast>),
}
