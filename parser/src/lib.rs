#![recursion_limit = "80"]
extern crate combine;
pub mod query_parser;

#[derive(Debug)]
pub enum Ast {
    Filter(String),
    Or(Vec<Ast>),
    And(Vec<Ast>),
}
