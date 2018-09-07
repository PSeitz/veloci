#![recursion_limit = "80"]
#[macro_use]
extern crate nom;
extern crate combine;

pub mod query_parser;

#[derive(Debug)]
pub enum Ast {
    Filter(String),
    Or(Vec<Ast>),
    And(Vec<Ast>),
}
