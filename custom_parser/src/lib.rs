//! A fast parser parsing querylanguage into an AST
#![warn(missing_debug_implementations,rust_2018_idioms)]
mod lexer;
pub mod parser;
pub mod ast;
pub mod error;

use serde::{Serialize, Deserialize};
pub use parser::parse;
pub use parser::parse_with_opt;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Options {
    /// This setting will disable parsing of the attribute specfier "attr:"
    /// e.g. "myfield:searchterm"
    pub no_attributes: bool,
    /// This setting will disable parsing of the parentheses
    /// e.g. "(nice)" - here the parentheses would be normally be part of the syntax and removed
    pub no_parentheses: bool,
    /// This setting will disable defining a levensthtein distance after a searchterm
    /// e.g. "searchterm~2"
    pub no_levensthein: bool,
    // pub no_quotes: bool
}
