//! A fast parser parsing querylanguage into an AST
#![warn(missing_debug_implementations,rust_2018_idioms)]
mod lexer;
pub mod parser;
pub mod ast;
pub mod error;

pub use parser::parse;

#[derive(Debug, Clone, Copy, Default)]
pub struct Options {
    /// This setting will disable parsing of the attribute specfier "attr:"
    pub no_attributes: bool,
    /// This setting will disable parsing of the parentheses
    pub no_parentheses: bool,
    /// This setting will disable defining a levensthtein distance after a searchterm, e.g. "searchterm~2"
    pub no_levensthein: bool,
    // pub no_quotes: bool,
}
