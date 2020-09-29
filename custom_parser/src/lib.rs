/*!
A fast parser parsing querylanguage into an AST

A handwritten parser parsing a predefined syntax, with options to disable
some syntax.

# Syntax

## Phrases
"this won't be tokenized"

## Attributes
name:fred
title:(my booktitle)

## Parentheses
(cool AND nice) OR (thick and woke)

## Levenshtein Distance

The levenshtein edit distance for a token can be defined with ~
e.g. searchterm~2 means a edit distance of 2 for searchterm,
that means searchtuam would be a hit, because the edit distance is 2.

*/
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
