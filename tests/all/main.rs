#![recursion_limit = "128"]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate serde_json;

#[macro_use]
extern crate more_asserts;

#[macro_use]
mod common;
mod test_code_search;
mod test_phrase;
mod test_query_generator;
mod test_scores;
mod test_why_found;
mod tests;
mod tests_facet;
mod tests_large;
mod tests_minimal;
