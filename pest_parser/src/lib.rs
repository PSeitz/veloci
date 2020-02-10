#![recursion_limit = "1024"]

#[macro_use]
extern crate pest_derive;

// use pest::prec_climber::{Assoc, Operator, PrecClimber};

//use pest::error::Error;
use pest::Parser;

#[derive(Parser)]
#[grammar = "grammar.pest"]
struct ExprParser;

/// error occurred when parsing user input
#[derive(Debug)]
pub struct ParseError {
    pub location: pest::error::InputLocation,
    pub expected: String,
}


#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! get_parsed_parts {
        ( $parse_result:ident ) => {
            {
                let mut tokens = vec![];
                let iter = $parse_result.next().unwrap();
                if(iter.as_str() != ""){
                    tokens.push(iter.as_str());
                }
                tokens.extend(iter.into_inner().map(|el| el.as_str()));
                tokens
            }
        };
    }

    #[test]
    fn it_works() {
        let mut parse_result = ExprParser::parse(Rule::query_part, "hal:lo").unwrap();
        let parts = get_parsed_parts!(parse_result);
        println!("{:?}", parts);
        assert_eq!(parts, &["hal:lo", "hal", "lo"]);
    }

    #[test]
    fn quoted() {
        let mut parse_result = ExprParser::parse(Rule::quoted_string, "\"nice\"").unwrap();
        let parts = get_parsed_parts!(parse_result);
        println!("{:?}", parts);
        assert_eq!(parts, &["\"nice\"", "nice"]);
    }
}




