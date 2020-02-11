#![recursion_limit = "1024"]

#[macro_use]
extern crate pest_derive;

#[macro_use]
extern crate pest_ast;

extern crate from_pest;
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


// mod ast {
//     use super::Rule;
//     use pest::Span;


//     #[derive(Debug, FromPest)]
//     #[pest_ast(rule(Rule::quoted_string))]
//     pub struct SearchTerm {
//         pub term: String,
//     }

// }


// fn parse_query(query: String) {
//     let mut parse_result = ExprParser::parse(Rule::expr, &query).unwrap().next().unwrap().into_inner();

//     let first_block = parse_result.next().unwrap();
//     match first_block.as_rule() {
//         Rule::paren_bool => {
//             // let mut inner_rules = line.into_inner(); // { name }
//             // current_section_name = inner_rules.next().unwrap().as_str();
//         }
//         Rule::query_part => {
//             // let mut inner_rules = line.into_inner(); // { name ~ "=" ~ value }

//             // let name: &str = inner_rules.next().unwrap().as_str();
//             // let value: &str = inner_rules.next().unwrap().as_str();

//             // // Insert an empty inner hash map if the outer hash map hasn't
//             // // seen this section name before.
//             // let section = properties.entry(current_section_name).or_default();
//             // section.insert(name, value);
//         }
//         // Rule::EOI => (),
//         _ => unreachable!(),
//     }

// }

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




