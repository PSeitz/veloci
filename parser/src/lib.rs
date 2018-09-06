#![recursion_limit = "80"]
#[macro_use]
extern crate nom;
extern crate combine;

#[macro_use]
extern crate lalrpop_util;

mod query_parser;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

#[derive(Debug)]
pub enum Ast {
    Filter(String),
    Or(Vec<Ast>),
    And(Vec<Ast>),
}

// named!(parens, delimited!(char!('('), is_not!(")"), char!(')')));
named!(parens<&str, &str>, delimited!(char!('('), is_not!(")"), char!(')')));

#[derive(Debug, PartialEq)]
pub struct Color {
    pub red: u8,
    pub green: u8,
    pub blue: u8,
}

fn from_hex(input: &str) -> Result<u8, std::num::ParseIntError> {
    u8::from_str_radix(input, 16)
}

fn is_hex_digit(c: char) -> bool {
    match c {
        '0'..='9' | 'a'..='f' | 'A'..='F' => true,
        _ => false,
    }
}

named!(hex_primary<&str, u8>,
    map_res!(take_while_m_n!(2, 2, is_hex_digit), from_hex)
);

named!(hex_color<&str, Color>,
    do_parse!(
               tag!("#")   >>
        red:   hex_primary >>
        green: hex_primary >>
        blue:  hex_primary >>
        (Color{red, green, blue})
    )
);

named!(get_greeting<&str,&str>,
    tag_s!("hi")
);

named!(get_and_left<&str,&str>,
    tag_s!(" AND")
);
named!(get_term<&str,Option<&str>>,
    opt!(nom::alphanumeric)
);

named!(get_field<&str, (&str,&str)>,
    pair!(
        nom::alphanumeric,
        tag_s!(":")
    )
);
// named!(get_term<&str, &str>,
//     pair!(
//         nom::alphanumeric,
//         tag_s!(":")
//     )
// );

named!(get_term_field<&str, (&str,&str,&str)>,
    tuple!(
        nom::alphanumeric,
        tag!(":"),
        nom::alphanumeric
    )
);

named!(get_term_with_opt_field<&str, &str>,
    alt!(
        recognize!(separated_pair!(nom::alphanumeric,char!(':'),nom::alphanumeric))|
        nom::alphanumeric
    )
);

#[test]
fn test_nom() {
    println!("{:?}", get_term_with_opt_field("author:fred "));
    println!("{:?}", get_term_with_opt_field("fred "));
    println!("{:?}", get_field("author:").unwrap());
    println!("{:?}", get_term("user AND stuff"));
    println!("{:?}", get_greeting("hi there"));
    println!("{:?}", parens("((2F14DF))"));
    println!("{:?}", hex_color("#2F14DF"));
    assert_eq!(hex_color("#2F14DF"), Ok(("", Color { red: 47, green: 20, blue: 223 })));
}

// lalrpop_mod!(pub calculator1); // synthesized by LALRPOP
// mod calculator1;
// #[test]
// fn calculator1() {
//     assert!(calculator1::TermParser::new().parse("22").is_ok());
//     assert!(calculator1::TermParser::new().parse("(22)").is_ok());
//     assert!(calculator1::TermParser::new().parse("((((22))))").is_ok());
//     assert!(calculator1::TermParser::new().parse("((22)").is_err());
// }

// pub mod calculator4;
// pub mod ast;

// #[test]
// fn calculator4() {
//     let expr = calculator4::ExprParser::new().parse("22 * 44 + 66").unwrap();
//     assert_eq!(&format!("{:?}", expr), "((22 * 44) + 66)");
// }

// pub mod query_tree;

// #[test]
// fn single_term() {
//     let expr = query_tree::ExprParser::new().parse("fred").unwrap();
//     println!("{:?}", expr);
//     assert_eq!(&format!("{:?}", expr), "Filter(\"fred\")");
// }

// #[test]
// fn parenthized_term() {
//     let expr = query_tree::ExprParser::new().parse("fred").unwrap();
//     println!("{:?}", expr);
//     assert_eq!(&format!("{:?}", expr), "Filter(\"fred\")");
// }

// #[test]
// fn parse_or() {
//     let expr = query_tree::ExprParser::new().parse("fred OR bernd").unwrap();
//     println!("{:?}", expr);
//     assert_eq!(&format!("{:?}", expr), "Or(Filter(\"fred\"), Filter(\"bernd\"))");
// }
// #[test]
// fn parse_and() {
//     let expr = query_tree::ExprParser::new().parse("fred AND bernd OR other").unwrap();
//     println!("{:?}", expr);
//     assert_eq!(&format!("{:?}", expr), "Or(And(Filter(\"fred\"), Filter(\"bernd\")), Filter(\"other\"))");
// }

// // #[test]
// // fn parse_and_precendence() {
// //     let expr = query_tree::ExprParser::new().parse("fred AND (bernd OR other)").unwrap();
// //     println!("{:?}", expr);
// //     assert_eq!(&format!("{:?}", expr), "Or(And(Filter(\"fred\"), Filter(\"bernd\")), Filter(\"other\"))");
// // }

// #[test]
// fn two_term() {
//     let expr = query_tree::ExprParser::new().parse("die erbin").unwrap();
//     println!("{:?}", expr);
//     assert_eq!(&format!("{:?}", expr), "Filter(\"die erbin\")");
// }

// #[test]
// fn two_terms() {
//     let expr = query_tree::ExprParser::new().parse("die AND erbin").unwrap();
//     println!("{:?}", expr);
//     assert_eq!(&format!("{:?}", expr), "And(Filter(\"die\"), Filter(\"erbin\"))");
// }

// #[test]
// fn twos_terms() {
//     let expr = query_tree::ExprParser::new().parse("die-erbin").unwrap();
//     println!("{:?}", expr);
//     assert_eq!(&format!("{:?}", expr), "Filter(\"die-erbin\")");
//     assert_eq!(&format!("{:?}", query_tree::ExprParser::new().parse("(die-erbin)").unwrap()), "Filter(\"(die-erbin)\")");
// }
