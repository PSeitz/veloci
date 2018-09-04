
#[macro_use]
extern crate nom;

#[macro_use] extern crate lalrpop_util;

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





// fn from_hex(input: &str) -> Result<u8, std::num::ParseIntError> {
//   u8::from_str_radix(input, 16)
// }

// fn is_hex_digit(c: char) -> bool {
//   match c {
//     '0'..='9' | 'a'..='f' | 'A'..='F' => true,
//     _ => false,
//   }
// }

// named!(hex_primary<&str, u8>,
//   map_res!(take_while_m_n!(2, 2, is_hex_digit), from_hex)
// );


// named!(hex_color<&str, Ast>,
//   do_parse!(
//            tag!("#")   >>
//     red:   hex_primary >>
//     green: hex_primary >>
//     blue:  hex_primary >>
//     (Ast)
//   )
// );


// lalrpop_mod!(pub calculator1); // synthesized by LALRPOP
// mod calculator1;
// #[test]
// fn calculator1() {
//     assert!(calculator1::TermParser::new().parse("22").is_ok());
//     assert!(calculator1::TermParser::new().parse("(22)").is_ok());
//     assert!(calculator1::TermParser::new().parse("((((22))))").is_ok());
//     assert!(calculator1::TermParser::new().parse("((22)").is_err());
// }

pub mod calculator4;
pub mod ast;

#[test]
fn calculator4() {
    let expr = calculator4::ExprParser::new()
        .parse("22 * 44 + 66")
        .unwrap();
    assert_eq!(&format!("{:?}", expr), "((22 * 44) + 66)");
}

pub mod query_tree;

#[test]
fn single_term() {
    let expr = query_tree::ExprParser::new()
        .parse("fred")
        .unwrap();
    println!("{:?}", expr);
    assert_eq!(&format!("{:?}", expr), "Filter(\"fred\")");
}

#[test]
fn parenthized_term() {
    let expr = query_tree::ExprParser::new()
        .parse("(((fred)))")
        .unwrap();
    println!("{:?}", expr);
    assert_eq!(&format!("{:?}", expr), "Filter(\"fred\")");
}

#[test]
fn parse_or() {
    let expr = query_tree::ExprParser::new()
        .parse("(((fred OR bernd)))")
        .unwrap();
    println!("{:?}", expr);
    assert_eq!(&format!("{:?}", expr), "Or(Filter(\"fred\"), Filter(\"bernd\"))");
}
#[test]
fn parse_and() {
    let expr = query_tree::ExprParser::new()
        .parse("(((fred AND bernd OR other)))")
        .unwrap();
    println!("{:?}", expr);
    assert_eq!(&format!("{:?}", expr), "Or(And(Filter(\"fred\"), Filter(\"bernd\")), Filter(\"other\"))");
}