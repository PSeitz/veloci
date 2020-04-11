use crate::lexer::{Lexer, TokenType, TokenWithText};

use std::{convert::From, fmt};

#[derive(Clone, PartialEq, Eq)]
pub struct UserFilter {
    pub field_name: Option<String>,
    pub phrase: String,
    pub levenshtein: Option<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterDef {
    pub field_name: Option<String>,
    pub levenshtein: Option<u8>,
}

impl fmt::Debug for UserFilter {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if let Some(levenshtein) = self.levenshtein {
            match self.field_name {
                Some(ref field_name) => write!(formatter, "{}:\"{}\"~{:?}", field_name, self.phrase, levenshtein),
                None => write!(formatter, "\"{}\"~{:?}", self.phrase, levenshtein),
            }
        } else {
            match self.field_name {
                Some(ref field_name) => write!(formatter, "{}:\"{}\"", field_name, self.phrase),
                None => write!(formatter, "\"{}\"", self.phrase),
            }
        }
    }
}

impl UserFilter {
    pub fn into_ast(self) -> UserAST {
        UserAST::Leaf(Box::new(self))
    }
}

impl From<&'static str> for UserAST {
    fn from(item: &str) -> Self {
        let mut filter = UserFilter {
            field_name: None,
            phrase: item.to_string(),
            levenshtein: None,
        };
        if item.chars().next().map(|c|c!='\"').unwrap_or(false){
            let parts_field = item.splitn(2, ':').collect::<Vec<_>>();
            if parts_field.len() > 1 {
                filter.field_name = Some(parts_field[0].to_string());
                filter.phrase = parts_field[1].to_string();
            }

            let parts_leven: Vec<String> = filter.phrase.splitn(2, '~').map(|el| el.to_string()).collect::<Vec<_>>();
            if parts_leven.len() > 1 {
                filter.phrase = parts_leven[0].to_string();
                filter.levenshtein = Some(parts_leven[1].parse().unwrap());
            }

        }
        UserAST::Leaf(Box::new(filter))
    }
}
impl From<&'static str> for Box<UserAST> {
    fn from(item: &'static str) -> Self {
        Box::new(item.into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Or,
    And,
}
impl Operator {
    fn to_string(self) -> &'static str {
        match self {
            Operator::Or => " OR ",
            Operator::And => " AND ",
        }
    }
}

impl From<&str> for Operator {
    fn from(item: &str) -> Self {
        if item == "OR" {
            return Operator::Or;
        }
        if item == "AND" {
            return Operator::And;
        }
        panic!("could not convert {:?} to operator", item);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserAST {
    Grouped(Box<UserAST>, FilterDef),
    Attributed(String, Box<UserAST>),
    BinaryClause(Box<UserAST>, Operator, Box<UserAST>),
    Clause(Operator, Vec<UserAST>),
    Leaf(Box<UserFilter>),
    Noop,
}

// impl From<&[&str;3]> for UserAST {
//     fn from(item: &[&str;3]) -> Self {
//         UserAST::BinaryClause(item[0].into(), item[1].into(), item[2].into())
//     }
// }
// impl From<(&'static str, Operator, &'static str)> for UserAST {
//     fn from(item: (&'static str, Operator, &'static str)) -> Self {
//         UserAST::BinaryClause(item.0.into(), item.1, item.2.into())
//     }
// }
impl From<(UserAST, Operator, UserAST)> for UserAST {
    fn from(item: (UserAST, Operator, UserAST)) -> Self {
        UserAST::BinaryClause(Box::new(item.0), item.1, Box::new(item.2))
    }
}

#[derive(Debug)]
enum ParseError {
    UnexpectedTokenType(String),
    ExpectedNumber(String),
}

#[derive(Debug)]
struct Parser {
    tokens: Vec<TokenWithText>,
    pos: usize,
}

macro_rules! return_binary_clause {
    ($self: ident, $operator: expr, $curr_token: ident) => {
        return Ok(UserAST::BinaryClause(Box::new(UserAST::Leaf(Box::new($curr_token))), $operator, Box::new($self._parse()?)));
    };
}

impl Parser {
    fn new(text: &str) -> Self {
        Parser {
            tokens: Lexer::new(text).get_tokens_with_text(),
            pos: 0,
        }
    }

    pub fn parse(text: &str) -> Result<UserAST, ParseError> {
        Parser::new(text)._parse()
        // let mut lexer = Lexer::new(text);
        // let tokens = lexer.get_tokens();
        // _parse(&mut 0, &tokens, &lexer).unwrap()
    }

    fn parse_after_tilde(&self, pos: usize) -> Result<u8, ParseError> {
        if !self.is_type(pos, TokenType::Literal) {
            let err = ParseError::UnexpectedTokenType(format!(
                "Expecting a levenshtein number after a '~' at position {:?}, but got {:?}",
                pos,
                self.tokens.get(pos)
            ));
            return Err(err);
        }

        let levenshtein: u8 = self
            .tokens
            .get(pos)
            .map(|t| &t.matched_text)
            .unwrap() // already checked above
            .parse()
            .map_err(|_e| ParseError::ExpectedNumber(format!("Expected number after tilde to define leventhsein distance but got {:?}", self.tokens.get(pos))))?;

        Ok(levenshtein)
    }

    fn _parse(&mut self) -> Result<UserAST, ParseError> {
        while let Some(next_token) = self.tokens.get(self.pos) {
            // let next_token = &tokens[pos];
            println!("{:?}", next_token.token.token_type);
            match next_token.token.token_type {
                // Unlimited length
                TokenType::Literal => {
                    if self.is_type(self.pos + 1, TokenType::Colon) {
                        // check is attributedef
                        self.pos += 2;
                        return Ok(UserAST::Attributed(next_token.matched_text.to_string(), Box::new(self._parse()?)));
                    }

                    // If it's not an attribute it's an leaf
                    let mut curr_token = UserFilter {
                        field_name: None,
                        levenshtein: None,
                        phrase: next_token.matched_text.to_string(),
                    };

                    // Lookahead to check if it is a leaf, and, or, parentheses, Tilde, Colon //

                    // Define Levenshtein distance
                    if self.is_type(self.pos + 1, TokenType::Tilde) {
                        let levenshtein: u8 = self.parse_after_tilde(self.pos + 2)?;
                        curr_token.levenshtein = Some(levenshtein);

                        self.pos += 2; // e.g. House~3 -> tokens [~], [3]
                    }

                    // Next is Literal - Implicit Or
                    if self.is_type(self.pos + 1, TokenType::Literal) {
                        self.pos += 1;
                        return_binary_clause!(self, Operator::Or, curr_token)
                    }

                    // Next is Explicit Or
                    if self.is_type(self.pos + 1, TokenType::Or) {
                        self.pos += 2;
                        return_binary_clause!(self, Operator::Or, curr_token)
                    }

                    // Next is And
                    if self.is_type(self.pos + 1, TokenType::And) {
                        self.pos += 2;
                        return_binary_clause!(self, Operator::And, curr_token)
                    }

                    // This is the end
                    if self.pos + 1 == self.tokens.len() {
                        return Ok(UserAST::Leaf(Box::new(curr_token)));
                    }
                    unimplemented!()

                    // while tokens.get(pos+1).map(|tok|tok.token_type == Literal).unwrap_or(false) {
                    //     pos+=1;
                    // }
                }

                // // Grouped
                // WhiteSpaces => {
                // },
                TokenType::ParenthesesOpen => unimplemented!(),
                TokenType::ParenthesesClose => unimplemented!(),
                TokenType::Tilde => unimplemented!(),
                TokenType::Colon => unimplemented!(),

                TokenType::Or => {
                    unimplemented!() // IMPOSSIBURU!
                }

                TokenType::And => {
                    unimplemented!() // IMPOSSIBURU!
                }
            }
        }
        return Ok(UserAST::Noop);
    }

    fn is_type(&self, pos: usize, token_type: TokenType) -> bool {
        self.get_type(pos).map(|el| el == token_type).unwrap_or(false)
    }

    fn get_type(&self, pos: usize) -> Option<TokenType> {
        self.tokens.get(pos).map(|el| el.token.token_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{Operator::*};

    #[test]
    fn test_and_or() {
        // parse("super cool fancy");
        // println!("asd {:?}", Parser::parse("super AND cool OR fancy")); // these are not the same
        assert_eq!(
            Parser::parse("super AND cool OR fancy").unwrap(),
            ("super".into(), And, ("cool".into(), Or, "fancy".into()).into()).into() // BinaryClause("super".into(), And, Box::new(("cool", Operator::Or, "fancy").into()))
        );
        // println!("asd {:?}", Parser::parse("super OR cool AND fancy"));

        assert_eq!(
            Parser::parse("super OR cool AND fancy").unwrap(),
            ("super".into(), Or, ("cool".into(), And, "fancy".into()).into()).into()
        );
    }

    #[test]
    fn test_implicit_or() {
        assert_eq!(
            Parser::parse("super cool OR fancy").unwrap(),
            ("super".into(), Or, ("cool".into(), Or, "fancy".into()).into()).into()
        );
        assert_eq!(
            Parser::parse("super cool").unwrap(),
            ("super".into(), Or, "cool".into()).into()
        );
        assert_eq!(
            Parser::parse("super cool").unwrap(),
            Parser::parse("super OR cool").unwrap()
        );
    }

    #[test]
    fn test_levenshtein() {
        assert_eq!(
            Parser::parse("fancy~1").unwrap(),
            UserAST::Leaf(Box::new(UserFilter{
                field_name: None,
                phrase: "fancy".to_string(),
                levenshtein: Some(1),
            }))
        );
        assert_eq!(
            Parser::parse("fancy~1").unwrap(),
            "fancy~1".into()
        );
        assert_eq!(
            Parser::parse("super cool OR fancy~1").unwrap(),
            ("super".into(), Or, ("cool".into(), Or, "fancy~1".into()).into()).into()
        );
    }

    #[test]
    fn test_attribute() {
        // assert_eq!(
        //     Parser::parse("field:fancy").unwrap(),
        //     UserAST::Leaf(Box::new(UserFilter{
        //         field_name: Some("field".to_string()),
        //         phrase: "fancy".to_string(),
        //         levenshtein: None,
        //     }))
        // );
        // assert_eq!(
        //     Parser::parse("field:fancy~1").unwrap(),
        //     UserAST::Leaf(Box::new(UserFilter{
        //         field_name: Some("field".to_string()),
        //         phrase: "fancy".to_string(),
        //         levenshtein: Some(1),
        //     }))
        // );

    }
}
