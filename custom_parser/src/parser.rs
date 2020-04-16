use crate::lexer::{Lexer, TokenType, TokenWithText};

use std::{convert::From, fmt};

#[derive(Clone, PartialEq, Eq)]
pub struct UserFilter {
    // pub field_name: Option<String>,
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
            write!(formatter, "\"{}\"~{:?}", self.phrase, levenshtein)
        } else {
            write!(formatter, "\"{}\"", self.phrase)
        }
    }
}

impl UserFilter {
    pub fn into_ast(self) -> UserAST {
        UserAST::Leaf(Box::new(self))
    }
}

// conversion for tests
impl From<&'static str> for UserAST {
    fn from(item: &str) -> Self {
        let mut filter = UserFilter {
            phrase: item.to_string(),
            levenshtein: None,
        };
        if item.chars().next().map(|c|c!='\"').unwrap_or(false){
            let parts_field = item.splitn(2, ':').collect::<Vec<_>>();
            if parts_field.len() > 1 {
                // filter.field_name = Some(parts_field[0].to_string());
                filter.phrase = parts_field[1].to_string();
            }

            let parts_leven: Vec<String> = filter.phrase.splitn(2, '~').map(|el| el.to_string()).collect::<Vec<_>>();
            if parts_leven.len() > 1 {
                filter.phrase = parts_leven[0].to_string();
                filter.levenshtein = Some(parts_leven[1].parse().unwrap());
            }

            if parts_field.len() > 1 {
                return UserAST::Attributed(parts_field[0].to_string(), Box::new(UserAST::Leaf(Box::new(filter))))
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
impl fmt::Display for Operator {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            Operator::Or => write!(formatter, "OR"),
            Operator::And => write!(formatter, "AND"),
        }
        
    }
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

#[derive(Clone, PartialEq, Eq)]
pub enum UserAST {
    // Grouped(Box<UserAST>, FilterDef),
    // Paren(Box<UserAST>),
    Attributed(String, Box<UserAST>),
    BinaryClause(Box<UserAST>, Operator, Box<UserAST>),
    // Clause(Operator, Vec<UserAST>),
    Leaf(Box<UserFilter>),
    // Noop,
}

impl fmt::Debug for UserAST {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            UserAST::Attributed(attr, ast) => {
                write!(formatter, "{}:{:?}", attr, ast)
            },
            UserAST::BinaryClause(ast1, op, ast2) => {
                write!(formatter, "({:?} {} {:?})", ast1, op, ast2)
            },
            UserAST::Leaf(filter) => {
                write!(formatter, "{:?}", filter)
            },
        }
    }
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

#[derive(Debug, Eq, PartialEq)]
pub enum ParseError {
    EmptyParentheses(String),
    UnexpectedTokenType(String),
    ExpectedNumber(String),
}

#[derive(Debug)]
pub struct Parser {
    tokens: Vec<TokenWithText>,
    pos: usize,
}

// macro_rules! return_binary_clause {
//     ($self: ident, $operator: expr, $curr_token: ident) => {
//         return Ok(UserAST::BinaryClause(Box::new(UserAST::Leaf(Box::new($curr_token))), $operator, Box::new($self._parse()?)));
//     };
// }
macro_rules! return_binary_clause {
    ($self: ident, $operator: expr, $curr_token: ident) => {
        return Ok(UserAST::BinaryClause(Box::new($curr_token), $operator, Box::new($self._parse()?)));
    };
}
// fn token_type_to_operator(token_type: TokenType) -> Option<Operator> {
//     match token_type {
//         TokenType::Literal | TokenType::Or => Some(Operator::Or),
//         TokenType::And => Some(Operator::And),
//         _ => None,
//     }
// }

impl Parser {
    pub fn new(text: &str) -> Self {
        Parser {
            tokens: Lexer::new(text).get_tokens_with_text(),
            pos: 0,
        }
    }

    pub fn unexpected_token_type(&self, pos: usize, message: &'static str, allowed_types: Option<&[Option<TokenType>]>) -> Result<(), ParseError> {
        let err = ParseError::UnexpectedTokenType(format!(
            "{} Unexpected token_type at position {:?}, got {}{:?}",
            message,
            pos,
            self.tokens.get(pos).map(|el|format!("{:?}", el)).unwrap_or_else(|| "EOF".to_string()) ,
            allowed_types.map(|el| format!(" allowed_types: {:?}", el)).unwrap_or_else(||"".to_string())
        ));
        return Err(err);
    }

    fn assert_allowed_types(&self, pos: usize, message: &'static str, allowed_types: &[Option<TokenType>]) -> Result<(), ParseError> {
        if !allowed_types.contains(&self.get_type(pos)){
            self.unexpected_token_type(pos, message, Some(allowed_types))?;
        }
        Ok(())
    }

    pub fn parse(text: &str) -> Result<UserAST, ParseError> {
        Parser::new(text)._parse()
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

    fn try_parse_user_filter(&mut self) -> Result<Option<UserFilter>, ParseError> {

        self.tokens.get(self.pos).cloned().map(|curr_token|{
            let mut curr_ast = UserFilter {
                levenshtein: None,
                phrase: curr_token.matched_text.to_string(),
            };

            // Optional: Define Levenshtein distance
            if self.is_type(self.pos + 1, TokenType::Tilde) {
                let levenshtein: u8 = self.parse_after_tilde(self.pos + 2)?;
                curr_ast.levenshtein = Some(levenshtein);

                self.pos += 2; // e.g. House~3 -> tokens [~], [3]
            }
            Ok(curr_ast)
        }).transpose()
    }

    fn parse_sub_expression(&mut self, curr_ast: UserAST) -> Result<UserAST, ParseError> {
        self.assert_allowed_types(self.pos, "", &[Some(TokenType::Literal), Some(TokenType::ParenthesesOpen), Some(TokenType::ParenthesesClose), Some(TokenType::And), Some(TokenType::Or)])?;
        
        if let Some(next_token_type) = self.get_type(self.pos + 1) {

            match next_token_type {
                TokenType::Literal => {
                    self.pos += 1;
                    return_binary_clause!(self, Operator::Or, curr_ast);
                }
                TokenType::Or => {
                    self.pos += 2;
                    return_binary_clause!(self, Operator::Or, curr_ast);
                }
                TokenType::And => {
                    self.pos += 2;
                    return_binary_clause!(self, Operator::And, curr_ast);
                }
                TokenType::AttributeLiteral | TokenType::ParenthesesOpen | TokenType::Tilde => {
                    unimplemented!()
                }
                TokenType::ParenthesesClose => {return Ok(curr_ast)},

            }

        }else{
            return Ok(curr_ast); // is last one
        }

    }

    fn _parse(&mut self) -> Result<UserAST, ParseError> {
        if let Some(curr_token) = self.tokens.get(self.pos).cloned() {

            match curr_token.token.token_type {
                TokenType::AttributeLiteral => {
                    // self.assert_allowed_types(self.pos + 1, "only token or ( allowed after attribute, attr: ", &[Some(TokenType::Literal), Some(TokenType::ParenthesesOpen)])?;
                    self.pos += 1;
                    //Check if attribute covers whole ast or only next literal

                    match self.get_type(self.pos) {
                        Some(TokenType::ParenthesesOpen) => return Ok(UserAST::Attributed(curr_token.matched_text.to_string(), Box::new(self._parse()?))),
                        Some(TokenType::Literal) => {
                            let curr_ast = self.try_parse_user_filter()?.unwrap();
                            let attributed_ast = UserAST::Attributed(curr_token.matched_text.to_string(), Box::new(UserAST::Leaf(Box::new(curr_ast))));
                            return self.parse_sub_expression(attributed_ast);
                        },
                        _=> self.unexpected_token_type(self.pos, "only token or ( allowed after attribute, attr: ", Some(&[Some(TokenType::Literal), Some(TokenType::ParenthesesOpen)]))?
                    };

                    // if self.get_type(self.pos) == Some(TokenType::ParenthesesOpen) {
                    //     return Ok(UserAST::Attributed(curr_token.matched_text.to_string(), Box::new(self._parse()?)));
                    // }else if self.get_type(self.pos) == Some(TokenType::Literal){
                    //     let curr_ast = self.try_parse_user_filter()?.unwrap();
                    //     let attributed_ast = UserAST::Attributed(curr_token.matched_text.to_string(), Box::new(UserAST::Leaf(Box::new(curr_ast))));
                    //     return self.parse_sub_expression(attributed_ast);
                    // }else{
                    //     self.unexpected_token_type(self.pos, "only token or ( allowed after attribute, attr: ", Some(&[Some(TokenType::Literal), Some(TokenType::ParenthesesOpen)]))?;
                    // }

                    // if let Some(curr_ast) = self.try_parse_user_filter()? {
                    //     let attributed_ast = UserAST::Attributed(curr_token.matched_text.to_string(), Box::new(UserAST::Leaf(Box::new(curr_ast))));
                    //     return self.parse_sub_expression(attributed_ast);
                    // }else{ // parentheses open
                    //     println!("{:?}", curr_token.matched_text);
                    //     return Ok(UserAST::Attributed(curr_token.matched_text.to_string(), Box::new(self._parse()?)));
                    // }
                }
                TokenType::Literal => {
                    let curr_ast = self.try_parse_user_filter()?.unwrap();
                    return self.parse_sub_expression(UserAST::Leaf(Box::new(curr_ast)));
                }

                TokenType::ParenthesesOpen => {
                    self.pos += 1;

                    let parenthesed_ast = self._parse()?;
                    self.assert_allowed_types(self.pos + 1, "", &[Some(TokenType::ParenthesesClose)])?;
                    self.pos += 1;
                    return self.parse_sub_expression(parenthesed_ast);
                    
                },
                TokenType::ParenthesesClose => unimplemented!(),
                TokenType::Tilde => {
                    self.unexpected_token_type(self.pos, "" , None)?; // IMPOSSIBURU!, should be covered by lookeaheads
                },

                TokenType::Or | TokenType::And => {
                    unimplemented!() // IMPOSSIBURU!, should be covered by lookeaheads
                }
            }
        }
        unreachable!()
        // return Ok(UserAST::Noop);
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
    use crate::parser::ParseError::*;
    use crate::parser::{Operator::*};

    #[test]
    fn simple() {
        assert_eq!(
            Parser::parse("hallo").unwrap(),
            "hallo".into()
        );
    }
    #[test]
    fn test_invalid() {
        assert_eq!(
            Parser::parse("field:what:ok").is_err(),
            true
            // Err(UnexpectedTokenType("Expecting a levenshtein number after a \'~\' at position 2, but got None".to_string()))
        );
    }

    #[test]
    fn test_phrases() {
        assert_eq!(
            Parser::parse("\"cool\")").unwrap(),
            ("cool".into())
        );
        assert_eq!(
            Parser::parse("\"cooles teil\")").unwrap(),
            ("cooles teil".into())
        );
    }

    #[test]
    fn test_parentheses() {
        assert_eq!(
            Parser::parse("(cool)").unwrap(),
            ("cool".into())
        );

        assert_eq!(
            Parser::parse("((((((cool))))))").unwrap(),
            ("cool".into())
        );
        assert_eq!(
            Parser::parse("((((((cool)))))) AND ((((((cool))))))").unwrap(),
            ("cool".into(), And, "cool".into()).into()
        );
        assert_eq!(
            Parser::parse("(super AND cool) OR fancy").unwrap(),
            ((("super".into(), And, "cool".into()).into()), Or, "fancy".into()).into()
        );
        assert_eq!(
            Parser::parse("(super AND cool) OR (fancy)").unwrap(),
            ((("super".into(), And, "cool".into()).into()), Or, "fancy".into()).into()
        );
        assert_eq!(
            Parser::parse("((super AND cool)) OR (fancy)").unwrap(),
            ((("super".into(), And, "cool".into()).into()), Or, "fancy".into()).into()
        );
        // println!("{:?}", Parser::parse("(cool)"));
    }
    #[test]
    fn test_and_or() {
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
                // field_name: None,
                phrase: "fancy".to_string(),
                levenshtein: Some(1),
            }))
        );
        assert_eq!(
            Parser::parse("fancy~"),
            Err(UnexpectedTokenType("Expecting a levenshtein number after a \'~\' at position 2, but got None".to_string()))
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
    fn test_attribute_and_levenshtein() {
        assert_eq!(
            Parser::parse("field:fancy~1").unwrap(),
            UserAST::Attributed("field".into(), Box::new(UserAST::Leaf(Box::new(UserFilter{
                phrase: "fancy".to_string(),
                levenshtein: Some(1),
            }))))
        );
    }

    #[test]
    fn test_attribute() {
        assert_eq!(
            Parser::parse("\"field\":fancy unlimited").unwrap(),
            (UserAST::Attributed("field".to_string(), "fancy".into()), Or, "unlimited".into()).into()
        );
        assert_eq!(
            Parser::parse("field:fancy unlimited").unwrap(),
            (UserAST::Attributed("field".to_string(), "fancy".into()), Or, "unlimited".into()).into()
        );
        assert_eq!(
            Parser::parse("fancy:"),
            Err(UnexpectedTokenType("only token or ( allowed after attribute, attr:  Unexpected token_type at position 1, got EOF\" allowed_types: [Some(Literal), Some(ParenthesesOpen)]\"".to_string()))
            // Err(UnexpectedTokenType("Expecting a levenshtein number after a \'~\' at position 2, but got None".to_string()))
        );
        assert_eq!(
            Parser::parse("field:fancy").unwrap(),
            UserAST::Attributed("field".into(), Box::new(UserAST::Leaf(Box::new(UserFilter{
                phrase: "fancy".to_string(),
                levenshtein: None,
            }))))
        );

        assert_eq!(
            Parser::parse("field:fancy").unwrap(),
            "field:fancy".into()
        );
        assert_eq!(
            Parser::parse("field:fancy~1").unwrap(),
            UserAST::Attributed("field".into(), Box::new(UserAST::Leaf(Box::new(UserFilter{
                phrase: "fancy".to_string(),
                levenshtein: Some(1),
            }))))
        );
        assert_eq!(
            Parser::parse("field:fancy~1").unwrap(),
            "field:fancy~1".into()
        );

    }

    #[test]
    fn test_attributed_block_1() {
        assert_eq!(
            Parser::parse("field:(fancy unlimited)").unwrap(),
            UserAST::Attributed("field".to_string(), Box::new(("fancy".into(), Or, "unlimited".into()).into()) )
        );
    }

    fn test_parse_query_to_ast_helper(query: &str, expected: &str) {
        let query_str = Parser::parse(query).unwrap();
        assert_eq!(format!("{:?}", query_str), expected);
    }

    // #[test]
    // fn test_attributed_block() {
    //     test_parse_query_to_ast_helper("field:(fancy unlimited)", "(field:\"(\" OR (\"fancy\" OR \"unlimited\"))");
    // }

    #[test]
    fn test_multi_spaces() {
        test_parse_query_to_ast_helper("a AND  b", "(\"a\" AND \"b\")");
    }

    #[test]
    fn test_special_chars() {
        test_parse_query_to_ast_helper("die drei ???", "(\"die\" OR (\"drei\" OR \"???\"))");
        test_parse_query_to_ast_helper("a+", "\"a+\"");
    }

    #[test]
    fn test_multi_and_to_flat() {
        test_parse_query_to_ast_helper("a AND b AND c", "(\"a\" AND (\"b\" AND \"c\"))"); // not flat
    }

    #[test]
    fn test_multi_or_to_flat() {
        test_parse_query_to_ast_helper("a OR b OR c", "(\"a\" OR (\"b\" OR \"c\"))"); // not flat
    }

    #[test]
    fn test_parse_query() {
        test_parse_query_to_ast_helper("a AND b", "(\"a\" AND \"b\")");
        test_parse_query_to_ast_helper("a:b", "a:\"b\"");
        test_parse_query_to_ast_helper("a:b OR c", "(a:\"b\" OR \"c\")");
        test_parse_query_to_ast_helper("a", "\"a\"");
        test_parse_query_to_ast_helper("食べる AND b", "(\"食べる\" AND \"b\")");

        //no precendence yet
        test_parse_query_to_ast_helper("a OR b AND c", "(\"a\" OR (\"b\" AND \"c\"))");
    }

    #[test]
    fn test_parse_multi_literals() {
        test_parse_query_to_ast_helper("a b", "(\"a\" OR \"b\")");
        test_parse_query_to_ast_helper("\"a b\"", "\"a b\"");
        test_parse_query_to_ast_helper("feld:10 b", "(feld:\"10\" OR \"b\")");
    }

}
