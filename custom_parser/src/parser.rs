use crate::error::marked_in_orig;
use crate::error::ParseError;
use crate::Options;
use crate::ast::*;
use crate::lexer::{Lexer, TokenType, Token};



#[derive(Debug)]
pub struct Parser<'a> {
    text: &'a str,
    tokens: Vec<Token>,
    pos: usize,
}

macro_rules! return_binary_clause {
    ($self: ident, $operator: expr, $curr_token: ident) => {
        return Ok(UserAST::BinaryClause(Box::new($curr_token), $operator, Box::new($self._parse()?)));
    };
}

pub(crate) fn get_text_for_token<'a>(text: &'a str, start: u32, stop: u32) -> &'a str {
    &text[start as usize ..stop as usize ]
}

pub fn parse(text: &str) -> Result<UserAST<'_, '_>, ParseError> {
    Parser::new(text)?._parse()
}
pub fn parse_with_opt(text: &str, options: Options) -> Result<UserAST<'_, '_>, ParseError> {
    Parser::new_with_opt(text, options)?._parse()
}

impl<'a> Parser<'a> {
    pub fn new(text: &'a str) -> Result<Self, ParseError> {
        let tokens = Lexer::new(text).get_tokens()?;
        Ok(Parser { tokens, pos: 0, text })
    }
    pub fn new_with_opt(text: &'a str, options: Options) -> Result<Self, ParseError> {
        let tokens = Lexer::new_with_opt(text, options).get_tokens()?;
        Ok(Parser { tokens, pos: 0, text })
    }

    fn unexpected_token_type(&self, message: &'static str, allowed_types: Option<&[Option<TokenType>]>) -> Result<(), ParseError> {
        // generate snippet
        let [start, stop] = self.tokens.get(self.pos).map(|next_token| [next_token.byte_start_pos as usize, next_token.byte_stop_pos as usize]).unwrap_or_else(|| [self.text.len(), self.text.len()]);
        let marked_in_orig = marked_in_orig(self.text, start, stop);

        let err = if message == ""{
            let message = format!(
                "{} Unexpected token_type, got {}{:?}",
                message,
                self.get_type().map(|el| format!("{:?}", el)).unwrap_or_else(|| "EOF".to_string()),
                allowed_types.map(|el| format!(" allowed_types: {:?}", el)).unwrap_or_else(|| "".to_string())
            );
            ParseError::UnexpectedTokenType(marked_in_orig, message)
        }else{
            ParseError::UnexpectedTokenType(marked_in_orig, message.to_string())
        };
        return Err(err);
    }

    fn assert_allowed_types(&self, message: &'static str, allowed_types: &[Option<TokenType>]) -> Result<(), ParseError> {
        if !allowed_types.contains(&self.get_type()) {
            self.unexpected_token_type(message, Some(allowed_types))?;
        }
        Ok(())
    }

    fn next_token(&mut self) -> Result<Token, ParseError> {
        let token = self.tokens.get(self.pos).unwrap();
        self.pos += 1;
        Ok(*token)
    }

    fn parse_user_filter(&mut self, curr_token: Token) -> Result<UserFilter<'a>, ParseError> {
        let mut curr_ast = UserFilter {
            levenshtein: None,
            phrase: get_text_for_token(self.text, curr_token.byte_start_pos, curr_token.byte_stop_pos),
        };

        // Optional: Define Levenshtein distance
        if self.is_type(TokenType::Tilde) {
            self.next_token()?; // Remove Tilde

            self.assert_allowed_types("Expecting a levenshtein number after a \'~\' ", &[Some(TokenType::Literal)])?;

            let lev_token = self.next_token()?; // Remove levenshtein number
            let levenshtein: u8 = get_text_for_token(self.text, lev_token.byte_start_pos, lev_token.byte_stop_pos)
                .parse()
                .map_err(|_e| ParseError::ExpectedNumber(format!("Expected number after tilde to define levenshtein distance but got {:?}", lev_token)))?;
            curr_ast.levenshtein = Some(levenshtein);
        }
        Ok(curr_ast)
    }

    fn parse_sub_expression(&mut self, curr_ast: UserAST<'a,'a>) -> Result<UserAST<'a,'a>, ParseError> {
        self.assert_allowed_types(
            "",
            &[
                Some(TokenType::AttributeLiteral),
                Some(TokenType::Literal),
                Some(TokenType::ParenthesesOpen),
                Some(TokenType::ParenthesesClose),
                Some(TokenType::And),
                Some(TokenType::Or),
                None,
            ],
        )?;

        if let Some(next_token_type) = self.get_type() {
            match next_token_type {
                TokenType::AttributeLiteral | TokenType::Literal => {
                    return_binary_clause!(self, Operator::Or, curr_ast);
                }
                TokenType::Or => {
                    self.next_token()?;
                    return_binary_clause!(self, Operator::Or, curr_ast);
                }
                TokenType::And => {
                    self.next_token()?;
                    return_binary_clause!(self, Operator::And, curr_ast);
                }
                 TokenType::ParenthesesOpen | TokenType::Tilde => unimplemented!(),
                TokenType::ParenthesesClose => return Ok(curr_ast),
            }
        } else {
            return Ok(curr_ast); // is last one
        }
    }

    fn _parse(&mut self) -> Result<UserAST<'a,'a>, ParseError> {
        let curr_token = self.next_token()?;
        match curr_token.token_type {
            TokenType::AttributeLiteral => {
                //Check if attribute covers whole ast or only next literal
                match self.get_type() {
                    Some(TokenType::ParenthesesOpen) => return Ok(UserAST::Attributed(get_text_for_token(self.text, curr_token.byte_start_pos, curr_token.byte_stop_pos), Box::new(self._parse()?))),
                    Some(TokenType::Literal) => {
                        let token2 = self.next_token()?;
                        let curr_ast = self.parse_user_filter(token2)?;
                        let attributed_ast = UserAST::Attributed(get_text_for_token(self.text, curr_token.byte_start_pos, curr_token.byte_stop_pos), Box::new(UserAST::Leaf(Box::new(curr_ast))));
                        return self.parse_sub_expression(attributed_ast);
                    }
                    _ => self.unexpected_token_type(
                        "only token or ( allowed after attribute ('attr:') ",
                        Some(&[Some(TokenType::Literal), Some(TokenType::ParenthesesOpen)]),
                    )?,
                };
            }
            TokenType::Literal => {
                let curr_ast = self.parse_user_filter(curr_token)?;
                return self.parse_sub_expression(UserAST::Leaf(Box::new(curr_ast)));
            }

            TokenType::ParenthesesOpen => {
                let parenthesed_ast = self._parse()?;
                self.assert_allowed_types("", &[Some(TokenType::ParenthesesClose)])?;
                self.next_token()?;
                return self.parse_sub_expression(parenthesed_ast);
            }
            TokenType::ParenthesesClose => unimplemented!(),
            TokenType::Tilde => {
                self.unexpected_token_type("", None)?; // IMPOSSIBURU!, should be covered by lookeaheads
            }

            TokenType::Or | TokenType::And => {
                unimplemented!() // IMPOSSIBURU!, should be covered by lookeaheads
            }
        }
        unreachable!()
    }

    fn is_type(&self, token_type: TokenType) -> bool {
        self.get_type().map(|el| el == token_type).unwrap_or(false)
    }

    fn get_type(&self) -> Option<TokenType> {
        self.tokens.get(self.pos).map(|el| el.token_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{Operator::*};

    #[test]
    fn simple() {
        assert_eq!(parse("hallo").unwrap(), "hallo".into());
    }
    #[test]
    fn test_invalid() {
        assert_eq!(
            parse("field:what:ok").is_err(),
            true // Err(UnexpectedTokenType("Expecting a levenshtein number after a \'~\' at position 2, but got None".to_string()))
        );
    }

    #[test]
    fn test_phrases() {
        assert_eq!(parse("\"cool\")").unwrap(), ("cool".into()));
        assert_eq!(parse("\"cooles teil\")").unwrap(), ("cooles teil".into()));
    }

    #[test]
    fn test_parentheses() {
        assert_eq!(parse("(cool)").unwrap(), ("cool".into()));

        assert_eq!(parse("((((((cool))))))").unwrap(), ("cool".into()));
        assert_eq!(parse("((((((cool)))))) AND ((((((cool))))))").unwrap(), ("cool".into(), And, "cool".into()).into());
        assert_eq!(
            parse("(super AND cool) OR fancy").unwrap(),
            ((("super".into(), And, "cool".into()).into()), Or, "fancy".into()).into()
        );
        assert_eq!(
            parse("(super AND cool) OR (fancy)").unwrap(),
            ((("super".into(), And, "cool".into()).into()), Or, "fancy".into()).into()
        );
        assert_eq!(
            parse("((super AND cool)) OR (fancy)").unwrap(),
            ((("super".into(), And, "cool".into()).into()), Or, "fancy".into()).into()
        );
        // println!("{:?}", parse("(cool)"));
    }
    #[test]
    fn test_and_or() {
        assert_eq!(
            parse("super AND cool OR fancy").unwrap(),
            ("super".into(), And, ("cool".into(), Or, "fancy".into()).into()).into() // BinaryClause("super".into(), And, Box::new(("cool", Operator::Or, "fancy").into()))
        );
        // println!("asd {:?}", parse("super OR cool AND fancy"));
        assert_eq!(
            parse("super OR cool AND fancy").unwrap(),
            ("super".into(), Or, ("cool".into(), And, "fancy".into()).into()).into()
        );
    }

    #[test]
    fn test_implicit_or() {
        assert_eq!(
            parse("super cool OR fancy").unwrap(),
            ("super".into(), Or, ("cool".into(), Or, "fancy".into()).into()).into()
        );
        assert_eq!(parse("super cool").unwrap(), ("super".into(), Or, "cool".into()).into());
        assert_eq!(parse("super cool").unwrap(), parse("super OR cool").unwrap());
    }

    #[test]
    fn test_levenshtein() {
        assert_eq!(
            parse("fancy~1").unwrap(),
            UserAST::Leaf(Box::new(UserFilter {
                // field_name: None,
                phrase: "fancy",
                levenshtein: Some(1),
            }))
        );
        assert_eq!(
            parse("fancy~"),
            Err(ParseError::UnexpectedTokenType("fancy~﹏﹏".to_string(), "Expecting a levenshtein number after a \'~\' ".to_string()))
        );
        assert_eq!(parse("fancy~1").unwrap(), "fancy~1".into());
        assert_eq!(
            parse("super cool OR fancy~1").unwrap(),
            ("super".into(), Or, ("cool".into(), Or, "fancy~1".into()).into()).into()
        );
    }

    #[test]
    fn test_attribute_and_levenshtein() {
        assert_eq!(
            parse("field:fancy~1").unwrap(),
            UserAST::Attributed(
                "field".into(),
                Box::new(UserAST::Leaf(Box::new(UserFilter {
                    phrase: "fancy",
                    levenshtein: Some(1),
                })))
            )
        );

        assert_eq!(
            parse("field:fancy~1").unwrap(),
            UserAST::Attributed(
                "field".into(),
                Box::new(UserAST::Leaf(Box::new(UserFilter {
                    phrase: "fancy",
                    levenshtein: Some(1),
                })))
            )
        );
        assert_eq!(parse("field:fancy~1").unwrap(), "field:fancy~1".into());
    }

    #[test]
    fn test_attribute_and_implicit_or_on_all() {
        assert_eq!(
            parse("\"field\":fancy unlimited").unwrap(),
            (UserAST::Attributed("field", "fancy".into()), Or, "unlimited".into()).into()
        );
    }

    #[test]
    fn test_attribute_quoted_field() {
        assert_eq!(
            parse("\"field\":fancy unlimited").unwrap(),
            (UserAST::Attributed("field", "fancy".into()), Or, "unlimited".into()).into()
        );
    }
    #[test]
    fn test_quote_on_quote() {
        assert_eq!(
            parse("\"field\"\"cool\"").unwrap(), // there should be a space
            ("field".into(), Or, "cool".into()).into()
        );
    }
    // #[test]
    // fn test_disabled_attribute_quoted_field() {
    //     let opt_no_attr = Options{no_attributes:true, ..Default::default()};
    //     assert_eq!(
    //         parse_with_opt("\"field\":fancy unlimited", opt_no_attr).unwrap(),
    //         ("field:fancy".into(), Or, "unlimited".into()).into()
    //     );
    // }
    #[test]
    fn test_attribute_simple() {
        assert_eq!(parse("field:fancy").unwrap(), "field:fancy".into());
        assert_eq!(
            parse("field:fancy").unwrap(),
            UserAST::Attributed(
                "field".into(),
                Box::new(UserAST::Leaf(Box::new(UserFilter {
                    phrase: "fancy",
                    levenshtein: None,
                })))
            )
        );
    }
    #[test]
    fn test_disabled_attribute_simple() {
        let opt_no_attr = Options{no_attributes:true, ..Default::default()};
        assert_eq!(
            parse_with_opt("field:fancy", opt_no_attr).unwrap(),
            UserAST::Leaf(Box::new(UserFilter {
                phrase: "field:fancy",
                levenshtein: None,
            }))
        );
    }
    #[test]
    fn test_attribute_after_text() {
        assert_eq!(
            parse("freestyle myattr:(super cool)").unwrap(),
            ("freestyle".into(),
                Or,
                (UserAST::Attributed("myattr", Box::new(("super".into(), Or, "cool".into()).into()) )) ).into());
    }
    #[test]
    fn test_attribute_errors() {
        assert_eq!(
            parse("fancy:"),
            Err(ParseError::UnexpectedTokenType(
                "fancy:﹏﹏".to_string(),
                "only token or ( allowed after attribute ('attr:') ".to_string()
            )) // Err(UnexpectedTokenType("Expecting a levenshtein number after a \'~\' at position 2, but got None".to_string()))
        );
    }

    #[test]
    fn test_attributed_block_1() {
        assert_eq!(
            parse("field:(fancy unlimited)").unwrap(),
            UserAST::Attributed("field", Box::new(("fancy".into(), Or, "unlimited".into()).into()))
        );
    }

    fn test_parse_query_to_ast_helper(query: &str, expected: &str) {
        let query_str = parse(query).unwrap();
        assert_eq!(format!("{:?}", query_str), expected);
    }

    #[test]
    fn test_attributed_block() {
        test_parse_query_to_ast_helper("field:(fancy unlimited)", "field:(\"fancy\" OR \"unlimited\")");
    }

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
