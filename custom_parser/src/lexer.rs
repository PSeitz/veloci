#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenType {
    // Unlimited
    AttributeLiteral,
    Literal,

    // 1-char - will break up literals
    ParenthesesOpen,
    ParenthesesClose,
    Tilde,

    // 2-char
    Or,

    //3-char
    And,
}

impl TokenType {
    fn from_single_char(cha: char) -> Option<Self> {
        match cha {
            '(' => Some(TokenType::ParenthesesOpen),
            ')' => Some(TokenType::ParenthesesClose),
            // '"'  => Some(TokenType::DoubleQuotes),
            // '\'' => Some(TokenType::SingleQuotes),
            '~' => Some(TokenType::Tilde),
            // ':' => Some(TokenType::Colon),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Token {
    pub(crate) byte_start_pos: u32,
    pub(crate) byte_stop_pos: u32,
    pub(crate) token_type: TokenType,
    // pub(crate) in_quotes: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct TokenWithText {
    pub(crate) token: Token,
    pub(crate) matched_text: String,
}

#[derive(Debug, Clone)]
pub(crate) struct Lexer<'a> {
    // in_quotes: bool,
    text: &'a str,
    chars: Vec<char>,
    current_pos: u32,
    current_byte_pos: u32,
}

impl<'a> Lexer<'a> {
    pub(crate) fn new(text: &'a str) -> Self {
        Lexer {
            chars: text.chars().collect(),
            current_pos: 0,
            current_byte_pos: 0,
            text,
            // in_quotes:false,
        }
    }

    pub(crate) fn get_tokens(&mut self) -> Vec<Token> {
        let mut tokens = Vec::with_capacity(self.chars.len() / 4);
        while let Some(token) = self.next_token() {
            tokens.push(token);
        }
        tokens
    }

    #[cfg(test)]
    pub(crate) fn get_token_types(&mut self) -> Vec<TokenType> {
        self.get_tokens().iter().map(|t| t.token_type).collect()
    }

    #[cfg(test)]
    pub(crate) fn get_tokens_text(&mut self) -> Vec<String> {
        self.get_tokens()
            .into_iter()
            .map(|token| self.text[token.byte_start_pos as usize ..token.byte_stop_pos as usize ].to_string())
            .collect()
    }

    pub(crate) fn next_token(&mut self) -> Option<Token> {
        self.eat_while(char::is_whitespace);

        if let Some(c) = self.cur_char() {
            // whitespace is ignored except in phrases
            let mut byte_start_pos = self.current_byte_pos;
            let mut token_type = match self.chars[self.current_pos as usize ..] {
                ['A', 'N', 'D', ' ', ..] if self.prev_char_is_whitespace() => {
                    // AND requires whitespace
                    self.eat_chars(3);
                    Some(TokenType::And)
                }
                ['O', 'R', ' ', ..] if self.prev_char_is_whitespace() => {
                    // OR requires whitespace
                    self.eat_chars(2);
                    Some(TokenType::Or)
                }
                _ => None,
            };

            if self.is_doublequote(self.current_pos) {
                self.eat_char();
                byte_start_pos += 1; // move behind quote
                while self.cur_char().is_some() && !self.is_doublequote(self.current_pos) {
                    self.eat_char();
                }
                let byte_stop_pos = self.current_byte_pos;
                self.eat_char();
                let token_type = if self.is_colon_at(self.current_pos) {
                    self.eat_char();
                    TokenType::AttributeLiteral
                } else {
                    TokenType::Literal
                };

                return Some(Token {
                    token_type,
                    byte_start_pos,
                    byte_stop_pos,
                });
            }

            if let Some(tt) = TokenType::from_single_char(c) {
                token_type = Some(tt);
                self.eat_char();
            }

            if let Some(token_type) = token_type {
                let byte_stop_pos = self.current_byte_pos;
                let next_token = Some(Token { token_type, byte_start_pos, byte_stop_pos });
                return next_token;
            }

            // Literal
            self.eat_char();
            self.eat_while(|c| !c.is_whitespace() && c != ':' && TokenType::from_single_char(c).is_none());
            let byte_stop_pos = self.current_byte_pos;
            let token_type = if self.is_colon_at(self.current_pos) {
                self.eat_char();
                TokenType::AttributeLiteral
            } else {
                TokenType::Literal
            };
            return Some(Token {
                token_type,
                byte_start_pos,
                byte_stop_pos,
            });
        } else {
            None
        }
    }

    pub fn is_colon_at(&self, pos: u32) -> bool {
        self.chars.get(pos as usize).map(|c| *c == ':').unwrap_or(false)
    }

    // is quote and not escaped
    pub fn is_doublequote(&self, pos: u32) -> bool {
        self.chars.get(pos as usize).cloned().map(|c| c == '"').unwrap_or(false) && (self.current_pos == 0 || self.chars.get(pos as usize  - 1).cloned().map(|c| c != '\\').unwrap_or(false))
    }

    pub fn eat_while<F>(&mut self, mut cond: F)
    where
        F: FnMut(char) -> bool,
    {
        while self.cur_char().map(|c| cond(c)).unwrap_or(false) {
            self.eat_char();
        }
    }

    pub fn eat_char(&mut self)
    {
        if let Some(cur_char) = self.cur_char() {
            self.current_pos += 1;
            self.current_byte_pos += cur_char.len_utf8() as u32;
        }
    }

    pub fn prev_char_is_whitespace(&self) -> bool {
        self.current_pos != 0 && self.chars.get(self.current_pos as usize - 1).cloned().map(char::is_whitespace).unwrap_or(false)
    }

    pub fn cur_char(&self) -> Option<char> {
        self.chars.get(self.current_pos as usize ).cloned()
    }

    pub fn eat_chars(&mut self, num: usize) {
        for _ in 0..num {
            self.eat_char();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{TokenType as TT, *};

    #[test]
    fn test_white_space_tokens() {
        assert_eq!(Lexer::new("    ").get_tokens_text(), vec![] as Vec<String>); // white space is ignored
    }

    #[test]
    fn test_literal_space_paren() {
        assert_eq!(Lexer::new("schlau (").get_tokens_text(), ["schlau", "("]);
    }

    #[test]
    fn test_combinations() {
        assert_eq!(Lexer::new(" schön und schlau").get_tokens_text(), ["schön", "und", "schlau"]);
        assert_eq!(Lexer::new("schlau").get_tokens_text(), ["schlau"]);
        assert_eq!(Lexer::new("schlau(").get_tokens_text(), ["schlau", "("]);
        assert_eq!(Lexer::new("schlau (").get_tokens_text(), ["schlau", "("]);
        assert_eq!(Lexer::new("coolAND AND (").get_tokens_text(), ["coolAND", "AND", "("]);
    }

    #[test]
    fn test_and_or() {
        assert_eq!(Lexer::new("coolAND AND (").get_tokens_text(), ["coolAND", "AND", "("]);
        assert_eq!(Lexer::new("coolAND AND (").get_token_types(), [TT::Literal, TT::And, TT::ParenthesesOpen]);
        assert_eq!(Lexer::new("or OR").get_token_types(), [TT::Literal, TT::Literal]);
        assert_eq!(Lexer::new("OR OR").get_token_types(), [TT::Literal, TT::Literal]);
        assert_eq!(Lexer::new("OR OR OR").get_token_types(), [TT::Literal, TT::Or, TT::Literal]);
        assert_eq!(Lexer::new("AND AND").get_token_types(), [TT::Literal, TT::Literal]);
        assert_eq!(Lexer::new("AND AND AND").get_token_types(), [TT::Literal, TT::And, TT::Literal]);
        // assert_eq!(Lexer::new("AND AND AND AND AND").get_token_types(), [TT::Literal, TT::And, TT::Literal, TT::And, TT::Literal]); Doees not work
        assert_eq!(Lexer::new("ANDand AND    ").get_token_types(), [TT::Literal, TT::And]);
        assert_eq!(Lexer::new("ANDand AND    ").get_tokens_text(), ["ANDand", "AND"]);
    }

    #[test]
    fn test_parentheses() {
        assert_eq!(Lexer::new("(cool)").get_tokens_text(), ["(", "cool", ")"]);
        assert_eq!(Lexer::new("(cool)").get_token_types(), [TT::ParenthesesOpen, TT::Literal, TT::ParenthesesClose]);
        assert_eq!(Lexer::new("(cool OR nice)AND").get_tokens_text(), ["(", "cool", "OR", "nice", ")", "AND"]);
        assert_eq!(
            Lexer::new("(cool OR nice)AND").get_token_types(),
            [TT::ParenthesesOpen, TT::Literal, TT::Or, TT::Literal, TT::ParenthesesClose, TT::Literal]
        );
    }

    #[test]
    fn test_quotes() {
        assert_eq!(Lexer::new(r#""my quote""#).get_tokens_text(), ["my quote"]);

        // this unclosed quotes here are allowed and will be part of the literal
        assert_eq!(Lexer::new(r#"asdf""#).get_tokens_text(), ["asdf\""]);

        // assert_eq!(Lexer::new(r#""my quote""#).get_tokens_text(), ["\"", "my", "quote", "\""]);
        // assert_eq!(Lexer::new(r#""my quote""#).get_tokens().into_iter().map(|t|t.in_quotes).collect::<Vec<_>>(), [false, true, true, true, false]);
    }

    #[test]
    fn test_tilde() {
        assert_eq!(Lexer::new("or~").get_token_types(), [TT::Literal, TT::Tilde]);
        assert_eq!(Lexer::new("~~").get_token_types(), [TT::Tilde, TT::Tilde]);
        assert_eq!(Lexer::new("~  ~").get_token_types(), [TT::Tilde, TT::Tilde]);
        assert_eq!(Lexer::new("~a~").get_token_types(), [TT::Tilde, TT::Literal, TT::Tilde]);
    }

    #[test]
    fn test_colon() {
        assert_eq!(Lexer::new("cool:nice").get_tokens_text(), ["cool", "nice"]);
        assert_eq!(Lexer::new("cool:nice").get_token_types(), [TT::AttributeLiteral, TT::Literal]);
        assert_eq!(Lexer::new("\"cool\":nice").get_tokens_text(), ["cool", "nice"]);
        assert_eq!(Lexer::new(r#""cool":nice"#).get_token_types(), [TT::AttributeLiteral, TT::Literal]);
    }
}
