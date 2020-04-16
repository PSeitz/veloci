#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenType {
    // Unlimited
    AttributeLiteral,
    Literal,

    // 1-char - will break up literals
    ParenthesesOpen,
    ParenthesesClose,
    // DoubleQuotes,
    // SingleQuotes,
    Tilde,
    // Colon,

    // 2-char
    // EscapedDoubleQuotes,
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

#[derive(Debug, Clone)]
pub(crate) struct Token {
    pub(crate) token_type: TokenType,
    pub(crate) start_pos: usize,
    pub(crate) stop_pos: usize,
    // pub(crate) in_quotes: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct TokenWithText {
    pub(crate) token: Token,
    pub(crate) matched_text: String,
}

#[derive(Debug, Clone)]
pub(crate) struct Lexer {
    // in_quotes: bool,
    chars: Vec<char>,
    current_pos: usize,
}

impl Lexer {
    pub(crate) fn new(input: &str) -> Self {
        Lexer {
            chars: input.chars().collect(),
            current_pos: 0,
            // in_quotes:false,
        }
    }

    pub(crate) fn get_tokens_text(&mut self) -> Vec<String> {
        self.get_tokens()
            .into_iter()
            .map(|token| self.chars[token.start_pos..token.stop_pos].into_iter().collect())
            .collect()
    }

    pub(crate) fn get_tokens_with_text(&mut self) -> Vec<TokenWithText> {
        self.get_tokens()
            .into_iter()
            .map(|token| TokenWithText {
                matched_text: self.get_matched_chars(&token).into_iter().collect(),
                token,
            })
            .collect()
    }

    pub(crate) fn get_tokens(&mut self) -> Vec<Token> {
        let mut tokens = vec![];
        while let Some(token) = self.next_token() {
            tokens.push(token);
        }
        tokens
    }

    pub(crate) fn get_token_types(&mut self) -> Vec<TokenType> {
        self.get_tokens().iter().map(|t| t.token_type).collect()
    }

    pub(crate) fn get_matched_chars(&self, token: &Token) -> &[char] {
        &self.chars[token.start_pos..token.stop_pos]
    }

    pub(crate) fn next_token(&mut self) -> Option<Token> {
        self.eat_while(char::is_whitespace);

        if let Some(c) = self.cur_char() {
            // whitespace is ignored except in phrases
            let mut start_pos = self.current_pos;
            let mut token_type = match self.chars[self.current_pos..] {
                ['A', 'N', 'D', ' ', ..] if self.prev_char_is_whitespace() => {
                    // AND requires whitespace
                    move_pos(&mut self.current_pos, 3);
                    Some(TokenType::And)
                }
                ['O', 'R', ' ', ..] if self.prev_char_is_whitespace() => {
                    // OR requires whitespace
                    move_pos(&mut self.current_pos, 2);
                    Some(TokenType::Or)
                }
                _ => None,
            };

            if self.is_doublequote(self.current_pos) {
                self.current_pos += 1;
                start_pos += 1; // move behind quote
                while self.cur_char().is_some() && !self.is_doublequote(self.current_pos) {
                    self.current_pos += 1;
                }
                let stop_pos = self.current_pos;
                self.current_pos += 1;
                let token_type = if self.is_colon_at(self.current_pos){
                    self.current_pos +=1;
                    TokenType::AttributeLiteral
                }else{
                    TokenType::Literal
                };

                return Some(Token {
                    token_type: token_type,
                    start_pos,
                    stop_pos,
                });
            }

            if let Some(tt) = TokenType::from_single_char(c) {
                token_type = Some(tt);
                move_pos(&mut self.current_pos, 1);
            }

            if let Some(token_type) = token_type {
                let stop_pos = self.current_pos;

                let next_token = Some(Token { token_type, start_pos, stop_pos });

                return next_token;
            }

            // Literal
            move_pos(&mut self.current_pos, 1);
            self.eat_while(|c| !c.is_whitespace() && c != ':' && TokenType::from_single_char(c).is_none());
            let stop_pos = self.current_pos;
            let token_type = if self.is_colon_at(self.current_pos){
                self.current_pos +=1;
                TokenType::AttributeLiteral
            }else{
                TokenType::Literal
            };
            return Some(Token {
                token_type: token_type,
                start_pos,
                stop_pos,
            });
        } else {
            None
        }
    }


    pub fn is_colon_at(&self, pos:usize) -> bool {
        self.chars.get(pos).map(|c| *c==':').unwrap_or(false)
    }
    // is quote and not escaped
    pub fn is_doublequote(&self, pos: usize) -> bool {
        self.chars.get(pos).cloned().map(|c| c == '"').unwrap_or(false) && (self.current_pos == 0 || self.chars.get(pos - 1).cloned().map(|c| c != '\\').unwrap_or(false))
    }

    pub fn eat_while<F>(&mut self, mut cond: F)
    where
        F: FnMut(char) -> bool,
    {
        while self.cur_char().map(|c| cond(c)).unwrap_or(false) {
            self.current_pos += 1;
        }
    }

    pub fn prev_char_is_whitespace(&self) -> bool {
        self.current_pos != 0 && self.chars.get(self.current_pos - 1).cloned().map(char::is_whitespace).unwrap_or(false)
    }

    pub fn cur_char(&self) -> Option<char> {
        self.chars.get(self.current_pos).cloned()
    }
}

pub fn move_pos(current_pos: &mut usize, num: usize) {
    *current_pos += num;
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

        // this unclosed parentheses are allowed and will be part of the literal
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
