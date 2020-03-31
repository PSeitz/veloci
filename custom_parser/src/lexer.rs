

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenType {
    Literal,
    And,
    Or,
    ParenthesesOpen,
    ParenthesesClose,
    DoubleQuotes,
    EscapedDoubleQuotes,
    SingleQuotes,
    WhiteSpaces
}

#[derive(Debug, Clone)]
struct Token {
    token_type: TokenType,
    start_pos: usize,
    stop_pos: usize,
    is_quoted: bool,
}


#[derive(Debug, Clone)]
struct TokenWithText {
    token: Token,
    matched_text: String
}


#[derive(Debug, Clone)]
struct Lexer {
    in_quotes: bool,
    chars: Vec<char>,
    current_pos: usize
}

impl Lexer {
    fn new(input: &str) -> Self{
        Lexer{
            chars: input.chars().collect(),
            current_pos:0,
            in_quotes:false,
        }
    }

    pub fn get_tokens_text(&mut self) -> Vec<String> {
        self.get_tokens().into_iter().map(|token|
            self.chars[token.start_pos..token.stop_pos].into_iter().collect()
        ).collect()
    }

    pub fn get_tokens_with_text(&mut self) -> Vec<TokenWithText> {
        self.get_tokens().into_iter().map(|token|
            TokenWithText{
                matched_text:self.get_matched_text(&token).into_iter().collect(),
                token, 
            }
        ).collect()
    }

    pub fn get_tokens(&mut self) -> Vec<Token> {
        let mut tokens = vec![];
        while let Some(token) = self.next_token() {
            tokens.push(token);
        }
        tokens
    }

    pub fn get_token_types(&mut self) -> Vec<TokenType> {
        self.get_tokens().iter().map(|t|t.token_type).collect()
    }

    fn get_matched_text(&self, token: &Token) -> &[char]{
        &self.chars[token.start_pos..token.stop_pos]
    }

    pub fn next_token(&mut self) -> Option<Token> {

        if let Some(c) = self.cur_char() {
            let start_pos = self.current_pos;
            if c.is_whitespace(){
                self.eat_while(char::is_whitespace);
                let stop_pos = self.current_pos;
                return Some(Token {
                    token_type: TokenType::WhiteSpaces,
                    start_pos,
                    stop_pos,
                    is_quoted: self.in_quotes,
                });
            }

            let start_pos = self.current_pos;
            let token_type = match self.chars[self.current_pos..] {
                ['A','N','D', ' ', ..] if self.prev_char_is_whitespace() => { // AND requires whitespace
                    move_pos(&mut self.current_pos, 3);
                    Some(TokenType::And)
                },
                ['O','R', ' ', ..] if self.prev_char_is_whitespace() => { // OR requires whitespace
                    move_pos(&mut self.current_pos, 2);
                    Some(TokenType::Or)
                },
                ['\\','"', ..] => {
                    move_pos(&mut self.current_pos, 2);
                    Some(TokenType::EscapedDoubleQuotes)
                },
                ['"', ..] => {
                    // self.in_quotes = !self.in_quotes;
                    move_pos(&mut self.current_pos, 1);
                    Some(TokenType::DoubleQuotes)
                },
                ['\'', ..] => {
                    move_pos(&mut self.current_pos, 1);
                    Some(TokenType::SingleQuotes)
                },
                ['(', ..] => {
                    move_pos(&mut self.current_pos, 1);
                    Some(TokenType::ParenthesesOpen)
                },
                [')', ..] => {
                    move_pos(&mut self.current_pos, 1);
                    Some(TokenType::ParenthesesClose)
                },
                _ => None,
            };

            if let Some(token_type) = token_type {
                let stop_pos = self.current_pos;

                // end quotes, switch before creating token
                if self.in_quotes && token_type == TokenType::DoubleQuotes {
                    self.in_quotes = !self.in_quotes;
                }

                let next_token = Some(Token {
                    token_type,
                    start_pos,
                    stop_pos,
                    // matched_text: &self.chars[start_pos..stop_pos],
                    is_quoted: self.in_quotes,
                });

                // start quotes, switch after creating token
                if !self.in_quotes && token_type == TokenType::DoubleQuotes {
                    self.in_quotes = !self.in_quotes;
                }
                return next_token;
            }

            // Literal
            move_pos(&mut self.current_pos, 1);
            self.eat_while(|c| 
                !c.is_whitespace() 
                && c != '(' 
                && c != ')' 
                && c != '"' 
            );
            let stop_pos = self.current_pos;
            return Some(Token {
                token_type: TokenType::Literal,
                start_pos,
                stop_pos,
                // matched_text: &self.chars[start_pos..stop_pos],
                is_quoted: self.in_quotes,
            });


        }else{
            None
        }

    }


    pub fn eat_while<F>(&mut self, mut cond: F) 
    where
    F: FnMut(char) -> bool,
    {
        while self.cur_char().map(|c|cond(c)).unwrap_or(false) {
            self.current_pos+=1;
        }
    }

    pub fn prev_char_is_whitespace(&self) -> bool {
        self.current_pos != 0 && self.chars.get(self.current_pos-1).cloned().map(char::is_whitespace).unwrap_or(false)
    }

    pub fn cur_char(&self) -> Option<char> {
        self.chars.get(self.current_pos).cloned()
    }

    pub fn peek_next_token(&self) -> Option<Token> {
        unimplemented!()
    }
}

pub fn move_pos(current_pos: &mut usize, num: usize){
    *current_pos += num;
} 

#[cfg(test)]
mod tests {
    use super::*;
    use super::TokenType as TT;

    #[test]
    fn test_white_space_tokens() {
        assert_eq!(Lexer::new("    ").get_tokens_text(), ["    "]);
    }

    #[test]
    fn test_literal_space_paren() {
        assert_eq!(Lexer::new("schlau (").get_tokens_text(), ["schlau", " ", "("]);
    }

    #[test]
    fn test_combinations() {
        assert_eq!(Lexer::new(" schön und schlau").get_tokens_text(), [" ", "schön", " ", "und", " ", "schlau"]);
        assert_eq!(Lexer::new("schlau").get_tokens_text(), ["schlau"]);
        assert_eq!(Lexer::new("schlau(").get_tokens_text(), ["schlau", "("]);
        assert_eq!(Lexer::new("schlau (").get_tokens_text(), ["schlau", " ", "("]);
        assert_eq!(Lexer::new("coolAND AND (").get_tokens_text(), ["coolAND", " ", "AND", " ", "("]);
    }

    #[test]
    fn test_and_or() {
        assert_eq!(Lexer::new("coolAND AND (").get_tokens_text(), ["coolAND", " ", "AND", " ", "("]);
        assert_eq!(Lexer::new("coolAND AND (").get_token_types(), [TT::Literal, TT::WhiteSpaces, TT::And, TT::WhiteSpaces, TT::ParenthesesOpen]);
        assert_eq!(Lexer::new("or OR").get_token_types(), [TT::Literal, TT::WhiteSpaces, TT::Literal]);
        assert_eq!(Lexer::new("OR OR").get_token_types(), [TT::Literal, TT::WhiteSpaces, TT::Literal]);
        assert_eq!(Lexer::new("OR OR OR").get_token_types(), [TT::Literal, TT::WhiteSpaces, TT::Or, TT::WhiteSpaces, TT::Literal]);
        assert_eq!(Lexer::new("AND AND").get_token_types(), [TT::Literal, TT::WhiteSpaces, TT::Literal]);
        assert_eq!(Lexer::new("AND AND AND").get_token_types(), [TT::Literal, TT::WhiteSpaces, TT::And, TT::WhiteSpaces, TT::Literal]);
        assert_eq!(Lexer::new("ANDand AND    ").get_token_types(), [TT::Literal, TT::WhiteSpaces, TT::And, TT::WhiteSpaces]);
        assert_eq!(Lexer::new("ANDand AND    ").get_tokens_text(), ["ANDand", " ", "AND", "    "]);
    }

    #[test]
    fn test_parentheses() {
        assert_eq!(Lexer::new("(cool)").get_tokens_text(), ["(", "cool", ")"]);
        assert_eq!(Lexer::new("(cool)").get_token_types(), [TT::ParenthesesOpen, TT::Literal, TT::ParenthesesClose]);
        assert_eq!(Lexer::new("(cool OR nice)AND").get_tokens_text(), ["(", "cool", " ", "OR", " ", "nice", ")", "AND"]);
        assert_eq!(Lexer::new("(cool OR nice)AND").get_token_types(), [TT::ParenthesesOpen, TT::Literal, TT::WhiteSpaces, TT::Or, TT::WhiteSpaces, TT::Literal, TT::ParenthesesClose, TT::Literal]);
    }


    #[test]
    fn test_quotes() {
        assert_eq!(Lexer::new(r#""my quote""#).get_tokens_text(), ["\"", "my", " ", "quote", "\""]);
        assert_eq!(Lexer::new(r#""my quote""#).get_tokens().into_iter().map(|t|t.is_quoted).collect::<Vec<_>>(), [false, true, true, true, false]);
    }
}

