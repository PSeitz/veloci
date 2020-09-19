#[derive(Debug, Eq, PartialEq)]
pub enum ParseError {
    EmptyParentheses(String),
    UnexpectedTokenType(String, String),
    ExpectedNumber(String),
}

pub(crate) fn marked_in_orig(text: &str, start:usize, stop: usize) -> String {
    format!("{}﹏{}﹏{}",&text[..start], &text[start..stop], &text[stop..] )
}