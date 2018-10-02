use regex::Regex;

pub trait Tokenizer {
    fn get_tokens<'a, F>(&self, orignal: &'a str, cb_text: &mut F)
    where
        F: FnMut(&'a str, bool);

    fn has_tokens(&self, orignal: &str) -> bool;
}

lazy_static! {
    // static ref TOKENIZER:Regex  = Regex::new(r#"([\s\(\),.…;・’\-\[\]{}'"“])+|([^\s\(\),.…;・’\-\[\]{}'"“]*)"#).unwrap();
    // static ref TOKENIZER:Regex  = Regex::new(r#"([\s])+|([^\s]*)"#).unwrap();
    static ref SEPERATORS:Regex = Regex::new(r#"(?P<seperator>[\s\(\),.…;・’\-\[\]{}'"“]+)"#).unwrap();
}

fn is_default_seperator(char: char) -> bool {
    match char {
        ' ' | '\t' | '\n' | '\r' | '(' | ')' | ',' | '.' | '…' | ';' | '・' | '’' | '—' | '-' | '\\' | '{' | '}' | '<' | '>' | '\'' | '"' | '“' | '™' => true,
        _ => false,
    }
}

#[derive(Debug)]
pub struct SimpleTokenizer {}
impl Tokenizer for SimpleTokenizer {
    fn has_tokens(&self, orignal: &str) -> bool {
        SEPERATORS.is_match(orignal)
    }

    fn get_tokens<'a, F>(&self, orignal: &'a str, cb_text: &mut F)
    where
        F: FnMut(&'a str, bool),
    {
        let mut last_byte_pos = 0;
        for (char_byte_pos, char) in orignal.char_indices() {
            if is_default_seperator(char) {
                if char_byte_pos != last_byte_pos {
                    cb_text(&orignal[last_byte_pos..char_byte_pos], false);
                }
                let next_pos = char_byte_pos + char.len_utf8();
                cb_text(&orignal[char_byte_pos..next_pos], false);
                last_byte_pos = next_pos;
            }
        }

        if last_byte_pos != orignal.len() {
            cb_text(&orignal[last_byte_pos..orignal.len()], false);
        }
    }
}
#[derive(Debug)]
pub struct SimpleTokenizerCharsIterateGroupTokens {}
impl Tokenizer for SimpleTokenizerCharsIterateGroupTokens {
    fn has_tokens(&self, orignal: &str) -> bool {
        SEPERATORS.is_match(orignal)
    }

    fn get_tokens<'a, F>(&self, orignal: &'a str, cb_text: &mut F)
    where
        F: FnMut(&'a str, bool),
    {
        let mut last_returned_byte = 0;
        let mut last_was_token = false;
        for (char_byte_pos, char) in orignal.char_indices() {
            if is_default_seperator(char) {
                if char_byte_pos == 0 {
                    last_was_token = true;
                } else if !last_was_token {
                    cb_text(&orignal[last_returned_byte..char_byte_pos], false);
                    last_was_token = true;
                    last_returned_byte = char_byte_pos;
                }
            } else if last_was_token {
                cb_text(&orignal[last_returned_byte..char_byte_pos], true);
                last_was_token = false;
                last_returned_byte = char_byte_pos;
            }
        }

        if last_returned_byte != orignal.len() {
            cb_text(&orignal[last_returned_byte..orignal.len()], last_was_token);
        }
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    #[allow(unused_imports)]
    use test;

    use std::fs::File;
    use std::io::prelude::*;

    #[allow(dead_code)]
    fn get_test_book() -> String {
        let mut f = File::open("test_files/1342-0.txt").unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();
        s
    }

    #[test]
    fn test_tokenizer_control_sequences_grouped() {
        let tokenizer = SimpleTokenizerCharsIterateGroupTokens {};
        let mut vec: Vec<String> = vec![];
        tokenizer.get_tokens("das \n ist ein txt, test", &mut |token: &str, _is_seperator: bool| {
            vec.push(token.to_string());
        });
        assert_eq!(vec, vec!["das", " \n ", "ist", " ", "ein", " ", "txt", ", ", "test"])
    }
    #[test]
    fn test_tokenizer_control_sequences_alt() {
        let tokenizer = SimpleTokenizer {};
        let mut vec: Vec<String> = vec![];
        assert_eq!(tokenizer.has_tokens("das \n ist ein txt, test"), true);
        tokenizer.get_tokens("das \n ist ein txt, test", &mut |token: &str, _is_seperator: bool| {
            vec.push(token.to_string());
        });
        assert_eq!(vec, vec!["das", " ", "\n", " ", "ist", " ", "ein", " ", "txt", ",", " ", "test"])
    }
    #[test]
    fn test_tokenize_taschenbuch_start_with_seperator() {
        let tokenizer = SimpleTokenizerCharsIterateGroupTokens {};
        let mut vec: Vec<String> = vec![];
        tokenizer.get_tokens(" Taschenbuch (kartoniert)", &mut |token: &str, _is_seperator: bool| {
            vec.push(token.to_string());
        });
        assert_eq!(vec, vec![" ", "Taschenbuch", " (", "kartoniert", ")"])
    }
    #[test]
    fn test_tokenize_start_with_single_token() {
        let tokenizer = SimpleTokenizerCharsIterateGroupTokens {};
        let mut vec: Vec<String> = vec![];
        tokenizer.get_tokens("T oll", &mut |token: &str, _is_seperator: bool| {
            vec.push(token.to_string());
        });
        assert_eq!(vec, vec!["T", " ", "oll"])
    }

    // #[bench]
    // fn bench_regex_iter(b: &mut test::Bencher) {
    //     let text = get_test_book();

    //     b.iter(|| {
    //         let mut vec: Vec<String> = vec![];
    //         for cap in TOKENIZER.captures_iter(&text) {
    //             // cb_text(&cap[0], *&cap.get(1).is_some());
    //             vec.push(cap[0].to_string());
    //         }
    //         vec
    //     })
    // }

    #[bench]
    fn bench_custom_stuff(b: &mut test::Bencher) {
        let tokenizer = SimpleTokenizer {};
        let text = get_test_book();
        b.iter(|| {
            let mut vec: Vec<String> = Vec::with_capacity(text.len() / 5);
            tokenizer.get_tokens(&text, &mut |token: &str, _is_seperator: bool| {
                vec.push(token.to_string());
            });
            vec
        })
    }
    #[bench]
    fn bench_custom_stuff_no_copy(b: &mut test::Bencher) {
        let tokenizer = SimpleTokenizer {};
        let text = get_test_book();
        b.iter(|| {
            let mut vec = Vec::with_capacity(text.len() / 5);
            tokenizer.get_tokens(&text, &mut |token: &str, _is_seperator: bool| {
                vec.push(token);
            });
            // vec
        })
    }

    #[bench]
    fn bench_custom_stuff_grouped_no_copy(b: &mut test::Bencher) {
        let tokenizer = SimpleTokenizerCharsIterateGroupTokens {};
        let text = get_test_book();
        b.iter(|| {
            let mut vec = Vec::with_capacity(text.len() / 5);
            tokenizer.get_tokens(&text, &mut |token: &str, _is_seperator: bool| {
                vec.push(token);
            });
            // vec
        })
    }

    // #[bench]
    // fn bench_split(b: &mut test::Bencher) {
    //     let text = get_test_book();
    //     b.iter(|| {
    //         let mut vec: Vec<String> = vec![];
    //         for token in (&text).split(" ") {
    //             vec.push(token.to_string());
    //         }
    //         vec
    //     })
    // }

    #[bench]
    fn bench_split_reserve(b: &mut test::Bencher) {
        let text = get_test_book();
        b.iter(|| {
            let mut vec: Vec<String> = Vec::with_capacity(text.len() / 5);
            for token in (&text).split(" ") {
                vec.push(token.to_string());
            }
            vec
        })
    }

    #[bench]
    fn bench_split_iterate_only(b: &mut test::Bencher) {
        let text = get_test_book();
        b.iter(|| {
            let mut vec: Vec<&str> = Vec::with_capacity(text.len() / 5);
            for token in (&text).split(" ") {
                vec.push(token);
            }
        })
    }

    // #[allow(unused_imports)]
    // use util;
    // #[bench]
    // fn bench_normalize_text_and_split(b: &mut test::Bencher) {
    //     let text = get_test_book();
    //     b.iter(|| {
    //         let mut vec: Vec<String> = vec![];
    //         for token in util::normalize_text(&text).split(" ") {
    //             vec.push(token.to_string());
    //         }
    //         vec
    //     })
    // }
}
