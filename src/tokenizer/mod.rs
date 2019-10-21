mod simple_tokenizer;
pub use simple_tokenizer::*;
use std::fmt::Debug;

mod simple_tokenizer_group;
pub use simple_tokenizer_group::*;

pub trait Tokenizer: Debug + Sync + Send {
    // fn get_tokens<'a, F>(&self, original: &'a str, cb_text: &mut F)
    // where
    //     F: FnMut(&'a str, bool);

    fn has_tokens(&self, original: &str) -> bool;

    /// iterator, with bool to flag seperators
    fn iter<'a>(&'a self, original: &'a str) -> Box<dyn Iterator<Item = (&'a str, bool)> + 'a>;
}

use regex::Regex;
lazy_static! {
    // static ref TOKENIZER:Regex  = Regex::new(r#"([\s])+|([^\s]*)"#).unwrap();
    static ref SEPERATORS:Regex = Regex::new(r#"(?P<seperator>[\s:\(\),.…;・’\-\[\]{}<>'"“]+)"#).unwrap();
}

pub const DEFAULT_SEPERATORS: [char; 26] = [
    ' ', '\t', '\n', '\r', ':', '(', ')', ',', '.', '…', ';', '・', '’', '—', '-', '\\', '[', ']', '{', '}', '<', '>', '\'', '"', '“', '™',
];

fn is_default_seperator(char: char) -> bool {
    match char {
        ' ' | '\t' | '\n' | '\r' | ':' | '(' | ')' | ',' | '.' | '…' | ';' | '・' | '’' | '—' | '-' | '\\' | '[' | ']' | '{' | '}' | '<' | '>' | '\'' | '"' | '“' | '™' => {
            true
        }
        _ => false,
    }
}

// pub trait TokenizerIter {
//     fn iter_tokens(&self) -> SimpleTokenizerGroupTokenIter;
//     fn has_tokens(&self) -> bool;
// }

// impl TokenizerIter for &str {
//     fn iter_tokens(&self) -> SimpleTokenizerGroupTokenIter {
//         SimpleTokenizerGroupTokenIter {
//             original: self,
//             last_returned_byte: 0,
//             last_was_token: false,
//             char_iter: self.char_indices(),
//         }
//     }

//     fn has_tokens(&self) -> bool {
//         SEPERATORS.is_match(self)
//     }
// }

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    #[allow(unused_imports)]
    use crate::test;

    use std::{fs::File, io::prelude::*};

    #[allow(dead_code)]
    fn get_test_book() -> String {
        let mut f = File::open("test_files/1342-0.txt").unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();
        s
    }

    #[test]
    fn test_tokenizer_control_sequences_grouped() {
        let tokenizer = SimpleTokenizerCharsIterateGroupTokens::default();
        let vec: Vec<_> = tokenizer.iter("das \n ist ein txt, test").map(|(token, _is_seperator)| token).collect();
        assert_eq!(vec, vec!["das", " \n ", "ist", " ", "ein", " ", "txt", ", ", "test"])
    }
    #[test]
    fn test_tokenizer_iter_control_sequences_grouped() {
        let tokenizer = SimpleTokenizerCharsIterateGroupTokens::default();
        let vec: Vec<&str> = tokenizer.iter("das \n ist ein txt, test").map(|el| el.0).collect();
        assert_eq!(vec, vec!["das", " \n ", "ist", " ", "ein", " ", "txt", ", ", "test"])
    }
    #[test]
    fn test_tokenizer_control_sequences_alt() {
        let tokenizer = SimpleTokenizer {};
        assert_eq!(tokenizer.has_tokens("das \n ist ein txt, test"), true);
        let vec: Vec<_> = tokenizer.iter("das \n ist ein txt, test").map(|(token, _is_seperator)| token).collect();
        assert_eq!(vec, vec!["das", " ", "\n", " ", "ist", " ", "ein", " ", "txt", ",", " ", "test"])
    }
    #[test]
    fn test_tokenize_taschenbuch_start_with_seperator() {
        let tokenizer = SimpleTokenizerCharsIterateGroupTokens::default();
        // let mut vec: Vec<String> = vec![];
        let vec: Vec<_> = tokenizer.iter(" Taschenbuch (kartoniert)").map(|(token, _is_seperator)| token).collect();
        // tokenizer.get_tokens(" Taschenbuch (kartoniert)", &mut |token: &str, _is_seperator: bool| {
        //     vec.push(token.to_string());
        // });
        assert_eq!(vec, vec![" ", "Taschenbuch", " (", "kartoniert", ")"])
    }
    #[test]
    fn test_tokenize_start_with_single_token() {
        let tokenizer = SimpleTokenizerCharsIterateGroupTokens::default();
        // let mut vec: Vec<String> = vec![];
        let vec: Vec<_> = tokenizer.iter("T oll").map(|(token, _is_seperator)| token).collect();
        // tokenizer.get_tokens("T oll", &mut |token: &str, _is_seperator: bool| {
        //     vec.push(token.to_string());
        // });
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

    // #[bench]
    // fn bench_tokenizer(b: &mut test::Bencher) {
    //     let tokenizer = SimpleTokenizer {};
    //     let text = get_test_book();
    //     b.iter(|| {
    //         let mut vec: Vec<String> = vec![];
    //         tokenizer.get_tokens(&text, &mut |token: &str, _is_seperator: bool| {
    //             vec.push(token.to_string());
    //         });
    //         vec
    //     })
    // }
    // #[bench]
    // fn bench_tokenizer_no_copy(b: &mut test::Bencher) {
    //     let tokenizer = SimpleTokenizer {};
    //     let text = get_test_book();
    //     b.iter(|| {
    //         let mut vec = vec![];
    //         tokenizer.get_tokens(&text, &mut |token: &str, _is_seperator: bool| {
    //             vec.push(token);
    //         });
    //         vec
    //     })
    // }

    // #[bench]
    // fn bench_tokenizer_grouped_no_copy(b: &mut test::Bencher) {
    //     let tokenizer = SimpleTokenizerCharsIterateGroupTokens::default();
    //     let text = get_test_book();
    //     b.iter(|| {
    //         let mut vec = vec![];
    //         tokenizer.get_tokens(&text, &mut |token: &str, _is_seperator: bool| {
    //             vec.push(token);
    //         });
    //         vec
    //     })
    // }

    #[bench]
    fn bench_tokenizer_grouped_iter_no_copy(b: &mut test::Bencher) {
        let tokenizer = SimpleTokenizerCharsIterateGroupTokens::default();
        let text = get_test_book();
        b.iter(|| {
            // let mut vec = vec![];
            let vec: Vec<&str> = tokenizer.iter(&text).map(|(token, _is_seperator)| token).collect();
            vec
        })
    }

    #[bench]
    fn bench_tokenizer_grouped_no_box_iter_no_copy(b: &mut test::Bencher) {
        // let tokenizer = SimpleTokenizerCharsIterateGroupTokens::default();
        let text = get_test_book();
        b.iter(|| {
            // let mut vec = vec![];
            let vec: Vec<&str> = SimpleTokenizerGroupTokenIter::from_str(&text, &DEFAULT_SEPERATORS)
                .map(|(token, _is_seperator)| token)
                .collect();
            vec
        })
    }

    // #[bench]
    // fn bench_tokenizer_grouped_no_copy_2(b: &mut test::Bencher) {
    //     use super::TokenizerIter;
    //     let text = get_test_book();
    //     let texto = text.as_str();
    //     b.iter(|| texto.iter_tokens().map(|el| el.0).collect::<Vec<&str>>())
    // }

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
            let mut vec: Vec<String> = vec![];
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
            let mut vec: Vec<&str> = vec![];
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
