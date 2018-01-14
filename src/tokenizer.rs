use regex::Regex;

pub trait Tokenizer {
    fn get_tokens<'a, F>(&self, orignal: &'a str, cb_text: &mut F)
    where
        F: FnMut(&'a str, bool);

    fn has_tokens(&self, orignal: &str) -> bool;
}

lazy_static! {
    static ref TOKENIZER:Regex  = Regex::new(r#"([\s\(\),.…;・’\-\[\]{}'"“])+|([^\s\(\),.…;・’\-\[\]{}'"“]*)"#).unwrap();
    // static ref TOKENIZER:Regex  = Regex::new(r#"([\s])+|([^\s]*)"#).unwrap();
    static ref SEPERATORS:Regex = Regex::new(r#"(?P<seperator>[\s\(\),.…;・’\-\[\]{}'"“]+)"#).unwrap();
}

#[derive(Debug)]
pub struct SimpleTokenizer {}
impl Tokenizer for SimpleTokenizer {
    fn get_tokens<'a, F>(&self, orignal: &'a str, cb_text: &mut F)
    where
        F: FnMut(&'a str, bool),
    {
        for cap in TOKENIZER.captures_iter(orignal) {
            // cb_text(&cap[0], *&cap.get(1).is_some());
            // println!("{:?} {:?}", &cap[0], &cap.get(1).is_some());
            //println!("Month: {} Day: {} Year: {}", &cap[2], &cap[3], &cap[1]);
        }
    }

    fn has_tokens(&self, orignal: &str) -> bool {
        SEPERATORS.is_match(orignal)
    }
}

#[derive(Debug)]
pub struct SimpleTokenizerCharsIterate {}
impl Tokenizer for SimpleTokenizerCharsIterate {
    fn get_tokens<'a, F>(&self, orignal: &'a str, cb_text: &mut F)
    where
        F: FnMut(&'a str, bool),
    {
        let mut last_byte_pos = 0;
        for (pos, char) in orignal.char_indices(){
            match char {
                ' '
                | '\t' | '\n' | '\r'
                |'('
                |')'
                |','
                |'.'
                |'…'
                |';'
                |'・'
                |'’'
                |'-'
                |'\\'
                |'{'
                |'}'
                |'\''
                |'"'
                |'“' => {
                    if pos != last_byte_pos {
                        cb_text(&orignal[last_byte_pos..pos], false);
                    }
                    let next_pos = pos+ char.len_utf8();
                    cb_text(&orignal[pos..next_pos], false);
                    last_byte_pos = next_pos;
                },
                _ => {},
            }
        }

        if last_byte_pos != orignal.len() {
            cb_text(&orignal[last_byte_pos..orignal.len()], false);
        }
    }

    fn has_tokens(&self, orignal: &str) -> bool {
        SEPERATORS.is_match(orignal)
    }
}
#[derive(Debug)]
pub struct SimpleTokenizerCharsIterateGroupTokens {}
impl Tokenizer for SimpleTokenizerCharsIterateGroupTokens {
    fn get_tokens<'a, F>(&self, orignal: &'a str, cb_text: &mut F)
    where
        F: FnMut(&'a str, bool),
    {
        let mut last_returned_byte = 0;
        let mut last_was_token = false;
        for (pos, char) in orignal.char_indices(){
            match char {
                ' '
                | '\t' | '\n' | '\r'
                |'('
                |')'
                |','
                |'.'
                |'…'
                |';'
                |'・'
                |'’'
                |'-'
                |'\\'
                |'{'
                |'}'
                |'\''
                |'"'
                |'“' => {
                    if !last_was_token {
                        cb_text(&orignal[last_returned_byte..pos], false);
                        last_was_token = true;
                        last_returned_byte = pos;
                    }
                    
                },
                _ => {
                    if last_was_token {
                        cb_text(&orignal[last_returned_byte..pos], true);
                        last_was_token = false;
                        last_returned_byte = pos;
                    }

                },
            }
        }

        if last_returned_byte != orignal.len() {
            cb_text(&orignal[last_returned_byte..orignal.len()], last_was_token);
        }
    }

    fn has_tokens(&self, orignal: &str) -> bool {
        SEPERATORS.is_match(orignal)
    }
}

#[allow(unused_imports)]
use test;

use std::fs::File;
use std::io::prelude::*;

fn get_test_book() -> String {
    let mut f = File::open("1342-0.txt").unwrap();
    let mut s = String::new();
    f.read_to_string(&mut s).unwrap();
    s
}

#[test]
fn test_tokenizer_control_sequences_grouped() {
    let tokenizer = SimpleTokenizerCharsIterateGroupTokens {};
    let mut vec: Vec<String> = vec![];
    tokenizer.get_tokens(
        "das \n ist ein txt, test",
        &mut |token: &str, _is_seperator: bool| {
            vec.push(token.to_string());
        },
    );
    assert_eq!(vec, vec!["das",
                        " \n ",
                        "ist",
                        " ",
                        "ein",
                        " ",
                        "txt",
                        ", ",
                        "test"])

}
#[test]
fn test_tokenizer_control_sequences_alt() {
    let tokenizer = SimpleTokenizerCharsIterate {};
    let mut vec: Vec<String> = vec![];
    tokenizer.get_tokens(
        "das \n ist ein txt, test",
        &mut |token: &str, _is_seperator: bool| {
            vec.push(token.to_string());
        },
    );
        assert_eq!(vec, vec!["das",
                        " ",
                        "\n",
                        " ",
                        "ist",
                        " ",
                        "ein",
                        " ",
                        "txt",
                        ",",
                        " ",
                        "test"])

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
// fn bench_regex_closure(b: &mut test::Bencher) {
//     let tokenizer = SimpleTokenizer {};
//     let text = get_test_book();
//     b.iter(|| {
//         let mut vec: Vec<String> = vec![];
//         tokenizer.get_tokens(
//             &text,
//             &mut |token: &str, _is_seperator: bool| {
//                 vec.push(token.to_string());
//             },
//         );
//         vec
//     })
// }

#[bench]
fn bench_custom_stuff(b: &mut test::Bencher) {
    let tokenizer = SimpleTokenizerCharsIterate {};
    let text = get_test_book();
    b.iter(|| {
        let mut vec: Vec<String> = Vec::with_capacity(text.len()/5);
        tokenizer.get_tokens(
            &text,
            &mut |token: &str, _is_seperator: bool| {
                vec.push(token.to_string());
            },
        );
        vec
    })
}
#[bench]
fn bench_custom_stuff_no_copy(b: &mut test::Bencher) {
    let tokenizer = SimpleTokenizerCharsIterate {};
    let text = get_test_book();
    b.iter(|| {
        let mut vec = Vec::with_capacity(text.len()/5);
        tokenizer.get_tokens(
            &text,
            &mut |token: &str, _is_seperator: bool| {
                vec.push(token);
            },
        );
        // vec
    })
}

#[bench]
fn bench_custom_stuff_grouped_no_copy(b: &mut test::Bencher) {
    let tokenizer = SimpleTokenizerCharsIterateGroupTokens {};
    let text = get_test_book();
    b.iter(|| {
        let mut vec = Vec::with_capacity(text.len()/5);
        tokenizer.get_tokens(
            &text,
            &mut |token: &str, _is_seperator: bool| {
                vec.push(token);
            },
        );
        // vec
    })
}

#[bench]
fn bench_regex_has_tokens(b: &mut test::Bencher) {
    let tokenizer = SimpleTokenizer {};
    let text = get_test_book();
    b.iter(|| {
        tokenizer.has_tokens(&text )
    })
}

// #[bench]
// fn bench_regex_closure_box(b: &mut test::Bencher) {
//     let tokenizer = Box::new(SimpleTokenizer {});
//     let text = get_test_book();
//     b.iter(|| {
//         let mut vec: Vec<String> = vec![];
//         tokenizer.get_tokens(
//             &text,
//             &mut |token: &str, _is_seperator: bool| {
//                 vec.push(token.to_string());
//             },
//         );
//         vec
//     })
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
        let mut vec: Vec<String> = Vec::with_capacity(text.len()/5);
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
        let mut vec: Vec<&str> = Vec::with_capacity(text.len()/5);
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
