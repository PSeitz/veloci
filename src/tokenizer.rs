use regex::Regex;

pub trait Tokenizer {
    fn get_tokens<F>(&self, orignal:&str, cb_text: &mut F)
        where F: FnMut(&str, bool);    

    fn has_tokens(&self, orignal:&str) -> bool;
}


lazy_static! {
    static ref TOKENIZER:Regex  = Regex::new(r#"([\s\(\),.…;・’\-\[\]{}'"“])+|([^\s\(\),.…;・’\-\[\]{}'"“]*)"#).unwrap();
    // static ref TOKENIZER:Regex  = Regex::new(r#"([\s])+|([^\s]*)"#).unwrap();
    static ref SEPERATORS:Regex = Regex::new(r#"(?P<seperator>[\s\(\),.…;・’\-\[\]{}'"“]+)"#).unwrap();
}


#[derive(Debug)]
pub struct SimpleTokenizer {}
impl Tokenizer for SimpleTokenizer {

    fn get_tokens<F>(&self, orignal:&str, cb_text: &mut F)
    where F: FnMut(&str, bool)
    {
        for cap in TOKENIZER.captures_iter(orignal) {
            cb_text(&cap[0], *&cap.get(1).is_some());
            // println!("{:?} {:?}", &cap[0], &cap.get(1).is_some());
            //println!("Month: {} Day: {} Year: {}", &cap[2], &cap[3], &cap[1]);
        }
    }

    fn has_tokens(&self, orignal:&str) -> bool{
        SEPERATORS.is_match(orignal)
    }
}

#[allow(unused_imports)]
use test;


#[test]
fn test_tokenizer_control_sequences() {
    let tokenizer = SimpleTokenizer{};
    let mut vec:Vec<String> = vec![];
    tokenizer.get_tokens("das \n ist ein txt, test", &mut |token:&str, _is_seperator: bool|{
        vec.push(token.to_string());
    });

    // for el in vec {
    //     print!("{}", el);
    // }
}

#[bench]
fn bench_iter(b: &mut test::Bencher) {
    b.iter(|| {

        let mut vec:Vec<String> = vec![];
        for cap in TOKENIZER.captures_iter("das ist ein txt, test") {
            // cb_text(&cap[0], *&cap.get(1).is_some());
            vec.push(cap[0].to_string());
        }
        vec
    })
}

#[bench]
fn bench_closure(b: &mut test::Bencher) {
    let tokenizer = SimpleTokenizer{};

    b.iter(||{
        let mut vec:Vec<String> = vec![];
        tokenizer.get_tokens("das ist ein txt, test", &mut |token:&str, _is_seperator: bool|{
            vec.push(token.to_string());
        });
        vec
    })
}

#[bench]
fn bench_closure_box(b: &mut test::Bencher) {
    let tokenizer = Box::new(SimpleTokenizer{});

    b.iter(||{
        let mut vec:Vec<String> = vec![];
        tokenizer.get_tokens("das ist ein txt, test", &mut |token:&str, _is_seperator: bool|{
            vec.push(token.to_string());
        });
        vec
    })
}

#[bench]
fn bench_no_regex(b: &mut test::Bencher) {
    b.iter(||{
        let mut vec:Vec<String> = vec![];
        for token in "das ist ein txt, test".split(" ") {
            vec.push(token.to_string());
        }
        vec
    })
}
#[allow(unused_imports)]
use util;
#[bench]
fn bench_normalize_and(b: &mut test::Bencher) {
    b.iter(||{
        let mut vec:Vec<String> = vec![];
        for token in util::normalize_text("das ist ein txt, test").split(" ") {
            vec.push(token.to_string());
        }
        vec
    })
}