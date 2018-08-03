
use fnv::FnvHashMap;
use fnv::FnvHashSet;

lazy_static! {
    static ref STOPWORDS: FnvHashMap<&'static str, FnvHashSet<String>> = {
        let mut m = FnvHashMap::default();
        m.insert("en", hashset_from_stop_word_list(include_str!("../stopword_lists/en")));
        m.insert("de", hashset_from_stop_word_list(include_str!("../stopword_lists/de")));
        m
    };
}

fn hashset_from_stop_word_list(text:&str) -> FnvHashSet<String> {
    text.lines().map(|el|el.to_string()).collect()
}

pub fn is_stopword(language: &str, text:&str) -> bool {
    STOPWORDS.get(language).unwrap().contains(text)
}

#[test]
fn test_stopword() {
    assert_eq!(is_stopword("de", "und"), true);
    assert_eq!(is_stopword("de", "der"), true);
    assert_eq!(is_stopword("de", "die"), true);
    assert_eq!(is_stopword("de", "das"), true);
    assert_eq!(is_stopword("de", "nixda"), false);

    assert_eq!(is_stopword("en", "in"), true);
}