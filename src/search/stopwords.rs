use fnv::{FnvHashMap, FnvHashSet};

static EN: &str = include_str!("../../stopword_lists/en");
static DE: &str = include_str!("../../stopword_lists/de");

lazy_static! {
    static ref STOPWORDS: FnvHashMap<&'static str, FnvHashSet<String>> = {
        let mut m = FnvHashMap::default();
        m.insert("en", hashset_from_stop_word_list(EN));
        m.insert("de", hashset_from_stop_word_list(DE));
        m
    };
}

fn hashset_from_stop_word_list(text: &str) -> FnvHashSet<String> {
    text.lines().map(|el| el.to_lowercase()).collect()
}

//TODO: EROR HANDLING
pub fn is_stopword(language: &str, text: &str) -> bool {
    let language = language.to_lowercase();
    STOPWORDS.get(&language as &str).unwrap().contains(text)
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
