

static DATA:&str = r#"[
    {                                           // anchor id 0
        "commonness": 20,
        "kanji": [
            { "text": "偉容", "commonness": 0}, // kanji id 0
            { "text": "威容","commonness": 5}   // kanji id 1
        ],
        "kana": [
            {
                "text": "いよう",
                "romaji": "Iyou",
                "commonness": 5,
            }
        ],
        "meanings": {   // meanings id 0
            "eng" : ["dignity", "majestic appearance", "will test"],
            "ger": ["majestätischer Anblick (m)", "majestätisches Aussehen (n)", "Majestät (f)"] // meanings.ger id 0, 1, 2 ..
        },
        "ent_seq": "1587680"
    },
    {                                           // anchor id 1
        "commonness": 20,
        "kanji": [
            { "text": "意欲", "commonness": 40}, // kanji id 2
            { "text": "意慾", "commonness": 0}   // kanji id 3
        ],
        "kana": [
            {
                "text": "いよく",
                "romaji": "Iyoku",
                "commonness": 40,
            }
        ],
        "meanings": { // meanings id 1
            "eng" : ["will", "desire", "urge", "having a long torso"],
            "ger": ["Wollen (n)", "Wille (m)", "Begeisterung (f)"] // meanings.ger id .. 5, 6 7
        },
        "ent_seq": "1587690"
    },
    {
        "commonness": 500,                                 // anchor id 2
        "kanji": [
            { "text": "意慾", "commonness": 20}   // kanji id 4
        ],
        "field1" : [{text:"awesome", rank:1}],
        "kana": [
            {
                "text": "いよく",
            }
        ],
        "meanings": { // meanings id 2
            "eng" : ["test1"],
            "ger": ["der test"] // meanings.ger id ..
        },
        "ent_seq": "1587700"
    },
    {
        "commonness": 551,
        "kanji": [
            {
                "text": "何の",
                "commonness": 526
            }
        ],
        "field1" : [{text:"awesome"}, {text:"nixhit"}],
        "kana": [
            {
                "text": "どの",
                "romaji": "Dono",
                "commonness": 25
            }
        ],
        "meanings": {
            "ger": [
                "welch"
            ]
        },
        "ent_seq": "1920240"
    },
    {
        "pos": [
            "adj-i"
        ],
        "commonness": 1,
        "misc": [],
        "kanji": [
            {
                "text": "柔らかい",
                "commonness": 57
            }
        ],
        "kana": [
            {
                "text": "やわらかい",
                "romaji": "Yawarakai",
                "commonness": 30
            }
        ],
        "meanings": {
            "ger": [
                "(1) weich",
                "stopword"
            ]
        },
        "ent_seq": "1605630"
    }
]"#;

#[cfg(test)]
mod tests {
    use util::normalize_text;

    #[test]
    fn it__super_duper_works() {
        assert_eq!(normalize_text("Hello"), "Hello");
    }

}
