#[derive(Serialize, Deserialize, Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct SnippetInfo {
    #[serde(default = "default_num_words_around_snippet")]
    pub num_words_around_snippet: i64,
    #[serde(default = "default_snippet_start")]
    pub snippet_start_tag: String,
    #[serde(default = "default_snippet_end")]
    pub snippet_end_tag: String,
    #[serde(default = "default_snippet_connector")]
    pub snippet_connector: String,
    #[serde(default = "default_max_snippets")]
    pub max_snippets: u32,
}

fn default_num_words_around_snippet() -> i64 {
    5
}
fn default_snippet_start() -> String {
    "<b>".to_string()
}
fn default_snippet_end() -> String {
    "</b>".to_string()
}
fn default_snippet_connector() -> String {
    " ... ".to_string()
}
fn default_max_snippets() -> u32 {
    std::u32::MAX
}

lazy_static! {
    pub(crate) static ref DEFAULT_SNIPPETINFO: SnippetInfo = SnippetInfo {
        num_words_around_snippet: default_num_words_around_snippet(),
        snippet_start_tag: default_snippet_start(),
        snippet_end_tag: default_snippet_end(),
        snippet_connector: default_snippet_connector(),
        max_snippets: default_max_snippets(),
    };
}
