
#[derive(Clone, PartialEq, Eq)]
pub struct UserFilter {
    /// the search term
    pub phrase: String,
    /// levenshtein edit distance https://en.wikipedia.org/wiki/Levenshtein_distance
    pub levenshtein: Option<u8>,
}

impl std::fmt::Debug for UserFilter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        if let Some(levenshtein) = self.levenshtein {
            write!(formatter, "\"{}\"~{:?}", self.phrase, levenshtein)
        } else {
            write!(formatter, "\"{}\"", self.phrase)
        }
    }
}