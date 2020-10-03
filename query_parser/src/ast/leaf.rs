#[derive(Clone, Copy, PartialEq, Eq)]
pub struct UserFilter<'a> {
    /// the search term
    pub phrase: &'a str,
    /// levenshtein edit distance https://en.wikipedia.org/wiki/Levenshtein_distance
    pub levenshtein: Option<u8>,
}

impl<'a> std::fmt::Debug for UserFilter<'a> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        if let Some(levenshtein) = self.levenshtein {
            write!(formatter, "\"{}\"~{:?}", self.phrase, levenshtein)
        } else {
            write!(formatter, "\"{}\"", self.phrase)
        }
    }
}