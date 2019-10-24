#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Explain {
    Boost(f32),
    MaxTokenToTextId(f32),
    TermToAnchor {
        term_score: f32,
        anchor_score: f32,
        final_score: f32,
        term_id: u32,
    },
    LevenshteinScore {
        score: f32,
        text_or_token_id: String,
        term_id: u32,
    },
    OrSumOverDistinctTerms(f32),
    NumDistintTermsBoost {
        distinct_boost: u32,
        new_score: u32,
    },
}
