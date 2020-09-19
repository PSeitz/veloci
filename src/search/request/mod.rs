pub mod boost_request;
pub mod facet_request;
pub mod search_request;
pub mod snippet_info;

use crate::search::*;
pub use boost_request::*;
pub use facet_request::*;
pub use search_request::*;
pub use snippet_info::*;

/// Internal and External structure for requests. Suitable for easy requests.
/// For more complex requests, e.g. with phrase boost, currently the convenience api `query_generator` is recommended.
#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct Request {
    /// or/and/search tree
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_req: Option<SearchRequest>,

    #[serde(skip_serializing_if = "Option::is_none")]
    /// or/and/search and suggest are mutually exclusive
    pub suggest: Option<Vec<RequestSearchPart>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<Vec<RequestBoostPart>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost_term: Option<Vec<RequestSearchPart>>,

    /// Will return facets `SearchResult`]: ../SearchResult.html for the specified fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<Vec<FacetRequest>>,

    /// list of requests tuples to phrase boost
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phrase_boosts: Option<Vec<RequestPhraseBoost>>,

    /// only return selected fields
    /// When select is enabled, the selected fields will be reconstructed from the indices.
    /// When select is not enabled, the document will be read from the compressed doc_store.
    pub select: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    /// filter does not affect the score, it just filters the result
    pub filter: Option<Box<SearchRequest>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default = "default_top")]
    pub top: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default = "default_skip")]
    pub skip: Option<usize>,

    /// Enables highlighting to show where in the text the tokens have hit.
    ///
    /// Applies only for searches.
    ///
    /// each document in [`SearchResult`] hit will contain informatio with a list of hightlighted text per field
    /// When select is enabled, the selected field will be reconstructed from the indices and why_found will be active during reconstruction.
    /// When select is not enabled, why_found will tokenize the hits and apply hightlighting with the list of term hits on the field.
    /// see also test boost_text_localitaet in tests folder
    #[serde(skip_serializing_if = "skip_false")]
    #[serde(default)]
    pub why_found: bool,

    /// text locality is when multiple tokens will hit in the same text
    ///
    /// Applies only for searches.
    ///
    /// e.g. if you have 2 documents with an array of texts with:
    /// doc1: ["my nice search engine"]
    /// doc2: ["my nice", "search engine"]
    /// search terms: "nice" and "engine"
    /// with `text_locality` doc1 will get a boost, because the terms are considered "closer"
    /// see also test boost_text_localitaet in tests folder
    ///
    /// default is false. this will add an additional index lookup (tokens_to_text_id) for all token hits
    #[serde(skip_serializing_if = "skip_false")]
    #[serde(default)]
    pub text_locality: bool,

    /// will try to explain the scores, some cases are not yet covered by explain
    #[serde(skip_serializing_if = "skip_false")]
    #[serde(default)]
    pub explain: bool,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug, Hash, PartialEq, Eq)]
pub struct RequestPhraseBoost {
    pub search1: RequestSearchPart,
    pub search2: RequestSearchPart,
}

// #[test]
// fn test_size() {
//     assert_eq!(std::mem::size_of::<SearchRequest>(), 10);
// }
