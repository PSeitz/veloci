use crate::search::{result::explain::Explain, Hit};
use fnv::FnvHashMap;

/// SearchResult` is the result form a search, without the document itself
///
/// A search will return a `SearchResult`, it will contain everything to build
/// the final `SearchResultWithDoc`.
#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct SearchResult {
    pub execution_time_ns: u64,
    pub num_hits: u64,
    pub data: Vec<Hit>,
    pub ids: Vec<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<FnvHashMap<String, Vec<(String, usize)>>>,
    #[serde(skip_serializing_if = "FnvHashMap::is_empty")]
    pub explain: FnvHashMap<u32, Vec<Explain>>,
    #[serde(skip_serializing_if = "FnvHashMap::is_empty")]
    pub why_found_info: FnvHashMap<u32, FnvHashMap<String, Vec<String>>>,
    #[serde(skip_serializing_if = "FnvHashMap::is_empty")]
    /// This is used for a fast why_found highlighting, by storing the matched tokens per field
    /// and using the tokens to highlight on the complete document (`highlight_on_original_document`)
    ///
    /// The other solution is to read all tokens of a document and rebuild the document while highlighting. This is much more costly.
    pub why_found_terms: FnvHashMap<String, Vec<String>>,
}
