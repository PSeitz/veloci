use crate::search::{result::explain::Explain, Hit};
use fnv::FnvHashMap;

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
    pub why_found_terms: FnvHashMap<String, Vec<String>>,
}
