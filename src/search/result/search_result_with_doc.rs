use crate::search::{result::explain::Explain, Hit};
use fnv::FnvHashMap;

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct SearchResultWithDoc {
    pub num_hits: u64,
    pub data: Vec<DocWithHit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<FnvHashMap<String, Vec<(String, usize)>>>,
}

// impl SearchResultWithDoc {
//     pub fn merge(&mut self, other: &SearchResultWithDoc) {
//         self.num_hits += other.num_hits;
//         self.data.extend(other.data.iter().cloned());
//         // if let Some(mut facets) = self.facets {  //TODO FACETS MERGE
//         //     // facets.extend()
//         // }
//     }
// }

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DocWithHit {
    pub doc: serde_json::Value,
    pub hit: Hit,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain: Option<Vec<Explain>>,
    #[serde(skip_serializing_if = "FnvHashMap::is_empty")]
    pub why_found: FnvHashMap<String, Vec<String>>,
}
