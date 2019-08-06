use crate::indices::metadata::*;
use crate::{create, error::VelociError, util};
use fnv::FnvHashMap;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PeristenceMetaData {
    pub num_docs: u64,
    pub bytes_indexed: u64,
    pub columns: FnvHashMap<String, FieldInfo>,
}

impl PeristenceMetaData {
    pub fn new(folder: &str) -> Result<PeristenceMetaData, VelociError> {
        let json = util::file_as_string(&(folder.to_string() + "/metaData.json"))?;
        Ok(serde_json::from_str(&json)?)
    }

    pub fn get_all_fields(&self) -> Vec<String> {
        self.columns.keys().map(|el| el.to_string()).collect()
    }
}

/// 'FieldInfo' corresponds to a field (like person.adresses[])and can have multiple indices
/// like person.adresses[].textindex.tokens_to_text_id, person.adresses[].textindex.text_id_to_anchor
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct FieldInfo {
    pub name: String,
    pub textindex_metadata: TextIndexValuesMetadata,
    pub indices: Vec<IndexMetadata>,
    /// special case when text_id equals document id
    pub is_identity_column: bool,
    pub has_fst: bool,
}



#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TextIndexValuesMetadata {
    pub num_text_ids: usize,
    pub num_long_text_ids: usize,
    pub options: create::FulltextIndexOptions,
}


#[derive(Debug)]
enum IndexType {
    TokensToTextID,
    TextIDToTokenIds,
    ToAnchorIDScore,
    PhrasePairToAnchor,
    ValueIDToParent,
    ParentToValueID,
    TextIDToAnchor,
    ParentToTextID,
    AnchorToTextID,
    BoostValidToValue,
    TokenValues,
}
