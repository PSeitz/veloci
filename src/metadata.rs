use crate::create;
use crate::error::VelociError;
use crate::persistence::*;
use crate::util;
use fnv::FnvHashMap;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PeristenceMetaData {
    pub num_docs: u64,
    pub bytes_indexed: u64,
    pub columns: FnvHashMap<String, ColumnInfo>,
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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ColumnInfo {
    pub name: String,
    pub textindex_metadata: TextIndexValuesMetadata,
    pub indices: Vec<IndexMetadata>,
    /// special case when text_id equals document id
    pub is_identity_column: bool,
    pub has_fst: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum IndexCategory {
    Boost,
    KeyValue,
    AnchorScore,
    Phrase,
}
impl Default for IndexCategory {
    fn default() -> IndexCategory {
        IndexCategory::KeyValue
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct IndexMetadata {
    pub path: String,
    pub index_category: IndexCategory,
    pub index_cardinality: IndexCardinality,
    #[serde(default)]
    pub is_empty: bool,
    pub loading_type: LoadingType,
    pub metadata: IndexValuesMetadata,
    pub data_type: DataType,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum DataType {
    U32,
    U64,
}

impl Default for DataType {
    fn default() -> DataType {
        DataType::U32
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum IndexCardinality {
    IndexIdToMultipleParentIndirect,
    IndexIdToOneParent,
}

impl Default for IndexCardinality {
    fn default() -> IndexCardinality {
        IndexCardinality::IndexIdToMultipleParentIndirect
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TextIndexValuesMetadata {
    pub num_text_ids: usize,
    pub num_long_text_ids: usize,
    pub options: create::FulltextIndexOptions,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy, PartialEq)]
pub struct IndexValuesMetadata {
    /// max value on the "right" side key -> value, key -> value ..
    pub max_value_id: u32,
    pub avg_join_size: f32,
    pub num_values: u64,
    pub num_ids: u32,
}

impl IndexValuesMetadata {
    pub fn new(max_value_id: u32) -> Self {
        IndexValuesMetadata {
            max_value_id,
            ..Default::default()
        }
    }
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
