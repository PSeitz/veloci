#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum LoadingType {
    InMemory,
    Disk,
}

impl Default for LoadingType {
    fn default() -> LoadingType {
        LoadingType::InMemory
    }
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
    IndirectIM,
    IndexIdToOneParent,
}

impl Default for IndexCardinality {
    fn default() -> IndexCardinality {
        IndexCardinality::IndirectIM
    }
}
