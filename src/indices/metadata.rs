#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Default)]
pub enum LoadingType {
    #[default]
    InMemory,
    Disk,
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

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Default)]
pub enum IndexCategory {
    Boost,
    #[default]
    KeyValue,
    AnchorScore,
    Phrase,
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub enum DataType {
    #[default]
    U32,
    U64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub enum IndexCardinality {
    #[default]
    IndirectIM,
    IndexIdToOneParent,
}
