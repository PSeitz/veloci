use crate::{
    directory::Directory,
    error::VelociError,
    indices::metadata::*,
    tokenizer::{Tokenizer, *},
};
use fnv::{FnvHashMap, FnvHashSet};
use std::{path::Path, sync::Arc};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PeristenceMetaData {
    pub num_docs: u64,
    pub bytes_indexed: u64,
    pub columns: FnvHashMap<String, FieldInfo>,
}

impl PeristenceMetaData {
    pub fn new(directory: &Box<dyn Directory>) -> Result<PeristenceMetaData, VelociError> {
        let json_bytes = directory.get_file_bytes(&Path::new("metaData.json"))?;
        dbg!(json_bytes.len());
        let mut obj: PeristenceMetaData = serde_json::from_slice(json_bytes.as_slice())?;

        for val in obj.columns.values_mut() {
            val.textindex_metadata.options.create_tokenizer(); //  TODO reuse default tokenizer
        }
        Ok(obj)
    }

    pub fn get_all_fields(&self) -> Vec<String> {
        self.columns.keys().map(|el| el.to_string()).collect()
    }
}

/// 'FieldInfo' corresponds to a field (like person.adresses[]) and can have multiple indices associated
/// like person.adresses[].textindex.tokens_to_text_id, person.adresses[].textindex.text_id_to_anchor
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct FieldInfo {
    pub name: String,
    pub textindex_metadata: TextIndexValuesMetadata,
    pub indices: Vec<IndexMetadata>,
    /// special case when text_id equals document id
    pub is_anchor_identity_column: bool,
    pub has_fst: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FulltextIndexOptions {
    pub tokenize: bool,
    // #[serde(default = "default_tokenizer")]
    // pub tokenizer: TokenizerStrategy,
    #[serde(skip)]
    pub tokenizer: Option<Arc<dyn Tokenizer>>, // TODO use arc properly or remove it, currently each FulltextIndexOptions has its own tokenizer
    pub tokenize_on_chars: Option<Vec<char>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stopwords: Option<FnvHashSet<String>>,
    #[serde(default = "default_text_length_store")]
    pub do_not_store_text_longer_than: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum TokenizerStrategy {
    Simple,
    Jp,
}

fn default_text_length_store() -> usize {
    64
}
impl Default for FulltextIndexOptions {
    fn default() -> FulltextIndexOptions {
        let mut obj = FulltextIndexOptions {
            tokenize: true,
            stopwords: None,
            tokenize_on_chars: None,
            // tokenizer: TokenizerStrategy::Simple,
            tokenizer: None,
            do_not_store_text_longer_than: default_text_length_store(),
        };
        obj.create_tokenizer();
        obj
    }
}

impl FulltextIndexOptions {
    pub fn new_with_tokenize() -> FulltextIndexOptions {
        let mut obj = FulltextIndexOptions {
            tokenize: true,
            ..Default::default()
        };
        obj.create_tokenizer();
        obj
    }

    pub fn create_tokenizer(&mut self) {
        if self.tokenize {
            if let Some(tokenize_on_chars) = &self.tokenize_on_chars {
                let t = SimpleTokenizerCharsIterateGroupTokens {
                    seperators: tokenize_on_chars.to_vec(),
                };
                self.tokenizer = Some(Arc::new(t));
            } else {
                self.tokenizer = Some(Arc::new(SimpleTokenizerCharsIterateGroupTokens::default()));
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TextIndexValuesMetadata {
    pub num_text_ids: usize,
    pub num_long_text_ids: usize,
    pub options: FulltextIndexOptions,
}
