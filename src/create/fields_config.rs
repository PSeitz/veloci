use crate::metadata::FulltextIndexOptions;
use super::features::{Features, IndexCreationType};
use crate::error::VelociError;

use fnv::{FnvHashMap, FnvHashSet};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateIndexConfig {
    fields_config: FnvHashMap<String, FieldConfig>,
    #[serde(default)]
    /// This can be used e.g. for documents, when only why found or snippets are used
    do_not_store_document: bool,
}

const ALL_FIELD_CONFIG: &str = "*GLOBAL*";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FieldsConfig(FnvHashMap<String, FieldConfig>);
impl FieldsConfig {
    pub fn get(&self, path: &str) -> &FieldConfig {
        let el = if path.ends_with(".textindex") {
            self.0.get(&path[..path.len() - 10])
        } else {
            self.0.get(path)
        };
        if let Some(el) = el {
            el
        } else {
            &self.0[ALL_FIELD_CONFIG]
        }
    }

    pub fn features_to_indices(&mut self) -> Result<(), VelociError> {
        if self.0.get(ALL_FIELD_CONFIG).is_none() {
            let default_field_config = FieldConfig::default();
            self.0.insert(ALL_FIELD_CONFIG.to_string(), default_field_config);
        }
        for (key, val) in self.0.iter_mut() {
            if val.features.is_some() && val.disabled_features.is_some() {
                return Err(VelociError::InvalidConfig(format!(
                    "features and disabled_features are not allowed at the same time in field {:?}",
                    key
                )));
            }

            if let Some(features) = val
                .features
                .clone()
                .or_else(|| val.disabled_features.as_ref().map(|disabled_features| Features::invert(disabled_features)))
            {
                let disabled = Features::features_to_disabled_indices(&features);
                let mut existing = val.disabled_indices.as_ref().cloned().unwrap_or_else(FnvHashSet::default);
                existing.extend(disabled);
                val.disabled_indices = Some(existing);
            }
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FieldConfig {
    #[serde(default)]
    pub facet: bool,
    pub fulltext: Option<FulltextIndexOptions>,
    pub disabled_indices: Option<FnvHashSet<IndexCreationType>>,
    pub features: Option<FnvHashSet<Features>>,
    pub disabled_features: Option<FnvHashSet<Features>>,
    pub boost: Option<BoostIndexOptions>,
}

impl Default for FieldConfig {
    fn default() -> FieldConfig {
        FieldConfig {
            facet: false,
            features: Some(Features::get_default_features()),
            disabled_features: None,
            fulltext: Some(FulltextIndexOptions::new_with_tokenize()),
            disabled_indices: None,
            boost: None,
        }
    }
}

impl FieldConfig {
    pub fn is_index_enabled(&self, index: IndexCreationType) -> bool {
        self.disabled_indices.as_ref().map(|el| !el.contains(&index)).unwrap_or(true)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BoostIndexOptions {
    boost_type: String,
}


#[test]
fn test_field_config_from_json() {
    use serde_json;
    let json = r#"{
        "MATNR" : {
           "facet":true,
           "fulltext" : {"tokenize":true},
           "disabled_indices": ["TokensToTextID", "TokenToAnchorIDScore", "PhrasePairToAnchor", "TextIDToTokenIds", "TextIDToParent", "ParentToTextID", "TextIDToAnchor"]
        },
        "ISMTITLE"     : {"fulltext": {"tokenize":true}, "features":["Search"]  },
        "ISMORIGTITLE" : {"fulltext": {"tokenize":true}, "disabled_features":["Search"]  },
        "ISMSUBTITLE1" : {"fulltext": {"tokenize":true}  },
        "ISMSUBTITLE2" : {"fulltext": {"tokenize":true}  },
        "ISMSUBTITLE3" : {"fulltext": {"tokenize":true}  },
        "ISMARTIST"    : {"fulltext": {"tokenize":true}  },
        "ISMLANGUAGES" : {"fulltext": {"tokenize":false} },
        "ISMPUBLDATE"  : {"fulltext": {"tokenize":false} },
        "EAN11"        : {"fulltext": {"tokenize":false} },
        "ISMORIDCODE"  : {"fulltext": {"tokenize":false} }
    }"#;
    let mut data: FieldsConfig = serde_json::from_str(json).unwrap();
    data.features_to_indices().unwrap();
    assert_eq!(data.get("MATNR").facet, true);
    assert_eq!(data.get("MATNR").is_index_enabled(IndexCreationType::TokensToTextID), false);
    assert_eq!(data.get("ISMTITLE").is_index_enabled(IndexCreationType::TokenToAnchorIDScore), true);
    assert_eq!(data.get("ISMTITLE").is_index_enabled(IndexCreationType::TokensToTextID), false);
    assert_eq!(data.get("ISMORIDCODE").fulltext.as_ref().unwrap().tokenize, false);
}
