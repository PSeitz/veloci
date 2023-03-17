use super::features::{Features, IndexCreationType};
use crate::{error::VelociError, metadata::FulltextIndexOptions};

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
        let path = path.strip_suffix(".textindex").unwrap_or(path);
        let el = self.0.get(path);
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

            if let Some(features) = val.features.clone().or_else(|| val.disabled_features.as_ref().map(Features::invert)) {
                let disabled = Features::features_to_disabled_indices(&features);
                let mut existing = val.disabled_indices.as_ref().cloned().unwrap_or_default();
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

pub fn config_from_string(indices: &str) -> Result<FieldsConfig, VelociError> {
    let mut configs = if indices.trim().starts_with('{') {
        serde_json::from_str(indices)?
    } else {
        let map: FnvHashMap<String, FieldConfig> = toml::from_str(indices)?;
        FieldsConfig(map)
    };
    for value in &mut configs.0.values_mut() {
        if let Some(fulltext) = &mut value.fulltext {
            fulltext.create_tokenizer();
            // if let Some(fulltext) = &fulltext.tokenize_on_chars {
            // }
        }
    }
    Ok(configs)
}

#[test]
fn test_field_config_from_json() {
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
    let mut data: FieldsConfig = config_from_string(json).unwrap();
    data.features_to_indices().unwrap();
    assert!(data.get("MATNR").facet);
    assert!(!data.get("MATNR").is_index_enabled(IndexCreationType::TokensToTextID));
    assert!(data.get("ISMTITLE").is_index_enabled(IndexCreationType::TokenToAnchorIDScore));
    assert!(!data.get("ISMTITLE").is_index_enabled(IndexCreationType::TokensToTextID));
    assert!(!data.get("ISMORIDCODE").fulltext.as_ref().unwrap().tokenize);
}

#[test]
fn test_field_config_from_toml() {
    let indices = r#"
        ["*GLOBAL*"]
            features = ["All"]
        ["commonness"]
            facet = true
        ["commonness".boost]
            boost_type = "int"
        ["ent_seq".fulltext]
            tokenize = true
        ["nofulltext".fulltext]
            tokenize = false
        ["tags[]"]
            facet = true
        ["field1[].rank".boost]
            boost_type = "int"
        ["field1[].text"]
            tokenize = true
        ["kanji[].text"]
            tokenize = true
        ["meanings.ger[]"]
            stopwords = ["stopword"]
            ["meanings.ger[]".fulltext]
                tokenize = true
        ["meanings.eng[]".fulltext]
            tokenize = true
        ["kanji[].commonness".boost]
            boost_type = "int"
        ["kana[].commonness".boost]
            boost_type = "int"
    "#;

    config_from_string(indices).unwrap();
}
