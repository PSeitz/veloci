use std::path::Path;

use crate::{
    create::buffered_index_to_direct_index,
    error::VelociError,
    indices::{metadata::*, *},
    persistence::{Persistence, *},
    plan_creator::execution_plan::PlanRequestSearchPart,
    search, search_field,
    util::StringAdd,
};
use buffered_index_writer::{self, BufferedIndexWriter};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenValuesConfig {
    path: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct TokenValueData {
    text: String,
    value: Option<f32>,
}

// Add boost values for a list of tokens
pub fn add_token_values_to_tokens(persistence: &mut Persistence, data_str: &str, config: &str) -> Result<(), VelociError> {
    let data: Vec<TokenValueData> = serde_json::from_str(data_str)?;
    let config: TokenValuesConfig = serde_json::from_str(config)?;

    let mut options: search::RequestSearchPart = search::RequestSearchPart {
        path: config.path.clone(),
        levenshtein_distance: Some(0),
        ..Default::default()
    };

    let mut buffered_index_data = BufferedIndexWriter::new_unstable_sorted(persistence.directory.box_clone());

    for el in data {
        if let Some(value) = el.value {
            options.terms = vec![el.text];
            options.ignore_case = Some(false);

            let mut options = PlanRequestSearchPart {
                request: options.clone(),
                get_scores: true,
                ..Default::default()
            };

            let hits = search_field::get_term_ids_in_field(persistence, &mut options)?;
            if !hits.hits_scores.is_empty() {
                buffered_index_data.add(hits.hits_scores[0].id, value.to_bits())?;
            }
        }
    }

    let path = config.path.add(TEXTINDEX).add(TOKEN_VALUES).add(BOOST_VALID_TO_VALUE);
    let mut store = buffered_index_to_direct_index(&persistence.directory, &path, buffered_index_data)?;

    store.flush()?;
    let index_metadata = IndexMetadata {
        index_category: IndexCategory::Boost,
        path: path.to_string(),
        is_empty: store.is_empty(),
        metadata: store.metadata,
        index_cardinality: IndexCardinality::SingleValue,
        data_type: DataType::U32,
    };

    let entry = persistence.metadata.columns.entry(config.path).or_insert_with(|| FieldInfo {
        has_fst: false,
        ..Default::default()
    });
    entry.indices.push(index_metadata);
    persistence.write_metadata()?;

    //TODO FIX LOAD FOR IN_MEMORY
    let data = persistence.directory.get_file_bytes(Path::new(&path))?;

    let store = SingleArrayPacked::<u32>::from_data(data, store.metadata);
    persistence.indices.boost_valueid_to_value.insert(path, Box::new(store));
    Ok(())
}
