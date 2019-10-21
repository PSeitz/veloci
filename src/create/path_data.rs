use crate::{
    create::{fields_config::FieldsConfig, *},
    metadata::FulltextIndexOptions,
    persistence::Persistence,
};
use buffered_index_writer::BufferedIndexWriter;

#[derive(Debug, Default)]
pub(crate) struct PathData {
    pub(crate) tokens_to_text_id: Option<Box<BufferedIndexWriter>>,
    pub(crate) token_to_anchor_id_score: Option<Box<BufferedIndexWriter<u32, (u32, u32)>>>,
    pub(crate) phrase_pair_to_anchor: Option<Box<BufferedIndexWriter<(u32, u32), u32>>>, // phrase_pair
    pub(crate) text_id_to_token_ids: Option<Box<BufferedTextIdToTokenIdsData>>,
    pub(crate) text_id_to_parent: Option<Box<BufferedIndexWriter>>,

    /// Used to recreate objects, keep oder
    pub(crate) parent_to_text_id: Option<Box<BufferedIndexWriter>>,
    /// Used to recreate objects, keep oder
    pub(crate) value_id_to_anchor: Option<Box<BufferedIndexWriter>>,
    pub(crate) text_id_to_anchor: Option<Box<BufferedIndexWriter>>,
    pub(crate) anchor_to_text_id: Option<Box<BufferedIndexWriter>>,
    pub(crate) boost: Option<Box<BufferedIndexWriter>>,
    pub(crate) fulltext_options: FulltextIndexOptions,
    pub(crate) is_anchor_identity_column: bool,
    pub(crate) skip_tokenizing: bool,
    pub(crate) term_data: TermDataInPath,
}

#[derive(Debug)]
pub(crate) struct BufferedTextIdToTokenIdsData {
    text_id_flag: FixedBitSet,
    pub(crate) data: BufferedIndexWriter,
}

impl BufferedTextIdToTokenIdsData {
    #[inline]
    pub(crate) fn contains(&self, text_id: u32) -> bool {
        self.text_id_flag.contains(text_id as usize)
    }

    #[inline]
    fn flag(&mut self, text_id: u32) {
        if self.text_id_flag.len() <= text_id as usize {
            self.text_id_flag.grow(text_id as usize + 1);
        }
        self.text_id_flag.insert(text_id as usize);
    }

    #[inline]
    pub(crate) fn add_all(&mut self, text_id: u32, token_ids: &[u32]) -> Result<(), io::Error> {
        self.flag(text_id);
        self.data.add_all(text_id, token_ids)
    }
}

pub(crate) fn prepare_path_data(temp_dir: &str, persistence: &Persistence, fields_config: &FieldsConfig, path: &str, term_data: TermDataInPath) -> PathData {
    let field_config = fields_config.get(path);
    let boost_info_data = if field_config.boost.is_some() {
        Some(Box::new(BufferedIndexWriter::new_for_sorted_id_insertion(temp_dir.to_string())))
    } else {
        None
    };
    // prepare direct access to resolve boost values directly to anchor
    let value_id_to_anchor = if field_config.boost.is_some() {
        Some(Box::new(BufferedIndexWriter::<u32, u32>::new_for_sorted_id_insertion(temp_dir.to_string())))
    } else {
        None
    };
    let anchor_to_text_id = if field_config.facet && is_1_to_n(path) {
        //Create facet index only for 1:N
        // anchor_id is monotonically increasing, hint buffered index writer, it's already sorted
        Some(Box::new(BufferedIndexWriter::new_for_sorted_id_insertion(temp_dir.to_string())))
    } else {
        None
    };

    let get_buffered_if_enabled = |val: IndexCreationType| -> Option<Box<BufferedIndexWriter>> {
        if field_config.is_index_enabled(val) {
            Some(Box::new(BufferedIndexWriter::new_unstable_sorted(temp_dir.to_string())))
        } else {
            None
        }
    };

    let tokens_to_text_id = get_buffered_if_enabled(IndexCreationType::TokensToTextID);
    let text_id_to_parent = get_buffered_if_enabled(IndexCreationType::TextIDToParent);
    let text_id_to_anchor = get_buffered_if_enabled(IndexCreationType::TextIDToAnchor);
    let phrase_pair_to_anchor = if field_config.is_index_enabled(IndexCreationType::PhrasePairToAnchor) {
        Some(Box::new(BufferedIndexWriter::new_unstable_sorted(temp_dir.to_string())))
    } else {
        None
    };
    let text_id_to_token_ids = if field_config.is_index_enabled(IndexCreationType::TextIDToTokenIds) {
        Some(Box::new(BufferedTextIdToTokenIdsData {
            text_id_flag: FixedBitSet::default(),
            data: BufferedIndexWriter::new_stable_sorted(temp_dir.to_string()), // Stable sort, else the token_ids will be reorderer in the wrong order
        }))
    } else {
        None
    };
    let parent_to_text_id = if field_config.is_index_enabled(IndexCreationType::ParentToTextID) {
        Some(Box::new(BufferedIndexWriter::new_for_sorted_id_insertion(temp_dir.to_string())))
    } else {
        None
    };

    let token_to_anchor_id_score = if field_config.is_index_enabled(IndexCreationType::TokenToAnchorIDScore) {
        Some(Box::new(BufferedIndexWriter::<u32, (u32, u32)>::new_unstable_sorted(temp_dir.to_string())))
    } else {
        None
    };

    let fulltext_options = field_config.fulltext.clone().unwrap_or_else(FulltextIndexOptions::new_with_tokenize);

    let skip_tokenizing = if !fulltext_options.tokenize {
        fulltext_options.tokenize
    } else {
        tokens_to_text_id.is_none() && token_to_anchor_id_score.is_none() && phrase_pair_to_anchor.is_none()
    };

    PathData {
        anchor_to_text_id,
        boost: boost_info_data,
        value_id_to_anchor,
        // parent_id is monotonically increasing, hint buffered index writer, it's already sorted
        parent_to_text_id,
        token_to_anchor_id_score,
        tokens_to_text_id,
        text_id_to_parent,
        text_id_to_anchor,
        phrase_pair_to_anchor,
        text_id_to_token_ids,
        fulltext_options,
        skip_tokenizing,
        is_anchor_identity_column: persistence.metadata.columns.get(path).map(|el| el.is_anchor_identity_column).unwrap_or(false),
        term_data,
    }
}
