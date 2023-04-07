mod calculate_score;
mod create_fulltext;
mod fast_lines;
mod features;
mod fields_config;
mod path_data;
mod token_values_to_tokens;
mod write_docs;
pub use token_values_to_tokens::*;

use self::{fast_lines::FastLinesTrait, features::IndexCreationType, fields_config::FieldsConfig};
use crate::directory::{load_data_pair, Directory};
use crate::{
    create::{
        calculate_score::{calculate_and_add_token_score_in_doc, calculate_token_score_for_entry},
        create_fulltext::AllTermsAndDocumentBuilder,
        fields_config::config_from_string,
        path_data::{prepare_path_data, PathData},
        write_docs::write_docs,
    },
    error::*,
    indices::{persistence_score::token_to_anchor_score_vint::*, *},
    metadata::FulltextIndexOptions,
    persistence::{Persistence, *},
    util::{StringAdd, *},
};
use buffered_index_writer::{self, BufferedIndexWriter};
use create_fulltext::{get_allterms_per_path, store_full_text_info_and_set_ids};
use fixedbitset::FixedBitSet;
use fnv::FnvHashMap;

use itertools::Itertools;

use num::ToPrimitive;
use rayon::prelude::*;

use std::path::Path;
use std::{
    self,
    fs::File,
    io::{self, BufRead},
    path::PathBuf,
    str,
};

type ValueId = u32;
type TokenId = u32;

// type TermMap = term_hashmap::HashMap<TermInfo>;
type TermMap = inohashmap::StringHashMap<TermInfo>;

const NUM_TERM_LIMIT_MSG: &str = "number of terms per field is currently limited to u32";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FacetIndex {
    facet: String,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct TermInfo {
    pub(crate) id: u32,
    pub(crate) num_occurences: u32,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ValIdPairToken {
    pub(crate) token_or_text_id: u32,
    pub(crate) token_pos: u32,
    pub(crate) num_occurences: u32,
}

#[derive(Debug, Default, Clone, Copy)]
#[allow(dead_code)]
pub(crate) struct TokenToAnchorScore {
    pub(crate) valid: u32,
    pub(crate) anchor_id: u32,
    pub(crate) score: u32,
}

#[inline]
// *mut FnvHashMap here or the borrow checker will complain, because of 'if let' expands the scope of the mutable ownership to the complete function
fn get_or_insert_prefer_get<'a, T, F>(map: *mut FnvHashMap<String, T>, key: &str, mut constructor: F) -> &'a mut T
where
    F: FnMut() -> T,
{
    unsafe {
        if let Some(e) = (*map).get_mut(key) {
            return e;
        }

        (*map).insert(key.to_string(), constructor());
        (*map).get_mut(key).unwrap()
    }
}

#[derive(Debug, Default)]
pub(crate) struct TermDataInPath {
    pub(crate) terms: TermMap,
    /// does not store texts longer than this in the fst in bytes
    pub(crate) do_not_store_text_longer_than: usize,
    pub(crate) id_counter_for_large_texts: u32,
}

fn is_1_to_n(path: &str) -> bool {
    path.contains("[]")
}

// use buffered_index_writer::KeyValue;
fn stream_iter_to_direct_index(iter: impl Iterator<Item = buffered_index_writer::KeyValue<u32, u32>>, target: &mut IndexIdToOneParentFlushing) -> Result<(), io::Error> {
    for kv in iter {
        target.add(kv.key, kv.value)?;
    }
    Ok(())
}

fn buffered_index_to_direct_index(directory: &Box<dyn Directory>, path: &str, mut buffered_index_data: BufferedIndexWriter) -> Result<IndexIdToOneParentFlushing, io::Error> {
    let mut store = IndexIdToOneParentFlushing::new(directory.box_clone(), Path::new(path).to_owned(), buffered_index_data.max_value_id);
    if buffered_index_data.is_in_memory() {
        stream_iter_to_direct_index(buffered_index_data.iter_inmemory(), &mut store)?;
    } else {
        stream_iter_to_direct_index(buffered_index_data.flush_and_kmerge()?, &mut store)?;
    }
    buffered_index_data.cleanup()?;

    //when there has been written something to disk flush the rest of the data too, so we have either all data im oder on disk
    if !store.is_in_memory() {
        store.flush()?;
    }

    Ok(store)
}

#[derive(Debug)]
struct PathDataIds {
    value_to_parent: Option<BufferedIndexWriter>,
    parent_to_value: Option<BufferedIndexWriter>,
    #[allow(dead_code)]
    value_to_anchor: Option<BufferedIndexWriter>,
}

fn get_text_info(all_terms: &mut TermDataInPath, value: &str) -> TermInfo {
    if all_terms.do_not_store_text_longer_than < value.len() {
        // *all_terms.long_terms.get(value).expect("did not found term")
        all_terms.id_counter_for_large_texts = all_terms.id_counter_for_large_texts.checked_add(1).expect(NUM_TERM_LIMIT_MSG);
        TermInfo {
            id: all_terms
                .terms
                .len()
                .to_u32()
                .expect(NUM_TERM_LIMIT_MSG)
                .checked_add(1)
                .expect(NUM_TERM_LIMIT_MSG)
                .checked_add(all_terms.id_counter_for_large_texts)
                .expect(NUM_TERM_LIMIT_MSG),
            num_occurences: 1,
        }
    } else {
        *all_terms.terms.get(value).expect("did not found term")
    }
}

macro_rules! add {
    ($index:expr, $val1:expr, $val2:expr) => {
        if let Some(el) = $index.as_mut() {
            el.add($val1, $val2)?;
        }
    };
}

type DataAndIds = Result<(FnvHashMap<String, PathData>, FnvHashMap<String, PathDataIds>), io::Error>;

fn parse_json_and_prepare_indices<I>(stream1: I, persistence: &Persistence, fields_config: &FieldsConfig, term_data: &mut AllTermsAndDocumentBuilder) -> DataAndIds
where
    I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>,
{
    let mut path_data: FnvHashMap<String, PathData> = FnvHashMap::default();

    let mut id_holder = json_converter::IDHolder::new();
    let mut tuples_to_parent_in_path: FnvHashMap<String, PathDataIds> = FnvHashMap::default();

    {
        info_time!("build path data");

        let mut tokens_ids = Vec::with_capacity(5);
        let mut tokens_to_anchor_id = Vec::with_capacity(10);

        let mut cb_text = |anchor_id: u32, value: &str, path: &str, parent_val_id: u32| -> Result<(), io::Error> {
            let data: &mut PathData = get_or_insert_prefer_get(&mut path_data, path, || {
                let term_data = term_data.terms_in_path.remove(path).unwrap_or_else(|| panic!("Couldn't find path in term_data {:?}", path));
                prepare_path_data(persistence, fields_config, path, term_data)
            });

            let text_info = get_text_info(&mut data.term_data, value);
            trace!("Found id {:?} for {:?}", text_info, value);

            add!(data.text_id_to_parent, text_info.id, parent_val_id);
            add!(data.parent_to_text_id, parent_val_id, text_info.id);

            if let Some(el) = data.text_id_to_anchor.as_mut() {
                if !data.is_anchor_identity_column {
                    // we don't need to store the relation, if they are identity
                    el.add(text_info.id, anchor_id)?;
                }
            }
            // data.text_id_to_anchor.add(text_info.id, anchor_id)?;
            add!(data.anchor_to_text_id, anchor_id, text_info.id);
            if let Some(el) = data.boost.as_mut() {
                if value.trim() != "" {
                    let my_number = value.parse::<f32>().unwrap_or_else(|_| panic!("Expected an f32 value but got {:?}", value));
                    if !my_number.is_nan() {
                        el.add(parent_val_id, my_number.to_bits())?;
                    }
                }
            }
            add!(data.value_id_to_anchor, parent_val_id, anchor_id);

            add!(
                data.token_to_anchor_id_score,
                text_info.id,
                (anchor_id, calculate_token_score_for_entry(0, text_info.num_occurences, 1, true))
            );

            if data.fulltext_options.tokenize {
                let tokenizer = data.fulltext_options.tokenizer.as_ref().unwrap_or_else(|| panic!("no tokenizer created for {:?}", path));
                if tokenizer.has_tokens(value) {
                    let mut current_token_pos = 0;

                    let text_ids_to_token_ids_already_stored = data.text_id_to_token_ids.as_ref().map(|el| el.contains(text_info.id)).unwrap_or(false);

                    let mut prev_token: Option<TokenId> = None;

                    for (token, is_seperator) in tokenizer.iter(value) {
                        let token_info = data.term_data.terms.get(token).expect("did not found token");
                        trace!("Adding to tokens_ids {:?} : {:?}", token, token_info);

                        if !text_ids_to_token_ids_already_stored {
                            tokens_ids.push(token_info.id);
                        }

                        add!(data.tokens_to_text_id, token_info.id, text_info.id);

                        if data.token_to_anchor_id_score.is_some() {
                            tokens_to_anchor_id.push(ValIdPairToken {
                                token_or_text_id: token_info.id,
                                num_occurences: token_info.num_occurences,
                                token_pos: current_token_pos,
                            });
                            current_token_pos += 1;
                        }

                        // seperators are currently ignored for the phrase_pairs, but this is questionable.
                        // there are cases where a seperator are still important, eg.
                        // <<cool>> , with < and > as seperators
                        // we still would want a phrase boost if we search for <<cool
                        // so we would need maybe two categories of seperators
                        if !is_seperator {
                            if let Some(el) = data.phrase_pair_to_anchor.as_mut() {
                                if let Some(prev_token) = prev_token {
                                    el.add((prev_token, token_info.id), anchor_id)?;
                                }
                                prev_token = Some(token_info.id);
                            }
                        }
                    }

                    if !text_ids_to_token_ids_already_stored {
                        trace!("Adding for {:?} {:?} token_ids {:?}", value, text_info.id, tokens_ids);
                        if let Some(el) = data.text_id_to_token_ids.as_mut() {
                            el.add_all(text_info.id, &tokens_ids).unwrap();
                        }
                    }

                    if let Some(token_to_anchor_id_score) = data.token_to_anchor_id_score.as_mut() {
                        calculate_and_add_token_score_in_doc(&mut tokens_to_anchor_id, anchor_id, current_token_pos, token_to_anchor_id_score)?;
                    }
                    // calculate_and_add_token_score_in_doc(&mut phrase_to_anchor_id, anchor_id, current_token_pos, &mut data.token_to_anchor_id_score, true)?;
                    tokens_to_anchor_id.clear();
                    // phrase_to_anchor_id.clear();
                    tokens_ids.clear();
                }
            }
            Ok(())
        };

        let mut callback_ids = |_anchor_id: u32, path: &str, value_id: u32, parent_val_id: u32| -> Result<(), io::Error> {
            let tuples: &mut PathDataIds = get_or_insert_prefer_get(&mut tuples_to_parent_in_path, path, || {
                let field_config = fields_config.get(path);
                //TODO FIXME BUG ALL SUB LEVELS ARE NOT HANDLED (not every supath has it's own config yet) ONLY THE LEAFES BEFORE .TEXTINDEX
                let value_to_parent = if field_config.is_index_enabled(IndexCreationType::ValueIDToParent) {
                    Some(BufferedIndexWriter::new_for_sorted_id_insertion(persistence.directory.box_clone()))
                } else {
                    None
                };
                let parent_to_value = if field_config.is_index_enabled(IndexCreationType::ParentToValueID) {
                    Some(BufferedIndexWriter::new_for_sorted_id_insertion(persistence.directory.box_clone()))
                } else {
                    None
                };

                PathDataIds {
                    value_to_parent,
                    parent_to_value,
                    value_to_anchor: None,
                }
            });
            if let Some(el) = tuples.value_to_parent.as_mut() {
                el.add(value_id, parent_val_id)?;
            }
            if let Some(el) = tuples.parent_to_value.as_mut() {
                el.add(parent_val_id, value_id)?;
            }
            Ok(())
        };

        json_converter::for_each_element(stream1, &mut id_holder, &mut cb_text, &mut callback_ids)?;
    }

    // std::mem::swap(&mut create_cache.term_data.id_holder, &mut id_holder);

    Ok((path_data, tuples_to_parent_in_path))
}

/// Only trace im data
#[cfg(not(tarpaulin_include))]
fn print_indices(path_data: &mut FnvHashMap<String, PathData>) {
    for (path, data) in path_data {
        let path = &path;

        if let Some(el) = data.tokens_to_text_id.as_ref() {
            trace!("{}\n{}", &path.add(TOKENS_TO_TEXT_ID), &el);
        }

        if let Some(el) = data.text_id_to_parent.as_ref() {
            trace!("{}\n{}", &path.add(VALUE_ID_TO_PARENT), &el);
        }
        if let Some(el) = data.parent_to_text_id.as_ref() {
            trace!("{}\n{}", &path.add(PARENT_TO_VALUE_ID), &el);
        }
        if let Some(el) = data.text_id_to_anchor.as_ref() {
            trace!("{}\n{}", &path.add(TEXT_ID_TO_ANCHOR), &el);
        }
        if let Some(el) = data.anchor_to_text_id.as_ref() {
            trace!("{}\n{}", &path.add(ANCHOR_TO_TEXT_ID), &el);
        }
    }
}

// use buffered_index_writer::KeyValue;
fn stream_iter_to_indirect_index(
    iter: impl Iterator<Item = buffered_index_writer::KeyValue<u32, u32>>,
    target: &mut IndirectIMFlushingInOrderVint,
    sort_and_dedup: bool,
) -> Result<(), io::Error> {
    for (id, group) in &iter.group_by(|el| el.key) {
        let mut group: Vec<u32> = group.map(|el| el.value).collect();
        if sort_and_dedup {
            group.sort_unstable();
            group.dedup();
        }
        target.add(id, group)?;
    }

    Ok(())
}

fn buffered_index_to_indirect_index_multiple(
    directory: &Box<dyn Directory>,
    path: &str,
    mut buffered_index_data: BufferedIndexWriter,
    sort_and_dedup: bool,
) -> Result<IndirectIMFlushingInOrderVint, VelociError> {
    let mut store = IndirectIMFlushingInOrderVint::new(directory, PathBuf::from(path.to_string()), buffered_index_data.max_value_id);

    if buffered_index_data.is_in_memory() {
        stream_iter_to_indirect_index(buffered_index_data.iter_inmemory(), &mut store, sort_and_dedup)?;
    } else {
        stream_iter_to_indirect_index(buffered_index_data.flush_and_kmerge()?, &mut store, sort_and_dedup)?;
    }
    buffered_index_data.cleanup()?;

    //when there has been written something to disk flush the rest of the data too, so we have either all data im oder on disk
    if !store.is_in_memory() {
        store.flush()?;
    }

    Ok(store)
}

fn stream_iter_to_anchor_score<T: AnchorScoreDataSize>(
    iter: impl Iterator<Item = buffered_index_writer::KeyValue<u32, (ValueId, ValueId)>>,
    target: &mut TokenToAnchorScoreVintFlushing<T>,
) -> Result<(), io::Error> {
    for (id, group) in &iter.group_by(|el| el.key) {
        let mut group: Vec<(ValueId, ValueId)> = group.map(|el| el.value).collect();
        group.sort_unstable_by_key(|el| el.0);
        dedup_keep_best_score_by(
            &mut group,
            |el1, el2| el1.0 == el2.0,
            |group| {
                let mut max_score = group.iter().map(|el| el.1).max().unwrap();
                // small boost for multi hits, but limiting to 5
                max_score += (group.len() as u32).min(5);
                (group[0].0, max_score)
            },
        );
        let mut scores = group.iter().flat_map(|el| [el.0, el.1]).collect::<Vec<_>>();
        target.set_scores(id, &mut scores)?;
    }

    Ok(())
}

// dedup, keep best hits for same term id, and truncate result
//
// Two Callbacks
// - One for comparison
// - One for merging hit group
fn dedup_keep_best_score_by<T, F1, F2>(hits: &mut Vec<T>, mut is_equal: F1, mut merge: F2)
where
    F1: FnMut(T, T) -> bool,
    F2: FnMut(&[T]) -> T,
    T: Copy + std::fmt::Debug,
{
    let mut write_idx = 0;
    let mut read_idx = 0;

    while read_idx != hits.len() {
        let group_start = read_idx;
        let mut group_end = read_idx;
        hits[write_idx] = hits[read_idx];
        while is_equal(hits[group_end], hits[group_start]) {
            group_end += 1;
            if group_end == hits.len() {
                break;
            }
        }
        let group_range = group_start..group_end;
        if group_range.len() > 1 {
            hits[write_idx] = merge(&hits[group_range]);
        }
        write_idx += 1;
        if group_end == hits.len() {
            break;
        }
        read_idx = group_end;
    }
    hits.truncate(write_idx);
}

pub fn add_anchor_score_flush(
    directory: &Box<dyn Directory>,
    path_col: &str,
    field_path: String,
    mut buffered_index_data: BufferedIndexWriter<ValueId, (ValueId, ValueId)>,
    indices: &mut IndicesFromRawData,
) -> Result<(), io::Error> {
    //If the buffered index_data is larger than 4GB, we switch to u64 for addressing the data block
    if buffered_index_data.bytes_written() < 2_u64.pow(32) {
        let mut store = TokenToAnchorScoreVintFlushing::<u32>::new(field_path.to_owned(), directory);
        // stream_buffered_index_writer_to_anchor_score(buffered_index_data, &mut store)?;
        if buffered_index_data.is_in_memory() {
            stream_iter_to_anchor_score(buffered_index_data.iter_inmemory(), &mut store)?;
        } else {
            stream_iter_to_anchor_score(buffered_index_data.flush_and_kmerge()?, &mut store)?;
        }
        buffered_index_data.cleanup()?;

        //when there has been written something to disk flush the rest of the data too, so we have either all data im oder on disk
        if !store.is_in_memory() {
            store.flush()?;
        }

        indices.push(IndexData {
            path_col: path_col.to_string(),
            path: field_path,
            index: IndexVariants::TokenToAnchorScoreU32(store),
            index_category: IndexCategory::AnchorScore,
        });
    } else {
        let mut store = TokenToAnchorScoreVintFlushing::<u64>::new(field_path.to_string(), directory);
        // stream_buffered_index_writer_to_anchor_score(buffered_index_data, &mut store)?;
        if buffered_index_data.is_in_memory() {
            stream_iter_to_anchor_score(buffered_index_data.iter_inmemory(), &mut store)?;
        } else {
            stream_iter_to_anchor_score(buffered_index_data.flush_and_kmerge()?, &mut store)?;
        }
        buffered_index_data.cleanup()?;

        //when there has been written something to disk flush the rest of the data too, so we have either all data im oder on disk
        if !store.is_in_memory() {
            store.flush()?;
        }

        indices.push(IndexData {
            path_col: path_col.to_string(),
            path: field_path,
            index: IndexVariants::TokenToAnchorScoreU64(store),
            index_category: IndexCategory::AnchorScore,
        });
    }

    Ok(())
}

fn stream_iter_to_phrase_index(
    iter: impl Iterator<Item = buffered_index_writer::KeyValue<(ValueId, ValueId), u32>>,
    target: &mut IndirectIMFlushingInOrderVintNoDirectEncode<(ValueId, ValueId)>,
) -> Result<(), io::Error> {
    for (id, group) in &iter.group_by(|el| el.key) {
        let mut group: Vec<u32> = group.map(|el| el.value).collect();
        group.sort_unstable();
        group.dedup();
        target.add(id, &group)?;
    }

    Ok(())
}

fn stream_buffered_index_writer_to_phrase_index(
    mut index_writer: BufferedIndexWriter<(ValueId, ValueId), u32>,
    target: &mut IndirectIMFlushingInOrderVintNoDirectEncode<(ValueId, ValueId)>,
) -> Result<(), io::Error> {
    // flush_and_kmerge will flush elements to disk, this is unnecessary for small indices, so we check for im
    if index_writer.is_in_memory() {
        stream_iter_to_phrase_index(index_writer.iter_inmemory(), target)?;
    } else {
        stream_iter_to_phrase_index(index_writer.flush_and_kmerge()?, target)?;
    }
    index_writer.cleanup()?;

    // when there has been written something to disk flush the rest of the data too, so we have either all data im oder on disk
    if !target.is_in_memory() {
        target.flush()?;
    }
    Ok(())
}
fn add_phrase_pair_flush(
    directory: &Box<dyn Directory>,
    path_col: &str,
    path: String,
    buffered_index_data: BufferedIndexWriter<(ValueId, ValueId), u32>,
    indices: &mut IndicesFromRawData,
) -> Result<(), io::Error> {
    let mut store = IndirectIMFlushingInOrderVintNoDirectEncode::<(ValueId, ValueId)>::new(directory.box_clone(), Path::new(&path).to_owned(), buffered_index_data.max_value_id);
    stream_buffered_index_writer_to_phrase_index(buffered_index_data, &mut store)?;

    indices.push(IndexData {
        path_col: path_col.to_string(),
        path,
        index: IndexVariants::Phrase(store),
        index_category: IndexCategory::Phrase,
    });
    Ok(())
}

pub type IndicesFromRawData = Vec<IndexData>;

#[derive(Debug)]
pub struct IndexData {
    path_col: String,
    path: String,
    index: IndexVariants,
    index_category: IndexCategory,
}

#[derive(Debug)]
enum IndexVariants {
    Phrase(IndirectIMFlushingInOrderVintNoDirectEncode<(ValueId, ValueId)>),
    SingleValue(IndexIdToOneParentFlushing),
    MultiValue(IndirectIMFlushingInOrderVint),
    TokenToAnchorScoreU32(TokenToAnchorScoreVintFlushing<u32>),
    TokenToAnchorScoreU64(TokenToAnchorScoreVintFlushing<u64>),
}

fn convert_raw_path_data_to_indices(
    directory: &Box<dyn Directory>,
    path_data: FnvHashMap<String, PathData>,
    tuples_to_parent_in_path: FnvHashMap<String, PathDataIds>,
    indices_json: &FieldsConfig,
    // facet_index: &FnvHashSet<String>,
) -> Result<IndicesFromRawData, VelociError> {
    info_time!("convert_raw_path_data_to_indices");
    let mut indices = IndicesFromRawData::default();

    let add_index_flush = |path_col: &str,
                           path: String,
                           buffered_index_data: BufferedIndexWriter,
                           is_always_1_to_1: bool,
                           sort_and_dedup: bool,
                           indices: &mut IndicesFromRawData|
     -> Result<(), VelociError> {
        if is_always_1_to_1 {
            let store = buffered_index_to_direct_index(directory, &path, buffered_index_data)?;
            indices.push(IndexData {
                path_col: path_col.to_string(),
                path,
                index: IndexVariants::SingleValue(store),
                index_category: IndexCategory::KeyValue,
            });
        } else {
            let store = buffered_index_to_indirect_index_multiple(directory, &path, buffered_index_data, sort_and_dedup)?;
            indices.push(IndexData {
                path_col: path_col.to_string(),
                path,
                index: IndexVariants::MultiValue(store),
                index_category: IndexCategory::KeyValue,
            });
        }
        Ok(())
    };

    let indices_res: Result<Vec<_>, VelociError> = path_data
        .into_par_iter()
        .map(|(mut path, data)| {
            let mut indices = IndicesFromRawData::default();
            let path_col = path.to_string();
            path += TEXTINDEX;
            let path = &path;

            if let Some(tokens_to_text_id) = data.tokens_to_text_id {
                add_index_flush(&path_col, path.add(TOKENS_TO_TEXT_ID), *tokens_to_text_id, false, true, &mut indices)?;
            }

            if let Some(token_to_anchor_id_score) = data.token_to_anchor_id_score {
                add_anchor_score_flush(directory, &path_col, path.add(TO_ANCHOR_ID_SCORE), *token_to_anchor_id_score, &mut indices)?;
            }

            if let Some(phrase_pair_to_anchor) = data.phrase_pair_to_anchor {
                add_phrase_pair_flush(directory, &path_col, path.add(PHRASE_PAIR_TO_ANCHOR), *phrase_pair_to_anchor, &mut indices)?;
            }

            let no_sort_and_dedup = false;
            if let Some(text_id_to_token_ids) = data.text_id_to_token_ids {
                add_index_flush(&path_col, path.add(TEXT_ID_TO_TOKEN_IDS), text_id_to_token_ids.data, false, no_sort_and_dedup, &mut indices)?;
            }

            if let Some(text_id_to_parent) = data.text_id_to_parent {
                add_index_flush(
                    &path_col,
                    path.add(VALUE_ID_TO_PARENT),
                    *text_id_to_parent,
                    false, // valueIdToParent relation is always 1 to 1, expect for text_ids, which can have multiple parents. Here we handle only text_ids therefore is this always false
                    no_sort_and_dedup,
                    &mut indices,
                )?;
            }

            if let Some(value_id_to_anchor) = data.value_id_to_anchor {
                add_index_flush(&path_col, path_col.add(VALUE_ID_TO_ANCHOR), *value_id_to_anchor, false, no_sort_and_dedup, &mut indices)?;
            }

            if let Some(parent_to_text_id) = data.parent_to_text_id {
                add_index_flush(
                    &path_col,
                    path.add(PARENT_TO_VALUE_ID),
                    *parent_to_text_id,
                    true, // This is parent_to_text_id here - Every Value id hat one associated text_id
                    no_sort_and_dedup,
                    &mut indices,
                )?;
            }

            if let Some(text_id_to_anchor) = data.text_id_to_anchor {
                add_index_flush(&path_col, path.add(TEXT_ID_TO_ANCHOR), *text_id_to_anchor, false, true, &mut indices)?;
            }

            if let Some(anchor_to_text_id) = data.anchor_to_text_id {
                add_index_flush(&path_col, path.add(ANCHOR_TO_TEXT_ID), *anchor_to_text_id, false, no_sort_and_dedup, &mut indices)?;
            }

            if let Some(buffered_index_data) = data.boost {
                let boost_path = extract_field_name(path).add(BOOST_VALID_TO_VALUE);

                let store = buffered_index_to_indirect_index_multiple(directory, &boost_path, *buffered_index_data, false)?;
                indices.push(IndexData {
                    path_col,
                    path: boost_path,
                    index: IndexVariants::MultiValue(store),
                    index_category: IndexCategory::Boost,
                });
            }

            Ok(indices)
        })
        .collect();

    for indice in indices_res? {
        indices.extend(indice);
    }

    let indices_res_2: Result<Vec<_>, VelociError> = tuples_to_parent_in_path
        .into_par_iter()
        .map(|(path, data)| {
            let mut indices = IndicesFromRawData::default();
            let path = &path;

            if let Some(value_to_parent) = data.value_to_parent {
                add_index_flush(
                    path,
                    path.add(VALUE_ID_TO_PARENT),
                    value_to_parent,
                    true, // valueIdToParent relation is always 1 to 1, expect for text_ids, which can have multiple parents. Here we handle all except .textindex data therefore is this always true
                    false,
                    &mut indices,
                )?;
            }
            if let Some(parent_to_value) = data.parent_to_value {
                add_index_flush(path, path.add(PARENT_TO_VALUE_ID), parent_to_value, false, false, &mut indices)?;
            }

            Ok(indices)
        })
        .collect();

    for indice in indices_res_2? {
        indices.extend(indice);
    }
    directory.sync_directory()?;

    Ok(indices)
}

pub fn convert_any_json_data_to_line_delimited<I: std::io::Read, O: std::io::Write>(input: I, mut out: O) -> Result<(), io::Error> {
    let stream = serde_json::Deserializer::from_reader(input).into_iter::<serde_json::Value>();

    for value in stream {
        let value = value?;
        if let Some(arr) = value.as_array() {
            for el in arr {
                out.write_all(el.to_string().as_bytes())?;
                out.write_all(b"\n")?;
            }
        } else {
            out.write_all(value.to_string().as_bytes())?;
            out.write_all(b"\n")?;
        }
    }
    Ok(())
}

#[test]
fn test_json_to_line_delimited() {
    let value = r#"[
        {"a": "b"},
        {"c": "d"}
    ]"#;
    let mut out: Vec<u8> = vec![];
    convert_any_json_data_to_line_delimited(value.as_bytes(), &mut out).unwrap();
    assert_eq!(String::from_utf8(out).unwrap(), "{\"a\":\"b\"}\n{\"c\":\"d\"}\n");

    let value = r#"{  "a": "b"}{"c": "d"}"#;
    let mut out: Vec<u8> = vec![];
    convert_any_json_data_to_line_delimited(value.as_bytes(), &mut out).unwrap();
    assert_eq!(String::from_utf8(out).unwrap(), "{\"a\":\"b\"}\n{\"c\":\"d\"}\n");
}

pub fn create_fulltext_index<I, J, K, S: AsRef<str>>(
    stream1: I,
    stream2: J,
    stream3: K,
    persistence: &mut Persistence,
    indices_json: &FieldsConfig,
    load_persistence: bool,
) -> Result<(), VelociError>
where
    I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>,
    J: Iterator<Item = Result<serde_json::Value, serde_json::Error>>,
    K: Iterator<Item = S>,
{
    let mut term_data = AllTermsAndDocumentBuilder::default();

    let doc_write_res = write_docs(persistence, stream3)?;
    get_allterms_per_path(stream1, indices_json, &mut term_data)?;

    let default_fulltext_options = FulltextIndexOptions::new_with_tokenize();
    {
        info_time!("set term ids and write fst");
        let reso: Result<FnvHashMap<String, FieldInfo>, io::Error> = term_data
            .terms_in_path
            .par_iter_mut()
            .map(|(path, terms_data)| {
                let mut textindex_metadata = TextIndexValuesMetadata::default();
                let options: &FulltextIndexOptions = indices_json.get(path).fulltext.as_ref().unwrap_or(&default_fulltext_options);
                let path_text_index = path.to_string() + TEXTINDEX;
                textindex_metadata.options = options.clone();
                let mut col_info = FieldInfo {
                    name: path.to_string(),
                    has_fst: true,
                    textindex_metadata,
                    ..Default::default()
                };

                store_full_text_info_and_set_ids(persistence, terms_data, &path_text_index, options, &mut col_info, &doc_write_res)?;
                Ok((path.to_string(), col_info))
            })
            .collect();

        for (path, col_info) in reso? {
            persistence.metadata.columns.insert(path.to_string(), col_info);
        }
        // persistence.metadata.fulltext_indices = reso?;
        persistence.load_all_fst()?;

        // info!(
        //     "All text memory {}",
        //     persistence::get_readable_size(term_data.terms_in_path.iter().map(|el| el.1.terms.memory_footprint()).sum())
        // );
        // info!(
        //     "All raw text data memory {}",
        //     persistence::get_readable_size(term_data.terms_in_path.iter().map(|el| el.1.terms.total_size_of_text_data()).sum())
        // );
    }

    // check_similarity(&data.terms_in_path);
    info_time!("create and (write) fulltext_index");
    //trace!("all_terms {:?}", term_data.terms_in_path);

    let (mut path_data, tuples_to_parent_in_path) = parse_json_and_prepare_indices(stream2, persistence, indices_json, &mut term_data)?;

    // std::mem::drop(create_cache);

    if log_enabled!(log::Level::Trace) {
        print_indices(&mut path_data);
    }

    let mut indices = convert_raw_path_data_to_indices(&persistence.directory, path_data, tuples_to_parent_in_path, indices_json)?;
    info_time!("write indices");
    for index_data in &mut indices {
        let mut index_metadata = IndexMetadata {
            index_category: index_data.index_category,
            path: index_data.path.to_string(),
            data_type: DataType::U32,
            ..Default::default()
        };

        match &mut index_data.index {
            IndexVariants::Phrase(store) => {
                store.flush()?;
                index_metadata.is_empty = store.is_empty();
                index_metadata.metadata = store.metadata;
            }
            IndexVariants::SingleValue(store) => {
                store.flush()?;
                index_metadata.is_empty = store.is_empty();
                index_metadata.metadata = store.metadata;
                index_metadata.index_cardinality = IndexCardinality::IndexIdToOneParent;
            }
            IndexVariants::MultiValue(store) => {
                store.flush()?;
                index_metadata.is_empty = store.is_empty();
                index_metadata.metadata = store.metadata;
                index_metadata.index_cardinality = IndexCardinality::IndirectIM;
            }
            IndexVariants::TokenToAnchorScoreU32(store) => {
                store.flush()?;
                index_metadata.is_empty = false;
                index_metadata.metadata = store.metadata;
            }
            IndexVariants::TokenToAnchorScoreU64(store) => {
                store.flush()?;
                index_metadata.is_empty = false;
                index_metadata.metadata = store.metadata;
                index_metadata.data_type = DataType::U64;
            }
        }
        let entry = persistence.metadata.columns.entry(index_data.path_col.to_string()).or_insert_with(|| FieldInfo {
            has_fst: false,
            ..Default::default()
        });
        entry.indices.push(index_metadata);
    }

    persistence.write_metadata()?;

    // load the converted indices
    if load_persistence {
        for index_data in indices {
            let path = index_data.path;
            match index_data.index {
                IndexVariants::Phrase(index) => {
                    if index.is_in_memory() {
                        persistence.indices.phrase_pair_to_anchor.insert(path, Box::new(index.into_im_store()));
                    //Move data
                    } else {
                        let (ind, data) = load_data_pair(&persistence.directory, Path::new(&path)).unwrap();
                        let store = IndirectIMBinarySearch::from_data(ind, data, index.metadata).unwrap();

                        persistence.indices.phrase_pair_to_anchor.insert(path, Box::new(store));
                    }
                }
                IndexVariants::SingleValue(index) => {
                    if index.is_in_memory() {
                        persistence.indices.key_value_stores.insert(path, Box::new(index.into_im_store()));
                    //Move data
                    } else {
                        let data = persistence.directory.get_file_bytes(&Path::new(&path))?;
                        let store = SingleArrayPacked::from_data(data, index.metadata); //load data with MMap
                        persistence.indices.key_value_stores.insert(path, Box::new(store));
                    }
                }
                IndexVariants::MultiValue(index) => {
                    if index_data.index_category == IndexCategory::Boost {
                        persistence.indices.boost_valueid_to_value.insert(path, index.into_store()?);
                    } else {
                        persistence.indices.key_value_stores.insert(path, index.into_store()?);
                    }
                }
                IndexVariants::TokenToAnchorScoreU32(index) => {
                    persistence.indices.token_to_anchor_score.insert(path, index.into_store()?);
                }
                IndexVariants::TokenToAnchorScoreU64(index) => {
                    persistence.indices.token_to_anchor_score.insert(path, index.into_store()?);
                }
            }
        }
    }

    //TEST FST AS ID MAPPER
    // let mut all_ids_as_str: TermMap = FnvHashMap::default();
    // for pair in &tuples {
    //     let padding = 10;
    //     all_ids_as_str.insert(format!("{:0padding$}", pair.valid, padding = padding), TermInfo::new(pair.parent_val_id)); // COMPRESSION 50-90%
    // }
    // store_fst(persistence, &all_ids_as_str, path_name.add(".valueIdToParent.fst")).expect("Could not store fst");
    //TEST FST AS ID MAPPER
    Ok(())
}

pub fn create_indices_from_str(persistence: &mut Persistence, data_str: &str, indices: &str, load_persistence: bool) -> Result<(), VelociError> {
    let stream1 = data_str.lines().map(serde_json::from_str);
    let stream2 = data_str.lines().map(serde_json::from_str);

    create_indices_from_streams(persistence, stream1, stream2, data_str.lines(), indices, load_persistence)
}
pub fn create_indices_from_file(persistence: &mut Persistence, data_path: &str, indices: &str, load_persistence: bool) -> Result<(), VelociError> {
    let stream1 = std::io::BufReader::new(File::open(data_path)?).fast_lines();
    let stream2 = std::io::BufReader::new(File::open(data_path)?).fast_lines();
    let stream3 = std::io::BufReader::new(File::open(data_path)?).lines().map(|line| line.unwrap());

    create_indices_from_streams(persistence, stream1, stream2, stream3, indices, load_persistence)
}

pub fn create_indices_from_streams<I, J, K, S: AsRef<str>>(
    persistence: &mut Persistence,
    stream1: I,
    stream2: J,
    stream3: K,
    indices: &str,
    load_persistence: bool,
) -> Result<(), VelociError>
where
    I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>,
    J: Iterator<Item = Result<serde_json::Value, serde_json::Error>>,
    K: Iterator<Item = S>,
{
    info_time!("total time create_indices for");

    let mut indices_json: FieldsConfig = config_from_string(indices)?;
    indices_json.features_to_indices()?;
    create_fulltext_index(stream1, stream2, stream3, persistence, &indices_json, load_persistence)?;

    info_time!("write json and metadata");

    Ok(())
}
