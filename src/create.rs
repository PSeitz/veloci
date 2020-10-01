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
    persistence::{self, Persistence, *},
    util::{self, StringAdd, *},
};
use buffered_index_writer::{self, BufferedIndexWriter};
use create_fulltext::{get_allterms_per_path, store_full_text_info_and_set_ids};
use fixedbitset::FixedBitSet;
use fnv::FnvHashMap;
use fst;
use itertools::Itertools;
use json_converter;
use log;
use memmap::MmapOptions;
use num::ToPrimitive;
use rayon::prelude::*;
use serde_json;
use std::{
    self,
    fs::File,
    io::{self, BufRead},
    path::PathBuf,
    str,
};
use term_hashmap;

type ValueId = u32;
type TokenId = u32;

type TermMap = term_hashmap::HashMap<TermInfo>;

const NUM_TERM_LIMIT_MSG: &str = "number of terms per field is currently limited to u32";
// const NUM_TERM_OCC_LIMIT_MSG: &str = "number of terms occurences per field is currently limited to u32";

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
pub(crate) struct TokenToAnchorScore {
    pub(crate) valid: u32,
    pub(crate) anchor_id: u32,
    pub(crate) score: u32,
}

// fn print_vec(vec: &[ValIdPair], valid_header: &str, parentid_header: &str) -> String {
//     format!("{}\t{}", valid_header, parentid_header)
//         + &vec.iter()
//             .map(|el| format!("\n{}\t{}", el.valid, el.parent_val_id))
//             .collect::<Vec<_>>()
//             .join("")
// }

// fn print_index_id_to_parent(vec: &IndirectIM<u32>, valid_header: &str, parentid_header: &str) -> String {
//     let keys = vec.get_keys();
//     format!("{}\t{}", valid_header, parentid_header)
//         + &keys.iter()
//             .map(|key| format!("\n{}\t{:?}", key, vec.get_values(u64::from(*key))))
//             .collect::<Vec<_>>()
//             .join("")
// }

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

// #[derive(Debug, Default)]
// pub struct CreateCache {
//     term_data: AllTermsAndDocumentBuilder,
// }

#[derive(Debug, Default)]
pub(crate) struct TermDataInPath {
    pub(crate) terms: TermMap,
    /// does not store texts longer than this in the fst in bytes
    pub(crate) do_not_store_text_longer_than: usize,
    pub(crate) id_counter_for_large_texts: u32,
}

// fn check_similarity(data: &FnvHashMap<String, TermMap>) {
//     let mut map: FnvHashMap<String, FnvHashMap<String, (f32, f32)>> = FnvHashMap::default();

//     info_time!("check_similarity");
//     for (path, terms) in data {
//         let num_terms = terms.len();
//         for (path_comp, terms_comp) in data.iter().filter(|&(path_comp, _)| path_comp != path) {
//             let num_similar = terms.keys().filter(|term| terms_comp.contains_key(term.as_str())).count();
//             let similiarity = num_similar as f32 / num_terms as f32;
//             //info!("Similiarity {:?} {:?} {:?}", path, path_comp, num_similar as f32 / num_terms as f32);
//             if map.contains_key(path_comp) {
//                 let aha = map.get_mut(path_comp).expect("did not found key").get_mut(path).expect("did not found key");
//                 aha.1 = similiarity;
//             // map.get_mut(path_comp).1 = num_similar as f32 / num_terms as f32
//             } else {
//                 let entry = map.entry(path.to_string()).or_insert(FnvHashMap::default());
//                 entry.insert(path_comp.to_string(), (similiarity, 0.));
//             }
//         }
//     }
//     for (path, sub) in map {
//         for (path2, data) in sub {
//             if data.0 > 0.1 {
//                 info!("{} {} {} {}", path, path2, data.0, data.1);
//             }
//         }
//     }
// }

// fn replace_term_ids<T: KeyValuePair>(yep: &mut Vec<T>, index: &[u32]) {
//     for el in yep.iter_mut() {
//         let val_id = el.get_key() as usize;
//         el.set_key(index[val_id]);
//     }
// }

// #[test]
// fn replace_term_ids_test() {
//     let mut yep = vec![];
//     yep.push(ValIdPair::new(1 as u32, 2 as u32));
//     replace_term_ids(&mut yep, &vec![10, 10]);
//     assert_eq!(yep, vec![ValIdPair::new(10 as u32, 2 as u32)]);
// }

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

fn buffered_index_to_direct_index(db_path: &str, path: &str, mut buffered_index_data: BufferedIndexWriter) -> Result<IndexIdToOneParentFlushing, io::Error> {
    let data_file_path = PathBuf::from(db_path).join(path);
    let mut store = IndexIdToOneParentFlushing::new(data_file_path, buffered_index_data.max_value_id);
    if buffered_index_data.is_in_memory() {
        stream_iter_to_direct_index(buffered_index_data.into_iter_inmemory(), &mut store)?;
    } else {
        stream_iter_to_direct_index(buffered_index_data.flush_and_kmerge()?, &mut store)?;
    }

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

fn parse_json_and_prepare_indices<I>(
    stream1: I,
    persistence: &Persistence,
    fields_config: &FieldsConfig,
    term_data: &mut AllTermsAndDocumentBuilder,
) -> Result<(FnvHashMap<String, PathData>, FnvHashMap<String, PathDataIds>), io::Error>
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
                prepare_path_data(&persistence.temp_dir(), &persistence, &fields_config, path, term_data)
            });

            let text_info = get_text_info(&mut data.term_data, &value);
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
                    let my_int = value.parse::<u32>().unwrap_or_else(|_| panic!("Expected an int value but got {:?}", value));
                    el.add(parent_val_id, my_int)?;
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

                    if let Some(el) = data.token_to_anchor_id_score.as_mut() {
                        calculate_and_add_token_score_in_doc(&mut tokens_to_anchor_id, anchor_id, current_token_pos, el)?;
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
                //TODO FIXME BUG ALL SUB LEVELS ARE NOT HANDLED (not every supath hat it's own config yet) ONLY THE LEAFES BEFORE .TEXTINDEX
                let value_to_parent = if field_config.is_index_enabled(IndexCreationType::ValueIDToParent) {
                    Some(BufferedIndexWriter::new_for_sorted_id_insertion(persistence.temp_dir()))
                } else {
                    None
                };
                let parent_to_value = if field_config.is_index_enabled(IndexCreationType::ParentToValueID) {
                    Some(BufferedIndexWriter::new_for_sorted_id_insertion(persistence.temp_dir()))
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
        // if let Some(el) = data.text_id_to_token_ids.as_ref() {
        //     trace!(
        //         "{}\n{}",
        //         &path.add(TEXT_ID_TO_TOKEN_IDS),
        //         print_index_id_to_parent(&el, "value_id", "token_id")
        //     );
        // }
        // trace!(
        //     "{}\n{}",
        //     &path.add(TEXT_ID_TO_TOKEN_IDS),
        //     print_index_id_to_parent(&data.text_id_to_token_ids, "value_id", "token_id")
        // );

        if let Some(el) = data.text_id_to_parent.as_ref() {
            trace!("{}\n{}", &path.add(VALUE_ID_TO_PARENT), &el);
        }
        // trace!("{}\n{}", &path.add(VALUE_ID_TO_PARENT), &data.text_id_to_parent);
        // trace!("{}\n{}", &path.add(PARENT_TO_VALUE_ID), &data.parent_to_text_id);
        if let Some(el) = data.parent_to_text_id.as_ref() {
            trace!("{}\n{}", &path.add(PARENT_TO_VALUE_ID), &el);
        }
        if let Some(el) = data.text_id_to_anchor.as_ref() {
            trace!("{}\n{}", &path.add(TEXT_ID_TO_ANCHOR), &el);
        }
        if let Some(el) = data.anchor_to_text_id.as_ref() {
            trace!("{}\n{}", &path.add(ANCHOR_TO_TEXT_ID), &el);
        }
        // trace!("{}\n{}", &path.add(TEXT_ID_TO_ANCHOR), &data.text_id_to_anchor);

        // trace!(
        //     "{}\n{}",
        //     &path.add(TEXT_ID_TO_ANCHOR),
        //     print_vec(&data.text_id_to_anchor, "anchor_id", "anchor_id")
        // );
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
    db_path: &str,
    path: &str,
    mut buffered_index_data: BufferedIndexWriter,
    sort_and_dedup: bool,
) -> Result<IndirectIMFlushingInOrderVint, VelociError> {
    let mut store = IndirectIMFlushingInOrderVint::new(get_file_path(db_path, path), buffered_index_data.max_value_id);

    if buffered_index_data.is_in_memory() {
        stream_iter_to_indirect_index(buffered_index_data.into_iter_inmemory(), &mut store, sort_and_dedup)?;
    } else {
        stream_iter_to_indirect_index(buffered_index_data.flush_and_kmerge()?, &mut store, sort_and_dedup)?;
    }

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
    use std::slice::from_raw_parts_mut;
    for (id, group) in &iter.group_by(|el| el.key) {
        let mut group: Vec<(ValueId, ValueId)> = group.map(|el| el.value).collect();
        group.sort_unstable_by_key(|el| el.0);
        group.dedup_by(|a, b| {
            //store only best hit
            if a.0 == b.0 {
                b.1 += a.1; // a is the latter and gets removed, so add to b
                true
            } else {
                false
            }
        });
        #[allow(trivial_casts)]
        let mut slice: &mut [u32] = unsafe {
            &mut *(from_raw_parts_mut(group.as_mut_ptr(), group.len() * 2) as *mut [(ValueId, ValueId)] as *mut [u32]) //DANGER ZONE: THIS COULD BREAK IF THE MEMORY LAYOUT OF TUPLE CHANGES
        };
        target.set_scores(id, &mut slice)?;
    }

    Ok(())
}

pub fn add_anchor_score_flush(
    db_path: &str,
    path_col: &str,
    path: String,
    mut buffered_index_data: BufferedIndexWriter<ValueId, (ValueId, ValueId)>,
    indices: &mut IndicesFromRawData,
) -> Result<(), io::Error> {
    let indirect_file_path = util::get_file_path(db_path, &path).set_ext(Ext::Indirect);
    let data_file_path = util::get_file_path(db_path, &path).set_ext(Ext::Data);
    //If the buffered index_data is larger than 4GB, we switch to u64 for addressing the data block
    if buffered_index_data.bytes_written() < 2_u64.pow(32) {
        let mut store = TokenToAnchorScoreVintFlushing::<u32>::new(indirect_file_path, data_file_path);
        // stream_buffered_index_writer_to_anchor_score(buffered_index_data, &mut store)?;
        if buffered_index_data.is_in_memory() {
            stream_iter_to_anchor_score(buffered_index_data.into_iter_inmemory(), &mut store)?;
        } else {
            stream_iter_to_anchor_score(buffered_index_data.flush_and_kmerge()?, &mut store)?;
        }

        //when there has been written something to disk flush the rest of the data too, so we have either all data im oder on disk
        if !store.is_in_memory() {
            store.flush()?;
        }

        indices.push(IndexData {
            path_col: path_col.to_string(),
            path,
            index: IndexVariants::TokenToAnchorScoreU32(store),
            loading_type: LoadingType::Disk,
            index_category: IndexCategory::AnchorScore,
        });
    } else {
        let mut store = TokenToAnchorScoreVintFlushing::<u64>::new(indirect_file_path, data_file_path);
        // stream_buffered_index_writer_to_anchor_score(buffered_index_data, &mut store)?;
        if buffered_index_data.is_in_memory() {
            stream_iter_to_anchor_score(buffered_index_data.into_iter_inmemory(), &mut store)?;
        } else {
            stream_iter_to_anchor_score(buffered_index_data.flush_and_kmerge()?, &mut store)?;
        }

        //when there has been written something to disk flush the rest of the data too, so we have either all data im oder on disk
        if !store.is_in_memory() {
            store.flush()?;
        }

        indices.push(IndexData {
            path_col: path_col.to_string(),
            path,
            index: IndexVariants::TokenToAnchorScoreU64(store),
            loading_type: LoadingType::Disk,
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
        stream_iter_to_phrase_index(index_writer.into_iter_inmemory(), target)?;
    } else {
        stream_iter_to_phrase_index(index_writer.flush_and_kmerge()?, target)?;
    }

    // when there has been written something to disk flush the rest of the data too, so we have either all data im oder on disk
    if !target.is_in_memory() {
        target.flush()?;
    }
    Ok(())
}
fn add_phrase_pair_flush(
    db_path: &str,
    path_col: &str,
    path: String,
    buffered_index_data: BufferedIndexWriter<(ValueId, ValueId), u32>,
    indices: &mut IndicesFromRawData,
) -> Result<(), io::Error> {
    let indirect_file_path = util::get_file_path(db_path, &path).set_ext(Ext::Indirect);
    let data_file_path = util::get_file_path(db_path, &path).set_ext(Ext::Data);

    let mut store = IndirectIMFlushingInOrderVintNoDirectEncode::<(ValueId, ValueId)>::new(indirect_file_path, data_file_path, buffered_index_data.max_value_id);
    stream_buffered_index_writer_to_phrase_index(buffered_index_data, &mut store)?;

    indices.push(IndexData {
        path_col: path_col.to_string(),
        path,
        index: IndexVariants::Phrase(store),
        loading_type: LoadingType::Disk,
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
    loading_type: LoadingType,
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
    db_path: &str,
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
                           indices: &mut IndicesFromRawData,
                           loading_type: LoadingType|
     -> Result<(), VelociError> {
        if is_always_1_to_1 {
            let store = buffered_index_to_direct_index(db_path, &path, buffered_index_data)?;
            indices.push(IndexData {
                path_col: path_col.to_string(),
                path,
                index: IndexVariants::SingleValue(store),
                loading_type,
                index_category: IndexCategory::KeyValue,
            });
        } else {
            let store = buffered_index_to_indirect_index_multiple(db_path, &path, buffered_index_data, sort_and_dedup)?;
            indices.push(IndexData {
                path_col: path_col.to_string(),
                path,
                index: IndexVariants::MultiValue(store),
                loading_type,
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
                add_index_flush(&path_col, path.add(TOKENS_TO_TEXT_ID), *tokens_to_text_id, false, true, &mut indices, LoadingType::Disk)?;
            }

            if let Some(token_to_anchor_id_score) = data.token_to_anchor_id_score {
                add_anchor_score_flush(&db_path, &path_col, path.add(TO_ANCHOR_ID_SCORE), *token_to_anchor_id_score, &mut indices)?;
            }

            if let Some(phrase_pair_to_anchor) = data.phrase_pair_to_anchor {
                add_phrase_pair_flush(&db_path, &path_col, path.add(PHRASE_PAIR_TO_ANCHOR), *phrase_pair_to_anchor, &mut indices)?;
            }

            let no_sort_and_dedup = false;
            if let Some(text_id_to_token_ids) = data.text_id_to_token_ids {
                add_index_flush(
                    &path_col,
                    path.add(TEXT_ID_TO_TOKEN_IDS),
                    text_id_to_token_ids.data,
                    false,
                    no_sort_and_dedup,
                    &mut indices,
                    LoadingType::Disk,
                )?;
            }

            if let Some(text_id_to_parent) = data.text_id_to_parent {
                add_index_flush(
                    &path_col,
                    path.add(VALUE_ID_TO_PARENT),
                    *text_id_to_parent,
                    false, // valueIdToParent relation is always 1 to 1, expect for text_ids, which can have multiple parents. Here we handle only text_ids therefore is this always false
                    no_sort_and_dedup,
                    &mut indices,
                    LoadingType::Disk,
                )?;
            }

            if let Some(value_id_to_anchor) = data.value_id_to_anchor {
                add_index_flush(
                    &path_col,
                    path_col.add(VALUE_ID_TO_ANCHOR),
                    *value_id_to_anchor,
                    false,
                    no_sort_and_dedup,
                    &mut indices,
                    LoadingType::Disk,
                )?;
            }

            let loading_type = if indices_json.get(path).facet && !is_1_to_n(path) {
                LoadingType::InMemory
            } else {
                LoadingType::Disk
            };

            if let Some(parent_to_text_id) = data.parent_to_text_id {
                add_index_flush(
                    &path_col,
                    path.add(PARENT_TO_VALUE_ID),
                    *parent_to_text_id,
                    true, // This is parent_to_text_id here - Every Value id hat one associated text_id
                    no_sort_and_dedup,
                    &mut indices,
                    loading_type,
                )?;
            }

            if let Some(text_id_to_anchor) = data.text_id_to_anchor {
                add_index_flush(&path_col, path.add(TEXT_ID_TO_ANCHOR), *text_id_to_anchor, false, true, &mut indices, LoadingType::Disk)?;
            }

            if let Some(anchor_to_text_id) = data.anchor_to_text_id {
                add_index_flush(
                    &path_col,
                    path.add(ANCHOR_TO_TEXT_ID),
                    *anchor_to_text_id,
                    false,
                    no_sort_and_dedup,
                    &mut indices,
                    LoadingType::InMemory,
                )?;
            }

            if let Some(buffered_index_data) = data.boost {
                let boost_path = extract_field_name(path).add(BOOST_VALID_TO_VALUE);

                let store = buffered_index_to_indirect_index_multiple(db_path, &boost_path, *buffered_index_data, false)?;
                indices.push(IndexData {
                    path_col: path_col.to_string(),
                    path: boost_path.to_string(),
                    index: IndexVariants::MultiValue(store),
                    loading_type: LoadingType::InMemory,
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
            let path_col = path.to_string();
            let path = &path;

            if let Some(value_to_parent) = data.value_to_parent {
                add_index_flush(
                    &path_col,
                    path.add(VALUE_ID_TO_PARENT),
                    value_to_parent,
                    true, // valueIdToParent relation is always 1 to 1, expect for text_ids, which can have multiple parents. Here we handle all except .textindex data therefore is this always true
                    false,
                    &mut indices,
                    LoadingType::Disk,
                )?;
            }
            if let Some(parent_to_value) = data.parent_to_value {
                add_index_flush(&path_col, path.add(PARENT_TO_VALUE_ID), parent_to_value, false, false, &mut indices, LoadingType::Disk)?;
            }

            Ok(indices)
        })
        .collect();

    for indice in indices_res_2? {
        indices.extend(indice);
    }

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
    mut persistence: &mut Persistence,
    indices_json: &FieldsConfig,
    load_persistence: bool,
) -> Result<(), VelociError>
where
    I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>,
    J: Iterator<Item = Result<serde_json::Value, serde_json::Error>>,
    K: Iterator<Item = S>,
{
    let mut term_data = AllTermsAndDocumentBuilder::default();

    let doc_write_res = write_docs(&mut persistence, stream3)?;
    get_allterms_per_path(stream1, &indices_json, &mut term_data)?;

    let default_fulltext_options = FulltextIndexOptions::new_with_tokenize();
    {
        info_time!("set term ids and write fst");
        let reso: Result<FnvHashMap<String, FieldInfo>, io::Error> = term_data
            .terms_in_path
            .par_iter_mut()
            .map(|(path, mut terms_data)| {
                let mut textindex_metadata = TextIndexValuesMetadata::default();
                let options: &FulltextIndexOptions = indices_json.get(&path).fulltext.as_ref().unwrap_or_else(|| &default_fulltext_options);
                let path_text_index = path.to_string() + TEXTINDEX;
                textindex_metadata.options = options.clone();
                let mut col_info = FieldInfo {
                    name: path.to_string(),
                    has_fst: true,
                    textindex_metadata,
                    ..Default::default()
                };

                store_full_text_info_and_set_ids(&persistence, &mut terms_data, &path_text_index, &options, &mut col_info, &doc_write_res)?;
                Ok((path.to_string(), col_info))
            })
            .collect();

        for (path, col_info) in reso? {
            persistence.metadata.columns.insert(path.to_string(), col_info);
        }
        // persistence.metadata.fulltext_indices = reso?;
        persistence.load_all_fst()?;

        info!(
            "All text memory {}",
            persistence::get_readable_size(term_data.terms_in_path.iter().map(|el| el.1.terms.memory_footprint()).sum())
        );
        info!(
            "All raw text data memory {}",
            persistence::get_readable_size(term_data.terms_in_path.iter().map(|el| el.1.terms.total_size_of_text_data()).sum())
        );
    }

    // check_similarity(&data.terms_in_path);
    info_time!("create and (write) fulltext_index");
    trace!("all_terms {:?}", term_data.terms_in_path);

    let (mut path_data, tuples_to_parent_in_path) = parse_json_and_prepare_indices(stream2, &persistence, &indices_json, &mut term_data)?;

    // std::mem::drop(create_cache);

    if log_enabled!(log::Level::Trace) {
        print_indices(&mut path_data);
    }

    let mut indices = convert_raw_path_data_to_indices(&persistence.db, path_data, tuples_to_parent_in_path, &indices_json)?;
    if persistence.persistence_type == persistence::PersistenceType::Persistent {
        info_time!("write indices");
        for index_data in &mut indices {
            let mut index_metadata = IndexMetadata {
                loading_type: index_data.loading_type,
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
    }

    // load the converted indices, without writing them
    if load_persistence {
        let doc_offsets_file = persistence.get_file_handle("data.offsets")?;
        let doc_offsets_mmap = unsafe { MmapOptions::new().map(&doc_offsets_file).unwrap() };
        persistence.indices.doc_offsets = Some(doc_offsets_mmap);

        for index_data in indices {
            let path = index_data.path;
            match index_data.index {
                IndexVariants::Phrase(index) => {
                    if index.is_in_memory() {
                        persistence.indices.phrase_pair_to_anchor.insert(path, Box::new(index.into_im_store())); //Move data
                    } else {
                        let store = IndirectIMBinarySearchMMAP::from_path(&(persistence.db.to_string() + "/" + &path), index.metadata)?; //load data with MMap
                        persistence.indices.phrase_pair_to_anchor.insert(path, Box::new(store));
                    }
                }
                IndexVariants::SingleValue(index) => {
                    if index.is_in_memory() {
                        persistence.indices.key_value_stores.insert(path, Box::new(index.into_im_store())); //Move data
                    } else {
                        let store = SingleArrayMMAPPacked::from_file(&persistence.get_file_handle(&path)?, index.metadata)?; //load data with MMap
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
    let stream1 = data_str.lines().map(|line| serde_json::from_str(&line));
    let stream2 = data_str.lines().map(|line| serde_json::from_str(&line));

    create_indices_from_streams(persistence, stream1, stream2, data_str.lines(), indices, load_persistence)
}
pub fn create_indices_from_file(persistence: &mut Persistence, data_path: &str, indices: &str, load_persistence: bool) -> Result<(), VelociError> {
    let stream1 = std::io::BufReader::new(File::open(data_path)?).fast_lines();
    let stream2 = std::io::BufReader::new(File::open(data_path)?).fast_lines();
    let stream3 = std::io::BufReader::new(File::open(data_path)?).lines().map(|line| line.unwrap());

    create_indices_from_streams(persistence, stream1, stream2, stream3, indices, load_persistence)
}

pub fn create_indices_from_streams<I, J, K, S: AsRef<str>>(
    mut persistence: &mut Persistence,
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
    info_time!("total time create_indices for {:?}", persistence.db);

    let mut indices_json: FieldsConfig = config_from_string(indices)?;
    indices_json.features_to_indices()?;
    create_fulltext_index(stream1, stream2, stream3, &mut persistence, &indices_json, load_persistence)?;

    info_time!("write json and metadata {:?}", persistence.db);

    Ok(())
}
