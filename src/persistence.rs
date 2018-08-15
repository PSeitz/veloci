use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::{self, File};
use std::io;
use std::io::prelude::*;
use std::marker::Sync;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{self, env, mem, str, u32};
use vint::vint::VintArrayIterator;

use num::cast::ToPrimitive;
use num::{self, Integer};

// use lru_cache;
use serde_json;

use fnv::FnvHashMap;

use log;

use fst::Map;
// use rayon::prelude::*;

use prettytable::format;
use prettytable::Table;

use create;
use persistence_data::*;
use persistence_data_binary_search::*;
use persistence_score::*;
use search::*;
use search::{self, SearchError};
use search_field;
use type_info;
use util;
use util::get_file_path;
use util::*;

use heapsize::HeapSizeOf;

use colored::*;
use lru_time_cache::LruCache;
use parking_lot::RwLock;
use std::str::FromStr;

pub const TOKENS_TO_TEXT_ID: &'static str = ".tokens_to_text_id";
pub const TEXT_ID_TO_TOKEN_IDS: &'static str = ".text_id_to_token_ids";
pub const TO_ANCHOR_ID_SCORE: &'static str = ".to_anchor_id_score";
pub const PHRASE_PAIR_TO_ANCHOR: &'static str = ".phrase_pair_to_anchor";
pub const VALUE_ID_TO_PARENT: &'static str = ".value_id_to_parent";
pub const PARENT_TO_VALUE_ID: &'static str = ".parent_to_value_id";
pub const TEXT_ID_TO_ANCHOR: &'static str = ".text_id_to_anchor";
// pub const PARENT_TO_TEXT_ID: &'static str = ".parent_to_text_id";
pub const ANCHOR_TO_TEXT_ID: &'static str = ".anchor_to_text_id";
pub const BOOST_VALID_TO_VALUE: &'static str = ".boost_valid_to_value";
pub const TOKEN_VALUES: &'static str = ".token_values";

pub const TEXTINDEX: &'static str = ".textindex";

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct MetaData {
    pub stores: Vec<KVStoreMetaData>,
    pub id_lists: FnvHashMap<String, IDList>,
    // pub key_value_stores: Vec<KVStoreMetaData>,
    // pub anchor_score_stores: Vec<KVStoreMetaData>,
    // pub boost_stores: Vec<KVStoreMetaData>,
    // pub text_index_metadata: TextIndexMetaData,
    pub fulltext_indices: FnvHashMap<String, TextIndexMetaData>,
}

impl MetaData {
    pub fn new(folder: &str) -> Result<MetaData, SearchError> {
        let json = util::file_as_string(&(folder.to_string() + "/metaData.json"))?;
        Ok(serde_json::from_str(&json)?)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TextIndexMetaData {
    pub num_text_ids: usize,
    pub num_long_text_ids: usize,
    pub options: create::FulltextIndexOptions,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy, HeapSizeOf, PartialEq)]
pub struct IndexMetaData {
    pub max_value_id: u32,
    pub avg_join_size: f32,
    pub num_values: u32,
    pub num_ids: u32,
}

impl IndexMetaData {
    pub fn new(max_value_id: u32) -> Self {
        IndexMetaData {
            max_value_id: max_value_id,
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
    IdList,
}
impl Default for IndexCategory {
    fn default() -> IndexCategory {
        IndexCategory::KeyValue
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct KVStoreMetaData {
    pub index_category: IndexCategory,
    pub path: String,
    pub index_type: KVStoreType,
    #[serde(default)]
    pub is_empty: bool,
    pub loading_type: LoadingType,
    pub metadata: IndexMetaData,
    // pub max_value_id: u32,  // max value on the "right" side key -> value, key -> value ..
    // pub avg_join_size: f32, // some join statistics
}

pub static EMPTY_BUCKET: u32 = 0;
pub static VALUE_OFFSET: u32 = 1; // because 0 is reserved for EMPTY_BUCKET

#[derive(Debug, Default)]
pub struct PersistenceIndices {
    pub key_value_stores: HashMap<String, Box<IndexIdToParent<Output = u32>>>,
    pub token_to_anchor_score: HashMap<String, Box<TokenToAnchorScore>>,
    pub phrase_pair_to_anchor: HashMap<String, Box<PhrasePairToAnchor<Input = (u32, u32)>>>,
    pub boost_valueid_to_value: HashMap<String, Box<IndexIdToParent<Output = u32>>>,
    index_64: HashMap<String, Box<IndexIdToParent<Output = u64>>>,
    pub fst: HashMap<String, Map>,
}

// impl PersistenceIndices {
//     fn merge(&mut self, other: PersistenceIndices) {
//         self.key_value_stores.extend(other.key_value_stores);
//         self.token_to_anchor_score.extend(other.token_to_anchor_score);
//         self.boost_valueid_to_value.extend(other.boost_valueid_to_value);
//         self.index_64.extend(other.index_64);
//         self.fst.extend(other.fst);
//     }
// }

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum PersistenceType {
    /// Transient Doesn't write indices, just holds them in memory. Good for small indices with incremental updates.
    Transient,
    Persistent,
}

pub struct Persistence {
    pub db: String, // folder
    pub meta_data: MetaData,
    pub persistence_type: PersistenceType,
    pub indices: PersistenceIndices,
    pub lru_cache: HashMap<String, LruCache<RequestSearchPart, SearchResult>>,
    // pub lru_fst: HashMap<String, LruCache<(String, u8), Box<fst::Automaton<State=Option<usize>>>>>,
    pub term_boost_cache: RwLock<LruCache<Vec<RequestSearchPart>, Vec<search_field::SearchFieldResult>>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum LoadingType {
    InMemory,
    InMemoryUnCompressed,
    Disk,
}

impl Default for LoadingType {
    fn default() -> LoadingType {
        LoadingType::InMemory
    }
}

impl FromStr for LoadingType {
    type Err = ();

    fn from_str(s: &str) -> Result<LoadingType, ()> {
        match s {
            "InMemoryUnCompressed" => Ok(LoadingType::InMemoryUnCompressed),
            "InMemory" => Ok(LoadingType::InMemory),
            "Disk" => Ok(LoadingType::Disk),
            _ => Err(()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum KVStoreType {
    IndexIdToMultipleParentIndirect,
    IndexIdToOneParent,
}

impl Default for KVStoreType {
    fn default() -> KVStoreType {
        KVStoreType::IndexIdToMultipleParentIndirect
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct IDList {
    pub path: String,
    pub size: u64,
    pub id_type: IDDataType,
    // pub doc_id_type: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum IDDataType {
    U32,
    U64,
}

pub trait IndexIdToParentData: Integer + Clone + num::NumCast + HeapSizeOf + Debug + Sync + Send + Copy + ToPrimitive + std::iter::Step + std::hash::Hash + 'static {}
impl<T> IndexIdToParentData for T where T: Integer + Clone + num::NumCast + HeapSizeOf + Debug + Sync + Send + Copy + ToPrimitive + std::iter::Step + std::hash::Hash + 'static {}

pub trait TokenToAnchorScore: Debug + HeapSizeOf + Sync + Send + type_info::TypeInfo {
    fn get_score_iter(&self, id: u32) -> AnchorScoreIter;
}

pub trait PhrasePairToAnchor: Debug + 'static + Sync + Send {
    type Input: Debug;
    fn get_values(&self, id: Self::Input) -> Option<Vec<u32>>;
}

#[derive(Debug, Clone)]
pub struct VintArrayIteratorOpt<'a> {
    pub(crate) single_value: i64,
    pub(crate) iter: std::boxed::Box<VintArrayIterator<'a>>,
}

impl<'a> VintArrayIteratorOpt<'a> {
    pub fn from_single_val(val: u32) -> Self {
        VintArrayIteratorOpt {
            single_value: val as i64,
            iter: Box::new(VintArrayIterator::from_slice(&[])),
        }
    }

    pub fn empty() -> Self {
        VintArrayIteratorOpt {
            single_value: -2,
            iter: Box::new(VintArrayIterator::from_slice(&[])),
        }
    }

    pub fn from_slice(data: &'a [u8]) -> Self {
        VintArrayIteratorOpt {
            single_value: -1,
            iter: Box::new(VintArrayIterator::from_slice(&data)),
        }
    }
}

impl<'a> Iterator for VintArrayIteratorOpt<'a> {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<u32> {
        if self.single_value == -2 {
            None
        } else if self.single_value == -1 {
            self.iter.next()
        } else {
            let tmp = self.single_value;
            self.single_value = -2;
            Some(tmp as u32)
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

// impl<'a> FusedIterator for VintArrayIteratorOpt<'a> {}

pub trait IndexIdToParent: Debug + HeapSizeOf + Sync + Send + type_info::TypeInfo {
    type Output: IndexIdToParentData;

    fn get_values_iter(&self, _id: u64) -> VintArrayIteratorOpt {
        unimplemented!()
    }

    fn get_values(&self, id: u64) -> Option<Vec<Self::Output>>;

    #[inline]
    fn append_values_for_ids(&self, ids: &[u32], vec: &mut Vec<Self::Output>) {
        for id in ids {
            if let Some(vals) = self.get_values(u64::from(*id)) {
                vec.reserve(vals.len());
                for id in vals {
                    vec.push(id);
                }
            }
        }
    }

    #[inline]
    fn count_values_for_ids(&self, ids: &[u32], _top: Option<u32>) -> FnvHashMap<Self::Output, usize> {
        let mut hits = FnvHashMap::default();
        for id in ids {
            if let Some(vals) = self.get_values(u64::from(*id)) {
                for id in vals {
                    let stat = hits.entry(id).or_insert(0);
                    *stat += 1;
                }
            }
        }
        hits
    }

    #[inline]
    fn get_value(&self, id: u64) -> Option<Self::Output> {
        self.get_values(id).map(|el| el[0])
    }

    //last needs to be the largest value_id
    fn get_keys(&self) -> Vec<Self::Output>;

    #[inline]
    fn get_num_keys(&self) -> usize {
        self.get_keys().len()
    }

    // #[inline]
    // fn is_1_to_n(&self) -> bool {
    //     let keys = self.get_keys();
    //     keys.iter()
    //         .any(|key| self.get_values(num::cast(*key).unwrap()).map(|values| values.len() > 1).unwrap_or(false))
    // }
}

pub fn trace_index_id_to_parent<T: IndexIdToParentData>(val: &IndexIdToParent<Output = T>) {
    if log_enabled!(log::Level::Trace) {
        let keys = val.get_keys();
        for key in keys.iter().take(100) {
            if let Some(vals) = val.get_values(num::cast(*key).unwrap()) {
                let mut to = std::cmp::min(vals.len(), 100);
                trace!("key {:?} to {:?}", key, &vals[0..to]);
            }
        }
    }
}

pub fn get_readable_size(value: usize) -> ColoredString {
    match value {
        0...1_000 => format!("{:?} b", value).blue(),
        1_001...1_000_000 => format!("{:?} kb", value / 1_000).green(),
        _ => format!("{:?} mb", value / 1_000_000).red(),
    }
}

pub fn get_readable_size_for_children<T: HeapSizeOf>(value: T) -> ColoredString {
    get_readable_size(value.heap_size_of_children())
}

impl Persistence {
    #[cfg_attr(feature = "flame_it", flame)]
    pub fn load_index_64(&mut self, path: &str) -> Result<(), search::SearchError> {
        let loading_type = load_type_from_env()?.unwrap_or(LoadingType::Disk);

        match loading_type {
            LoadingType::InMemoryUnCompressed | LoadingType::InMemory => {
                let file_path = get_file_path(&self.db, path);
                self.indices.index_64.insert(
                    path.to_string(),
                    Box::new(IndexIdToOneParent::<u64, u64> {
                        data: load_index_u64(&file_path)?,
                        metadata: IndexMetaData {
                            max_value_id: u32::MAX,
                            avg_join_size: 1.0,
                            ..Default::default()
                        },
                        ok: std::marker::PhantomData,
                    }),
                );
            }
            LoadingType::Disk => {
                let store = SingleArrayMMAP::<u64>::from_path(&get_file_path(&self.db, path), IndexMetaData::default())?; //TODO METADATA WRONG
                self.indices.index_64.insert(path.to_string(), Box::new(store));
            }
        }

        Ok(())
    }

    fn load_types_index_to_one<T: IndexIdToParentData>(data_direct_path: &str, metadata: IndexMetaData) -> Result<Box<IndexIdToParent<Output = u32>>, search::SearchError> {
        let store = IndexIdToOneParent::<u32, T> {
            data: decode_bit_packed_vals(&file_path_to_bytes(data_direct_path)?, get_bytes_required(metadata.max_value_id)),
            metadata: metadata,
            ok: std::marker::PhantomData,
        };
        Ok(Box::new(store) as Box<IndexIdToParent<Output = u32>>)
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn load_from_disk(&mut self) -> Result<(), search::SearchError> {
        info_time!("loaded persistence {:?}", &self.db);
        self.load_all_id_lists()?;

        // for el in &self.meta_data.key_value_stores {
        //     self.lru_cache.insert(el.path.clone(), LruCache::with_capacity(0));
        // }

        //ANCHOR TO SCORE
        for el in &self.meta_data.stores {
            let indirect_path = get_file_path(&self.db, &el.path) + ".indirect";
            let indirect_data_path = get_file_path(&self.db, &el.path) + ".data";
            let loading_type = get_loading_type(el.loading_type)?;
            match el.index_category {
                IndexCategory::Phrase => {
                    //Insert dummy index, to seperate between emtpy indexes and nonexisting indexes
                    if el.is_empty {
                        let store = IndexIdToMultipleParentIndirectBinarySearch::<(u32, u32)> {
                            start_pos: vec![],
                            data: vec![],
                            metadata: el.metadata,
                        };
                        self.indices
                            .phrase_pair_to_anchor
                            .insert(el.path.to_string(), Box::new(store) as Box<PhrasePairToAnchor<Input = (u32, u32)>>);
                        continue;
                    }

                    let store: Box<PhrasePairToAnchor<Input = (u32, u32)>> = match loading_type {
                        LoadingType::Disk => Box::new(IndexIdToMultipleParentIndirectBinarySearchMMAP::from_path(&get_file_path(&self.db, &el.path), el.metadata)?),
                        LoadingType::InMemoryUnCompressed | LoadingType::InMemory => {
                            Box::new(IndexIdToMultipleParentIndirectBinarySearchMMAP::from_path(&get_file_path(&self.db, &el.path), el.metadata)?)
                        }
                    };
                    self.indices.phrase_pair_to_anchor.insert(el.path.to_string(), store);
                }
                IndexCategory::AnchorScore => {
                    let store: Box<TokenToAnchorScore> = match loading_type {
                        LoadingType::Disk => Box::new(TokenToAnchorScoreVintMmap::from_path(&indirect_path, &indirect_data_path)?),
                        LoadingType::InMemoryUnCompressed | LoadingType::InMemory => {
                            let mut store = TokenToAnchorScoreVintIM::default();
                            store.read(&indirect_path, &indirect_data_path).unwrap();
                            Box::new(store)
                        }
                    };
                    self.indices.token_to_anchor_score.insert(el.path.to_string(), store);
                }
                IndexCategory::Boost => {
                    match el.index_type {
                        KVStoreType::IndexIdToMultipleParentIndirect => {
                            // let meta = IndexMetaData{max_value_id: el.metadata.max_value_id, avg_join_size:el.avg_join_size, ..Default::default()};
                            let store = PointingMMAPFileReader::from_path(&get_file_path(&self.db, &el.path), el.metadata)?;
                            self.indices.boost_valueid_to_value.insert(el.path.to_string(), Box::new(store));
                        }
                        KVStoreType::IndexIdToOneParent => {
                            let store = SingleArrayMMAPPacked::<u32>::from_path(&get_file_path(&self.db, &el.path), el.metadata)?;
                            self.indices.boost_valueid_to_value.insert(el.path.to_string(), Box::new(store));
                        }
                    }
                }
                IndexCategory::KeyValue => {
                    info_time!("loaded key_value_store {:?}", &el.path);
                    let data_direct_path = get_file_path(&self.db, &el.path);

                    //Insert dummy index, to seperate between emtpy indexes and nonexisting indexes
                    if el.is_empty {
                        let store = IndexIdToOneParent::<u32, u32> {
                            data: vec![],
                            metadata: el.metadata,
                            ok: std::marker::PhantomData,
                        };
                        self.indices
                            .key_value_stores
                            .insert(el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>>);
                        continue;
                    }

                    let store = match loading_type {
                        LoadingType::InMemoryUnCompressed | LoadingType::InMemory => match el.index_type {
                            KVStoreType::IndexIdToMultipleParentIndirect => {
                                let indirect_u32 = bytes_to_vec_u32(&file_path_to_bytes(&indirect_path)?);
                                let store = IndexIdToMultipleParentIndirect {
                                    start_pos: indirect_u32,
                                    data: file_path_to_bytes(&indirect_data_path)?,
                                    cache: LruCache::with_capacity(0),
                                    metadata: IndexMetaData {
                                        max_value_id: el.metadata.max_value_id,
                                        avg_join_size: el.metadata.avg_join_size,
                                        num_values: 0,
                                        num_ids: 0,
                                    },
                                };
                                Box::new(store) as Box<IndexIdToParent<Output = u32>>
                            }
                            KVStoreType::IndexIdToOneParent => {
                                let bytes_required = get_bytes_required(el.metadata.max_value_id) as u8;
                                if bytes_required == 1 {
                                    Self::load_types_index_to_one::<u8>(&data_direct_path, el.metadata)?
                                } else if bytes_required == 2 {
                                    Self::load_types_index_to_one::<u16>(&data_direct_path, el.metadata)?
                                } else {
                                    Self::load_types_index_to_one::<u32>(&data_direct_path, el.metadata)?
                                }
                            }
                        },
                        LoadingType::Disk => match el.index_type {
                            KVStoreType::IndexIdToMultipleParentIndirect => {
                                let meta = IndexMetaData {
                                    max_value_id: el.metadata.max_value_id,
                                    avg_join_size: el.metadata.avg_join_size,
                                    ..Default::default()
                                };
                                let store = PointingMMAPFileReader::from_path(&get_file_path(&self.db, &el.path), meta)?;
                                Box::new(store) as Box<IndexIdToParent<Output = u32>>
                            }
                            KVStoreType::IndexIdToOneParent => {
                                let store = SingleArrayMMAPPacked::<u32>::from_path(&data_direct_path, el.metadata)?;

                                Box::new(store) as Box<IndexIdToParent<Output = u32>>
                            }
                        },
                    };

                    self.indices.key_value_stores.insert(el.path.to_string(), store);
                }
                IndexCategory::IdList => {}
            }
        }

        // let loaded_data: Result<Vec<(String, Box<IndexIdToParent<Output = u32>>)>, SearchError> = self
        //     .meta_data
        //     .key_value_stores
        //     .clone()
        //     .into_par_iter()
        //     .map(|el| {
        //         // info!("loading key_value_store {:?}", &el.path);
        //         info_time!("loaded key_value_store {:?}", &el.path);

        //         let loading_type = get_loading_type(el.loading_type)?;
        //         let data_direct_path = get_file_path(&self.db, &el.path);

        //         //Insert dummy index, to seperate between emtpy indexes and nonexisting indexes
        //         if el.is_empty {
        //             let store = IndexIdToOneParent::<u32, u32> {
        //                 data: vec![],
        //                 max_value_id: 0,
        //                 ok: std::marker::PhantomData,
        //                 avg_join_size: 1.0,
        //             };
        //             return Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>>));
        //         }

        //         match loading_type {
        //             LoadingType::InMemoryUnCompressed | LoadingType::InMemory => match el.index_type {
        //                 KVStoreType::IndexIdToMultipleParentIndirect => {
        //                     let indirect_path = get_file_path(&self.db, &el.path) + ".indirect";
        //                     let indirect_data_path = get_file_path(&self.db, &el.path) + ".data";
        //                     let indirect_u32 = bytes_to_vec_u32(&file_handle_to_bytes(&get_file_handle_complete_path(&indirect_path)?)?);
        //                     // let data_u32 = bytes_to_vec_u32(&file_handle_to_bytes(&get_file_handle_complete_path(&indirect_data_path)?)?);

        //                     let store = IndexIdToMultipleParentIndirect {
        //                         start_pos: indirect_u32,
        //                         data: file_handle_to_bytes(&get_file_handle_complete_path(&indirect_data_path)?)?,
        //                         cache: lru_cache::LruCache::new(0),
        //                         metadata: IndexMetaData{
        //                             max_value_id: el.max_value_id,
        //                             avg_join_size: el.avg_join_size,
        //                             num_values: 0,
        //                             num_ids: 0,
        //                         }
        //                     };

        //                     Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>>))
        //                 }
        //                 KVStoreType::IndexIdToOneParent => {
        //                     let bytes_required = get_bytes_required(el.max_value_id) as u8;
        //                     if bytes_required == 1 {
        //                         let store = IndexIdToOneParent::<u32, u8> {
        //                             data: decode_bit_packed_vals(&file_path_to_bytes(&data_direct_path)?, get_bytes_required(el.max_value_id)),
        //                             max_value_id: el.max_value_id,
        //                             ok: std::marker::PhantomData,
        //                             avg_join_size: el.avg_join_size,
        //                         };
        //                         Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>>))
        //                     } else if bytes_required == 2 {
        //                         let store = IndexIdToOneParent::<u32, u16> {
        //                             data: decode_bit_packed_vals(&file_path_to_bytes(&data_direct_path)?, get_bytes_required(el.max_value_id)),
        //                             max_value_id: el.max_value_id,
        //                             ok: std::marker::PhantomData,
        //                             avg_join_size: el.avg_join_size,
        //                         };
        //                         Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>>))
        //                     } else {
        //                         let store = IndexIdToOneParent::<u32, u32> {
        //                             data: decode_bit_packed_vals(&file_path_to_bytes(&data_direct_path)?, get_bytes_required(el.max_value_id)),
        //                             max_value_id: el.max_value_id,
        //                             ok: std::marker::PhantomData,
        //                             avg_join_size: el.avg_join_size,
        //                         };

        //                         Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>>))
        //                     }
        //                 }
        //             },
        //             LoadingType::Disk => match el.index_type {
        //                 KVStoreType::IndexIdToMultipleParentIndirect => {
        //                     let meta = IndexMetaData{max_value_id: el.max_value_id, avg_join_size:el.avg_join_size, ..Default::default()};
        //                     let store = PointingMMAPFileReader::from_path(&get_file_path(&self.db, &el.path), meta)?;

        //                     Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>>))
        //                 }
        //                 KVStoreType::IndexIdToOneParent => {
        //                     let store = SingleArrayMMAPPacked::<u32>::from_path(&data_direct_path, el.max_value_id)?;

        //                     Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>>))
        //                 }
        //             },
        //         }
        //     })
        //     .collect();

        // match loaded_data {
        //     Err(e) => return Err(e),
        //     Ok(dat) => for el in dat {
        //         self.indices.key_value_stores.insert(el.0, el.1);
        //     },
        // };

        // Load Boost Indices
        // for el in &self.meta_data.boost_stores {
        //     match el.index_type {
        //         KVStoreType::IndexIdToMultipleParentIndirect => {
        //             let meta = IndexMetaData{max_value_id: el.max_value_id, avg_join_size:el.avg_join_size, ..Default::default()};
        //             let store = PointingMMAPFileReader::from_path(&get_file_path(&self.db, &el.path), meta)?;
        //             self.indices.boost_valueid_to_value.insert(el.path.to_string(), Box::new(store));
        //         }
        //         KVStoreType::IndexIdToOneParent => {
        //             // let data_file = self.get_file_handle(&el.path)?;
        //             // let data_metadata = self.get_file_metadata_handle(&el.path)?;
        //             // let store = SingleArrayMMAP::<u32>::new(&data_file, data_metadata, el.max_value_id);

        //             let store = SingleArrayMMAPPacked::<u32>::from_path(&get_file_path(&self.db, &el.path), el.max_value_id)?;
        //             // self.indices
        //             //     .boost_valueid_to_value
        //             //     .insert(el.path.to_string(), Box::new(IndexIdToOneParentMayda::<u32>::new(&store, u32::MAX)));
        //             self.indices.boost_valueid_to_value.insert(el.path.to_string(), Box::new(store));
        //         }
        //     }
        // }

        self.load_all_fst()?;
        Ok(())
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn load_all_fst(&mut self) -> Result<(), search::SearchError> {
        for path in self.meta_data.fulltext_indices.keys() {
            let map = self.load_fst(path)?;
            self.indices.fst.insert(path.to_string(), map);
        }
        Ok(())
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn load_all_id_lists(&mut self) -> Result<(), search::SearchError> {
        for idlist in self.meta_data.id_lists.clone().values() {
            match idlist.id_type {
                IDDataType::U32 => {}
                IDDataType::U64 => self.load_index_64(&idlist.path)?,
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn load_fst(&self, path: &str) -> Result<Map, search::SearchError> {
        unsafe {
            Ok(Map::from_path(&get_file_path(&self.db, &(path.to_string() + ".fst")))?) //(path.to_string() + ".fst"))?)
        }
        // In memory version
        // let mut f = self.get_file_handle(&(path.to_string() + ".fst"))?;
        // let mut buffer: Vec<u8> = Vec::new();
        // f.read_to_end(&mut buffer)?;
        // buffer.shrink_to_fit();
        // Ok(Map::from_bytes(buffer)?)
    }

    // #[cfg_attr(feature = "flame_it", flame)]
    // pub fn get_fst(&self, path: &str) -> Result<(&Map), search::SearchError> {
    //     self.indices
    //         .fst
    //         .get(path)
    //         .ok_or_else(|| From::from(format!("fst {} not found loaded in indices", path)))
    // }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_file_handle(&self, path: &str) -> Result<File, search::SearchError> {
        Ok(File::open(PathBuf::from(get_file_path(&self.db, path))).map_err(|err| search::SearchError::StringError(format!("Could not open {} {:?}", path, err)))?)
    }

    // #[cfg_attr(feature = "flame_it", flame)]
    // pub(crate) fn get_file_search(&self, path: &str) -> FileSearch {
    //     FileSearch::new(path, self.get_file_handle(path).unwrap())
    // }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_boost(&self, path: &str) -> Result<&IndexIdToParent<Output = u32>, search::SearchError> {
        self.indices
            .boost_valueid_to_value
            .get(path)
            .map(|el| el.as_ref())
            .ok_or_else(|| path_not_found(path.as_ref()))
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn has_index(&self, path: &str) -> bool {
        self.indices.key_value_stores.contains_key(path)
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_token_to_anchor<S: AsRef<str>>(&self, path: S) -> Result<&TokenToAnchorScore, search::SearchError> {
        let path = path.as_ref().add(TO_ANCHOR_ID_SCORE);
        self.indices
            .token_to_anchor_score
            .get(&path)
            .map(|el| el.as_ref())
            .ok_or_else(|| path_not_found(path.as_ref()))
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_phrase_pair_to_anchor<S: AsRef<str>>(&self, path: S) -> Result<&PhrasePairToAnchor<Input = (u32, u32)>, search::SearchError> {
        // let path = path.as_ref().add(TO_ANCHOR_ID_SCORE);
        self.indices
            .phrase_pair_to_anchor
            .get(path.as_ref())
            .map(|el| el.as_ref())
            .ok_or_else(|| path_not_found(path.as_ref()))
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_valueid_to_parent<S: AsRef<str>>(&self, path: S) -> Result<&IndexIdToParent<Output = u32>, search::SearchError> {
        self.indices
            .key_value_stores
            .get(path.as_ref())
            .map(|el| el.as_ref())
            .ok_or_else(|| path_not_found(path.as_ref()))
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_offsets(&self, path: &str) -> Result<&IndexIdToParent<Output = u64>, search::SearchError> {
        self.indices
            .index_64
            .get(&(path.to_string() + ".offsets"))
            .map(|el| el.as_ref())
            .ok_or_else(|| path_not_found(path.as_ref()))
    }

    pub fn get_number_of_documents(&self) -> Result<usize, search::SearchError> {
        Ok(self.get_offsets("data")?.get_num_keys() - 1) //the last offset marks the end and not a document
    }

    pub fn get_bytes_indexed(&self) -> Result<usize, search::SearchError> {
        let offsets = self.get_offsets("data")?;
        let last_id = offsets.get_num_keys() - 1;
        Ok(offsets.get_value(last_id as u64).unwrap() as usize) //the last offset marks the end and not a document
    }

    // #[cfg_attr(feature = "flame_it", flame)]
    // pub fn write_json_to_disk<'a, T>(&mut self, data: StreamDeserializer<'a, T, Value>, path: &str) -> Result<(), io::Error>
    // where
    //     T: serde_json::de::Read<'a>,
    // {
    //     let mut offsets = vec![];
    //     let mut file_out = self.get_buffered_writer(path)?;
    //     let mut current_offset = 0;

    //     util::iter_json_stream(data, &mut |el: &serde_json::Value| {
    //         let el_str = el.to_string().into_bytes();

    //         file_out.write_all(&el_str).unwrap();
    //         offsets.push(current_offset as u64);
    //         current_offset += el_str.len();
    //     });

    //     offsets.push(current_offset as u64);
    //     let (id_list_path, id_list) = self.write_offset(&vec_to_bytes_u64(&offsets), &offsets, &(path.to_string() + ".offsets"))?;
    //     self.meta_data.id_lists.insert(id_list_path, id_list);
    //     Ok(())
    // }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_buffered_writer(&self, path: &str) -> Result<io::BufWriter<fs::File>, io::Error> {
        use std::fs::OpenOptions;
        let file = OpenOptions::new().read(true).append(true).create(true).open(&get_file_path(&self.db, path))?;

        // Ok(io::BufWriter::new(File::create(&get_file_path(&self.db, path))?))
        Ok(io::BufWriter::new(file))
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn write_data(&self, path: &str, data: &[u8]) -> Result<(), io::Error> {
        File::create(&get_file_path(&self.db, path))?.write_all(data)?;
        Ok(())
    }

    // fn store_fst(all_terms: &Vec<String>, path:&str) -> Result<(), fst::Error> {
    //     info_time!("store_fst");
    //     let now = Instant::now();
    //     let wtr = io::BufWriter::new(File::create("map.fst")?);
    //     // Create a builder that can be used to insert new key-value pairs.
    //     let mut build = MapBuilder::new(wtr)?;
    //     for (i, line) in all_terms.iter().enumerate() {
    //         build.insert(line, i).unwrap();
    //     }
    //     build.finish()?;
    //     Ok(())
    // }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn write_meta_data(&self) -> Result<(), io::Error> {
        self.write_data("metaData.json", serde_json::to_string_pretty(&self.meta_data)?.as_bytes())
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn write_offset<T: Clone + Integer + num::NumCast + Copy + Debug>(&self, bytes: &[u8], data: &[T], path: &str) -> Result<((String, IDList)), io::Error> {
        debug_time!("Wrote Index {} With size {:?}", path, data.len());
        File::create(util::get_file_path(&self.db, path))?.write_all(bytes)?;
        info!("Wrote Index {} With size {:?}", path, data.len());
        trace!("{:?}", data);
        let sizo = match mem::size_of::<T>() {
            4 => IDDataType::U32,
            8 => IDDataType::U64,
            _ => panic!("wrong sizeee"),
        };
        Ok((
            path.to_string(),
            IDList {
                path: path.to_string(),
                size: data.len() as u64,
                id_type: sizo,
                // doc_id_type: check_is_docid_type(data),
            },
        ))
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn create(db: String) -> Result<Self, io::Error> {
        Self::create_type(db, PersistenceType::Persistent)
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn create_type(db: String, persistence_type: PersistenceType) -> Result<Self, io::Error> {
        use std::path::Path;
        if Path::new(&db).exists() {
            fs::remove_dir_all(&db)?;
        }
        fs::create_dir_all(&db)?;
        let meta_data = MetaData { ..Default::default() };
        Ok(Persistence {
            persistence_type,
            meta_data,
            db,
            lru_cache: HashMap::default(),
            term_boost_cache: RwLock::new(LruCache::with_expiry_duration_and_capacity(Duration::new(3600, 0), 10)),
            indices: PersistenceIndices::default(),
        })
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn load<P: AsRef<Path>>(db: P) -> Result<Self, search::SearchError> {
        let meta_data = MetaData::new(db.as_ref().to_str().unwrap())?;
        let mut pers = Persistence {
            persistence_type: PersistenceType::Persistent,
            meta_data,
            db: db.as_ref().to_str().unwrap().to_string(),
            lru_cache: HashMap::default(), // LruCache::new(50),
            term_boost_cache: RwLock::new(LruCache::with_expiry_duration_and_capacity(Duration::new(3600, 0), 10)),
            indices: PersistenceIndices::default(),
        };
        pers.load_from_disk()?;
        pers.print_heap_sizes();
        Ok(pers)
    }

    pub fn print_heap_sizes(&self) {
        info!(
            "indices.index_64 {}",
            // get_readable_size_for_children(&self.indices.index_64)
            get_readable_size(self.indices.index_64.heap_size_of_children())
        );
        info!(
            "indices.key_value_stores {}",
            get_readable_size(self.indices.key_value_stores.heap_size_of_children()) // get_readable_size_for_children(&self.indices.key_value_stores)
        );
        info!("indices.boost_valueid_to_value {}", get_readable_size_for_children(&self.indices.boost_valueid_to_value));
        info!("indices.token_to_anchor_score {}", get_readable_size_for_children(&self.indices.token_to_anchor_score));
        info!("indices.fst {}", get_readable_size(self.get_fst_sizes()));
        info!("------");
        let total_size = self.get_fst_sizes()
            + self.indices.key_value_stores.heap_size_of_children()
            + self.indices.index_64.heap_size_of_children()
            + self.indices.boost_valueid_to_value.heap_size_of_children()
            + self.indices.token_to_anchor_score.heap_size_of_children();

        info!("totale size {}", get_readable_size(total_size));

        let mut print_and_size = vec![];
        for (k, v) in &self.indices.key_value_stores {
            print_and_size.push((v.heap_size_of_children(), v.type_name(), k));
        }
        for (k, v) in &self.indices.token_to_anchor_score {
            print_and_size.push((v.heap_size_of_children(), v.type_name(), k));
        }
        for (k, v) in &self.indices.index_64 {
            print_and_size.push((v.heap_size_of_children(), v.type_name(), k));
        }
        for (k, v) in &self.indices.fst {
            print_and_size.push((v.as_fst().size(), "FST".to_string(), k));
        }
        // Sort by size
        print_and_size.sort_by_key(|row| row.0);

        // Create the table
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        table.add_row(row!["Type", "Path", "Size"]);
        for row in print_and_size {
            table.add_row(row![row.1, row.2, get_readable_size(row.0)]);
        }

        info!("{}", table);
    }

    pub fn get_all_fields(&self) -> Vec<String> {
        self.meta_data.fulltext_indices.keys().map(|el| util::extract_field_name(el)).collect()
    }

    fn get_fst_sizes(&self) -> usize {
        self.indices.fst.iter().map(|(_, v)| v.as_fst().size()).sum()
    }
}

fn path_not_found(path: &str) -> search::SearchError {
    let error = format!("Did not found path in indices {}", path);
    error!("{:?}", error);
    From::from(error)
}

// #[derive(Debug)]
// pub(crate) struct FileSearch {
//     path: String,
//     // offsets: Vec<u64>,
//     file: File,
//     buffer: Vec<u8>,
// }

// impl FileSearch {
//     fn load_text(&mut self, pos: u64, offsets: &IndexIdToParent<Output = u64>) {
//         use std::io::{SeekFrom};
//         // @Temporary Use Result
//         let string_size = offsets.get_value(pos + 1).unwrap() - offsets.get_value(pos).unwrap() - 1;
//         // let mut buffer:Vec<u8> = Vec::with_capacity(string_size as usize);
//         // unsafe { buffer.set_len(string_size as usize); }
//         self.buffer.resize(string_size as usize, 0);
//         self.file.seek(SeekFrom::Start(offsets.get_value(pos).unwrap())).unwrap();
//         self.file.read_exact(&mut self.buffer).unwrap();
//         // unsafe {str::from_utf8_unchecked(&buffer)}
//         // let s = unsafe {str::from_utf8_unchecked(&buffer)};
//         // str::from_utf8(&buffer).unwrap() // @Temporary  -> use unchecked if stable
//     }

//     pub fn get_text_for_id(&mut self, pos: usize, offsets: &IndexIdToParent<Output = u64>) -> String {
//         self.load_text(pos as u64, offsets);
//         str::from_utf8(&self.buffer).unwrap().to_string()
//     }

//     fn new(path: &str, file: File) -> Self {
//         // load_index_64_into_cache(&(path.to_string()+".offsets")).unwrap();
//         FileSearch {
//             path: path.to_string(),
//             file,
//             buffer: Vec::with_capacity(50 as usize),
//         }
//     }

//     // pub fn binary_search(&mut self, term: &str, persistence: &Persistence) -> Result<(String, i64), io::Error> {
//     //     // let cache_lock = INDEX_64_CACHE.read().unwrap();
//     //     // let offsets = cache_lock.get(&(self.path.to_string()+".offsets")).unwrap();
//     //     let offsets = persistence.indices.index_64.get(&(self.path.to_string() + ".offsets")).unwrap();
//     //     debug_time!("term binary_search");
//     //     if offsets.len() < 2 {
//     //         return Ok(("".to_string(), -1));
//     //     }
//     //     let mut low = 0;
//     //     let mut high = offsets.len() - 2;
//     //     let mut i;
//     //     while low <= high {
//     //         i = (low + high) >> 1;
//     //         self.load_text(i, offsets);
//     //         // info!("Comparing {:?}", str::from_utf8(&buffer).unwrap());
//     //         // comparison = comparator(arr[i], find);
//     //         if str::from_utf8(&self.buffer).unwrap() < term {
//     //             low = i + 1;
//     //             continue;
//     //         }
//     //         if str::from_utf8(&self.buffer).unwrap() > term {
//     //             high = i - 1;
//     //             continue;
//     //         }
//     //         return Ok((str::from_utf8(&self.buffer).unwrap().to_string(), i as i64));
//     //     }
//     //     Ok(("".to_string(), -1))
//     // }
// }

fn load_type_from_env() -> Result<Option<LoadingType>, search::SearchError> {
    if let Some(val) = env::var_os("LoadingType") {
        let conv_env = val
            .clone()
            .into_string()
            .map_err(|_err| search::SearchError::StringError(format!("Could not convert LoadingType environment variable to utf-8: {:?}", val)))?;
        let loading_type = LoadingType::from_str(&conv_env)
            .map_err(|_err| search::SearchError::StringError("only InMemoryUnCompressed, InMemory or Disk allowed for LoadingType environment variable".to_string()))?;
        Ok(Some(loading_type))
    } else {
        Ok(None)
    }
}

fn get_loading_type(loading_type: LoadingType) -> Result<LoadingType, search::SearchError> {
    let mut loading_type = loading_type;
    if let Some(val) = load_type_from_env()? {
        // Overrule Loadingtype from env
        loading_type = val;
    }
    Ok(loading_type)
}

pub(crate) fn vec_to_bytes_u32(data: &[u32]) -> Vec<u8> {
    vec_to_bytes(data)
}

//TODO Only LittleEndian supported currently
pub(crate) fn vec_to_bytes<T>(data: &[T]) -> Vec<u8> {
    let mut out_dat: Vec<u8> = vec_with_size_uninitialized(data.len() * std::mem::size_of::<T>());
    unsafe {
        let ptr = data.as_ptr() as *const u8;
        ptr.copy_to_nonoverlapping(out_dat.as_mut_ptr(), data.len() * std::mem::size_of::<T>());
    }
    // LittleEndian::write_u32_into(data, &mut wtr);
    out_dat
}

pub(crate) fn vec_to_bytes_u64(data: &[u64]) -> Vec<u8> {
    // let mut wtr: Vec<u8> = vec_with_size_uninitialized(data.len() * std::mem::size_of::<u64>());
    // LittleEndian::write_u64_into(data, &mut wtr);
    // wtr
    vec_to_bytes(data)
}

pub(crate) fn bytes_to_vec_u32(data: &[u8]) -> Vec<u32> {
    bytes_to_vec::<u32>(&data)
}
pub(crate) fn bytes_to_vec_u64(data: &[u8]) -> Vec<u64> {
    bytes_to_vec::<u64>(&data)
}
pub(crate) fn bytes_to_vec<T>(data: &[u8]) -> Vec<T> {
    let mut out_dat = vec_with_size_uninitialized(data.len() / std::mem::size_of::<T>());
    // LittleEndian::read_u64_into(&data, &mut out_dat);
    unsafe {
        let ptr = data.as_ptr() as *const T;
        ptr.copy_to_nonoverlapping(out_dat.as_mut_ptr(), data.len() / std::mem::size_of::<T>());
    }
    out_dat
}

pub(crate) fn file_path_to_bytes<P: AsRef<Path> + std::fmt::Debug>(s1: P) -> Result<Vec<u8>, search::SearchError> {
    let f = get_file_handle_complete_path(s1)?;
    file_handle_to_bytes(&f)
}

pub(crate) fn file_handle_to_bytes(f: &File) -> Result<Vec<u8>, search::SearchError> {
    let file_size = { f.metadata()?.len() as usize };
    let mut reader = std::io::BufReader::new(f);
    let mut buffer: Vec<u8> = Vec::with_capacity(file_size + 1);
    reader.read_to_end(&mut buffer)?;
    // buffer.shrink_to_fit();
    Ok(buffer)
}

pub(crate) fn load_index_u32<P: AsRef<Path> + std::fmt::Debug>(s1: P) -> Result<Vec<u32>, search::SearchError> {
    info!("Loading Index32 {:?} ", s1);
    Ok(bytes_to_vec_u32(&file_path_to_bytes(s1)?))
}

pub(crate) fn load_index_u64<P: AsRef<Path> + std::fmt::Debug>(s1: P) -> Result<Vec<u64>, search::SearchError> {
    info!("Loading Index64 {:?} ", s1);
    Ok(bytes_to_vec_u64(&file_path_to_bytes(s1)?))
}

// fn check_is_docid_type<T: Integer + num::NumCast + Copy>(data: &[T]) -> bool {
//     for (index, value_id) in data.iter().enumerate() {
//         let blub: usize = num::cast(*value_id).unwrap();
//         if blub != index {
//             return false;
//         }
//     }
//     true
// }

pub(crate) fn get_file_handle_complete_path<P: AsRef<Path> + std::fmt::Debug>(path: P) -> Result<File, search::SearchError> {
    Ok(File::open(&path).map_err(|err| search::SearchError::StringError(format!("Could not open {:?} {:?}", path, err)))?)
}
