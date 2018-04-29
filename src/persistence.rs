use std::fs::{self, File};
use std::io::prelude::*;

use std::collections::HashMap;
use std::fmt::Debug;
#[allow(unused_imports)]
use std::io::{self, Cursor, SeekFrom};
use std::marker::Sync;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{self, env, mem, str, u32};

use create;
use util;
use util::get_file_path;
use util::*;

use search::*;
use search::{self, SearchError};
use search_field;

use num::cast::ToPrimitive;
use num::{self, Integer, NumCast};

use serde_json;
use serde_json::StreamDeserializer;
use serde_json::Value;

use bincode::{deserialize, serialize};
use fnv::FnvHashMap;

use byteorder::{ByteOrder, LittleEndian};
use log;
use mayda;

use fst::Map;
use rayon::prelude::*;

use prettytable::format;
use prettytable::Table;

use persistence_data::*;
use persistence_score::*;
use type_info;

use heapsize::HeapSizeOf;

use colored::*;
use lru_time_cache::LruCache;
use parking_lot::RwLock;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct MetaData {
    pub id_lists: FnvHashMap<String, IDList>,
    pub key_value_stores: Vec<KVStoreMetaData>,
    pub anchor_score_stores: Vec<KVStoreMetaData>,
    pub boost_stores: Vec<KVStoreMetaData>,
    pub fulltext_indices: FnvHashMap<String, create::FulltextIndexOptions>,
}

impl MetaData {
    pub fn new(folder: &str) -> Result<MetaData, SearchError> {
        let json = util::file_as_string(&(folder.to_string() + "/metaData.json"))?;
        Ok(serde_json::from_str(&json)?)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct KVStoreMetaData {
    pub path: String,
    pub is_1_to_n: bool, // In the sense of 1:n   1key, n values
    pub persistence_type: KVStoreType,
    pub loading_type: LoadingType,
    #[serde(default = "default_max_value_id")]
    pub max_value_id: u32, // max value on the "right" side key -> value, key -> value ..
    #[serde(default = "default_avg_join")]
    pub avg_join_size: f32, // some join statistics
}

pub static NOT_FOUND: u32 = u32::MAX;

#[derive(Debug, Default)]
pub struct PersistenceIndices {
    // pub index_id_to_parent: HashMap<(String,String), Vec<Vec<u32>>>,
    pub index_id_to_parento: HashMap<String, Box<IndexIdToParent<Output = u32>>>,
    pub token_to_anchor_to_score: HashMap<String, Box<TokenToAnchorScore>>,
    pub boost_valueid_to_value: HashMap<String, Box<IndexIdToParent<Output = u32>>>,
    index_64: HashMap<String, Box<IndexIdToParent<Output = u64>>>,
    pub fst: HashMap<String, Map>,
}

pub struct Persistence {
    pub db: String, // folder
    pub meta_data: MetaData,
    pub indices: PersistenceIndices,
    pub lru_cache: HashMap<String, LruCache<RequestSearchPart, SearchResult>>,
    // pub lru_fst: HashMap<String, LruCache<(String, u8), Box<fst::Automaton<State=Option<usize>>>>>,
    pub term_boost_cache: RwLock<LruCache<Vec<RequestSearchPart>, Vec<search_field::SearchFieldResult>>>,
}

//TODO Only tmp
fn default_max_value_id() -> u32 {
    std::u32::MAX
}
fn default_avg_join() -> f32 {
    1000.0
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
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

use std::str::FromStr;

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
    ParallelArrays,
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
    pub doc_id_type: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum IDDataType {
    U32,
    U64,
}
// use persistence_data;

pub trait IndexIdToParentData:
    Integer + Clone + NumCast + mayda::utility::Bits + HeapSizeOf + Debug + Sync + Send + Copy + ToPrimitive + std::iter::Step + std::hash::Hash + 'static
{
}
impl<T> IndexIdToParentData for T
where
    T: Integer + Clone + NumCast + mayda::utility::Bits + HeapSizeOf + Debug + Sync + Send + Copy + ToPrimitive + std::iter::Step + std::hash::Hash + 'static,
{
}

pub trait TokenToAnchorScore: Debug + HeapSizeOf + Sync + Send + type_info::TypeInfo {
    fn get_scores(&self, id: u32) -> Option<Vec<AnchorScore>>;
    fn get_max_id(&self) -> usize;
}

pub trait IndexIdToParent: Debug + HeapSizeOf + Sync + Send + type_info::TypeInfo {
    type Output: IndexIdToParentData;

    fn get_values(&self, id: u64) -> Option<Vec<Self::Output>>;

    #[inline]
    fn append_values(&self, id: u64, vec: &mut Vec<Self::Output>) {
        if let Some(vals) = self.get_values(id) {
            vec.reserve(vals.len());
            for id in vals {
                vec.push(id);
            }
        }
    }

    #[inline]
    fn append_values_for_ids(&self, ids: &[u32], vec: &mut Vec<Self::Output>) {
        for id in ids {
            if let Some(vals) = self.get_values(*id as u64) {
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
            if let Some(vals) = self.get_values(*id as u64) {
                // vec.reserve(vals.len());
                for id in vals {
                    let stat = hits.entry(id).or_insert(0);
                    *stat += 1;
                }
            }
        }
        hits
    }

    #[inline]
    fn get_count_for_id(&self, id: u64) -> Option<usize> {
        self.get_values(id).map(|el| el.len())
    }

    #[inline]
    fn get_mutliple_value(&self, range: std::ops::RangeInclusive<usize>) -> Option<Vec<Self::Output>> {
        let mut dat = Vec::with_capacity(range.size_hint().0);
        for i in range {
            dat.push(self.get_value(i as u64).unwrap())
        }
        Some(dat)
    }

    #[inline]
    fn get_value(&self, id: u64) -> Option<Self::Output> {
        self.get_values(id).map(|el| el[0])
    }

    //last needs to be the largest value_id
    fn get_keys(&self) -> Vec<Self::Output>;

    #[inline]
    fn is_1_to_n(&self) -> bool {
        let keys = self.get_keys();
        keys.iter()
            .any(|key| self.get_values(NumCast::from(*key).unwrap()).map(|values| values.len() > 1).unwrap_or(false))
    }
}

pub fn trace_index_id_to_parent<T: IndexIdToParentData>(val: &Box<IndexIdToParent<Output = T>>) {
    if log_enabled!(log::Level::Trace) {
        let keys = val.get_keys();
        for key in keys.iter().take(100) {
            if let Some(vals) = val.get_values(NumCast::from(*key).unwrap()) {
                let to = std::cmp::min(vals.len(), 100);
                trace!("key {:?} to {:?}", key, &vals[0..to]);
            }
        }
    }
}

pub fn get_readable_size(value: usize) -> ColoredString {
    match value {
        0...1_000 => format!("{:?} b", value).blue(),
        1_000...1_000_000 => format!("{:?} kb", value / 1_000).green(),
        _ => format!("{:?} mb", value / 1_000_000).red(),
    }
}

pub fn get_readable_size_for_childs<T: HeapSizeOf>(value: T) -> ColoredString {
    get_readable_size(value.heap_size_of_children())
}

impl Persistence {
    fn get_fst_sizes(&self) -> usize {
        self.indices.fst.iter().map(|(_, v)| v.as_fst().size()).sum()
    }

    pub fn get_all_properties(&self) -> Vec<String> {
        self.meta_data.fulltext_indices.keys().map(|el| util::extract_field_name(el)).collect()
    }

    pub fn print_heap_sizes(&self) {
        info!(
            "indices.index_64 {}",
            // get_readable_size_for_childs(&self.indices.index_64)
            get_readable_size(self.indices.index_64.heap_size_of_children())
        );
        info!(
            "indices.index_id_to_parento {}",
            get_readable_size(self.indices.index_id_to_parento.heap_size_of_children()) // get_readable_size_for_childs(&self.indices.index_id_to_parento)
        );
        info!(
            "indices.boost_valueid_to_value {}",
            get_readable_size_for_childs(&self.indices.boost_valueid_to_value)
        );
        info!(
            "indices.token_to_anchor_to_score {}",
            get_readable_size_for_childs(&self.indices.token_to_anchor_to_score)
        );
        info!("indices.fst {}", get_readable_size(self.get_fst_sizes()));
        info!("------");
        let total_size = self.get_fst_sizes() + self.indices.index_id_to_parento.heap_size_of_children() + self.indices.index_64.heap_size_of_children()
            + self.indices.boost_valueid_to_value.heap_size_of_children()
            + self.indices.token_to_anchor_to_score.heap_size_of_children();

        info!("totale size {}", get_readable_size(total_size));

        let mut print_and_size = vec![];
        for (k, v) in &self.indices.index_id_to_parento {
            print_and_size.push((v.heap_size_of_children(), v.type_name(), k));
        }
        for (k, v) in &self.indices.token_to_anchor_to_score {
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

        println!("{}", table);
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn load(db: String) -> Result<Self, search::SearchError> {
        let meta_data = MetaData::new(&db)?;
        let mut pers = Persistence {
            meta_data,
            db,
            lru_cache: HashMap::default(), // LruCache::new(50),
            // lru_fst: HashMap::default(), // LruCache::new(50),
            term_boost_cache: RwLock::new(LruCache::with_expiry_duration_and_capacity(Duration::new(3600, 0), 10)),
            indices: PersistenceIndices::default(),
        };
        pers.load_all_to_cache()?;
        pers.print_heap_sizes();
        Ok(pers)
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn create(db: String) -> Result<Self, io::Error> {
        fs::create_dir_all(&db)?;
        let meta_data = MetaData { ..Default::default() };
        Ok(Persistence {
            meta_data,
            db,
            lru_cache: HashMap::default(),
            // lru_fst: HashMap::default(),
            term_boost_cache: RwLock::new(LruCache::with_expiry_duration_and_capacity(Duration::new(3600, 0), 10)),
            indices: PersistenceIndices::default(),
        })
    }

    pub fn write_score_index_vint(&self, store: &TokenToAnchorScoreVint, path: &str, loading_type: LoadingType) -> Result<(KVStoreMetaData), io::Error> {
        let indirect_file_path = util::get_file_path(&self.db, &(path.to_string() + ".indirect"));
        let data_file_path = util::get_file_path(&self.db, &(path.to_string() + ".data"));

        store.write(&indirect_file_path, &data_file_path)?;

        Ok(KVStoreMetaData {
            loading_type: loading_type,
            persistence_type: KVStoreType::IndexIdToMultipleParentIndirect,
            is_1_to_n: false,
            path: path.to_string(),
            max_value_id: 0, //TODO ?
            avg_join_size: 0.0,
        })
    }

    pub fn write_indirect_index(&self, store: &IndexIdToMultipleParentIndirect<u32>, path: &str, loading_type: LoadingType) -> Result<(KVStoreMetaData), io::Error> {
        let max_value_id = *store.data.iter().max_by_key(|el| *el).unwrap_or(&0);
        let avg_join_size = calc_avg_join_size(store.num_values, store.num_ids);

        let indirect_file_path = util::get_file_path(&self.db, &(path.to_string() + ".indirect"));
        let data_file_path = util::get_file_path(&self.db, &(path.to_string() + ".data"));

        File::create(indirect_file_path)?.write_all(&vec_to_bytes_u32(&store.start_pos))?;
        File::create(data_file_path)?.write_all(&vec_to_bytes_u32(&store.data))?;

        Ok(KVStoreMetaData {
            loading_type: loading_type,
            persistence_type: KVStoreType::IndexIdToMultipleParentIndirect,
            is_1_to_n: store.is_1_to_n(),
            path: path.to_string(),
            max_value_id: max_value_id,
            avg_join_size: avg_join_size,
        })
    }

    pub fn write_direct_index(
        &self,
        data: &IndexIdToParent<Output = u32>,
        path: &str,
        max_value_id: u32,
        loading_type: LoadingType,
    ) -> Result<(KVStoreMetaData), io::Error> {
        let data_file_path = util::get_file_path(&self.db, &(path.to_string() + ".data_direct"));

        let store = IndexIdToOneParent::new(data);

        File::create(data_file_path)?.write_all(&vec_to_bytes_u32(&store.data))?;

        Ok(KVStoreMetaData {
            loading_type: loading_type,
            persistence_type: KVStoreType::IndexIdToOneParent,
            is_1_to_n: false,
            path: path.to_string(),
            max_value_id: max_value_id,
            avg_join_size: 1 as f32, //TODO FIXME CHECKO NULLOS, 1 is not exact enough
        })
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn write_tuple_pair(
        &self,
        tuples: &mut Vec<create::ValIdPair>,
        path: &str,
        is_always_1_to_1: bool,
        loading_type: LoadingType,
    ) -> Result<(KVStoreMetaData), io::Error> {
        let meta_data = self.write_tuple_pair_dedup(tuples, path, false, is_always_1_to_1, loading_type)?;
        Ok(meta_data)
    }

    pub fn write_tuple_pair_dedup(
        &self,
        tuples: &mut Vec<create::ValIdPair>,
        path: &str,
        sort_and_dedup: bool,
        is_always_1_to_1: bool,
        loading_type: LoadingType,
    ) -> Result<(KVStoreMetaData), io::Error> {
        info_time!("write_tuple_pair_dedup");
        let data = valid_pair_to_parallel_arrays::<u32>(tuples);

        if is_always_1_to_1 {
            let max_value_id = tuples.iter().max_by_key(|el| el.parent_val_id).map(|el| el.parent_val_id).unwrap_or(0);
            Ok(self.write_direct_index(&data, path, max_value_id, loading_type)?)
        } else {
            // self.create_write_indirect_index(&data, path, sort_and_dedup, loading_type)?;
            let store = IndexIdToMultipleParentIndirect::new_sort_and_dedup(&data, sort_and_dedup);
            Ok(self.write_indirect_index(&store, path, loading_type)?)
        }
        //Parallel
        // let encoded: Vec<u8> = serialize(&data, Infinite).unwrap();
        // File::create(util::get_file_path(&self.db, &path.to_string()))?.write_all(&encoded)?;
        
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn write_boost_tuple_pair(&self, tuples: &mut Vec<create::ValIdToValue>, path: &str) -> Result<(KVStoreMetaData), io::Error> {
        let data = boost_pair_to_parallel_arrays::<u32>(tuples);
        let encoded: Vec<u8> = serialize(&data).unwrap();
        let boost_path = path.to_string() + ".boost_valid_to_value";
        File::create(util::get_file_path(&self.db, &boost_path))?.write_all(&encoded)?;

        Ok(KVStoreMetaData {
            loading_type: LoadingType::Disk,
            persistence_type: KVStoreType::ParallelArrays,
            is_1_to_n: data.is_1_to_n(),
            path: boost_path.to_string(),
            max_value_id: tuples.iter().max_by_key(|el| el.value).unwrap().value,
            avg_join_size: 1.0, //FixMe? multiple boosts?
        })
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn write_offset<T: Clone + Integer + NumCast + Copy + Debug>(&self, bytes: &[u8], data: &[T], path: &str) -> Result<((String, IDList)), io::Error> {
        debug_time!(format!("Wrote Index {} With size {:?}", path, data.len()));
        File::create(util::get_file_path(&self.db, path))?.write_all(bytes)?;
        info!("Wrote Index {} With size {:?}", path, data.len());
        trace!("{:?}", data);
        let sizo = match mem::size_of::<T>() {
            4 => IDDataType::U32,
            8 => IDDataType::U64,
            _ => panic!("wrong sizeee"),
        };
        // self.meta_data.id_lists.insert(
        //     path.to_string(),
        //     IDList {
        //         path: path.to_string(),
        //         size: data.len() as u64,
        //         id_type: sizo,
        //         doc_id_type: check_is_docid_type(data),
        //     },
        // );
        Ok((path.to_string(),
            IDList {
                path: path.to_string(),
                size: data.len() as u64,
                id_type: sizo,
                doc_id_type: check_is_docid_type(data),
            }))
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
    pub fn write_data(&self, path: &str, data: &[u8]) -> Result<(), io::Error> {
        File::create(&get_file_path(&self.db, path))?.write_all(data)?;
        Ok(())
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_buffered_writer(&self, path: &str) -> Result<io::BufWriter<fs::File>, io::Error> {
        Ok(io::BufWriter::new(File::create(&get_file_path(&self.db, path))?))
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn write_json_to_disk<'a, T>(&mut self, data: StreamDeserializer<'a, T, Value>, path: &str) -> Result<(), io::Error>
    where
        T: serde_json::de::Read<'a>,
    {
        let mut offsets = vec![];
        let mut file_out = io::BufWriter::new(File::create(&get_file_path(&self.db, path))?);
        let mut current_offset = 0;
        // let arro = data.as_array().unwrap();

        // let mut encoder = snap::Encoder::new();
        // let mut dat = vec_to_bytes_u32(&vals);

        util::iter_json_stream(data, &mut |el: &serde_json::Value| {
            let el_str = el.to_string().into_bytes();

            // let mut compressed_doc = encoder.compress_vec(&el_str).unwrap();
            // compressed_doc.shrink_to_fit();
            // file_out.write_all(&compressed_doc).unwrap();

            file_out.write_all(&el_str).unwrap();
            offsets.push(current_offset as u64);
            current_offset += el_str.len();
            // current_offset += compressed_doc.len();
        });

        // for el in arro {
        //     let el_str = el.to_string().into_bytes();
        //     buffer.write_all(&el_str)?;
        //     offsets.push(current_offset as u64);
        //     current_offset += el_str.len();
        // }
        offsets.push(current_offset as u64);
        // println!("json offsets: {:?}", offsets);
        let (id_list_path, id_list) = self.write_offset(&vec_to_bytes_u64(&offsets), &offsets, &(path.to_string() + ".offsets"))?;
        self.meta_data.id_lists.insert(id_list_path, id_list);
        Ok(())
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_offsets(&self, path: &str) -> Result<&Box<IndexIdToParent<Output = u64>>, search::SearchError> {
        // Option<&IndexIdToParent<Output=u64>>
        self.indices
            .index_64
            .get(&(path.to_string() + ".offsets"))
            .ok_or_else(|| From::from(format!("Did not found path in indices {:?}", path)))
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_valueid_to_parent(&self, path: &str) -> Result<&Box<IndexIdToParent<Output = u32>>, search::SearchError> {
        self.indices.index_id_to_parento.get(path).ok_or_else(|| {
            let error = format!("Did not found path in indices {:?}", path);
            println!("{:?}", error);
            From::from(error)
        })
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_token_to_anchor(&self, path: &str) -> Result<&Box<TokenToAnchorScore>, search::SearchError> {
        let path = path.to_string() + ".to_anchor_id_score";
        self.indices.token_to_anchor_to_score.get(&path).ok_or_else(|| {
            let error = format!("Did not found path in indices {}", path);
            println!("{:?}", error);
            From::from(error)
        })
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn has_index(&self, path: &str) -> bool {
        self.indices.index_id_to_parento.contains_key(path)
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_boost(&self, path: &str) -> Result<&Box<IndexIdToParent<Output = u32>>, search::SearchError> {
        self.indices
            .boost_valueid_to_value
            .get(path)
            .ok_or_else(|| From::from(format!("Did not found path in indices {:?}", path)))
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_file_search(&self, path: &str) -> FileSearch {
        FileSearch::new(path, self.get_file_handle(path).unwrap())
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_file_handle_complete_path(&self, path: &str) -> Result<File, search::SearchError> {
        Ok(File::open(path).map_err(|err| search::SearchError::StringError(format!("Could not open {} {:?}", path, err)))?)
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_file_metadata_handle_complete_path(&self, path: &str) -> Result<fs::Metadata, io::Error> {
        Ok(fs::metadata(path)?)
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_file_handle(&self, path: &str) -> Result<File, search::SearchError> {
        Ok(File::open(PathBuf::from(get_file_path(&self.db, path)))
            .map_err(|err| search::SearchError::StringError(format!("Could not open {} {:?}", path, err)))?)
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_file_metadata_handle(&self, path: &str) -> Result<fs::Metadata, io::Error> {
        Ok(fs::metadata(PathBuf::from(&get_file_path(&self.db, path)))?)
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn load_fst(&self, path: &str) -> Result<Map, search::SearchError> {
        unsafe {
            Ok(Map::from_path(&get_file_path(&self.db, &(path.to_string() + ".fst")))?) //(path.to_string() + ".fst"))?)
        }
        //In memory version
        // let mut f = self.get_file_handle(&(path.to_string() + ".fst"))?;
        // let mut buffer: Vec<u8> = Vec::new();
        // f.read_to_end(&mut buffer)?;
        // buffer.shrink_to_fit();
        // Ok(Map::from_bytes(buffer)?)
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn get_fst(&self, path: &str) -> Result<&Map, search::SearchError> {
        self.indices.fst.get(path).ok_or_else(|| From::from(format!("{} does not exist", path)))
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn load_all_to_cache(&mut self) -> Result<(), search::SearchError> {
        info_time!(format!("loaded persistence {:?}", &self.db));
        for (_, ref idlist) in &self.meta_data.id_lists.clone() {
            match &idlist.id_type {
                &IDDataType::U32 => {}
                &IDDataType::U64 => self.load_index_64(&idlist.path)?,
            }
        }

        for el in &self.meta_data.key_value_stores {
            self.lru_cache.insert(el.path.clone(), LruCache::with_capacity(0));
        }

        //ANCHOR TO SCORE
        for el in &self.meta_data.anchor_score_stores {
            let loading_type = get_loading_type(el.loading_type.clone())?;

            let indirect_path = get_file_path(&self.db, &el.path) + ".indirect";
            let indirect_data_path = get_file_path(&self.db, &el.path) + ".data";
            let start_and_end_file = File::open(&indirect_path).unwrap();
            let data_file = File::open(&indirect_data_path).unwrap();
            match loading_type {
                // LoadingType::Disk => {
                //     let store = TokenToAnchorScoreMmap::new(&start_and_end_file, &data_file);
                //     self.indices.token_to_anchor_to_score.insert(el.path.to_string(), Box::new(store));
                // }
                // LoadingType::InMemoryUnCompressed | LoadingType::InMemory => {
                //     let mut store = TokenToAnchorScoreBinary::default();
                //     store.read(&indirect_path, &indirect_data_path).unwrap();
                //     self.indices.token_to_anchor_to_score.insert(el.path.to_string(), Box::new(store));
                // }
                LoadingType::Disk => {
                    let store = Box::new(TokenToAnchorScoreVintMmap::new(&start_and_end_file, &data_file));
                    self.indices.token_to_anchor_to_score.insert(el.path.to_string(), store);
                }
                LoadingType::InMemoryUnCompressed | LoadingType::InMemory => {
                    let mut store = TokenToAnchorScoreVint::default();
                    store.read(&indirect_path, &indirect_data_path).unwrap();
                    self.indices.token_to_anchor_to_score.insert(el.path.to_string(), Box::new(store));
                }
            }

            // token_to_anchor_to_score
        }

        let loaded_data: Result<Vec<(String, Box<IndexIdToParent<Output = u32>>)>, SearchError> = self.meta_data
            .key_value_stores
            .clone()
            .into_par_iter()
            .map(|el| {
                // info!("loading key_value_store {:?}", &el.path);
                info_time!(format!("loaded key_value_store {:?}", &el.path));

                let loading_type = get_loading_type(el.loading_type.clone())?;

                let indirect_path = get_file_path(&self.db, &el.path) + ".indirect";
                let indirect_data_path = get_file_path(&self.db, &el.path) + ".data";
                let data_direct_path = get_file_path(&self.db, &el.path) + ".data_direct";

                match loading_type {
                    LoadingType::InMemoryUnCompressed => match el.persistence_type {
                        KVStoreType::IndexIdToMultipleParentIndirect => {
                            let indirect_u32 = bytes_to_vec_u32(&file_to_bytes(&indirect_path)?);
                            let data_u32 = bytes_to_vec_u32(&file_to_bytes(&indirect_data_path)?);

                            let store = IndexIdToMultipleParentIndirect {
                                start_pos: indirect_u32,
                                data: data_u32,
                                max_value_id: el.max_value_id,
                                avg_join_size: el.avg_join_size,
                                num_values: 0,
                                num_ids: 0,
                            };

                            return Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>>));
                        }
                        KVStoreType::ParallelArrays => panic!("WAAAAAAA PAAAANIIC"),
                        KVStoreType::IndexIdToOneParent => {
                            let store = IndexIdToOneParent {
                                data: bytes_to_vec_u32(&file_to_bytes(&data_direct_path)?),
                                max_value_id: el.max_value_id,
                            };

                            return Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>>));
                        }
                    },
                    LoadingType::InMemory => match el.persistence_type {
                        KVStoreType::IndexIdToMultipleParentIndirect => {
                            let indirect_u32 = bytes_to_vec_u32(&file_to_bytes(&indirect_path)?);
                            let data_u32 = bytes_to_vec_u32(&file_to_bytes(&indirect_data_path)?);

                            let store = IndexIdToMultipleParentIndirect {
                                start_pos: indirect_u32,
                                data: data_u32,
                                max_value_id: el.max_value_id,
                                avg_join_size: el.avg_join_size,
                                num_values: 0,
                                num_ids: 0,
                            };

                            return Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>>));
                        }
                        KVStoreType::ParallelArrays => panic!("WAAAAAAA"),
                        KVStoreType::IndexIdToOneParent => {
                            let data_u32 = bytes_to_vec_u32(&file_to_bytes(&data_direct_path)?);

                            let store = IndexIdToOneParentMayda::from_vec(&data_u32, el.max_value_id);

                            return Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>>));
                        }
                    },
                    LoadingType::Disk => {
                        match el.persistence_type {
                            KVStoreType::IndexIdToMultipleParentIndirect => {
                                let start_and_end_file = self.get_file_handle_complete_path(&indirect_path)?;
                                let data_file = self.get_file_handle_complete_path(&indirect_data_path)?;
                                let indirect_metadata = self.get_file_metadata_handle_complete_path(&indirect_path)?;
                                let data_metadata = self.get_file_metadata_handle(&(el.path.to_string() + ".data"))?;
                                let store = PointingMMAPFileReader::new(
                                    &start_and_end_file,
                                    &data_file,
                                    indirect_metadata,
                                    &data_metadata,
                                    el.max_value_id,
                                    el.avg_join_size,
                                );
                                // let store = PointingArrayFileReader::new(
                                //     start_and_end_file,
                                //     data_file,
                                //     indirect_metadata,
                                //     // data_metadata,
                                //     el.max_value_id,
                                //     el.avg_join_size,
                                // );

                                // let store = PointingArrayFileReader { start_and_end_file: el.path.to_string()+ ".indirect", data_file: el.path.to_string()+ ".data", persistence: self.db.to_string()};
                                // self.indices
                                //     .index_id_to_parento
                                //     .insert(el.path.to_string(), Box::new(store));

                                return Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>>));
                            }
                            KVStoreType::ParallelArrays => panic!("WAAAAAAA"),
                            KVStoreType::IndexIdToOneParent => {
                                let data_file = self.get_file_handle_complete_path(&data_direct_path)?;
                                let data_metadata = self.get_file_metadata_handle_complete_path(&data_direct_path)?;
                                let store = SingleArrayMMAP::<u32>::new(data_file, data_metadata, el.max_value_id);

                                return Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>>));
                            }
                        }
                    }
                }
            })
            .collect();

        match loaded_data {
            Err(e) => return Err(e),
            Ok(dat) => for el in dat {
                self.indices.index_id_to_parento.insert(el.0, el.1);
            },
        };

        // Load Boost Indices
        for el in &self.meta_data.boost_stores {
            let encoded = file_to_bytes(&get_file_path(&self.db, &el.path))?;
            let store: ParallelArrays<u32> = deserialize(&encoded[..]).unwrap();
            self.indices
                .boost_valueid_to_value
                .insert(el.path.to_string(), Box::new(IndexIdToOneParentMayda::<u32>::new(&store, u32::MAX))); // TODO: enable other Diskbased Types
        }

        // Load FST
        for (ref path, _) in &self.meta_data.fulltext_indices {
            let map = self.load_fst(path)?;
            self.indices.fst.insert(path.to_string(), map);
        }
        Ok(())
    }

    #[cfg_attr(feature = "flame_it", flame)]
    pub fn load_index_64(&mut self, path: &str) -> Result<(), search::SearchError> {
        let loading_type = load_type_from_env()?.unwrap_or(LoadingType::Disk);

        match loading_type {
            LoadingType::InMemoryUnCompressed => {
                let file_path = get_file_path(&self.db, path);
                self.indices.index_64.insert(
                    path.to_string(),
                    Box::new(IndexIdToOneParent {
                        data: load_index_u64(&file_path)?,
                        max_value_id: u32::MAX,
                    }),
                );
            }
            LoadingType::InMemory => {
                let file_path = get_file_path(&self.db, path);
                self.indices.index_64.insert(
                    path.to_string(),
                    Box::new(IndexIdToOneParentMayda::from_vec(&load_index_u64(&file_path)?, u32::MAX)),
                );
            }
            LoadingType::Disk => {
                let data_file = self.get_file_handle(path)?;
                let data_metadata = self.get_file_metadata_handle(path)?;

                self.indices
                    .index_64
                    .insert(path.to_string(), Box::new(SingleArrayFileReader::<u64>::new(data_file, data_metadata)));
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct FileSearch {
    path: String,
    // offsets: Vec<u64>,
    file: File,
    buffer: Vec<u8>,
}

impl FileSearch {
    fn new(path: &str, file: File) -> Self {
        // load_index_64_into_cache(&(path.to_string()+".offsets")).unwrap();
        FileSearch {
            path: path.to_string(),
            file: file,
            buffer: Vec::with_capacity(50 as usize),
        }
    }

    pub fn get_text_for_id<'a>(&mut self, pos: usize, offsets: &IndexIdToParent<Output = u64>) -> String {
        self.load_text(pos as u64, offsets);
        str::from_utf8(&self.buffer).unwrap().to_string() // TODO maybe avoid clone
    }

    fn load_text<'a>(&mut self, pos: u64, offsets: &IndexIdToParent<Output = u64>) {
        // @Temporary Use Result
        let string_size = offsets.get_value(pos + 1).unwrap() - offsets.get_value(pos).unwrap() - 1;
        // let mut buffer:Vec<u8> = Vec::with_capacity(string_size as usize);
        // unsafe { buffer.set_len(string_size as usize); }
        self.buffer.resize(string_size as usize, 0);
        self.file.seek(SeekFrom::Start(offsets.get_value(pos).unwrap())).unwrap();
        self.file.read_exact(&mut self.buffer).unwrap();
        // unsafe {str::from_utf8_unchecked(&buffer)}
        // let s = unsafe {str::from_utf8_unchecked(&buffer)};
        // str::from_utf8(&buffer).unwrap() // @Temporary  -> use unchecked if stable
    }

    // pub fn binary_search(&mut self, term: &str, persistence: &Persistence) -> Result<(String, i64), io::Error> {
    //     // let cache_lock = INDEX_64_CACHE.read().unwrap();
    //     // let offsets = cache_lock.get(&(self.path.to_string()+".offsets")).unwrap();
    //     let offsets = persistence.indices.index_64.get(&(self.path.to_string() + ".offsets")).unwrap();
    //     debug_time!("term binary_search");
    //     if offsets.len() < 2 {
    //         return Ok(("".to_string(), -1));
    //     }
    //     let mut low = 0;
    //     let mut high = offsets.len() - 2;
    //     let mut i;
    //     while low <= high {
    //         i = (low + high) >> 1;
    //         self.load_text(i, offsets);
    //         // info!("Comparing {:?}", str::from_utf8(&buffer).unwrap());
    //         // comparison = comparator(arr[i], find);
    //         if str::from_utf8(&self.buffer).unwrap() < term {
    //             low = i + 1;
    //             continue;
    //         }
    //         if str::from_utf8(&self.buffer).unwrap() > term {
    //             high = i - 1;
    //             continue;
    //         }
    //         return Ok((str::from_utf8(&self.buffer).unwrap().to_string(), i as i64));
    //     }
    //     Ok(("".to_string(), -1))
    // }
}

fn load_type_from_env() -> Result<Option<LoadingType>, search::SearchError> {
    if let Some(val) = env::var_os("LoadingType") {
        let conv_env = val.clone()
            .into_string()
            .map_err(|_err| search::SearchError::StringError(format!("Could not convert LoadingType environment variable to utf-8: {:?}", val)))?;
        let loading_type = LoadingType::from_str(&conv_env).map_err(|_err| {
            search::SearchError::StringError("only InMemoryUnCompressed, InMemory or Disk allowed for LoadingType environment variable".to_string())
        })?;
        Ok(Some(loading_type))
    } else {
        Ok(None)
    }
}

fn get_loading_type(loading_type: LoadingType) -> Result<LoadingType, search::SearchError> {
    let mut loading_type = loading_type.clone();
    if let Some(val) = load_type_from_env()? {
        // Overrule Loadingtype from env
        loading_type = val;
    }
    Ok(loading_type)
}

pub fn vec_to_bytes_u32(data: &[u32]) -> Vec<u8> {
    let mut wtr: Vec<u8> = vec_with_size_uninitialized(data.len() * std::mem::size_of::<u32>());
    LittleEndian::write_u32_into(data, &mut wtr);
    wtr
}
pub fn vec_to_bytes_u64(data: &[u64]) -> Vec<u8> {
    let mut wtr: Vec<u8> = vec_with_size_uninitialized(data.len() * std::mem::size_of::<u64>());
    LittleEndian::write_u64_into(data, &mut wtr);
    wtr
}

pub fn bytes_to_vec_u32(data: &[u8]) -> Vec<u32> {
    bytes_to_vec::<u32>(&data)
}
pub fn bytes_to_vec_u64(data: &[u8]) -> Vec<u64> {
    bytes_to_vec::<u64>(&data)
}
pub fn bytes_to_vec<T>(data: &[u8]) -> Vec<T> {
    let mut out_dat = vec_with_size_uninitialized(data.len() / std::mem::size_of::<T>());
    // LittleEndian::read_u64_into(&data, &mut out_dat);
    unsafe {
        let ptr = std::mem::transmute::<*const u8, *const T>(data.as_ptr());
        ptr.copy_to_nonoverlapping(out_dat.as_mut_ptr(), data.len() / std::mem::size_of::<T>());
    }
    out_dat
}

pub fn file_to_bytes<P: AsRef<Path>>(s1: P) -> Result<Vec<u8>, io::Error> {
    let f = File::open(s1)?;
    file_handle_to_bytes(&f)
}

pub fn file_handle_to_bytes(f: &File) -> Result<Vec<u8>, io::Error> {
    let file_size = { f.metadata()?.len() as usize };
    let mut reader = std::io::BufReader::new(f);
    let mut buffer: Vec<u8> = Vec::with_capacity(file_size);
    reader.read_to_end(&mut buffer)?;
    // buffer.shrink_to_fit();
    Ok(buffer)
}

pub fn load_index_u32<P: AsRef<Path> + std::fmt::Debug>(s1: P) -> Result<Vec<u32>, io::Error> {
    info!("Loading Index32 {:?} ", s1);
    Ok(bytes_to_vec_u32(&file_to_bytes(s1)?))
}

pub fn load_index_u64<P: AsRef<Path> + std::fmt::Debug>(s1: P) -> Result<Vec<u64>, io::Error> {
    info!("Loading Index64 {:?} ", s1);
    Ok(bytes_to_vec_u64(&file_to_bytes(s1)?))
}

fn check_is_docid_type<T: Integer + NumCast + Copy>(data: &[T]) -> bool {
    for (index, value_id) in data.iter().enumerate() {
        let blub: usize = num::cast(*value_id).unwrap();
        if blub != index {
            return false;
        }
    }
    true
}
