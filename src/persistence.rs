use std::fs::{self, File};
use std::io::prelude::*;
use std::io::{self, Cursor, SeekFrom};
use std::str;
use std::collections::HashMap;
use std::fmt::Debug;
use std::mem;
use std::marker::Sync;
use std;
use util;
use util::get_file_path;
use std::env;

use num::{self, Integer, NumCast};
use num::cast::ToPrimitive;

use serde_json;
use serde_json::Value;

use fnv::FnvHashMap;
use bincode::{deserialize, serialize, Infinite};

use create;
#[allow(unused_imports)]
use mayda;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log;

#[allow(unused_imports)]
use rayon::prelude::*;
#[allow(unused_imports)]
use fst::{IntoStreamer, Map, MapBuilder, Set};

use prettytable::Table;
use prettytable::format;

use persistence_data::*;

#[allow(unused_imports)]
use search::{self, SearchError};

#[allow(unused_imports)]
use heapsize::{heap_size_of, HeapSizeOf};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct MetaData {
    pub id_lists: FnvHashMap<String, IDList>,
    pub key_value_stores: Vec<KVStoreMetaData>,
    pub boost_stores: Vec<KVStoreMetaData>,
    pub fulltext_indices: FnvHashMap<String, create::FulltextIndexOptions>,
}

impl MetaData {
    pub fn new(folder: &str) -> MetaData {
        let json = util::file_as_string(&(folder.to_string() + "/metaData.json")).unwrap();
        serde_json::from_str(&json).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct KVStoreMetaData {
    pub path: String,
    pub is_1_to_n: bool, // In the sense of 1:n   1key, n values
    pub persistence_type: KVStoreType,
    pub loading_type: LoadingType,
    #[serde(default = "default_max_value_id")]
    pub max_value_id: u32, // max value on the "right" side key -> value, key -> value ..
    #[serde(default = "default_avg_join")]
    pub avg_join_size: u32, // some join statistics
}

//TODO Only tmp
fn default_max_value_id() -> u32 {
    std::u32::MAX
}
fn default_avg_join() -> u32 {
    1000
}

// impl KVStoreMetaData {
//     fn new(valid_path: &str, parentid_path: &str, is_1_to_n: bool, persistence_type: KVStoreType, loading_type: LoadingType) -> KVStoreMetaData {
//         KVStoreMetaData{persistence_type:KVStoreType::ParallelArrays, is_1_to_n:has_duplicates, valid_path: valid_path.clone(), parentid_path:parentid_path.clone()}
//     }
// }

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum LoadingType {
    InMemory,
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
use persistence_data;

pub trait IndexIdToParentData: Integer + Clone + NumCast + mayda::utility::Bits + HeapSizeOf + Debug + Sync + Send + Copy + ToPrimitive + std::iter::Step + std::hash::Hash {}
impl<T> IndexIdToParentData for T
where
    T: Integer + Clone + NumCast + mayda::utility::Bits + HeapSizeOf + Debug + Sync + Send + Copy + ToPrimitive + std::iter::Step + std::hash::Hash,
{
}

pub trait IndexIdToParent: Debug + HeapSizeOf + Sync + Send + persistence_data::TypeInfo {
    type Output: IndexIdToParentData;

    #[inline]
    fn get_values(&self, id: u64) -> Option<Vec<Self::Output>>;

    #[inline]
    fn append_values(&self, id: u64, vec: &mut Vec<Self::Output>){
        if let Some(vals) = self.get_values(id) {
            vec.reserve(vals.len());
            for id in vals {
                vec.push(id);
            }
        }
    }

    #[inline]
    fn append_values_for_ids(&self, ids: &[u32], vec: &mut Vec<Self::Output>){
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
    fn count_values_for_ids(&self, ids: &[u32], top:Option<u32>) -> FnvHashMap<Self::Output, usize> {
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
    // fn get_mutliple_values(&self, range: std::ops::RangeInclusive<usize>) -> Vec<Option<Vec<Self::Output>>> {
    //     let mut dat = Vec::with_capacity(range.size_hint().0);
    //     for i in range {
    //         // dat.extend(self.get_values(i as u64).unwrap());
    //         dat.push(self.get_values(i as u64))
    //     }
    //     dat
    // }

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
        keys.iter().any(|key| {
            self.get_values(NumCast::from(*key).unwrap())
                .map(|values| values.len() > 1)
                .unwrap_or(false)
        })
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

use std::u32;
pub static NOT_FOUND: u32 = u32::MAX;

#[derive(Debug, Default)]
pub struct PersistenceCache {
    // pub index_id_to_parent: HashMap<(String,String), Vec<Vec<u32>>>,
    pub index_id_to_parento: HashMap<String, Box<IndexIdToParent<Output = u32>>>,
    pub boost_valueid_to_value: HashMap<String, Box<IndexIdToParent<Output = u32>>>,
    index_64: HashMap<String, Box<IndexIdToParent<Output = u64>>>,
    // index_32: HashMap<String, Vec<u32>>,
    pub fst: HashMap<String, Map>,
}

#[derive(Debug, Default)]
pub struct Persistence {
    pub db: String, // folder
    pub meta_data: MetaData,
    pub cache: PersistenceCache,
}

use colored::*;

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
        self.cache.fst.iter().map(|(_, v)| v.as_fst().size()).sum()
    }

    pub fn print_heap_sizes(&self) {
        info!(
            "cache.index_64 {}",
            get_readable_size_for_childs(&self.cache.index_64)
        );
        info!(
            "cache.index_id_to_parento {}",
            get_readable_size_for_childs(&self.cache.index_id_to_parento)
        );
        info!(
            "cache.boost_valueid_to_value {}",
            get_readable_size_for_childs(&self.cache.boost_valueid_to_value)
        );
        info!("cache.fst {}", get_readable_size(self.get_fst_sizes()));
        info!("------");
        let total_size = self.get_fst_sizes() + self.cache.index_id_to_parento.heap_size_of_children() + self.cache.index_64.heap_size_of_children()
            + self.cache.boost_valueid_to_value.heap_size_of_children();

        info!("totale size {}", get_readable_size(total_size));

        let mut print_and_size = vec![];
        for (k, v) in &self.cache.index_id_to_parento {
            print_and_size.push((v.heap_size_of_children(), v.type_name(), k));
        }
        for (k, v) in &self.cache.index_64 {
            print_and_size.push((v.heap_size_of_children(), v.type_name(), k));
        }
        for (k, v) in &self.cache.fst {
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

    #[flame]
    pub fn load(db: String) -> Result<Self, search::SearchError> {
        let meta_data = MetaData::new(&db);
        let mut pers = Persistence {
            meta_data,
            db,
            ..Default::default()
        };
        pers.load_all_to_cache()?;
        pers.print_heap_sizes();
        Ok(pers)
    }

    #[flame]
    pub fn create(db: String) -> Result<Self, io::Error> {
        fs::create_dir_all(&db)?;
        let meta_data = MetaData {
            ..Default::default()
        };
        Ok(Persistence {
            meta_data,
            db,
            ..Default::default()
        })
    }

    pub fn write_indirect_index(&mut self, data: &IndexIdToParent<Output = u32>, path: &str, sort_and_dedup: bool, max_value_id:u32) -> Result<(), io::Error> {
        let indirect_file_path = util::get_file_path(&self.db, &(path.to_string() + ".indirect"));
        let data_file_path = util::get_file_path(&self.db, &(path.to_string() + ".data"));

        let store = IndexIdToMultipleParentIndirect::new_sort_and_dedup(data, sort_and_dedup);

        let avg_join_size = if store.start_and_end.len() == 0 {
            0
        }else{
            store.data.len()/store.start_and_end.len()/2 //Attention, this works only of there is no compression of any kind
        };

        File::create(indirect_file_path)?.write_all(&vec_to_bytes_u32(&store.start_and_end))?;
        File::create(data_file_path)?.write_all(&vec_to_bytes_u32(&store.data))?;
        self.meta_data.key_value_stores.push(KVStoreMetaData {
            loading_type: LoadingType::InMemory,
            persistence_type: KVStoreType::IndexIdToMultipleParentIndirect,
            is_1_to_n: store.is_1_to_n(),
            path: path.to_string(),
            max_value_id: max_value_id,
            avg_join_size: avg_join_size as u32,
        });

        Ok(())
    }

    #[flame]
    pub fn write_tuple_pair(&mut self, tuples: &mut Vec<create::ValIdPair>, path: &str) -> Result<(), io::Error> {
        self.write_tuple_pair_dedup(tuples, path, false)?;
        Ok(())
    }

    pub fn write_tuple_pair_dedup(&mut self, tuples: &mut Vec<create::ValIdPair>, path: &str, sort_and_dedup: bool) -> Result<(), io::Error> {
        let data = valid_pair_to_parallel_arrays::<u32>(tuples);
        let max_value_id = tuples.iter().max_by_key(|el| el.parent_val_id).map(|el| el.parent_val_id).unwrap_or(0);
        self.write_indirect_index(&data, path, sort_and_dedup, max_value_id)?;
        //Parallel
        // let encoded: Vec<u8> = serialize(&data, Infinite).unwrap();
        // File::create(util::get_file_path(&self.db, &path.to_string()))?.write_all(&encoded)?;

        Ok(())
    }
    #[flame]
    pub fn write_boost_tuple_pair(&mut self, tuples: &mut Vec<create::ValIdToValue>, path: &str) -> Result<(), io::Error> {
        // let boost_paths = util::boost_path(path);
        // let has_duplicates = has_valid_duplicates(&tuples.iter().map(|el| el as &create::GetValueId).collect());
        let data = boost_pair_to_parallel_arrays::<u32>(tuples);
        // let data = parrallel_arrays_to_pointing_array(data.values1, data.values2);
        let encoded: Vec<u8> = serialize(&data, Infinite).unwrap();
        let boost_path = path.to_string() + ".boost_valid_to_value";
        File::create(util::get_file_path(&self.db, &boost_path))?.write_all(&encoded)?;

        self.meta_data.boost_stores.push(KVStoreMetaData {
            loading_type: LoadingType::InMemory,
            persistence_type: KVStoreType::ParallelArrays,
            is_1_to_n: data.is_1_to_n(),
            path: boost_path.to_string(),
            max_value_id: tuples.iter().max_by_key(|el| el.value).unwrap().value,
            avg_join_size: 1 //FixMe? multiple boosts?
        });
        Ok(())
    }

    #[flame]
    pub fn write_index<T: Clone + Integer + NumCast + Copy + Debug>(&mut self, bytes: &Vec<u8>, data: &Vec<T>, path: &str) -> Result<(), io::Error> {
        info_time!(format!("Wrote Index {} With size {:?}", path, data.len()));
        File::create(util::get_file_path(&self.db, path))?.write_all(&bytes)?;
        info!("Wrote Index {} With size {:?}", path, data.len());
        trace!("{:?}", data);
        let sizo = match mem::size_of::<T>() {
            4 => IDDataType::U32,
            8 => IDDataType::U64,
            _ => panic!("wrong sizeee"),
        };
        self.meta_data.id_lists.insert(
            path.to_string(),
            IDList {
                path: path.to_string(),
                size: data.len() as u64,
                id_type: sizo,
                doc_id_type: check_is_docid_type(&data),
            },
        );
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
    //     println!("test_build_fst ms: {}", (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));
    //     Ok(())
    // }

    #[flame]
    pub fn write_meta_data(&self) -> Result<(), io::Error> {
        self.write_data(
            "metaData.json",
            serde_json::to_string_pretty(&self.meta_data)?.as_bytes(),
        )
    }

    #[flame]
    pub fn write_data(&self, path: &str, data: &[u8]) -> Result<(), io::Error> {
        File::create(&get_file_path(&self.db, path))?.write_all(data)?;
        Ok(())
    }

    #[flame]
    pub fn get_buffered_writer(&self, path: &str) -> Result<io::BufWriter<fs::File>, io::Error> {
        Ok(io::BufWriter::new(File::create(&get_file_path(
            &self.db,
            path,
        ))?))
    }

    #[flame]
    pub fn write_json_to_disk(&mut self, arro: &Vec<Value>, path: &str) -> Result<(), io::Error> {
        let mut offsets = vec![];
        let mut buffer = File::create(&get_file_path(&self.db, &path))?;
        let mut current_offset = 0;
        // let arro = data.as_array().unwrap();
        for el in arro {
            let el_str = el.to_string().into_bytes();
            buffer.write_all(&el_str)?;
            offsets.push(current_offset as u64);
            current_offset += el_str.len();
        }
        offsets.push(current_offset as u64);
        // println!("json offsets: {:?}", offsets);
        self.write_index(
            &vec_to_bytes_u64(&offsets),
            &offsets,
            &(path.to_string() + ".offsets"),
        )?;
        Ok(())
    }

    #[flame]
    pub fn get_offsets(&self, path: &str) -> Result<&Box<IndexIdToParent<Output = u64>>, search::SearchError> {
        // Option<&IndexIdToParent<Output=u64>>
        self.cache
            .index_64
            .get(&(path.to_string() + ".offsets"))
            .ok_or_else(|| From::from(format!("Did not found path in cache {:?}", path)))
    }

    #[flame]
    pub fn get_valueid_to_parent(&self, path: &str) -> Result<&Box<IndexIdToParent<Output = u32>>, search::SearchError> {
        self.cache
            .index_id_to_parento
            .get(path)
            .ok_or_else(|| From::from(format!("Did not found path in cache {:?}", path)))
    }

    #[flame]
    pub fn has_facet_index(&self, path: &str) -> bool {
        self.cache
            .index_id_to_parento
            .contains_key(path)
    }

    #[flame]
    pub fn get_boost(&self, path: &str) -> Result<&Box<IndexIdToParent<Output = u32>>, search::SearchError> {
        self.cache
            .boost_valueid_to_value
            .get(path)
            .ok_or_else(|| From::from(format!("Did not found path in cache {:?}", path)))
    }

    #[flame]
    pub fn get_file_search(&self, path: &str) -> FileSearch {
        FileSearch::new(path, self.get_file_handle(path).unwrap())
    }

    #[flame]
    pub fn get_file_handle(&self, path: &str) -> Result<File, search::SearchError> {
        Ok(File::open(&get_file_path(&self.db, path)).map_err(|_err| search::SearchError::StringError(format!("Could not open {:?}", path)))?)
    }

    #[flame]
    pub fn get_file_metadata_handle(&self, path: &str) -> Result<fs::Metadata, io::Error> {
        Ok(fs::metadata(&get_file_path(&self.db, path))?)
    }

    #[flame]
    pub fn load_fst(&self, path: &str) -> Result<Map, search::SearchError> {
        let mut f = self.get_file_handle(&(path.to_string() + ".fst"))?;
        let mut buffer: Vec<u8> = Vec::new();
        f.read_to_end(&mut buffer)?;
        buffer.shrink_to_fit();
        Ok(Map::from_bytes(buffer)?)
    }

    #[flame]
    pub fn get_fst(&self, path: &str) -> Result<&Map, search::SearchError> {
        self.cache
            .fst
            .get(path)
            .ok_or(From::from(format!("{} does not exist", path)))
    }

    // pub fn get_create_char_offset_info(&self, path: &str,character: &str) -> Result<Option<OffsetInfo>, search::SearchError> { // @Temporary - replace SearchError
    //     let char_offset = CharOffset::new(path)?;
    //     return Ok(char_offset.get_char_offset_info(character, &self.cache.index_64).ok());
    // }

    #[flame]
    pub fn load_all_to_cache(&mut self) -> Result<(), search::SearchError> {
        for (_, ref idlist) in &self.meta_data.id_lists.clone() {
            match &idlist.id_type {
                &IDDataType::U32 => {}
                &IDDataType::U64 => self.load_index_64(&idlist.path)?,
            }
        }

        // let r: Result<Vec<_>, SearchError> = steps
        //         .into_par_iter()
        //         .map(|step| {
        //             step.execute_step(persistence)
        //             // execute_step(step.clone(), persistence)
        //         })
        //         .collect();

        //     if r.is_err() {
        //         Err(r.unwrap_err())
        //     } else {
        //         Ok(())
        //     }


        let loaded_data: Result<Vec<(String, Box<IndexIdToParent<Output = u32>>)>, SearchError> = self.meta_data.key_value_stores.clone().into_par_iter().map(|el| {
            info_time!(format!("loaded key_value_store {:?}", &el.path));

            let mut loading_type = el.loading_type.clone();
            if let Some(val) = load_type_from_env()? {
                loading_type = val;
            }

            match loading_type {
                LoadingType::InMemory => {
                    let indirect = file_to_bytes(&(get_file_path(&self.db, &el.path) + ".indirect"))?;
                    let data = file_to_bytes(&(get_file_path(&self.db, &el.path) + ".data"))?;
                    let indirect_u32 = bytes_to_vec_u32(&indirect);
                    let data_u32 = bytes_to_vec_u32(&data);

                    // return Ok((el.path.to_string(), Box::new(IndexIdToMultipleParentIndirect{start_and_end: indirect_u32, data:data_u32}) as Box<IndexIdToParent<Output = u32>> ));
                    // self.cache
                    //         .index_id_to_parento
                    //         .insert(el.path.to_string(), Box::new(IndexIdToMultipleParentIndirect{start_and_end: indirect_u32, data:data_u32}));

                    // {
                    //     let start_and_end_file = self.get_file_handle(&(el.path.to_string() + ".indirect"))?;
                    //     let data_file = self.get_file_handle(&(el.path.to_string() + ".data"))?;
                    //     let data_metadata = self.get_file_metadata_handle(&(el.path.to_string() + ".indirect"))?;
                    //     let store = PointingArrayFileReader::new(start_and_end_file, data_file, data_metadata);
                    //     // self.cache
                    //     //         .index_id_to_parento
                    //     //         .insert(el.path.to_string(), Box::new(IndexIdToMultipleParent::new(&store)));

                    //     return Ok((el.path.to_string(), Box::new(IndexIdToMultipleParent::new(&store)) as Box<IndexIdToParent<Output = u32>> ));
                    // }


                    {
                        let store = IndexIdToMultipleParentCompressedMaydaINDIRECTOne {
                            size: indirect_u32.len() / 2,
                            start_and_end: to_monotone(&indirect_u32),
                            data: to_uniform(&data_u32),
                            max_value_id: el.max_value_id,
                            avg_join_size: el.avg_join_size,
                        };

                        return Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>> ));
                        // if el.is_1_to_n {
                        //     return Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>> ));
                        // } else {
                        //     return Ok((el.path.to_string(), Box::new(IndexIdToOneParentMayda::<u32>::new(&store)) as Box<IndexIdToParent<Output = u32>> ));
                        // }
                    }
                    // self.cache
                    //         .index_id_to_parento
                    //         .insert(el.path.to_string(), Box::new(store));


                    // if el.is_1_to_n {
                    //     self.cache
                    //         .index_id_to_parento
                    //         .insert(el.path.to_string(), Box::new(store));
                    // } else {
                    //     self.cache.index_id_to_parento.insert(
                    //         el.path.to_string(),
                    //         Box::new(IndexIdToOneParentMayda::<u32>::new(&store)),
                    //     );
                    // }

                    // let store: Box<IndexIdToParent<Output = u32>> = {
                    //     match el.persistence_type {
                    //         // KVStoreType::ParallelArrays => {
                    //         //     let encoded = file_to_bytes(&get_file_path(&self.db, &el.path))?;
                    //         //     Box::new(deserialize::<ParallelArrays<u32>>(&encoded[..]).unwrap())
                    //         // }
                    //         KVStoreType::IndexIdToMultipleParentIndirect => {
                    //             let indirect = file_to_bytes(&(get_file_path(&self.db, &el.path) + ".indirect"))?;
                    //             let data = file_to_bytes(&(get_file_path(&self.db, &el.path) + ".data"))?;
                    //             Box::new(IndexIdToMultipleParentIndirect::from_data(
                    //                 bytes_to_vec_u32(&indirect),
                    //                 bytes_to_vec_u32(&data),
                    //             ))
                    //         }
                    //         _ => panic!("unecpected type ParallelArrays")
                    //     }
                    // };

                    // if el.is_1_to_n {
                    //     // self.cache.index_id_to_parento.insert(el.path.to_string(), Box::new(IndexIdToMultipleParentCompressedSnappy::new(&store)));
                    //     self.cache.index_id_to_parento.insert(
                    //         el.path.to_string(),
                    //         Box::new(IndexIdToMultipleParentCompressedMaydaINDIRECTOne::<u32>::new(&*store)),
                    //     );
                    // } else {
                    //     self.cache.index_id_to_parento.insert(
                    //         el.path.to_string(),
                    //         Box::new(IndexIdToOneParentMayda::<u32>::new(&*store)),
                    //     );
                    // }
                }
                LoadingType::Disk => {
                    let start_and_end_file = self.get_file_handle(&(el.path.to_string() + ".indirect"))?;
                    let data_file = self.get_file_handle(&(el.path.to_string() + ".data"))?;
                    let data_metadata = self.get_file_metadata_handle(&(el.path.to_string() + ".indirect"))?;
                    let store = PointingArrayFileReader::new(start_and_end_file, data_file, data_metadata, el.max_value_id, el.avg_join_size);

                    // let store = PointingArrayFileReader { start_and_end_file: el.path.to_string()+ ".indirect", data_file: el.path.to_string()+ ".data", persistence: self.db.to_string()};
                    // self.cache
                    //     .index_id_to_parento
                    //     .insert(el.path.to_string(), Box::new(store));

                    return Ok((el.path.to_string(), Box::new(store) as Box<IndexIdToParent<Output = u32>> ));
                }
            }
        }).collect();

        match loaded_data {
            Err(e) => return Err(e),
            Ok(dat) => {
                for el in dat {
                    self.cache.index_id_to_parento.insert(el.0, el.1);
                }
            },
        };

        // Load Boost Indices
        for el in &self.meta_data.boost_stores {
            let encoded = file_to_bytes(&get_file_path(&self.db, &el.path))?;
            let store: ParallelArrays<u32> = deserialize(&encoded[..]).unwrap();
            self.cache.boost_valueid_to_value.insert(
                el.path.to_string(),
                Box::new(IndexIdToOneParentMayda::<u32>::new(&store)),
            );
        }

        // Load FST
        for (ref path, _) in &self.meta_data.fulltext_indices {
            let map = self.load_fst(path)?; // "Could not load FST"
            self.cache.fst.insert(path.to_string(), map);
        }
        Ok(())
    }

    #[flame]
    pub fn load_index_64(&mut self, path: &str) -> Result<(), search::SearchError> {
        let loading_type = load_type_from_env()?.unwrap_or(LoadingType::InMemory);

        match loading_type {
            LoadingType::InMemory => {
                let file_path = get_file_path(&self.db, path);
                self.cache.index_64.insert(
                    path.to_string(),
                    Box::new(IndexIdToOneParentMayda::from_vec(&load_index_u64(
                        &file_path,
                    )?)),
                );
            }
            LoadingType::Disk => {
                let data_file = self.get_file_handle(&path)?;
                let data_metadata = self.get_file_metadata_handle(&path)?;

                self.cache.index_64.insert(
                    path.to_string(),
                    Box::new(SingleArrayFileReader::<u64>::new(data_file, data_metadata)),
                );
            }
        }

        Ok(())
    }
    // pub fn load_index_32(&mut self, s1: &str) -> Result<(), io::Error> {
    //     if self.cache.index_32.contains_key(s1){return Ok(()); }
    //     self.cache.index_32.insert(s1.to_string(), load_indexo(&get_file_path(&self.db, s1))?);
    //     Ok(())
    // }
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
        self.file
            .seek(SeekFrom::Start(offsets.get_value(pos).unwrap()));
        self.file.read_exact(&mut self.buffer).unwrap();
        // unsafe {str::from_utf8_unchecked(&buffer)}
        // let s = unsafe {str::from_utf8_unchecked(&buffer)};
        // str::from_utf8(&buffer).unwrap() // @Temporary  -> use unchecked if stable
    }

    // pub fn binary_search(&mut self, term: &str, persistence: &Persistence) -> Result<(String, i64), io::Error> {
    //     // let cache_lock = INDEX_64_CACHE.read().unwrap();
    //     // let offsets = cache_lock.get(&(self.path.to_string()+".offsets")).unwrap();
    //     let offsets = persistence.cache.index_64.get(&(self.path.to_string() + ".offsets")).unwrap();
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

// fn bytes_to_vec<T: Clone>(mut data: &mut Vec<u8>) -> Vec<T> {
//     if let Some((result, remaining)) = unsafe { decode::<Vec<T>>(&mut data) } {
//         assert!(remaining.len() == 0);
//         result.clone()
//     }else{
//         panic!("Could no load Vector");
//     }
// }

// fn vec_to_bytes<T: Clone + Integer + NumCast + Copy + Debug>(data:&Vec<T>) -> Vec<u8> {
//     let mut bytes:Vec<u8> = Vec::new();
//     unsafe { encode(data, &mut bytes); };
//     bytes
// }

fn load_type_from_env() -> Result<Option<LoadingType>, search::SearchError> {
    if let Some(val) = env::var_os("LoadingType") {
        let loading_type = LoadingType::from_str(&val.into_string().unwrap())
            .map_err(|_err| search::SearchError::StringError("only InMemory or Disk allowed for LoadingType environment variable".to_string()))?;
        Ok(Some(loading_type))
    } else {
        Ok(None)
    }
}

pub fn vec_to_bytes_u32(data: &Vec<u32>) -> Vec<u8> {
    let mut wtr: Vec<u8> = Vec::with_capacity(data.len() * std::mem::size_of::<u32>());
    for el in data {
        wtr.write_u32::<LittleEndian>(*el).unwrap();
    }
    wtr.shrink_to_fit();
    wtr
}
pub fn vec_to_bytes_u64(data: &Vec<u64>) -> Vec<u8> {
    let mut wtr: Vec<u8> = Vec::with_capacity(data.len() * std::mem::size_of::<u64>());
    for el in data {
        wtr.write_u64::<LittleEndian>(*el).unwrap();
    }
    wtr.shrink_to_fit();
    wtr
}
pub fn bytes_to_vec_u32(data: &[u8]) -> Vec<u32> {
    let mut out_dat = Vec::with_capacity(data.len() / std::mem::size_of::<u32>());
    let mut rdr = Cursor::new(data);
    while let Ok(el) = rdr.read_u32::<LittleEndian>() {
        out_dat.push(el);
    }
    out_dat.shrink_to_fit();
    out_dat
}
pub fn bytes_to_vec_u64(data: &[u8]) -> Vec<u64> {
    let mut out_dat = Vec::with_capacity(data.len() / std::mem::size_of::<u64>());
    let mut rdr = Cursor::new(data);
    while let Ok(el) = rdr.read_u64::<LittleEndian>() {
        out_dat.push(el);
    }
    out_dat.shrink_to_fit();
    out_dat
}

fn file_to_bytes(s1: &str) -> Result<Vec<u8>, io::Error> {
    let file_size = { fs::metadata(s1)?.len() as usize };
    let f = File::open(s1)?;
    let mut reader = std::io::BufReader::new(f);
    let mut buffer: Vec<u8> = Vec::with_capacity(file_size);
    reader.read_to_end(&mut buffer)?;
    // buffer.shrink_to_fit();
    Ok(buffer)
}

pub fn load_index_u32(s1: &str) -> Result<Vec<u32>, io::Error> {
    info!("Loading Index32 {} ", s1);
    Ok(bytes_to_vec_u32(&file_to_bytes(s1)?))
}

pub fn load_index_u64(s1: &str) -> Result<Vec<u64>, io::Error> {
    info!("Loading Index64 {} ", s1);
    Ok(bytes_to_vec_u64(&file_to_bytes(s1)?))
}

// fn load_indexo<T: Clone>(s1: &str) -> Result<Vec<T>, io::Error> {
//     info!("Loading Index32 {} ", s1);
//     let mut buffer = file_to_bytes(s1)?;
//     Ok(bytes_to_vec::<T>(&mut buffer))
// }

fn check_is_docid_type<T: Integer + NumCast + Copy>(data: &Vec<T>) -> bool {
    for (index, value_id) in data.iter().enumerate() {
        let blub: usize = num::cast(*value_id).unwrap();
        if blub != index {
            return false;
        }
    }
    return true;
}
