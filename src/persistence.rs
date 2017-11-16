use std::fs::{self, File};
use std::io::prelude::*;
use std::io::{self, Cursor, SeekFrom};
use std::str;
use std::collections::HashMap;
use std::fmt::Debug;
use std::mem;
use std::marker::Sync;
use std::sync::Mutex;

use util;
use util::get_file_path;

use serde_json;
use serde_json::Value;

use fnv::FnvHashMap;
use bincode::{deserialize, serialize, Infinite};

// use abomonation::{encode, decode, Abomonation};
use create;
use snap;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[allow(unused_imports)]
use fst::{IntoStreamer, Levenshtein, Map, MapBuilder, Set};

use persistence_data::*;

#[allow(unused_imports)]
use search::{self, SearchError};
use num::{self, Integer, NumCast};

#[allow(unused_imports)]
use heapsize::{heap_size_of, HeapSizeOf};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct MetaData {
    pub id_lists:         FnvHashMap<String, IDList>,
    pub key_value_stores: Vec<KVStoreMetaData>,
    pub boost_stores:     Vec<KVStoreMetaData>,
    pub fulltext_indices: FnvHashMap<String, create::FulltextIndexOptions>,
}

impl MetaData {
    pub fn new(folder: &str) -> MetaData {
        let json = util::file_as_string(&(folder.to_string() + "/metaData.json")).unwrap();
        serde_json::from_str(&json).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KVStoreMetaData {
    pub path:               String,
    pub key_has_duplicates: bool, // In the sense of 1:n   1key, n values
    pub persistence_type:   KVStoreType,
    pub loading_type:       LoadingType,
}

// impl KVStoreMetaData {
//     fn new(valid_path: &str, parentid_path: &str, key_has_duplicates: bool, persistence_type: KVStoreType, loading_type: LoadingType) -> KVStoreMetaData {
//         KVStoreMetaData{persistence_type:KVStoreType::ParallelArrays, key_has_duplicates:has_duplicates, valid_path: valid_path.clone(), parentid_path:parentid_path.clone()}
//     }
// }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum LoadingType {
    InMemory,
    Disk,
}

impl Default for LoadingType {
    fn default() -> LoadingType {
        LoadingType::InMemory
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KVStoreType {
    ParallelArrays,
    PointingArrays,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IDList {
    pub path:        String,
    pub size:        u64,
    pub id_type:     IDDataType,
    pub doc_id_type: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum IDDataType {
    U32,
    U64,
}

pub trait IndexIdToParent: Debug + HeapSizeOf + Sync + Send {
    fn get_values(&self, id: u64) -> Option<Vec<u32>>;
    fn get_value(&self, id: u64) -> Option<u32> {
        self.get_values(id).map(|el| el[0])
    }
    fn get_keys(&self) -> Vec<u32>;
}

use std::i32;
static NOT_FOUND: i32 = i32::MIN;

#[derive(Debug)]
struct IndexIdToMultipleParent {
    data: Vec<Vec<u32>>,
}
impl IndexIdToMultipleParent {
    fn new(data: &IndexIdToParent) -> IndexIdToMultipleParent {
        let data = id_to_parent_to_array_of_array(data);
        // let data = data.iter().map(|el| if el.len() >0 { el[0] as i32 } else{ NOT_FOUND }).collect();
        IndexIdToMultipleParent { data }
    }
}
impl IndexIdToParent for IndexIdToMultipleParent {
    fn get_values(&self, id: u64) -> Option<Vec<u32>> {
        self.data.get(id as usize).map(|el| el.clone())
    }
    fn get_keys(&self) -> Vec<u32> {
        (0..self.data.len() as u32).collect()
    }
}

impl HeapSizeOf for IndexIdToMultipleParent {
    fn heap_size_of_children(&self) -> usize {
        self.data.heap_size_of_children()
    }
}

lazy_static! {
    static ref SNAP_DECODER: Mutex<snap::Decoder> = {
        Mutex::new(snap::Decoder::new())
    };
}

#[derive(Debug)]
#[allow(dead_code)]
struct IndexIdToMultipleParentCompressed {
    data: Vec<Vec<u8>>,
}
impl IndexIdToMultipleParentCompressed {
    #[allow(dead_code)]
    fn new(store: &IndexIdToParent) -> IndexIdToMultipleParentCompressed {
        let mut encoder = snap::Encoder::new();
        let mut data: Vec<Vec<u8>> = store
            .get_keys()
            .iter()
            .map(|el| {
                let el = store.get_values(*el as u64).unwrap_or_else(|| vec![]);
                let mut dat = encoder.compress_vec(&vec_to_bytes_u32(&el.clone())).unwrap();
                dat.shrink_to_fit();
                dat
            })
            .collect();
        data.shrink_to_fit();
        IndexIdToMultipleParentCompressed { data }
    }
}

impl IndexIdToParent for IndexIdToMultipleParentCompressed {
    fn get_values(&self, id: u64) -> Option<Vec<u32>> {
        self.data.get(id as usize).map(|el| {
            // el.clone()
            // let mut decoder = snap::Decoder::new();
            bytes_to_vec_u32(&SNAP_DECODER.lock().unwrap().decompress_vec(el).unwrap())
        })
    }
    fn get_keys(&self) -> Vec<u32> {
        (0..self.data.len() as u32).collect()
    }
}

impl HeapSizeOf for IndexIdToMultipleParentCompressed {
    fn heap_size_of_children(&self) -> usize {
        self.data.heap_size_of_children()
    }
}

#[derive(Debug)]
struct IndexIdToOneParent {
    data: Vec<i32>,
}
impl IndexIdToOneParent {
    fn new(data: &IndexIdToParent) -> IndexIdToOneParent {
        let data = id_to_parent_to_array_of_array(data);
        let data = data.iter().map(|el| if el.len() > 0 { el[0] as i32 } else { NOT_FOUND }).collect();
        IndexIdToOneParent { data }
    }
}
impl IndexIdToParent for IndexIdToOneParent {
    fn get_values(&self, id: u64) -> Option<Vec<u32>> {
        self.get_value(id).map(|el| vec![el])
    }
    fn get_value(&self, id: u64) -> Option<u32> {
        let val = self.data.get(id as usize);
        match val {
            Some(val) => if *val == NOT_FOUND {
                None
            } else {
                Some(val.clone() as u32)
            },
            None => None,
        }
    }
    fn get_keys(&self) -> Vec<u32> {
        (0..self.data.len() as u32).collect()
    }
}
impl HeapSizeOf for IndexIdToOneParent {
    fn heap_size_of_children(&self) -> usize {
        self.data.heap_size_of_children()
    }
}

#[derive(Debug, Default)]
pub struct PersistenceCache {
    // pub index_id_to_parent: HashMap<(String,String), Vec<Vec<u32>>>,
    pub index_id_to_parento:    HashMap<String, Box<IndexIdToParent>>,
    pub boost_valueid_to_value: HashMap<String, Box<IndexIdToParent>>,
    pub index_64:               HashMap<String, Vec<u64>>,
    // index_32: HashMap<String, Vec<u32>>,
    pub fst: HashMap<String, Map>,
}

#[derive(Debug, Default)]
pub struct Persistence {
    pub db:        String, // folder
    pub meta_data: MetaData,
    pub cache:     PersistenceCache,
}

// fn has_duplicates<T: Copy + Clone + Integer>(data: &Vec<T>) -> bool {
//     if data.len() == 0 {return false;}
//     let mut prev = data[0];
//     for el in data[1..].iter() {
//         if *el == prev {return true; }
//         prev = *el;
//     }
//     return false;
// }

fn has_valid_duplicates(data: &Vec<&create::GetValueId>) -> bool {
    if data.len() == 0 {
        return false;
    }
    let mut prev = data[0].get_value_id();
    for el in data[1..].iter() {
        if el.get_value_id() == prev {
            return true;
        }
        prev = el.get_value_id();
    }
    return false;
}

// fn has_valid_pair_duplicates(data: &Vec<ValIdToValue>) -> bool {
//     if data.len() == 0 {return false;}
//     let mut prev = data[0].valid;
//     for el in data[1..].iter() {
//         if *el.valid == prev {return true; }
//         prev = *el;
//     }
//     return false;
// }

impl Persistence {
    pub fn print_heap_sizes(&self) {
        println!("cache.index_64 {:?}mb", self.cache.index_64.heap_size_of_children() / 1_000_000);
        println!("cache.index_id_to_parento {:?}mb", self.cache.index_id_to_parento.heap_size_of_children() / 1_000_000);
        println!("cache.boost_valueid_to_value {:?}mb", self.cache.boost_valueid_to_value.heap_size_of_children() / 1_000_000);
        for (k, v) in &self.cache.index_id_to_parento {
            println!("{:?} {:?}mb", k, v.heap_size_of_children() / 1_000_000);
        }
        // println!("cache.fst {:?}mb", self.cache.fst.heap_size_of_children()/1_000_000);
    }

    pub fn load(db: String) -> Result<Self, io::Error> {
        let meta_data = MetaData::new(&db);
        let mut pers = Persistence { meta_data, db, ..Default::default() };
        pers.load_all_to_cache()?;
        Ok(pers)
    }

    pub fn create(db: String) -> Result<Self, io::Error> {
        fs::create_dir_all(&db)?;
        let meta_data = MetaData { ..Default::default() };
        Ok(Persistence { meta_data, db, ..Default::default() })
    }

    pub fn write_tuple_pair(&mut self, tuples: &mut Vec<create::ValIdPair>, path: &str) -> Result<(), io::Error> {
        let has_duplicates = has_valid_duplicates(&tuples.iter().map(|el| el as &create::GetValueId).collect());
        let data = valid_pair_to_parallel_arrays(tuples);
        // let has_duplicates = has_duplicates(&data.values1);
        let encoded: Vec<u8> = serialize(&data, Infinite).unwrap();
        File::create(util::get_file_path(&self.db, &path.to_string()))?.write_all(&encoded)?;

        self.meta_data.key_value_stores.push(KVStoreMetaData {
            loading_type:       LoadingType::InMemory,
            persistence_type:   KVStoreType::ParallelArrays,
            key_has_duplicates: has_duplicates,
            path:               path.to_string(),
        });
        Ok(())
    }
    pub fn write_boost_tuple_pair(&mut self, tuples: &mut Vec<create::ValIdToValue>, path: &str) -> Result<(), io::Error> {
        // let boost_paths = util::boost_path(path);
        let has_duplicates = has_valid_duplicates(&tuples.iter().map(|el| el as &create::GetValueId).collect());
        let data = boost_pair_to_parallel_arrays(tuples);
        // let data = parrallel_arrays_to_pointing_array(data.values1, data.values2);
        let encoded: Vec<u8> = serialize(&data, Infinite).unwrap();
        let boost_path = path.to_string() + ".boost_valid_to_value";
        File::create(util::get_file_path(&self.db, &boost_path))?.write_all(&encoded)?;

        self.meta_data.boost_stores.push(KVStoreMetaData {
            loading_type:       LoadingType::InMemory,
            persistence_type:   KVStoreType::ParallelArrays,
            key_has_duplicates: has_duplicates,
            path:               boost_path.to_string(),
        });
        Ok(())
    }

    pub fn write_index<T: Clone + Integer + NumCast + Copy + Debug>(&mut self, bytes: &Vec<u8>, data: &Vec<T>, path: &str) -> Result<(), io::Error> {
        // let bytes = vec_to_bytes(&data);
        File::create(util::get_file_path(&self.db, path))?.write_all(&bytes)?;
        // unsafe { File::create(path)?.write_all(typed_to_bytes(data))?; }
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
                path:        path.to_string(),
                size:        data.len() as u64,
                id_type:     sizo,
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

    pub fn write_meta_data(&self) -> Result<(), io::Error> {
        self.write_data("metaData.json", serde_json::to_string_pretty(&self.meta_data).unwrap().as_bytes())
    }

    pub fn write_data(&self, path: &str, data: &[u8]) -> Result<(), io::Error> {
        File::create(&get_file_path(&self.db, path))?.write_all(data)?;
        Ok(())
    }

    pub fn get_buffered_writer(&self, path: &str) -> Result<io::BufWriter<fs::File>, io::Error> {
        Ok(io::BufWriter::new(File::create(&get_file_path(&self.db, path))?))
    }

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
        self.write_index(&vec_to_bytes_u64(&offsets), &offsets, &(path.to_string() + ".offsets"))?;
        Ok(())
    }

    pub fn get_valueid_to_parent(&self, path: &str) -> &Box<IndexIdToParent> {
        // @Temporary Check if in cache
        self.cache.index_id_to_parento.get(path).expect(&format!("Did not found path in cache {:?}", path))
    }

    pub fn get_boost(&self, path: &str) -> &Box<IndexIdToParent> {
        // @Temporary Check if in cache
        self.cache
            .boost_valueid_to_value
            .get(path)
            .expect(&format!("Did not found path in cache {:?}", path))
    }

    pub fn get_file_search(&self, path: &str) -> FileSearch {
        FileSearch::new(path, self.get_file_handle(path).unwrap())
    }

    pub fn get_file_handle(&self, path: &str) -> Result<File, io::Error> {
        Ok(File::open(&get_file_path(&self.db, path))?)
    }

    pub fn load_fst(&self, path: &str) -> Result<Map, search::SearchError> {
        let mut f = self.get_file_handle(&(path.to_string() + ".fst"))?;
        let mut buffer: Vec<u8> = Vec::new();
        f.read_to_end(&mut buffer)?;
        buffer.shrink_to_fit();
        Ok(Map::from_bytes(buffer)?)
    }

    pub fn get_fst(&self, path: &str) -> Result<&Map, search::SearchError> {
        Ok(self.cache.fst.get(path).expect("load fst no found"))
    }

    // pub fn get_create_char_offset_info(&self, path: &str,character: &str) -> Result<Option<OffsetInfo>, search::SearchError> { // @Temporary - replace SearchError
    //     let char_offset = CharOffset::new(path)?;
    //     return Ok(char_offset.get_char_offset_info(character, &self.cache.index_64).ok());
    // }

    pub fn load_all_to_cache(&mut self) -> Result<(), io::Error> {
        for (_, ref idlist) in &self.meta_data.id_lists.clone() {
            match &idlist.id_type {
                &IDDataType::U32 => {}
                &IDDataType::U64 => self.load_index_64(&idlist.path)?,
            }
        }

        for el in &self.meta_data.key_value_stores {
            info_time!("create key_value_store");

            let encoded = file_to_bytes(&get_file_path(&self.db, &el.path))
                .expect(&format!("Could not Load {:?}", get_file_path(&self.db, &el.path)));
            let store: ParallelArrays = deserialize(&encoded[..]).unwrap();
            // let store = parrallel_arrays_to_pointing_array(store.values1, store.values2);
            if el.key_has_duplicates {
                // self.cache.index_id_to_parento.insert(el.path.to_string(), Box::new(IndexIdToMultipleParentCompressed::new(&store)));
                self.cache
                    .index_id_to_parento
                    .insert(el.path.to_string(), Box::new(IndexIdToMultipleParent::new(&store)));
            } else {
                self.cache
                    .index_id_to_parento
                    .insert(el.path.to_string(), Box::new(IndexIdToOneParent::new(&store)));
            }
        }

        // Load Boost Indices
        for el in &self.meta_data.boost_stores {
            let encoded = file_to_bytes(&get_file_path(&self.db, &el.path))
                .expect(&format!("Could not Load {:?}", get_file_path(&self.db, &el.path)));
            let store: ParallelArrays = deserialize(&encoded[..]).unwrap();
            // let store = parrallel_arrays_to_pointing_array(store.values1, store.values2);
            let data = id_to_parent_to_array_of_array(&store);
            let data = data.iter().map(|el| if el.len() > 0 { el[0] as i32 } else { NOT_FOUND }).collect();
            self.cache.boost_valueid_to_value.insert(el.path.to_string(), Box::new(IndexIdToOneParent { data }));
        }

        // Load FST
        for (ref path, _) in &self.meta_data.fulltext_indices {
            let map = self.load_fst(path).expect("Could not load FST");
            self.cache.fst.insert(path.to_string(), map);
        }
        Ok(())
    }

    pub fn load_index_64(&mut self, s1: &str) -> Result<(), io::Error> {
        if self.cache.index_64.contains_key(s1) {
            return Ok(());
        }
        self.cache.index_64.insert(s1.to_string(), load_index_u64(&get_file_path(&self.db, s1))?);
        Ok(())
    }
    // pub fn load_index_32(&mut self, s1: &str) -> Result<(), io::Error> {
    //     if self.cache.index_32.contains_key(s1){return Ok(()); }
    //     self.cache.index_32.insert(s1.to_string(), load_indexo(&get_file_path(&self.db, s1))?);
    //     Ok(())
    // }
}

#[test]
fn test_snap() {
    let mut encoder = snap::Encoder::new();
    let mut data: Vec<Vec<u32>> = vec![];
    data.push(vec![
        11, 12, 13, 14, 15, 16, 17, 18, 19, 110, 111, 112, 113, 114, 115, 116, 117, 118
    ]);
    data.push(vec![10, 11, 12, 13, 14, 15]);
    data.push(vec![10]);
    println!("data orig {:?}", data.heap_size_of_children());
    // let data2:Vec<Vec<u8>> = data.iter().map(|el| {
    //     let mut el = el.clone();
    //     el.sort();
    //     let mut dat = encoder.compress_vec(&vec_to_bytes(&el)).unwrap();
    //     dat.shrink_to_fit();
    //     dat
    // }).collect();
    // println!("data abono compressed {:?}", data2.heap_size_of_children());

    // let data3:Vec<Vec<u8>> = data.iter().map(|el| {
    //     let el = el.clone();
    //     let mut dat = vec_to_bytes(&el);
    //     dat.shrink_to_fit();
    //     dat
    // }).collect();
    // println!("data abono bytes {:?}", data3.heap_size_of_children());

    let data4: Vec<Vec<u8>> = data.iter().map(|el| vec_to_bytes_u32(el)).collect();
    println!("data byteorder {:?}", data4.heap_size_of_children());

    let data5: Vec<Vec<u8>> = data.iter()
        .map(|el| {
            let mut dat = encoder.compress_vec(&vec_to_bytes_u32(el)).unwrap();
            dat.shrink_to_fit();
            dat
        })
        .collect();
    println!("data byteorder compressed {:?}", data5.heap_size_of_children());

    // let mut test_vec:Vec<u32> = vec![10];
    // test_vec.shrink_to_fit();
    // let mut bytes:Vec<u8> = Vec::new();
    // unsafe { encode(&test_vec, &mut bytes); };
    // bytes.shrink_to_fit();
    // println!("{:?}", test_vec);
    // println!("{:?}", bytes);

    let mut wtr: Vec<u8> = vec![];
    wtr.write_u32::<LittleEndian>(10).unwrap();
    println!("wtr {:?}", wtr);
}


pub fn id_to_parent_to_array_of_array(store: &IndexIdToParent) -> Vec<Vec<u32>> {
    let mut data = vec![];
    let mut valids = store.get_keys();
    valids.dedup();
    if valids.len() == 0 {
        return data;
    }
    data.resize(*valids.last().unwrap() as usize + 1, vec![]);

    info_time!("create insert key_value_store");
    for valid in valids {
        let mut vals = store.get_values(valid as u64).unwrap();
        vals.sort();
        data[valid as usize] = vals;
    }
    data
}


// #[derive(Debug)]
// pub struct OffsetInfo {
//     pub byte_range_start: u64,
//     pub byte_range_end: u64,
//     pub line_offset: u64,
// }

// #[derive(Debug)]
// pub struct CharOffset {
//     path: String,
//     chars: Vec<String>,
// }


// impl CharOffset {
//     fn new(path:&str) -> Result<CharOffset, SearchError> {
//         let char_offset = CharOffset {
//             path: path.to_string(),
//             chars: util::file_as_string(&(path.to_string()+".char_offsets.chars"))?.lines().collect::<Vec<_>>().iter().map(|el| el.to_string()).collect(), // @Cleanup // @Temporary  sinlge  collect
//         };
//         trace!("Loaded CharOffset:{} ", path );
//         trace!("{:?}", char_offset);
//         Ok(char_offset)
//     }
//     pub fn get_char_offset_info(&self,character: &str, ix64: &HashMap<String, Vec<u64>>) -> Result<OffsetInfo, usize>{
//         match self.chars.binary_search(&character.to_string()) {
//             Ok(index) => Ok(self.get_offset_info(index, ix64)),
//             Err(nearest_index) => Ok(self.get_offset_info(nearest_index-1, ix64)),
//         }
//     }
//     fn get_offset_info(&self, index: usize, ix64: &HashMap<String, Vec<u64>>) -> OffsetInfo {
//         let byte_offsets_start = ix64.get(&(self.path.to_string()+".char_offsets.byteOffsetsStart")).unwrap();
//         let byte_offsets_end =   ix64.get(&(self.path.to_string()+".char_offsets.byteOffsetsEnd")).unwrap();
//         let line_offsets =       ix64.get(&(self.path.to_string()+".char_offsets.lineOffset")).unwrap();

//         trace!("get_offset_info path:{}\tindex:{}\toffsetSize: {}", self.path, index, byte_offsets_start.len());
//         return OffsetInfo{byte_range_start: byte_offsets_start[index], byte_range_end: byte_offsets_end[index], line_offset: line_offsets[index]};
//     }
// }



#[derive(Debug)]
pub struct FileSearch {
    path: String,
    // offsets: Vec<u64>,
    file:   File,
    buffer: Vec<u8>,
}


impl FileSearch {
    fn new(path: &str, file: File) -> Self {
        // load_index_64_into_cache(&(path.to_string()+".offsets")).unwrap();
        FileSearch {
            path:   path.to_string(),
            file:   file,
            buffer: Vec::with_capacity(50 as usize),
        }
    }

    fn load_text<'a>(&mut self, pos: usize, offsets: &Vec<u64>) {
        // @Temporary Use Result
        let string_size = offsets[pos + 1] - offsets[pos] - 1;
        // let mut buffer:Vec<u8> = Vec::with_capacity(string_size as usize);
        // unsafe { buffer.set_len(string_size as usize); }
        self.buffer.resize(string_size as usize, 0);
        self.file.seek(SeekFrom::Start(offsets[pos])).unwrap();
        self.file.read_exact(&mut self.buffer).unwrap();
        // unsafe {str::from_utf8_unchecked(&buffer)}
        // let s = unsafe {str::from_utf8_unchecked(&buffer)};
        // str::from_utf8(&buffer).unwrap() // @Temporary  -> use unchecked if stable
    }

    pub fn binary_search(&mut self, term: &str, persistence: &Persistence) -> Result<(String, i64), io::Error> {
        // let cache_lock = INDEX_64_CACHE.read().unwrap();
        // let offsets = cache_lock.get(&(self.path.to_string()+".offsets")).unwrap();
        let offsets = persistence.cache.index_64.get(&(self.path.to_string() + ".offsets")).unwrap();
        debug_time!("term binary_search");
        if offsets.len() < 2 {
            return Ok(("".to_string(), -1));
        }
        let mut low = 0;
        let mut high = offsets.len() - 2;
        let mut i;
        while low <= high {
            i = (low + high) >> 1;
            self.load_text(i, offsets);
            // println!("Comparing {:?}", str::from_utf8(&buffer).unwrap());
            // comparison = comparator(arr[i], find);
            if str::from_utf8(&self.buffer).unwrap() < term {
                low = i + 1;
                continue;
            }
            if str::from_utf8(&self.buffer).unwrap() > term {
                high = i - 1;
                continue;
            }
            return Ok((str::from_utf8(&self.buffer).unwrap().to_string(), i as i64));
        }
        Ok(("".to_string(), -1))
    }
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


pub fn vec_to_bytes_u32(data: &Vec<u32>) -> Vec<u8> {
    let mut wtr: Vec<u8> = vec![];
    for el in data {
        wtr.write_u32::<LittleEndian>(*el).unwrap();
    }
    wtr.shrink_to_fit();
    wtr
}
pub fn vec_to_bytes_u64(data: &Vec<u64>) -> Vec<u8> {
    let mut wtr: Vec<u8> = vec![];
    for el in data {
        wtr.write_u64::<LittleEndian>(*el).unwrap();
    }
    wtr.shrink_to_fit();
    wtr
}
pub fn bytes_to_vec_u32(data: &[u8]) -> Vec<u32> {
    let mut out_dat = vec![];
    let mut rdr = Cursor::new(data);
    while let Ok(el) = rdr.read_u32::<LittleEndian>() {
        out_dat.push(el);
    }
    out_dat.shrink_to_fit();
    out_dat
}
pub fn bytes_to_vec_u64(data: &[u8]) -> Vec<u64> {
    let mut out_dat = vec![];
    let mut rdr = Cursor::new(data);
    while let Ok(el) = rdr.read_u64::<LittleEndian>() {
        out_dat.push(el);
    }
    out_dat.shrink_to_fit();
    out_dat
}

fn file_to_bytes(s1: &str) -> Result<Vec<u8>, io::Error> {
    let mut f = File::open(s1)?;
    let mut buffer: Vec<u8> = Vec::new();
    f.read_to_end(&mut buffer)?;
    buffer.shrink_to_fit();
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
