use std::fs::{self, File};
use std::io::prelude::*;
use std::io::{self, Cursor, SeekFrom};
use std::str;
use std::collections::HashMap;
use std::fmt::Debug;
use std::mem;
use std::marker::Sync;

use util;
use util::get_file_path;

use serde_json;
use serde_json::Value;

use fnv::FnvHashMap;
use bincode::{deserialize, serialize, Infinite};

use create;
use mayda;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log;

#[allow(unused_imports)]
use fst::{IntoStreamer, Map, MapBuilder, Set};

use prettytable::Table;
// use prettytable::row::Row;
// use prettytable::cell::Cell;
use prettytable::format;

use persistence_data::*;

#[allow(unused_imports)]
use search::{self, SearchError};
use num::{self, Integer, NumCast};

#[allow(unused_imports)]
use heapsize::{heap_size_of, HeapSizeOf};

use std::env;

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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
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
    pub path:        String,
    pub size:        u64,
    pub id_type:     IDDataType,
    pub doc_id_type: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum IDDataType {
    U32,
    U64,
}
use persistence_data;


pub trait IndexIdToParent: Debug + HeapSizeOf + Sync + Send + persistence_data::TypeInfo {
    fn get_values(&self, id: u64) -> Option<Vec<u32>>;
    fn get_values_compr(&self, _id: u64) -> Option<mayda::Uniform<u32>>{
        unimplemented!()
    }
    fn get_value(&self, id: u64) -> Option<u32> {
        self.get_values(id).map(|el| el[0])
    }

    //last needs to be the largest value_id
    fn get_keys(&self) -> Vec<u32>;

}

pub fn trace_index_id_to_parent(val: &Box<IndexIdToParent>) {
    if log_enabled!(log::Level::Trace) {
        let keys = val.get_keys();
        for key in keys.iter() {
            if let Some(vals) = val.get_values(*key as u64) {
                trace!("key {:?} to {:?}", key, vals );
            }
        }
    }
}

use std::i32;
pub static NOT_FOUND: i32 = i32::MIN;

#[derive(Debug, Default)]
pub struct PersistenceCache {
    // pub index_id_to_parent: HashMap<(String,String), Vec<Vec<u32>>>,
    pub index_id_to_parento:    HashMap<String, Box<IndexIdToParent>>,
    pub boost_valueid_to_value: HashMap<String, Box<IndexIdToParent>>,
    index_64:               HashMap<String, Vec<u64>>,
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

use colored::*;

pub fn get_readable_size(value: usize) -> ColoredString {
    match value {
        0 ... 1_000 => format!("{:?} b", value).blue(),
        1_000 ... 1_000_000 => format!("{:?} kb", value / 1_000).green(),
        _ => format!("{:?} mb", value / 1_000_000).red(),
    }
}

impl Persistence {

    fn get_fst_sizes(&self) -> usize {
        self.cache.fst.iter().map(|(_,v)| v.as_fst().size()).sum()
    }

    pub fn print_heap_sizes(&self) {
        info!("cache.index_64 {}", get_readable_size(self.cache.index_64.heap_size_of_children()));
        info!("cache.index_id_to_parento {}", get_readable_size(self.cache.index_id_to_parento.heap_size_of_children()));
        info!("cache.boost_valueid_to_value {}", get_readable_size(self.cache.boost_valueid_to_value.heap_size_of_children()));
        info!("cache.fst {}", get_readable_size( self.get_fst_sizes()));
        info!("------");
        let total_size = self.get_fst_sizes()
            + self.cache.index_id_to_parento.heap_size_of_children()
            + self.cache.index_64.heap_size_of_children()
            + self.cache.boost_valueid_to_value.heap_size_of_children();

        info!("totale size {}", get_readable_size(total_size) );

        let mut print_and_size = vec![];
        // Add a row per time
        for (k, v) in &self.cache.index_id_to_parento {

            print_and_size.push((v.heap_size_of_children(), v.type_name(), k ));
            // println!("{:?} {:?} mb", k, v.heap_size_of_children() / 1_000_000);
            // table.add_row(row![v.type_name(), k, get_readable_size(v.heap_size_of_children() )]);
        }

        for (k, v) in &self.cache.fst {
            print_and_size.push((v.as_fst().size(), "FST".to_string(), k ));
            // println!("{:?} {:?} mb", k, v.heap_size_of_children() / 1_000_000);
            // table.add_row(row![v.type_name(), k, get_readable_size(v.heap_size_of_children() )]);
        }
        print_and_size.sort_by_key(|row|row.0);

        // Create the table
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        table.add_row(row!["Type", "Path", "Size"]);
        for row in print_and_size {
            table.add_row(row![row.1, row.2, get_readable_size(row.0)]);
        }

        info!("{}", table);
        // table.printstd();

        // for (k, v) in &self.cache.fst {
        //     // println!("cache.fst {:?}  {:?}mb", k, mem::size_of_val(v)/1_000_000);
        //     println!("cache.fst {:?}  {:?}mb", k, v.heap_size_of_children());
        // }
        // println!("cache.fst {:?}mb", self.cache.fst.heap_size_of_children()/1_000_000);
    }

    #[flame]
    pub fn load(db: String) -> Result<Self, search::SearchError> {
        let meta_data = MetaData::new(&db);
        let mut pers = Persistence { meta_data, db, ..Default::default() };
        pers.load_all_to_cache()?;
        pers.print_heap_sizes();
        Ok(pers)
    }

    #[flame]
    pub fn create(db: String) -> Result<Self, io::Error> {
        fs::create_dir_all(&db)?;
        let meta_data = MetaData { ..Default::default() };
        Ok(Persistence { meta_data, db, ..Default::default() })
    }

    #[flame]
    pub fn write_tuple_pair(&mut self, tuples: &mut Vec<create::ValIdPair>, path: &str) -> Result<(), io::Error> {
        let has_duplicates = has_valid_duplicates(&tuples.iter().map(|el| el as &create::GetValueId).collect());
        let data = valid_pair_to_parallel_arrays(tuples);
        // if data.values1.len() > 0 {
        //     trace!("data.values1 {:?} \n {:?}", path, data.values1 );
        //     trace!("data.values2 {:?} \n {:?}", path, data.values2 );
        // }
        // let has_duplicates = has_duplicates(&data.values1);

        //Indirect
        let indirect_file_path = util::get_file_path(&self.db, &(path.to_string() + ".indirect"));
        let data_file_path = util::get_file_path(&self.db, &(path.to_string() + ".data"));
        let store = IndexIdToMultipleParentIndirect::new(&data);
        File::create(indirect_file_path)?.write_all(&vec_to_bytes_u32(&store.start_and_end)).unwrap();
        File::create(data_file_path)?.write_all(&vec_to_bytes_u32(&store.data)).unwrap();

        //Parallel
        // let encoded: Vec<u8> = serialize(&data, Infinite).unwrap();
        // File::create(util::get_file_path(&self.db, &path.to_string()))?.write_all(&encoded)?;

        self.meta_data.key_value_stores.push(KVStoreMetaData {
            loading_type:       LoadingType::InMemory,
            persistence_type:   KVStoreType::IndexIdToMultipleParentIndirect,
            key_has_duplicates: has_duplicates,
            path:               path.to_string(),
        });
        Ok(())
    }
    #[flame]
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

    #[flame]
    pub fn write_meta_data(&self) -> Result<(), io::Error> {
        self.write_data("metaData.json", serde_json::to_string_pretty(&self.meta_data)?.as_bytes())
    }

    #[flame]
    pub fn write_data(&self, path: &str, data: &[u8]) -> Result<(), io::Error> {
        File::create(&get_file_path(&self.db, path))?.write_all(data)?;
        Ok(())
    }

    #[flame]
    pub fn get_buffered_writer(&self, path: &str) -> Result<io::BufWriter<fs::File>, io::Error> {
        Ok(io::BufWriter::new(File::create(&get_file_path(&self.db, path))?))
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
        self.write_index(&vec_to_bytes_u64(&offsets), &offsets, &(path.to_string() + ".offsets"))?;
        Ok(())
    }

    #[flame]
    pub fn get_offsets(&self, path: &str) -> Option<&Vec<u64>> {
        self
        .cache
        .index_64
        .get(&(path.to_string() + ".offsets"))
    }

    #[flame]
    pub fn get_valueid_to_parent(&self, path: &str) -> Result<&Box<IndexIdToParent>, search::SearchError> {
        self.cache.index_id_to_parento.get(path)
        .ok_or_else(|| From::from(format!("Did not found path in cache {:?}", path)))
    }

    #[flame]
    pub fn get_boost(&self, path: &str) -> Result<&Box<IndexIdToParent>, search::SearchError> {
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
        Ok(File::open(&get_file_path(&self.db, path)).map_err(|_err| {
            search::SearchError::StringError(format!("Could not open {:?}", path))
        })?)
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
        self.cache.fst.get(path).ok_or(From::from(format!("{} does not exist", path)))
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

        for el in &self.meta_data.key_value_stores {
            info_time!(format!("loaded key_value_store {:?}", &el.path));

            let mut loading_type = el.loading_type.clone();
            if let Some(val) = env::var_os("LoadingType") {
                loading_type = LoadingType::from_str(&val.into_string().unwrap())
                .map_err(|_err| search::SearchError::StringError("only InMemory or Disk allowed for LoadingType environment variable".to_string()))?;
            }

            match loading_type {
                LoadingType::InMemory => {
                    let store: Box<IndexIdToParent> = {

                        match el.persistence_type {
                            KVStoreType::ParallelArrays => {
                                let encoded = file_to_bytes(&get_file_path(&self.db, &el.path))?;
                                Box::new(deserialize::<ParallelArrays>(&encoded[..]).unwrap())
                            },
                            KVStoreType::IndexIdToMultipleParentIndirect => {
                                let indirect = file_to_bytes(&(get_file_path(&self.db, &el.path)+ ".indirect"))?;
                                let data = file_to_bytes(&(get_file_path(&self.db, &el.path)+ ".data"))?;
                                Box::new(IndexIdToMultipleParentIndirect::from_data(bytes_to_vec_u32(&indirect), bytes_to_vec_u32(&data)))

                                // let encoded = file_to_bytes(&get_file_path(&self.db, &el.path))?;
                                // Box::new(deserialize::<IndexIdToMultipleParentIndirect>(&encoded[..]).unwrap())
                            },
                        }

                    };

                    if el.key_has_duplicates {
                        // self.cache.index_id_to_parento.insert(el.path.to_string(), Box::new(IndexIdToMultipleParentCompressedSnappy::new(&store)));
                        self.cache
                            .index_id_to_parento
                            .insert(el.path.to_string(), Box::new(IndexIdToMultipleParentCompressedMaydaINDIRECTOne::new(&*store)));
                    } else {
                        self.cache
                            .index_id_to_parento
                            .insert(el.path.to_string(), Box::new(IndexIdToOneParentMayda::new(&*store)));
                    }

                },
                LoadingType::Disk => {

                    let start_and_end_file = self.get_file_handle(&(el.path.to_string()+ ".indirect"))?;
                    let data_file = self.get_file_handle(&(el.path.to_string()+ ".data"))?;
                    let data_metadata = self.get_file_metadata_handle(&(el.path.to_string()+ ".indirect"))?;
                    let store = PointingArrayFileReader { start_and_end_file, data_file, data_metadata };

                    // let store = PointingArrayFileReader { start_and_end_file: el.path.to_string()+ ".indirect", data_file: el.path.to_string()+ ".data", persistence: self.db.to_string()};
                    self.cache
                        .index_id_to_parento
                        .insert(el.path.to_string(), Box::new(store));

                },
            }


        }

        // Load Boost Indices
        for el in &self.meta_data.boost_stores {
            let encoded = file_to_bytes(&get_file_path(&self.db, &el.path))?;
            let store: ParallelArrays = deserialize(&encoded[..]).unwrap();
            self.cache.boost_valueid_to_value.insert(el.path.to_string(), Box::new(IndexIdToOneParentMayda::new(&store)));
        }

        // Load FST
        for (ref path, _) in &self.meta_data.fulltext_indices {
            let map = self.load_fst(path)?; // "Could not load FST"
            self.cache.fst.insert(path.to_string(), map);
        }
        Ok(())
    }

    #[flame]
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

    pub fn get_text_for_id<'a>(&mut self, pos: usize, offsets: &Vec<u64>) -> String {
        self.load_text(pos, offsets);
        str::from_utf8(&self.buffer).unwrap().to_string() // TODO maybe avoid clone
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
            // info!("Comparing {:?}", str::from_utf8(&buffer).unwrap());
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
