
#[allow(unused_imports)]
use std::fs::{self, File};
use std::io::prelude::*;
#[allow(unused_imports)]
use std::io::{self, BufRead};
#[allow(unused_imports)]
use std::time::Duration;

#[allow(unused_imports)]
use std::thread;
#[allow(unused_imports)]
use std::sync::mpsc::sync_channel;

#[allow(unused_imports)]
use std::io::SeekFrom;
use util;
#[allow(unused_imports)]
use util::get_file_path;
use util::get_file_path_2;
#[allow(unused_imports)]
use fnv::FnvHashSet;

#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::cmp::Ordering;

use serde_json;
#[allow(unused_imports)]
use serde_json::Value;

#[allow(unused_imports)]
use std::env;
use fnv::FnvHashMap;

use std::str;
use abomonation::{encode, decode};

use std::sync::RwLock;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct MetaData {
    pub id_lists: FnvHashMap<String, IDList>,
    pub key_value_stores: Vec<(String, String)>
}

use create;

impl MetaData {
    pub fn new(path: &str) -> MetaData {
        let json = util::file_as_string(&(path.to_string()+"/metaData")).unwrap();
        serde_json::from_str(&json).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IDList {
    pub path: String,
    pub size: u64,
    pub id_type: IDDataType,
    pub doc_id_type:bool
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum IDDataType {
    U32,
    U64,
}

//TODO Move everything with getFilepath to persistence
// use persistence object with folder and metadata
// move cache here

use search;
use search::SearchError;
#[derive(Debug, Clone, Default)]
pub struct Persistence {
    db: String, // folder
    meta_data: MetaData,
    pub index_id_to_parent: HashMap<(String,String), Vec<Vec<u32>>>,
    pub index_64: HashMap<String, Vec<u64>>,
    pub index_32: HashMap<String, Vec<u32>>
}
impl Persistence {
    pub fn load(db: String) -> Result<Self, io::Error> {
        let meta_data = MetaData::new(&db);
        let mut pers = Persistence{meta_data, db, index_id_to_parent:HashMap::default(), ..Default::default()};
        pers.load_all()?;
        Ok(pers)
    }

    pub fn create(db: String) -> Result<Self, io::Error>  {
        fs::create_dir_all(&db)?;
        let meta_data = MetaData {key_value_stores:vec![], id_lists: FnvHashMap::default()};
        Ok(Persistence{meta_data, db, index_id_to_parent:HashMap::default(), ..Default::default()})
    }

    pub fn write_tuple_pair(&mut self, tuples: &mut Vec<create::ValIdPair>, path_valid: String, path_parentid:String) -> Result<(), io::Error> {
        tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
        self.write_index(&tuples.iter().map(|ref el| el.valid      ).collect::<Vec<_>>(),   &path_valid)?;
        self.write_index(&tuples.iter().map(|ref el| el.parent_val_id).collect::<Vec<_>>(), &path_parentid)?;
        self.meta_data.key_value_stores.push((path_valid, path_parentid));
        Ok(())
    }

    pub fn write_index(&mut self, data:&Vec<u32>, path:&str) -> Result<(), io::Error> {
        let mut bytes:Vec<u8> = Vec::new();
        unsafe { encode(data, &mut bytes); }
        File::create(util::get_file_path_2(&self.db, path))?.write_all(&bytes)?;
        // unsafe { File::create(path)?.write_all(typed_to_bytes(data))?; }
        info!("Wrote Index32 {} With size {:?}", path, data.len());
        trace!("{:?}", data);
        self.meta_data.id_lists.insert(path.to_string(), IDList{path: path.to_string(), size: data.len() as u64, id_type: IDDataType::U32, doc_id_type:check_is_docid_type32(&data)});
        Ok(())
    }

    pub fn write_meta_data(&self) -> Result<(), io::Error> {
        let meta_data_str = serde_json::to_string_pretty(&self.meta_data).unwrap();
        let mut buffer = File::create(&get_file_path_2(&self.db, "metaData"))?;
        buffer.write_all(&meta_data_str.as_bytes())?;
        Ok(())
    }

    pub fn write_data(&self, path: &str, data:&[u8]) -> Result<(), io::Error> {
        File::create(&get_file_path_2(&self.db, path))?.write_all(data)?;
        Ok(())
    }

    pub fn write_index64(&mut self, data:&Vec<u64>, path:&str) -> Result<(), io::Error> {
        let mut bytes:Vec<u8> = Vec::new();
        unsafe { encode(data, &mut bytes); }
        File::create(util::get_file_path_2(&self.db, path))?.write_all(&bytes)?;

        // unsafe { File::create(path)?.write_all(typed_to_bytes(data))?; }
        info!("Wrote Index64 {} With size {:?}", path, data.len());
        trace!("{:?}", data);
        self.meta_data.id_lists.insert(path.to_string(), IDList{path: path.to_string(), size: data.len() as u64, id_type: IDDataType::U64, doc_id_type:check_is_docid_type64(&data)});
        Ok(())
    }

    pub fn write_json_to_disk(&mut self, arro: &Vec<Value>, path:&str) -> Result<(), io::Error> {
        let mut offsets = vec![];
        let mut buffer = File::create(&get_file_path_2(&self.db, &path))?;
        let mut current_offset = 0;
        // let arro = data.as_array().unwrap();
        for el in arro {
            let el_str = el.to_string().into_bytes();
            buffer.write_all(&el_str)?;
            offsets.push(current_offset as u64);
            current_offset += el_str.len();
        }
        // println!("json offsets: {:?}", offsets);
        self.write_index64(&offsets, &(path.to_string()+".offsets"))?;
        Ok(())
    }

    pub fn get_file_access(&self, path: &str) -> FileSearch{
        FileSearch::new(&get_file_path_2(&self.db, path))
    }

    pub fn get_file_handle(&self, path: &str) -> Result<File, io::Error> {
        Ok(File::open(&get_file_path_2(&self.db, path))?)
    }

    pub fn get_create_char_offset_info(&self, path: &str,character: &str) -> Result<Option<OffsetInfo>, search::SearchError> { // @Temporary - replace SearchError
        let char_offset = CharOffset::new(&get_file_path_2(&self.db, path))?;
        return Ok(char_offset.get_char_offset_info(character).ok());
    }

    pub fn load_all(&mut self) -> Result<(), io::Error> {
        println!("{:?}", self.meta_data);
        let mut all_tuple_paths = vec![];
        for &(ref valid, ref parentid) in &self.meta_data.key_value_stores {
            all_tuple_paths.push(valid.to_string());
            all_tuple_paths.push(parentid.to_string());
        }

        for (_, ref idlist) in &self.meta_data.id_lists.clone() {
            if all_tuple_paths.contains(&idlist.path) {
                continue;
            }
            match &idlist.id_type {
                &IDDataType::U32 => self.load_index_32(&idlist.path).expect(&("Could not load ".to_string() + &idlist.path)),
                &IDDataType::U64 => self.load_index_64(&idlist.path)?
            }
        }

        for &(ref valid, ref parentid) in &self.meta_data.key_value_stores {
            infoTime!("create key_value_store");
            let mut data = vec![];
            let mut valids = load_index(&get_file_path_2(&self.db, valid)).unwrap();
            valids.dedup();
            if valids.len() == 0 { continue; }
            data.resize(*valids.last().unwrap() as usize + 1, vec![]);

            let store = IndexKeyValueStore::new(&(get_file_path_2(&self.db, valid), get_file_path_2(&self.db, parentid)));
            infoTime!("create insert key_value_store");
            for valid in valids {
                data[valid as usize] = store.get_values(valid);
            }

            self.index_id_to_parent.insert((valid.clone(), parentid.clone()), data);

        }

        Ok(())
    }

    pub fn load_index_64(&mut self, s1: &str) -> Result<(), io::Error> {
        if self.index_64.contains_key(s1){return Ok(()); }
        self.index_64.insert(s1.to_string(), load_index_64(&get_file_path_2(&self.db, s1))?);
        Ok(())
    }
    pub fn load_index_32(&mut self, s1: &str) -> Result<(), io::Error> {
        if self.index_32.contains_key(s1){return Ok(()); }
        self.index_32.insert(s1.to_string(), load_index(&get_file_path_2(&self.db, s1))?);
        Ok(())
    }
}


#[derive(Debug)]
pub struct OffsetInfo {
    pub byte_range_start: u64,
    pub byte_range_end: u64,
    pub line_offset: u64,
}

#[derive(Debug)]
pub struct CharOffset {
    path: String,
    chars: Vec<String>,
    // byte_offsets_start: Vec<u64>,
    // byte_offsets_end: Vec<u64>,
    // line_offsets: Vec<u64>,
}


impl CharOffset {
    fn new(path:&str) -> Result<CharOffset, SearchError> {
        load_index_64_into_cache(&(path.to_string()+".char_offsets.byteOffsetsStart"))?;
        load_index_64_into_cache(&(path.to_string()+".char_offsets.byteOffsetsEnd"))?;
        load_index_64_into_cache(&(path.to_string()+".char_offsets.lineOffset"))?;
        let char_offset = CharOffset {
            path: path.to_string(),
            chars: util::file_as_string(&(path.to_string()+".char_offsets.chars"))?.lines().collect::<Vec<_>>().iter().map(|el| el.to_string()).collect(), // @Cleanup // @Temporary  sinlge  collect
            // byte_offsets_start: load_index_64_into_cache(&(path.to_string()+".char_offsets.byteOffsetsStart"))?,
            // byte_offsets_end: load_index_64_into_cache(&(path.to_string()+".char_offsets.byteOffsetsEnd"))?,
            // line_offsets: load_index_64_into_cache(&(path.to_string()+".char_offsets.lineOffset"))?
        };
        trace!("Loaded CharOffset:{} ", path );
        trace!("{:?}", char_offset);
        Ok(char_offset)
    }
    pub fn get_char_offset_info(&self,character: &str) -> Result<OffsetInfo, usize>{
        match self.chars.binary_search(&character.to_string()) {
            Ok(index) => Ok(self.get_offset_info(index)),
            Err(nearest_index) => Ok(self.get_offset_info(nearest_index-1)),
        }
        // let char_index = self.chars.binary_search(&character.to_string()).unwrap(); // .unwrap() -> find closest offset
        // Ok(self.get_offset_info(char_index))
        // self.chars.binary_search(&character) { Ok(char_index) => this.get_offset_info(char_index),Err(_) => };
    }
    pub fn get_offset_info(&self, index: usize) -> OffsetInfo {
        let cache_lock = INDEX_64_CACHE.read().unwrap();
        let byte_offsets_start = cache_lock.get(&(self.path.to_string()+".char_offsets.byteOffsetsStart")).unwrap();
        let byte_offsets_end =   cache_lock.get(&(self.path.to_string()+".char_offsets.byteOffsetsEnd")).unwrap();
        let line_offsets =       cache_lock.get(&(self.path.to_string()+".char_offsets.lineOffset")).unwrap();

        trace!("get_offset_info path:{}\tindex:{}\toffsetSize: {}", self.path, index, byte_offsets_start.len());
        return OffsetInfo{byte_range_start: byte_offsets_start[index], byte_range_end: byte_offsets_end[index], line_offset: line_offsets[index]};
    }

}

#[derive(Debug)]
pub struct FileSearch {
    path: String,
    // offsets: Vec<u64>,
    file: File,
    buffer: Vec<u8>
}


impl FileSearch {

    fn new(path: &str) -> Self {
        load_index_64_into_cache(&(path.to_string()+".offsets")).unwrap();
        FileSearch{path:path.to_string(), file: File::open(path).unwrap(), buffer: Vec::with_capacity(50 as usize)}
    }

    fn load_text<'a>(&mut self, pos: usize, offsets:&Vec<u64>) { // @Temporary Use Result
        let string_size = offsets[pos+1] - offsets[pos] - 1;
        // let mut buffer:Vec<u8> = Vec::with_capacity(string_size as usize);
        // unsafe { buffer.set_len(string_size as usize); }
        self.buffer.resize(string_size as usize, 0);
        self.file.seek(SeekFrom::Start(offsets[pos])).unwrap();
        self.file.read_exact(&mut self.buffer).unwrap();
        // unsafe {str::from_utf8_unchecked(&buffer)}
        // let s = unsafe {str::from_utf8_unchecked(&buffer)};
        // str::from_utf8(&buffer).unwrap() // @Temporary  -> use unchecked if stable
    }

    pub fn binary_search(&mut self, term: &str) -> Result<(String, i64), io::Error> {
        let cache_lock = INDEX_64_CACHE.read().unwrap();
        let offsets = cache_lock.get(&(self.path.to_string()+".offsets")).unwrap();
        debugTime!("term binary_search");
        if offsets.len() < 2  {
            return Ok(("".to_string(), -1));
        }
        // let mut buffer:Vec<u8> = Vec::with_capacity(50 as usize);
        // let mut f = File::open(&self.path)?;
        let mut low = 0;
        let mut high = offsets.len() - 2;
        let mut i;
        while low <= high {
            i = (low + high) >> 1;
            self.load_text(i, offsets);
            // println!("Comparing {:?}", str::from_utf8(&buffer).unwrap());
        // comparison = comparator(arr[i], find);
            if str::from_utf8(&self.buffer).unwrap() < term { low = i + 1; continue }
            if str::from_utf8(&self.buffer).unwrap() > term { high = i - 1; continue }
            return Ok((str::from_utf8(&self.buffer).unwrap().to_string(), i as i64))
        }
        Ok(("".to_string(), -1))
    }
}



lazy_static! {
    pub static ref INDEX_64_CACHE: RwLock<HashMap<String, Vec<u64>>> = RwLock::new(HashMap::new());
    pub static ref INDEX_32_CACHE: RwLock<HashMap<String, Vec<u32>>> = RwLock::new(HashMap::new());
    pub static ref INDEX_ID_TO_PARENT: RwLock<HashMap<(String,String), Vec<Vec<u32>>>> = RwLock::new(HashMap::new()); // attr -> [[1,2], [22]]
}


fn load_index_64_into_cache(s1: &str) -> Result<(), io::Error> {

    {
        let cache = INDEX_64_CACHE.read().unwrap();
        if cache.contains_key(s1){
            return Ok(());
        }
    }
    {
        info!("Loading Index64 {} ", s1);
        let mut f = File::open(s1)?;
        let mut buffer: Vec<u8> = Vec::new();
        f.read_to_end(&mut buffer)?;
        buffer.shrink_to_fit();

        if let Some((result, remaining)) = unsafe { decode::<Vec<u64>>(&mut buffer) } {
            assert!(remaining.len() == 0);
            // Ok(result.clone())
            let mut cache = INDEX_64_CACHE.write().unwrap();
            cache.insert(s1.to_string(), result.clone());
        }else{
            panic!("Could no load Vector");
        }

        Ok(())

    }

}

fn load_index_into_cache(s1: &str) -> Result<(), io::Error> {

    {
        let cache = INDEX_32_CACHE.read().unwrap();
        if cache.contains_key(s1){
            return Ok(());
        }
    }
    {
        info!("Loading Index32 {} ", s1);
        let mut f = File::open(s1)?;
        let mut buffer: Vec<u8> = Vec::new();
        f.read_to_end(&mut buffer)?;
        buffer.shrink_to_fit();

        if let Some((result, remaining)) = unsafe { decode::<Vec<u32>>(&mut buffer) } {
            assert!(remaining.len() == 0);
            // Ok(result.clone())
            let mut cache = INDEX_32_CACHE.write().unwrap();
            cache.insert(s1.to_string(), result.clone());
        }else{
            panic!("Could no load Vector");
        }

        Ok(())

    }


}

fn load_index_64(s1: &str) -> Result<Vec<u64>, io::Error> {
    info!("Loading Index32 {} ", s1);
    let mut f = File::open(s1)?;
    let mut buffer: Vec<u8> = Vec::new();
    f.read_to_end(&mut buffer)?;
    buffer.shrink_to_fit();
    // let buf_len = buffer.len();

    if let Some((result, remaining)) = unsafe { decode::<Vec<u64>>(&mut buffer) } {
        assert!(remaining.len() == 0);
        Ok(result.clone())
    }else{
        panic!("Could no load Vector");
    }
}


fn load_index(s1: &str) -> Result<Vec<u32>, io::Error> {
    info!("Loading Index32 {} ", s1);
    let mut f = File::open(s1)?;
    let mut buffer: Vec<u8> = Vec::new();
    f.read_to_end(&mut buffer)?;
    buffer.shrink_to_fit();
    // let buf_len = buffer.len();

    if let Some((result, remaining)) = unsafe { decode::<Vec<u32>>(&mut buffer) } {
        assert!(remaining.len() == 0);
        Ok(result.clone())
    }else{
        panic!("Could no load Vector");
    }
}


// fn check_is_docid_type<T: std::cmp::PartialEq>(data: &Vec<T>) -> bool {
//     for (index, value_id) in data.iter().enumerate(){
//         if *value_id as usize != index  {
//             return false
//         }
//     }
//     return true
// }

fn check_is_docid_type32(data: &Vec<u32>) -> bool {
    for (index, value_id) in data.iter().enumerate(){
        if *value_id as usize != index  {
            return false
        }
    }
    return true
}

fn check_is_docid_type64(data: &Vec<u64>) -> bool {
    for (index, value_id) in data.iter().enumerate(){
        if *value_id as usize != index  {
            return false
        }
    }
    return true
}





#[derive(Debug)]
struct IndexKeyValueStore {
    values1: Vec<u32>,
    values2: Vec<u32>,
}

impl IndexKeyValueStore {
    fn new(key:&(String, String)) -> Self {
        IndexKeyValueStore { values1: load_index(&key.0).unwrap(), values2: load_index(&key.1).unwrap() }
    }
    fn get_value(&self, find: u32) -> Option<u32> {
        match self.values1.binary_search(&find) {
            Ok(pos) => { Some(self.values2[pos]) },
            Err(_) => {None},
        }
    }
    fn get_values(&self, find: u32) -> Vec<u32> {
        let mut result = Vec::new();
        match self.values1.binary_search(&find) {
            Ok(mut pos) => {
                let val_len = self.values1.len();
                while pos < val_len && self.values1[pos] == find{
                    result.push(self.values2[pos]);
                    pos+=1;
                }
            },Err(_) => {},
        }
        result
    }
}








