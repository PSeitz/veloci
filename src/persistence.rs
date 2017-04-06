
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

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Serialize, Deserialize, Debug)]
pub struct IDList {
    pub path: String,
    pub size: u64,
    pub id_type: IDDataType,
    pub doc_id_type:bool
}

#[derive(Serialize, Deserialize, Debug)]
pub enum IDDataType {
    U32,
    U64,
}

//TODO Move everything with getFilepath to persistence
// use persistence object with folder and metadata
// move cache here


#[derive(Debug)]
pub struct Persistence {
    db: String, // folder
    meta_data: MetaData
}
impl Persistence {
    pub fn load(db: String) -> Result<Self, io::Error> {
        let meta_data = MetaData::new(&db);
        load_all(&meta_data)?;
        Ok(Persistence{meta_data, db})
    }

    pub fn create(db: String) -> Result<Self, io::Error>  {
        fs::create_dir_all(&db)?;
        let meta_data = MetaData {key_value_stores:vec![], id_lists: FnvHashMap::default()};
        Ok(Persistence{meta_data, db})
    }


    pub fn write_tuple_pair(&mut self, tuples: &mut Vec<create::ValIdPair>, path_valid: String, path_parentid:String) -> Result<(), io::Error> {

        tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
        // let path_name = util::get_path_name(attr_name, is_text_index);
        // trace!("\nValueIdToParent {:?}: {}", path_name, print_vec(&tuples));
        self.write_index(&tuples.iter().map(|ref el| el.valid      ).collect::<Vec<_>>(),   &path_valid)?;
        self.write_index(&tuples.iter().map(|ref el| el.parent_val_id).collect::<Vec<_>>(), &path_parentid)?;

        self.meta_data.key_value_stores.push((util::get_file_path_2(&self.db, &path_valid), util::get_file_path_2(&self.db, &path_parentid)));
        Ok(())

    }

    pub fn write_index(&mut self, data:&Vec<u32>, path:&str) -> Result<(), io::Error> {

        let mut bytes:Vec<u8> = Vec::new();
        unsafe { encode(data, &mut bytes); }
        File::create(util::get_file_path_2(&self.db, path))?.write_all(&bytes)?;
        // unsafe { File::create(path)?.write_all(typed_to_bytes(data))?; }
        info!("Wrote Index32 {} With size {:?}", path, data.len());
        trace!("{:?}", data);
        self.meta_data.id_lists.insert(util::get_file_path_2(&self.db, path), IDList{path: util::get_file_path_2(&self.db, path), size: data.len() as u64, id_type: IDDataType::U32, doc_id_type:check_is_docid_type32(&data)});
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
        self.meta_data.id_lists.insert(util::get_file_path_2(&self.db, path), IDList{path: util::get_file_path_2(&self.db, path), size: data.len() as u64, id_type: IDDataType::U64, doc_id_type:check_is_docid_type64(&data)});
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


}

lazy_static! {
    pub static ref INDEX_64_CACHE: RwLock<HashMap<String, Vec<u64>>> = RwLock::new(HashMap::new());
    pub static ref INDEX_32_CACHE: RwLock<HashMap<String, Vec<u32>>> = RwLock::new(HashMap::new());
    pub static ref INDEX_ID_TO_PARENT: RwLock<HashMap<(String,String), Vec<Vec<u32>>>> = RwLock::new(HashMap::new()); // attr -> [[1,2], [22]]
}

pub fn load_all(meta_data: &MetaData) -> Result<(), io::Error> {
    println!("{:?}", meta_data);
    let mut all_tuple_paths = vec![];
    for &(ref valid, ref parentid) in &meta_data.key_value_stores {
        all_tuple_paths.push(valid.to_string());
        all_tuple_paths.push(parentid.to_string());
    }

    for (_, ref idlist) in &meta_data.id_lists {
        if all_tuple_paths.contains(&idlist.path) {
            continue;
        }
        match &idlist.id_type {
            &IDDataType::U32 => load_index_into_cache(&idlist.path).expect(&("Could not load ".to_string() + &idlist.path)),
            &IDDataType::U64 => load_index_64(&idlist.path)?
        }
    }

    for &(ref valid, ref parentid) in &meta_data.key_value_stores {
        infoTime!("create key_value_store");
        let mut data = vec![];
        let mut valids = load_index(valid).unwrap();
        valids.dedup();
        if valids.len() == 0 { continue; }
        data.resize(*valids.last().unwrap() as usize + 1, vec![]);

        let store = IndexKeyValueStore::new(&(valid.clone(), parentid.clone()));
        infoTime!("create insert key_value_store");
        for valid in valids {
            data[valid as usize] = store.get_values(valid);
        }

        let mut cache = INDEX_ID_TO_PARENT.write().unwrap();
        cache.insert((valid.clone(), parentid.clone()), data);

    }

    Ok(())
}

pub fn load_index_64(s1: &str) -> Result<(), io::Error> {

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

pub fn load_index_into_cache(s1: &str) -> Result<(), io::Error> {

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








