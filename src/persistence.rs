
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

#[derive(Debug)]
struct Persistence {
	db: String, // folder
	meta_data: MetaData
}

//TODO Move everything with getFilepath to persistence
// use persistence object with folder and metadata
// move cache here

lazy_static! {
    pub static ref INDEX_64_CACHE: RwLock<HashMap<String, Vec<u64>>> = RwLock::new(HashMap::new());
    pub static ref INDEX_32_CACHE: RwLock<HashMap<String, Vec<u32>>> = RwLock::new(HashMap::new());
    pub static ref INDEX_ID_TO_PARENT: RwLock<HashMap<(String,String), Vec<Vec<u32>>>> = RwLock::new(HashMap::new()); // attr -> [[1,2], [22]]
}
// use search;
pub fn load_all(meta_data: &MetaData) -> Result<(), io::Error> {
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
            &IDDataType::U32 => load_index_into_cache(&idlist.path)?,
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


pub fn load_index(s1: &str) -> Result<Vec<u32>, io::Error> {
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

use create;

pub fn write_tuple_pair(tuples: &mut Vec<create::ValIdPair>, path_valid: String, path_parentid:String, meta_data: &mut MetaData) -> Result<(), io::Error> {

    tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
    // let path_name = util::get_path_name(attr_name, is_text_index);
    // trace!("\nValueIdToParent {:?}: {}", path_name, print_vec(&tuples));
    write_index(&tuples.iter().map(|ref el| el.valid      ).collect::<Vec<_>>(),   &path_valid, meta_data)?;
    write_index(&tuples.iter().map(|ref el| el.parent_val_id).collect::<Vec<_>>(), &path_parentid, meta_data)?;

    meta_data.key_value_stores.push((path_valid, path_parentid));
    Ok(())

}

pub fn write_index(data:&Vec<u32>, path:&str, meta_data: &mut MetaData) -> Result<(), io::Error> {

    let mut bytes:Vec<u8> = Vec::new();
    unsafe { encode(data, &mut bytes); }
    File::create(path)?.write_all(&bytes)?;
    // unsafe { File::create(path)?.write_all(typed_to_bytes(data))?; }
    info!("Wrote Index32 {} With size {:?}", path, data.len());
    trace!("{:?}", data);
    meta_data.id_lists.insert(path.to_string(), IDList{path: path.to_string(), size: data.len() as u64, id_type: IDDataType::U32, doc_id_type:check_is_docid_type32(&data)});
    Ok(())
}

pub fn write_index64(data:&Vec<u64>, path:&str, meta_data: &mut MetaData) -> Result<(), io::Error> {
    let mut bytes:Vec<u8> = Vec::new();
    unsafe { encode(data, &mut bytes); }
    File::create(path)?.write_all(&bytes)?;

    // unsafe { File::create(path)?.write_all(typed_to_bytes(data))?; }
    info!("Wrote Index64 {} With size {:?}", path, data.len());
    trace!("{:?}", data);
    meta_data.id_lists.insert(path.to_string(), IDList{path: path.to_string(), size: data.len() as u64, id_type: IDDataType::U64, doc_id_type:check_is_docid_type64(&data)});
    Ok(())
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








