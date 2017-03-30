use std::fs::{self, File};
use std::io::prelude::*;
#[allow(unused_imports)]
use std::io::{self, BufRead};
#[allow(unused_imports)]
use std::time::Duration;

#[allow(unused_imports)]
use futures::sync::{oneshot, mpsc};
#[allow(unused_imports)]
use std::thread;
#[allow(unused_imports)]
use std::sync::mpsc::sync_channel;

#[allow(unused_imports)]
use std::io::SeekFrom;
use util;
use util::get_file_path;
use fnv::FnvHashSet;

#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::cmp::Ordering;

use serde_json;
use serde_json::Value;

#[allow(unused_imports)]
use std::env;
use fnv::FnvHashMap;

use std::str;
use abomonation::{encode, decode};

#[derive(Serialize, Deserialize, Debug)]
pub struct MetaData {
    pub id_lists: FnvHashMap<String, IDList>
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
	metaData: MetaData
}

//TODO Move everything with getFilepath to persistence
// use persistence object with folder and metadata
// move cache here

pub fn load_index_64(s1: &str) -> Result<(Vec<u64>), io::Error> {
    info!("Loading Index64 {} ", s1);
    let mut f = File::open(s1)?;
    let mut buffer: Vec<u8> = Vec::new();
    f.read_to_end(&mut buffer)?;
    buffer.shrink_to_fit();
    // let buf_len = buffer.len();

    // let mut read: Vec<u64> = unsafe { mem::transmute(buffer) };
    // unsafe { read.set_len(buf_len/8); }
    // info!("Loaded Index64 {} With size {:?}",s1,  read.len());
    // Ok(read)

    if let Some((result, remaining)) = unsafe { decode::<Vec<u64>>(&mut buffer) } {
        assert!(remaining.len() == 0);
        Ok(result.clone())
    }else{
        panic!("Could no load Vector");
    }

}



pub fn load_index(s1: &str) -> Result<(Vec<u32>), io::Error> {
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
use std;
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

pub fn write_index(data:&Vec<u32>, path:&str, metadata: &mut MetaData) -> Result<(), io::Error> {

    let mut bytes:Vec<u8> = Vec::new();
    unsafe { encode(data, &mut bytes); }
    File::create(path)?.write_all(&bytes)?;
    // unsafe { File::create(path)?.write_all(typed_to_bytes(data))?; }
    info!("Wrote Index32 {} With size {:?}", path, data.len());
    trace!("{:?}", data);
    metadata.id_lists.insert(path.to_string(), IDList{path: path.to_string(), size: data.len() as u64, id_type: IDDataType::U32, doc_id_type:check_is_docid_type32(&data)});
    Ok(())
}

pub fn write_index64(data:&Vec<u64>, path:&str, metadata: &mut MetaData) -> Result<(), io::Error> {
    let mut bytes:Vec<u8> = Vec::new();
    unsafe { encode(data, &mut bytes); }
    File::create(path)?.write_all(&bytes)?;

    // unsafe { File::create(path)?.write_all(typed_to_bytes(data))?; }
    info!("Wrote Index64 {} With size {:?}", path, data.len());
    trace!("{:?}", data);
    metadata.id_lists.insert(path.to_string(), IDList{path: path.to_string(), size: data.len() as u64, id_type: IDDataType::U64, doc_id_type:check_is_docid_type64(&data)});
    Ok(())
}