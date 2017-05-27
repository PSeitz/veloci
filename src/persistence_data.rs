use std::fs::{File};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::cmp::Ordering;

use persistence::Persistence;
use persistence;
use create;

use heapsize::{HeapSizeOf, heap_size_of};
use bincode::{serialize, deserialize, Infinite};

use util;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PointingArrays {
    arr1: Vec<u64>, // offset
    arr2: Vec<u8>
}

fn parrallel_arrays_to_pointing_array(keys: Vec<u32>, values: Vec<u32>) -> PointingArrays {
    let mut valids = keys.clone();
    valids.dedup();
    let mut arr1 = vec![];
    let mut arr2 = vec![];
    if valids.len() == 0 { return PointingArrays{arr1, arr2}; }

    let store = ParallelArrays { values1: keys.clone(), values2: values.clone() };
    let mut offset = 0;
    for valid in valids {
        let mut vals = store.get_values(valid);
        vals.sort();
        let data = persistence::vec_to_bytes_u32(&vals); // @Temporary Add Compression
        arr1.push(offset);
        arr2.extend(data.iter().cloned());
        offset += data.len() as u64;
    }
    arr1.push(offset);
    PointingArrays{arr1, arr2}
}

#[derive(Debug)]
pub struct PointingArrayFileReader<'a> {
    pub path1: String,
    pub path2: String,
    pub persistence:&'a Persistence
}

impl<'a> PointingArrayFileReader<'a> {
    fn new(key:&(String, String), persistence:&'a Persistence) -> Self {
        PointingArrayFileReader { path1: key.0.clone(), path2: key.1.clone(), persistence }
    }
    fn get_values(&self, find: u32) -> Vec<u32> {
        let mut data:Vec<u8> = Vec::with_capacity(8);
        let mut file = self.persistence.get_file_handle(&self.path1).unwrap();// -> Result<File, io::Error>
        load_bytes(&mut data, &mut file, find as u64 *8);


        let mut result = Vec::new();
        // match self.values1.binary_search(&find) {
        //     Ok(mut pos) => {
        //         //this is not a lower_bounds search so we MUST move to the first hit
        //         while pos != 0 && self.values1[pos - 1] == find {pos-=1;}
        //         let val_len = self.values1.len();
        //         while pos < val_len && self.values1[pos] == find{
        //             result.push(self.values2[pos]);
        //             pos+=1;
        //         }
        //     },Err(_) => {},
        // }
        result
    }
}
impl<'a> HeapSizeOf for PointingArrayFileReader<'a> {
    fn heap_size_of_children(&self) -> usize{self.path1.heap_size_of_children() + self.path2.heap_size_of_children() }
}


//                                                                  ParallelArrays

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ParallelArrays {
    pub values1: Vec<u32>,
    pub values2: Vec<u32>
}

impl ParallelArrays {
    pub fn new(key:&(String, String)) -> Self {
        ParallelArrays { values1: persistence::load_index_u32(&key.0).unwrap(), values2: persistence::load_index_u32(&key.1).unwrap() }
    }
    pub fn get_values(&self, find: u32) -> Vec<u32> {
        let mut result = Vec::new();
        match self.values1.binary_search(&find) {
            Ok(mut pos) => {
                //this is not a lower_bounds search so we MUST move to the first hit
                while pos != 0 && self.values1[pos - 1] == find {pos-=1;}
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

pub fn parrallel_arrays_to_array_of_array(store: &ParallelArrays) -> Vec<Vec<u32>> {
    let mut data = vec![];
    let mut valids = store.values1.clone();
    valids.dedup();
    if valids.len() == 0 { return data; }
    data.resize(*valids.last().unwrap() as usize + 1, vec![]);

    infoTime!("create insert key_value_store");
    for valid in valids {
        let mut vals = store.get_values(valid);
        vals.sort();
        data[valid as usize] = vals;
    }
    data
}

pub fn valid_pair_to_parallel_arrays(tuples: &mut Vec<create::ValIdPair>) -> ParallelArrays {
    tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
    let valids = tuples.iter().map(|ref el| el.valid      ).collect::<Vec<_>>();
    let parent_val_ids = tuples.iter().map(|ref el| el.parent_val_id).collect::<Vec<_>>();
    ParallelArrays{values1:valids, values2:parent_val_ids}
}

pub fn boost_pair_to_parallel_arrays(tuples: &mut Vec<create::ValIdToValue>) -> ParallelArrays {
    tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
    let valids = tuples.iter().map(|ref el| el.valid      ).collect::<Vec<_>>();
    let values = tuples.iter().map(|ref el| el.value).collect::<Vec<_>>();
    ParallelArrays{values1:valids, values2:values}
}


#[test]
fn test_index_parrallel_arrays() {
    let ix = ParallelArrays{values1: vec![0,0,1], values2: vec![0,1,2]};
    assert_eq!(ix.get_values(0), vec![0,1]);
}



fn load_bytes(buffer:&mut Vec<u8>, file:&mut File, offset:u64) { // @Temporary Use Result
    // let string_size = offsets[pos+1] - offsets[pos] - 1;
    // let mut buffer:Vec<u8> = Vec::with_capacity(string_size as usize);
    // unsafe { buffer.set_len(string_size as usize); }
    // buffer.resize(string_size as usize, 0);
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read_exact(buffer).unwrap();
    // unsafe {str::from_utf8_unchecked(&buffer)}
    // let s = unsafe {str::from_utf8_unchecked(&buffer)};
    // str::from_utf8(&buffer).unwrap() // @Temporary  -> use unchecked if stable
}
