use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::cmp::Ordering;

#[allow(unused_imports)]
use heapsize::{heap_size_of, HeapSizeOf};
#[allow(unused_imports)]
use bincode::{deserialize, serialize, Infinite};

use persistence::Persistence;
use persistence::IndexIdToParent;
use persistence::IndexIdToOneParent;
use persistence::IndexIdToMultipleParent;
use persistence::IndexIdToMultipleParentCompressedSnappy;
use persistence::IndexIdToMultipleParentCompressedMayda;
use persistence;
use create;



pub trait TypeInfo: Sync + Send  {
    fn type_name(&self) -> String;
    fn type_of(&self) -> String;
}

macro_rules! impl_type_info {
    ($($name:ident$(<$($T:ident),+>)*),*) => {
        $(impl_type_info_single!($name$(<$($T),*>)*);)*
    };
}

macro_rules! mut_if {
    ($name:ident = $value:expr, $($any:expr)+) => (let mut $name = $value;);
    ($name:ident = $value:expr,) => (let $name = $value;);
}

macro_rules! impl_type_info_single {
    ($name:ident$(<$($T:ident),+>)*) => {
        impl$(<$($T: TypeInfo),*>)* TypeInfo for $name$(<$($T),*>)* {
            fn type_name(&self) -> String {
                mut_if!(res = String::from(stringify!($name)), $($($T)*)*);
                $(
                    res.push('<');
                    $(
                        res.push_str(&$T::type_name(&self));
                        res.push(',');
                    )*
                    res.pop();
                    res.push('>');
                )*
                res
            }
            fn type_of(&self) -> String {
                $name$(::<$($T),*>)*::type_name(&self)
            }
        }
    }
}

impl_type_info!(PointingArrays, ParallelArrays, IndexIdToOneParent, IndexIdToMultipleParent, IndexIdToMultipleParentCompressedSnappy, IndexIdToMultipleParentCompressedMayda);


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PointingArrays {
    arr1:         Vec<u64>, // offset
    arr2:         Vec<u8>,
    indirect_ids: Vec<u32>,
}

// impl PointingArrays {
//     pub fn get_values(&self, find: u32) -> Vec<u32> {
//         let ref bytes = self.arr2[self.arr1[find as usize] as usize..self.arr1[find as usize+1] as usize];
//         persistence::bytes_to_vec_u32(bytes)
//     }
// }

impl IndexIdToParent for PointingArrays {
    fn get_values(&self, id: u64) -> Option<Vec<u32>> {
        self.indirect_ids.get(id as usize).map(|pos| {
            let ref bytes = self.arr2[self.arr1[*pos as usize] as usize..self.arr1[*pos as usize + 1] as usize];
            persistence::bytes_to_vec_u32(bytes)
        })
        // let pos = self.indirect_ids[id as usize] as usize;
        // let ref bytes = self.arr2[self.arr1[pos] as usize..self.arr1[pos+1] as usize];
        // Some(persistence::bytes_to_vec_u32(bytes))
    }
    fn get_keys(&self) -> Vec<u32> {
        let mut keys = vec![];
        let mut pos = 0;
        for id in self.indirect_ids.iter() {
            if *id == u32::MAX {
                pos += 1;
                continue;
            }
            keys.push(pos);
            pos += 1;
        }
        keys
    }
}
impl HeapSizeOf for PointingArrays {
    fn heap_size_of_children(&self) -> usize {
        self.arr1.heap_size_of_children() + self.arr2.heap_size_of_children()
    }
}

use std::u32;
pub fn parrallel_arrays_to_pointing_array(keys: Vec<u32>, values: Vec<u32>) -> PointingArrays {
    trace!("keys {:?}", keys);
    trace!("values {:?}", values);
    let mut valids = keys.clone();
    valids.dedup();
    let mut indirect_ids = vec![];
    let mut arr1 = vec![];
    let mut arr2 = vec![];
    if valids.len() == 0 {
        return PointingArrays { arr1, arr2, indirect_ids };
    }

    let store = ParallelArrays { values1: keys.clone(), values2: values.clone() };
    let mut offset = 0;
    let mut pos = 0;
    for valid in valids {
        let mut vals = store.get_values(valid as u64).unwrap();
        vals.sort();
        let data = persistence::vec_to_bytes_u32(&vals); // @Temporary Add Compression
        arr1.push(offset);
        if indirect_ids.len() <= valid as usize {
            indirect_ids.resize(valid as usize + 1, u32::MAX);
        }
        indirect_ids[valid as usize] = pos;
        arr2.extend(data.iter().cloned());
        offset += data.len() as u64;
        pos += 1;
    }
    arr1.push(offset);
    PointingArrays { arr1, arr2, indirect_ids }
}



#[test]
fn test_pointing_array() {
    let keys = vec![0, 0, 1, 2, 3, 3];
    let values = vec![5, 6, 9, 9, 9, 50000];
    let pointing_array = parrallel_arrays_to_pointing_array(keys, values);
    let values = pointing_array.get_values(3);
    assert_eq!(values, Some(vec![9, 50000]));

    // let keys=   vec![0, 1, 3, 6, 8, 10];
    // let values= vec![7, 9, 4, 7, 9, 4];
    // let pointing_array = parrallel_arrays_to_pointing_array(keys, values);
    // assert_eq!(pointing_array.get_values(6), Some(vec![7]));
    // assert_eq!(pointing_array.get_values(8), Some(vec![9]));

    fn check(keys: Vec<u32>, values: Vec<u32>) {
        let ix = ParallelArrays { values1: keys, values2: values };
        let pointing_array = parrallel_arrays_to_pointing_array(ix.values1.clone(), ix.values2.clone());
        for key in ix.get_keys() {
            assert_eq!(pointing_array.get_values(key as u64), ix.get_values(key as u64));
        }
        assert_eq!(ix.get_keys(), pointing_array.get_keys());
    }

    check(vec![2, 3, 5, 8, 10, 12, 13, 14], vec![4, 0, 6, 1, 7, 5, 3, 2]);
    check(vec![0, 1, 4, 6, 7, 9, 11, 13], vec![5, 8, 5, 5, 8, 14, 5, 14]);
    // let pointing_array = parrallel_arrays_to_pointing_array(ix.values1.clone(), ix.values2.clone());
    // for key in ix.get_keys() {
    //     assert_eq!(pointing_array.get_values(key as u64), ix.get_values(key as u64));
    // }

    // [0, 1, 4, 6, 7, 9, 11, 13]
    // [5, 8, 5, 5, 8, 14, 5, 14]
}

#[derive(Debug)]
pub struct PointingArrayFileReader<'a> {
    pub path1:       String,
    pub path2:       String,
    pub persistence: &'a Persistence,
}

impl<'a> PointingArrayFileReader<'a> {
    // fn new(key:&(String, String), persistence:&'a Persistence) -> Self {
    //     PointingArrayFileReader { path1: key.0.clone(), path2: key.1.clone(), persistence }
    // }
    // fn get_values(&self, find: u32) -> Vec<u32> {
    //     let mut data:Vec<u8> = Vec::with_capacity(8);
    //     let mut file = self.persistence.get_file_handle(&self.path1).unwrap();// -> Result<File, io::Error>
    //     load_bytes(&mut data, &mut file, find as u64 *8);


    //     let result = Vec::new();
    //     // match self.values1.binary_search(&find) {
    //     //     Ok(mut pos) => {
    //     //         //this is not a lower_bounds search so we MUST move to the first hit
    //     //         while pos != 0 && self.values1[pos - 1] == find {pos-=1;}
    //     //         let val_len = self.values1.len();
    //     //         while pos < val_len && self.values1[pos] == find{
    //     //             result.push(self.values2[pos]);
    //     //             pos+=1;
    //     //         }
    //     //     },Err(_) => {},
    //     // }
    //     result
    // }
}
impl<'a> HeapSizeOf for PointingArrayFileReader<'a> {
    fn heap_size_of_children(&self) -> usize {
        self.path1.heap_size_of_children() + self.path2.heap_size_of_children()
    }
}


//                                                                  ParallelArrays

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ParallelArrays {
    pub values1: Vec<u32>,
    pub values2: Vec<u32>,
}

impl IndexIdToParent for ParallelArrays {
    fn get_values(&self, id: u64) -> Option<Vec<u32>> {
        let mut result = Vec::new();
        match self.values1.binary_search(&(id as u32)) {
            Ok(mut pos) => {
                //this is not a lower_bounds search so we MUST move to the first hit
                while pos != 0 && self.values1[pos - 1] == id as u32 {
                    pos -= 1;
                }
                let val_len = self.values1.len();
                while pos < val_len && self.values1[pos] == id as u32 {
                    result.push(self.values2[pos]);
                    pos += 1;
                }
            }
            Err(_) => {}
        }
        Some(result)
    }
    fn get_keys(&self) -> Vec<u32> {
        self.values1.clone()
    }
}
impl HeapSizeOf for ParallelArrays {
    fn heap_size_of_children(&self) -> usize {
        self.values1.heap_size_of_children() + self.values2.heap_size_of_children()
    }
}


// fn convert_valid_pair(arg: Type) -> RetType {
//     unimplemented!();
// }

#[flame]
pub fn valid_pair_to_parallel_arrays(tuples: &mut Vec<create::ValIdPair>) -> ParallelArrays {
    tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
    let valids =         tuples.iter().map(|ref el| el.valid).collect::<Vec<_>>();
    let parent_val_ids = tuples.iter().map(|ref el| el.parent_val_id).collect::<Vec<_>>();
    ParallelArrays { values1: valids, values2: parent_val_ids }
    // parrallel_arrays_to_pointing_array(data.values1, data.values2)
}

#[flame]
pub fn boost_pair_to_parallel_arrays(tuples: &mut Vec<create::ValIdToValue>) -> ParallelArrays {
    tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
    let valids = tuples.iter().map(|ref el| el.valid).collect::<Vec<_>>();
    let values = tuples.iter().map(|ref el| el.value).collect::<Vec<_>>();
    ParallelArrays { values1: valids, values2: values }
    // parrallel_arrays_to_pointing_array(data.values1, data.values2)
}


#[test]
fn test_index_parrallel_arrays() {
    let ix = ParallelArrays { values1: vec![0, 0, 1], values2: vec![0, 1, 2] };
    assert_eq!(ix.get_values(0).unwrap(), vec![0, 1]);
}


fn _load_bytes(buffer: &mut Vec<u8>, file: &mut File, offset: u64) {
    // @Temporary Use Result
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read_exact(buffer).unwrap();
}
