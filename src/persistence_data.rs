use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::cmp::Ordering;

#[allow(unused_imports)]
use heapsize::{heap_size_of, HeapSizeOf};
#[allow(unused_imports)]
use bincode::{deserialize, serialize, Infinite};

use persistence::*;
use persistence::Persistence;
// use persistence::IndexIdToParent;
// use persistence::IndexIdToOneParent;
// use persistence::IndexIdToMultipleParent;
// use persistence::IndexIdToMultipleParentCompressedSnappy;
// use persistence::IndexIdToMultipleParentCompressedMaydaDIRECT;
// use persistence::IndexIdToMultipleParentCompressedMaydaINDIRECT;
// use persistence::IndexIdToOneParentMayda;
use persistence;
use create;
use mayda;
use snap;
use std::i32;
#[allow(unused_imports)]
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[allow(unused_imports)]
use mayda::{Uniform, Encode};

use std::sync::Mutex;
use lru_cache::LruCache;

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

impl_type_info!(PointingArrays, ParallelArrays, IndexIdToOneParent,
    IndexIdToMultipleParent, IndexIdToMultipleParentCompressedSnappy,
    IndexIdToMultipleParentCompressedMaydaDIRECT, IndexIdToMultipleParentCompressedMaydaINDIRECT,
    IndexIdToMultipleParentCompressedMaydaINDIRECTOne, IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse, IndexIdToOneParentMayda);


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PointingArrays {
    arr1:         Vec<u64>, // offset
    arr2:         Vec<u8>,
    indirect_ids: Vec<u32>,
}



#[derive(Debug, HeapSizeOf)]
pub struct IndexIdToMultipleParent {
    data: Vec<Vec<u32>>,
}
impl IndexIdToMultipleParent {
    #[allow(dead_code)]
    pub fn new(data: &IndexIdToParent) -> IndexIdToMultipleParent {
        IndexIdToMultipleParent { data: id_to_parent_to_array_of_array(data) }
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

lazy_static! {
    static ref SNAP_DECODER: Mutex<snap::Decoder> = {
        Mutex::new(snap::Decoder::new())
    };
}

#[derive(Debug, HeapSizeOf)]
#[allow(dead_code)]
pub struct IndexIdToMultipleParentCompressedSnappy {
    data: Vec<Vec<u8>>,
}
impl IndexIdToMultipleParentCompressedSnappy {
    #[allow(dead_code)]
    pub fn new(store: &IndexIdToParent) -> IndexIdToMultipleParentCompressedSnappy {
        let data = id_to_parent_to_array_of_array_snappy(store);
        IndexIdToMultipleParentCompressedSnappy { data }
    }
}

impl IndexIdToParent for IndexIdToMultipleParentCompressedSnappy {
    fn get_values(&self, id: u64) -> Option<Vec<u32>> {
        self.data.get(id as usize).map(|el| {
            bytes_to_vec_u32(&SNAP_DECODER.lock().unwrap().decompress_vec(el).unwrap())
        })
    }
    fn get_keys(&self) -> Vec<u32> {
        (0..self.data.len() as u32).collect()
    }
}


#[derive(Debug, HeapSizeOf)]
#[allow(dead_code)]
pub struct IndexIdToMultipleParentCompressedMaydaDIRECT {
    data: Vec<mayda::Uniform<u32>>,
}
impl IndexIdToMultipleParentCompressedMaydaDIRECT {
    #[allow(dead_code)]
    pub fn new(store: &IndexIdToParent) -> IndexIdToMultipleParentCompressedMaydaDIRECT {
        let data = id_to_parent_to_array_of_array_mayda(store);
        IndexIdToMultipleParentCompressedMaydaDIRECT { data }
    }
}

impl IndexIdToParent for IndexIdToMultipleParentCompressedMaydaDIRECT {
    fn get_values(&self, id: u64) -> Option<Vec<u32>> {
        self.data.get(id as usize).map(|el| {
            el.decode()
        })
    }
    fn get_keys(&self) -> Vec<u32> {
        (0..self.data.len() as u32).collect()
    }
}
use mayda::Access;

#[derive(Debug, HeapSizeOf)]
#[allow(dead_code)]
pub struct IndexIdToMultipleParentCompressedMaydaINDIRECT {
    // pointers: Vec<(u32, u32)>, //start, end
    start: mayda::Uniform<u32>,
    end: mayda::Uniform<u32>,
    data: mayda::Uniform<u32>,
    size: usize,
}
impl IndexIdToMultipleParentCompressedMaydaINDIRECT {
    #[allow(dead_code)]
    pub fn new(store: &IndexIdToParent) -> IndexIdToMultipleParentCompressedMaydaINDIRECT {
        // let (pointers, data) = id_to_parent_to_array_of_array_mayda_indirect(store);
        let (size, start, end, data) = id_to_parent_to_array_of_array_mayda_indirect(store);
        IndexIdToMultipleParentCompressedMaydaINDIRECT { start, end, data, size }
    }
}

impl IndexIdToParent for IndexIdToMultipleParentCompressedMaydaINDIRECT {
    fn get_values(&self, id: u64) -> Option<Vec<u32>> {
        if id >= self.size as u64 {
            None
        }
        else {
            Some(self.data.access(self.start.access(id as usize) as usize .. self.end.access(id as usize) as usize).clone())
        }
    }
    fn get_values_compr(&self, _id: u64) -> Option<mayda::Uniform<u32>>{
        unimplemented!()
    }

    fn get_keys(&self) -> Vec<u32> {
        (0..self.start.len() as u32).collect()
    }

}

#[derive(Debug, HeapSizeOf)]
#[allow(dead_code)]
pub struct IndexIdToMultipleParentCompressedMaydaINDIRECTOne {
    // pointers: Vec<(u32, u32)>, //start, end
    start_and_end: mayda::Monotone<u32>,
    data: mayda::Uniform<u32>,
    size: usize,
}
impl IndexIdToMultipleParentCompressedMaydaINDIRECTOne {
    #[allow(dead_code)]
    pub fn new(store: &IndexIdToParent) -> IndexIdToMultipleParentCompressedMaydaINDIRECTOne {
        // let (pointers, data) = id_to_parent_to_array_of_array_mayda_indirect(store);
        let (size, start_and_end, data) = id_to_parent_to_array_of_array_mayda_indirect_one(store);

        println!("start_and_end {}", get_readable_size(start_and_end.heap_size_of_children()));
        println!("data {}", get_readable_size(data.heap_size_of_children()));

        IndexIdToMultipleParentCompressedMaydaINDIRECTOne { start_and_end, data, size }
    }
}

impl IndexIdToParent for IndexIdToMultipleParentCompressedMaydaINDIRECTOne {
    fn get_values(&self, id: u64) -> Option<Vec<u32>> {
        if id >= self.size as u64 {
            None
        }
        else {
            let positions = self.start_and_end.access((id * 2) as usize..=((id * 2) as usize + 1));
            Some(self.data.access(positions[0] as usize .. positions[1] as usize).clone())
        }
    }
    fn get_values_compr(&self, _id: u64) -> Option<mayda::Uniform<u32>>{
        unimplemented!()
    }

    fn get_keys(&self) -> Vec<u32> {
        (0..(self.start_and_end.len()/2) as u32).collect()
    }

}

#[derive(Debug, HeapSizeOf)]
#[allow(dead_code)]
pub struct IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse {
    // pointers: Vec<(u32, u32)>, //start, end
    start_and_end: mayda::Uniform<u32>,
    data: mayda::Uniform<u32>,
    size: usize,
}
impl IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse {
    #[allow(dead_code)]
    pub fn new(store: &IndexIdToParent) -> IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse {
        // let (pointers, data) = id_to_parent_to_array_of_array_mayda_indirect(store);
        let (size, start_and_end, data) = id_to_parent_to_array_of_array_mayda_indirect_one_reuse_existing(store);

        println!("start_and_end {}", get_readable_size(start_and_end.heap_size_of_children()));
        println!("data {}", get_readable_size(data.heap_size_of_children()));

        IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse { start_and_end, data, size }
    }
}

impl IndexIdToParent for IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse {
    fn get_values(&self, id: u64) -> Option<Vec<u32>> {
        if id >= self.size as u64 {
            None
        }
        else {
            let positions = self.start_and_end.access((id * 2) as usize..=((id * 2) as usize + 1));
            Some(self.data.access(positions[0] as usize .. positions[1] as usize).clone())
        }
    }
    fn get_values_compr(&self, _id: u64) -> Option<mayda::Uniform<u32>>{
        unimplemented!()
    }

    fn get_keys(&self) -> Vec<u32> {
        (0..(self.start_and_end.len()/2) as u32).collect()
    }

}

#[test]
fn test_mayda_compressed_one() {

    let keys =   vec![0, 0, 1, 2, 3, 3];
    let values = vec![5, 6, 9, 9, 9, 50000];

    let store = ParallelArrays { values1: keys.clone(), values2: values.clone() };
    let mayda = IndexIdToMultipleParentCompressedMaydaINDIRECTOne::new(&store);


    let yep = to_uniform(&values);
    assert_eq!(yep.access(0..=1), vec![5, 6]);

    assert_eq!(mayda.get_keys(), vec![0, 1, 2, 3]);
    assert_eq!(mayda.get_values(0).unwrap(), vec![5, 6]);
    assert_eq!(mayda.get_values(1).unwrap(), vec![9]);
    assert_eq!(mayda.get_values(2).unwrap(), vec![9]);
    assert_eq!(mayda.get_values(3).unwrap(), vec![9, 50000]);


}


#[derive(Debug, HeapSizeOf)]
pub struct IndexIdToOneParent {
    pub data: Vec<i32>,
}
impl IndexIdToOneParent {
    pub fn new(data: &IndexIdToParent) -> IndexIdToOneParent {
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

#[derive(Debug, HeapSizeOf)]
pub struct IndexIdToOneParentMayda {
    data: mayda::Uniform<i32>,
    size: usize,
}
impl IndexIdToOneParentMayda {
    #[allow(dead_code)]
    pub fn new(data: &IndexIdToParent) -> IndexIdToOneParentMayda {
        let yep = IndexIdToOneParent::new(data);
        IndexIdToOneParentMayda { size: yep.data.len(), data: to_uniform_i32(&yep.data) }
    }
}
impl IndexIdToParent for IndexIdToOneParentMayda {
    fn get_values(&self, id: u64) -> Option<Vec<u32>> {
        self.get_value(id).map(|el| vec![el])
    }
    fn get_value(&self, id: u64) -> Option<u32> {
        if id >= self.size as u64 {return None};
        let val = self.data.access(id as usize);

        match val {
            i32::MIN => None,
            _ =>  Some(val.clone() as u32)
        }
    }
    fn get_keys(&self) -> Vec<u32> {
        (0..self.data.len() as u32).collect()
    }
}



pub fn id_to_parent_to_array_of_array(store: &IndexIdToParent) -> Vec<Vec<u32>> {
    let mut data = vec![];
    let mut valids = store.get_keys();
    valids.dedup();
    if valids.len() == 0 {
        return data;
    }
    data.resize(*valids.last().unwrap() as usize + 1, vec![]);

    // debug_time!("convert key_value_store to vec vec");
    for valid in valids {
        let mut vals = store.get_values(valid as u64).unwrap();
        // vals.sort(); // WHY U SORT ?
        data[valid as usize] = vals;
    }
    data
}


pub fn id_to_parent_to_array_of_array_snappy(store: &IndexIdToParent) -> Vec<Vec<u8>> {
    let mut data = vec![];
    let mut valids = store.get_keys();
    valids.dedup();
    if valids.len() == 0 {
        return data;
    }
    data.resize(*valids.last().unwrap() as usize + 1, vec![]);

    // debug_time!("convert key_value_store to vec vec");
    for valid in valids {
        let mut encoder = snap::Encoder::new();
        let mut vals = store.get_values(valid as u64).unwrap();
        // println!("{:?}", vals);
        // let mut dat = vec_to_bytes_u32(&vals);
        let mut dat = encoder.compress_vec(&vec_to_bytes_u32(&vals)).unwrap();
        dat.shrink_to_fit();
        data[valid as usize] = dat;
    }
    data
}

pub fn id_to_parent_to_array_of_array_mayda(store: &IndexIdToParent) -> Vec<mayda::Uniform<u32>> {
    let mut data = vec![];
    let mut valids = store.get_keys();
    valids.dedup();
    if valids.len() == 0 {
        return data;
    }
    data.resize(*valids.last().unwrap() as usize + 1, mayda::Uniform::new());

    // debug_time!("convert key_value_store to vec vec");
    for valid in valids {
        let mut uniform = mayda::Uniform::new();
        let mut vals = store.get_values(valid as u64).unwrap();
        uniform.encode(&vals).unwrap();
        data[valid as usize] = uniform;
    }
    data
}

//TODO TRY WITH FROM ITERATOR oder so
pub fn to_uniform(data: &Vec<u32>) -> mayda::Uniform<u32>{
    let mut uniform = mayda::Uniform::new();
    uniform.encode(&data).unwrap();
    uniform
}
pub fn to_uniform_i32(data: &Vec<i32>) -> mayda::Uniform<i32>{
    let mut uniform = mayda::Uniform::new();
    uniform.encode(&data).unwrap();
    uniform
}
pub fn to_monotone(data: &Vec<u32>) -> mayda::Monotone<u32>{
    let mut uniform = mayda::Monotone::new();
    uniform.encode(&data).unwrap();
    uniform
}

pub fn id_to_parent_to_array_of_array_mayda_indirect(store: &IndexIdToParent) -> (usize, mayda::Uniform<u32>, mayda::Uniform<u32>, mayda::Uniform<u32>) { //start, end, data
    let mut data = vec![];
    let mut valids = store.get_keys();
    valids.dedup();
    if valids.len() == 0 {
        return (0, mayda::Uniform::default(), mayda::Uniform::default(), mayda::Uniform::default());
    }
    let mut start_pos = vec![];
    let mut end_pos = vec![];
    start_pos.resize(*valids.last().unwrap() as usize + 1, 0);
    end_pos.resize(*valids.last().unwrap() as usize + 1, 0);

    // let mut start_and_end = vec![];
    // start_and_end.resize(*valids.last().unwrap() as usize + 1, (0, 0));
    let mut offset = 0;
    // debug_time!("convert key_value_store to vec vec");

    for valid in valids {
        let mut vals = store.get_values(valid as u64).unwrap();
        let start = offset;
        data.extend(&vals);
        offset += vals.len() as u32;
        // start_and_end.push((start, offset));
        // start_and_end[valid as usize] = (start, offset);

        start_pos[valid as usize] = start;
        end_pos[valid as usize] = offset;
    }

    data.shrink_to_fit();
    // let mut uniform = mayda::Uniform::new();
    // uniform.encode(&data).unwrap();
    // (start_and_end, uniform)

    (start_pos.len(), to_uniform(&start_pos), to_uniform(&end_pos), to_uniform(&data))
}

pub fn id_to_parent_to_array_of_array_mayda_indirect_one(store: &IndexIdToParent) -> (usize, mayda::Monotone<u32>, mayda::Uniform<u32>) { //start, end, data
    let mut data = vec![];
    let mut valids = store.get_keys();
    valids.dedup();
    if valids.len() == 0 {
        return (0, mayda::Monotone::default(), mayda::Uniform::default());
    }
    let mut start_and_end_pos = vec![];
    start_and_end_pos.resize((*valids.last().unwrap() as usize + 1) * 2, 0);

    // let mut start_and_end = vec![];
    // start_and_end.resize(*valids.last().unwrap() as usize + 1, (0, 0));
    let mut offset = 0;
    // debug_time!("convert key_value_store to vec vec");

    // for valid in valids {
    //     let mut vals = store.get_values(valid as u64).unwrap();
    //     let start = offset;
    //     data.extend(&vals);
    //     offset += vals.len() as u32;
    //     // start_and_end.push((start, offset));
    //     // start_and_end[valid as usize] = (start, offset);
    //     start_and_end_pos[valid as usize * 2] = start;
    //     start_and_end_pos[(valid as usize * 2) + 1] = offset;
    // }

    for valid in 0..=*valids.last().unwrap() {
        let mut vals = store.get_values(valid as u64).unwrap();
        let start = offset;
        data.extend(&vals);
        offset += vals.len() as u32;
        // start_and_end.push((start, offset));
        // start_and_end[valid as usize] = (start, offset);

        start_and_end_pos[valid as usize * 2] = start;
        start_and_end_pos[(valid as usize * 2) + 1] = offset;
    }

    data.shrink_to_fit();
    // let mut uniform = mayda::Uniform::new();
    // uniform.encode(&data).unwrap();
    // (start_and_end, uniform)

    // println!("start_and_end_pos {:?}", start_and_end_pos);
    // println!("data {:?}", data);

    // println!("WAAAAAAAAA {:?}", start_and_end_pos);

    (start_and_end_pos.len()/2, to_monotone(&start_and_end_pos), to_uniform(&data))
}


pub fn id_to_parent_to_array_of_array_mayda_indirect_one_reuse_existing(store: &IndexIdToParent) -> (usize, mayda::Uniform<u32>, mayda::Uniform<u32>) { //start, end, data
    let mut data = vec![];
    let mut valids = store.get_keys();
    valids.dedup();
    if valids.len() == 0 {
        return (0, mayda::Uniform::default(), mayda::Uniform::default());
    }
    let mut start_and_end_pos = vec![];
    start_and_end_pos.resize((*valids.last().unwrap() as usize + 1) * 2, 0);

    // let mut start_and_end = vec![];
    // start_and_end.resize(*valids.last().unwrap() as usize + 1, (0, 0));
    let mut offset = 0;
    // debug_time!("convert key_value_store to vec vec");

    let mut cache = LruCache::new(250);

    for valid in 0..=*valids.last().unwrap() {
        let mut vals = store.get_values(valid as u64).unwrap();

        if let Some(&mut (start, offset)) = cache.get_mut(&vals) { //reuse and reference existing data
            start_and_end_pos[valid as usize * 2] = start;
            start_and_end_pos[(valid as usize * 2) + 1] = offset;
        }else{
            let start = offset;
            data.extend(&vals);
            offset += vals.len() as u32;
            // start_and_end.push((start, offset));
            // start_and_end[valid as usize] = (start, offset);

            start_and_end_pos[valid as usize * 2] = start;
            start_and_end_pos[(valid as usize * 2) + 1] = offset;

            cache.insert(vals, (start, offset));
        }
    }

    data.shrink_to_fit();

    // println!("WAAAAAAAAA {:?}", start_and_end_pos);

    (start_and_end_pos.len()/2, to_uniform(&start_and_end_pos), to_uniform(&data))
}













impl IndexIdToParent for PointingArrays {
    fn get_values(&self, id: u64) -> Option<Vec<u32>> {
        self.indirect_ids.get(id as usize).map(|pos| {
            let ref bytes = self.arr2[self.arr1[*pos as usize] as usize..self.arr1[*pos as usize + 1] as usize];
            persistence::bytes_to_vec_u32(bytes)
        })
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




#[test]
fn test_snap() {
    let mut encoder = snap::Encoder::new();
    let mut data: Vec<Vec<u32>> = vec![];
    data.push(vec![
        11, 12, 13, 14, 15, 16, 17, 18, 19, 110, 111, 112, 113, 114, 115, 116, 117, 118
    ]);
    data.push(vec![10, 11, 12, 13, 14, 15]);
    data.push(vec![10]);
    info!("data orig {:?}", data.heap_size_of_children());
    // let data2:Vec<Vec<u8>> = data.iter().map(|el| {
    //     let mut el = el.clone();
    //     el.sort();
    //     let mut dat = encoder.compress_vec(&vec_to_bytes(&el)).unwrap();
    //     dat.shrink_to_fit();
    //     dat
    // }).collect();
    // info!("data abono compressed {:?}", data2.heap_size_of_children());

    // let data3:Vec<Vec<u8>> = data.iter().map(|el| {
    //     let el = el.clone();
    //     let mut dat = vec_to_bytes(&el);
    //     dat.shrink_to_fit();
    //     dat
    // }).collect();
    // info!("data abono bytes {:?}", data3.heap_size_of_children());

    let data4: Vec<Vec<u8>> = data.iter().map(|el| vec_to_bytes_u32(el)).collect();
    info!("data byteorder {:?}", data4.heap_size_of_children());

    let data5: Vec<Vec<u8>> = data.iter()
        .map(|el| {
            let mut dat = encoder.compress_vec(&vec_to_bytes_u32(el)).unwrap();
            dat.shrink_to_fit();
            dat
        })
        .collect();
    info!("data byteorder compressed {:?}", data5.heap_size_of_children());

    // let mut test_vec:Vec<u32> = vec![10];
    // test_vec.shrink_to_fit();
    // let mut bytes:Vec<u8> = Vec::new();
    // unsafe { encode(&test_vec, &mut bytes); };
    // bytes.shrink_to_fit();
    // info!("{:?}", test_vec);
    // info!("{:?}", bytes);

    let mut wtr: Vec<u8> = vec![];
    wtr.write_u32::<LittleEndian>(10).unwrap();
    info!("wtr {:?}", wtr);
}

