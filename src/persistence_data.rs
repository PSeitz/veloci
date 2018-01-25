use std;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::cmp::Ordering;

#[allow(unused_imports)]
use heapsize::{heap_size_of, HeapSizeOf};
#[allow(unused_imports)]
use bincode::{deserialize, serialize, Infinite};

#[allow(unused_imports)]
use util::*;

use persistence::*;
pub use persistence_data_indirect::*;
#[allow(unused_imports)]
use persistence;
use create;
use mayda;
use snap;
#[allow(unused_imports)]
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[allow(unused_imports)]
use mayda::{Access, AccessInto, Encode, Uniform};
use parking_lot::Mutex;
use lru_cache::LruCache;

use std::io::Cursor;
use std::fs;
#[allow(unused_imports)]
use std::fmt::Debug;
use num::cast::ToPrimitive;
use num::{Integer, NumCast};
use std::marker::PhantomData;

#[allow(unused_imports)]
use fnv::FnvHashMap;
#[allow(unused_imports)]
use fnv::FnvHashSet;

pub trait TypeInfo: Sync + Send {
    fn type_name(&self) -> String;
    fn type_of(&self) -> String;
}
macro_rules! mut_if {
    ($name:ident = $value:expr, $($any:expr)+) => (let mut $name = $value;);
    ($name:ident = $value:expr,) => (let $name = $value;);
}

macro_rules! impl_type_info_single_templ {
    ($name:ident$(<$($T:ident),+>)*) => {
        impl<D: IndexIdToParentData>$(<$($T: TypeInfo),*>)* TypeInfo for $name<D>$(<$($T),*>)* {
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

impl_type_info_single_templ!(IndexIdToMultipleParentCompressedMaydaDIRECT);
impl_type_info_single_templ!(IndexIdToMultipleParent);
impl_type_info_single_templ!(IndexIdToOneParentMayda);
impl_type_info_single_templ!(IndexIdToOneParent);
// impl_type_info_single_templ!(FSTToOneParent);
impl_type_info_single_templ!(ParallelArrays);

impl_type_info_single_templ!(SingleArrayFileReader);

#[derive(Debug, HeapSizeOf)]
pub struct IndexIdToMultipleParent<T: IndexIdToParentData> {
    pub data: Vec<Vec<T>>,
}
impl<T: IndexIdToParentData> IndexIdToMultipleParent<T> {
    #[allow(dead_code)]
    pub fn new(data: &IndexIdToParent<Output = T>) -> IndexIdToMultipleParent<T> {
        IndexIdToMultipleParent {
            data: id_to_parent_to_array_of_array(data),
        }
    }
}
impl<T: IndexIdToParentData> IndexIdToParent for IndexIdToMultipleParent<T> {
    type Output = T;
    fn get_values(&self, id: u64) -> Option<Vec<T>> {
        let vec: Option<Vec<T>> = self.data
            .get(id as usize)
            .map(|el| el.iter().map(|el| NumCast::from(*el).unwrap()).collect());
        if vec.is_some() && vec.as_ref().unwrap().len() == 0 {
            return None;
        }
        vec
    }
    fn get_keys(&self) -> Vec<T> {
        (NumCast::from(0).unwrap()..NumCast::from(self.data.len()).unwrap()).collect()
    }

    #[inline]
    fn count_values_for_ids(&self, ids: &[u32], _top: Option<u32>) -> FnvHashMap<T, usize> {
        let mut hits = FnvHashMap::default();
        let size = self.data.len();
        for id in ids {
            if *id >= size as u32 {
                continue;
            } else {
                for hit_id in &self.data[*id as usize] {
                    let stat = hits.entry(*hit_id).or_insert(0);
                    *stat += 1;
                }
            }
        }
        hits
    }
}

#[derive(Debug, HeapSizeOf)]
#[allow(dead_code)]
pub struct IndexIdToMultipleParentCompressedMaydaDIRECT<T: IndexIdToParentData> {
    data: Vec<mayda::Uniform<T>>,
}
impl<T: IndexIdToParentData> IndexIdToMultipleParentCompressedMaydaDIRECT<T> {
    #[allow(dead_code)]
    pub fn new(store: &IndexIdToParent<Output = T>) -> IndexIdToMultipleParentCompressedMaydaDIRECT<T> {
        let data = id_to_parent_to_array_of_array_mayda(store);
        IndexIdToMultipleParentCompressedMaydaDIRECT { data }
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for IndexIdToMultipleParentCompressedMaydaDIRECT<T> {
    type Output = T;
    default fn get_values(&self, id: u64) -> Option<Vec<T>> {
        self.data.get(id as usize).map(|el| {
            el.decode()
                .iter()
                .map(|el| NumCast::from(*el).unwrap())
                .collect()
        })
    }
    fn get_keys(&self) -> Vec<T> {
        (NumCast::from(0).unwrap()..NumCast::from(self.data.len()).unwrap()).collect()
    }
}

// impl IndexIdToParent for IndexIdToMultipleParentCompressedMaydaDIRECT<u32> {
//     type Output = u32;
//     fn get_values(&self, id: u64) -> Option<Vec<u32>> {
//         self.data.get(id as usize).map(|el| {
//             el.decode()
//         })
//     }
// }

// impl IndexIdToParent for IndexIdToMultipleParentIndirect<u32> {
//     // fn get_values(&self, id: u64) -> Option<Vec<u32>> {
//     //     if id >= self.get_size() as u64 {None }
//     //     else {
//     //         let positions = &self.start_and_end[(id * 2) as usize..=((id * 2) as usize + 1)];
//     //         if positions[0] == positions[1] {return None}
//     //         Some(self.data[NumCast::from(positions[0]).unwrap() .. NumCast::from(positions[1]).unwrap()].to_vec())
//     //     }
//     // }

//     fn count_values_for_ids(&self, ids: &[u32], hits: &mut FnvHashMap<u32, usize>, top:Option<u32>){
//         let size = self.get_size();
//         for id in ids {
//             if *id >= size as u32 {
//                 continue;
//             } else {
//                 let positions = &self.start_and_end[(*id * 2) as usize..=((*id * 2) as usize + 1)];
//                 if positions[0] == positions[1] {
//                     continue;
//                 }

//                 for hit_id in &self.data[positions[0] as usize ..positions[1] as usize] {
//                     let stat = hits.entry(*hit_id).or_insert(0);
//                     *stat += 1;
//                 }
//             }

//         }
//     }
// }

#[derive(Debug, HeapSizeOf)]
pub struct IndexIdToOneParent<T: IndexIdToParentData> {
    pub data: Vec<T>,
}
impl<T: IndexIdToParentData> IndexIdToOneParent<T> {
    pub fn new(data: &IndexIdToParent<Output = T>) -> IndexIdToOneParent<T> {
        let data: Vec<Vec<T>> = id_to_parent_to_array_of_array(data);
        let data = data.iter()
            .map(|el| {
                if el.len() > 0 {
                    NumCast::from(el[0]).unwrap()
                } else {
                    NumCast::from(NOT_FOUND).unwrap()
                }
            })
            .collect();
        IndexIdToOneParent { data }
    }
}
impl<T: IndexIdToParentData> IndexIdToParent for IndexIdToOneParent<T> {
    type Output = T;
    #[inline]
    fn get_values(&self, id: u64) -> Option<Vec<T>> {
        self.get_value(id).map(|el| vec![el])
    }
    fn get_value(&self, id: u64) -> Option<T> {
        let val = self.data.get(id as usize);
        match val {
            Some(val) => {
                if val.to_u64().unwrap() == NOT_FOUND.to_u64().unwrap() {
                    None
                } else {
                    Some(*val)
                }
            }
            None => None,
        }
    }
    fn get_keys(&self) -> Vec<T> {
        (NumCast::from(0).unwrap()..NumCast::from(self.data.len()).unwrap()).collect()
    }
}

// lazy_static! {
//     static ref NOT_FOUND_U64: u64 = {
//         let not_found_u64 = u32::MAX;
//         let  yo = not_found_u64.to_u64().unwrap();
//         yo
//     };
// }

#[derive(Debug, HeapSizeOf)]
pub struct IndexIdToOneParentMayda<T: IndexIdToParentData> {
    pub data: mayda::Uniform<T>,
    pub size: usize,
}
impl<T: IndexIdToParentData> IndexIdToOneParentMayda<T> {
    #[allow(dead_code)]
    pub fn new(data: &IndexIdToParent<Output = T>) -> IndexIdToOneParentMayda<T> {
        let yep = IndexIdToOneParent::new(data);
        IndexIdToOneParentMayda {
            size: yep.data.len(),
            data: to_uniform(&yep.data),
        }
    }
    #[allow(dead_code)]
    pub fn from_vec(data: &Vec<T>) -> IndexIdToOneParentMayda<T> {
        IndexIdToOneParentMayda {
            size: data.len(),
            data: to_uniform(&data),
        }
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for IndexIdToOneParentMayda<T> {
    type Output = T;
    #[inline]
    fn get_values(&self, id: u64) -> Option<Vec<T>> {
        self.get_value(id).map(|el| vec![el])
    }
    #[inline]
    fn get_value(&self, id: u64) -> Option<T> {
        if id >= self.size as u64 {
            return None;
        };
        let val = self.data.access(id as usize);
        if  val.to_u32().unwrap() == u32::MAX {
            None
        }else{
            Some(val)
        }

        // let not_found_u64 = u32::MAX;
        // let yo = not_found_u64.to_u64().unwrap();

        // match val.to_u64().unwrap() {
        //     yo => None,
        //     _ => Some(val),
        // }
    }
    #[inline]
    fn get_mutliple_value(&self, range: std::ops::RangeInclusive<usize>) -> Option<Vec<T>> {
        Some(self.data.access(range))
    }
    fn get_keys(&self) -> Vec<T> {
        (NumCast::from(0).unwrap()..NumCast::from(self.data.len()).unwrap()).collect()
    }
}

// #[derive(Debug, HeapSizeOf)]
// pub struct FSTToOneParent<T: IndexIdToParentData> {
//     pub data: Vec<T>,
// }
// impl<T: IndexIdToParentData> FSTToOneParent<T> {
//     pub fn new(data: &IndexIdToParent<Output = T>) -> FSTToOneParent<T> {
//         let data: Vec<Vec<T>> = id_to_parent_to_array_of_array(data);
//         let data = data.iter()
//             .map(|el| {
//                 if el.len() > 0 {
//                     NumCast::from(el[0]).unwrap()
//                 } else {
//                     NumCast::from(NOT_FOUND).unwrap()
//                 }
//             })
//             .collect();
//         FSTToOneParent { data }
//     }
// }
// impl<T: IndexIdToParentData> IndexIdToParent for FSTToOneParent<T> {
//     type Output = T;
//     fn get_values(&self, id: u64) -> Option<Vec<T>> {
//         self.get_value(id).map(|el| vec![el])
//     }
//     fn get_value(&self, id: u64) -> Option<T> {
//         let val = self.data.get(id as usize);
//         match val {
//             Some(val) => {
//                 if val.to_u32().unwrap() == NOT_FOUND {
//                     None
//                 } else {
//                     Some(*val)
//                 }
//             }
//             None => None,
//         }
//     }
//     fn get_keys(&self) -> Vec<T> {
//         (NumCast::from(0).unwrap()..NumCast::from(self.data.len()).unwrap()).collect()
//     }
// }

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ParallelArrays<T: IndexIdToParentData> {
    pub values1: Vec<T>,
    pub values2: Vec<T>,
}

impl<T: IndexIdToParentData> IndexIdToParent for ParallelArrays<T> {
    type Output = T;
    #[inline]
    fn get_values(&self, id: u64) -> Option<Vec<T>> {
        let mut result = Vec::new();
        let casted_id = NumCast::from(id).unwrap();
        match self.values1.binary_search(&casted_id) {
            Ok(mut pos) => {
                //this is not a lower_bounds search so we MUST move to the first hit
                while pos != 0 && self.values1[pos - 1] == casted_id {
                    pos -= 1;
                }
                let val_len = self.values1.len();
                while pos < val_len && self.values1[pos] == casted_id {
                    result.push(self.values2[pos]);
                    pos += 1;
                }
            }
            Err(_) => {}
        }
        if result.len() == 0 {
            None
        } else {
            Some(result)
        }
    }
    fn get_keys(&self) -> Vec<T> {
        let mut keys: Vec<T> = self.values1
            .iter()
            .map(|el| NumCast::from(*el).unwrap())
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }
}
impl<T: IndexIdToParentData> HeapSizeOf for ParallelArrays<T> {
    fn heap_size_of_children(&self) -> usize {
        self.values1.heap_size_of_children() + self.values2.heap_size_of_children()
    }
}

#[derive(Debug)]
pub struct SingleArrayFileReader<T: IndexIdToParentData> {
    pub data_file: Mutex<fs::File>,
    pub data_metadata: Mutex<fs::Metadata>,
    pub ok: PhantomData<T>,
}

trait GetSize {
    fn get_size(&self) -> usize;
}

impl<T: IndexIdToParentData> SingleArrayFileReader<T> {
    pub fn new(data_file: fs::File, data_metadata: fs::Metadata) -> Self {
        SingleArrayFileReader {
            data_file: Mutex::new(data_file),
            data_metadata: Mutex::new(data_metadata),
            ok: PhantomData,
        }
    }
    // default fn get_size(&self) -> usize {
    //     unimplemented!()
    // }
}
impl<T: IndexIdToParentData> GetSize for SingleArrayFileReader<T> {
    default fn get_size(&self) -> usize {
        unimplemented!()
    }
}

impl GetSize for SingleArrayFileReader<u32> {
    fn get_size(&self) -> usize {
        self.data_metadata.lock().len() as usize / 4
    }
}
impl GetSize for SingleArrayFileReader<u64> {
    fn get_size(&self) -> usize {
        self.data_metadata.lock().len() as usize / 8
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for SingleArrayFileReader<T> {
    type Output = T;
    default fn get_value(&self, _find: u64) -> Option<T> {
        unimplemented!()
    }
    default fn get_values(&self, _find: u64) -> Option<Vec<T>> {
        unimplemented!()
    }
    fn get_keys(&self) -> Vec<T> {
        (NumCast::from(0).unwrap()..NumCast::from(self.get_size()).unwrap()).collect()
    }
}

impl IndexIdToParent for SingleArrayFileReader<u64> {
    fn get_value(&self, find: u64) -> Option<u64> {
        get_reader(
            std::mem::size_of::<u64>(),
            find,
            1,
            &self.data_file,
            &self.data_metadata,
        ).map(|mut rdr| rdr.read_u64::<LittleEndian>().unwrap())
    }

    fn get_mutliple_value(&self, range: std::ops::RangeInclusive<usize>) -> Option<Vec<Self::Output>> {
        get_bytes(
            std::mem::size_of::<u64>(),
            range.start as u64,
            range.size_hint().0 as u64,
            &self.data_file,
            &self.data_metadata,
        ).map(|bytes| {
            bytes_to_vec_u64(&bytes) // TODO Performance In place bytes to u64 ?
        })
    }
}
impl IndexIdToParent for SingleArrayFileReader<u32> {
    fn get_value(&self, find: u64) -> Option<u32> {
        get_reader(
            std::mem::size_of::<u32>(),
            find,
            1,
            &self.data_file,
            &self.data_metadata,
        ).map(|mut rdr| rdr.read_u32::<LittleEndian>().unwrap())
    }
    fn get_mutliple_value(&self, range: std::ops::RangeInclusive<usize>) -> Option<Vec<Self::Output>> {
        get_bytes(
            std::mem::size_of::<u32>(),
            range.start as u64,
            range.size_hint().0 as u64,
            &self.data_file,
            &self.data_metadata,
        ).map(|bytes| {
            bytes_to_vec_u32(&bytes) // TODO Performance In place bytes to u32 ?
        })
    }
}
impl<T: IndexIdToParentData> HeapSizeOf for SingleArrayFileReader<T> {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}

fn get_bytes(block_size: usize, find: u64, num_elem: u64, data_file: &Mutex<fs::File>, data_metadata: &Mutex<fs::Metadata>) -> Option<Vec<u8>> {
    let size = data_metadata.lock().len() as usize / block_size;
    if find >= size as u64 {
        return None;
    }
    let data_bytes = load_bytes(
        &*data_file.lock(),
        find as u64 * block_size as u64,
        block_size * num_elem as usize,
    );

    Some(data_bytes)
}
fn get_reader(block_size: usize, find: u64, num_elem: u64, data_file: &Mutex<fs::File>, data_metadata: &Mutex<fs::Metadata>) -> Option<Cursor<Vec<u8>>> {
    // Some(Cursor::new(bytes))
    get_bytes(block_size, find, num_elem, data_file, data_metadata).map(|bytes| Cursor::new(bytes))
}

pub fn id_to_parent_to_array_of_array<T: IndexIdToParentData>(store: &IndexIdToParent<Output = T>) -> Vec<Vec<T>> {
    let mut data: Vec<Vec<T>> = prepare_data_for_array_of_array(store, &Vec::new);
    let valids = store.get_keys();

    for valid in valids {
        if let Some(vals) = store.get_values(NumCast::from(valid).unwrap()) {
            data[valid.to_usize().unwrap()] = vals.iter().map(|el| NumCast::from(*el).unwrap()).collect();
            // vals.sort(); // WHY U SORT ?
        }
    }
    data
}

pub fn id_to_parent_to_array_of_array_snappy(store: &IndexIdToParent<Output = u32>) -> Vec<Vec<u8>> {
    let mut data: Vec<Vec<u8>> = prepare_data_for_array_of_array(store, &Vec::new);
    let valids = store.get_keys();

    // debug_time!("convert key_value_store to vec vec");
    for valid in valids {
        let mut encoder = snap::Encoder::new();
        let mut vals = store.get_values(NumCast::from(valid).unwrap()).unwrap();
        // println!("{:?}", vals);
        // let mut dat = vec_to_bytes_u32(&vals);
        let mut dat = encoder.compress_vec(&vec_to_bytes_u32(&vals)).unwrap();
        dat.shrink_to_fit();
        data[valid.to_usize().unwrap()] = dat;
    }
    data
}
pub fn id_to_parent_to_array_of_array_mayda<T: IndexIdToParentData>(store: &IndexIdToParent<Output = T>) -> Vec<mayda::Uniform<T>> {
    let mut data: Vec<mayda::Uniform<T>> = prepare_data_for_array_of_array(store, &mayda::Uniform::new);
    let valids = store.get_keys();

    // debug_time!("convert key_value_store to vec vec");
    for valid in valids {
        let mut uniform = mayda::Uniform::new();
        if let Some(vals) = store.get_values(NumCast::from(valid).unwrap()) {
            let yeps: Vec<T> = vals.iter().map(|el| NumCast::from(*el).unwrap()).collect();
            uniform.encode(&yeps).unwrap();
            data[valid.to_usize().unwrap()] = uniform;
        }
    }
    data
}

fn prepare_data_for_array_of_array<T: Clone, K: IndexIdToParentData>(store: &IndexIdToParent<Output = K>, f: &Fn() -> T) -> Vec<T> {
    let mut data = vec![];
    let mut valids = store.get_keys();
    valids.dedup();
    if valids.len() == 0 {
        return data;
    }
    data.resize(valids.last().unwrap().to_usize().unwrap() + 1, f());
    data
}

// fn prepare_data_for_array_of_array<T:IndexIdToParentData, K:>(store: &IndexIdToParent<Output=T>, f: &Fn() -> Vec<T>) -> Vec<Vec<T>> {
//     let mut data = vec![];
//     let mut valids = store.get_keys();
//     valids.dedup();
//     if valids.len() == 0 {
//         return data;
//     }
//     data.resize(*valids.last().unwrap() as usize + 1, f());
//     data

// }

//TODO TRY WITH FROM ITERATOR oder so
pub fn to_uniform<T: mayda::utility::Bits>(data: &Vec<T>) -> mayda::Uniform<T> {
    let mut uniform = mayda::Uniform::new();
    uniform.encode(&data).unwrap();
    uniform
}
pub fn to_monotone<T: mayda::utility::Bits>(data: &Vec<T>) -> mayda::Monotone<T> {
    let mut uniform = mayda::Monotone::new();
    uniform.encode(&data).unwrap();
    uniform
}

// pub fn id_to_parent_to_array_of_array_mayda_indirect(store: &IndexIdToParent) -> (usize, mayda::Uniform<u32>, mayda::Uniform<u32>, mayda::Uniform<u32>) { //start, end, data
//     let mut data = vec![];
//     let mut valids = store.get_keys();
//     valids.dedup();
//     if valids.len() == 0 {
//         return (0, mayda::Uniform::default(), mayda::Uniform::default(), mayda::Uniform::default());
//     }
//     let mut start_pos = vec![];
//     let mut end_pos = vec![];
//     start_pos.resize(*valids.last().unwrap() as usize + 1, 0);
//     end_pos.resize(*valids.last().unwrap() as usize + 1, 0);

//     let mut offset = 0;
//     // debug_time!("convert key_value_store to vec vec");

//     for valid in valids {
//         let mut vals = store.get_values(valid as u64).unwrap();
//         let start = offset;
//         data.extend(&vals);
//         offset += vals.len() as u32;

//         start_pos[valid as usize] = start;
//         end_pos[valid as usize] = offset;
//     }

//     data.shrink_to_fit();

//     (start_pos.len(), to_uniform(&start_pos), to_uniform(&end_pos), to_uniform(&data))
// }
use num;

fn to_indirect_arrays<T: Integer + Clone + NumCast + mayda::utility::Bits + Copy, K: IndexIdToParentData>(
    store: &IndexIdToParent<Output = K>,
    cache_size: usize,
) -> (Vec<T>, Vec<T>) {
    to_indirect_arrays_dedup(store, cache_size, false)
}

fn to_indirect_arrays_dedup<T: Integer + Clone + NumCast + mayda::utility::Bits + Copy, K: IndexIdToParentData>(
    store: &IndexIdToParent<Output = K>,
    cache_size: usize,
    sort_and_dedup: bool,
) -> (Vec<T>, Vec<T>) {
    let mut data = vec![];
    let mut valids = store.get_keys();
    valids.dedup();
    if valids.len() == 0 {
        return (vec![], vec![]);
    }
    let mut start_and_end_pos = vec![];
    let last_id = *valids.last().unwrap();
    start_and_end_pos.resize(
        (valids.last().unwrap().to_usize().unwrap() + 1) * 2,
        T::zero(),
    );

    let mut offset = 0;

    let mut cache = LruCache::new(cache_size);

    for valid in 0..=num::cast(last_id).unwrap() {
        let start = offset;
        if let Some(mut vals) = store.get_values(valid as u64) {
            if sort_and_dedup {
                vals.sort();
                vals.dedup();
            }

            if let Some(&mut (start, offset)) = cache.get_mut(&vals) {
                //reuse and reference existing data
                start_and_end_pos[valid as usize * 2] = start;
                start_and_end_pos[(valid as usize * 2) + 1] = offset;
            } else {
                let start = offset;

                for val in &vals {
                    data.push(num::cast(*val).unwrap());
                }
                offset += vals.len() as u64;

                if cache_size > 0 {
                    cache.insert(
                        vals,
                        (num::cast(start).unwrap(), num::cast(offset).unwrap()),
                    );
                }
                start_and_end_pos[valid as usize * 2] = num::cast(start).unwrap();
                start_and_end_pos[(valid as usize * 2) + 1] = num::cast(offset).unwrap();
            }
        } else {
            // add latest offsets, so the data is monotonically increasing -> better compression
            start_and_end_pos[valid as usize * 2] = num::cast(start).unwrap();
            start_and_end_pos[(valid as usize * 2) + 1] = num::cast(offset).unwrap();
        }
    }
    data.shrink_to_fit();

    (start_and_end_pos, data)
}

pub fn id_to_parent_to_array_of_array_mayda_indirect_one<T: Integer + Clone + NumCast + mayda::utility::Bits + Copy, K: IndexIdToParentData>(
    store: &IndexIdToParent<Output = K>,
) -> (usize, mayda::Monotone<T>, mayda::Uniform<T>) {
    //start, end, data
    let (start_and_end_pos, data) = to_indirect_arrays(store, 0);
    (
        start_and_end_pos.len() / 2,
        to_monotone(&start_and_end_pos),
        to_uniform(&data),
    )
}

pub fn id_to_parent_to_array_of_array_mayda_indirect_one_reuse_existing<T: Integer + Clone + NumCast + mayda::utility::Bits + Copy, K: IndexIdToParentData>(
    store: &IndexIdToParent<Output = K>,
) -> (usize, mayda::Uniform<T>, mayda::Uniform<T>) {
    //start, end, data
    let (start_and_end_pos, data) = to_indirect_arrays(store, 250);
    (
        start_and_end_pos.len() / 2,
        to_uniform(&start_and_end_pos),
        to_uniform(&data),
    )
}

use std::u32;

// #[test]
// fn test_pointing_array() {
//     let keys = vec![0, 0, 1, 2, 3, 3];
//     let values = vec![5, 6, 9, 9, 9, 50000];
//     let pointing_array = parrallel_arrays_to_pointing_array(keys, values);
//     let values = pointing_array.get_values(3);
//     assert_eq!(values, Some(vec![9, 50000]));

//     // let keys=   vec![0, 1, 3, 6, 8, 10];
//     // let values= vec![7, 9, 4, 7, 9, 4];
//     // let pointing_array = parrallel_arrays_to_pointing_array(keys, values);
//     // assert_eq!(pointing_array.get_values(6), Some(vec![7]));
//     // assert_eq!(pointing_array.get_values(8), Some(vec![9]));

//     fn check(keys: Vec<u32>, values: Vec<u32>) {
//         let ix = ParallelArrays { values1: keys, values2: values };
//         let pointing_array = parrallel_arrays_to_pointing_array(ix.values1.clone(), ix.values2.clone());
//         for key in ix.get_keys() {
//             assert_eq!(pointing_array.get_values(key as u64), ix.get_values(key as u64));
//         }
//         assert_eq!(ix.get_keys(), pointing_array.get_keys());
//     }

//     check(vec![2, 3, 5, 8, 10, 12, 13, 14], vec![4, 0, 6, 1, 7, 5, 3, 2]);
//     check(vec![0, 1, 4, 6, 7, 9, 11, 13], vec![5, 8, 5, 5, 8, 14, 5, 14]);
//     // let pointing_array = parrallel_arrays_to_pointing_array(ix.values1.clone(), ix.values2.clone());
//     // for key in ix.get_keys() {
//     //     assert_eq!(pointing_array.get_values(key as u64), ix.get_values(key as u64));
//     // }

//     // [0, 1, 4, 6, 7, 9, 11, 13]
//     // [5, 8, 5, 5, 8, 14, 5, 14]
// }

// #[derive(Debug)]
// #[allow(dead_code)]
// pub struct PointingArrayFileReader2<'a> {
//     pub start_and_end_file:  fs::File, // Vec<u32>  start, end, start, end
//     pub data_file:           fs::File, // Vec data
//     pub data_metadata:       fs::Metadata, // Vec data
//     pub persistence:         &'a Persistence, // Vec data
//     // pub persistence: String,
// }

// impl<'a>  IndexIdToParent for PointingArrayFileReader2<'a> {

//     fn get_values(&self, _find: u64) -> Option<Vec<u32>> {
//         None
//     }

//     fn get_keys(&self) -> Vec<T> {
//         unimplemented!()
//     }
// }
// impl<'a> HeapSizeOf for PointingArrayFileReader2<'a> {
//     fn heap_size_of_children(&self) -> usize {
//         0
//     }
// }

// impl<'a> TypeInfo for PointingArrayFileReader2<'a>   {
//     fn type_name(&self) -> String {
//         "String".to_string()
//     }
//     fn type_of(&self) -> String {
//         "String".to_string()
//     }
// }

fn load_bytes(file: &File, offset: u64, num_bytes: usize) -> Vec<u8> {
    let mut data = vec![];
    data.resize(num_bytes, 0);
    load_bytes_into(&mut data, file, offset);
    data
}

fn load_bytes_into(buffer: &mut Vec<u8>, mut file: &File, offset: u64) {
    // @Temporary Use Result
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read_exact(buffer).unwrap();
}

#[flame]
pub fn valid_pair_to_parallel_arrays<T: IndexIdToParentData>(tuples: &mut Vec<create::ValIdPair>) -> ParallelArrays<T> {
    tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
    let valids = tuples
        .iter()
        .map(|ref el| NumCast::from(el.valid).unwrap())
        .collect::<Vec<_>>();
    let parent_val_ids = tuples
        .iter()
        .map(|ref el| NumCast::from(el.parent_val_id).unwrap())
        .collect::<Vec<_>>();
    ParallelArrays {
        values1: valids,
        values2: parent_val_ids,
    }
}

#[flame]
pub fn boost_pair_to_parallel_arrays<T: IndexIdToParentData>(tuples: &mut Vec<create::ValIdToValue>) -> ParallelArrays<T> {
    tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
    let valids = tuples
        .iter()
        .map(|ref el| NumCast::from(el.valid).unwrap())
        .collect::<Vec<_>>();
    let values = tuples
        .iter()
        .map(|ref el| NumCast::from(el.value).unwrap())
        .collect::<Vec<_>>();
    ParallelArrays {
        values1: valids,
        values2: values,
    }
}

#[test]
fn test_index_parrallel_arrays() {
    let ix = ParallelArrays {
        values1: vec![0, 0, 1],
        values2: vec![0, 1, 2],
    };
    assert_eq!(ix.get_values(0).unwrap(), vec![0, 1]);
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

    let data4: Vec<Vec<u8>> = data.iter().map(|el| vec_to_bytes_u32(el)).collect();
    info!("data byteorder {:?}", data4.heap_size_of_children());

    let data5: Vec<Vec<u8>> = data.iter()
        .map(|el| {
            let mut dat = encoder.compress_vec(&vec_to_bytes_u32(el)).unwrap();
            dat.shrink_to_fit();
            dat
        })
        .collect();
    info!(
        "data byteorder compressed {:?}",
        data5.heap_size_of_children()
    );

    let mut wtr: Vec<u8> = vec![];
    wtr.write_u32::<LittleEndian>(10).unwrap();
    info!("wtr {:?}", wtr);
}

#[cfg(test)]
mod tests {
    use test;
    use super::*;
    use rand;

    fn get_test_data_1_to_1() -> IndexIdToOneParent<u64> {
        let values = vec![5, 6, 9, 9, 9, 50000];
        IndexIdToOneParent { data: values }
    }

    fn check_test_data_1_to_1(store: &IndexIdToParent<Output = u64>) {
        assert_eq!(store.get_keys(), vec![0, 1, 2, 3, 4, 5]);
        assert_eq!(store.get_value(0).unwrap(), 5);
        assert_eq!(store.get_value(1).unwrap(), 6);
        assert_eq!(store.get_value(2).unwrap(), 9);
        assert_eq!(store.get_value(3).unwrap(), 9);
        assert_eq!(store.get_value(4).unwrap(), 9);
        assert_eq!(store.get_value(5).unwrap(), 50000);
        assert_eq!(store.get_value(6), None);
    }

    fn get_test_data_1_to_n() -> ParallelArrays<u32> {
        let keys = vec![0, 0, 1, 2, 3, 3];
        let values = vec![5, 6, 9, 9, 9, 50000];

        let store = ParallelArrays {
            values1: keys.clone(),
            values2: values.clone(),
        };
        store
    }

    fn check_test_data_1_to_n(store: &IndexIdToParent<Output = u32>) {
        assert_eq!(store.get_keys(), vec![0, 1, 2, 3]);
        assert_eq!(store.get_values(0).unwrap(), vec![5, 6]);
        assert_eq!(store.get_values(1).unwrap(), vec![9]);
        assert_eq!(store.get_values(2).unwrap(), vec![9]);
        assert_eq!(store.get_values(3).unwrap(), vec![9, 50000]);
        assert_eq!(store.get_values(4), None);

        let mut vec = vec![];
        store.append_values_for_ids(&[0, 1, 2, 3, 4, 5], &mut vec);
        assert_eq!(vec, vec![5, 6, 9, 9, 9, 50000]);

        let map = store.count_values_for_ids(&[0, 1, 2, 3, 4, 5], None);
        assert_eq!(map.get(&5).unwrap(), &1);
        assert_eq!(map.get(&9).unwrap(), &3);
    }

    #[test]
    fn test_index_id_to_multiple_vec_vec_flat() {
        let data = get_test_data_1_to_n();
        let store = IndexIdToMultipleParent::new(&data);
        check_test_data_1_to_n(&store);
    }

    #[test]
    fn test_testdata() {
        let data = get_test_data_1_to_n();
        check_test_data_1_to_n(&data);
    }

    mod test_direct_1_to_1 {
        use super::*;

        #[test]
        fn test_single_file_array() {
            let store = get_test_data_1_to_1();

            fs::create_dir_all("test_single_file_array").unwrap();
            File::create("test_single_file_array/data")
                .unwrap()
                .write_all(&vec_to_bytes_u64(&store.data))
                .unwrap();

            let data_file = File::open(&get_file_path("test_single_file_array", "data")).unwrap();
            let data_metadata = fs::metadata(&get_file_path("test_single_file_array", "data")).unwrap();
            let store = SingleArrayFileReader::<u64>::new(data_file, data_metadata);
            check_test_data_1_to_1(&store);
        }

    }

    mod test_indirect {
        use super::*;
        use rand::distributions::{IndependentSample, Range};

        #[test]
        fn test_mayda_compressed_one() {
            let store = get_test_data_1_to_n();
            let mayda = IndexIdToMultipleParentCompressedMaydaINDIRECTOne::<u32>::new(&store);
            // let yep = to_uniform(&values);
            // assert_eq!(yep.access(0..=1), vec![5, 6]);
            check_test_data_1_to_n(&mayda);
        }

        fn get_test_data_large(num_ids: usize, max_num_values_per_id: usize) -> ParallelArrays<u32> {
            let mut rng = rand::thread_rng();
            let between = Range::new(0, max_num_values_per_id);

            let mut keys = vec![];
            let mut values = vec![];

            for x in 0..num_ids {
                let num_values = between.ind_sample(&mut rng) as u64;

                for _ in 0..num_values {
                    keys.push(x as u32);
                    // values.push(pseudo_rand((x as u32 * i as u32) as u32));
                    values.push(between.ind_sample(&mut rng) as u32);
                }
            }
            ParallelArrays {
                values1: keys,
                values2: values,
            }
        }

        #[bench]
        fn indirect_pointing_mayda(b: &mut test::Bencher) {
            let mut rng = rand::thread_rng();
            let between = Range::new(0, 40_000);
            let store = get_test_data_large(40_000, 15);
            let mayda = IndexIdToMultipleParentCompressedMaydaINDIRECTOne::<u32>::new(&store);

            b.iter(|| mayda.get_values(between.ind_sample(&mut rng)))
        }

        pub fn bench_fnvhashmap_group_by(num_entries: u32, max_val: u32) -> FnvHashMap<u32, u32> {
            let mut hits: FnvHashMap<u32, u32> = FnvHashMap::default();
            hits.reserve(num_entries as usize);
            let mut rng = rand::thread_rng();
            let between = Range::new(0, max_val);
            for _x in 0..num_entries {
                let stat = hits.entry(between.ind_sample(&mut rng)).or_insert(0);
                *stat += 1;
            }
            hits
        }

        pub fn bench_vec_group_by_direct(num_entries: u32, max_val: u32, hits: &mut Vec<u32>) -> &mut Vec<u32> {
            // let mut hits:Vec<u32> = vec![];
            hits.resize(max_val as usize + 1, 0);
            let mut rng = rand::thread_rng();
            let between = Range::new(0, max_val);
            for _x in 0..num_entries {
                hits[between.ind_sample(&mut rng) as usize] += 1;
            }
            hits
        }
        pub fn bench_vec_group_by_direct_u16(num_entries: u32, max_val: u32, hits: &mut Vec<u8>) -> &mut Vec<u8> {
            // let mut hits:Vec<u32> = vec![];
            hits.resize(max_val as usize + 1, 0);
            let mut rng = rand::thread_rng();
            let between = Range::new(0, max_val);
            for _x in 0..num_entries {
                hits[between.ind_sample(&mut rng) as usize] += 1;
            }
            hits
        }

        pub fn bench_vec_group_by_flex(num_entries: u32, max_val: u32) -> Vec<u32> {
            let mut hits: Vec<u32> = vec![];
            // hits.resize(max_val as usize + 1, 0);
            let mut rng = rand::thread_rng();
            let between = Range::new(0, max_val);
            for _x in 0..num_entries {
                let id = between.ind_sample(&mut rng) as usize;
                if hits.len() <= id {
                    hits.resize(id + 1, 0);
                }
                hits[id] += 1;
            }
            hits
        }

        // pub fn bench_vec_group_by_rand(num_entries: u32, max_val:u32) -> Vec<u32>{
        //     let mut hits:Vec<u32> = vec![];
        //     hits.resize(1, 0);
        //     let mut rng = rand::thread_rng();
        //     let between = Range::new(0, max_val);
        //     for x in 0..num_entries {
        //         hits[0] = between.ind_sample(&mut rng);
        //     }
        //     hits
        // }

        //20x break even ?
        #[bench]
        fn bench_group_by_fnvhashmap_0(b: &mut test::Bencher) {
            b.iter(|| {
                bench_fnvhashmap_group_by(700_000, 5_000_000);
            })
        }

        #[bench]
        fn bench_group_by_vec_direct_0(b: &mut test::Bencher) {
            b.iter(|| {
                bench_vec_group_by_direct(700_000, 5_000_000, &mut vec![]);
            })
        }
        #[bench]
        fn bench_group_by_vec_direct_u16_0(b: &mut test::Bencher) {
            b.iter(|| {
                bench_vec_group_by_direct_u16(700_000, 5_000_000, &mut vec![]);
            })
        }

        #[bench]
        fn bench_group_by_vec_direct_0_pre_alloc(b: &mut test::Bencher) {
            let mut dat = vec![];
            b.iter(|| {
                bench_vec_group_by_direct(700_000, 5_000_000, &mut dat);
            })
        }

        #[bench]
        fn bench_group_by_vec_flex_0(b: &mut test::Bencher) {
            b.iter(|| {
                bench_vec_group_by_flex(700_000, 5_000_000);
            })
        }
        // #[bench]
        // fn bench_group_by_rand_0(b: &mut test::Bencher) {
        //     b.iter(|| {
        //         bench_vec_group_by_rand(700_000, 50_000);
        //     })
        // }

        #[bench]
        fn indirect_pointing_uncompressed_im(b: &mut test::Bencher) {
            let mut rng = rand::thread_rng();
            let between = Range::new(0, 40_000);
            let store = get_test_data_large(40_000, 15);
            let mayda = IndexIdToMultipleParent::<u32>::new(&store);

            b.iter(|| mayda.get_values(between.ind_sample(&mut rng)))
        }

    }

}
