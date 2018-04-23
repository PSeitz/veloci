use std;
use std::cmp::Ordering;
use std::fs::File;

use heapsize::{HeapSizeOf};

use util::*;

use byteorder::{LittleEndian, ReadBytesExt};
use create;
use mayda;
use persistence::*;
pub use persistence_data_indirect::*;
use snap;

use facet::*;

use mayda::{Access, Encode};
use parking_lot::Mutex;

use num;
use num::cast::ToPrimitive;
use num::{NumCast};
use std::fs;
use std::io::Cursor;
use std::marker::PhantomData;

use type_info::TypeInfo;

use fnv::FnvHashMap;
use std::u32;

use memmap::Mmap;
use memmap::MmapOptions;

impl_type_info_single_templ!(IndexIdToMultipleParentCompressedMaydaDIRECT);
impl_type_info_single_templ!(IndexIdToMultipleParent);
impl_type_info_single_templ!(IndexIdToOneParentMayda); // TODO ADD TESTST FOR IndexIdToOneParentMayda
impl_type_info_single_templ!(IndexIdToOneParent);
// impl_type_info_single_templ!(FSTToOneParent);
impl_type_info_single_templ!(ParallelArrays);

impl_type_info_single_templ!(SingleArrayFileReader);
impl_type_info_single_templ!(SingleArrayMMAP);

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
        let vec: Option<Vec<T>> = self.data.get(id as usize).map(|el| el.iter().map(|el| NumCast::from(*el).unwrap()).collect());
        if vec.is_some() && vec.as_ref().unwrap().is_empty() {
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
            }
            for hit_id in &self.data[*id as usize] {
                let stat = hits.entry(*hit_id).or_insert(0);
                *stat += 1;
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
        self.data
            .get(id as usize)
            .map(|el| el.decode().iter().map(|el| NumCast::from(*el).unwrap()).collect())
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
    pub max_value_id: u32,
}
impl<T: IndexIdToParentData> IndexIdToOneParent<T> {
    pub fn new(data: &IndexIdToParent<Output = T>) -> IndexIdToOneParent<T> {
        let data: Vec<Vec<T>> = id_to_parent_to_array_of_array(data);
        let data = data.iter()
            .map(|el| {
                if !el.is_empty() {
                    NumCast::from(el[0]).unwrap()
                } else {
                    NumCast::from(NOT_FOUND).unwrap()
                }
            })
            .collect();
        IndexIdToOneParent { data, max_value_id: u32::MAX } //TODO FIX max_value_id
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
                if val.to_u64().unwrap() == u32::MAX as u64 {
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

    #[inline]
    fn count_values_for_ids(&self, ids: &[u32], top: Option<u32>) -> FnvHashMap<T, usize> {
        count_values_for_ids(ids, top, self.max_value_id, |id: u64| self.get_value(id))
    }
}

#[inline]
fn count_values_for_ids<T: IndexIdToParentData, F>(ids: &[u32], top: Option<u32>, max_value_id: u32, get_value: F) -> FnvHashMap<T, usize>
where
    F: Fn(u64) -> Option<T>,
{
    let mut coll: Box<AggregationCollector<T>> = get_collector(ids.len() as u32, 1.0, max_value_id);
    for id in ids {
        if let Some(hit) = get_value(*id as u64) {
            coll.add(hit);
        }
    }
    coll.to_map(top)
}

#[derive(Debug, HeapSizeOf)]
pub struct IndexIdToOneParentMayda<T: IndexIdToParentData> {
    pub data: mayda::Uniform<T>,
    pub size: usize,
    pub max_value_id: u32,
}
impl<T: IndexIdToParentData> IndexIdToOneParentMayda<T> {
    #[allow(dead_code)]
    pub fn new(data: &IndexIdToParent<Output = T>, max_value_id: u32) -> IndexIdToOneParentMayda<T> {
        let yep = IndexIdToOneParent::new(data);
        IndexIdToOneParentMayda {
            size: yep.data.len(),
            data: to_uniform(&yep.data),
            max_value_id,
        }
    }

    #[allow(dead_code)]
    pub fn from_vec(data: &[T], max_value_id: u32) -> IndexIdToOneParentMayda<T> {
        IndexIdToOneParentMayda {
            size: data.len(),
            data: to_uniform(data),
            max_value_id,
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
        if val.to_u64().unwrap() == u32::MAX as u64 {
            None
        } else {
            Some(val)
        }
    }

    #[inline]
    fn get_mutliple_value(&self, range: std::ops::RangeInclusive<usize>) -> Option<Vec<T>> {
        Some(self.data.access(range))
    }

    fn get_keys(&self) -> Vec<T> {
        (NumCast::from(0).unwrap()..NumCast::from(self.data.len()).unwrap()).collect()
    }

    #[inline]
    fn count_values_for_ids(&self, ids: &[u32], top: Option<u32>) -> FnvHashMap<T, usize> {
        count_values_for_ids(ids, top, self.max_value_id, |id: u64| self.get_value(id))
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
        if let Ok(mut pos) = self.values1.binary_search(&casted_id) {
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
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    fn get_keys(&self) -> Vec<T> {
        let mut keys: Vec<T> = self.values1.iter().map(|el| NumCast::from(*el).unwrap()).collect();
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
pub struct SingleArrayMMAP<T: IndexIdToParentData> {
    pub data_file: Mmap,                    //TODO CHECK MUTEX NEEEDED
    pub data_metadata: Mutex<fs::Metadata>, //TODO PLS FIXME max_value_id??, avg_join_size??
    pub max_value_id: u32,
    pub ok: PhantomData<T>,
}

impl<T: IndexIdToParentData> SingleArrayMMAP<T> {
    pub fn new(data_file: fs::File, data_metadata: fs::Metadata, max_value_id: u32) -> Self {
        let data_file = unsafe {
            MmapOptions::new()
                .len(std::cmp::max(data_metadata.len() as usize, 4048))
                .map(&data_file)
                .unwrap()
        };
        SingleArrayMMAP {
            data_file: data_file,
            data_metadata: Mutex::new(data_metadata),
            max_value_id,
            ok: PhantomData,
        }
    }

    fn get_size(&self) -> usize {
        self.data_metadata.lock().len() as usize / 4
    }
}
impl<T: IndexIdToParentData> HeapSizeOf for SingleArrayMMAP<T> {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for SingleArrayMMAP<T> {
    type Output = T;

    default fn get_value(&self, _find: u64) -> Option<T> {
        // implemented for u32, u64
        unimplemented!()
    }

    default fn get_values(&self, find: u64) -> Option<Vec<T>> {
        self.get_value(find).map(|el| vec![el])
    }

    fn get_keys(&self) -> Vec<T> {
        (NumCast::from(0).unwrap()..NumCast::from(self.get_size()).unwrap()).collect()
    }
}

impl IndexIdToParent for SingleArrayMMAP<u32> {
    #[inline]
    fn get_value(&self, find: u64) -> Option<u32> {
        if find >= self.get_size() as u64 {
            return None;
        }
        let pos = find as usize * 4;
        let id = (&self.data_file[pos..pos + 4]).read_u32::<LittleEndian>().unwrap();
        if id == u32::MAX {
            None
        } else {
            Some(NumCast::from(id).unwrap())
        }
    }
}
impl IndexIdToParent for SingleArrayMMAP<u64> {
    #[inline]
    fn get_value(&self, find: u64) -> Option<u64> {
        if find >= self.get_size() as u64 {
            return None;
        }
        let pos = find as usize * 8;
        let id = (&self.data_file[pos..pos + 8]).read_u64::<LittleEndian>().unwrap();
        if id == u32::MAX as u64 {
            None
        } else {
            Some(NumCast::from(id).unwrap())
        }
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
    #[inline]
    fn get_value(&self, find: u64) -> Option<u64> {
        get_reader(std::mem::size_of::<u64>(), find, 1, &self.data_file, &self.data_metadata)
            .map(|mut rdr| rdr.read_u64::<LittleEndian>().unwrap())
            .filter(|el| *el != num::cast::<u32, u64>(u32::MAX).unwrap())
    }

    #[inline]
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

    #[inline]
    fn get_values(&self, find: u64) -> Option<Vec<u64>> {
        self.get_value(find).map(|el| vec![el])
    }
}
impl IndexIdToParent for SingleArrayFileReader<u32> {
    #[inline]
    fn get_value(&self, find: u64) -> Option<u32> {
        get_reader(std::mem::size_of::<u32>(), find, 1, &self.data_file, &self.data_metadata)
            .map(|mut rdr| rdr.read_u32::<LittleEndian>().unwrap())
            .filter(|el| *el != u32::MAX)
    }

    #[inline]
    fn get_values(&self, find: u64) -> Option<Vec<u32>> {
        self.get_value(find).map(|el| vec![el])
    }

    #[inline]
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

#[inline]
fn get_bytes(block_size: usize, find: u64, num_elem: u64, data_file: &Mutex<fs::File>, data_metadata: &Mutex<fs::Metadata>) -> Option<Vec<u8>> {
    let size = data_metadata.lock().len() as usize / block_size;
    if find >= size as u64 {
        return None;
    }
    let data_bytes = load_bytes(&*data_file.lock(), find as u64 * block_size as u64, block_size * num_elem as usize);

    Some(data_bytes)
}

#[inline]
fn get_reader(block_size: usize, find: u64, num_elem: u64, data_file: &Mutex<fs::File>, data_metadata: &Mutex<fs::Metadata>) -> Option<Cursor<Vec<u8>>> {
    get_bytes(block_size, find, num_elem, data_file, data_metadata).map(Cursor::new)
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
    if valids.is_empty() {
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
pub fn to_uniform<T: mayda::utility::Bits>(data: &[T]) -> mayda::Uniform<T> {
    let mut uniform = mayda::Uniform::new();
    uniform.encode(data).unwrap();
    uniform
}
pub fn to_monotone<T: mayda::utility::Bits>(data: &[T]) -> mayda::Monotone<T> {
    let mut uniform = mayda::Monotone::new();
    uniform.encode(data).unwrap();
    uniform
}

fn load_bytes(file: &File, offset: u64, num_bytes: usize) -> Vec<u8> {
    let mut data = vec![];
    data.resize(num_bytes, 0);
    load_bytes_into(&mut data, file, offset);
    data
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn valid_pair_to_parallel_arrays<T: IndexIdToParentData>(tuples: &mut Vec<create::ValIdPair>) -> ParallelArrays<T> {
    tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
    let valids = tuples.iter().map(|el| NumCast::from(el.valid).unwrap()).collect::<Vec<_>>();
    let parent_val_ids = tuples.iter().map(|el| NumCast::from(el.parent_val_id).unwrap()).collect::<Vec<_>>();
    ParallelArrays {
        values1: valids,
        values2: parent_val_ids,
    }
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn boost_pair_to_parallel_arrays<T: IndexIdToParentData>(tuples: &mut Vec<create::ValIdToValue>) -> ParallelArrays<T> {
    tuples.sort_by(|a, b| a.valid.partial_cmp(&b.valid).unwrap_or(Ordering::Equal));
    let valids = tuples.iter().map(|el| NumCast::from(el.valid).unwrap()).collect::<Vec<_>>();
    let values = tuples.iter().map(|el| NumCast::from(el.value).unwrap()).collect::<Vec<_>>();
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
    use byteorder::WriteBytesExt;
    let mut encoder = snap::Encoder::new();
    let mut data: Vec<Vec<u32>> = vec![];
    data.push(vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 110, 111, 112, 113, 114, 115, 116, 117, 118]);
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
    info!("data byteorder compressed {:?}", data5.heap_size_of_children());

    let mut wtr: Vec<u8> = vec![];
    wtr.write_u32::<LittleEndian>(10).unwrap();
    info!("wtr {:?}", wtr);
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand;
    use test;

    fn get_test_data_1_to_1<T: IndexIdToParentData>() -> IndexIdToOneParent<T> {
        let values = vec![5, 6, 9, 9, 9, 50000];
        IndexIdToOneParent {
            data: values.iter().map(|el| NumCast::from(*el).unwrap()).collect(),
            max_value_id: 50000,
        }
    }

    fn check_test_data_1_to_1<T: IndexIdToParentData>(store: &IndexIdToParent<Output = T>) {
        assert_eq!(
            store.get_keys().iter().map(|el| el.to_u32().unwrap()).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5]
        );
        assert_eq!(store.get_value(0).unwrap().to_u32().unwrap(), 5);
        assert_eq!(store.get_value(1).unwrap().to_u32().unwrap(), 6);
        assert_eq!(store.get_value(2).unwrap().to_u32().unwrap(), 9);
        assert_eq!(store.get_value(3).unwrap().to_u32().unwrap(), 9);
        assert_eq!(store.get_value(4).unwrap().to_u32().unwrap(), 9);
        assert_eq!(store.get_value(5).unwrap().to_u32().unwrap(), 50000);
        assert_eq!(store.get_value(6), None);
    }

    mod test_direct_1_to_1 {
        use super::*;
        use std::io::prelude::*;
        use tempfile::tempfile;
        #[test]
        fn test_index_id_to_parent_im() {
            let store = get_test_data_1_to_1::<u32>();
            check_test_data_1_to_1(&store);
        }

        #[test]
        fn test_single_file_array_u64() {
            let store = get_test_data_1_to_1();

            let mut file = tempfile().unwrap();
            file.write_all(&vec_to_bytes_u64(&store.data)).unwrap();

            let meta_data = file.metadata().unwrap();
            let store = SingleArrayFileReader::<u64>::new(file, meta_data);
            check_test_data_1_to_1(&store);
        }

        #[test]
        fn test_single_file_array_u32() {
            let store = get_test_data_1_to_1();

            let mut file = tempfile().unwrap();

            file.write_all(&vec_to_bytes_u32(&store.data)).unwrap();

            let meta_data = file.metadata().unwrap();
            let store = SingleArrayFileReader::<u32>::new(file, meta_data);
            check_test_data_1_to_1(&store);
        }

    }

    mod test_indirect {
        use super::*;
        use rand::distributions::{IndependentSample, Range};

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
        pub fn bench_vec_group_by_direct_u8(num_entries: u32, max_val: u32, hits: &mut Vec<u8>) -> &mut Vec<u8> {
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
                bench_vec_group_by_direct_u8(700_000, 5_000_000, &mut vec![]);
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
