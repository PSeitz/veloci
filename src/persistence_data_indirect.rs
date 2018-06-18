use heapsize::HeapSizeOf;
use lru_cache::LruCache;
use util::*;

use byteorder::{LittleEndian, ReadBytesExt};

use persistence::*;
use type_info::TypeInfo;

// use mayda;
// use mayda::Encode;
use parking_lot::Mutex;

use std;
use std::fs;
use std::io;
use std::io::Write;

use num;
use num::cast::ToPrimitive;
use num::{Integer, NumCast};
use std::marker::PhantomData;

use facet::*;

use fnv::FnvHashMap;
use itertools::Itertools;

use memmap::Mmap;
use memmap::MmapOptions;

impl_type_info_single_templ!(IndexIdToMultipleParentIndirect);
// impl_type_info_single_templ!(IndexIdToMultipleParentIndirectFlushing);
// impl_type_info_single_templ!(IndexIdToMultipleParentCompressedMaydaINDIRECTOne);
// impl_type_info_single_templ!(IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse);
// impl_type_info_single_templ!(PointingArrayFileReader);
impl_type_info_single_templ!(PointingMMAPFileReader);

const EMPTY_BUCKET: u32 = 0;

pub fn calc_avg_join_size(num_values: u32, num_ids: u32) -> f32 {
    num_values as f32 / std::cmp::max(1, num_ids).to_f32().unwrap()
}

/// This data structure assumes that a set is only called once for a id, and ids are set in order.
#[derive(Serialize, Deserialize, Debug, Clone, HeapSizeOf)]
pub struct IndexIdToMultipleParentIndirectFlushingInOrder<T: IndexIdToParentData> {
    pub ids_cache: Vec<u32>,
    pub data_cache: Vec<T>,
    pub current_data_offset: u32,
    /// Already written ids_cache
    pub current_id_offset: u32,
    pub indirect_path: String,
    pub data_path: String,
    pub max_value_id: u32,
    pub avg_join_size: f32,
    pub num_values: u32,
    pub num_ids: u32,
}

pub fn flush_to_file_indirect(indirect_path: &str, data_path: &str, indirect_data: &[u8], data: &[u8]) -> Result<(), io::Error> {
    let mut indirect = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .append(true)
        .create(true)
        .open(&indirect_path)
        .unwrap();
    let mut data_cache = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .append(true)
        .create(true)
        .open(&data_path)
        .unwrap();

    indirect.write_all(indirect_data)?;
    data_cache.write_all(data)?;

    Ok(())
}

// TODO: Indirect Stuff @Performance @Memory
// use vint for data
// use vint for indirect, use not highest bit in indirect, but the highest unused bit. Max(value_id, single data_id, which would be encoded in the valueid index)
//
impl<T: IndexIdToParentData> IndexIdToMultipleParentIndirectFlushingInOrder<T> {
    pub fn new(indirect_path: String, data_path: String) -> Self {
        let mut data_cache = vec![];
        data_cache.resize(1, T::zero()); // resize data by one, because 0 is reserved for the empty buckets
        IndexIdToMultipleParentIndirectFlushingInOrder {
            ids_cache: vec![],
            data_cache,
            current_data_offset: 0,
            current_id_offset: 0,
            indirect_path,
            data_path,
            max_value_id: 0,
            avg_join_size: 0.,
            num_values: 0,
            num_ids: 0,
        }
    }

    pub fn into_im_store(self) -> IndexIdToMultipleParentIndirect<T> {
        let mut store = IndexIdToMultipleParentIndirect::default();
        //TODO this conversion is not needed, it's always u32
        store.start_pos = self.ids_cache.iter().map(|el| num::cast(*el).unwrap()).collect::<Vec<_>>();
        store.data = self.data_cache;
        store.max_value_id = self.max_value_id;
        store.avg_join_size = calc_avg_join_size(self.num_values, self.num_ids);
        store.num_values = self.num_values;
        store.num_ids = self.num_ids;
        store
    }

    #[inline]
    pub fn add(&mut self, id: u32, add_data: Vec<T>) -> Result<(), io::Error> {
        //set max_value_id
        for el in &add_data {
            self.max_value_id = std::cmp::max((*el).to_u32().unwrap(), self.max_value_id);
        }
        self.num_values += 1;
        self.num_ids += add_data.len() as u32;

        let id_pos = (id - self.current_id_offset) as usize;
        if self.ids_cache.len() <= id_pos {
            //TODO this could become very big, check memory consumption upfront, and flush directly to disk, when a resize would step over a certain threshold @Memory
            self.ids_cache.resize(id_pos + 1, EMPTY_BUCKET);
        }

        if add_data.len() == 1 {
            let mut val: u32 = add_data[0].to_u32().unwrap();
            set_high_bit(&mut val); // encode directly, much wow, much compression, gg memory consumption
            self.ids_cache[id_pos] = val;
        } else {
            self.ids_cache[id_pos] = self.current_data_offset + self.data_cache.len() as u32;
            self.data_cache.push(num::cast(add_data.len()).unwrap());
            self.data_cache.extend(add_data);
        }

        //TODO threshold 0 is buggy?
        if self.ids_cache.len() * 4 + self.data_cache.len() >= 4_000_000 {
            // TODO: Make good flushes every 4MB currently
            self.flush()?;
        }
        Ok(())
    }

    #[inline]
    pub fn is_in_memory(&self) -> bool {
        self.current_id_offset == 0
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ids_cache.is_empty() && self.current_id_offset == 0
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        if self.ids_cache.is_empty() {
            return Ok(());
        }

        //TODO this conversion is not needed, it's always u32
        let conv_to_u32 = self.data_cache.iter().map(|el| num::cast(*el).unwrap()).collect::<Vec<_>>();

        self.current_id_offset += self.ids_cache.len() as u32;
        self.current_data_offset += self.data_cache.len() as u32;

        flush_to_file_indirect(
            &self.indirect_path,
            &self.data_path,
            &vec_to_bytes_u32(&self.ids_cache),
            &vec_to_bytes_u32(&conv_to_u32),
        )?;

        self.data_cache.clear();
        self.ids_cache.clear();

        self.avg_join_size = calc_avg_join_size(self.num_values, self.num_ids);

        Ok(())
    }
}

// /// This data structure assumes that a set is only called once for a id.
// #[derive(Serialize, Deserialize, Debug, Clone, HeapSizeOf, Default)]
// pub struct IndexIdToMultipleParentIndirectFlushing<T: IndexIdToParentData> {
//     pub cache: Vec<(u32, Vec<T>)>,
//     pub indirect_path: String,
//     pub data_path: String,
//     pub max_value_id: u32,
//     pub avg_join_size: f32,
//     pub num_values: u32,
//     pub num_ids: u32,
// }

// impl<T: IndexIdToParentData> IndexIdToMultipleParentIndirectFlushing<T> {
//     pub fn set(&mut self, id: u32, add_data: Vec<T>) -> Result<(), io::Error> {
//         //set max_value_id
//         for el in add_data.iter() {
//             self.max_value_id = std::cmp::max((*el).to_u32().unwrap(), self.max_value_id);
//         }
//         self.num_values += 1;
//         self.num_ids += add_data.len() as u32;
//         self.cache.push((id, add_data));
//         if self.cache.len() >= 1000 {
//             // TODO: Make good
//             self.flush()?;
//         }
//         Ok(())
//     }

//     pub fn flush(&mut self) -> Result<(), io::Error> {
//         if self.cache.is_empty() {
//             return Ok(());
//         }
//         let mut indirect = std::fs::OpenOptions::new()
//             .read(true)
//             .write(true)
//             .create(true)
//             .open(&self.indirect_path)
//             .unwrap();
//         let mut data = std::fs::OpenOptions::new()
//             .read(true)
//             .write(true)
//             .append(true)
//             .create(true)
//             .open(&self.data_path)
//             .unwrap();
//         let curr_len = indirect.metadata()?.len();
//         let max_key = self.cache.iter().max_by_key(|el| el.0).unwrap().0;
//         let required_size = (max_key + 1) as u64 * std::mem::size_of::<u32>() as u64; // +1 because 0 needs 4 bytes
//         if required_size > curr_len as u64 {
//             indirect.set_len(required_size as u64)?;
//         }

//         let mut data_pos = data.metadata()?.len();
//         if data_pos == 0 {
//             data.set_len(4)?; // resize data by one, because 0 is reserved for the empty buckets
//             data_pos = data.metadata()?.len();
//         }
//         let mut id_to_data_pos = vec![];
//         let mut all_bytes = vec![];
//         self.cache.sort_unstable_by_key(|(id, _)| *id);
//         for (id, add_data) in self.cache.iter() {
//             if add_data.len() == 1 {
//                 let mut val: u32 = add_data[0].to_u32().unwrap();
//                 set_high_bit(&mut val); // encode directly, much wow, much compression
//                 id_to_data_pos.push((*id, val));
//             } else {
//                 let conv_to_u32 = add_data.iter().map(|el| num::cast(*el).unwrap()).collect::<Vec<_>>();
//                 let add_bytes = vec_to_bytes_u32(&conv_to_u32);
//                 id_to_data_pos.push((*id, data_pos as u32 / 4)); // as long as the data is nonpacked, we can multiply the offset always by 4 bytes, quadrupling the space which can be addressed with u32 to 16GB

//                 // add len of data
//                 let buffer: [u8; 4] = unsafe { std::mem::transmute(add_data.len() as u32) };
//                 data_pos += 4 as u64;
//                 all_bytes.extend(&buffer);

//                 data_pos += add_bytes.len() as u64;
//                 all_bytes.extend(add_bytes);
//             }
//         }
//         //write data file
//         data.write_all(&all_bytes)?;
//         //write indirect file
//         for (id, data_pos) in id_to_data_pos {
//             let buffer: [u8; 4] = unsafe { std::mem::transmute(data_pos) };
//             write_bytes_at(&buffer, &mut indirect, id as u64 * 4).unwrap();
//         }

//         self.avg_join_size = calc_avg_join_size(self.num_values, self.num_ids);
//         self.cache.clear();

//         Ok(())
//     }
// }

// /// This data structure allows multiple add calls for a id.
// #[derive(Serialize, Deserialize, Debug, Clone, HeapSizeOf, Default)]
// pub struct IndexIdToMultipleParentIndirectFlushingInc<T: IndexIdToParentData> {
//     // pub cache: Vec<(u32, Vec<T>)>,
//     pub cache: Vec<(u32, T)>,
//     pub indirect_path: String,
//     pub data_path: String,
//     pub max_value_id: u32,
//     pub avg_join_size: f32,
//     pub num_values: u32,
//     pub num_ids: u32,
//     pub cache_size: u32,
// }

// impl<T: IndexIdToParentData> IndexIdToMultipleParentIndirectFlushingInc<T> {
//     pub fn add_multi(&mut self, id: u32, add_data: Vec<T>) -> Result<(), io::Error> {
//         for el in add_data {
//             self.add(id, el)?;
//         }
//         Ok(())
//     }

//     #[inline]
//     pub fn add(&mut self, id: u32, value: T) -> Result<(), io::Error> {
//         self.max_value_id = std::cmp::max(value.to_u32().unwrap(), self.max_value_id);
//         self.num_values += 1;
//         self.num_ids += 1;
//         self.cache_size += 1;
//         self.cache.push((id, value));
//         if self.cache_size >= 1_000_000 {
//             // flush after 1Mb values
//             self.flush()?;
//             self.cache_size = 0;
//         }
//         Ok(())
//     }

//     pub fn flush(&mut self) -> Result<(), io::Error> {
//         if self.cache.is_empty() {
//             return Ok(());
//         }
//         let mut indirect = std::fs::OpenOptions::new()
//             .read(true)
//             .write(true)
//             .create(true)
//             .open(&self.indirect_path)
//             .unwrap();
//         let mut data = std::fs::OpenOptions::new()
//             .read(true)
//             .write(true)
//             .append(true)
//             .create(true)
//             .open(&self.data_path)
//             .unwrap();
//         let curr_len = indirect.metadata()?.len();
//         let max_key = self.cache.iter().max_by_key(|el| el.0).unwrap().0;
//         let required_size = (max_key + 1) as u64 * std::mem::size_of::<u32>() as u64; // +1 because 0 needs 4 bytes
//         if required_size > curr_len as u64 {
//             indirect.set_len(required_size as u64)?;
//         }

//         let mut data_pos = data.metadata()?.len();
//         if data_pos == 0 {
//             data.set_len(4)?; // resize data by one, because 0 is reserved for the empty buckets
//             data_pos = data.metadata()?.len();
//         }

//         let reader_of_existing_data = PointingArrayFileReader::<u32>::new_from_path(&self.indirect_path, &self.data_path);

//         let mut id_to_data_pos = vec![];
//         let mut all_bytes = vec![];
//         self.cache.sort_unstable_by_key(|(id, _)| *id);
//         for (id, mut group) in &self.cache.iter().group_by(|el| el.0) {
//             let mut add_data: Vec<u32> = group.map(|el| el.1).map(|el| el.to_u32().unwrap()).collect();
//             if let Some(existing_data) = reader_of_existing_data.get_values(id.into()) {
//                 add_data.extend(existing_data);
//             }

//             if add_data.len() == 1 {
//                 set_high_bit(&mut add_data[0]); // encode directly, much wow, much compression
//                 id_to_data_pos.push((id, add_data[0]));
//             } else {
//                 let add_bytes = vec_to_bytes_u32(&add_data);
//                 id_to_data_pos.push((id, data_pos as u32 / 4)); // as long as the data is nonpacked, we can multiply the offset always by 4 bytes, quadrupling the space which can be addressed with u32 to 16GB

//                 // add len of data
//                 let buffer: [u8; 4] = unsafe { std::mem::transmute(add_data.len() as u32) };
//                 data_pos += 4 as u64;
//                 all_bytes.extend(&buffer);

//                 data_pos += add_bytes.len() as u64;
//                 all_bytes.extend(add_bytes);
//             }
//         }
//         //write data file
//         data.write_all(&all_bytes)?;
//         //write indirect file
//         for (id, data_pos) in id_to_data_pos {
//             let buffer: [u8; 4] = unsafe { std::mem::transmute(data_pos) };
//             write_bytes_at(&buffer, &mut indirect, id as u64 * 4).unwrap();
//         }

//         self.avg_join_size = calc_avg_join_size(self.num_values, self.num_ids);
//         self.cache.clear();

//         Ok(())
//     }
// }

#[derive(Debug, Clone)]
pub struct IndexIdToMultipleParentIndirect<T: IndexIdToParentData> {
    pub start_pos: Vec<T>,
    pub cache: LruCache<Vec<T>, u32>,
    pub data: Vec<T>,
    pub max_value_id: u32,
    pub avg_join_size: f32,
    pub num_values: u32,
    pub num_ids: u32,
}
impl<T: IndexIdToParentData> HeapSizeOf for IndexIdToMultipleParentIndirect<T> {
    fn heap_size_of_children(&self) -> usize {
        self.start_pos.heap_size_of_children()
            + self.data.heap_size_of_children()
            + self.max_value_id.heap_size_of_children()
            + self.avg_join_size.heap_size_of_children()
            + self.num_values.heap_size_of_children()
            + self.num_ids.heap_size_of_children()
    }
}

impl<T: IndexIdToParentData> Default for IndexIdToMultipleParentIndirect<T> {
    fn default() -> IndexIdToMultipleParentIndirect<T> {
        let mut data = vec![];
        data.resize(1, T::zero()); // resize data by one, because 0 is reserved for the empty buckets
        IndexIdToMultipleParentIndirect {
            start_pos: vec![],
            cache: LruCache::new(250),
            data,
            max_value_id: 0,
            avg_join_size: 0.0,
            num_values: 0,
            num_ids: 0,
        }
    }
}

impl<T: IndexIdToParentData> IndexIdToMultipleParentIndirect<T> {
    #[inline]
    fn get_size(&self) -> usize {
        self.start_pos.len()
    }

    pub fn set(&mut self, id: u32, add_data: Vec<T>) {
        //TODO INVALIDATE OLD DATA IF SET TWICE?
        let pos: usize = id as usize;
        let required_size = pos + 1;
        if self.start_pos.len() < required_size {
            self.start_pos.resize(required_size, num::cast(EMPTY_BUCKET).unwrap());
        }
        let add_data_len = add_data.len();
        let start = self.data.len();
        if add_data.len() == 1 {
            let mut val: u32 = add_data[0].to_u32().unwrap();
            self.max_value_id = std::cmp::max(val, self.max_value_id);
            set_high_bit(&mut val); // encode directly, much wow, much compression
            self.start_pos[pos] = num::cast(val).unwrap();
        } else if let Some(&mut start) = self.cache.get_mut(&add_data) {
    //reuse and reference existing data
    self.start_pos[pos] = num::cast(start).unwrap();
} else {
    self.start_pos[pos] = num::cast(start).unwrap();
    self.data.push(num::cast(add_data.len()).unwrap());
    for val in add_data.iter() {
        self.max_value_id = std::cmp::max(val.to_u32().unwrap(), self.max_value_id);
        self.data.push(*val);
    }

    self.cache.insert(add_data, num::cast(start).unwrap());
    self.start_pos[pos] = num::cast(start).unwrap();
}
        self.num_values += 1;
        self.num_ids += add_data_len as u32;
        self.avg_join_size = calc_avg_join_size(self.num_values, self.num_ids);
    }

    #[allow(dead_code)]
    pub fn new_sort_and_dedup(data: &IndexIdToParent<Output = T>, sort_and_dedup: bool) -> IndexIdToMultipleParentIndirect<T> {
        let (max_value_id, num_values, num_ids, start_pos, data) = to_indirect_arrays_dedup(data, 0, sort_and_dedup);

        IndexIdToMultipleParentIndirect {
            start_pos,
            data,
            cache: LruCache::new(250),
            max_value_id: max_value_id.to_u32().unwrap(),
            avg_join_size: calc_avg_join_size(num_values, num_ids),
            num_values,
            num_ids,
        }
    }

    #[allow(dead_code)]
    pub fn new(data: &IndexIdToParent<Output = T>) -> IndexIdToMultipleParentIndirect<T> {
        IndexIdToMultipleParentIndirect::new_sort_and_dedup(data, false)
    }
}

#[test]
fn test_pointing_array_add() {
    let mut def = IndexIdToMultipleParentIndirect::<u32>::default();
    def.set(0 as u32, vec![1, 2, 3]);
    def.set(2 as u32, vec![3, 4, 3]);
    assert_eq!(def.get_values(0), Some(vec![1, 2, 3]));
    assert_eq!(def.get_values(2), Some(vec![3, 4, 3]));
    assert_eq!(def.get_values(1), None);
}
#[test]
fn test_pointing_array_add_out_of_order() {
    let mut def = IndexIdToMultipleParentIndirect::<u32>::default();
    def.set(5 as u32, vec![2, 0, 1]);
    def.set(3 as u32, vec![4, 0, 6]);
    def.set(8 as u32, vec![10]); //encoded

    assert_eq!(def.get_values(5), Some(vec![2, 0, 1]));
    assert_eq!(def.get_values(3), Some(vec![4, 0, 6]));
    assert_eq!(def.get_values(1), None);
    assert_eq!(def.get_keys(), vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);
}

impl<T: IndexIdToParentData> IndexIdToParent for IndexIdToMultipleParentIndirect<T> {
    type Output = T;

    #[inline]
    fn count_values_for_ids(&self, ids: &[u32], top: Option<u32>) -> FnvHashMap<T, usize> {
        // FIXME MAX ID WRONG SOMETIMES
        let mut coll: Box<AggregationCollector<T>> = get_collector(ids.len() as u32, self.avg_join_size, self.max_value_id);
        let size = self.get_size();

        let mut positions_vec = Vec::with_capacity(8);
        for id_chunk in &ids.into_iter().chunks(8) {
            for id in id_chunk {
                if *id >= size as u32 {
                    continue;
                }
                let pos = *id as usize;
                // let positions = &self.start_pos[pos..=pos + 1];
                let data_start_pos = self.start_pos[pos];
                let data_start_pos_length = data_start_pos.to_u32().unwrap();
                if let Some(val) = get_encoded(data_start_pos_length) {
                    coll.add(num::cast(val).unwrap());
                    continue;
                }
                // if positions[0].to_u32().unwrap() == u32::MAX {
                //     //data encoded in indirect array
                //     coll.add(positions[1]);
                //     continue;
                // }

                if data_start_pos_length != EMPTY_BUCKET {
                    positions_vec.push(data_start_pos_length);
                }
            }

            for position in &positions_vec {
                let length: u32 = NumCast::from(self.data[*position as usize]).unwrap();
                for id in &self.data[NumCast::from(*position + 1).unwrap()..NumCast::from(*position + 1 + length).unwrap()] {
                    // for id in &self.data[NumCast::from(position[0]).unwrap()..NumCast::from(position[1]).unwrap()] {
                    coll.add(*id);
                }
            }
            positions_vec.clear();
        }
        coll.to_map(top)
    }

    fn get_keys(&self) -> Vec<T> {
        (NumCast::from(0).unwrap()..NumCast::from(self.get_size()).unwrap()).collect()
    }

    #[inline]
    default fn get_values(&self, id: u64) -> Option<Vec<T>> {
        if id >= self.get_size() as u64 {
            None
        } else {
            // let positions = &self.start_pos[(id * 2) as usize..=((id * 2) as usize + 1)];
            let data_start_pos = self.start_pos[id as usize];
            let data_start_pos_length = data_start_pos.to_u32().unwrap();
            if let Some(val) = get_encoded(data_start_pos_length) {
                return Some(vec![NumCast::from(val).unwrap()]);
            }
            if data_start_pos_length == EMPTY_BUCKET {
                return None;
            }
            // if positions[0] == positions[1] {
            //     return None;
            // }
            let data_length: u32 = NumCast::from(self.data[data_start_pos_length as usize]).unwrap();
            let data_start_pos = data_start_pos_length + 1;
            let end: u32 = data_start_pos + data_length;
            Some(self.data[NumCast::from(data_start_pos).unwrap()..NumCast::from(end).unwrap()].to_vec())
        }
    }
}

// #[derive(Debug, HeapSizeOf)]
// #[allow(dead_code)]
// pub struct IndexIdToMultipleParentCompressedMaydaINDIRECTOne<T: IndexIdToParentData> {
//     pub start_pos: mayda::Monotone<T>,
//     pub data: mayda::Uniform<T>,
//     pub size: usize,
//     pub max_value_id: u32,
//     pub avg_join_size: f32,
// }

// impl<T: IndexIdToParentData> IndexIdToMultipleParentCompressedMaydaINDIRECTOne<T> {
//     #[allow(dead_code)]
//     pub fn new(store: &IndexIdToParent<Output = T>) -> IndexIdToMultipleParentCompressedMaydaINDIRECTOne<T> {
//         let (max_value_id, num_values, num_ids, size, start_pos, data) = id_to_parent_to_array_of_array_mayda_indirect_one(store);

//         info!("start_pos {}", get_readable_size(start_pos.heap_size_of_children()));
//         info!("data {}", get_readable_size(data.heap_size_of_children()));
//         IndexIdToMultipleParentCompressedMaydaINDIRECTOne {
//             start_pos,
//             data,
//             size,
//             max_value_id: NumCast::from(max_value_id).unwrap(),
//             avg_join_size: calc_avg_join_size(num_values, num_ids),
//         }
//     }
// }

// impl<T: IndexIdToParentData> IndexIdToParent for IndexIdToMultipleParentCompressedMaydaINDIRECTOne<T> {
//     type Output = T;

//     #[inline]
//     fn append_values(&self, id: u64, vec: &mut Vec<T>) {
//         if let Some(vals) = self.get_values(id) {
//             for id in vals {
//                 vec.push(id);
//             }
//         }
//     }

//     #[inline]
//     fn count_values_for_ids(&self, ids: &[u32], top: Option<u32>) -> FnvHashMap<T, usize> {
//         // Inserts are cheaper in a vec, bigger max_value_ids are more expensive in a vec
//         let mut coll: Box<AggregationCollector<T>> = get_collector(ids.len() as u32, self.avg_join_size, self.max_value_id);

//         // let mut data_cache:Vec<T> = vec![];
//         // let chunk_size = 8;
//         // let mut positions_vec = Vec::with_capacity(chunk_size * 2);
//         // positions_vec.resize(chunk_size * 2, T::zero());
//         // let mut current_pos = 0;
//         // // for id_chunk in &ids.into_iter().chunks(chunk_size) {
//         // for mut x in (0..ids.len()).step_by(chunk_size) {
//         //     // for id in &ids[x..x+chunk_size] {
//         //     let ende = std::cmp::min(x+chunk_size, ids.len());
//         //     for mut id_pos in x..ende {
//         //         let id = ids[id_pos];
//         //         if id >= self.size as u32 {
//         //             continue;
//         //         } else {
//         //             let start = (id * 2) as usize;
//         //             let mut end = start + 1;
//         //             let mut next_continuous_id = id+1;

//         //             while next_continuous_id < ids.len() as u32
//         //                 && next_continuous_id < self.size as u32
//         //                 && id_pos < ende
//         //                 && next_continuous_id == ids[id_pos+1]
//         //             {
//         //                 id_pos += 1;
//         //                 end = next_continuous_id as usize * 2 + 1;
//         //                 next_continuous_id+=1;
//         //             }

//         //             if start + 1 == end {
//         //                 self.start_pos.access_into(start ..= end, &mut positions_vec[current_pos ..= current_pos+1]);
//         //             }else{
//         //                 let start_pos_in_data = self.start_pos.access(start);
//         //                 let end_pos_in_data = self.start_pos.access(end);
//         //                 positions_vec[current_pos] = start_pos_in_data;
//         //                 positions_vec[current_pos+1] = end_pos_in_data;
//         //                 print!("start_pos_in_data {:?}", start_pos_in_data);
//         //                 print!("end_pos_in_data {:?}", end_pos_in_data);
//         //             }

//         //             if positions_vec[current_pos] != positions_vec[current_pos+1]{ // skip data with no values
//         //                 current_pos += 2;
//         //             }
//         //         }
//         //     }

//         //     for x in (0..current_pos).step_by(2) {
//         //         let end_pos_data = positions_vec[x+1].to_usize().unwrap();
//         //         let start_pos_data = positions_vec[x].to_usize().unwrap();
//         //         data_cache.resize(end_pos_data - start_pos_data, T::zero());
//         //         let new_len = data_cache.len();

//         //         self.data.access_into(start_pos_data..end_pos_data, &mut data_cache[0 .. new_len]);
//         //         for id in data_cache.iter() {
//         //             // let stat = hits.entry(*id).or_insert(0);
//         //             // *stat += 1;
//         //             coll.add(*id)
//         //         }
//         //     }
//         //     current_pos=0;
//         //     // x+=8;
//         // }

//         // let mut agg_hits = vec![];
//         // agg_hits.resize(256, 0);

//         // let mut positions:Vec<T> = vec![];
//         // positions.resize(2, T::zero());
//         // let mut data_cache:Vec<T> = vec![];
//         // let mut iter = ids.iter().peekable();
//         // while let Some(id) = iter.next(){

//         //     if *id >= self.size as u32 {
//         //         continue;
//         //     } else {

//         //         let mut end_id = *id;
//         //         let mut continuous_id = end_id+1;
//         //         loop{
//         //             if Some(&&continuous_id) == iter.peek(){
//         //                 let next = iter.next().unwrap() + 1;
//         //                 if next >= self.size as u32 {
//         //                     continue;
//         //                 }
//         //                 end_id = next;
//         //                 continuous_id = end_id+1;
//         //             }
//         //             else{
//         //                 break;
//         //             }
//         //             if end_id - *id > 64 {
//         //                 break; //group max 64 items
//         //             }
//         //         }

//         //         if *id == end_id {
//         //             self.start_pos.access_into((*id * 2) as usize..=((*id * 2) as usize + 1), &mut positions[0..=1]);
//         //         }else{
//         //             let start_pos_in_data = self.start_pos.access((*id * 2) as usize);
//         //             let end_pos_in_data = self.start_pos.access((end_id * 2) as usize + 1);
//         //             positions[0] = start_pos_in_data;
//         //             positions[1] = end_pos_in_data;
//         //         }

//         //         if positions[0] == positions[1] {
//         //             continue;
//         //         }

//         //         // let current_len = data_cache.len();
//         //         data_cache.resize(positions[1].to_usize().unwrap() - positions[0].to_usize().unwrap(), T::zero());
//         //         let new_len = data_cache.len();

//         //         self.data.access_into(NumCast::from(positions[0]).unwrap()..NumCast::from(positions[1]).unwrap(), &mut data_cache[0 .. new_len]);
//         //         for id in data_cache.iter() {
//         //             coll.add(*id);
//         //         }

//         //     }

//         // }

//         for id in ids {
//             if let Some(vals) = self.get_values(*id as u64) {
//                 for id in vals {
//                     coll.add(id);
//                 }
//             }
//         }
//         coll.to_map(top)

//         // let mut positions: Vec<T> = vec![];
//         // positions.resize(2, T::zero());
//         // let mut data_cache: Vec<T> = vec![];
//         // for id in ids {
//         //     if *id >= self.size as u32 {
//         //         continue;
//         //     }
//         //     let pos = self.start_pos.access(*id as usize);
//         //     // self.start_pos.access_into((*id * 2) as usize..=((*id * 2) as usize + 1), &mut positions[0..=1]);

//         //     if positions[0].to_u32().unwrap() == u32::MAX {
//         //         //data encoded in indirect array
//         //         coll.add(positions[1]);
//         //         continue;
//         //     }

//         //     if positions[0] == positions[1] {
//         //         continue;
//         //     }

//         //     // let current_len = data_cache.len();
//         //     data_cache.resize(positions[1].to_usize().unwrap() - positions[0].to_usize().unwrap(), T::zero());
//         //     let new_len = data_cache.len();

//         //     self.data.access_into(
//         //         NumCast::from(positions[0]).unwrap()..NumCast::from(positions[1]).unwrap(),
//         //         &mut data_cache[0..new_len],
//         //     );
//         //     for id in &data_cache {
//         //         coll.add(*id);
//         //     }
//         // }

//         // let hits: FnvHashMap<T, usize> = coll.to_map(top);

//         // hits
//     }

//     fn get_keys(&self) -> Vec<T> {
//         (NumCast::from(0).unwrap()..NumCast::from(self.start_pos.len()).unwrap()).collect()
//     }

//     #[inline]
//     fn get_values(&self, id: u64) -> Option<Vec<T>> {
//         get_values_indirect_generic(id, self.size as u64, &self.start_pos, &self.data)
//     }

//     // #[inline]
//     // fn get_count_for_id(&self, id: u64) -> Option<usize> {
//     //     if id >= self.size as u64 {
//     //         None
//     //     } else {
//     //         let positions = self.start_pos.access((id * 2) as usize..=((id * 2) as usize + 1));
//     //         (positions[1] - positions[0]).to_usize()
//     //     }
//     // }
// }

// // impl IndexIdToParent for IndexIdToMultipleParentCompressedMaydaINDIRECTOne<u32> {
// //     type Output = u32;
// //     fn get_values(&self, id: u64) -> Option<Vec<u32>> {
// //         get_values_indirect(id, self.size as u64, &self.start_pos, &self.data)
// //     }
// // }

// // #[inline(always)]
// // fn get_values_indirect<T, K>(id: u64, size:u64, start_pos: &T, data: &K) -> Option<Vec<u32>> where
// //     T: mayda::utility::Access<std::ops::RangeInclusive<usize>, Output=Vec<u32>> + mayda::utility::Access<std::ops::Range<usize>, Output=Vec<u32>>,
// //     K: mayda::utility::Access<std::ops::RangeInclusive<usize>, Output=Vec<u32>> + mayda::utility::Access<std::ops::Range<usize>, Output=Vec<u32>>
// //     {
// //     if id >= size { None }
// //     else {
// //         let positions = start_pos.access((id * 2) as usize..=((id * 2) as usize + 1));
// //         if positions[0] == positions[1] {return None}

// //         Some(data.access(positions[0] as usize .. positions[1] as usize))
// //     }
// // }

// #[inline]
// fn get_values_indirect_generic<T, K, M>(id: u64, size: u64, start_pos: &T, data: &K) -> Option<Vec<M>>
// where
//     T: mayda::utility::Access<std::ops::RangeInclusive<usize>, Output = Vec<M>>
//         + mayda::utility::Access<std::ops::Range<usize>, Output = Vec<M>>
//         + mayda::utility::Access<usize, Output = M>
//         + mayda::utility::AccessInto<std::ops::RangeInclusive<usize>, M>
//         + mayda::utility::AccessInto<std::ops::Range<usize>, M>,
//     K: mayda::utility::Access<std::ops::RangeInclusive<usize>, Output = Vec<M>>
//         + mayda::utility::Access<std::ops::Range<usize>, Output = Vec<M>>
//         + mayda::utility::Access<usize, Output = M>
//         + mayda::utility::AccessInto<std::ops::RangeInclusive<usize>, M>
//         + mayda::utility::AccessInto<std::ops::Range<usize>, M>,
//     M: IndexIdToParentData,
// {
//     if id >= size as u64 {
//         None
//     } else {
//         // start_pos.access_into((id * 2) as usize..=((id * 2) as usize + 1), &mut positions[0..=1]);

//         let data_start_pos = start_pos.access(id as usize);
//         let data_start_pos_length = data_start_pos.to_u32().unwrap();
//         if let Some(val) = get_encoded(data_start_pos_length) {
//             return Some(vec![NumCast::from(val).unwrap()]);
//         }
//         if data_start_pos_length == EMPTY_BUCKET {
//             return None;
//         }

//         let data_length: u32 = NumCast::from(data.access(data_start_pos_length as usize)).unwrap();
//         let data_start_pos = data_start_pos_length + 1;
//         let end: u32 = data_start_pos + data_length;
//         Some(data.access(NumCast::from(data_start_pos).unwrap()..NumCast::from(end).unwrap()))
//     }
// }

// #[derive(Debug, HeapSizeOf)]
// #[allow(dead_code)]
// pub struct IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse<T: IndexIdToParentData> {
//     start_pos: mayda::Uniform<T>,
//     data: mayda::Uniform<T>,
//     size: usize,
// }
// impl<T: IndexIdToParentData> IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse<T> {
//     #[allow(dead_code)]
//     pub fn new(store: &IndexIdToParent<Output = T>) -> IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse<T> {
//         let (_max_value_id, _num_values, _num_ids, size, start_pos, data) = id_to_parent_to_array_of_array_mayda_indirect_one_reuse_existing(store);

//         info!("start_pos {}", get_readable_size(start_pos.heap_size_of_children()));
//         info!("data {}", get_readable_size(data.heap_size_of_children()));

//         IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse { start_pos, data, size }
//     }
// }

// impl<T: IndexIdToParentData> IndexIdToParent for IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse<T> {
//     type Output = T;

//     fn get_keys(&self) -> Vec<T> {
//         (NumCast::from(0).unwrap()..NumCast::from(self.start_pos.len()).unwrap()).collect()
//     }

//     #[inline]
//     fn get_values(&self, id: u64) -> Option<Vec<T>> {
//         get_values_indirect_generic(id, self.size as u64, &self.start_pos, &self.data)
//     }
// }

#[derive(Debug)]
pub struct PointingMMAPFileReader<T: IndexIdToParentData> {
    pub start_and_end_file: Mmap,
    pub data_file: Mmap,
    pub indirect_metadata: Mutex<fs::Metadata>,
    pub ok: PhantomData<T>,
    pub max_value_id: u32,
    pub avg_join_size: f32,
}

impl<T: IndexIdToParentData> PointingMMAPFileReader<T> {
    #[inline]
    fn get_size(&self) -> usize {
        self.indirect_metadata.lock().len() as usize / 4
    }

    pub fn new(
        start_and_end_file: &fs::File,
        data_file: &fs::File,
        indirect_metadata: fs::Metadata,
        _data_metadata: &fs::Metadata,
        max_value_id: u32,
        avg_join_size: f32,
    ) -> Self {
        let start_and_end_file = unsafe {
            MmapOptions::new()
                // .len(std::cmp::max(indirect_metadata.len() as usize, 4048))
                .map(&start_and_end_file)
                .unwrap()
        };
        let data_file = unsafe {
            MmapOptions::new()
                // .len(std::cmp::max(data_metadata.len() as usize, 4048))
                .map(&data_file)
                .unwrap()
        };
        PointingMMAPFileReader {
            start_and_end_file,
            data_file,
            indirect_metadata: Mutex::new(indirect_metadata),
            ok: PhantomData,
            max_value_id,
            avg_join_size,
        }
    }
}

impl<T: IndexIdToParentData> HeapSizeOf for PointingMMAPFileReader<T> {
    fn heap_size_of_children(&self) -> usize {
        0 //FIXME
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for PointingMMAPFileReader<T> {
    type Output = T;

    fn get_keys(&self) -> Vec<T> {
        (NumCast::from(0).unwrap()..NumCast::from(self.get_size()).unwrap()).collect()
    }

    default fn get_values(&self, find: u64) -> Option<Vec<T>> {
        get_u32_values_from_pointing_mmap_file(
            //FIXME BUG BUG if file is not u32
            find,
            self.get_size(),
            &self.start_and_end_file,
            &self.data_file,
        ).map(|el| el.iter().map(|el| NumCast::from(*el).unwrap()).collect())
    }
}

#[inline(always)]
fn get_u32_values_from_pointing_mmap_file(find: u64, size: usize, start_pos: &Mmap, data_file: &Mmap) -> Option<Vec<u32>> {
    // dump!(bytes_to_vec_u32(&start_pos[..]), bytes_to_vec_u32(&data_file[..]));
    // trace_time!("get_u32_values_from_mmap_file");
    if find >= size as u64 {
        None
    } else {
        let start_index = find as usize * 4;
        let data_start_pos = (&start_pos[start_index as usize..start_index + 4]).read_u32::<LittleEndian>().unwrap();

        let data_start_pos_length = data_start_pos.to_u32().unwrap();
        if let Some(val) = get_encoded(data_start_pos_length) {
            return Some(vec![NumCast::from(val).unwrap()]);
        }
        if data_start_pos_length == EMPTY_BUCKET {
            return None;
        }
        let data_length_index = data_start_pos as usize * 4;
        let data_length: u32 = (&data_file[data_length_index..data_length_index + 4]).read_u32::<LittleEndian>().unwrap();
        // let data_start_pos = data_start_pos_length + 1;
        let data_start_index = data_length_index + 4;
        let end: usize = (data_start_index + data_length as usize * 4) as usize;
        Some(bytes_to_vec_u32(&data_file[data_start_index..end]))
    }

    // let start_pos = find as usize * 8;
    // let start = (&start_pos[start_pos..start_pos + 4]).read_u32::<LittleEndian>().unwrap();
    // let end = (&start_pos[start_pos + 4..start_pos + 8]).read_u32::<LittleEndian>().unwrap();

    // if start == u32::MAX {
    //     //data encoded in indirect array
    //     return Some(vec![end]);
    // }

    // if start == end {
    //     return None;
    // }

    // trace_time!("mmap bytes_to_vec_u32");
    // Some(bytes_to_vec_u32(&data_file[start as usize * 4..end as usize * 4]))
}

// #[derive(Debug)]
// pub struct PointingArrayFileReader<T: IndexIdToParentData> {
//     pub start_and_end_file: Mutex<fs::File>,
//     pub data_file: Mutex<fs::File>,
//     pub start_and_end_: Mutex<fs::Metadata>,
//     pub ok: PhantomData<T>,
//     pub max_value_id: u32,
//     pub avg_join_size: f32,
// }

// impl<T: IndexIdToParentData> PointingArrayFileReader<T> {
//     #[inline]
//     fn get_size(&self) -> usize {
//         self.start_and_end_.lock().len() as usize / 4
//     }

//     pub fn new(start_and_end_file: fs::File, data_file: fs::File, start_and_end_: fs::Metadata, max_value_id: u32, avg_join_size: f32) -> Self {
//         PointingArrayFileReader {
//             start_and_end_file: Mutex::new(start_and_end_file),
//             data_file: Mutex::new(data_file),
//             start_and_end_: Mutex::new(start_and_end_),
//             ok: PhantomData,
//             max_value_id: max_value_id,
//             avg_join_size: avg_join_size,
//         }
//     }

//     pub fn new_from_path(start_and_end_file: &str, data_file: &str) -> Self {
//         PointingArrayFileReader {
//             start_and_end_file: Mutex::new(File::open(start_and_end_file).unwrap()),
//             data_file: Mutex::new(File::open(data_file).unwrap()),
//             start_and_end_: Mutex::new(File::open(start_and_end_file).unwrap().metadata().unwrap()),
//             ok: PhantomData,
//             max_value_id: 0,
//             avg_join_size: 0.,
//         }
//     }
// }

// impl<T: IndexIdToParentData> IndexIdToParent for PointingArrayFileReader<T> {
//     type Output = T;

//     fn get_keys(&self) -> Vec<T> {
//         (NumCast::from(0).unwrap()..NumCast::from(self.get_size()).unwrap()).collect()
//     }

//     default fn get_values(&self, find: u64) -> Option<Vec<T>> {
//         get_u32_values_from_pointing_file(
//             //FIXME BUG BUG if file is not u32
//             find,
//             self.get_size(),
//             &self.start_and_end_file,
//             &self.data_file,
//         ).map(|el| el.iter().map(|el| NumCast::from(*el).unwrap()).collect())
//     }
// }
// impl<T: IndexIdToParentData> HeapSizeOf for PointingArrayFileReader<T> {
//     fn heap_size_of_children(&self) -> usize {
//         0
//     }
// }

// impl IndexIdToParent for PointingArrayFileReader<u32> {
//     #[inline]
//     fn count_values_for_ids(&self, ids: &[u32], top: Option<u32>) -> FnvHashMap<u32, usize> {
//         // Inserts are cheaper in a vec, bigger max_value_ids are more expensive in a vec
//         let mut coll: Box<AggregationCollector<u32>> = get_collector(ids.len() as u32, self.avg_join_size, self.max_value_id);

//         let size = self.get_size();
//         for id in ids {
//             //TODO don't copy, just stream ids
//             if let Some(vals) = get_u32_values_from_pointing_file(*id as u64, size, &self.start_and_end_file, &self.data_file) {
//                 for id in vals {
//                     coll.add(id);
//                 }
//             }
//         }
//         coll.to_map(top)
//     }

//     #[inline]
//     fn get_values(&self, find: u64) -> Option<Vec<u32>> {
//         get_u32_values_from_pointing_file(find, self.get_size(), &self.start_and_end_file, &self.data_file)
//     }
// }

fn get_encoded(mut val: u32) -> Option<u32> {
    if is_hight_bit_set(val) {
        //data encoded in indirect array
        unset_high_bit(&mut val);
        Some(val)
    } else {
        None
    }
}

// #[inline(always)]
// fn get_u32_values_from_pointing_file(find: u64, size: usize, start_and_end_file: &Mutex<fs::File>, data_file: &Mutex<fs::File>) -> Option<Vec<u32>> {
//     // trace_time!("get_u32_values_from_pointing_file");
//     if find >= size as u64 {
//         return None;
//     }
//     let mut offsets: Vec<u8> = vec_with_size_uninitialized(4);
//     load_bytes_into(&mut offsets, &*start_and_end_file.lock(), find as u64 * 4);

//     let pos_in_data_or_encoded_id = {
//         let mut rdr = Cursor::new(&offsets);
//         rdr.read_u32::<LittleEndian>().unwrap() //TODO AVOID CONVERT
//     };

//     if let Some(val) = get_encoded(pos_in_data_or_encoded_id) {
//         return Some(vec![val]);
//     }

//     if pos_in_data_or_encoded_id == EMPTY_BUCKET {
//         return None;
//     }

//     load_bytes_into(&mut offsets, &*data_file.lock(), pos_in_data_or_encoded_id as u64 * 4); // first el is length of data
//     let mut rdr = Cursor::new(offsets);
//     let data_start = pos_in_data_or_encoded_id + 1; // actual data starts after len
//     let data_end = data_start + rdr.read_u32::<LittleEndian>().unwrap(); //TODO AVOID CONVERT JUST CAST LIEKE A DAREDEVIL

//     Some(get_my_data_danger_zooone(data_start, data_end, data_file))
// }

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

// //TODO TRY WITH FROM ITERATOR oder so
// pub fn to_uniform<T: mayda::utility::Bits>(data: &[T]) -> mayda::Uniform<T> {
//     let mut uniform = mayda::Uniform::new();
//     uniform.encode(data).unwrap();
//     uniform
// }
// pub fn to_monotone<T: mayda::utility::Bits>(data: &[T]) -> mayda::Monotone<T> {
//     let mut uniform = mayda::Monotone::new();
//     uniform.encode(data).unwrap();
//     uniform
// }

// fn to_indirect_arrays<T: Integer + Clone + NumCast + Copy + IndexIdToParentData, K: IndexIdToParentData>(
//     store: &IndexIdToParent<Output = K>,
//     cache_size: usize,
// ) -> (T, u32, u32, Vec<T>, Vec<T>) {
//     to_indirect_arrays_dedup(store, cache_size, false)
// }

fn to_indirect_arrays_dedup<T: Integer + Clone + NumCast + Copy + IndexIdToParentData, K: IndexIdToParentData>(
    store: &IndexIdToParent<Output = K>,
    _cache_size: usize,
    sort_and_dedup: bool,
) -> (T, u32, u32, Vec<T>, Vec<T>) {
    // let mut data = vec![];
    let mut valids = store.get_keys();
    valids.dedup();
    if valids.is_empty() {
        return (T::zero(), 0, 0, vec![], vec![]);
    }
    // let mut start_pos_in_data = vec![];
    let last_id = *valids.last().unwrap();
    // start_pos_in_data.resize((valids.last().unwrap().to_usize().unwrap() + 1) * 2, num::cast(u32::MAX).unwrap()); // don't use u32::MAX u32::MAX means the data is directly encoded

    // let mut offset = 0;

    // let num_ids = last_id;
    // let mut num_values = 0;

    // let mut cache = LruCache::new(cache_size);

    // let mut max_value_id = T::zero();

    let mut yepp = IndexIdToMultipleParentIndirect::<T> {
        start_pos: vec![],
        data: vec![],
        cache: LruCache::new(250),
        max_value_id: 0,
        avg_join_size: 0.,
        num_values: 0,
        num_ids: 0,
    };
    yepp.data.resize(1, T::zero()); // resize data by one, because 0 is reserved for the empty buckets

    for valid in 0..=num::cast(last_id).unwrap() {
        //let start = offset;
        if let Some(mut vals) = store.get_values(valid as u64) {
            if sort_and_dedup {
                vals.sort();
                vals.dedup();
            }
            yepp.set(valid, vals.iter().map(|el| num::cast(*el).unwrap()).collect::<Vec<_>>());
        }
    }

    // for valid in 0..=num::cast(last_id).unwrap() {
    //     //let start = offset;
    //     if let Some(mut vals) = store.get_values(valid as u64) {
    //         num_values += vals.len();
    //         if vals.len() == 1 {

    //             max_value_id = std::cmp::max(max_value_id, num::cast(vals[0]).unwrap());

    //             // Special Case Decode value direct into start and end, start is u32::MAX and end is da value
    //             let mut val: u32 = num::cast(vals[0]).unwrap();
    //             set_high_bit(&mut val);
    //             start_pos_in_data[valid as usize] = num::cast(val).unwrap();

    //             // start_pos_in_data[valid as usize * 2] = num::cast(u32::MAX).unwrap();
    //             // start_pos_in_data[(valid as usize * 2) + 1] = num::cast(vals[0]).unwrap();
    //             continue;
    //         }
    //         if sort_and_dedup {
    //             vals.sort();
    //             vals.dedup();
    //         }

    //         if let Some(&mut start) = cache.get_mut(&vals) {
    //             //reuse and reference existing data
    //             start_pos_in_data[valid as usize] = start;
    //             // start_pos_in_data[valid as usize * 2] = start;
    //             // start_pos_in_data[(valid as usize * 2) + 1] = offset;
    //         } else {
    //             let start = offset;

    //             data.push(num::cast(vals.len()).unwrap()); // data knows its length
    //             offset += 1;
    //             for val in &vals {
    //                 max_value_id = std::cmp::max(max_value_id, num::cast(*val).unwrap());
    //                 data.push(num::cast(*val).unwrap());
    //             }
    //             offset += vals.len() as u64;

    //             if cache_size > 0 {
    //                 cache.insert(vals, num::cast(start).unwrap());
    //             }
    //             start_pos_in_data[valid as usize] = num::cast(start).unwrap();
    //             // start_pos_in_data[valid as usize * 2] = num::cast(start).unwrap();
    //             // start_pos_in_data[(valid as usize * 2) + 1] = num::cast(offset).unwrap();
    //         }
    //     } else {
    //         // add latest offsets, so the data is monotonically increasing -> better compression
    //         // start_pos_in_data[valid as usize] = num::cast(start).unwrap();
    //         // start_pos_in_data[valid as usize * 2] = num::cast(start).unwrap();
    //         // start_pos_in_data[(valid as usize * 2) + 1] = num::cast(offset).unwrap();
    //     }
    // }
    // data.shrink_to_fit();
    // let max_value_id = *data.iter().max_by_key(|el| *el).unwrap_or(&T::zero());

    // trace!("start_pos_in_data {:?}", yepp.start_pos.iter().map(|el:&T|el.to_u32().unwrap()).collect::<Vec<_>>());
    // trace!("data {:?}",              yepp.data.iter().map(|el:&T|el.to_u32().unwrap()).collect::<Vec<_>>());

    // let avg_join_size = num_values as f32 / std::cmp::max(K::one(), num_ids).to_f32().unwrap();
    // (max_value_id, num_values as u32, num::cast(num_ids).unwrap(), start_pos_in_data, data)
    (
        num::cast(yepp.max_value_id).unwrap(),
        yepp.num_values as u32,
        num::cast(yepp.num_ids).unwrap(),
        yepp.start_pos,
        yepp.data,
    )
}

// pub fn id_to_parent_to_array_of_array_mayda_indirect_one<
//     T: Integer + Clone + NumCast + mayda::utility::Bits + Copy + IndexIdToParentData,
//     K: IndexIdToParentData,
// >(
//     store: &IndexIdToParent<Output = K>,
// ) -> (T, u32, u32, usize, mayda::Monotone<T>, mayda::Uniform<T>) {
//     //start, end, data
//     let (max_value_id, num_values, num_ids, start_pos, data) = to_indirect_arrays(store, 0);
//     (max_value_id, num_values, num_ids, start_pos.len(), to_monotone(&start_pos), to_uniform(&data))
// }

// pub fn id_to_parent_to_array_of_array_mayda_indirect_one_reuse_existing<
//     T: Integer + Clone + NumCast + mayda::utility::Bits + Copy + IndexIdToParentData,
//     K: IndexIdToParentData,
// >(
//     store: &IndexIdToParent<Output = K>,
// ) -> (T, u32, u32, usize, mayda::Uniform<T>, mayda::Uniform<T>) {
//     //start, end, data
//     let (max_value_id, num_values, num_ids, start_pos, data) = to_indirect_arrays(store, 250);
//     (max_value_id, num_values, num_ids, start_pos.len(), to_uniform(&start_pos), to_uniform(&data))
// }

use std::u32;

#[cfg(test)]
mod tests {
    use super::*;
    use persistence_data::*;
    use std::fs::File;

    fn get_test_data_1_to_n() -> ParallelArrays<u32> {
        let keys = vec![0, 0, 1, 2, 3, 3, 5, 9, 10];
        let values = vec![5, 6, 9, 9, 9, 50000, 80, 0, 0];

        let store = ParallelArrays {
            values1: keys.clone(),
            values2: values.clone(),
        };
        store
    }

    fn check_test_data_1_to_n(store: &IndexIdToParent<Output = u32>) {
        assert_eq!(store.get_keys(), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(store.get_values(0), Some(vec![5, 6]));
        assert_eq!(store.get_values(1), Some(vec![9]));
        assert_eq!(store.get_values(2), Some(vec![9]));
        assert_eq!(store.get_values(3), Some(vec![9, 50000]));
        assert_eq!(store.get_values(4), None);
        assert_eq!(store.get_values(5), Some(vec![80]));
        assert_eq!(store.get_values(6), None);
        assert_eq!(store.get_values(9), Some(vec![0]));
        assert_eq!(store.get_values(10), Some(vec![0]));
        assert_eq!(store.get_values(11), None);

        let map = store.count_values_for_ids(&[0, 1, 2, 3, 4, 5], None);
        assert_eq!(map.get(&5).unwrap(), &1);
        assert_eq!(map.get(&9).unwrap(), &3);
    }

    mod test_indirect {
        use super::*;
        use tempfile::tempdir;
        #[test]
        fn test_pointing_file_andmmap_array() {
            let store = get_test_data_1_to_n();
            let (max_value_id, num_values, num_ids, keys, values) = to_indirect_arrays_dedup(&store, 0, false);
            let dir = tempdir().unwrap();
            let indirect_path = dir.path().join("indirect");
            let data_path = dir.path().join("data");
            File::create(&indirect_path).unwrap().write_all(&vec_to_bytes_u32(&keys)).unwrap();
            File::create(&data_path).unwrap().write_all(&vec_to_bytes_u32(&values)).unwrap();

            let start_and_end_file = File::open(&indirect_path).unwrap();
            let data_file = File::open(&data_path).unwrap();
            let indirect_metadata = fs::metadata(&indirect_path).unwrap();
            let data_metadata = fs::metadata(&data_path).unwrap();

            let store = PointingMMAPFileReader::new(
                &start_and_end_file,
                &data_file,
                indirect_metadata,
                &data_metadata,
                max_value_id,
                calc_avg_join_size(num_values, num_ids),
            );
            check_test_data_1_to_n(&store);
            // let indirect_metadata = fs::metadata(&indirect_path).unwrap();
            // let store = PointingArrayFileReader::new(
            //     start_and_end_file,
            //     data_file,
            //     indirect_metadata,
            //     max_value_id,
            //     calc_avg_join_size(num_values, num_ids),
            // );
            // check_test_data_1_to_n(&store);
        }

        #[test]
        fn test_flushing_in_order_indirect() {
            let store = get_test_data_1_to_n();

            let dir = tempdir().unwrap();
            let indirect_path = dir.path().join(".indirect");
            let data_path = dir.path().join(".data");

            let mut ind = IndexIdToMultipleParentIndirectFlushingInOrder::<u32>::new(
                indirect_path.to_str().unwrap().to_string(),
                data_path.to_str().unwrap().to_string(),
            );

            for key in store.get_keys() {
                if let Some(vals) = store.get_values(key.into()) {
                    ind.add(key, vals).unwrap();
                    ind.flush().unwrap();
                }
            }
            ind.flush().unwrap();

            let start_and_end_file = File::open(&indirect_path).unwrap();
            let data_file = File::open(&data_path).unwrap();
            let indirect_metadata = fs::metadata(&indirect_path).unwrap();
            let data_metadata = fs::metadata(&data_path).unwrap();

            let store = PointingMMAPFileReader::new(
                &start_and_end_file,
                &data_file,
                indirect_metadata,
                &data_metadata,
                ind.max_value_id,
                calc_avg_join_size(ind.num_values, ind.num_ids),
            );
            check_test_data_1_to_n(&store);
        }

        // #[test]
        // fn test_flushing_indirect() {
        //     let store = get_test_data_1_to_n();

        //     let dir = tempdir().unwrap();
        //     let indirect_path = dir.path().join(".indirect");
        //     let data_path = dir.path().join(".data");

        //     // File::create(&indirect_path).unwrap();
        //     // File::create(&data_path).unwrap();

        //     let mut ind = IndexIdToMultipleParentIndirectFlushing::<u32>::default();
        //     ind.indirect_path = indirect_path.to_str().unwrap().to_string();
        //     ind.data_path = data_path.to_str().unwrap().to_string();
        //     // ind.path = dir.path().to_str().unwrap().to_string();

        //     for key in store.get_keys() {
        //         if let Some(vals) = store.get_values(key.into()) {
        //             ind.set(key, vals).unwrap();
        //         }
        //     }
        //     ind.flush().unwrap();

        //     let start_and_end_file = File::open(&indirect_path).unwrap();
        //     let data_file = File::open(&data_path).unwrap();
        //     let indirect_metadata = fs::metadata(&indirect_path).unwrap();
        //     let data_metadata = fs::metadata(&data_path).unwrap();

        //     let store = PointingMMAPFileReader::new(
        //         &start_and_end_file,
        //         &data_file,
        //         indirect_metadata,
        //         &data_metadata,
        //         ind.max_value_id,
        //         calc_avg_join_size(ind.num_values, ind.num_ids),
        //     );
        //     check_test_data_1_to_n(&store);
        // }

        // #[test]
        // fn test_flushing_inc_indirect() {
        //     let store = get_test_data_1_to_n();

        //     let dir = tempdir().unwrap();
        //     let indirect_path = dir.path().join(".indirect");
        //     let data_path = dir.path().join(".data");

        //     let mut ind = IndexIdToMultipleParentIndirectFlushingInc::<u32>::default();
        //     ind.indirect_path = indirect_path.to_str().unwrap().to_string();
        //     ind.data_path = data_path.to_str().unwrap().to_string();

        //     for key in store.get_keys() {
        //         if let Some(vals) = store.get_values(key.into()) {
        //             ind.add_multi(key, vals).unwrap();
        //             ind.flush().unwrap();
        //         }
        //     }
        //     ind.flush().unwrap();

        //     let start_and_end_file = File::open(&indirect_path).unwrap();
        //     let data_file = File::open(&data_path).unwrap();
        //     let indirect_metadata = fs::metadata(&indirect_path).unwrap();
        //     let data_metadata = fs::metadata(&data_path).unwrap();

        //     let store = PointingMMAPFileReader::new(
        //         &start_and_end_file,
        //         &data_file,
        //         indirect_metadata,
        //         &data_metadata,
        //         ind.max_value_id,
        //         calc_avg_join_size(ind.num_values, ind.num_ids),
        //     );
        //     check_test_data_1_to_n(&store);
        // }

        #[test]
        fn test_pointing_array_index_id_to_multiple_parent_indirect() {
            let store = get_test_data_1_to_n();
            let store = IndexIdToMultipleParentIndirect::new(&store);
            check_test_data_1_to_n(&store);
        }

        // #[test]
        // fn test_mayda_compressed_one() {
        //     let store = get_test_data_1_to_n();
        //     let mayda = IndexIdToMultipleParentCompressedMaydaINDIRECTOne::<u32>::new(&store);
        //     // let yep = to_uniform(&values);
        //     // assert_eq!(yep.access(0..=1), vec![5, 6]);
        //     check_test_data_1_to_n(&mayda);
        // }

        // fn get_test_data_large(num_ids: usize, max_num_values_per_id: usize) -> ParallelArrays<u32> {
        //     let mut rng = rand::thread_rng();
        //     let between = Range::new(0, max_num_values_per_id);

        //     let mut keys = vec![];
        //     let mut values = vec![];

        //     for x in 0..num_ids {
        //         let num_values = between.ind_sample(&mut rng) as u64;

        //         for _i in 0..num_values {
        //             keys.push(x as u32);
        //             values.push(between.ind_sample(&mut rng) as u32);
        //         }
        //     }
        //     ParallelArrays {
        //         values1: keys,
        //         values2: values,
        //     }
        // }

        // #[bench]
        // fn indirect_pointing_mayda(b: &mut test::Bencher) {
        //     let mut rng = rand::thread_rng();
        //     let between = Range::new(0, 40_000);
        //     let store = get_test_data_large(40_000, 15);
        //     let mayda = IndexIdToMultipleParentCompressedMaydaINDIRECTOne::<u32>::new(&store);

        //     b.iter(|| mayda.get_values(between.ind_sample(&mut rng)))
        // }

        // #[bench]
        // fn indirect_pointing_uncompressed_im(b: &mut test::Bencher) {
        //     let mut rng = rand::thread_rng();
        //     let between = Range::new(0, 40_000);
        //     let store = get_test_data_large(40_000, 15);
        //     let mayda = IndexIdToMultipleParent::<u32>::new(&store);

        //     b.iter(|| mayda.get_values(between.ind_sample(&mut rng)))
        // }

    }

}
