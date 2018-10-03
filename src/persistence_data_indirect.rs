use heapsize::HeapSizeOf;
use lru_time_cache::LruCache;
use util::*;

use byteorder::{LittleEndian, ReadBytesExt};

use persistence::*;
use type_info::TypeInfo;

use facet::*;
use num;
use num::cast::ToPrimitive;
use std;
use std::fmt;
use std::fs::File;
use std::io;
use std::io::Write;
use std::marker::PhantomData;
use std::u32;

use fnv::FnvHashMap;
use itertools::Itertools;

use memmap::Mmap;
use memmap::MmapOptions;

impl_type_info_single_templ!(IndexIdToMultipleParentIndirect);
impl_type_info_single_templ!(PointingMMAPFileReader);

const EMPTY_BUCKET: u32 = 0;

pub(crate) fn calc_avg_join_size(num_values: u64, num_ids: u32) -> f32 {
    num_values as f32 / std::cmp::max(1, num_ids).to_f32().unwrap()
}

pub(crate) fn flush_to_file_indirect(indirect_path: &str, data_path: &str, indirect_data: &[u8], data: &[u8]) -> Result<(), io::Error> {
    let mut indirect = std::fs::OpenOptions::new().read(true).write(true).append(true).create(true).open(&indirect_path).unwrap();
    let mut data_cache = std::fs::OpenOptions::new().read(true).write(true).append(true).create(true).open(&data_path).unwrap();

    indirect.write_all(indirect_data)?;
    data_cache.write_all(data)?;

    Ok(())
}

use vint::vint::*;

fn to_serialized_vint_array(add_data: Vec<u32>) -> Vec<u8> {
    let mut vint = VIntArray::default();
    for el in add_data {
        vint.encode(el);
    }
    vint.serialize()
}

/// This data structure assumes that a set is only called once for a id, and ids are set in order.
#[derive(Debug, Clone, HeapSizeOf)]
pub struct IndexIdToMultipleParentIndirectFlushingInOrderVint {
    pub ids_cache: Vec<u32>,
    pub data_cache: Vec<u8>,
    pub current_data_offset: u32,
    /// Already written ids_cache
    pub current_id_offset: u32,
    pub path: String,
    pub metadata: IndexMetaData,
}

// TODO: Indirect Stuff @Performance @Memory
// use vint for indirect, use not highest bit in indirect, but the highest unused bit. Max(value_id, single data_id, which would be encoded in the valueid index)
//
impl IndexIdToMultipleParentIndirectFlushingInOrderVint {
    pub fn new(path: String, max_value_id: u32) -> Self {
        let mut data_cache = vec![];
        data_cache.resize(1, 0); // resize data by one, because 0 is reserved for the empty buckets
        IndexIdToMultipleParentIndirectFlushingInOrderVint {
            ids_cache: vec![],
            data_cache,
            current_data_offset: 0,
            current_id_offset: 0,
            path,
            // indirect_path,
            // data_path,
            metadata: IndexMetaData::new(max_value_id),
        }
    }

    pub fn into_im_store(mut self) -> IndexIdToMultipleParentIndirect<u32> {
        let mut store = IndexIdToMultipleParentIndirect::default();
        self.metadata.avg_join_size = calc_avg_join_size(self.metadata.num_values, self.metadata.num_ids);
        store.start_pos = self.ids_cache;
        store.metadata = self.metadata;
        store.data = self.data_cache;
        store
    }

    pub fn into_store(mut self) -> Result<Box<IndexIdToParent<Output = u32>>, search::SearchError> {
        if self.is_in_memory() {
            Ok(Box::new(self.into_im_store()))
        } else {
            self.flush()?;
            let store = PointingMMAPFileReader::from_path(&self.path, self.metadata)?;
            Ok(Box::new(store))
        }
    }

    #[inline]
    pub fn add(&mut self, id: u32, add_data: Vec<u32>) -> Result<(), io::Error> {
        self.metadata.num_values += 1;
        self.metadata.num_ids += add_data.len() as u32;

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
            if let Some(pos_in_data) = (self.current_data_offset as usize + self.data_cache.len()).to_u32() {
                self.ids_cache[id_pos] = pos_in_data;
                self.data_cache.extend(to_serialized_vint_array(add_data));
            } else {
                //Handle Overflow
                panic!("Too much data, can't adress with u32");
            }
        }

        if self.ids_cache.len() * std::mem::size_of::<u32>() + self.data_cache.len() >= 4_000_000 {
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

        self.current_id_offset += self.ids_cache.len() as u32;
        self.current_data_offset += self.data_cache.len() as u32;

        flush_to_file_indirect(
            &(self.path.to_string() + ".indirect"),
            &(self.path.to_string() + ".data"),
            &vec_to_bytes(&self.ids_cache),
            &self.data_cache,
        )?;

        self.data_cache.clear();
        self.ids_cache.clear();

        self.metadata.avg_join_size = calc_avg_join_size(self.metadata.num_values, self.metadata.num_ids);

        Ok(())
    }
}

#[derive(Clone)]
pub struct IndexIdToMultipleParentIndirect<T: IndexIdToParentData> {
    pub start_pos: Vec<T>,
    pub cache: LruCache<Vec<T>, u32>,
    pub data: Vec<u8>,
    pub metadata: IndexMetaData,
}
impl<T: IndexIdToParentData> HeapSizeOf for IndexIdToMultipleParentIndirect<T> {
    fn heap_size_of_children(&self) -> usize {
        self.start_pos.heap_size_of_children() + self.data.heap_size_of_children() + self.metadata.heap_size_of_children()
    }
}

impl<T: IndexIdToParentData> fmt::Debug for IndexIdToMultipleParentIndirect<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IndexIdToMultipleParentIndirect {{ start_pos: {:?}, data: {:?} }}", self.start_pos, self.data)
    }
}

impl<T: IndexIdToParentData> Default for IndexIdToMultipleParentIndirect<T> {
    fn default() -> IndexIdToMultipleParentIndirect<T> {
        let mut data = vec![];
        data.resize(1, 1); // resize data by one, because 0 is reserved for the empty buckets
        IndexIdToMultipleParentIndirect {
            start_pos: vec![],
            cache: LruCache::with_capacity(250),
            data,
            metadata: IndexMetaData::new(0),
        }
    }
}

impl<T: IndexIdToParentData> IndexIdToMultipleParentIndirect<T> {
    #[inline]
    fn get_size(&self) -> usize {
        self.start_pos.len()
    }

    #[inline]
    fn count_values_for_ids_for_agg<C: AggregationCollector<T>>(&self, ids: &[u32], top: Option<u32>, mut coll: C) -> FnvHashMap<T, usize> {
        let size = self.get_size();

        let mut positions_vec = Vec::with_capacity(8);
        for id_chunk in &ids.into_iter().chunks(8) {
            for id in id_chunk {
                if *id >= size as u32 {
                    continue;
                }
                let pos = *id as usize;
                let data_start_pos = self.start_pos[pos];
                let data_start_pos_or_data = data_start_pos.to_u32().unwrap();
                if let Some(val) = get_encoded(data_start_pos_or_data) {
                    coll.add(num::cast(val).unwrap());
                    continue;
                }
                if data_start_pos_or_data != EMPTY_BUCKET {
                    positions_vec.push(data_start_pos_or_data);
                }
            }

            for position in &positions_vec {
                let iter = VintArrayIterator::from_serialized_vint_array(&self.data[*position as usize..]);
                for el in iter {
                    coll.add(num::cast(el).unwrap());
                }
            }
            positions_vec.clear();
        }
        Box::new(coll).to_map(top)
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for IndexIdToMultipleParentIndirect<T> {
    type Output = T;

    #[inline]
    fn count_values_for_ids(&self, ids: &[u32], top: Option<u32>) -> FnvHashMap<T, usize> {
        if should_prefer_vec(ids.len() as u32, self.metadata.avg_join_size, self.metadata.max_value_id) {
            let mut dat = vec![];
            dat.resize(self.metadata.max_value_id as usize + 1, T::zero());
            self.count_values_for_ids_for_agg(ids, top, dat)
        } else {
            let map = FnvHashMap::default();
            // map.reserve((ids.len() as f32 * self.metadata.avg_join_size) as usize);  TODO TO PROPERLY RESERVE HERE, NUMBER OF DISTINCT VALUES IS NEEDED IN THE INDEX
            self.count_values_for_ids_for_agg(ids, top, map)
        }
    }

    fn get_keys(&self) -> Vec<T> {
        (num::cast(0).unwrap()..num::cast(self.get_size()).unwrap()).collect()
    }

    fn get_values_iter(&self, id: u64) -> VintArrayIteratorOpt {
        if id >= self.get_size() as u64 {
            VintArrayIteratorOpt {
                single_value: -2,
                iter: Box::new(VintArrayIterator::from_serialized_vint_array(&[])),
            }
        } else {
            let data_start_pos = self.start_pos[id as usize];
            let data_start_pos_or_data = data_start_pos.to_u32().unwrap();
            if let Some(val) = get_encoded(data_start_pos_or_data) {
                return VintArrayIteratorOpt {
                    single_value: val as i64,
                    iter: Box::new(VintArrayIterator::from_serialized_vint_array(&[])),
                };
            }
            if data_start_pos_or_data == EMPTY_BUCKET {
                return VintArrayIteratorOpt {
                    single_value: -2,
                    iter: Box::new(VintArrayIterator::from_serialized_vint_array(&[])),
                };
            }
            VintArrayIteratorOpt {
                single_value: -1,
                iter: Box::new(VintArrayIterator::from_serialized_vint_array(&self.data[data_start_pos.to_usize().unwrap()..])),
            }
        }
    }

    #[inline]
    default fn get_values(&self, id: u64) -> Option<Vec<T>> {
        if id >= self.get_size() as u64 {
            None
        } else {
            let data_start_pos = self.start_pos[id as usize];
            let data_start_pos_or_data = data_start_pos.to_u32().unwrap();
            if let Some(val) = get_encoded(data_start_pos_or_data) {
                return Some(vec![num::cast(val).unwrap()]);
            }
            if data_start_pos_or_data == EMPTY_BUCKET {
                return None;
            }

            let iter = VintArrayIterator::from_serialized_vint_array(&self.data[data_start_pos.to_usize().unwrap()..]);
            let decoded_data: Vec<u32> = iter.collect();
            Some(decoded_data.iter().map(|el| num::cast(*el).unwrap()).collect())
        }
    }
}

use search;
use util::open_file;

#[derive(Debug)]
pub struct PointingMMAPFileReader<T: IndexIdToParentData> {
    pub start_pos: Mmap,
    pub data: Mmap,
    pub size: usize,
    pub ok: PhantomData<T>,
    pub metadata: IndexMetaData,
}

impl<T: IndexIdToParentData> PointingMMAPFileReader<T> {
    #[inline]
    fn get_size(&self) -> usize {
        self.size
    }

    pub fn from_path(path: &str, metadata: IndexMetaData) -> Result<Self, search::SearchError> {
        let start_pos = unsafe { MmapOptions::new().map(&open_file(path.to_string() + ".indirect")?).unwrap() };
        let data = unsafe { MmapOptions::new().map(&open_file(path.to_string() + ".data")?).unwrap() };
        Ok(PointingMMAPFileReader {
            start_pos,
            data,
            size: File::open(path.to_string() + ".indirect")?.metadata()?.len() as usize / std::mem::size_of::<T>(),
            ok: PhantomData,
            metadata,
        })
    }
}

impl<T: IndexIdToParentData> HeapSizeOf for PointingMMAPFileReader<T> {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for PointingMMAPFileReader<T> {
    type Output = T;

    fn get_keys(&self) -> Vec<T> {
        (num::cast(0).unwrap()..num::cast(self.get_size()).unwrap()).collect()
    }

    fn get_values_iter(&self, id: u64) -> VintArrayIteratorOpt {
        if id >= self.get_size() as u64 {
            VintArrayIteratorOpt {
                single_value: -2,
                iter: Box::new(VintArrayIterator::from_serialized_vint_array(&[])),
            }
        } else {
            let start_index = id as usize * std::mem::size_of::<T>();
            let data_start_pos = (&self.start_pos[start_index as usize..start_index + 4]).read_u32::<LittleEndian>().unwrap();
            let data_start_pos_or_data = data_start_pos.to_u32().unwrap();
            if let Some(val) = get_encoded(data_start_pos_or_data) {
                return VintArrayIteratorOpt {
                    single_value: val as i64,
                    iter: Box::new(VintArrayIterator::from_serialized_vint_array(&[])),
                };
            }
            if data_start_pos_or_data == EMPTY_BUCKET {
                return VintArrayIteratorOpt {
                    single_value: -2,
                    iter: Box::new(VintArrayIterator::from_serialized_vint_array(&[])),
                };
            }
            VintArrayIteratorOpt {
                single_value: -1,
                iter: Box::new(VintArrayIterator::from_serialized_vint_array(&self.data[data_start_pos.to_usize().unwrap()..])),
            }
        }
    }

    default fn get_values(&self, id: u64) -> Option<Vec<T>> {
        get_u32_values_from_pointing_mmap_file_vint(
            //FIXME BUG BUG if file is not u32
            id,
            self.get_size(),
            &self.start_pos,
            &self.data,
        ).map(|el| el.iter().map(|el| num::cast(*el).unwrap()).collect())
    }
}

#[inline(always)]
fn get_u32_values_from_pointing_mmap_file_vint(id: u64, size: usize, start_pos: &Mmap, data: &Mmap) -> Option<Vec<u32>> {
    if id >= size as u64 {
        None
    } else {
        let start_index = id as usize * std::mem::size_of::<u32>();
        let data_start_pos = (&start_pos[start_index as usize..start_index + std::mem::size_of::<u32>()])
            .read_u32::<LittleEndian>()
            .unwrap();

        let data_start_pos_or_data = data_start_pos.to_u32().unwrap();
        if let Some(val) = get_encoded(data_start_pos_or_data) {
            return Some(vec![num::cast(val).unwrap()]);
        }
        if data_start_pos_or_data == EMPTY_BUCKET {
            return None;
        }

        let iter = VintArrayIterator::from_serialized_vint_array(&data[data_start_pos as usize..]);
        let decoded_data: Vec<u32> = iter.collect();
        Some(decoded_data)
    }
}

fn get_encoded(mut val: u32) -> Option<u32> {
    if is_hight_bit_set(val) {
        //data encoded in indirect array
        unset_high_bit(&mut val);
        Some(val)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use persistence_data::*;

    fn get_test_data_1_to_n_ind(path: &str) -> IndexIdToMultipleParentIndirectFlushingInOrderVint {
        let mut store = IndexIdToMultipleParentIndirectFlushingInOrderVint::new(path.to_string(), u32::MAX);
        store.add(0, vec![5, 6]).unwrap();
        store.add(1, vec![9]).unwrap();
        store.add(2, vec![9]).unwrap();
        store.add(3, vec![9, 50000]).unwrap();
        store.add(5, vec![80]).unwrap();
        store.add(9, vec![0]).unwrap();
        store.add(10, vec![0]).unwrap();
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
    fn check_test_data_1_to_n_iter(store: &IndexIdToParent<Output = u32>) {
        let empty_vec: Vec<u32> = vec![];
        assert_eq!(store.get_keys(), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(store.get_values_iter(0).collect::<Vec<u32>>(), vec![5, 6]);
        assert_eq!(store.get_values_iter(1).collect::<Vec<u32>>(), vec![9]);
        assert_eq!(store.get_values_iter(2).collect::<Vec<u32>>(), vec![9]);
        assert_eq!(store.get_values_iter(3).collect::<Vec<u32>>(), vec![9, 50000]);
        assert_eq!(store.get_values_iter(4).collect::<Vec<u32>>(), empty_vec);
        assert_eq!(store.get_values_iter(5).collect::<Vec<u32>>(), vec![80]);
        assert_eq!(store.get_values_iter(6).collect::<Vec<u32>>(), empty_vec);
        assert_eq!(store.get_values_iter(9).collect::<Vec<u32>>(), vec![0]);
        assert_eq!(store.get_values_iter(10).collect::<Vec<u32>>(), vec![0]);
        assert_eq!(store.get_values_iter(11).collect::<Vec<u32>>(), empty_vec);

        let map = store.count_values_for_ids(&[0, 1, 2, 3, 4, 5], None);
        assert_eq!(map.get(&5).unwrap(), &1);
        assert_eq!(map.get(&9).unwrap(), &3);
    }

    mod test_indirect {
        use super::*;
        use tempfile::tempdir;
        #[test]
        fn test_pointing_file_andmmap_array() {
            let dir = tempdir().unwrap();
            let path = dir.path().join("testfile").to_str().unwrap().to_string();
            let mut store = get_test_data_1_to_n_ind(&path);
            store.flush().unwrap();

            let store = PointingMMAPFileReader::from_path(&path, store.metadata).unwrap();
            check_test_data_1_to_n(&store);
            check_test_data_1_to_n_iter(&store);
        }

        #[test]
        fn test_flushing_in_order_indirect() {
            let dir = tempdir().unwrap();
            let path = dir.path().join("testfile").to_str().unwrap().to_string();
            let store = get_test_data_1_to_n_ind(&path).into_im_store();

            let mut ind = IndexIdToMultipleParentIndirectFlushingInOrderVint::new(path.to_string(), u32::MAX);

            for key in store.get_keys() {
                if let Some(vals) = store.get_values(key.into()) {
                    ind.add(key, vals).unwrap();
                    ind.flush().unwrap();
                }
            }
            ind.flush().unwrap();

            let store = PointingMMAPFileReader::from_path(&path, store.metadata).unwrap();
            check_test_data_1_to_n(&store);
            check_test_data_1_to_n_iter(&store);
        }

        #[test]
        fn test_pointing_array_index_id_to_multiple_parent_indirect() {
            let store = get_test_data_1_to_n_ind("test_ind");
            let store = store.into_im_store();
            check_test_data_1_to_n(&store);
            check_test_data_1_to_n_iter(&store);
        }

    }

}
