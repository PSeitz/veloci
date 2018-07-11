use heapsize::HeapSizeOf;
use std::cmp::Ordering::Greater;

use persistence::*;
use persistence_data_indirect::calc_avg_join_size;
use persistence_data_indirect::flush_to_file_indirect;
use type_info::TypeInfo;

use std;
use std::fs::File;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::u32;

use memmap::Mmap;
use memmap::MmapOptions;

impl_type_info_single_templ!(IndexIdToMultipleParentIndirectFlushingInOrderVintNoDirectEncode);
impl_type_info_single_templ!(IndexIdToMultipleParentIndirectBinarySearchMMAP);

use vint::vint::*;

/// This data structure assumes that a set is only called once for a id, and ids are set in order.
#[derive(Debug, Clone, HeapSizeOf)]
pub struct IndexIdToMultipleParentIndirectFlushingInOrderVintNoDirectEncode<T> {
    pub ids_cache: Vec<(T, u32)>,
    pub data_cache: Vec<u8>,
    pub current_data_offset: u32,
    /// Already written ids_cache
    pub current_id_offset: u32,
    pub indirect_path: String,
    pub data_path: String,
    pub metadata: IndexMetaData,
}

impl<T: Default + std::fmt::Debug> IndexIdToMultipleParentIndirectFlushingInOrderVintNoDirectEncode<T> {
    pub fn new(indirect_path: String, data_path: String, max_value_id: u32) -> Self {
        let mut data_cache = vec![];
        data_cache.resize(1, 0); // resize data by one, because 0 is reserved for the empty buckets
        IndexIdToMultipleParentIndirectFlushingInOrderVintNoDirectEncode {
            ids_cache: vec![],
            data_cache,
            current_data_offset: 0,
            current_id_offset: 0,
            indirect_path,
            data_path,
            metadata: IndexMetaData::new(max_value_id),
        }
    }

    pub fn into_im_store(mut self) -> IndexIdToMultipleParentIndirectBinarySearch<T> {
        let mut store = IndexIdToMultipleParentIndirectBinarySearch::default();
        store.start_pos = self.ids_cache;

        store.data = self.data_cache;
        self.metadata.avg_join_size = calc_avg_join_size(self.metadata.num_values, self.metadata.num_ids);
        store.metadata = self.metadata;
        store
    }

    #[inline]
    pub fn add(&mut self, id: T, add_data: Vec<u32>) -> Result<(), io::Error> {
        self.metadata.num_values += 1;
        self.metadata.num_ids += add_data.len() as u32;

        let data_pos = self.current_data_offset + self.data_cache.len() as u32;

        self.ids_cache.push((id, data_pos));
        self.data_cache.extend(to_serialized_vint_array(add_data));
        if self.ids_cache.len() * std::mem::size_of::<T>() + self.data_cache.len() >= 4_000_000 {
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

        flush_to_file_indirect(&self.indirect_path, &self.data_path, &vec_to_bytes(&self.ids_cache), &self.data_cache)?;

        self.data_cache.clear();
        self.ids_cache.clear();

        self.metadata.avg_join_size = calc_avg_join_size(self.metadata.num_values, self.metadata.num_ids);

        Ok(())
    }
}

fn to_serialized_vint_array(add_data: Vec<u32>) -> Vec<u8> {
    let mut vint = VIntArray::default();
    for el in add_data {
        vint.encode(el);
    }
    vint.serialize()
}

#[derive(Debug, Clone, Default, HeapSizeOf)]
pub struct IndexIdToMultipleParentIndirectBinarySearch<T> {
    pub start_pos: Vec<(T, u32)>,
    pub data: Vec<u8>,
    pub metadata: IndexMetaData,
}
// impl<T: Ord + Copy> IndexIdToMultipleParentIndirectBinarySearch<T> {

//     #[inline]
//     pub fn get_values(&self, id: T) -> Option<Vec<u32>> {
//         let hit = self.start_pos.binary_search_by_key(&id, |ref el| el.0);
//         match hit {
//             Ok(pos) => {
//                 let data_pos = self.start_pos[pos].1;
//                 let iter = VintArrayIterator::from_slice(&self.data[data_pos as usize..]);
//                 let decoded_data: Vec<u32> = iter.collect();
//                 Some(decoded_data)
//             },
//             Err(_) => None,
//         }
//     }
// }

impl<T: 'static + Ord + Copy + Default + std::fmt::Debug + Sync + Send> PhrasePairToAnchor for IndexIdToMultipleParentIndirectBinarySearch<T> {
    type Input = T;

    #[inline]
    fn get_values(&self, id: Self::Input) -> Option<Vec<u32>> {
        let hit = self.start_pos.binary_search_by_key(&id, |ref el| el.0);
        match hit {
            Ok(pos) => {
                let data_pos = self.start_pos[pos].1;
                let iter = VintArrayIterator::from_slice(&self.data[data_pos as usize..]);
                let decoded_data: Vec<u32> = iter.collect();
                Some(decoded_data)
            }
            Err(_) => None,
        }
    }
}
use search;
use util::open_file;

#[derive(Debug)]
pub struct IndexIdToMultipleParentIndirectBinarySearchMMAP<T> {
    pub start_pos: Mmap,
    pub data: Mmap,
    pub ok: PhantomData<T>,
    pub metadata: IndexMetaData,
    pub size: usize,
}
impl<T: Ord + Copy + Default + std::fmt::Debug> HeapSizeOf for IndexIdToMultipleParentIndirectBinarySearchMMAP<T> {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}
impl<T: Ord + Copy + Default + std::fmt::Debug> IndexIdToMultipleParentIndirectBinarySearchMMAP<T> {
    pub fn from_path<P: AsRef<Path>>(path: P, metadata: IndexMetaData) -> Result<Self, search::SearchError> {
        let ind_file = File::open(path.as_ref().with_extension("indirect"))?;
        let start_pos = unsafe { MmapOptions::new().map(&open_file((path.as_ref()).with_extension("indirect"))?).unwrap() };
        let data = unsafe { MmapOptions::new().map(&open_file((path.as_ref()).with_extension("data"))?).unwrap() };
        Ok(IndexIdToMultipleParentIndirectBinarySearchMMAP {
            start_pos,
            data,
            size: ind_file.metadata()?.len() as usize / std::mem::size_of::<(T, u32)>(),
            ok: PhantomData,
            metadata,
        })
    }

    fn get(&self, pos: usize) -> (T, u32) {
        let mut out: (T, u32) = Default::default();
        let byte_pos = std::mem::size_of::<(T, u32)>() * pos;
        unsafe {
            self.start_pos[byte_pos as usize..]
                .as_ptr()
                .copy_to_nonoverlapping(&mut out as *mut (T, u32) as *mut u8, std::mem::size_of::<(T, u32)>());
        }
        out
    }

    #[inline]
    fn binary_search(&self, id: T) -> Option<(T, u32)> {
        let s = self;
        let mut size = s.size;
        if size == 0 {
            return None;
        }
        let mut base = 0usize;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            // mid is always in [0, size), that means mid is >= 0 and < size.
            // mid >= 0: by definition
            // mid < size: mid = size / 2 + size / 4 + size / 8 ...
            let cmp = s.get(mid).0.cmp(&id); //(unsafe { s.get(mid) });
            base = if cmp == Greater { base } else { mid };
            size -= half;
        }
        // base is always in [0, size) because base <= mid.
        // let cmp = f(unsafe { s.get(base) });
        let hit = s.get(base);
        if id == hit.0 {
            Some(hit)
        } else {
            None
        }
    }
}

impl<T: 'static + Ord + Copy + Default + std::fmt::Debug + Sync + Send> PhrasePairToAnchor for IndexIdToMultipleParentIndirectBinarySearchMMAP<T> {
    type Input = T;

    #[inline]
    fn get_values(&self, id: Self::Input) -> Option<Vec<u32>> {
        let hit = self.binary_search(id);
        hit.map(|el| {
            let data_pos = el.1;
            VintArrayIterator::from_slice(&self.data[data_pos as usize..]).collect()
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile::tempdir;

    fn get_test_data_1_to_n_ind(ind_path: String, data_path: String) -> IndexIdToMultipleParentIndirectFlushingInOrderVintNoDirectEncode<(u32, u32)> {
        let mut store = IndexIdToMultipleParentIndirectFlushingInOrderVintNoDirectEncode::new(ind_path, data_path, u32::MAX);
        store.add((0, 0), vec![5, 6]).unwrap();
        store.add((0, 1), vec![9]).unwrap();
        store.add((2, 0), vec![9]).unwrap();
        store.add((2, 3), vec![9, 50000]).unwrap();
        store.add((5, 0), vec![80]).unwrap();
        store.add((5, 9), vec![0]).unwrap();
        store.add((5, 10), vec![0]).unwrap();
        store
    }

    #[test]
    fn test_in_memory() {
        let dir = tempdir().unwrap();
        let indirect_path = dir.path().join("indirect").to_str().unwrap().to_string();
        let data_path = dir.path().join("data").to_str().unwrap().to_string();
        let store = get_test_data_1_to_n_ind(indirect_path.to_string(), data_path.to_string());

        let yop = store.into_im_store();

        assert_eq!(yop.get_values((0, 0)), Some(vec![5, 6]));
        assert_eq!(yop.get_values((0, 1)), Some(vec![9]));
        assert_eq!(yop.get_values((0, 2)), None);
        assert_eq!(yop.get_values((2, 0)), Some(vec![9]));
        assert_eq!(yop.get_values((2, 3)), Some(vec![9, 50000]));
        assert_eq!(yop.get_values((5, 0)), Some(vec![80]));
        assert_eq!(yop.get_values((5, 9)), Some(vec![0]));
        assert_eq!(yop.get_values((5, 10)), Some(vec![0]));
    }

    #[test]
    fn test_mmap() {
        let dir = tempdir().unwrap();
        let indirect_path = dir.path().join("yop.indirect").to_str().unwrap().to_string();
        let data_path = dir.path().join("yop.data").to_str().unwrap().to_string();
        let mut store = get_test_data_1_to_n_ind(indirect_path.to_string(), data_path.to_string());
        store.flush().unwrap();
        // let yop = store.into_im_store();
        let store = IndexIdToMultipleParentIndirectBinarySearchMMAP::<(u32, u32)>::from_path(dir.path().join("yop"), store.metadata).unwrap();
        assert_eq!(store.size, 7);
        assert_eq!(store.get(0), ((0, 0), 1));
        assert_eq!(store.get(1), ((0, 1), 4));

        assert_eq!(store.get_values((0, 0)), Some(vec![5, 6]));
        assert_eq!(store.get_values((0, 1)), Some(vec![9]));
        assert_eq!(store.get_values((0, 2)), None);
        assert_eq!(store.get_values((2, 0)), Some(vec![9]));
        assert_eq!(store.get_values((2, 3)), Some(vec![9, 50000]));
        assert_eq!(store.get_values((5, 0)), Some(vec![80]));
        assert_eq!(store.get_values((5, 9)), Some(vec![0]));
        assert_eq!(store.get_values((5, 10)), Some(vec![0]));
    }

}
