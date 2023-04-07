use super::calc_avg_join_size;
use crate::{error::VelociError, indices::*, persistence::*, type_info::TypeInfo, util::*};
use directory::Directory;
use ownedbytes::OwnedBytes;
use std::{self, cmp::Ordering::Greater, io, marker::PhantomData, path::PathBuf, u32};
use vint32::{iterator::VintArrayIterator, vint_array::VIntArray};

impl_type_info_single_templ!(IndirectIMFlushingInOrderVintNoDirectEncode);
impl_type_info_single_templ!(IndirectIMBinarySearch);

/// This data structure assumes that a set is only called once for a id, and ids are set in order.
#[derive(Debug, Clone)]
pub(crate) struct IndirectIMFlushingInOrderVintNoDirectEncode<T> {
    pub(crate) ids_cache: Vec<(T, u32)>,
    pub(crate) data_cache: Vec<u8>,
    pub(crate) current_data_offset: u32,
    /// Already written ids_cache
    pub(crate) current_id_offset: u32,
    pub(crate) path: PathBuf,
    #[allow(dead_code)]
    pub(crate) metadata: IndexValuesMetadata,
    directory: Box<dyn Directory>,
}

impl<T: Default + std::fmt::Debug> IndirectIMFlushingInOrderVintNoDirectEncode<T> {
    pub(crate) fn new(directory: Box<dyn Directory>, path: PathBuf, max_value_id: u32) -> Self {
        let mut data_cache = vec![];
        data_cache.resize(1, 0); // resize data by one, because 0 is reserved for the empty buckets
        IndirectIMFlushingInOrderVintNoDirectEncode {
            ids_cache: vec![],
            data_cache,
            current_data_offset: 0,
            current_id_offset: 0,
            metadata: IndexValuesMetadata::new(max_value_id),
            directory,
            path,
        }
    }

    pub(crate) fn into_im_store(mut self) -> IndirectIMBinarySearchIM<T> {
        let mut store = IndirectIMBinarySearchIM::default();
        store.start_pos = self.ids_cache;

        store.data = self.data_cache;
        self.metadata.avg_join_size = calc_avg_join_size(self.metadata.num_values, self.metadata.num_ids);
        store.metadata = self.metadata;
        store
    }

    #[inline]
    pub(crate) fn add(&mut self, id: T, add_data: &[u32]) -> Result<(), io::Error> {
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
    pub(crate) fn is_in_memory(&self) -> bool {
        self.current_id_offset == 0
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.ids_cache.is_empty() && self.current_id_offset == 0
    }

    pub(crate) fn flush(&mut self) -> Result<(), io::Error> {
        if self.ids_cache.is_empty() {
            return Ok(());
        }

        self.current_id_offset += self.ids_cache.len() as u32;
        self.current_data_offset += self.data_cache.len() as u32;

        self.directory.append(&self.path.set_ext(Ext::Indirect), &vec_to_bytes(&self.ids_cache))?;
        self.directory.append(&self.path.set_ext(Ext::Data), &self.data_cache)?;

        self.data_cache.clear();
        self.ids_cache.clear();

        self.metadata.avg_join_size = calc_avg_join_size(self.metadata.num_values, self.metadata.num_ids);

        Ok(())
    }
}

fn to_serialized_vint_array(add_data: &[u32]) -> Vec<u8> {
    let vint = VIntArray::from_vals(add_data);
    vint.serialize()
}

#[derive(Debug, Clone, Default)]
pub(crate) struct IndirectIMBinarySearchIM<T> {
    pub(crate) start_pos: Vec<(T, u32)>,
    pub(crate) data: Vec<u8>,
    pub(crate) metadata: IndexValuesMetadata,
}

impl<T: 'static + Ord + Copy + Default + std::fmt::Debug + Sync + Send> PhrasePairToAnchor for IndirectIMBinarySearchIM<T> {
    type Input = T;

    #[inline]
    fn get_values(&self, id: Self::Input) -> Option<Vec<u32>> {
        let hit = self.start_pos.binary_search_by_key(&id, |el| el.0);
        match hit {
            Ok(pos) => {
                let data_pos = self.start_pos[pos].1;
                let iter = VintArrayIterator::from_serialized_vint_array(&self.data[data_pos as usize..]);
                let decoded_data: Vec<u32> = iter.collect();
                Some(decoded_data)
            }
            Err(_) => None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct IndirectIMBinarySearch<T> {
    pub(crate) start_pos: OwnedBytes,
    pub(crate) data: OwnedBytes,
    pub(crate) ok: PhantomData<T>,
    #[allow(dead_code)]
    pub(crate) metadata: IndexValuesMetadata,
    pub(crate) size: usize,
}

impl<T: Ord + Copy + Default + std::fmt::Debug> IndirectIMBinarySearch<T> {
    pub fn from_data(start_pos: OwnedBytes, data: OwnedBytes, metadata: IndexValuesMetadata) -> Result<Self, VelociError> {
        let size = start_pos.len() / std::mem::size_of::<(T, u32)>();
        Ok(IndirectIMBinarySearch {
            start_pos,
            data,
            size,
            ok: PhantomData,
            metadata,
        })
    }

    #[inline]
    fn binary_search(&self, id: T) -> Option<(T, u32)> {
        binary_search_slice(self.size, id, &self.start_pos)
    }
}

#[inline]
#[allow(trivial_casts)]
fn decode_pos<T: Copy + Default, K: Copy + Default>(pos: usize, slice: &[u8]) -> (T, K) {
    let mut out: (T, K) = Default::default();
    let byte_pos = std::mem::size_of::<(T, K)>() * pos;
    unsafe {
        slice[byte_pos..]
            .as_ptr()
            .copy_to_nonoverlapping(&mut out as *mut (T, K) as *mut u8, std::mem::size_of::<(T, K)>());
    }
    out
}

#[inline]
pub(crate) fn binary_search_slice<T: Ord + Copy + Default + std::fmt::Debug, K: Copy + Default>(mut size: usize, id: T, slice: &[u8]) -> Option<(T, K)> {
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
        let cmp = decode_pos::<T, K>(mid, slice).0.cmp(&id); //(unsafe { s.decode_pos(mid) });
        base = if cmp == Greater { base } else { mid };
        size -= half;
    }
    // base is always in [0, size) because base <= mid.
    // let cmp = f(unsafe { s.decode_pos(base) });
    let hit = decode_pos(base, slice);
    if id == hit.0 {
        Some(hit)
    } else {
        None
    }
}

impl<T: 'static + Ord + Copy + Default + std::fmt::Debug + Sync + Send> PhrasePairToAnchor for IndirectIMBinarySearch<T> {
    type Input = T;

    #[inline]
    fn get_values(&self, id: Self::Input) -> Option<Vec<u32>> {
        let hit = self.binary_search(id);
        hit.map(|el| {
            let data_pos = el.1;
            VintArrayIterator::from_serialized_vint_array(&self.data[data_pos as usize..]).collect()
        })
    }
}

#[cfg(test)]
mod tests {

    use crate::directory::load_data_pair;

    use super::*;
    use directory::RamDirectory;

    fn get_test_data_1_to_n_ind(directory: Box<dyn Directory>, path: PathBuf) -> IndirectIMFlushingInOrderVintNoDirectEncode<(u32, u32)> {
        let mut store = IndirectIMFlushingInOrderVintNoDirectEncode::new(directory, path, u32::MAX);
        store.add((0, 0), &[5, 6]).unwrap();
        store.add((0, 1), &[9]).unwrap();
        store.add((2, 0), &[9]).unwrap();
        store.add((2, 3), &[9, 50000]).unwrap();
        store.add((5, 0), &[80]).unwrap();
        store.add((5, 9), &[0]).unwrap();
        store.add((5, 10), &[0]).unwrap();
        store
    }

    #[test]
    fn test_in_memory() {
        let directory: Box<dyn Directory> = Box::new(RamDirectory::create());
        let path = Path::new("yop").to_owned();
        let store = get_test_data_1_to_n_ind(directory.box_clone(), path);

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
        let directory: Box<dyn Directory> = Box::new(RamDirectory::create());
        let path = Path::new("yop").to_owned();
        let mut store = get_test_data_1_to_n_ind(directory.box_clone(), path.clone());
        store.flush().unwrap();
        let (ind, data) = load_data_pair(&directory, Path::new(&path)).unwrap();
        let store = IndirectIMBinarySearch::<(u32, u32)>::from_data(ind, data, store.metadata).unwrap();
        assert_eq!(store.size, 7);
        assert_eq!(decode_pos(0, &store.start_pos), ((0, 0), 1));
        assert_eq!(decode_pos(1, &store.start_pos), ((0, 1), 4));

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
