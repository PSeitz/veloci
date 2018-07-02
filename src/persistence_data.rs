use std;
use std::u32;
use std::io;
use std::io::Write;
use std::marker::PhantomData;
use std::fs::{File};

use heapsize::HeapSizeOf;
use byteorder::{LittleEndian, ReadBytesExt};

use persistence::EMPTY_BUCKET;
use persistence::*;
pub(crate) use persistence_data_indirect::*;

use facet::*;
use profiler;
use num;

use type_info::TypeInfo;
use fnv::FnvHashMap;

use memmap::Mmap;
use memmap::MmapOptions;

// impl_type_info_dual_templ!(IndexIdToOneParent);
// impl_type_info_single_templ!(IndexIdToOneParentPacked);
// impl_type_info_single_templ!(ParallelArrays);
impl_type_info_single_templ!(SingleArrayMMAP);
impl_type_info_single_templ!(SingleArrayMMAPPacked);

/// This data structure assumes that a set is only called once for a id, and ids are set in order.
#[derive(Serialize, Debug, Clone, HeapSizeOf, Default)]
pub struct IndexIdToOneParentFlushing {
    pub cache: Vec<u32>,
    pub current_id_offset: u32,
    pub path: String,
    pub max_value_id: u32,
    pub num_values: u32,
    pub avg_join_size: f32,
}

impl IndexIdToOneParentFlushing {
    pub fn new(path: String, max_value_id: u32) -> IndexIdToOneParentFlushing {
        IndexIdToOneParentFlushing { path, max_value_id, ..Default::default() }
    }
    pub fn into_im_store(self) -> IndexIdToOneParent<u32, u32> {
        let mut store = IndexIdToOneParent::default();
        store.avg_join_size = calc_avg_join_size(self.num_values, self.cache.len() as u32);
        store.data = self.cache;
        store.max_value_id = self.max_value_id;
        // store.num_values = self.num_values;
        // store.num_ids = self.num_ids;
        store
    }

    #[inline]
    pub fn add(&mut self, id: u32, val: u32) -> Result<(), io::Error> {
        self.num_values += 1;

        let id_pos = (id - self.current_id_offset) as usize;
        if self.cache.len() <= id_pos {
            //TODO this could become very big, check memory consumption upfront, and flush directly to disk, when a resize would step over a certain threshold @Memory
            self.cache.resize(id_pos + 1, EMPTY_BUCKET);
        }

        self.cache[id_pos] = val + 1; //+1 because EMPTY_BUCKET = 0 is already reserved

        if self.cache.len() * 4 >= 4_000_000 {
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
        self.cache.is_empty() && self.current_id_offset == 0
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        if self.cache.is_empty() {
            return Ok(());
        }

        self.current_id_offset += self.cache.len() as u32;

        let mut data = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(&self.path)
            .unwrap();

        let bytes_required = get_bytes_required(self.max_value_id);

        let mut bytes = vec![];
        encode_vals(&self.cache, bytes_required, &mut bytes).unwrap();
        data.write_all(&bytes)?;

        // data.write_all(&vec_to_bytes_u32(&self.cache))?;

        self.avg_join_size = calc_avg_join_size(self.num_values, self.current_id_offset + self.cache.len() as u32);
        self.cache.clear();

        Ok(())
    }
}


#[derive(Debug, Clone, Copy, HeapSizeOf)]
pub enum BytesRequired {
    One = 1,
    Two,
    Three,
    Four,
}

#[inline]
pub fn get_bytes_required(mut val: u32) -> BytesRequired {
    val = val+1;  //+1 because EMPTY_BUCKET = 0 is already reserved
    if val < 1 << 8 {
        BytesRequired::One
    } else if val < 1 << 16 {
        BytesRequired::Two
    } else if val < 1 << 24 {
        BytesRequired::Three
    } else {
        BytesRequired::Four
    }
}
use std::mem;
#[inline]
pub fn encode_vals<O: std::io::Write>(vals: &[u32], bytes_required:BytesRequired, out: &mut O) -> Result<(), io::Error> {

    //Maximum speed, Maximum unsafe
    use std::slice;
    unsafe {
        let slice =
            slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * mem::size_of::<u32>());
        let mut pos = 0;
        while pos != slice.len(){
            out.write_all(&slice[pos .. pos + bytes_required as usize])?;
            pos+=4;
        }
    }
    Ok(())
}

use std::ptr::copy_nonoverlapping;


// // pub fn decode_bit_packed_val(val: &[u8], num_bits: u8, index: usize) -> u32 {
// #[inline]
// pub fn decode_bit_packed_val<T: IndexIdToParentData>(data: &[u8], bytes_required: BytesRequired, index: usize) -> Option<T> {
//     let bit_pos_start = index * bytes_required as usize;
//     if bit_pos_start >= data.len() {
//         None
//     }else{
//         let mut out = T::zero();
//         unsafe {
//             copy_nonoverlapping(data.as_ptr().add(bit_pos_start), &mut out as *mut T as *mut u8, bytes_required as usize);
//         }
//         if out == T::zero() {
//             return None;
//         }
//         return Some(out - T::one());
//     }
// }

// pub fn decode_bit_packed_val(val: &[u8], num_bits: u8, index: usize) -> u32 {
#[inline]
pub fn decode_bit_packed_val<T: IndexIdToParentData>(data: &[u8], bytes_required: BytesRequired, index: usize) -> Option<T> {
    let bit_pos_start = index * bytes_required as usize;
    if bit_pos_start >= data.len() {
        None
    }else{
        let mut out = T::zero();
        unsafe {
            copy_nonoverlapping(data.as_ptr().add(bit_pos_start), &mut out as *mut T as *mut u8, bytes_required as usize);
        }
        if out == T::zero() {
            return None;
        }
        return Some(out - T::one());
    }
}

// pub fn decode_bit_packed_val(val: &[u8], num_bits: u8, index: usize) -> u32 {
pub fn decode_bit_packed_vals<T: IndexIdToParentData>(data: &[u8], bytes_required: BytesRequired) -> Vec<T> {
    let mut out:Vec<u8> = vec![];
    out.resize(data.len() * std::mem::size_of::<T>() / bytes_required as usize, 0);
    let mut pos = 0;
    let mut out_pos = 0;
    while pos < data.len(){
        out[out_pos .. out_pos + bytes_required as usize].clone_from_slice(&data[pos .. pos + bytes_required as usize]);
        pos+=bytes_required as usize;
        out_pos+=std::mem::size_of::<T>();
    }
    bytes_to_vec(&out)
}

#[test]
fn test_encodsing_and_decoding_bitpacking() {
    let vals: Vec<u32> = vec![123, 33, 545, 99];

    let bytes_required = get_bytes_required(*vals.iter().max().unwrap() as u32);

    let mut bytes = vec![];

    encode_vals(&vals, bytes_required, &mut bytes).unwrap();

    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 0), Some(122));
    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 1), Some(32));
    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 2), Some(544));
    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 3), Some(98));
    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 4), None);
    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 5), None);

    let vals: Vec<u32> = vec![50001, 33];
    let bytes_required = get_bytes_required(*vals.iter().max().unwrap() as u32);
    let mut bytes = vec![];

    encode_vals(&vals, bytes_required, &mut bytes).unwrap();

    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 0), Some(50_000));
    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 1), Some(32));
    assert_eq!(decode_bit_packed_val::<u32>(&bytes, bytes_required, 2), None);
}

#[inline]
fn count_values_for_ids<F, T: IndexIdToParentData>(ids: &[u32], top: Option<u32>, avg_join_size:f32, max_value_id:u32, get_value: F) -> FnvHashMap<T, usize>
where
    F: Fn(u64) -> Option<T>
{
    if should_prefer_vec(ids.len() as u32, avg_join_size, max_value_id) {
        let mut dat = vec![];
        dat.resize(max_value_id as usize + 1, T::zero());
        count_values_for_ids_for_agg(ids, top, dat, get_value)
    }else {
        let map = FnvHashMap::default();
        // map.reserve((ids.len() as f32 * avg_join_size) as usize); TODO TO PROPERLY RESERVE HERE, NUMBER OF DISTINCT VALUES IS NEEDED IN THE INDEX
        count_values_for_ids_for_agg(ids, top, map, get_value)
    }

}

#[derive(Debug, Default, HeapSizeOf)]
pub struct IndexIdToOneParent<T: IndexIdToParentData, K:IndexIdToParentData> {
    pub data: Vec<K>,
    pub ok: PhantomData<T>,
    pub max_value_id: u32,
    pub avg_join_size: f32,
}

impl<T: IndexIdToParentData, K:IndexIdToParentData> TypeInfo for IndexIdToOneParent<T, K> {
    fn type_name(&self) -> String {
        unsafe { std::intrinsics::type_name::<Self>().to_string() }
    }
}

impl<T: IndexIdToParentData, K:IndexIdToParentData> IndexIdToParent for IndexIdToOneParent<T, K> {
    type Output = T;

    #[inline]
    fn count_values_for_ids(&self, ids: &[u32], top: Option<u32>) -> FnvHashMap<T, usize> {
        count_values_for_ids(ids, top, self.avg_join_size, self.max_value_id, |id: u64| self.get_value(id))
    }

    fn get_keys(&self) -> Vec<T> {
        (num::cast(0).unwrap()..num::cast(self.data.len()).unwrap()).collect()
    }

    #[inline]
    fn get_values_iter(&self, id: u64) -> VintArrayIteratorOpt {
        if let Some(val) = self.get_value(id) {
            VintArrayIteratorOpt::from_single_val(num::cast(val).unwrap())
        } else {
            VintArrayIteratorOpt::empty()
        }
    }

    fn get_value(&self, id: u64) -> Option<T> {
        let val = self.data.get(id as usize);
        match val {
            Some(val) => {
                if val.to_u32().unwrap() == EMPTY_BUCKET {
                    None
                } else {
                    Some(num::cast(*val - K::one()).unwrap())
                }
            }
            None => None,
        }
    }

    #[inline]
    fn get_values(&self, id: u64) -> Option<Vec<T>> {
        self.get_value(id).map(|el| vec![el])
    }

    #[inline]
    fn get_num_keys(&self) -> usize {
        self.data.len()
    }
}

#[inline]
fn count_values_for_ids_for_agg<C:AggregationCollector<T>, T: IndexIdToParentData, F>(ids: &[u32], top: Option<u32>, mut coll:C, get_value: F) -> FnvHashMap<T, usize>
where
    F: Fn(u64) -> Option<T>,
{
    // let mut coll: Box<AggregationCollector<T>> = get_collector(ids.len() as u32, 1.0, max_value_id);
    for id in ids {
        if let Some(hit) = get_value(u64::from(*id)) {
            coll.add(hit);
        }
    }
    Box::new(coll).to_map(top)
}

// #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
// pub struct ParallelArrays<T: IndexIdToParentData> {
//     pub values1: Vec<T>,
//     pub values2: Vec<T>,
// }

// impl<T: IndexIdToParentData> IndexIdToParent for ParallelArrays<T> {
//     type Output = T;

//     fn get_keys(&self) -> Vec<T> {
//         let mut keys: Vec<T> = self.values1.iter().map(|el| num::cast(*el).unwrap()).collect();
//         keys.sort();
//         keys.dedup();
//         keys
//     }
//     #[inline]
//     fn get_values(&self, id: u64) -> Option<Vec<T>> {
//         let mut result = Vec::new();
//         let casted_id = num::cast(id).unwrap();
//         if let Ok(mut pos) = self.values1.binary_search(&casted_id) {
//             //this is not a lower_bounds search so we MUST move to the first hit
//             while pos != 0 && self.values1[pos - 1] == casted_id {
//                 pos -= 1;
//             }
//             let val_len = self.values1.len();
//             while pos < val_len && self.values1[pos] == casted_id {
//                 result.push(self.values2[pos]);
//                 pos += 1;
//             }
//         }
//         if result.is_empty() {
//             None
//         } else {
//             Some(result)
//         }
//     }
// }
// impl<T: IndexIdToParentData> HeapSizeOf for ParallelArrays<T> {
//     fn heap_size_of_children(&self) -> usize {
//         self.values1.heap_size_of_children() + self.values2.heap_size_of_children()
//     }
// }

#[derive(Debug)]
pub struct SingleArrayMMAPPacked<T: IndexIdToParentData> {
    pub data_file: Mmap,
    pub size: usize, //TODO PLS FIX avg_join_size
    pub max_value_id: u32,
    pub ok: PhantomData<T>,
    pub bytes_required: BytesRequired,
}

impl<T: IndexIdToParentData> SingleArrayMMAPPacked<T> {
    fn get_size(&self) -> usize {
        self.size
    }

    pub fn from_path(path: &str, max_value_id: u32) -> Result<Self, io::Error> {
        let data_file = unsafe { MmapOptions::new().map(&File::open(path)?).unwrap() };
        Ok(SingleArrayMMAPPacked {
            data_file,
            size: File::open(path)?.metadata()?.len() as usize / get_bytes_required(max_value_id) as usize,
            max_value_id,
            ok: PhantomData,
            bytes_required: get_bytes_required(max_value_id),
        })
    }
}
impl<T: IndexIdToParentData> HeapSizeOf for SingleArrayMMAPPacked<T> {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for SingleArrayMMAPPacked<T> {
    type Output = T;

    fn get_keys(&self) -> Vec<T> {
        (num::cast(0).unwrap()..num::cast(self.get_size()).unwrap()).collect()
    }

    #[inline]
    default fn get_num_keys(&self) -> usize {
        self.get_size()
    }

    #[inline]
    default fn get_values(&self, id: u64) -> Option<Vec<T>> {
        self.get_value(id).map(|el| vec![el])
    }

    #[inline]
    default fn get_value(&self, id: u64) -> Option<T> {
        decode_bit_packed_val::<T>(&self.data_file, self.bytes_required, id as usize)
    }

    #[inline]
    fn get_values_iter(&self, id: u64) -> VintArrayIteratorOpt {
        if let Some(val) = self.get_value(id) {
            VintArrayIteratorOpt::from_single_val(num::cast(val).unwrap())
        } else {
            VintArrayIteratorOpt::empty()
        }
    }
}


#[derive(Debug)]
pub struct SingleArrayMMAP<T: IndexIdToParentData> {
    pub data_file: Mmap,
    pub size: usize, //TODO PLS FIX add avg_join_size
    pub max_value_id: u32,
    pub ok: PhantomData<T>,
}

impl<T: IndexIdToParentData> SingleArrayMMAP<T> {
    #[inline]
    fn get_size(&self) -> usize {
        self.size
    }

    pub fn from_path(path: &str, max_value_id: u32) -> Result<Self, io::Error> {
        let data_file = unsafe { MmapOptions::new().map(&File::open(path)?).unwrap() };
        Ok(SingleArrayMMAP {
            data_file,
            size: File::open(path)?.metadata()?.len() as usize / std::mem::size_of::<T>(),
            max_value_id,
            ok: PhantomData,
        })
    }
}
impl<T: IndexIdToParentData> HeapSizeOf for SingleArrayMMAP<T> {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for SingleArrayMMAP<T> {
    type Output = T;

    fn get_keys(&self) -> Vec<T> {
        (num::cast(0).unwrap()..num::cast(self.get_size()).unwrap()).collect()
    }

    #[inline]
    default fn get_num_keys(&self) -> usize {
        self.get_size()
    }

    default fn get_values(&self, id: u64) -> Option<Vec<T>> {
        self.get_value(id).map(|el| vec![el])
    }

    default fn get_value(&self, _find: u64) -> Option<T> {
        unimplemented!() // implemented for u32, u64
    }

    fn get_values_iter(&self, id: u64) -> VintArrayIteratorOpt {
        if let Some(val) = self.get_value(id) {
            VintArrayIteratorOpt::from_single_val(num::cast(val).unwrap())
        } else {
            VintArrayIteratorOpt::empty()
        }
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
        if id == EMPTY_BUCKET {
            None
        } else {
            Some(num::cast(id - 1).unwrap())
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
        if id == u64::from(EMPTY_BUCKET) {
            None
        } else {
            Some(num::cast(id - 1).unwrap())
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use rand;
    use test;

    // fn get_test_data_1_to_1<T: IndexIdToParentData>() -> IndexIdToOneParent<T> {
    //     let values = vec![5, 6, 9, 9, 9, 50000];
    //     IndexIdToOneParent {
    //         data: values.iter().map(|el| num::cast(*el).unwrap()).collect(),
    //         max_value_id: 50000,
    //         avg_join_size: 1.0
    //     }
    // }

    fn get_test_data_1_to_1() -> Vec<u32> {
        vec![5, 6, 9, 9, 9, 50000]
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

        let empty_vec: Vec<u32> = vec![];
        assert_eq!(store.get_values_iter(0).collect::<Vec<u32>>(), vec![5]);
        assert_eq!(store.get_values_iter(1).collect::<Vec<u32>>(), vec![6]);
        assert_eq!(store.get_values_iter(2).collect::<Vec<u32>>(), vec![9]);
        assert_eq!(store.get_values_iter(3).collect::<Vec<u32>>(), vec![9]);
        assert_eq!(store.get_values_iter(4).collect::<Vec<u32>>(), vec![9]);
        assert_eq!(store.get_values_iter(5).collect::<Vec<u32>>(), vec![50000]);
        assert_eq!(store.get_values_iter(6).collect::<Vec<u32>>(), empty_vec);
        assert_eq!(store.get_values_iter(11).collect::<Vec<u32>>(), empty_vec);

        // let map = store.count_values_for_ids(&[0, 1, 2, 3, 4, 5], None);
        // assert_eq!(map.get(&5).unwrap(), &1);
        // assert_eq!(map.get(&9).unwrap(), &3);

    }

    mod test_direct_1_to_1 {
        use tempfile::tempdir;
        use super::*;
        // #[test]
        // fn test_index_id_to_parent_im() {
        //     let store = get_test_data_1_to_1::<u32>();
        //     check_test_data_1_to_1(&store);
        // }

        #[test]
        fn test_index_id_to_parent_flushing() {
            let dir = tempdir().unwrap();
            let data_path = dir.path().join("data").to_str().unwrap().to_string();
            let mut ind = IndexIdToOneParentFlushing::new(data_path.to_string(), *get_test_data_1_to_1().iter().max().unwrap());
            for (key, val) in get_test_data_1_to_1().iter().enumerate() {
                ind.add(key as u32, *val as u32).unwrap();
                ind.flush().unwrap();
            }
            let store = SingleArrayMMAPPacked::<u32>::from_path(&data_path, ind.max_value_id).unwrap();
            check_test_data_1_to_1(&store);
        }

        #[test]
        fn test_index_id_to_parent_im() {
            let dir = tempdir().unwrap();
            let data_path = dir.path().join("data").to_str().unwrap().to_string();
            let mut ind = IndexIdToOneParentFlushing::new(data_path.to_string(), *get_test_data_1_to_1().iter().max().unwrap());
            for (key, val) in get_test_data_1_to_1().iter().enumerate() {
                ind.add(key as u32, *val as u32).unwrap();
            }
            check_test_data_1_to_1(&ind.into_im_store());
        }

    }

    mod test_indirect {
        use super::*;
        use rand::distributions::{IndependentSample, Range};

        pub(crate) fn bench_fnvhashmap_group_by(num_entries: u32, max_val: u32) -> FnvHashMap<u32, u32> {
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

        pub(crate) fn bench_vec_group_by_direct(num_entries: u32, max_val: u32, hits: &mut Vec<u32>) -> &mut Vec<u32> {
            // let mut hits:Vec<u32> = vec![];
            hits.resize(max_val as usize + 1, 0);
            let mut rng = rand::thread_rng();
            let between = Range::new(0, max_val);
            for _x in 0..num_entries {
                hits[between.ind_sample(&mut rng) as usize] += 1;
            }
            hits
        }
        pub(crate) fn bench_vec_group_by_direct_u8(num_entries: u32, max_val: u32, hits: &mut Vec<u8>) -> &mut Vec<u8> {
            // let mut hits:Vec<u32> = vec![];
            hits.resize(max_val as usize + 1, 0);
            let mut rng = rand::thread_rng();
            let between = Range::new(0, max_val);
            for _x in 0..num_entries {
                hits[between.ind_sample(&mut rng) as usize] += 1;
            }
            hits
        }

        pub(crate) fn bench_vec_group_by_flex(num_entries: u32, max_val: u32) -> Vec<u32> {
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
