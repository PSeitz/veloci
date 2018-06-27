use std;
use std::u32;
use std::io;
use std::io::Write;
use std::marker::PhantomData;
use std::fs::{self, File};

use heapsize::HeapSizeOf;
use byteorder::{LittleEndian, ReadBytesExt};

use persistence::EMPTY_BUCKET;
use persistence::*;
pub(crate) use persistence_data_indirect::*;

use facet::*;
use parking_lot::Mutex;
use num;

use type_info::TypeInfo;
use fnv::FnvHashMap;

use memmap::Mmap;
use memmap::MmapOptions;

impl_type_info_single_templ!(IndexIdToOneParent);
impl_type_info_single_templ!(ParallelArrays);
impl_type_info_single_templ!(SingleArrayMMAP);

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
    pub fn new(path: String) -> IndexIdToOneParentFlushing {
        IndexIdToOneParentFlushing { path, ..Default::default() }
    }
    pub fn into_im_store(self) -> IndexIdToOneParent<u32> {
        let mut store = IndexIdToOneParent::default();
        //TODO this conversion is not needed, it's always u32
        store.avg_join_size = calc_avg_join_size(self.num_values, self.cache.len() as u32);
        store.data = self.cache;
        store.max_value_id = self.max_value_id;
        // store.num_values = self.num_values;
        // store.num_ids = self.num_ids;
        store
    }

    #[inline]
    pub fn add(&mut self, id: u32, val: u32) -> Result<(), io::Error> {
        self.max_value_id = std::cmp::max(val, self.max_value_id);
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

        data.write_all(&vec_to_bytes_u32(&self.cache))?;

        self.avg_join_size = calc_avg_join_size(self.num_values, self.current_id_offset + self.cache.len() as u32);
        self.cache.clear();

        Ok(())
    }
}

#[derive(Debug, Default, HeapSizeOf)]
pub struct IndexIdToOneParent<T: IndexIdToParentData> {
    pub data: Vec<T>,
    pub max_value_id: u32,
    pub avg_join_size: f32,
}
// impl<T: IndexIdToParentData> IndexIdToOneParent<T> {
//     pub fn new(data: &IndexIdToParent<Output = T>) -> IndexIdToOneParent<T> {
//         let data: Vec<Vec<T>> = id_to_parent_to_array_of_array(data);
//         let data = data.iter()
//             .map(|el| {
//                 if !el.is_empty() {
//                     num::cast(el[0]).unwrap()
//                 } else {
//                     num::cast(EMPTY_BUCKET).unwrap()
//                 }
//             })
//             .collect();
//         IndexIdToOneParent { data, max_value_id: u32::MAX, avg_join_size: 1.0} //TODO FIX max_value_id
//     }
// }
impl<T: IndexIdToParentData> IndexIdToParent for IndexIdToOneParent<T> {
    type Output = T;

    #[inline]
    fn count_values_for_ids(&self, ids: &[u32], top: Option<u32>) -> FnvHashMap<T, usize> {
        count_values_for_ids(ids, top, self.max_value_id, |id: u64| self.get_value(id))
    }

    fn get_keys(&self) -> Vec<T> {
        (num::cast(0).unwrap()..num::cast(self.data.len()).unwrap()).collect()
    }

    fn get_value(&self, id: u64) -> Option<T> {
        let val = self.data.get(id as usize);
        match val {
            Some(val) => {
                if val.to_u32().unwrap() == EMPTY_BUCKET {
                    None
                } else {
                    Some(*val - T::one())
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
fn count_values_for_ids<T: IndexIdToParentData, F>(ids: &[u32], top: Option<u32>, max_value_id: u32, get_value: F) -> FnvHashMap<T, usize>
where
    F: Fn(u64) -> Option<T>,
{
    let mut coll: Box<AggregationCollector<T>> = get_collector(ids.len() as u32, 1.0, max_value_id);
    for id in ids {
        if let Some(hit) = get_value(u64::from(*id)) {
            coll.add(hit);
        }
    }
    coll.to_map(top)
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ParallelArrays<T: IndexIdToParentData> {
    pub values1: Vec<T>,
    pub values2: Vec<T>,
}

impl<T: IndexIdToParentData> IndexIdToParent for ParallelArrays<T> {
    type Output = T;

    fn get_keys(&self) -> Vec<T> {
        let mut keys: Vec<T> = self.values1.iter().map(|el| num::cast(*el).unwrap()).collect();
        keys.sort();
        keys.dedup();
        keys
    }
    fn get_values_iter(&self, id: u64) -> VintArrayIteratorOpt {
        if let Some(val) = self.get_value(id) {
            VintArrayIteratorOpt::from_single_val(num::cast(val).unwrap())
        } else {
            VintArrayIteratorOpt::empty()
        }
    }
    #[inline]
    fn get_values(&self, id: u64) -> Option<Vec<T>> {
        let mut result = Vec::new();
        let casted_id = num::cast(id).unwrap();
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
}
impl<T: IndexIdToParentData> HeapSizeOf for ParallelArrays<T> {
    fn heap_size_of_children(&self) -> usize {
        self.values1.heap_size_of_children() + self.values2.heap_size_of_children()
    }
}

#[derive(Debug)]
pub struct SingleArrayMMAP<T: IndexIdToParentData> {
    pub data_file: Mmap,
    pub data_metadata: Mutex<fs::Metadata>, //TODO PLS FIXME max_value_id??, avg_join_size??
    pub max_value_id: u32,
    pub ok: PhantomData<T>,
}

impl<T: IndexIdToParentData> SingleArrayMMAP<T> {
    fn get_size(&self) -> usize {
        self.data_metadata.lock().len() as usize / std::mem::size_of::<T>()
    }

    pub fn new(data_file: &fs::File, data_metadata: fs::Metadata, max_value_id: u32) -> Self {
        let data_file = unsafe {
            MmapOptions::new()
                .len(std::cmp::max(data_metadata.len() as usize, 4048))
                .map(&data_file)
                .unwrap()
        };
        SingleArrayMMAP {
            data_file,
            data_metadata: Mutex::new(data_metadata),
            max_value_id,
            ok: PhantomData,
        }
    }
    pub fn from_path(path: &str, max_value_id: u32) -> Result<Self, io::Error> {
        let data_file = unsafe { MmapOptions::new().map(&File::open(path)?).unwrap() };
        Ok(SingleArrayMMAP {
            data_file,
            data_metadata: Mutex::new(File::open(path)?.metadata()?),
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

// pub(crate) fn id_to_parent_to_array_of_array<T: IndexIdToParentData>(store: &IndexIdToParent<Output = T>) -> Vec<Vec<T>> {
//     let mut data: Vec<Vec<T>> = prepare_data_for_array_of_array(store, &Vec::new);
//     let valids = store.get_keys();

//     for valid in valids {
//         if let Some(vals) = store.get_values(num::cast(valid).unwrap()) {
//             data[valid.to_usize().unwrap()] = vals.iter().map(|el| num::cast(*el).unwrap()).collect();
//         }
//     }
//     data
// }

// fn prepare_data_for_array_of_array<T: Clone, K: IndexIdToParentData>(store: &IndexIdToParent<Output = K>, f: &Fn() -> T) -> Vec<T> {
//     let mut data = vec![];
//     let mut valids = store.get_keys();
//     valids.dedup();
//     if valids.is_empty() {
//         return data;
//     }
//     data.resize(valids.last().unwrap().to_usize().unwrap() + 1, f());
//     data
// }

// #[cfg_attr(feature = "flame_it", flame)]
// pub(crate) fn valid_pair_to_parallel_arrays<T: IndexIdToParentData>(tuples: &mut Vec<create::ValIdPair>) -> ParallelArrays<T> {
//     tuples.sort_unstable_by_key(|a| a.valid);
//     let valids = tuples.iter().map(|el| num::cast(el.valid).unwrap()).collect::<Vec<_>>();
//     let parent_val_ids = tuples.iter().map(|el| num::cast(el.parent_val_id).unwrap()).collect::<Vec<_>>();
//     ParallelArrays {
//         values1: valids,
//         values2: parent_val_ids,
//     }
// }

// // #[cfg_attr(feature = "flame_it", flame)]
// pub(crate) fn valid_pair_to_direct_index<T: create::KeyValuePair>(tuples: &mut [T]) -> IndexIdToOneParent<u32> {
//     //-> Box<IndexIdToParent<Output = u32>> {
//     tuples.sort_unstable_by_key(|a| a.get_key());
//     let mut index = IndexIdToOneParent::<u32>::default();
//     //TODO store max_value_id and resize index
//     for el in tuples.iter() {
//         index.data.resize(el.get_key() as usize + 1, NOT_FOUND);
//         index.data[el.get_key() as usize] = el.get_value();
//         index.max_value_id = std::cmp::max(index.max_value_id, el.get_value());
//     }

//     index
// }

// // #[cfg_attr(feature = "flame_it", flame)]
// pub(crate) fn valid_pair_to_direct_index<T: create::KeyValuePair>(tuples: &mut [T]) -> IndexIdToOneParent<u32> {
//     //-> Box<IndexIdToParent<Output = u32>> {
//     tuples.sort_unstable_by_key(|a| a.get_key());
//     let mut index = IndexIdToOneParent::<u32>::default();
//     //TODO store max_value_id and resize index
//     for el in tuples.iter() {
//         index.data.resize(el.get_key() as usize + 1, NOT_FOUND);
//         index.data[el.get_key() as usize] = el.get_value();
//         index.max_value_id = std::cmp::max(index.max_value_id, el.get_value());
//     }

//     index
// }

#[test]
fn test_index_parrallel_arrays() {
    let ix = ParallelArrays {
        values1: vec![0, 0, 1],
        values2: vec![0, 1, 2],
    };
    assert_eq!(ix.get_values(0).unwrap(), vec![0, 1]);
}

// #[test]
// fn test_snap() {
//     use byteorder::WriteBytesExt;
//     let mut encoder = snap::Encoder::new();
//     let mut data: Vec<Vec<u32>> = vec![];
//     data.push(vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 110, 111, 112, 113, 114, 115, 116, 117, 118]);
//     data.push(vec![10, 11, 12, 13, 14, 15]);
//     data.push(vec![10]);
//     info!("data orig {:?}", data.heap_size_of_children());

//     let data4: Vec<Vec<u8>> = data.iter().map(|el| vec_to_bytes_u32(el)).collect();
//     info!("data byteorder {:?}", data4.heap_size_of_children());

//     let data5: Vec<Vec<u8>> = data.iter()
//         .map(|el| {
//             let mut dat = encoder.compress_vec(&vec_to_bytes_u32(el)).unwrap();
//             dat.shrink_to_fit();
//             dat
//         })
//         .collect();
//     info!("data byteorder compressed {:?}", data5.heap_size_of_children());

//     let mut wtr: Vec<u8> = vec![];
//     wtr.write_u32::<LittleEndian>(10).unwrap();
//     info!("wtr {:?}", wtr);
// }

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
            let mut ind = IndexIdToOneParentFlushing::new(data_path.to_string());
            for (key, val) in get_test_data_1_to_1().iter().enumerate() {
                ind.add(key as u32, *val as u32).unwrap();
                ind.flush().unwrap();
            }
            let store = SingleArrayMMAP::<u32>::from_path(&data_path, ind.max_value_id).unwrap();
            check_test_data_1_to_1(&store);
        }

        #[test]
        fn test_index_id_to_parent_im() {
            let dir = tempdir().unwrap();
            let data_path = dir.path().join("data").to_str().unwrap().to_string();
            let mut ind = IndexIdToOneParentFlushing::new(data_path.to_string());
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
