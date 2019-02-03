use std;
use std::fs::File;
use std::io;
use std::io::Write;
use std::marker::PhantomData;
use std::ptr::copy_nonoverlapping;
use std::u32;
// use byteorder::{LittleEndian, ReadBytesExt};
// use heapsize::HeapSizeOf;

use crate::error::VelociError;
use crate::persistence::EMPTY_BUCKET;
use crate::persistence::*;
pub(crate) use crate::persistence_data_indirect::*;

use crate::facet::*;
use num;

use crate::type_info::TypeInfo;
use fnv::FnvHashMap;

use memmap::Mmap;
use memmap::MmapOptions;

impl_type_info_single_templ!(SingleArrayMMAPPacked);

/// This data structure assumes that a set is only called once for a id, and ids are set in order.
#[derive(Serialize, Debug, Clone, Default)]
pub(crate) struct IndexIdToOneParentFlushing {
    pub(crate) cache: Vec<u32>,
    pub(crate) current_id_offset: u32,
    pub(crate) path: String,
    pub(crate) metadata: IndexValuesMetadata,
}

impl IndexIdToOneParentFlushing {
    pub(crate) fn new(path: String, max_value_id: u32) -> IndexIdToOneParentFlushing {
        IndexIdToOneParentFlushing {
            path,
            metadata: IndexValuesMetadata {
                max_value_id,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub(crate) fn into_im_store(self) -> SingleArrayIM<u32, u32> {
        let mut store = SingleArrayIM::default();
        store.metadata.avg_join_size = calc_avg_join_size(self.metadata.num_values, self.cache.len() as u32);
        store.data = self.cache;
        store.metadata = self.metadata;
        store
    }

    // pub(crate) fn into_store(mut self) -> Result<Box<dyn IndexIdToParent<Output = u32>>, VelociError> {
    //     if self.is_in_memory() {
    //         Ok(Box::new(self.into_im_store()))
    //     } else {
    //         self.flush()?;
    //         let store = SingleArrayMMAPPacked::<u32>::from_file(&File::open(self.path)?, self.metadata)?;
    //         Ok(Box::new(store))
    //     }
    // }

    #[inline]
    pub(crate) fn add(&mut self, id: u32, val: u32) -> Result<(), io::Error> {
        self.metadata.num_values += 1;

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
    pub(crate) fn is_in_memory(&self) -> bool {
        self.current_id_offset == 0
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.cache.is_empty() && self.current_id_offset == 0
    }

    pub(crate) fn flush(&mut self) -> Result<(), io::Error> {
        if self.cache.is_empty() {
            return Ok(());
        }

        self.current_id_offset += self.cache.len() as u32;

        let mut data = std::fs::OpenOptions::new().read(true).write(true).append(true).create(true).open(&self.path)?;

        let bytes_required = get_bytes_required(self.metadata.max_value_id);

        let mut bytes = vec![];
        encode_vals(&self.cache, bytes_required, &mut bytes)?;
        data.write_all(&bytes)?;

        self.metadata.avg_join_size = calc_avg_join_size(self.metadata.num_values, self.current_id_offset + self.cache.len() as u32);
        self.cache.clear();

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum BytesRequired {
    One = 1,
    Two,
    Three,
    Four,
}

#[inline]
pub(crate) fn get_bytes_required(mut val: u32) -> BytesRequired {
    val += val; //+1 because EMPTY_BUCKET = 0 is already reserved
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
pub(crate) fn encode_vals<O: std::io::Write>(vals: &[u32], bytes_required: BytesRequired, out: &mut O) -> Result<(), io::Error> {
    //Maximum speed, Maximum unsafe
    use std::slice;
    unsafe {
        let slice = slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * mem::size_of::<u32>());
        let mut pos = 0;
        while pos != slice.len() {
            out.write_all(&slice[pos..pos + bytes_required as usize])?;
            pos += 4;
        }
    }
    Ok(())
}

#[inline]
pub(crate) fn decode_bit_packed_val<T: IndexIdToParentData>(data: &[u8], bytes_required: BytesRequired, index: usize) -> Option<T> {
    let bit_pos_start = index * bytes_required as usize;
    if bit_pos_start >= data.len() {
        None
    } else {
        let mut out = T::zero();
        unsafe {
            copy_nonoverlapping(data.as_ptr().add(bit_pos_start), &mut out as *mut T as *mut u8, bytes_required as usize);
        }
        if out == T::zero() {
            // == EMPTY_BUCKET
            None
        } else {
            Some(out - T::one())
        }
    }
}

pub(crate) fn decode_bit_packed_vals<T: IndexIdToParentData>(data: &[u8], bytes_required: BytesRequired) -> Vec<T> {
    let mut out: Vec<u8> = vec![];
    out.resize(data.len() * std::mem::size_of::<T>() / bytes_required as usize, 0);
    let mut pos = 0;
    let mut out_pos = 0;
    while pos < data.len() {
        out[out_pos..out_pos + bytes_required as usize].clone_from_slice(&data[pos..pos + bytes_required as usize]);
        pos += bytes_required as usize;
        out_pos += std::mem::size_of::<T>();
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
fn count_values_for_ids<F, T: IndexIdToParentData>(ids: &[u32], top: Option<u32>, avg_join_size: f32, max_value_id: u32, get_value: F) -> FnvHashMap<T, usize>
where
    F: Fn(u64) -> Option<T>,
{
    if should_prefer_vec(ids.len() as u32, avg_join_size, max_value_id) {
        let mut dat = vec![];
        dat.resize(max_value_id as usize + 1, T::zero());
        count_values_for_ids_for_agg(ids, top, dat, get_value)
    } else {
        let map = FnvHashMap::default();
        // map.reserve((ids.len() as f32 * avg_join_size) as usize); TODO TO PROPERLY RESERVE HERE, NUMBER OF DISTINCT VALUES IS NEEDED IN THE INDEX
        count_values_for_ids_for_agg(ids, top, map, get_value)
    }
}

#[derive(Debug, Default)]
pub(crate) struct SingleArrayIM<T: IndexIdToParentData, K: IndexIdToParentData> {
    pub(crate) data: Vec<K>,
    pub(crate) ok: PhantomData<T>,
    pub(crate) metadata: IndexValuesMetadata,
}

impl<T: IndexIdToParentData, K: IndexIdToParentData> TypeInfo for SingleArrayIM<T, K> {
    fn type_name(&self) -> String {
        unsafe { std::intrinsics::type_name::<Self>().to_string() }
    }
}

impl<T: IndexIdToParentData, K: IndexIdToParentData> IndexIdToParent for SingleArrayIM<T, K> {
    type Output = T;

    #[inline]
    fn count_values_for_ids(&self, ids: &[u32], top: Option<u32>) -> FnvHashMap<T, usize> {
        count_values_for_ids(ids, top, self.metadata.avg_join_size, self.metadata.max_value_id, |id: u64| self.get_value(id))
    }

    // fn get_keys(&self) -> Vec<T> {
    //     (num::cast(0).unwrap()..num::cast(self.data.len()).unwrap()).collect()
    // }

    #[inline]
    fn get_values_iter(&self, id: u64) -> VintArrayIteratorOpt<'_> {
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
                if val.to_u32().unwrap_or_else(|| panic!("could not cast to u32 {:?}", val)) == EMPTY_BUCKET {
                    None
                } else {
                    Some(num::cast(*val - K::one()).unwrap_or_else(|| panic!("could not cast to u32 {:?}", *val - K::one())))
                }
            }
            None => None,
        }
    }

    #[inline]
    fn get_values(&self, id: u64) -> Option<Vec<T>> {
        self.get_value(id).map(|el| vec![el])
    }

    // #[inline]
    // fn get_num_keys(&self) -> usize {
    //     self.data.len()
    // }
}

#[inline]
fn count_values_for_ids_for_agg<C: AggregationCollector<T>, T: IndexIdToParentData, F>(ids: &[u32], top: Option<u32>, mut coll: C, get_value: F) -> FnvHashMap<T, usize>
where
    F: Fn(u64) -> Option<T>,
{
    for id in ids {
        if let Some(hit) = get_value(u64::from(*id)) {
            coll.add(hit);
        }
    }
    Box::new(coll).to_map(top)
}

#[derive(Debug)]
// Loads integer with flexibel widths 1, 2 or 4 byte
pub(crate) struct SingleArrayMMAPPacked<T: IndexIdToParentData> {
    pub(crate) data_file: Mmap,
    pub(crate) size: usize,
    pub(crate) metadata: IndexValuesMetadata,
    pub(crate) ok: PhantomData<T>,
    pub(crate) bytes_required: BytesRequired,
}

impl<T: IndexIdToParentData> SingleArrayMMAPPacked<T> {
    // fn get_size(&self) -> usize {
    //     self.size
    // }

    pub(crate) fn from_file(file: &File, metadata: IndexValuesMetadata) -> Result<Self, VelociError> {
        let data_file = unsafe { MmapOptions::new().map(&file)? };
        Ok(SingleArrayMMAPPacked {
            data_file,
            size: file.metadata()?.len() as usize / get_bytes_required(metadata.max_value_id) as usize,
            metadata,
            ok: PhantomData,
            bytes_required: get_bytes_required(metadata.max_value_id),
        })
    }
    // pub(crate) fn from_path(path: &str, metadata: IndexValuesMetadata) -> Result<Self, VelociError> {
    //     let data_file = unsafe { MmapOptions::new().map(&open_file(path)?)? };
    //     Ok(SingleArrayMMAPPacked {
    //         data_file,
    //         size: File::open(path)?.metadata()?.len() as usize / get_bytes_required(metadata.max_value_id) as usize,
    //         metadata,
    //         ok: PhantomData,
    //         bytes_required: get_bytes_required(metadata.max_value_id),
    //     })
    // }
}
// impl<T: IndexIdToParentData> HeapSizeOf for SingleArrayMMAPPacked<T> {
//     fn heap_size_of_children(&self) -> usize {
//         0
//     }
// }

impl<T: IndexIdToParentData> IndexIdToParent for SingleArrayMMAPPacked<T> {
    type Output = T;

    // fn get_keys(&self) -> Vec<T> {
    //     (num::cast(0).unwrap()..num::cast(self.get_size()).unwrap()).collect()
    // }

    // #[inline]
    // default fn get_num_keys(&self) -> usize {
    //     self.get_size()
    // }

    #[inline]
    default fn get_values(&self, id: u64) -> Option<Vec<T>> {
        self.get_value(id).map(|el| vec![el])
    }

    #[inline]
    default fn get_value(&self, id: u64) -> Option<T> {
        decode_bit_packed_val::<T>(&self.data_file, self.bytes_required, id as usize)
    }

    #[inline]
    fn get_values_iter(&self, id: u64) -> VintArrayIteratorOpt<'_> {
        if let Some(val) = self.get_value(id) {
            VintArrayIteratorOpt::from_single_val(num::cast(val).unwrap())
        } else {
            VintArrayIteratorOpt::empty()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use rand;
    // use test;

    // fn get_test_data_1_to_1<T: IndexIdToParentData>() -> SingleArrayIM<T> {
    //     let values = vec![5, 6, 9, 9, 9, 50000];
    //     SingleArrayIM {
    //         data: values.iter().map(|el| num::cast(*el).unwrap()).collect(),
    //         max_value_id: 50000,
    //         avg_join_size: 1.0
    //     }
    // }

    fn get_test_data_1_to_1() -> Vec<u32> {
        vec![5, 6, 9, 9, 9, 50000]
    }
    fn check_test_data_1_to_1<T: IndexIdToParentData>(store: &dyn IndexIdToParent<Output = T>) {
        // assert_eq!(store.get_keys().iter().map(|el| el.to_u32().unwrap()).collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 5]);
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
    }

    mod test_direct_1_to_1 {
        use super::*;
        use tempfile::tempdir;

        #[test]
        fn test_index_id_to_parent_flushing() {
            let dir = tempdir().unwrap();
            let data_path = dir.path().join("data").to_str().unwrap().to_string();
            let mut ind = IndexIdToOneParentFlushing::new(data_path.to_string(), *get_test_data_1_to_1().iter().max().unwrap());
            for (key, val) in get_test_data_1_to_1().iter().enumerate() {
                ind.add(key as u32, *val as u32).unwrap();
                ind.flush().unwrap();
            }
            let store = SingleArrayMMAPPacked::<u32>::from_file(&File::open(data_path).unwrap(), ind.metadata).unwrap();
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
        // use super::*;
        // use rand::Rng;
        // pub(crate) fn bench_fnvhashmap_group_by(num_entries: u32, max_val: u32) -> FnvHashMap<u32, u32> {
        //     let mut hits: FnvHashMap<u32, u32> = FnvHashMap::default();
        //     hits.reserve(num_entries as usize);
        //     let mut rng = rand::thread_rng();
        //     for _x in 0..num_entries {
        //         let stat = hits.entry(rng.gen_range(0, max_val)).or_insert(0);
        //         *stat += 1;
        //     }
        //     hits
        // }

        // pub(crate) fn bench_vec_group_by_direct(num_entries: u32, max_val: u32, hits: &mut Vec<u32>) -> &mut Vec<u32> {
        //     hits.resize(max_val as usize + 1, 0);
        //     let mut rng = rand::thread_rng();
        //     for _x in 0..num_entries {
        //         hits[rng.gen_range(0, max_val as usize)] += 1;
        //     }
        //     hits
        // }
        // pub(crate) fn bench_vec_group_by_direct_u8(num_entries: u32, max_val: u32, hits: &mut Vec<u8>) -> &mut Vec<u8> {
        //     hits.resize(max_val as usize + 1, 0);
        //     let mut rng = rand::thread_rng();
        //     for _x in 0..num_entries {
        //         hits[rng.gen_range(0, max_val as usize)] += 1;
        //     }
        //     hits
        // }

        // pub(crate) fn bench_vec_group_by_flex(num_entries: u32, max_val: u32) -> Vec<u32> {
        //     let mut hits: Vec<u32> = vec![];
        //     let mut rng = rand::thread_rng();
        //     for _x in 0..num_entries {
        //         let id = rng.gen_range(0, max_val as usize);
        //         if hits.len() <= id {
        //             hits.resize(id + 1, 0);
        //         }
        //         hits[id] += 1;
        //     }
        //     hits
        // }

        //20x break even ?
        // #[bench]
        // fn bench_group_by_fnvhashmap_0(b: &mut test::Bencher) {
        //     b.iter(|| {
        //         bench_fnvhashmap_group_by(700_000, 5_000_000);
        //     })
        // }

        // #[bench]
        // fn bench_group_by_vec_direct_0(b: &mut test::Bencher) {
        //     b.iter(|| {
        //         bench_vec_group_by_direct(700_000, 5_000_000, &mut vec![]);
        //     })
        // }
        // #[bench]
        // fn bench_group_by_vec_direct_u16_0(b: &mut test::Bencher) {
        //     b.iter(|| {
        //         bench_vec_group_by_direct_u8(700_000, 5_000_000, &mut vec![]);
        //     })
        // }

        // #[bench]
        // fn bench_group_by_vec_direct_0_pre_alloc(b: &mut test::Bencher) {
        //     let mut dat = vec![];
        //     b.iter(|| {
        //         bench_vec_group_by_direct(700_000, 5_000_000, &mut dat);
        //     })
        // }

        // #[bench]
        // fn bench_group_by_vec_flex_0(b: &mut test::Bencher) {
        //     b.iter(|| {
        //         bench_vec_group_by_flex(700_000, 5_000_000);
        //     })
        // }
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
