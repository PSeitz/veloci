use super::{super::*, *};

use std::{
    self,
    io::{self, Write},
    u32,
};

/// This data structure assumes that a set is only called once for a id, and ids are set in order.
#[derive(Serialize, Debug, Clone, Default)]
pub(crate) struct IndexIdToOneParentFlushing {
    pub(crate) cache: Vec<u32>,
    pub(crate) current_id_offset: u32,
    pub(crate) path: PathBuf,
    pub(crate) metadata: IndexValuesMetadata,
}

impl IndexIdToOneParentFlushing {
    pub(crate) fn new(path: PathBuf, max_value_id: u32) -> IndexIdToOneParentFlushing {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::*;

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
        use std::fs::File;
        use tempfile::tempdir;

        #[test]
        fn test_index_id_to_parent_flushing() {
            let dir = tempdir().unwrap();
            let data_path = dir.path().join("data");
            let mut ind = IndexIdToOneParentFlushing::new(data_path.clone(), *get_test_data_1_to_1().iter().max().unwrap());
            for (key, val) in get_test_data_1_to_1().iter().enumerate() {
                ind.add(key as u32, *val).unwrap();
                ind.flush().unwrap();
            }
            let store = SingleArrayMMAPPacked::<u32>::from_file(&File::open(data_path).unwrap(), ind.metadata).unwrap();
            check_test_data_1_to_1(&store);
        }

        #[test]
        fn test_index_id_to_parent_im() {
            let dir = tempdir().unwrap();
            let data_path = dir.path().join("data");
            let mut ind = IndexIdToOneParentFlushing::new(data_path, *get_test_data_1_to_1().iter().max().unwrap());
            for (key, val) in get_test_data_1_to_1().iter().enumerate() {
                ind.add(key as u32, *val).unwrap();
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
