#[cfg(feature = "create")]
mod create_indirect;
mod indirect_im;
mod indirect_mmap;

use crate::util::{is_hight_bit_set, unset_high_bit};

#[cfg(feature = "create")]
pub(crate) use create_indirect::*;
pub(crate) use indirect_im::*;
pub(crate) use indirect_mmap::*;

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
    use crate::persistence::IndexIdToParent;

    fn get_test_data_1_to_n_ind(path: &str) -> IndirectIMFlushingInOrderVint {
        let mut store = IndirectIMFlushingInOrderVint::new(path.to_string(), std::u32::MAX);
        store.add(0, vec![5, 6]).unwrap();
        store.add(1, vec![9]).unwrap();
        store.add(2, vec![9]).unwrap();
        store.add(3, vec![9, 50000]).unwrap();
        store.add(5, vec![80]).unwrap();
        store.add(9, vec![0]).unwrap();
        store.add(10, vec![0]).unwrap();
        store
    }

    fn check_test_data_1_to_n(store: &dyn IndexIdToParent<Output = u32>) {
        // assert_eq!(store.get_keys(), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
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
    fn check_test_data_1_to_n_iter(store: &dyn IndexIdToParent<Output = u32>) {
        let empty_vec: Vec<u32> = vec![];
        // assert_eq!(store.get_keys(), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
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

            let store = IndirectMMap::from_path(&path, store.metadata).unwrap();
            check_test_data_1_to_n(&store);
            check_test_data_1_to_n_iter(&store);
        }

        // #[test]
        // fn test_flushing_in_order_indirect() {
        //     let dir = tempdir().unwrap();
        //     let path = dir.path().join("testfile").to_str().unwrap().to_string();
        //     let store = get_test_data_1_to_n_ind(&path).into_im_store();

        //     let mut ind = IndirectIMFlushingInOrderVint::new(path.to_string(), u32::MAX);

        //     for key in store.get_keys() {
        //         if let Some(vals) = store.get_values(key.into()) {
        //             ind.add(key, vals).unwrap();
        //             ind.flush().unwrap();
        //         }
        //     }
        //     ind.flush().unwrap();

        //     let store = IndirectMMap::from_path(&path, store.metadata).unwrap();
        //     check_test_data_1_to_n(&store);
        //     check_test_data_1_to_n_iter(&store);
        // }

        #[test]
        fn test_pointing_array_index_id_to_multiple_parent_indirect() {
            let store = get_test_data_1_to_n_ind("test_ind");
            let store = store.into_im_store();
            check_test_data_1_to_n(&store);
            check_test_data_1_to_n_iter(&store);
        }
    }
}
