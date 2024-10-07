#[cfg(feature = "create")]
mod create_indirect;
mod indirect;

use crate::util::{is_hight_bit_set, unset_high_bit};

#[cfg(feature = "create")]
pub(crate) use create_indirect::*;
pub(crate) use indirect::*;

// TODO handle u64
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
    use crate::{directory::Directory, persistence::IndexIdToParent};
    use std::path::PathBuf;

    fn get_test_data_1_to_n_ind(directory: &Box<dyn Directory>, path: PathBuf) -> IndirectFlushingInOrderVint {
        let mut store = IndirectFlushingInOrderVint::new(directory, path, u32::MAX);
        store.add(0, vec![5, 6]).unwrap();
        store.add(1, vec![9]).unwrap();
        store.add(2, vec![9]).unwrap();
        store.add(3, vec![9, 50000]).unwrap();
        store.add(5, vec![80]).unwrap();
        store.add(9, vec![0]).unwrap();
        store.add(10, vec![0]).unwrap();
        store
    }

    fn check_test_data_1_to_n(store: &Box<dyn IndexIdToParent<Output = u32>>) {
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
    fn check_test_data_1_to_n_iter(store: &Box<dyn IndexIdToParent<Output = u32>>) {
        let empty_vec: Vec<u32> = vec![];
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
        use std::path::Path;

        use crate::directory::{load_data_pair, Directory, RamDirectory};

        use super::*;
        #[test]
        fn test_pointing_file_andmmap_array() {
            let path = Path::new("testfile");
            let directory: Box<dyn Directory> = Box::<RamDirectory>::default();
            let mut store = get_test_data_1_to_n_ind(&directory, path.to_owned());
            store.flush().unwrap();

            let (ind, data) = load_data_pair(&directory, Path::new(&path)).unwrap();
            let store: Box<dyn IndexIdToParent<Output = u32>> = Box::new(Indirect::from_data(ind, data, store.metadata).unwrap());
            check_test_data_1_to_n(&store);
            check_test_data_1_to_n_iter(&store);
        }

        #[test]
        fn test_pointing_array_index_id_to_multiple_parent_indirect() {
            let directory: Box<dyn Directory> = Box::<RamDirectory>::default();
            let store = get_test_data_1_to_n_ind(&directory, PathBuf::from("test_ind"));
            let store = store.into_store().unwrap();
            check_test_data_1_to_n(&store);
            check_test_data_1_to_n_iter(&store);
        }
    }
}
