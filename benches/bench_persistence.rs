#![feature(collection_placement)]
#![feature(placement_in_syntax)]
#![feature(test)]

// #[macro_use]
// extern crate serde_derive;

#[macro_use]
extern crate serde_json;

extern crate rand;
extern crate search_lib;
extern crate serde;
extern crate test;

#[macro_use]
extern crate criterion;

use criterion::Criterion;
use rand::distributions::{IndependentSample, Range};
use search_lib::doc_loader::*;
use search_lib::persistence::*;
use search_lib::persistence_data_indirect::*;
use search_lib::search::*;
use search_lib::*;
use std::env;

// fn prepare_indirect_pointing_file_array(folder: &str, store: &IndexIdToParent<Output = u32>) -> PointingArrayFileReader<u32> {
//     let (max_value_id, num_values, num_ids, start_and_end_pos, data) = to_indirect_arrays(store, 0);

//     fs::create_dir_all(folder).unwrap();
//     let data_path = get_file_path(folder, "data");
//     let indirect_path = get_file_path(folder, "indirect");
//     File::create(&data_path).unwrap().write_all(&vec_to_bytes_u32(&start_and_end_pos)).unwrap();
//     File::create(&indirect_path).unwrap().write_all(&vec_to_bytes_u32(&data)).unwrap();

//     let start_and_end_file = File::open(&data_path).unwrap();
//     let data_file = File::open(&data_path).unwrap();
//     let data_metadata = fs::metadata(&data_path).unwrap();
//     let store = PointingArrayFileReader::new(start_and_end_file, data_file, data_metadata, max_value_id, calc_avg_join_size(num_values, num_ids));
//     store
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

// fn persistence(c: &mut Criterion) {
//     let store_tmp = get_test_data_large(10_000_000, 2);
//     let store = prepare_indirect_pointing_file_array("test_pointing_file_array_perf", &store_tmp);
//     let ids:Vec<u32> = (0 .. 70000).collect();

//     c.bench_function("file indirec u32 count 70_000 _values_for_ids", |b| b.iter(|| store.count_values_for_ids(&ids, Some(10))));

// }

// criterion_group!(benches, persistence);
// criterion_main!(benches);

fn main() {
    unimplemented!();
}
