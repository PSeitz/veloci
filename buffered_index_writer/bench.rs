extern crate itertools;

#[macro_use]
extern crate log;
#[macro_use]
extern crate measure_time;
extern crate env_logger;

extern crate buffered_index_writer;
extern crate tempfile;

use itertools::Itertools;

use std::io::prelude::*;
use std::io::BufWriter;
use tempfile::tempfile;

fn main() {
    env_logger::init();
    test_vec();
    test_vec_buffwriter();
    test_vec_parrallel();
    // test_tree();
    test_vec();
    test_vec_buffwriter();
    test_vec_parrallel();
    // test_tree();

    // test_vec_buffwriter();
    // test_vec_buffwriter();
}

fn u32_to_bytes(val: u32) -> [u8; 4] {
    let buffer: [u8; 4] = unsafe { std::mem::transmute(val) };
    buffer
}

#[inline]
pub fn vec_with_size_uninitialized<T>(size: usize) -> Vec<T> {
    let mut buffer = Vec::with_capacity(size);
    unsafe {
        buffer.set_len(size);
    }
    buffer
}

fn test_vec() {
    let num_loops = 65_000_000;
    info_time!("vecco group by");
    let mut vecco: Vec<(u32, u32)> = vec![];
    for i in 0..num_loops {
        let id = i * 7 % 850_000;
        vecco.push((id, 5));
    }
    vecco.sort_unstable_by_key(|el| el.0);

    let mut ids_file = BufWriter::new(tempfile().unwrap());
    let mut data_file = BufWriter::new(tempfile().unwrap());

    for (id, group) in &vecco.iter().group_by(|el| el.0) {

        ids_file.write(&u32_to_bytes(id)).unwrap();
        let mut num = 0;
        for el in group {
            data_file.write(&u32_to_bytes(el.0)).unwrap();
            num += 1;
        }
        ids_file.write(&u32_to_bytes(num)).unwrap();
    }

    // (all_data, all_ids)
}

fn test_vec_buffwriter() -> (u32, u32) {
    let mut ind = buffered_index_writer::BufferedIndexWriter::new();

    let num_loops = 65_000_000;
    info_time!("test_vec_buffwriter");
    for i in 0..num_loops {
        let id = i * 7 % 850_000;
        ind.add(id, 5).unwrap();
    }
    ind.flush().unwrap();

    let data: Vec<_> = ind.flush_and_kmerge().unwrap().collect();
    data[0]
}

fn test_vec_parrallel() {
    let num_loops = 65_000_000;
    info_time!("vecco parraeeel");
    let mut vecco: Vec<(u32, u32)> = vec![];
    for i in 0..num_loops {
        let id = i * 7 % 850_000;
        vecco.push((id, 5));
    }
    vecco.sort_unstable_by_key(|el| el.0);

    let mut ids_file = BufWriter::new(tempfile().unwrap());
    let mut data_file = BufWriter::new(tempfile().unwrap());

    let vec1: Vec<u32> = vecco.iter().map(|el| el.0).collect();
    let vec2: Vec<u32> = vecco.iter().map(|el| el.1).collect();

    //Maximum speed, Maximum unsafe
    use std::slice;
    unsafe {
        ids_file.write(&slice::from_raw_parts(vec1.as_ptr() as *const u8, vec1.len() * 4)).unwrap();
        data_file.write(&slice::from_raw_parts(vec2.as_ptr() as *const u8, vec2.len() * 4)).unwrap();
    }
}

use std::collections::BTreeMap;

fn test_tree() -> BTreeMap<u32, Vec<u32>> {
    let num_loops = 65_000_000;
    info_time!("BTreeMap");
    let mut tree: BTreeMap<u32, Vec<u32>> = BTreeMap::default();
    for i in 0..num_loops {
        let id = i * 7 % 850_000;
        let data = tree.entry(id).or_insert(vec![]);
        data.push(5);
    }

    tree
    // let mut ids_file = BufWriter::new(tempfile().unwrap());
    // let mut data_file = BufWriter::new(tempfile().unwrap());

    // for (id, data) in tree {
    //     // all_ids.push((id, data.len() as u32));
    //     // all_data.push(data.len() as u32);
    //     // all_data.extend(data);
    //     // ids_file.write(&u32_to_bytes(id)).unwrap();
    //     // ids_file.write(&u32_to_bytes(data.len() as u32)).unwrap();

    //     // let data_len = data.len();
    //     // let data_ptr = data.as_ptr() as *const u8;

    //     // for el in data {
    //     //     // data_file.write(&all_data).unwrap();
    //     //     data_file.write(&u32_to_bytes(el)).unwrap();
    //     // }

    // }

    // (all_data, all_ids)
}
