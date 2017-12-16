#[macro_use]
extern crate criterion;
extern crate itertools;
extern crate fnv;

use criterion::Criterion;


// use bit_set::BitSet;

use std::collections::HashMap;
use fnv::FnvHashMap;
use std::hash::{Hasher, BuildHasherDefault};

#[allow(dead_code)]
static K1K: u32 =   1000;
#[allow(dead_code)]
static K3K: u32 =   3000;
#[allow(dead_code)]
static K10K: u32 =  10000;
#[allow(dead_code)]
static K100K: u32 = 100000;
#[allow(dead_code)]
static K300K: u32 = 300000;
#[allow(dead_code)]
static K500K: u32 = 500000;
#[allow(dead_code)]
static K3MIO: u32 = 3000000;
#[allow(dead_code)]
static MIO: u32 =   1000000;


pub struct NaiveHasher(u64);
impl Default for NaiveHasher {
    fn default() -> Self {
        NaiveHasher(0)
    }
}
impl Hasher for NaiveHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unimplemented!()
    }
    // fn write_u64(&mut self, i: u64) {
    //     self.0 = i ^ i >> 7;
    // }
    fn write_u32(&mut self, i: u32) {
        self.0 = (i ^ i >> 3) as u64 ;
    }
}
type NaiveBuildHasher = BuildHasherDefault<NaiveHasher>;
pub type NaiveHashMap<K, V> = HashMap<K, V, NaiveBuildHasher>;
type Map = NaiveHashMap<u32, u32>;


pub fn bench_fnvhashmap_insert(num_entries: u32) -> FnvHashMap<u32, f32>{
    let mut hits:FnvHashMap<u32, f32> = FnvHashMap::default();
    hits.reserve(num_entries as usize);
    for x in 0..num_entries {
        hits.insert(x * 8, 0.22);
    }
    hits
}

pub fn bench_naivehashmap_insert(num_entries: u32) -> NaiveHashMap<u32, f32>{
    let mut hits:NaiveHashMap<u32, f32> = NaiveHashMap::default();
    hits.reserve(num_entries as usize);
    for x in 0..num_entries {
        hits.insert(x * 8, 0.22);
    }
    hits
}


pub fn bench_fnvhashmap_insert_with_lookup(num_hits: u32, token_hits: u32){
    let mut hits:FnvHashMap<u32, f32> = bench_fnvhashmap_insert(num_hits);
    for x in num_hits..token_hits {
        let stat = hits.entry(x * 65 as u32).or_insert(0.0);
        *stat += 2.0;
    }
}

pub fn bench_naivehashmap_insert_with_lookup(num_hits: u32, token_hits: u32){
    let mut hits:NaiveHashMap<u32, f32> = bench_naivehashmap_insert(num_hits);
    for x in num_hits..token_hits {
        let stat = hits.entry(x * 65 as u32).or_insert(0.0);
        *stat += 2.0;
    }
}

pub fn bench_vec_insert(num_entries: u32) -> Vec<(u32, f32)>{
    let mut hits:Vec<(u32, f32)> = vec![];
    hits.reserve(num_entries as usize);
    for x in 0..num_entries {
        hits.push((x * 8, 0.22));
    }
    hits
}

pub fn bench_vec_insert_with_lookup(num_hits: u32, token_hits: u32) -> Vec<(u32, f32)> {
    let mut hits:Vec<(u32, f32)> = bench_vec_insert(num_hits);
    hits.sort_by(|a, b| a.0.cmp(&b.0));
    for x in num_hits..token_hits {

        let res = hits.binary_search_by(|el| el.0.cmp(&x));
        match res {
            Ok(pos) => hits[pos].1 += 2.0,
            Err(pos) => hits.insert(pos, (x, 0.0))
        }

        // let stat = hits.entry(x * 65 as u32).or_insert(0.0);
        // *stat += 2.0;
    }
    hits
}
use itertools::Itertools;

pub fn bench_vec_insert_with_lookup_collect_in_2_vec(num_hits: u32, token_hits: u32) -> Vec<(u32, f32)> {
    let mut hits:Vec<(u32, f32)> = bench_vec_insert(num_hits);
    hits.reserve(token_hits as usize);
    for x in num_hits..token_hits {
        hits.push((x * 8, 0.25));
        // let stat = hits.entry(x * 65 as u32).or_insert(0.0);
        // *stat += 2.0;
    }
    hits.sort_by(|a, b| a.0.cmp(&b.0));

    let mut hits_3:Vec<(u32, f32)> = vec![];
    hits_3.reserve(hits.len());

    for (key, mut group) in &hits.into_iter().group_by(|elt| elt.0) {
        // Check that the sum of each group is +/- 4.
        // assert_eq!(4, group.sum::<i32>().abs());
        hits_3.push((key, group.next().unwrap().1));
    }
    hits_3
}


fn criterion_benchmark(c: &mut Criterion) {
    Criterion::default()
        .bench_function("bench_vec_insert_with_lookup 3Mio", |b| b.iter(|| bench_vec_insert_with_lookup_collect_in_2_vec(K3MIO, K3MIO)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);


// #[cfg(test)]
// mod bench_collection {

// use test::Bencher;
// use super::*;

//     // #[bench]
//     // fn bench_fnvhashmap_insert_with_lookup_100k(b: &mut Bencher) {
//     //     b.iter(|| bench_fnvhashmap_insert_with_lookup(K100K, K100K));
//     // }

//     // #[bench]
//     // fn bench_naivehashmap_insert_with_lookup_100k(b: &mut Bencher) {
//     //     b.iter(|| bench_naivehashmap_insert_with_lookup(K100K, K100K));
//     // }

//     // #[bench]
//     // fn bench_fnvhashmap_insert_100k(b: &mut Bencher) {
//     //     b.iter(|| bench_fnvhashmap_insert(K100K));
//     // }

//     // #[bench]
//     // fn bench_naivehashmap_insert_100k(b: &mut Bencher) {
//     //     b.iter(|| bench_naivehashmap_insert(K100K));
//     // }



//     // #[bench]
//     // fn bench_fnvhashmap_insert_with_lookup_10k(b: &mut Bencher) {
//     //     b.iter(|| bench_fnvhashmap_insert_with_lookup(K10K, K10K));
//     // }

//     // #[bench]
//     // fn bench_naivehashmap_insert_with_lookup_10k(b: &mut Bencher) {
//     //     b.iter(|| bench_naivehashmap_insert_with_lookup(K10K, K10K));
//     // }

//     // #[bench]
//     // fn bench_fnvhashmap_insert_10k(b: &mut Bencher) {
//     //     b.iter(|| bench_fnvhashmap_insert(K10K));
//     // }

//     // #[bench]
//     // fn bench_naivehashmap_insert_10k(b: &mut Bencher) {
//     //     b.iter(|| bench_naivehashmap_insert(K10K));
//     // }



//     // #[bench]
//     // fn bench_fnvhashmap_insert_with_lookup_3mio(b: &mut Bencher) {
//     //     b.iter(|| bench_fnvhashmap_insert_with_lookup(K3MIO, K3MIO));
//     // }

//     #[bench]
//     fn bench_naivehashmap_insert_with_lookup_3mio(b: &mut Bencher) {
//         b.iter(|| bench_naivehashmap_insert_with_lookup(K3MIO, K3MIO));
//     }

//     // #[bench]
//     // fn bench_fnvhashmap_insert_3mio(b: &mut Bencher) {
//     //     b.iter(|| bench_fnvhashmap_insert(K3MIO));
//     // }

//     #[bench]
//     fn bench_naivehashmap_insert_3mio(b: &mut Bencher) {
//         b.iter(|| bench_naivehashmap_insert(K3MIO));
//     }

//     // #[bench]
//     // fn bench_vec_insert_3mio(b: &mut Bencher) {
//     //     b.iter(|| bench_vec_insert(K300K));
//     // }

//     // #[bench]
//     // fn bench_vec_insert_with_lookup_3mio(b: &mut Bencher) {
//     //     b.iter(|| bench_vec_insert_with_lookup(K300K, K3MIO));
//     // }

//     #[bench]
//     fn bench_vec_insert_3mio(b: &mut Bencher) {
//         b.iter(|| bench_vec_insert(K300K));
//     }

//     #[bench]
//     fn bench_vec_insert_with_lookup_collect_in_2_vec_3mio(b: &mut Bencher) {
//         b.iter(|| bench_vec_insert_with_lookup_collect_in_2_vec(K3MIO, K3MIO));
//     }



// }