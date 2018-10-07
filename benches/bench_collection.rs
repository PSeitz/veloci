#![feature(entry_and_modify)]
#![feature(test)]
#[macro_use]
extern crate criterion;
extern crate fnv;
extern crate itertools;
extern crate test;
// extern crate trie;

// use criterion::Criterion;

// use bit_set::BitSet;
use fnv::FnvHashMap;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

// use trie::map;
// use trie::map::Map;

#[allow(dead_code)]
static K1K: u32 = 1000;
#[allow(dead_code)]
static K3K: u32 = 3000;
#[allow(dead_code)]
static K10K: u32 = 10000;
#[allow(dead_code)]
static K100K: u32 = 100000;
#[allow(dead_code)]
static K300K: u32 = 300000;
#[allow(dead_code)]
static K500K: u32 = 500000;
#[allow(dead_code)]
static K3MIO: u32 = 3000000;
#[allow(dead_code)]
static K2MIO: u32 = 2000000;
#[allow(dead_code)]
static MIO: u32 = 1000000;

pub struct NaiveHasher(u64);
impl Default for NaiveHasher {
    fn default() -> Self {
        NaiveHasher(0)
    }
}
impl Hasher for NaiveHasher {
    fn write_u64(&mut self, i: u64) {
        self.0 = i ^ i >> 7;
    }

    fn write(&mut self, _: &[u8]) {
        unimplemented!()
    }

    fn finish(&self) -> u64 {
        self.0
    }
    // fn write_u32(&mut self, i: u32) {
    //     self.0 = (i ^ i >> 3) as u64 ;
    // }
}
type NaiveBuildHasher = BuildHasherDefault<NaiveHasher>;
pub type NaiveHashMap<K, V> = HashMap<K, V, NaiveBuildHasher>;

pub fn bench_fnvhashmap_insert(num_entries: u32) -> FnvHashMap<u32, f32> {
    let mut hits: FnvHashMap<u32, f32> = FnvHashMap::default();
    hits.reserve(num_entries as usize);
    for x in 0..num_entries {
        hits.insert(pseudo_rand(x), 0.22);
    }
    hits
}

pub fn bench_naivehashmap_insert(num_entries: u32) -> NaiveHashMap<u64, f32> {
    let mut hits: NaiveHashMap<u64, f32> = NaiveHashMap::default();
    hits.reserve(num_entries as usize);
    for x in 0..num_entries {
        hits.insert(u64::from(pseudo_rand(x)), 0.22);
    }
    hits
}

// pub fn bench_triemap_insert(num_entries: u32) -> trie::Map<f32> {
//     let mut hits: trie::Map<f32> = trie::Map::default();
//     // hits.reserve(num_entries as usize);
//     for x in 0..num_entries {
//         hits.insert(pseudo_rand(x) as usize, 0.22);
//     }
//     hits
// }
// pub fn bench_triemap_insert_with_lookup(num_hits: u32, token_hits: u32) {
//     let mut hits: trie::Map<f32> = bench_triemap_insert(num_hits);
//     for x in 0..token_hits {
//         let stat = hits.entry(pseudo_rand(x) as usize).or_insert(0.0);
//         *stat += 2.0;
//     }
// }

pub fn bench_fnvhashmap_insert_with_lookup(num_hits: u32, token_hits: u32) {
    let mut hits: FnvHashMap<u32, f32> = bench_fnvhashmap_insert(num_hits);
    for x in 0..token_hits {
        let stat = hits.entry(pseudo_rand(x)).or_insert(0.0);
        *stat += 2.0;
    }
}

pub fn bench_naivehashmap_insert_with_lookup(num_hits: u32, token_hits: u32) {
    let mut hits: NaiveHashMap<u64, f32> = bench_naivehashmap_insert(num_hits);
    for x in 0..token_hits {
        let stat = hits.entry(u64::from(pseudo_rand(x))).or_insert(0.0);
        *stat += 2.0;
    }
}

#[inline(always)]
fn pseudo_rand(num: u32) -> u32 {
    num * (num % 8) as u32
}

pub fn bench_naivehashmap_insert_with_lookup_modify(num_hits: u32, token_hits: u32) {
    let mut hits: NaiveHashMap<u64, f32> = bench_naivehashmap_insert(num_hits);
    for x in 0..token_hits {
        hits.entry(u64::from(pseudo_rand(x))).and_modify(|e| *e += 2.0).or_insert(0.0);
    }
}

pub fn bench_vec_insert(num_entries: u32) -> Vec<(u32, f32)> {
    let mut hits: Vec<(u32, f32)> = vec![];
    hits.reserve(num_entries as usize);
    for x in 0..num_entries {
        hits.push((pseudo_rand(x), 0.22));
    }
    hits
}

use itertools::Itertools;

pub fn bench_vec_insert_with_group_by_in_2_vec(num_hits: u32, token_hits: u32) -> Vec<(u32, f32)> {
    let mut hits: Vec<(u32, f32)> = bench_vec_insert(num_hits);
    hits.reserve(token_hits as usize);
    for x in 0..token_hits {
        hits.push((pseudo_rand(x), 0.25));
        // let stat = hits.entry(x * 8 as u32).or_insert(0.0);
        // *stat += 2.0;
    }
    hits.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let mut hits_2: Vec<(u32, f32)> = vec![];
    hits_2.reserve(hits.len());

    for (key, mut group) in &hits.into_iter().group_by(|elt| elt.0) {
        hits_2.push((key, group.next().unwrap().1));
    }
    hits_2
}

// fn criterion_benchmark(c: &mut Criterion) {
//     Criterion::default()
//         .bench_function("bench_vec_insert_with_lookup 3Mio", |b| b.iter(|| bench_vec_insert_with_group_by_in_2_vec(K3MIO, K3MIO)));
// }

// criterion_group!(benches, criterion_benchmark);
// criterion_main!(benches);

#[cfg(test)]
mod bench_collection {

    use super::*;
    use super::test::Bencher;

    // #[bench]
    // fn bench_fnvhashmap_insert_with_lookup_100k(b: &mut Bencher) {
    //     b.iter(|| bench_fnvhashmap_insert_with_lookup(K100K, K100K));
    // }

    // #[bench]
    // fn bench_naivehashmap_insert_with_lookup_100k(b: &mut Bencher) {
    //     b.iter(|| bench_naivehashmap_insert_with_lookup(K100K, K100K));
    // }

    #[bench]
    fn bench_fnvhashmap_insert_300k(b: &mut Bencher) {
        b.iter(|| bench_fnvhashmap_insert(K300K));
    }

    // #[bench]
    // fn bench_naivehashmap_insert_100k(b: &mut Bencher) {
    //     b.iter(|| bench_naivehashmap_insert(K100K));
    // }

    // #[bench]
    // fn bench_fnvhashmap_insert_with_lookup_10k(b: &mut Bencher) {
    //     b.iter(|| bench_fnvhashmap_insert_with_lookup(K10K, K10K));
    // }

    // #[bench]
    // fn bench_naivehashmap_insert_with_lookup_10k(b: &mut Bencher) {
    //     b.iter(|| bench_naivehashmap_insert_with_lookup(K10K, K10K));
    // }

    // #[bench]
    // fn bench_fnvhashmap_insert_10k(b: &mut Bencher) {
    //     b.iter(|| bench_fnvhashmap_insert(K10K));
    // }

    // #[bench]
    // fn bench_naivehashmap_insert_10k(b: &mut Bencher) {
    //     b.iter(|| bench_naivehashmap_insert(K10K));
    // }

    #[bench]
    fn bench_fnvhashmap_insert_with_lookup_300k(b: &mut Bencher) {
        b.iter(|| bench_fnvhashmap_insert_with_lookup(K300K, K300K));
    }

    #[bench]
    fn bench_naivehashmap_insert_with_lookup_300k(b: &mut Bencher) {
        b.iter(|| bench_naivehashmap_insert_with_lookup(K300K, K2MIO));
    }
    // #[bench]
    // fn bench_naivehashmap_insert_with_lookup_300k_mod(b: &mut Bencher) {
    //     b.iter(|| bench_naivehashmap_insert_with_lookup_modify(K300K, K2MIO));
    // }

    #[bench]
    fn bench_naivehashmap_insert_300k(b: &mut Bencher) {
        b.iter(|| bench_naivehashmap_insert(K300K));
    }

    // #[bench]
    // fn bench_triemap_insert_with_lookup_300k(b: &mut Bencher) {
    //     b.iter(|| bench_triemap_insert_with_lookup(K300K, K300K));
    // }

    // #[bench]
    // fn bench_triemap_insert_300k(b: &mut Bencher) {
    //     b.iter(|| bench_triemap_insert_with_lookup(K300K, 0));
    // }

    // #[bench]
    // fn bench_vec_insert_300k(b: &mut Bencher) {
    //     b.iter(|| bench_vec_insert(K300K));
    // }

    // #[bench]
    // fn bench_vec_insert_with_lookup_300k(b: &mut Bencher) {
    //     b.iter(|| bench_vec_insert_with_lookup(K300K, K3MIO));
    // }

    #[bench]
    fn bench_vec_insert_300k(b: &mut Bencher) {
        b.iter(|| bench_vec_insert(K300K));
    }

    #[bench]
    fn bench_vec_insert_with_lookup_collect_in_2_vec_300k(b: &mut Bencher) {
        b.iter(|| bench_vec_insert_with_group_by_in_2_vec(K300K, K300K));
    }

    // #[bench]
    // fn btree_map(b: &mut test::Bencher) {
    //     let mut hits:NaiveHashMap<u64, f32> = NaiveHashMap::default();
    //     map_bench(&mut hits, b, K300K, K300K);
    // }

}
