#![feature(test)]

extern crate test;

#[macro_use]
extern crate serde_json;

extern crate serde;
extern crate rand;
extern crate futures;
extern crate futures_cpupool;
extern crate tokio_timer;
extern crate regex;
extern crate fnv;



#[macro_use] extern crate lazy_static;

// extern crate rustc_serialize;
mod util;
mod search;


fn main() {
    
    println!("Hello, world!");

    search::main2();
}


#[test]
fn it_works() {
    assert_eq!(util::normalizeText("Hello"), "Hello");
    assert_eq!(util::normalizeText("(Hello)"), "Hello");
    assert_eq!(util::normalizeText("\"H,ell-;o"), "Hello");
    assert_eq!(util::normalizeText("Hello(f)"), "Hello");
    assert_eq!(util::normalizeText("Hello(2)"), "Hello");
}


use std::collections::HashMap;
use fnv::FnvHashMap;

extern crate bit_set;

pub fn bench_fnvhashmap_insert(num_hits: u32, token_hits: u32){
    let mut hits:FnvHashMap<u32, f32> = FnvHashMap::default();
    for x in 0..num_hits {
        hits.insert(x * 8, 0.22);
    }
    for x in 0..token_hits {
        let stat = hits.entry(x*12 as u32).or_insert(0.0);
        *stat += 2.0;
    }
}

pub fn bench_hashmap_insert(num_hits: u32, token_hits: u32){
    let mut hits:HashMap<u32, f32> = HashMap::default();
    for x in 0..num_hits {
        hits.insert(x * 8, 0.22);
    }
    for x in 0..token_hits {
        let stat = hits.entry(x*12 as u32).or_insert(0.0);
        *stat += 2.0;
    }
}

pub fn bench_vc_scoreonly_insert(num_hits: u32, token_hits: u32){

    let mut scores:Vec<f32> = Vec::new();
    scores.resize(50, 0.0);
    for x in 0..num_hits {
        let val_id = x * 80 as u32;
        if val_id >= scores.len() as u32 {
            scores.resize((val_id as f32 * 1.5) as usize, 0.0);
        }
        scores[val_id  as usize] = 0.22;
    }
    for x in 0..token_hits {
        if x >= scores.len() as u32 {
            scores.resize((x as f32 * 1.5) as usize, 0.0);
        }
        scores[x as usize] += 2.0;
    }
}

// pub fn bench_bit_vec_insert(){
//     let mut hits = BitSet::new();
//     let mut scores:Vec<f32> = Vec::new();
//     for x in 0..100000 {
//         hits.insert(x * 8);
//         scores.push(0.22);
//     }
//     for x in 0..100000 {
//         hits.binary_search(&(x*12 as u32));

//         let res = match hits.binary_search(&(x*12 as u32)) {
//             Ok(value) => { Some(scores[value]) },
//             Err(_) => {None},
//         };

//     }
// }

// static  K100K = 100000;



static K1K: u32 =   1000;
static K3K: u32 =  3000;
// static K10K: u32 =  10000;
// static K100K: u32 = 100000;
// static MIO: u32 =   1000000;

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;
    

    #[bench]
    fn bench_fnvhashmap_insert_(b: &mut Bencher) {
        b.iter(|| bench_fnvhashmap_insert(K1K, K3K));
    }

    #[bench]
    fn bench_hashmap_insert_(b: &mut Bencher) {
        b.iter(|| bench_hashmap_insert(K1K, K3K));
    }

    #[bench]
    fn bench_vec_scoreonly_insert_(b: &mut Bencher) {
        b.iter(|| bench_vc_scoreonly_insert(K1K, K3K));
    }


}