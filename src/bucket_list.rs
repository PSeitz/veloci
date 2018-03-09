// use bit_set::BitSet;

// use std::collections::HashMap;
// use fnv::FnvHashMap;

// pub fn bench_fnvhashmap_insert(num_hits: u32, token_hits: u32){
//     let mut hits:FnvHashMap<u32, f32> = FnvHashMap::default();
//     for x in 0..num_hits {
//         hits.insert(x * 8, 0.22);
//     }
//     for x in num_hits..token_hits {
//         let stat = hits.entry(x * 65 as u32).or_insert(0.0);
//         *stat += 2.0;
//     }
// }

// // pub fn bench_hashmap_insert(num_hits: u32, token_hits: u32){
// //     let mut hits:HashMap<u32, f32> = HashMap::default();
// //     for x in 0..num_hits {
// //         hits.insert(x * 8, 0.22);
// //     }
// //     for x in num_hits..token_hits {
// //         let stat = hits.entry(x * 65 as u32).or_insert(0.0);
// //         *stat += 2.0;
// //     }
// // }

// // pub fn bench_fnvhashmap_extend(num_hits: u32, token_hits: u32){
// //     let mut hits:FnvHashMap<u32, f32> = FnvHashMap::default();
// //     for x in 0..num_hits {
// //         hits.insert(x * 8, 0.22);
// //     }
// //     let mut hits2:FnvHashMap<u32, f32> = FnvHashMap::default();
// //     for x in num_hits..token_hits {
// //         hits2.insert(x * 65, 0.22);
// //     }
// //     hits.extend(hits2);
// // }

// pub fn bench_vc_scoreonly_insert(num_hits: u32, token_hits: u32){

//     let mut scores:Vec<f32> = Vec::new();
//     scores.resize(50, 0.0);
//     for x in 0..num_hits {
//         let val_id = x * 8 as u32;
//         if val_id >= scores.len() as u32 {
//             scores.resize((val_id as f32 * 1.5) as usize, 0.0);
//         }
//         scores[val_id  as usize] = 0.22;
//     }
//     for x in num_hits..token_hits {
//         let val_id = x * 65 as u32;
//         if val_id >= scores.len() as u32 {
//             scores.resize((val_id as f32 * 1.5) as usize, 0.0);
//         }
//         scores[val_id as usize] += 2.0;
//     }
// }

// pub fn bench_bucketed_insert(num_hits: u32, token_hits: u32){

//     let mut scores = BucketedScoreList::default();
//     for x in 0..num_hits {
//         scores.insert((x * 8) as u64, 0.22);
//     }
//     for x in num_hits..token_hits {
//         let val_id = x * 65;
//         let yop = scores.get(val_id as u64).unwrap_or(0.0) + 2.0;
//         scores.insert(val_id as u64, yop);
//     }
// }

// pub fn bench_bucketed_insert_single_big_value(){
//     let mut scores = BucketedScoreList::default();
//     scores.insert((120_000_000) as u64, 0.22);
// }

// // pub fn bench_bit_vec_insert(){
// //     let mut hits = BitSet::new();
// //     let mut scores:Vec<f32> = Vec::new();
// //     for x in 0..100000 {
// //         hits.insert(x * 8);
// //         scores.push(0.22);
// //     }
// //     for x in 0..100000 {
// //         hits.binary_search(&(x*12 as u32));

// //         let res = match hits.binary_search(&(x*12 as u32)) {
// //             Ok(value) => { Some(scores[value]) },
// //             Err(_) => {None},
// //         };

// //     }
// // }

// use std::cmp;
// const BUCKET_SIZE:u32 = 131072;

// #[derive(Debug, Clone)]
// pub struct BucketedScoreList {
//     arr: Vec<Vec<f32>>,
//     hits: Vec<BitSet>
// }
// use std::f32;
// impl BucketedScoreList {
//     pub fn default() -> Self {
//         BucketedScoreList{arr:vec![], hits:vec![]}
//     }
//     fn split_index(index: u64) -> (usize, usize) {
//         let pos =     (index & 0b0000000000000000000000000000000000000000000000011111111111111111) as usize;
//         let bucket = ((index & 0b1111111111111111111111111111111111111111111111100000000000000000) / BUCKET_SIZE as u64) as usize;
//         (pos, bucket)
//     }
//     pub fn insert(& mut self, index: u64, value:f32) {
//         // let pos =      (index & 0b00000000000000000001111111111111) as usize;
//         // let bucket = ((index & 0b11111111111111111110000000000000) / 8192) as usize;
//         // let pos =      (index & 0b00000000000000000111111111111111) as usize;
//         // let bucket = ((index & 0b11111111111111111000000000000000) / 32768) as usize;

//         let (pos, bucket) = BucketedScoreList::split_index(index);
//         if pos > index as usize { panic!("pos > index {:?}", index);}
//         if self.arr.len() <= bucket {
//             self.arr.resize(bucket + 1, vec![]);
//             self.hits.resize(bucket + 1, BitSet::new());
//         }
//         if self.arr[bucket].len() <= pos { // @Hack  Fix Iterator
//             self.arr[bucket].resize(cmp::min(((pos + 1) as f32 * 1.5) as usize, BUCKET_SIZE as usize + 2) , f32::NEG_INFINITY); // @Hack  +2
//         }
//         self.arr[bucket][pos] = value;
//         // if value == f32::NEG_INFINITY {
//         //     self.hits[bucket].remove(pos);
//         // }else{
//         //     self.hits[bucket].insert(pos);
//         // }
//     }

//     pub fn get(&self, index: u64) -> Option<f32> {
//         let (pos, bucket) = BucketedScoreList::split_index(index);
//         if self.arr.len() <= bucket {
//             None
//         }else{
//             self.arr[bucket].get(pos).map(|el| *el)
//         }
//     }

//     pub fn contains_key(&self, index: u64) -> bool {
//         let (pos, bucket) = BucketedScoreList::split_index(index);
//         if self.arr.len() <= bucket {
//             false
//         }else{
//             self.arr[bucket].get(pos).map_or(false, |el|*el != f32::NEG_INFINITY)
//         }
//     }

//     // fn merge_bucket(source_bucket: &Vec<f32>, target_bucket: &mut Vec<f32>) {
//     // }

//     pub fn extend(&mut self, other:&BucketedScoreList){
//         for el in other.iter() {
//             self.insert(el.0 as u64, el.1);
//         }
//     }

//     pub fn into_iter(self) -> BucketedScoreListIntoIterator {
//         BucketedScoreListIntoIterator { list: self, bucket: 0, pos: 0 }
//     }

//     pub fn iter<'a>(& 'a self) -> BucketedScoreListIterator<'a> {
//         BucketedScoreListIterator { list: &self, bucket: 0, pos: 0 }
//     }

//     pub fn retain<F>(&mut self, mut fun: F)
//         where F: FnMut(u32, f32) -> bool {
//             let mut to_be_removed = vec![];
//             for el in self.iter() {
//                 if !fun(el.0, el.1){
//                     to_be_removed.push(el.0);
//                     // self.insert(el.0 as u64, f32::NEG_INFINITY);
//                 }
//             }
//             for el in to_be_removed {
//                 self.insert(el as u64, f32::NEG_INFINITY);
//             }
//     }

// //     fn get_text_lines<F>(folder:&str, path: &str, exact_search:Option<String>, character: Option<&str>, mut fun: F) -> Result<(), SearchError>
// // where F: FnMut(&str, u32) {

// }

// #[derive(Debug, Clone)]
// pub struct BucketedScoreListIntoIterator {
//     list: BucketedScoreList,
//     bucket: usize,
//     pos: usize
// }

// #[derive(Debug, Clone)]
// pub struct BucketedScoreListIterator<'a>  {
//     list: & 'a BucketedScoreList,
//     bucket: usize,
//     pos: usize
// }

// // macro_rules! moveToNextBucket {
// //     ($pos:ident, $bucket:ident, $list:ident, $current_bucket:ident, $current_Iter:ident) => (
// //         *$pos=0;
// //         *$bucket+=1;
// //         if *$bucket >= $list.arr.len() { return None; }
// //         $current_bucket = &$list.arr[*$bucket];
// //         $current_Iter = $list.hits[*$bucket].iter().skip(0);
// //     )
// // }

// // fn move_in_bucket<'a>(list: &'a BucketedScoreList, bucket: &mut usize, pos:&mut usize) -> Option<(u32, f32)> {
// //     let mut current_bucket = &list.arr[*bucket];
// //     let mut current_Iter = list.hits[*bucket].iter().skip(*pos);

// //     let mut nextHit = current_Iter.next();
// //     while nextHit.is_none() {
// //         moveToNextBucket!(pos, bucket, list, current_bucket, current_Iter);
// //         nextHit = current_Iter.next();
// //     }
// //     *pos = nextHit.unwrap();

// //     // while current_bucket[*pos] == f32::NEG_INFINITY {
// //     //     *pos+=1;
// //     //     if current_bucket.get(*pos).is_none() {
// //     //         moveToNextBucket!(pos, bucket, list, current_bucket);
// //     //         while current_bucket.len() == 0 { // find next filled bucket
// //     //             moveToNextBucket!(pos, bucket, list, current_bucket);
// //     //         }
// //     //     }
// //     // }
// //     let next_val = Some(((*bucket*BUCKET_SIZE as usize+*pos) as u32, current_bucket[*pos]));
// //     *pos+=1;
// //     return next_val
// // }

// // fn next_el_in_bucketed<'a>(list: &'a BucketedScoreList, bucket: &mut usize, pos:&mut usize) -> Option<(u32, f32)> {
// //     if *bucket >= list.arr.len() { return None; }
// //     let mut current_bucket = &list.arr[*bucket];
// //     let mut current_Iter = list.hits[*bucket].iter().skip(*pos);
// //     if current_bucket.len() != 0 && *pos < current_bucket.len() {
// //         return move_in_bucket(list, bucket, pos);
// //     }else{
// //         moveToNextBucket!(pos, bucket, list, current_bucket, current_Iter);
// //         while current_bucket.len() == 0 {
// //             moveToNextBucket!(pos, bucket, list, current_bucket, current_Iter);
// //         }
// //         return move_in_bucket(list, bucket, pos);
// //     }
// // }

// macro_rules! moveToNextBucket {
//     ($pos:ident, $bucket:ident, $list:ident, $current_bucket:ident) => (
//         *$pos=0;
//         *$bucket+=1;
//         if *$bucket >= $list.arr.len() { return None; }
//         $current_bucket = &$list.arr[*$bucket];
//     )
// }

// fn move_in_bucket<'a>(list: &'a BucketedScoreList, bucket: &mut usize, pos:&mut usize) -> Option<(u32, f32)> {
//     let mut current_bucket = &list.arr[*bucket];
//     // let mut current_bitvecIter = &list.arr[*bucket].iter();
//     while current_bucket[*pos] == f32::NEG_INFINITY {
//         *pos+=1;
//         if current_bucket.get(*pos).is_none() {
//             moveToNextBucket!(pos, bucket, list, current_bucket);
//             while current_bucket.len() == 0 { // find next filled bucket
//                 moveToNextBucket!(pos, bucket, list, current_bucket);
//             }
//         }
//     }
//     let next_val = Some(((*bucket*BUCKET_SIZE as usize+*pos) as u32, current_bucket[*pos]));
//     *pos+=1;
//     return next_val
// }

// fn next_el_in_bucketed<'a>(list: &'a BucketedScoreList, bucket: &mut usize, pos:&mut usize) -> Option<(u32, f32)> {
//     if *bucket >= list.arr.len() { return None; }
//     let mut current_bucket = &list.arr[*bucket];
//     if current_bucket.len() != 0 && *pos < current_bucket.len() {
//         return move_in_bucket(list, bucket, pos);
//     }else{
//         moveToNextBucket!(pos, bucket, list, current_bucket);
//         while current_bucket.len() == 0 {
//             moveToNextBucket!(pos, bucket, list, current_bucket);
//         }
//         return move_in_bucket(list, bucket, pos);
//     }
// }

// impl<'a> Iterator for BucketedScoreListIterator<'a> {
//     type Item = (u32, f32);
//     fn next(&mut self) -> Option<(u32, f32)> {
//         next_el_in_bucketed(self.list, &mut self.bucket, &mut self.pos)
//     }
// }

// impl Iterator for BucketedScoreListIntoIterator {
//     type Item = (u32, f32);

//     fn next(&mut self) -> Option<(u32, f32)> {
//         // next_el_in_bucketed(&self.list, &mut self.bucket, &mut self.pos)
//         None
//     }
// }

// #[test]
// fn test_bucketed_score_list() {

//     let mut scores = BucketedScoreList::default();
//     scores.insert(0, 0.22);
//     scores.insert(5, 0.22);
//     scores.insert(131071, 0.22);
//     scores.insert(131072, 0.22);
//     scores.insert(131073, 0.22);
//     scores.insert(250_000, 0.22);
//     scores.insert(1_000_000, 0.22);
//     {
//         let mut yop = scores.iter();
//         assert_eq!(yop.next(), Some((0, 0.22)));
//         assert_eq!(yop.next(), Some((5, 0.22)));
//         assert_eq!(yop.next(), Some((131071, 0.22)));
//         assert_eq!(yop.next(), Some((131072, 0.22)));
//         assert_eq!(yop.next(), Some((131073, 0.22)));
//         assert_eq!(yop.next(), Some((250_000, 0.22)));
//         assert_eq!(yop.next(), Some((1_000_000, 0.22)));
//         assert_eq!(yop.next(), None);
//         assert_eq!(yop.next(), None);

//         let mut extended = BucketedScoreList::default();
//         extended.extend(&scores);
//         let mut yop = extended.iter();
//         assert_eq!(yop.next(), Some((0, 0.22)));
//         assert_eq!(yop.next(), Some((5, 0.22)));
//         assert_eq!(yop.next(), Some((131071, 0.22)));
//         assert_eq!(yop.next(), Some((131072, 0.22)));
//         assert_eq!(yop.next(), Some((131073, 0.22)));
//         assert_eq!(yop.next(), Some((250_000, 0.22)));
//         assert_eq!(yop.next(), Some((1_000_000, 0.22)));
//         assert_eq!(yop.next(), None);
//         assert_eq!(yop.next(), None);
//     }

//     assert_eq!(scores.contains_key(1_000_000), true);

//     scores.retain(|key, _| key != 1_000_000);
//     assert_eq!(scores.contains_key(1_000_000), false);

//     // let ja = asdf.next();
//     // ja.next();
//     // let jaja = vec![];
//     // jaja.iter().next();

// }

// #[test]
// fn test_bucketed_score_list_insert_8() {

//     let mut scores = BucketedScoreList::default();
//     scores.insert(6, 0.22);
//     scores.insert(8, 0.22);
//     scores.insert(7, 0.22);
//     scores.insert(9, 0.22);
//     let mut yop = scores.iter();
//     assert_eq!(yop.next(), Some((6, 0.22)));
//     assert_eq!(yop.next(), Some((7, 0.22)));
//     assert_eq!(yop.next(), Some((8, 0.22)));
//     assert_eq!(yop.next(), Some((9, 0.22)));
//     assert_eq!(yop.next(), None);

// }

// #[test]
// fn test_bucketed_score_list_gap() {

//     let mut scores = BucketedScoreList::default();
//     scores.insert(1_000_000, 0.22);
//     let mut yop = scores.iter();
//     assert_eq!(yop.next(), Some((1_000_000, 0.22)));

// }

// // impl Extend<(u64, f32)> for BucketedScoreList {
// //     // add code here
// //     fn extend<T: IntoIterator<Item=(u64, f32)>>(&mut self, iter: T){
// //     }
// // }

// // pub fn quadratic_yes() {
// //     let mut one = HashSet::new();
// //     for i in 1..500000 {
// //         one.insert(i);
// //     }
// //     let mut two = HashSet::new();
// //     for v in one {
// //         two.insert(v);
// //     }
// // }

// pub fn quadratic_no(num_hits: u32) {
//     let mut one = HashMap::new();
//     for i in 1..num_hits {
//         one.insert(i, 0.5);
//     }
//     let mut two = HashMap::new();
//     two.extend(one);
// }

// // static  K100K = 100000;

// #[allow(dead_code)]
// static K1K: u32 =   1000;
// #[allow(dead_code)]
// static K3K: u32 =   3000;
// #[allow(dead_code)]
// static K10K: u32 =  10000;
// #[allow(dead_code)]
// static K100K: u32 = 100000;
// #[allow(dead_code)]
// static K300K: u32 = 300000;
// #[allow(dead_code)]
// static K500K: u32 = 500000;
// #[allow(dead_code)]
// static K3MIO: u32 = 3000000;
// #[allow(dead_code)]
// static MIO: u32 =   1000000;

// #[cfg(test)]
// mod testo {

// use test::Bencher;
// use super::*;

//     #[bench]
//     fn bench_fnvhashmap_insert_(b: &mut Bencher) {
//         b.iter(|| bench_fnvhashmap_insert(K100K, K100K));
//     }

//     // #[bench]
//     // fn bench_hashmap_insert_(b: &mut Bencher) {
//     //     b.iter(|| bench_hashmap_insert(K100K, K300K));
//     // }

//     // #[bench]
//     // fn bench_hashmap_extend_(b: &mut Bencher) {
//     //     b.iter(|| bench_fnvhashmap_extend(K100K, K100K));
//     // }

//     #[bench]
//     fn bench_bucketed_insert_single_big_value_(b: &mut Bencher) {
//         b.iter(|| bench_bucketed_insert_single_big_value());
//     }

//     #[bench]
//     fn bench_vec_scoreonly_insert_(b: &mut Bencher) {
//         b.iter(|| bench_vc_scoreonly_insert(K100K, K100K));
//     }

//     // #[bench]
//     // fn quadratic_yes_(b: &mut Bencher) {
//     //     b.iter(|| quadratic_yes());
//     // }

//     #[bench]
//     fn bench_bucketed_insert_(b: &mut Bencher) {
//         b.iter(|| bench_bucketed_insert(K100K, K100K));
//     }

//     #[bench]
//     fn bench_iterate_bit_set_(b: &mut Bencher) {
//         // It's a regular set
//         let mut s = BitSet::new();
//         s.insert(0);
//         s.insert(20001);
//         s.insert(20002);
//         s.insert(50001);
//         s.insert(50002);
//         s.insert(50003);
//         s.insert(50004);
//         s.insert(50005);
//         s.insert(121072);
//         s.insert(121072);
//         s.insert(121072);
//         b.iter(|| {
//             for el in s.iter() {
//                 info!("{:?}", el);
//             }
//         });
//     }

//     #[bench]
//     fn bench_iterate_bucketedlist_set_(b: &mut Bencher) {
//         // It's a regular set
//         let mut s = BucketedScoreList::default();
//         s.insert(0, 0.22);
//         s.insert(20001, 0.22);
//         s.insert(20002, 0.22);
//         s.insert(50001, 0.22);
//         s.insert(50002, 0.22);
//         s.insert(50003, 0.22);
//         s.insert(50004, 0.22);
//         s.insert(50005, 0.22);
//         s.insert(121072, 0.22);
//         s.insert(121072, 0.22);
//         s.insert(121072, 0.22);
//         b.iter(|| {
//             for el in s.iter() {
//                 info!("{:?}", el);
//             }
//         });
//     }

//     // #[bench]
//     // fn quadratic_noo_(b: &mut Bencher) {
//     //     b.iter(|| quadratic_no(K500K));
//     // }

// }
