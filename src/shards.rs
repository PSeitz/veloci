// use crate::{
//     create,
//     error::VelociError,
//     persistence::{self, *},
//     query_generator,
//     search::{self, *},
// };
// use itertools::Itertools;
// use rayon::prelude::*;
// use serde_json;
// use std::{self, cmp::Ordering};

// struct Shard {
//     shard_id: u64,
//     persistence: Persistence,
// }

// pub struct Shards {
//     path: String,
//     shards: Vec<Shard>,
//     current_id: atomic::AtomicUsize,
// }

// fn get_top_n_sort_from_iter<'a, F: Clone, T, I: Iterator<Item = &'a T>>(mut iter: I, top: usize, mut compare: F) -> Vec<(&'a T)>
// where
//     for<'r, 's> F: FnMut(&'r &T, &'s &T) -> Ordering,
// {
//     let mut num = 0;
//     let mut top_n: Vec<&T> = vec![];
//     while let Some(el) = iter.next() {
//         top_n.push(el);
//         num += 1;
//         if num == top {
//             break;
//         }
//     }

//     let mut current_worst = top_n.last().cloned().unwrap();

//     for el in iter {
//         if let Ordering::Greater = compare(&el, &current_worst) {
//             continue;
//         }

//         if !top_n.is_empty() && (top_n.len() % (top * 5)) == 0 {
//             top_n.sort_unstable_by(compare.clone());
//             top_n.truncate(top);
//             current_worst = top_n.last().unwrap();
//         }

//         top_n.push(el);
//     }

//     top_n.sort_unstable_by(compare.clone());
//     top_n.truncate(top);

//     top_n
// }

// #[test]
// fn test_top_n_sort() {
//     let dat = vec![
//         3, 5, 9, 10, 10, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
//     ];
//     let yops = get_top_n_sort_from_iter(dat.iter(), 2, |a, b| b.cmp(&a));
//     assert_eq!(yops, vec![&10, &10]);
//     let yops = get_top_n_sort_from_iter(dat.iter(), 2, |a, b| a.cmp(&b));
//     assert_eq!(yops, vec![&3, &5]);
// }

// struct ShardResult<'a> {
//     result: SearchResult,
//     shard: &'a Shard,
// }

// struct ShardResultHit<'a> {
//     shard: &'a Shard,
//     hit: Hit,
//     result: &'a SearchResult,
// }

// use std::io::BufRead;
// fn merge_persistences(persistences: &[&Persistence], mut target_persistence: &mut Persistence, indices: &str) -> Result<(), VelociError> {
//     let get_doc_json_stream = || {
//         persistences.iter().flat_map(|pers| {
//             std::io::BufReader::new(pers.get_file_handle("data").unwrap())
//                 .lines()
//                 .map(|line| serde_json::from_str(&line.unwrap()))
//         })
//     };
//     let get_doc_stream = || {
//         persistences
//             .iter()
//             .flat_map(|pers| std::io::BufReader::new(pers.get_file_handle("data").unwrap()).lines().map(|el| el.unwrap()))
//     };
//     create::create_indices_from_streams(&mut target_persistence, get_doc_json_stream(), get_doc_json_stream(), get_doc_stream(), indices, None, true)?;
//     Ok(())
// }

// impl Shards {
//     pub fn new(path: String) -> Self {
//         Shards {
//             shards: vec![],
//             path,
//             current_id: AtomicUsize::new(0),
//         }
//     }

//     pub fn insert(&mut self, docs: &str, indices: &str) -> Result<(), VelociError> {
//         // extend existing persistence or create new persistence and add to list
//         // info!("self.shards.len() {:?}", self.shards.len());
//         if self.shards.is_empty() {
//             self.add_new_shard_from_docs(docs, indices).unwrap();
//         } else {
//             let use_existing_shard_for_docs = false;
//             // {
//             //     let min_shard = self.shards.iter().min_by_key(|shard| shard.persistence.get_number_of_documents().unwrap()).unwrap();
//             //     use_existing_shard_for_docs = min_shard.persistence.get_number_of_documents().unwrap() < 100;
//             //     info!("{:?}", use_existing_shard_for_docs);
//             //     if use_existing_shard_for_docs {
//             //         let stream1_1 = Deserializer::from_reader(min_shard.persistence.get_file_handle("data").unwrap()).into_iter::<Value>();
//             //         let streams1 = stream1_1.chain(Deserializer::from_str(&docs).into_iter::<Value>());
//             //         let stream2_1 = Deserializer::from_reader(min_shard.persistence.get_file_handle("data").unwrap()).into_iter::<Value>();
//             //         let streams2 = stream2_1.chain(Deserializer::from_str(&docs).into_iter::<Value>());
//             //         let mut new_shard = self.get_new_shard().unwrap();
//             //         create::create_indices_from_streams(&mut new_shard.persistence, streams1, streams2, indices, None)?;
//             //     }
//             // }

//             if !use_existing_shard_for_docs {
//                 self.add_new_shard_from_docs(docs, indices).unwrap();
//             }
//         }

//         if self.shards.len() > 15 {
//             // info!("pre shards.len {:?}", self.shards.len());
//             let mut invalid_shards = vec![];
//             let mut new_shards = vec![];
//             {
//                 self.shards.sort_unstable_by_key(|shard| shard.persistence.get_bytes_indexed());
//                 // for (_, group) in &self.shards.iter().group_by(|shard| (shard.persistence.get_bytes_indexed() / 10_000_000)) {
//                 for (_, group) in &self.shards.iter().group_by(|shard| (shard.persistence.get_bytes_indexed() as f32).log10().round() as u32) {
//                     let shard_group: Vec<&Shard> = group.collect();
//                     if shard_group.len() == 1 {
//                         continue;
//                     }

//                     invalid_shards.extend(shard_group.iter().map(|shard| shard.shard_id));

//                     let mut new_shard = self.get_new_shard().unwrap();
//                     let persistences: Vec<&Persistence> = shard_group.iter().map(|shard| &shard.persistence).collect();
//                     merge_persistences(&persistences, &mut new_shard.persistence, indices)?;
//                     new_shards.push(new_shard);
//                 }
//             }

//             //TODO LOCK DURING SWITCH
//             self.shards.retain(|shard| {
//                 if invalid_shards.contains(&shard.shard_id) {
//                     // info!("deleting {:?}", &shard.persistence.db);
//                     fs::remove_dir_all(&shard.persistence.db).unwrap();
//                     false
//                 } else {
//                     true
//                 }
//             });

//             self.shards.extend(new_shards);
//             // info!("shards.len {:?}", self.shards.len());
//         }

//         Ok(())
//     }

//     fn add_new_shard_from_docs(&mut self, docs: &str, indices: &str) -> Result<(), VelociError> {
//         let mut new_shard = self.get_new_shard()?;
//         // info!("new shard {:?}", new_shard.persistence.db);
//         create::create_indices_from_str(&mut new_shard.persistence, docs, indices, None, true).unwrap();
//         self.shards.push(new_shard);
//         Ok(())
//     }

//     fn get_new_shard(&self) -> Result<(Shard), VelociError> {
//         let shard_id = self.current_id.fetch_add(1, atomic::Ordering::SeqCst);
//         let path = self.path.to_owned() + "/" + &shard_id.to_string();
//         let persistence = Persistence::create_type(path.to_string(), persistence::PersistenceType::Persistent)?;
//         Ok(Shard {
//             shard_id: shard_id as u64,
//             persistence,
//         })
//     }

//     pub fn search_all_shards_from_qp(&self, q_params: &query_generator::SearchQueryGeneratorParameters, select: &Option<Vec<String>>) -> Result<SearchResultWithDoc, VelociError> {
//         let mut all_search_results = SearchResultWithDoc::default();

//         let r: Vec<ShardResult<'_>> = self
//             .shards
//             .par_iter()
//             .map(|shard| {
//                 print_time!("search shard {:?}", shard.shard_id);
//                 let request = query_generator::search_query(&shard.persistence, q_params.clone()).unwrap();
//                 let result = search::search(request, &shard.persistence)?;
//                 Ok(ShardResult { shard: &shard, result })
//             })
//             .collect::<Result<Vec<ShardResult<'_>>, VelociError>>()?;

//         let total_num_hits: u64 = r.iter().map(|shard_result| shard_result.result.num_hits).sum();

//         let all_shard_results: Vec<_> = r
//             .iter()
//             .flat_map(|shard_result| {
//                 shard_result.result.data.iter().map(move |hit| ShardResultHit {
//                     shard: shard_result.shard,
//                     hit: hit.clone(),
//                     result: &shard_result.result,
//                 })
//             })
//             .collect();

//         let top_n_shard_results = get_top_n_sort_from_iter(all_shard_results.iter(), q_params.top.unwrap_or(10), |a, b| {
//             b.hit.score.partial_cmp(&a.hit.score).unwrap_or(Ordering::Equal)
//         });

//         let data: Vec<DocWithHit> = top_n_shard_results
//             .iter()
//             .map(|el| {
//                 let hits: Vec<Hit> = vec![el.hit.clone()];
//                 search::to_documents(&el.shard.persistence, &hits, &select, &el.result)[0].clone()
//             })
//             .collect();

//         all_search_results.num_hits = total_num_hits;
//         all_search_results.data = data;

//         Ok(all_search_results)
//     }

//     pub fn search_all_shards(&self, request: &search::Request) -> Result<SearchResultWithDoc, VelociError> {
//         let select = request.select.clone();
//         let mut all_results = SearchResultWithDoc::default();
//         for shard in &self.shards {
//             let hits = search::search(request.clone(), &shard.persistence)?;
//             let result = search::to_search_result(&shard.persistence, hits, &select);
//             all_results.merge(&result); //TODO merge with above
//         }
//         Ok(all_results)
//     }

//     pub fn load(path: String) -> Result<(Shards), VelociError> {
//         let mut shards = vec![];
//         let mut shard_id: u64 = 0;
//         for entry in fs::read_dir(path.to_string())? {
//             let entry = entry?;
//             let path = entry.path();
//             shards.push(Shard {
//                 shard_id,
//                 persistence: persistence::Persistence::load(path.to_str().unwrap())?,
//             });
//             shard_id += 1;
//         }
//         Ok(Shards {
//             shards,
//             path,
//             current_id: AtomicUsize::new(shard_id as usize),
//         })
//     }
// }
