use super::*;

pub use self::search_field_result::*;
use std::{self, f32, u32};

use fnv::{FnvHashMap};
use itertools::Itertools;

pub fn get_shortest_result<T: std::iter::ExactSizeIterator>(results: &[T]) -> usize {
    let mut shortest = (0, std::u64::MAX);
    for (index, res) in results.iter().enumerate() {
        if (res.len() as u64) < shortest.1 {
            shortest = (index, res.len() as u64);
        }
    }
    shortest.0
}

pub fn get_longest_result<T: std::iter::ExactSizeIterator>(results: &[T]) -> usize {
    let mut longest = (0, std::u64::MIN);
    for (index, res) in results.iter().enumerate() {
        if (res.len() as u64) > longest.1 {
            longest = (index, res.len() as u64);
        }
    }
    longest.0
}

fn merge_term_id_hits(results: &mut Vec<SearchFieldResult>) -> FnvHashMap<String, FnvHashMap<String, Vec<TermId>>> {
    //attr -> term -> hits
    let mut term_id_hits_in_field: FnvHashMap<String, FnvHashMap<String, Vec<TermId>>> = FnvHashMap::default();
    for el in results.iter_mut() {
        for (attr, mut v) in el.term_id_hits_in_field.drain() {
            // term_id_hits_in_field.insert(attr, v);
            let attr_term_hits = term_id_hits_in_field.entry(attr).or_default();
            for (term, hits) in v.drain() {
                attr_term_hits.insert(term, hits);
            }
        }
    }
    debug!("Fields: {}, term_id_hits_in_field {:?}", results.iter().map(|el| &el.request.path).join(" "), term_id_hits_in_field);
    term_id_hits_in_field
}
fn merge_term_id_texts(results: &mut Vec<SearchFieldResult>) -> FnvHashMap<String, Vec<String>> {
    //attr -> term_texts
    let mut term_text_in_field: FnvHashMap<String, Vec<String>> = FnvHashMap::default();
    for el in results.iter_mut() {
        for (attr, v) in el.term_text_in_field.drain() {
            let attr_term_hits = term_text_in_field.entry(attr).or_default();
            attr_term_hits.extend(v.iter().cloned());
        }
    }

    debug!("Fields: {}, term_text_in_field {:?}", results.iter().map(|el| &el.request.path).join(" "), term_text_in_field);
    term_text_in_field
}


// #[cfg(test)]
// mod bench_union_hits_score {
//     use super::*;
//     use crate::test;
//     #[bench]
//     fn bench_boost_intersect_hits_vec_multi(b: &mut test::Bencher) {
//         let req = RequestSearchPart{terms:vec!["a".to_string()], ..Default::default()};
//         let hits1 = SearchFieldResult {hits_scores:(0..4_000_00).map(|i| Hit::new(i * 5 as u32, 1.2 as f32)).collect(), request:req.clone(), ..Default::default()};
//         let hits2 = SearchFieldResult {hits_scores:(0..40_000).map(|i| Hit::new(i * 3 as u32, 4.2 as f32)).collect(), request:req.clone(), ..Default::default()};
//         let hits3 = SearchFieldResult {hits_scores:(0..120_000).map(|i| Hit::new(i * 13 as u32, 3.2 as f32)).collect(), request:req.clone(), ..Default::default()};
//         let hits4 = SearchFieldResult {hits_scores:(0..300_000).map(|i| Hit::new(i * 3 as u32, 2.9 as f32)).collect(), request:req.clone(), ..Default::default()};

//         // let results:Vec<_> = vec![hits1, hits2, hits3, hits4].into_iter().map(|hits_scores|SearchFieldResult {hits_scores, ..Default::default() }).collect();
//         let results:Vec<_> = vec![hits1, hits2, hits3, hits4];

//         b.iter(|| {
//             union_hits_score(results)
//         })
//     }
// }

pub fn union_hits_score(mut or_results: Vec<SearchFieldResult>) -> SearchFieldResult {
    // trace!("Union Input:\n{}", serde_json::to_string_pretty(&or_results).unwrap());

    if or_results.is_empty() {
        return SearchFieldResult { ..Default::default() };
    }
    if or_results.len() == 1 {
        let res = or_results.swap_remove(0);
        return res;
    }

    trace!("Union Input:");
    for el in &or_results {
        trace!("{}", el);
    }

    let term_id_hits_in_field = { merge_term_id_hits(&mut or_results) };
    let term_text_in_field = { merge_term_id_texts(&mut or_results) };

    let index_longest: usize = get_longest_result(&or_results.iter().map(|el| el.hits_scores.iter()).collect::<Vec<_>>());

    let longest_len = or_results[index_longest].hits_scores.len() as f32;
    let len_total: usize = or_results.iter().map(|el| el.hits_scores.len()).sum();
    let sum_other_len = len_total as f32 - longest_len;

    {
        debug_time!("union hits sort input");
        for res in &mut or_results {
            res.hits_scores.sort_unstable_by_key(|el| el.id);
            //TODO ALSO DEDUP??? - Results from field are deduped search_field.rs:551 dedup_by
        }
    }

    let explain = or_results[0].request.explain;

    let mut terms = or_results.iter().map(|res| res.request.terms[0].to_string()).collect::<Vec<_>>();
    terms.sort();
    terms.dedup();

    let mut fields = or_results.iter().map(|res| res.request.path.to_string()).collect::<Vec<_>>();
    fields.sort();
    fields.dedup();
    info!("or connect search terms {:?}", terms);

    let mut union_hits = Vec::with_capacity(longest_len as usize + sum_other_len as usize / 2);
    let mut explain_hits = FnvHashMap::default();
    {
        let iterators: Vec<_> = or_results
            .iter()
            .map(|res| {
                let term_id = terms.iter().position(|ref x| x == &&res.request.terms[0]).unwrap() as u8; //TODO This could be term ids for AND search results
                let field_id = fields.iter().position(|ref x| x == &&res.request.path).unwrap() as u8;
                // res.hits_scores.iter().map(move |el| (el.hit, f16::from_f32(el.score), term_id, field_id))

                res.iter(term_id, field_id)

                // res.hits_scores.iter().map(move |hit| MiniHit {
                //     id: hit.id,
                //     score: f16::from_f32(hit.score),
                //     term_id: term_id,
                //     field_id: field_id,
                // })
            })
            .collect();

        // let mergo = iterators.into_iter().kmerge_by(|a, b| a.1.id < b.1.id);
        let mergo = iterators.into_iter().kmerge_by(|a, b| a.id < b.id);

        // let mergo = kmerge_by::kmerge_by(iterators.into_iter(), |a, b| a.id < b.id);
        // let mergo = kmerge_by::kmerge_by(iterators.into_iter(), |a, b| a.id < b.id);

        debug_time!("union hits kmerge");

        let mut max_scores_per_term: Vec<f32> = vec![];
        max_scores_per_term.resize(terms.len(), 0.0);
        // let mut field_id_hits = 0;
        for (id, group) in &mergo.group_by(|el| el.id) {
            //reset scores to 0
            for el in &mut max_scores_per_term {
                *el = 0.;
            }
            for el in group {
                // max_scores_per_term[el.term_id as usize] = max_scores_per_term[el.term_id as usize].max(el.score.to_f32());
                max_scores_per_term[el.term_id as usize] = max_scores_per_term[el.term_id as usize].max(el.score);
            }

            // let num_distinct_terms = term_id_hits.count_ones() as f32;
            let num_distinct_terms = max_scores_per_term.iter().filter(|el| *el >= &0.00001).count() as f32;
            // sum_score = sum_score * num_distinct_terms * num_distinct_terms;

            let sum_over_distinct_with_distinct_term_boost = max_scores_per_term.iter().sum::<f32>() as f32 * num_distinct_terms * num_distinct_terms;
            debug_assert!(sum_over_distinct_with_distinct_term_boost != std::f32::NAN);
            debug_assert!(sum_over_distinct_with_distinct_term_boost != std::f32::INFINITY);
            union_hits.push(Hit::new(id, sum_over_distinct_with_distinct_term_boost));
            if explain {
                let explain = explain_hits.entry(id).or_insert_with(|| vec![]);
                // explain.push(format!("or sum_over_distinct_terms {:?}", max_scores_per_term.iter().sum::<f32>() as f32));
                explain.push(Explain::OrSumOverDistinctTerms(max_scores_per_term.iter().sum::<f32>() as f32));
                if num_distinct_terms > 1. {
                    // explain.push(format!("num_distinct_terms boost {:?} to {:?}", num_distinct_terms * num_distinct_terms, sum_over_distinct_with_distinct_term_boost));
                    // explain.push(Explain::NumDistinctTermsBoost{distinct_boost:num_distinct_terms * num_distinct_terms, new_score:sum_over_distinct_with_distinct_term_boost});
                }
            }
        }
    }

    if explain {
        for hit in union_hits.iter() {
            for res in or_results.iter() {
                if let Some(exp) = res.explain.get(&hit.id) {
                    let explain = explain_hits.entry(hit.id).or_insert_with(|| vec![]);
                    explain.extend_from_slice(exp);
                }
            }
        }
    }

    let res = SearchFieldResult {
        term_id_hits_in_field,
        term_text_in_field,
        hits_scores: union_hits,
        explain: explain_hits,
        request: or_results[0].request.clone(), // set this to transport fields like explain
        ..Default::default()
    };
    trace!("Union Output:\n{}", &res);
    res
}

pub fn union_hits_ids(mut or_results: Vec<SearchFieldResult>) -> SearchFieldResult {
    if or_results.is_empty() {
        return SearchFieldResult { ..Default::default() };
    }
    if or_results.len() == 1 {
        let res = or_results.swap_remove(0);
        return res;
    }

    let index_longest: usize = get_longest_result(&or_results.iter().map(|el| el.hits_ids.iter()).collect::<Vec<_>>());

    let longest_len = or_results[index_longest].hits_ids.len() as f32;
    let len_total: usize = or_results.iter().map(|el| el.hits_ids.len()).sum();
    let sum_other_len = len_total as f32 - longest_len;

    {
        debug_time!("filter union hits sort input");
        for res in &mut or_results {
            res.hits_ids.sort_unstable();
        }
    }

    let mut union_hits = Vec::with_capacity(longest_len as usize + sum_other_len as usize / 2);
    {
        let mergo = or_results.iter().map(|res| res.hits_ids.iter()).kmerge();
        debug_time!("filter union hits kmerge");
        for (id, mut _group) in &mergo.group_by(|el| *el) {
            union_hits.push(*id);
        }
    }

    SearchFieldResult {
        hits_ids: union_hits,
        request: or_results[0].request.clone(), // set this to transport fields like explain
        ..Default::default()
    }
}

#[test]
fn union_hits_vec_test() {
    let hits1 = vec![10, 0, 5]; // unsorted
    let hits2 = vec![0, 3, 10, 20];

    let res = union_hits_ids(vec![
        SearchFieldResult {
            hits_ids: hits1,
            ..Default::default()
        },
        SearchFieldResult {
            hits_ids: hits2,
            ..Default::default()
        },
    ]);
    assert_eq!(res.hits_ids, vec![0, 3, 5, 10, 20]);
}

// #[test]
// fn union_hits_vec_test() {
//     let hits1 = vec![Hit::new(10, 20.0), Hit::new(0, 10.0), Hit::new(5, 20.0)]; // unsorted
//     let hits2 = vec![Hit::new(0, 20.0), Hit::new(3, 20.0), Hit::new(10, 30.0), Hit::new(20, 30.0)];

//     let yop = vec![
//         SearchFieldResult {
//             request: RequestSearchPart {
//                 terms: vec!["a".to_string()],
//                 ..Default::default()
//             },
//             hits_scores: hits1,
//             ..Default::default()
//         },
//         SearchFieldResult {
//             request: RequestSearchPart {
//                 terms: vec!["b".to_string()],
//                 ..Default::default()
//             },
//             hits_scores: hits2,
//             ..Default::default()
//         },
//     ];

//     let res = union_hits_score(yop);

//     assert_eq!(
//         res.hits_scores,
//         // vec![Hit::new(0, 120.0), Hit::new(3, 20.0), Hit::new(5, 20.0), Hit::new(10, 200.0), Hit::new(20, 30.0)] //sum_score
//         vec![Hit::new(0, 80.0), Hit::new(3, 20.0), Hit::new(5, 20.0), Hit::new(10, 120.0), Hit::new(20, 30.0)] //max_score
//     );
// }

pub fn intersect_score_hits_with_ids(mut score_results: SearchFieldResult, mut id_hits: SearchFieldResult) -> SearchFieldResult {
    score_results.hits_scores.sort_unstable_by_key(|el| el.id);
    id_hits.hits_ids.sort_unstable();

    let mut id_iter = id_hits.hits_ids.iter();
    if let Some(first) = id_iter.next() {
        let mut current: u32 = *first;
        score_results.hits_scores.retain(|ref hit| {
            while current < hit.id {
                current = *id_iter.next().unwrap_or_else(|| &u32::MAX);
            }
            hit.id == current
        });
    }
    score_results
}

#[test]
fn test_intersect_score_hits_with_ids() {
    let hits1 = vec![Hit::new(10, 20.0), Hit::new(0, 20.0), Hit::new(5, 20.0)]; // unsorted
    let hits2 = vec![0, 10];

    let res = intersect_score_hits_with_ids(
        SearchFieldResult {
            hits_scores: hits1,
            ..Default::default()
        },
        SearchFieldResult {
            hits_ids: hits2,
            ..Default::default()
        },
    );

    assert_eq!(res.hits_scores, vec![Hit::new(0, 20.0), Hit::new(10, 20.0)]);
}

fn check_score_iter_for_id(iter_n_current: &mut (impl Iterator<Item = Hit>, Hit), current_id: u32) -> bool {
    if (iter_n_current.1).id == current_id {
        return true;
    }
    if (iter_n_current.1).id > current_id {
        return false;
    }
    let iter = &mut iter_n_current.0;
    for el in iter {
        let id = el.id;
        iter_n_current.1 = el;
        if id > current_id {
            return false;
        }
        if id == current_id {
            return true;
        }
    }
    false
}

pub fn intersect_hits_score(mut and_results: Vec<SearchFieldResult>) -> SearchFieldResult {
    if and_results.is_empty() {
        return SearchFieldResult { ..Default::default() };
    }
    if and_results.len() == 1 {
        let res = and_results.swap_remove(0);
        return res;
    }

    trace!("Intersect Input:");
    for el in &and_results {
        trace!("{}", el);
    }


    // trace!("Intersect Input:\n{}", serde_json::to_string_pretty(&and_results).unwrap());

    let should_explain = and_results[0].request.explain;
    let term_id_hits_in_field = { merge_term_id_hits(&mut and_results) };
    let term_text_in_field = { merge_term_id_texts(&mut and_results) };

    let index_shortest = get_shortest_result(&and_results.iter().map(|el| el.hits_scores.iter()).collect::<Vec<_>>());

    for res in &mut and_results {
        res.hits_scores.sort_unstable_by_key(|el| el.id); //TODO ALSO DEDUP???
    }
    let mut shortest_result = and_results.swap_remove(index_shortest).hits_scores;

    // let mut iterators = &and_results.iter().map(|el| el.hits_scores.iter()).collect::<Vec<_>>();

    let mut intersected_hits = Vec::with_capacity(shortest_result.len());
    {
        let mut iterators_and_current = and_results
            .iter_mut()
            .map(|el| {
                let mut iterator = el.hits_scores.iter().cloned();
                let current = iterator.next();
                (iterator, current)
            })
            .filter(|el| el.1.is_some())
            .map(|el| (el.0, el.1.unwrap()))
            .collect::<Vec<_>>();

        for current_el in &mut shortest_result {
            let current_id = current_el.id;
            let current_score = current_el.score;

            if iterators_and_current.iter_mut().all(|iter_n_current| check_score_iter_for_id(iter_n_current, current_id)) {
                let mut score = iterators_and_current.iter().map(|el| (el.1).score).sum();
                score += current_score; //TODO SCORE Max oder Sum FOR AND
                intersected_hits.push(Hit::new(current_id, score));
            }
        }
    }
    let mut explain_hits = FnvHashMap::default();
    if should_explain {
        for hit in intersected_hits.iter() {
            for res in and_results.iter() {
                if let Some(exp) = res.explain.get(&hit.id) {
                    let explain = explain_hits.entry(hit.id).or_insert_with(|| vec![]);
                    explain.extend_from_slice(exp);
                }
            }
        }
    }

    // all_results
    let res = SearchFieldResult {
        term_id_hits_in_field,
        term_text_in_field,
        explain: explain_hits,
        hits_scores: intersected_hits,
        request: and_results[0].request.clone(), // set this to transport fields like explain TODO FIX - ALL AND TERMS should be reflected
        ..Default::default()
    };

    trace!("Intersect Output:\n{}", &res);

    res
}

fn check_id_iter_for_id(iter_n_current: &mut (impl Iterator<Item = u32>, u32), current_id: u32) -> bool {
    if (iter_n_current.1) == current_id {
        return true;
    }
    if (iter_n_current.1) > current_id {
        return false;
    }
    let iter = &mut iter_n_current.0;
    for id in iter {
        iter_n_current.1 = id;
        if id > current_id {
            return false;
        }
        if id == current_id {
            return true;
        }
    }
    false
}

pub fn intersect_hits_ids(mut and_results: Vec<SearchFieldResult>) -> SearchFieldResult {
    if and_results.is_empty() {
        return SearchFieldResult { ..Default::default() };
    }
    if and_results.len() == 1 {
        let res = and_results.swap_remove(0);
        return res;
    }
    let index_shortest = get_shortest_result(&and_results.iter().map(|el| el.hits_ids.iter()).collect::<Vec<_>>());

    for res in &mut and_results {
        res.hits_ids.sort_unstable(); //TODO ALSO DEDUP???
    }
    let mut shortest_result = and_results.swap_remove(index_shortest).hits_ids;

    // let mut iterators = &and_results.iter().map(|el| el.hits_ids.iter()).collect::<Vec<_>>();

    let mut intersected_hits = Vec::with_capacity(shortest_result.len());
    {
        let mut iterators_and_current = and_results
            .iter_mut()
            .map(|el| {
                let mut iterator = el.hits_ids.iter().cloned();
                let current = iterator.next();
                (iterator, current)
            })
            .filter(|el| el.1.is_some())
            .map(|el| (el.0, el.1.unwrap()))
            .collect::<Vec<_>>();

        for current_id in &mut shortest_result {
            if iterators_and_current.iter_mut().all(|iter_n_current| check_id_iter_for_id(iter_n_current, *current_id)) {
                intersected_hits.push(*current_id);
            }
        }
    }
    // all_results
    SearchFieldResult {
        hits_ids: intersected_hits,
        ..Default::default()
    }
}

#[test]
fn intersect_hits_ids_test() {
    let hits1 = vec![10, 0, 5]; // unsorted
    let hits2 = vec![0, 3, 10, 20];

    let yop = vec![
        SearchFieldResult {
            hits_ids: hits1,
            ..Default::default()
        },
        SearchFieldResult {
            hits_ids: hits2,
            ..Default::default()
        },
    ];

    let res = intersect_hits_ids(yop);

    assert_eq!(res.hits_ids, vec![0, 10]);
}

#[test]
fn intersect_hits_scores_test() {
    let hits1 = vec![Hit::new(10, 20.0), Hit::new(0, 20.0), Hit::new(5, 20.0)]; // unsorted
    let hits2 = vec![Hit::new(0, 20.0), Hit::new(3, 20.0), Hit::new(10, 30.0), Hit::new(20, 30.0)];

    let yop = vec![
        SearchFieldResult {
            hits_scores: hits1,
            ..Default::default()
        },
        SearchFieldResult {
            hits_scores: hits2,
            ..Default::default()
        },
    ];

    let res = intersect_hits_score(yop);

    assert_eq!(res.hits_scores, vec![Hit::new(0, 40.0), Hit::new(10, 50.0)]);
}

#[test]
fn intersect_hits_scores_test_reg() {
    let hits1 = vec![Hit::new(704, 13.7),
        Hit::new(19921, 39.4),
        Hit::new(20000, 13.7),
        Hit::new(44650, 39.4)];

    let hits2 = vec![Hit::new(18779, 28.199999),
        Hit::new(20000, 14.400001),
        Hit::new(32606, 39.4),
        Hit::new(130721, 13.3),
        Hit::new(168854, 2.0666666)
    ];

    let yop = vec![
        SearchFieldResult {
            hits_scores: hits1,
            ..Default::default()
        },
        SearchFieldResult {
            hits_scores: hits2,
            ..Default::default()
        },
    ];

    let res = intersect_hits_score(yop);

    assert_eq!(res.hits_scores.len(), 1);
    assert_eq!(res.hits_scores[0].id, 20000);
}
