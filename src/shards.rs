use persistence;
use persistence::*;
use query_generator;
use rayon::prelude::*;
use search;
use create;
use search::*;
use std::cmp::Ordering;
use std::ops::Range;
use std::fs;

struct Shard {
    shard_id: usize,
    doc_range: Range<usize>,
    persistence: Persistence
}

pub struct Shards {
    path: String,
    shards: Vec<Shard>,
}

fn get_top_n_sort_from_iter<'a, F: Clone, T, I: Iterator<Item = &'a T>>(mut iter: I, top: usize, mut compare: F) -> Vec<(&'a T)>
where
    for<'r, 's> F: FnMut(&'r &T, &'s &T) -> Ordering,
{
    let mut num = 0;
    let mut top_n: Vec<&T> = vec![];
    while let Some(el) = iter.next() {
        top_n.push(el);
        num += 1;
        if num == top {
            break;
        }
    }

    let mut current_worst = top_n.last().cloned().unwrap();

    for el in iter {
        match compare(&el, &current_worst) {
            Ordering::Greater => {
                continue;
            }
            _ => {}
        }

        if !top_n.is_empty() && (top_n.len() % (top * 5)) == 0 {
            top_n.sort_unstable_by(compare.clone());
            top_n.truncate(top);
            current_worst = top_n.last().unwrap();
        }

        top_n.push(el);
    }

    top_n.sort_unstable_by(compare.clone());
    top_n.truncate(top);

    top_n
}

#[test]
fn test_top_n_sort() {
    let dat = vec![
        3, 5, 9, 10, 10, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9
    ];
    let yops = get_top_n_sort_from_iter(dat.iter(), 2, |a, b| b.cmp(&a));
    assert_eq!(yops, vec![&10, &10]);
    let yops = get_top_n_sort_from_iter(dat.iter(), 2, |a, b| a.cmp(&b));
    assert_eq!(yops, vec![&3, &5]);
}


struct ShardResult<'a> {
    result: SearchResult,
    shard: &'a Shard,
}

struct ShardResultHit<'a> {
    shard: &'a Shard,
    hit: Hit,
    result: &'a SearchResult,
}

impl Shards {

    fn insert(&mut self, docs: String, indices: &str) -> Result<(), create::CreateError> {
        // extend existing persistence or create new persistence and add to list
        if self.shards.is_empty() {
            self.add_new_shard_from_docs(docs, indices);
        }else{
            let min_pers = self.shards.iter().min_by_key(|shard| shard.persistence.get_number_of_documents().unwrap()).unwrap();
            if min_pers.persistence.get_number_of_documents().unwrap() < 10_000{

            }else{

            }
        }

        // self.add_new_shard_from_docs(docs, indices);

        Ok(())
    }

    fn add_new_shard_from_docs(&mut self, docs: String, indices: &str) -> Result<(), search::SearchError> {
        let shard_id = self.shards.len();
        let path = (self.path.to_owned() + "/" + &shard_id.to_string());
        create::create_indices(&path, &docs, indices);
        self.shards.push(Shard{shard_id: shard_id, doc_range:Range{start:0, end:0}, persistence:persistence::Persistence::load(path)?});
        Ok(())
    }

    pub fn search_all_shards_from_qp(
        &self,
        q_params: &query_generator::SearchQueryGeneratorParameters,
        select: Option<Vec<String>>,
    ) -> Result<SearchResultWithDoc, search::SearchError> {
        let mut all_search_results = SearchResultWithDoc::default();

        // let r: Result<Vec<_>, search::SearchError> = self.shards.par_iter().map(|(num, persistence)| {
        //     print_time!("search shard");
        //     let request = query_generator::search_query(&persistence, q_params.clone());
        //     let hits = search::search(request, persistence)?;
        //     Ok(search::to_search_result(&persistence, hits, select.clone()))
        // }).collect();

        // for result in r?{
        //     all_search_results.merge(&result);
        // }

        let r: Vec<ShardResult> = self.shards
            .par_iter()
            .map(|shard| {
                print_time!(format!("search shard {:?}", shard.shard_id));
                let request = query_generator::search_query(&shard.persistence, q_params.clone());
                let result = search::search(request, &shard.persistence)?;
                Ok(ShardResult {
                    shard: &shard,
                    result: result,
                })
            })
            .collect::<Result<Vec<ShardResult>, search::SearchError>>()?;

        let total_num_hits: u64 = r.iter().map(|shard_result| shard_result.result.num_hits).sum();

        let all_shard_results: Vec<_> = r.iter()
            .flat_map(|shard_result| {
                shard_result.result.data.iter().map(move |hit| ShardResultHit {
                    shard: shard_result.shard,
                    hit: hit.clone(),
                    result: &shard_result.result,
                })
            })
            .collect();
        // for shard_result in r.iter() {
        //     all_shard_results.extend(shard_result.result.data.iter().map(|hit| ShardResultHit{shard_id:shard_result.shard_id, hit:hit.clone(), result:&shard_result.result}));
        // }

        let top_n_shard_results = get_top_n_sort_from_iter(all_shard_results.iter(), q_params.top.unwrap_or(10), |a, b| {
            b.hit.score.partial_cmp(&a.hit.score).unwrap_or(Ordering::Equal)
        });

        let data: Vec<DocWithHit> = top_n_shard_results
            .iter()
            .map(|el| {
                let hits: Vec<Hit> = vec![el.hit.clone()];
                search::to_documents(&el.shard.persistence, &hits, select.clone(), &el.result)[0].clone()
            })
            .collect();

        all_search_results.num_hits = total_num_hits;
        all_search_results.data = data;

        Ok(all_search_results)
    }

    pub fn search_all_shards(&self, request: search::Request) -> Result<SearchResultWithDoc, search::SearchError> {
        let select = request.select.clone();
        let mut all_results = SearchResultWithDoc::default();
        for shard in self.shards.iter() {
            let hits = search::search(request.clone(), &shard.persistence)?;
            let result = search::to_search_result(&shard.persistence, hits, select.clone());
            all_results.merge(&result);//TODO merge with above
        }
        Ok(all_results)
    }

    pub fn load(path: String) -> Result<(Shards), search::SearchError> {
        let mut shards = vec![];
        let mut shard_id = 0;
        for entry in fs::read_dir(path.to_string())? {
            let entry = entry?;
            let path = entry.path();
            println!("{:?}", path);
            shards.push(Shard{shard_id: shard_id, doc_range:Range{start:0, end:0}, persistence:persistence::Persistence::load(path.to_str().unwrap())?});
            shard_id += 1;
        }

        // let mut shards = vec![];
        // for shard_id in 0..range{
        //     let database = path.to_string()+"_"+&shard_id.to_string();
        //     shards.push((shard_id, persistence::Persistence::load(database)?));
        // }
        Ok(Shards { shards, path })
    }
}
