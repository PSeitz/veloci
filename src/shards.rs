use search;
use search::*;
use query_generator;
use persistence::*;
use persistence;
use rayon::prelude::*;
use std::cmp::Ordering;

pub struct Shards {
    persistences: Vec<(u32, Persistence)>
}

fn get_top_n_sort_from_iter<'a, F: Clone, T, I: Iterator<Item = &'a T>>(mut iter: I, top: usize, mut compare: F) -> Vec<(&'a T)>
where
   for<'r, 's> F: FnMut(&'r &T, &'s &T) -> Ordering
{
    let mut num = 0;
    let mut top_n: Vec<&T> = vec![];
    while let Some(el) = iter.next() {
        top_n.push(el);
        num+=1;
        if num == top{
            break;
        }
    }

    let mut current_worst = top_n.last().cloned().unwrap();

    for el in iter {
        match compare(&el, &current_worst) {
            Ordering::Greater =>{continue;},
            _ => {},
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
    let dat = vec![3, 5, 9, 10, 10, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9];
    let yops = get_top_n_sort_from_iter(dat.iter(), 2, |a, b| b.cmp(&a));
    assert_eq!(yops, vec![&10, &10]);
    let yops = get_top_n_sort_from_iter(dat.iter(), 2, |a, b| a.cmp(&b));
    assert_eq!(yops, vec![&3, &5]);

}
use std::fs;
impl Shards {

    pub fn load(path: String) -> Result<(Shards), search::SearchError>{
        let mut persistences = vec![];
        let mut shard_num = 0;
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            println!("{:?}", path);
            persistences.push((shard_num, persistence::Persistence::load(path.to_str().unwrap())?));
            shard_num +=1;
        }

        // let mut persistences = vec![];
        // for shard_num in 0..range{
        //     let database = path.to_string()+"_"+&shard_num.to_string();
        //     persistences.push((shard_num, persistence::Persistence::load(database)?));
        // }
        Ok(Shards{persistences})
    }

    pub fn search_all_shards(&self, request: search::Request) -> Result<SearchResultWithDoc, search::SearchError>{
        let select = request.select.clone();
        let mut all_results = SearchResultWithDoc::default();
        for (_num, persistence) in self.persistences.iter() {
            let hits = search::search(request.clone(), persistence)?;
            let result = search::to_search_result(&persistence, hits, select.clone());
            all_results.merge(&result);
        }
        Ok(all_results)
    }

    pub fn search_all_shards_from_qp(&self, q_params: &query_generator::SearchQueryGeneratorParameters, select: Option<Vec<String>>) -> Result<SearchResultWithDoc, search::SearchError>{
        let mut all_search_results = SearchResultWithDoc::default();
        // let mut all_results:Vec<(u32, SearchResult)> = vec![];
        // let mut all_results:Vec<(u32, Hit)> = vec![];
        // let mut all_results:Vec<(u32, Hit, &SearchResult)> = vec![];


        // let r: Result<Vec<_>, search::SearchError> = self.persistences.par_iter().map(|(num, persistence)| {
        //     print_time!("search shard");
        //     let request = query_generator::search_query(&persistence, q_params.clone());
        //     let hits = search::search(request, persistence)?;
        //     Ok(search::to_search_result(&persistence, hits, select.clone()))
        // }).collect();

        // for result in r?{
        //     all_search_results.merge(&result);
        // }

        let r: Vec<(u32, SearchResult)> = self.persistences.par_iter().map(|(num, persistence)| {
            print_time!(format!("search shard {:?}", num));
            let request = query_generator::search_query(&persistence, q_params.clone());
            let hits = search::search(request, persistence)?;
            // Ok(search::to_search_result(&persistence, hits, select.clone()))
            Ok((*num, hits))
        }).collect::<Result<Vec<(u32, SearchResult)>, search::SearchError>>()?;

        let mut all_results:Vec<_> = vec![];

        let total_num_hits:u64 = r.iter().map(|el| el.1.num_hits).sum();

        for result in r.iter(){
            all_results.extend(result.1.data.iter().map(|el| (result.0, el.clone(), &result.1)));
        }

        let top_hits = get_top_n_sort_from_iter(all_results.iter(), q_params.top.unwrap_or(10), |a, b| b.1.score.partial_cmp(&a.1.score).unwrap_or(Ordering::Equal));

        let data:Vec<DocWithHit> = top_hits.iter().map(|el|{
            let shard_num = el.0;
            let hit = &el.1;
            let searchresult = el.2;

            let hits:Vec<Hit> = vec![hit.clone()];
            search::to_documents(&self.persistences[shard_num as usize].1, &hits, select.clone(), &searchresult)[0].clone()
        }).collect();

        all_search_results.num_hits = total_num_hits;
        all_search_results.data = data;



        Ok(all_search_results)
    }
}

