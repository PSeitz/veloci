use search;
use search::*;
use query_generator;
use persistence::*;
use persistence;
use rayon::prelude::*;

pub struct Shards {
    persistences: Vec<(u32, Persistence)>
}


impl Shards {

    pub fn load(path: String, range:u32) -> Result<(Shards), search::SearchError>{
        let mut persistences = vec![];
        for shard_num in 0..range{
            let database = path.to_string()+"_"+&shard_num.to_string();
            persistences.push((shard_num, persistence::Persistence::load(database)?));
        }
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

        // let r: Result<Vec<_>, search::SearchError> = self.persistences.par_iter().map(|(_num, persistence)| {
        //     let request = query_generator::search_query(&persistence, q_params.clone());
        //     let hits = search::search(request, persistence)?;
        //     Ok(search::to_search_result(&persistence, hits, select.clone()))
        // }).collect();

        // for result in r?{
        //     all_search_results.merge(&result);
        // }

        for (num, persistence) in self.persistences.iter() {
            let mut request = query_generator::search_query(&persistence, q_params.clone());
            // let select = request.select.clone();
            let hits = search::search(request, persistence)?;
            // all_results.push((*num, hits));
            let result = search::to_search_result(&persistence, hits, select.clone());
            all_search_results.merge(&result);
        }

        all_search_results.data = apply_top_skip(&all_search_results.data, Some(0), Some(10));

        Ok(all_search_results)
    }
}



// fn search_in_persistence(persistence: &Persistence, request: search_lib::search::Request, _enable_flame: bool) -> Result<SearchResult, search::SearchError> {
//     // info!("Searching ... ");
//     let select = request.select.clone();
//     let hits = {
//         info_time!("Searching ... ");
//         search::search(request, &persistence)?
//     };
//     info!("Loading Documents... ");
//     let doc = {
//         info_time!("Loading Documents...  ");
//         let result =(search::to_search_result(&persistence, hits, select))
//     };
//     debug!("Returning ... ");
//     Ok(doc)
// }