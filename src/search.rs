pub(crate) mod boost;
pub mod read_document;
pub mod request;
pub mod result;
pub mod search_field;
mod set_op;
pub mod sort;
pub mod stopwords;
pub mod why_found;

pub(crate) use self::boost::*;
pub use self::{result::*, search_field::*, set_op::*};
use self::{sort::top_n_sort, why_found::get_why_found};
pub use crate::search::{read_document::read_data, read_document::read_tree, request::*};
use crate::{
    error::VelociError,
    expression::ScoreExpression,
    facet,
    highlight_field::highlight_on_original_document,
    persistence::{Persistence, *},
    plan_creator::{execution_plan::*, plan::*},
    util::{self, *},
};
use doc_store::DocLoader;
use fnv::{FnvHashMap, FnvHashSet};
use rayon::prelude::*;

use std::{
    self,
    cmp::{self, Ordering},
    f32, mem, str, u32,
};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
enum TextLocalitySetting {
    Enabled,
    #[default]
    Disabled,
    Fields(Vec<String>),
}

pub fn skip_false(val: &bool) -> bool {
    !*val
}

fn default_top() -> Option<usize> {
    Some(10)
}
fn default_skip() -> Option<usize> {
    None
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Hit {
    pub id: u32,
    pub score: f32,
}

impl Hit {
    pub fn new(id: u32, score: f32) -> Hit {
        Hit { id, score }
    }
}

pub fn to_documents(persistence: &Persistence, hits: &[Hit], select: &Option<Vec<String>>, result: &SearchResult) -> Vec<DocWithHit> {
    // This is a fastpath why_found highlighting
    let tokens_set = {
        result
            .why_found_terms
            .iter()
            .map(|(path, terms)| {
                let tokens_set: FnvHashSet<String> = terms.iter().map(|el| el.to_string()).collect();
                (path.to_string(), tokens_set)
            })
            .collect()
    };

    let doc_store_data = persistence.get_file_bytes("data").expect("could not open document store");
    let doc_store = DocLoader::open(doc_store_data.as_ref());
    hits.iter()
        .map(|hit| {
            if let Some(ref select) = select {
                DocWithHit {
                    doc: read_data(persistence, hit.id, select).unwrap(), // TODO validate fields
                    hit: hit.clone(),
                    explain: result.explain.get(&hit.id).cloned(),
                    why_found: result.why_found_info.get(&hit.id).cloned().unwrap_or_default(),
                }
            } else {
                let doc_str = doc_store.get_doc(hit.id).unwrap(); // TODO No unwrapo
                let ayse = highlight_on_original_document(persistence, &doc_str, &tokens_set);

                DocWithHit {
                    doc: serde_json::from_str(&doc_str).unwrap(),
                    hit: hit.clone(),
                    explain: result.explain.get(&hit.id).cloned(),
                    why_found: ayse,
                }
            }
        })
        .collect::<Vec<_>>()
}

pub fn to_search_result(persistence: &Persistence, hits: SearchResult, select: &Option<Vec<String>>) -> SearchResultWithDoc {
    SearchResultWithDoc {
        data: to_documents(persistence, &hits.data, select, &hits),
        num_hits: hits.num_hits,
        facets: hits.facets,
        execution_time_ns: hits.execution_time_ns,
    }
}

#[inline]
fn get_all_value_ids(ids: &[u32], token_to_text_id: &dyn IndexIdToParent<Output = u32>) -> Vec<u32> {
    let mut text_ids: Vec<u32> = vec![];
    for id in ids {
        text_ids.extend(token_to_text_id.get_values_iter(u64::from(*id)))
    }
    text_ids
}

#[inline]
pub fn sort_by_score_and_id(a: &Hit, b: &Hit) -> Ordering {
    let cmp = b.score.partial_cmp(&a.score);
    if cmp == Some(Ordering::Equal) {
        b.id.partial_cmp(&a.id).unwrap_or(Ordering::Equal)
    } else {
        cmp.unwrap()
    }
}

pub fn explain_plan(mut request: Request, _persistence: &Persistence) -> Result<String, VelociError> {
    request.top = request.top.or(Some(10));

    let mut plan = Plan::default();
    plan_creator(request, &mut plan);

    let mut dot_graph = vec![];
    render_plan_to(&plan, &mut dot_graph);
    Ok(String::from_utf8(dot_graph)?)
}

pub fn search(mut request: Request, persistence: &Persistence) -> Result<SearchResult, VelociError> {
    let start_time = std::time::Instant::now();
    info_time!("search");
    request.top = request.top.or(Some(10));

    let mut res = {
        info_time!("search terms");
        let mut plan = Plan::default();
        if request.search_req.is_none() {
            return Err(VelociError::InvalidRequest {
                message: format!("search_req is None, but is required in search, request: {:?}", request),
            });
        }
        plan_creator(request.clone(), &mut plan);

        if log_enabled!(log::Level::Debug) {
            let mut dot_graph = vec![];
            render_plan_to(&plan, &mut dot_graph);
            debug!("{}", String::from_utf8(dot_graph)?);
        }

        let plan_result = plan.plan_result.as_ref().unwrap().clone();
        for stepso in plan.get_ordered_steps() {
            execute_steps(stepso, persistence)?;
        }
        let res = plan_result.recv().unwrap();
        drop(plan_result);
        res
    };

    let mut search_result = SearchResult { ..Default::default() };
    search_result.explain = res.explain.clone();

    if let Some(boost_term) = request.boost_term {
        res = apply_boost_term(persistence, res, &boost_term)?;
    }

    if request.text_locality {
        info_time!("boost_text_locality_all");
        let boost_anchor = boost_text_locality_all(persistence, &mut res.term_id_hits_in_field)?;
        res = apply_boost_from_iter(res, &mut boost_anchor.iter().cloned());
    }
    let term_id_hits_in_field = res.term_id_hits_in_field;
    search_result.why_found_terms = res.term_text_in_field;

    if let Some(facets_req) = request.facets {
        info_time!("all_facets {:?}", facets_req.iter().map(|el| el.field.clone()).collect::<Vec<_>>());

        let hit_ids: Vec<u32> = {
            // get sorted ids, for facets
            debug_time!("get_and_sort_for_factes");
            let mut hit_ids: Vec<u32> = res.hits_scores.iter().map(|el| el.id).collect();
            debug_time!("get_and_sort_for_factes sort only!!!");
            hit_ids.sort_unstable();
            hit_ids
        };

        search_result.facets = Some(
            facets_req
                .par_iter()
                .map(|facet_req| (facet_req.field.to_string(), facet::get_facet(persistence, facet_req, &hit_ids).unwrap()))
                .collect(),
        );
    }
    search_result.num_hits = res.hits_scores.len() as u64;
    {
        debug_time!("sort search by score");
        if let Some(top) = request.top {
            search_result.data = top_n_sort(res.hits_scores, top as u32 + request.skip.unwrap_or(0) as u32);
        } else {
            search_result.data = res.hits_scores;
            search_result.data.sort_unstable_by(sort_by_score_and_id);
        }
    }

    apply_top_skip(&mut search_result.data, request.skip, request.top);

    if request.why_found && request.select.is_some() {
        let anchor_ids: Vec<u32> = search_result.data.iter().map(|el| el.id).collect();
        let why_found_info = get_why_found(persistence, &anchor_ids, &term_id_hits_in_field)?;
        search_result.why_found_info = why_found_info;
    }
    // let time_in_ms = (start.elapsed().as_micros() as f64 * 1_000.0) + (start.elapsed().subsec_nanos() as f64 / 1000_000.0);
    search_result.execution_time_ns = start_time.elapsed().as_nanos() as u64;
    Ok(search_result)
}

pub fn apply_top_skip<T: Clone>(hits: &mut Vec<T>, skip: Option<usize>, top: Option<usize>) {
    if let Some(mut skip) = skip {
        skip = cmp::min(skip, hits.len());
        hits.drain(..skip);
    }
    if let Some(mut top) = top {
        top = cmp::min(top, hits.len());
        hits.drain(top..);
    }
}

#[inline]
fn join_and_get_text_for_ids(persistence: &Persistence, id: u32, prop: &str) -> Result<Option<String>, VelociError> {
    // TODO CHECK field_name exists previously
    let field_name = prop.add(TEXTINDEX);
    let text_value_id_opt = join_for_1_to_1(persistence, id, &field_name.add(PARENT_TO_VALUE_ID))?;
    if let Some(text_value_id) = text_value_id_opt {
        let text = if text_value_id >= persistence.metadata.columns[prop].textindex_metadata.num_text_ids as u32 {
            let text_id_to_token_ids = persistence.get_valueid_to_parent(field_name.add(TEXT_ID_TO_TOKEN_IDS))?;
            let vals = text_id_to_token_ids.get_values(u64::from(text_value_id));
            if let Some(vals) = vals {
                vals.iter()
                    .map(|token_id| get_text_for_id(persistence, &field_name, *token_id))
                    .collect::<Vec<_>>()
                    .concat()
            } else {
                return Err(VelociError::MissingTextId {
                    text_value_id,
                    field_name: field_name.add(TEXT_ID_TO_TOKEN_IDS),
                });
            }
        } else {
            get_text_for_id(persistence, &field_name, text_value_id)
        };

        Ok(Some(text))
    } else {
        Ok(None)
    }
}

//TODO CHECK FIELD VALIDTY
pub fn get_read_tree_from_fields(persistence: &Persistence, fields: &[String]) -> util::NodeTree {
    let all_steps: Vec<Vec<String>> = fields
        .iter()
        .filter(|path| persistence.has_index(&path.add(TEXTINDEX).add(PARENT_TO_VALUE_ID)))
        .map(|field| util::get_all_steps_to_anchor(field))
        .collect();
    to_node_tree(all_steps)
}

pub fn join_to_parent_ids(persistence: &Persistence, input: &SearchFieldResult, path: &str, _trace_time_info: &str) -> Result<SearchFieldResult, VelociError> {
    let mut total_values = 0;
    let num_hits = input.hits_ids.len();

    let mut hits = Vec::with_capacity(num_hits);
    let kv_store = persistence.get_valueid_to_parent(path)?;

    let should_explain = input.request.is_explain();

    let mut explain_hits: FnvHashMap<u32, Vec<Explain>> = FnvHashMap::default();

    for id in &input.hits_ids {
        if let Some(values) = kv_store.get_values(u64::from(*id)).as_ref() {
            total_values += values.len();
            hits.reserve(values.len());
            // trace!("value_id: {:?} values: {:?} ", value_id, values);
            for parent_val_id in values {
                hits.push(*parent_val_id);

                if should_explain {
                    let expains = input.explain.get(id).unwrap_or_else(|| panic!("could not find explain for id {:?}", *id));
                    explain_hits.entry(*parent_val_id).or_insert_with(|| expains.clone());
                }
            }
        }
    }
    hits.sort_unstable();
    hits.dedup();

    debug!("{:?} hits hit {:?} distinct ({:?} total ) in column {:?}", num_hits, hits.len(), total_values, path);
    let mut res = SearchFieldResult::new_from(input);
    res.hits_ids = hits;
    res.explain = explain_hits;
    Ok(res)
}

pub fn join_for_1_to_1(persistence: &Persistence, value_id: u32, path: &str) -> Result<std::option::Option<u32>, VelociError> {
    let kv_store = persistence.get_valueid_to_parent(path)?;
    // trace!("path {:?} id {:?} resulto {:?}", path, value_id, kv_store.get_value(value_id as u64));
    Ok(kv_store.get_value(u64::from(value_id)))
}

pub fn join_for_1_to_n(persistence: &Persistence, value_id: u32, path: &str) -> Result<Option<Vec<u32>>, VelociError> {
    let kv_store = persistence.get_valueid_to_parent(path)?;
    Ok(kv_store.get_values(u64::from(value_id)))
}
