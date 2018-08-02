use fnv::FnvHashMap;
use itertools::Itertools;
use num;
use persistence::*;
use search::*;
use search_field::*;
use std;
use std::cmp::Ordering;
use util;
use util::StringAdd;

fn get_top_facet_group<T: IndexIdToParentData>(hits: &FnvHashMap<T, usize>, top: Option<usize>) -> Vec<(T, u32)> {
    let groups: Vec<(T, u32)> = hits.iter().map(|ref tupl| (*tupl.0, *tupl.1 as u32)).collect();
    sort_and_apply_top_skip_group(groups, top)
}

fn sort_and_apply_top_skip_group<T: IndexIdToParentData>(mut groups: Vec<(T, u32)>, top: Option<usize>) -> Vec<(T, u32)> {
    groups.sort_unstable_by(|a, b| b.1.cmp(&a.1));
    groups = apply_top_skip(&groups, None, top);
    groups
}

fn get_groups_with_text(persistence: &Persistence, groups: &[(u32, u32)], field: &str) -> Vec<(String, usize)> {
    groups.iter().map(|el| (get_text_for_id(persistence, field, el.0), el.1 as usize)).collect()
}

// TODO Check ignorecase, check duplicates in facet data
// For ignorecase, we probably need a term_ids -> lower case term id mapping index - read all texts annd aggregate may be too slow
pub fn get_facet(persistence: &Persistence, req: &FacetRequest, ids: &[u32]) -> Result<Vec<(String, usize)>, SearchError> {
    info_time!("facets in field {:?}", req.field);
    trace!("get_facet for ids {:?}", ids);
    let steps = util::get_steps_to_anchor(&req.field);
    info!("facet on {:?}", steps);

    // one step facet special case
    if steps.len() == 1 || persistence.has_index(&(steps.last().unwrap().add(ANCHOR_TO_TEXT_ID))) {
        let path = if steps.len() == 1 {
            steps.first().unwrap().add(PARENT_TO_VALUE_ID)
        } else {
            steps.last().unwrap().add(ANCHOR_TO_TEXT_ID)
        };
        let kv_store = persistence.get_valueid_to_parent(path)?;
        let hits = {
            debug_time!("facet count_values_for_ids {:?}", req.field);
            kv_store.count_values_for_ids(ids, req.top.map(|el| el as u32))
        };

        debug_time!("facet collect and get texts {:?}", req.field);

        let groups = get_top_facet_group(&hits, req.top);

        let groups_with_text = get_groups_with_text(persistence, &groups, steps.last().unwrap());
        debug!("{:?}", groups_with_text);
        return Ok(groups_with_text);
    }

    let mut next_level_ids = join_anchor_to_leaf(persistence, ids, &steps)?;

    let mut groups = vec![];
    {
        debug_time!("facet group by field {:?}", req.field);
        next_level_ids.sort_unstable();
        for (key, group) in &next_level_ids.into_iter().group_by(|el| *el) {
            groups.push((key, group.count() as u32));
        }
        groups = sort_and_apply_top_skip_group(groups, req.top);
    }
    let groups_with_text = get_groups_with_text(persistence, &groups, steps.last().unwrap());
    debug!("{:?}", groups_with_text);
    Ok(groups_with_text)
}

pub(crate) fn join_anchor_to_leaf(persistence: &Persistence, ids: &[u32], steps: &[String]) -> Result<Vec<u32>, SearchError> {
    let mut next_level_ids = { join_for_n_to_m(persistence, ids, &(steps.first().unwrap().add(PARENT_TO_VALUE_ID)))? };
    for step in steps.iter().skip(1) {
        trace!("facet step {:?}", step);
        next_level_ids = join_for_n_to_m(persistence, &next_level_ids, &(step.add(PARENT_TO_VALUE_ID)))?;
    }

    Ok(next_level_ids)
}

#[cfg_attr(feature = "flame_it", flame)]
fn join_for_n_to_m(persistence: &Persistence, value_ids: &[u32], path: &str) -> Result<Vec<u32>, SearchError> {
    let kv_store = persistence.get_valueid_to_parent(path)?;
    let mut hits = vec![];
    hits.reserve(value_ids.len()); // TODO reserve by statistics

    kv_store.append_values_for_ids(value_ids, &mut hits);

    trace!("hits {:?}", hits);
    Ok(hits)
}

pub(crate) trait AggregationCollector<T: IndexIdToParentData> {
    fn add(&mut self, id: T);
    fn to_map(self: Box<Self>, top: Option<u32>) -> FnvHashMap<T, usize>;
}

pub(crate) fn should_prefer_vec(num_ids: u32, avg_join_size: f32, max_value_id: u32) -> bool {
    let num_inserts = (num_ids as f32 * avg_join_size) as u32;
    let vec_len = max_value_id.saturating_add(1);

    let prefer_vec = num_inserts * 20 > vec_len;
    debug!("prefer_vec {} {}>{}", prefer_vec, num_inserts * 20, vec_len);
    prefer_vec
}

fn get_top_n_sort_from_iter<T: num::Zero + std::cmp::PartialOrd + Copy + std::fmt::Debug, K: Copy, I: Iterator<Item = (K, T)>>(iter: I, top: usize) -> Vec<(K, T)> {
    let mut top_n: Vec<(K, T)> = vec![];

    let mut current_worst = T::zero();
    for el in iter {
        if el.1 < current_worst {
            continue;
        }

        if !top_n.is_empty() && top_n.len() == 200 + top {
            // 200 + top proved to be good
            top_n.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
            top_n.truncate(top);
            current_worst = top_n.last().unwrap().1;
            trace!("facet new worst {:?}", current_worst);
        }

        top_n.push((el.0, el.1));
    }
    top_n
}

impl<T: IndexIdToParentData> AggregationCollector<T> for Vec<T> {
    fn to_map(self: Box<Self>, top: Option<u32>) -> FnvHashMap<T, usize> {
        debug_time!("aggregation vec to_map");

        if let Some(top) = top {
            get_top_n_sort_from_iter(self.iter().enumerate().filter(|el| *el.1 != T::zero()).map(|el| (el.0, *el.1)), top as usize)
                .into_iter()
                .map(|el| (num::cast(el.0).unwrap(), num::cast(el.1).unwrap()))
                .collect()
        } else {
            let mut groups: Vec<(u32, T)> = self.iter().enumerate().filter(|el| *el.1 != T::zero()).map(|el| (el.0 as u32, *el.1)).collect();
            groups.sort_by(|a, b| b.1.cmp(&a.1));
            groups.into_iter().map(|el| (num::cast(el.0).unwrap(), num::cast(el.1).unwrap())).collect()
        }
    }

    #[inline]
    fn add(&mut self, id: T) {
        let id_usize = id.to_usize().unwrap();
        debug_assert!(self.len() > id_usize, "max_value_id metadata wrong, therefore facet vec wrong size");
        unsafe {
            let elem = self.get_unchecked_mut(id_usize);
            *elem = *elem + T::one();
        }
    }
}

impl<T: IndexIdToParentData> AggregationCollector<T> for FnvHashMap<T, usize> {
    fn to_map(self: Box<Self>, _top: Option<u32>) -> FnvHashMap<T, usize> {
        *self
    }

    #[inline]
    fn add(&mut self, id: T) {
        let stat = self.entry(id).or_insert(0);
        *stat += 1;
    }
}
