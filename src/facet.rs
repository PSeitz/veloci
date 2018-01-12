use persistence::*;
use search::*;
use search_field::*;
use util;
use itertools::Itertools;

// use fnv::FnvHashMap;

//TODO Check ignorecase, check duplicates in facet data
pub fn get_facet(persistence: &Persistence, req: &FacetRequest, ids: &Vec<u32>) -> Result<Vec<(String, usize)>, SearchError> {
    info_time!(format!("facets in field {:?}", req.field));
    trace!("get_facet for ids {:?}", ids);
    let steps = util::get_steps_to_anchor(&req.field);
    println!("{:?}", steps);

    let mut next_level_ids = {
        debug_time!(format!("facets in field first join {:?}", req.field));
        join_for_n_to_m(
            persistence,
            &ids,
            &(steps.first().unwrap().to_string() + ".parentToValueId"),
        )?
    };
    for step in steps.iter().skip(1) {
        debug_time!(format!("facet step {:?}", step));
        debug!("facet step {:?}", step);
        next_level_ids = join_for_n_to_m(
            persistence,
            &next_level_ids,
            &(step.to_string() + ".parentToValueId"),
        )?;
    }

    let mut groups = vec![];
    {
        debug_time!(format!("facet group by field {:?}", req.field));
        next_level_ids.sort();
        for (key, group) in &next_level_ids.into_iter().group_by(|el| *el) {
            groups.push((key, group.count()));
        }
        groups.sort_by(|a, b| b.1.cmp(&a.1));
        groups = apply_top_skip(groups, 0, req.top);
    }

    let groups_with_text = groups
        .iter()
        .map(|el| {
            (
                get_text_for_id(persistence, steps.last().unwrap(), el.0),
                el.1,
            )
        })
        .collect();
    println!("{:?}", groups_with_text);
    Ok(groups_with_text)
}

#[flame]
pub fn join_for_n_to_m(persistence: &Persistence, value_ids: &[u32], path: &str) -> Result<Vec<u32>, SearchError> {
    let kv_store = persistence.get_valueid_to_parent(path)?;
    let mut hits = vec![];
    hits.reserve(value_ids.len()); // reserve by statistics
    for id in value_ids {
        if let Some(value_ids) = kv_store.get_values(*id as u64) {
            trace!("adding value_ids {:?}", value_ids);
            hits.extend(value_ids.iter());
        }
    }
    trace!("hits {:?}", hits);
    // Ok(value_ids.iter().flat_map(|el| kv_store.get_values(*el as u64).unwrap_or(vec![])).collect())
    // Ok(kv_store.get_values(value_id as u64))
    Ok(hits)
}

//TODO in_place version
#[flame]
pub fn join_for_n_to_n(persistence: &Persistence, value_ids: &Vec<u32>, path: &str) -> Result<Vec<u32>, SearchError> {
    let kv_store = persistence.get_valueid_to_parent(path)?;

    Ok(value_ids
        .iter()
        .flat_map(|el| kv_store.get_value(*el as u64))
        .collect())
    // Ok(kv_store.get_values(value_id as u64))
}
