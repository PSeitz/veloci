use persistence::*;
use search::*;
use search_field::*;
use util::*;
use util;
use itertools::Itertools;

//TODO Check ignorecase, check duplicates in facet data
pub fn get_facet(persistence: &Persistence, field:String, mut ids:Vec<u32>) -> Result<String, SearchError> {


    println!("{:?}", util::get_steps_to_anchor(&field));
    let steps = util::get_steps_to_anchor(&field);

    println!("{:?}", steps);
    // let tree = to_node_tree(vec![]);

    println!("ids {:?}", ids );
    for step in steps.iter() {

        ids = join_for_n_to_m(persistence, &ids, &(step.to_string()+".parentToValueId"))?;
    }

    ids.sort();
    println!("ids {:?}", ids );

    let mut groups = vec![];
    for (key, group) in &ids.into_iter().group_by(|el| *el) {
        groups.push((key, group.count()));
    }

    // groups.sort_by_key(|el|el.1);

    groups.sort_by(|a, b| b.1.cmp(&a.1));

    let mut groups_with_text = vec![];
    for el in groups {
        groups_with_text.push((get_text_for_id(persistence, steps.last().unwrap(), el.0), el.1))
    }

    println!("groups_with_text {:?}", groups_with_text );
    // read_tree(persistence, &ids, tree);
    Ok("".to_string())
}



#[flame]
pub fn join_for_n_to_m(persistence: &Persistence, value_ids: &[u32], path: &str) -> Result<Vec<u32>, SearchError>
{
    let kv_store = persistence.get_valueid_to_parent(path)?;
    let mut hits = vec![];
    for id in value_ids {	
        if let Some(value_ids) = kv_store.get_values(*id as u64) {
            println!("adding {:?}", value_ids);
            hits.extend(value_ids.iter());
        }
    }
    // Ok(value_ids.iter().flat_map(|el| kv_store.get_values(*el as u64).unwrap_or(vec![])).collect())
    // Ok(kv_store.get_values(value_id as u64))
    Ok(hits)
}

//TODO in_place version
#[flame]
pub fn join_for_n_to_n(persistence: &Persistence, value_ids: &Vec<u32>, path: &str) -> Result<Vec<u32>, SearchError>
{
    let kv_store = persistence.get_valueid_to_parent(path)?;

    Ok(value_ids.iter().flat_map(|el| kv_store.get_value(*el as u64)).collect())
    // Ok(kv_store.get_values(value_id as u64))
}
