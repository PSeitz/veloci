use crate::{
    error::VelociError,
    persistence::{Persistence, PARENT_TO_VALUE_ID},
    search::*,
    util::NodeTree,
};

pub fn read_data(persistence: &Persistence, id: u32, fields: &[String]) -> Result<serde_json::Value, VelociError> {
    let tree = get_read_tree_from_fields(persistence, fields);
    read_tree(persistence, id, &tree)
}

fn read_tree(persistence: &Persistence, id: u32, tree: &NodeTree) -> Result<serde_json::Value, VelociError> {
    let mut json = json!({});
    match *tree {
        NodeTree::Map(ref map) => {
            for (prop, sub_tree) in map.iter() {
                let current_path = prop.add(PARENT_TO_VALUE_ID);
                let is_array = prop.ends_with("[]");
                match *sub_tree {
                    NodeTree::IsLeaf => {
                        if is_array {
                            if let Some(sub_ids) = join_for_1_to_n(persistence, id, &current_path)? {
                                let mut sub_data = vec![];
                                for sub_id in sub_ids {
                                    if let Some(texto) = join_and_get_text_for_ids(persistence, sub_id, prop)? {
                                        sub_data.push(json!(texto));
                                    }
                                }
                                json[extract_prop_name(prop)] = json!(sub_data);
                            }
                        } else if let Some(texto) = join_and_get_text_for_ids(persistence, id, prop)? {
                            json[extract_prop_name(prop)] = json!(texto);
                        }
                    }
                    NodeTree::Map(ref _next) => {
                        if !persistence.has_index(&current_path) {
                            // Special case a node without information an object in object e.g. there is no information 1:n to store
                            json[extract_prop_name(prop)] = read_tree(persistence, id, &sub_tree)?;
                        } else if let Some(sub_ids) = join_for_1_to_n(persistence, id, &current_path)? {
                            if is_array {
                                let mut sub_data = vec![];
                                for sub_id in sub_ids {
                                    sub_data.push(read_tree(persistence, sub_id, &sub_tree)?);
                                }
                                json[extract_prop_name(prop)] = json!(sub_data);
                            } else if let Some(sub_id) = sub_ids.get(0) {
                                json[extract_prop_name(prop)] = read_tree(persistence, *sub_id, &sub_tree)?;
                            }
                        }
                    }
                }
            }
        }
        NodeTree::IsLeaf => {}
    }

    Ok(json)
}
