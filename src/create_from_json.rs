
use util;
use fnv::FnvHashMap;
use create::TermInfo;
use serde_json::Value;
use std::str;

use create;

pub struct ForEachOpt {
    pub parent_pos_in_path:        u32,
    pub current_parent_id_counter: u32,
    pub value_id_counter:          u32,
}

pub fn convert_to_string(value: &Value) -> String {
    match value {
        &Value::String(ref s) => s.as_str().to_string(),
        &Value::Number(ref i) if i.is_u64() => i.as_u64().unwrap().to_string(),
        &Value::Number(ref i) if i.is_f64() => i.as_f64().unwrap().to_string(),
        &Value::Bool(ref i) => i.to_string(),
        _ => "".to_string(),
    }
}


pub fn for_each_element_in_path<F>(data: &Value, opt: &mut ForEachOpt, path2: &str, cb: &mut F)
where
    F: FnMut(&str, u32, u32),
{
    // value, value_id, parent_val_id   // TODO ADD Template for Value

    let path = util::remove_array_marker(path2);
    let paths = path.split(".").collect::<Vec<_>>();
    debug!(" **** parent in path {:?}", paths[opt.parent_pos_in_path as usize]);
    if let Some(arr) = data.as_array() {
        for el in arr {
            walk(el, 0, opt, &paths, cb);
            if opt.parent_pos_in_path == 0 {
                opt.current_parent_id_counter += 1;
            }
        }
    } else {
        walk(data, 0, opt, &paths, cb);
    }
}



pub fn walk<F>(mut current_el: &Value, start_pos: u32, opt: &mut ForEachOpt, paths: &Vec<&str>, cb: &mut F)
where
    F: FnMut(&str, u32, u32),
{
    for i in start_pos..(paths.len() as u32) {
        let is_last_path = i == paths.len() as u32 - 1;
        
        let is_parent_path_pos = i == opt.parent_pos_in_path && i != 0;
        let comp = paths[i as usize];
        // println!("MOVE TO NEXT");
        // println!("{:?}", comp);
        // println!("{:?}", current_el.to_string());
        // println!("{:?}", current_el.get(comp));
        if !current_el.get(comp).is_some() {
            break;
        }
        let next_el = &current_el[comp];
        // println!("{:?}", next_el);
        if let Some(current_el_arr) = next_el.as_array() {  // WALK ARRAY
            if is_last_path {
                for el in current_el_arr {
                    if !el.is_null() {
                        cb(&convert_to_string(&el), opt.value_id_counter, opt.current_parent_id_counter);
                        opt.value_id_counter += 1;
                        // trace!("opt.value_id_counter increase {:?}", opt.value_id_counter);
                    }
                }
            } else { // ARRAY BUT NOT LAST PATH
                let next_level = i + 1;
                for subarr_el in current_el_arr {
                    walk(subarr_el, next_level, opt, paths, cb);
                    if is_parent_path_pos {
                        opt.current_parent_id_counter += 1;
                        // trace!("opt.current_parent_id_counter increase {:?}", opt.current_parent_id_counter);
                    }else{
                        // trace!("************** Er denkt das wäre der Parent {:?}", paths[opt.current_parent_id_counter as usize]); 
                        // trace!("Aber das ist der Parent {:?}", comp);
                    }
                }
            }
        } else { // WALK OBJECT
            if is_last_path {
                if !next_el.is_null() {
                    cb(&convert_to_string(&next_el), opt.value_id_counter, opt.current_parent_id_counter);
                    opt.value_id_counter += 1;
                }
            }else{
                opt.value_id_counter += 1;
            }
        }
        current_el = next_el
    }
}



pub fn get_allterms(data: &Value, path: &str, options: &create::FulltextIndexOptions) -> FnvHashMap<String, TermInfo> {
    let mut terms: FnvHashMap<String, TermInfo> = FnvHashMap::default();

    let mut opt = ForEachOpt {
        parent_pos_in_path:        0,
        current_parent_id_counter: 0,
        value_id_counter:          0,
    };

    for_each_element_in_path(&data, &mut opt, &path, &mut |value: &str, _value_id: u32, _parent_val_id: u32| {
        let normalized_text = util::normalize_text(value);
        trace!("normalized_text: {:?}", normalized_text);
        if options.stopwords.as_ref().map(|el| el.contains(&normalized_text)).unwrap_or(false) {
            return;
        }

        {
            let stat = terms.entry(normalized_text.clone()).or_insert(TermInfo::default());
            stat.num_occurences += 1;
        }

        if options.tokenize && normalized_text.split(" ").count() > 1 {
            for token in normalized_text.split(" ") {
                let token_str = token.to_string();
                if options.stopwords.as_ref().map(|el| el.contains(&normalized_text)).unwrap_or(false) {
                    continue;
                }
                // terms.insert(token_str);
                let stat = terms.entry(token_str.clone()).or_insert(TermInfo::default());
                stat.num_occurences += 1;
            }
        }
    });

    create::set_ids(&mut terms);
    terms

    // let mut v: Vec<String> = terms.into_iter().collect::<Vec<String>>();
    // v.sort();
    // v
}
