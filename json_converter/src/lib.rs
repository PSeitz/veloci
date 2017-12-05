#![feature(entry_and_modify)]
#[macro_use]
extern crate serde_json;
extern crate fnv;

use fnv::FnvHashMap;
use serde_json::Value;
use std::str;

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


pub fn for_each_element<F, F2>(data: &Value, id_provider: &mut IDProvider, opt: &mut ForEachOpt, cb_text: &mut F, cb_ids: &mut F2)
where
    F: FnMut(&str, &str, u32, u32),
    F2: FnMut(&str, u32, u32)
{
    let root_id = Some(id_provider.get_id(""));
    for_each_elemento(data, id_provider, root_id, "".to_owned(), "", opt, cb_text, cb_ids);
}

pub fn for_each_elemento<F, F2>(data: &Value, id_provider: &mut IDProvider, parent_id:Option<u32>, mut current_path:String, current_el_name:&str, opt: &mut ForEachOpt, cb_text: &mut F, cb_ids: &mut F2)
where
    F: FnMut(&str, &str, u32, u32),
    F2: FnMut(&str, u32, u32)
{
    if let Some(arr) = data.as_array() {
        current_path = current_path + current_el_name + "[]";
        for el in arr {
            let id = id_provider.get_id(&current_path);
            if let Some(pat) = parent_id {
                cb_ids(&current_path, id, pat);
            }
            for_each_elemento(el, id_provider, Some(id), current_path.clone(), "", opt, cb_text, cb_ids);
        }
    } else if let Some(obj) = data.as_object() {
        let delimiter = if current_path.len() == 0 {""} else {"."};
        current_path = current_path + delimiter + current_el_name;
        for (key, ref value) in obj.iter() {
            for_each_elemento(value, id_provider, parent_id, current_path.clone(), key, opt, cb_text, cb_ids);
        }
    } else {
        cb_text(&convert_to_string(&data), &(current_path + current_el_name), 1, 2);
    }
}

pub trait IDProvider {
    fn get_id(&mut self, path: &str) -> u32;
}

#[derive(Debug)]
struct IDHolder {
    ids: FnvHashMap<String, u32>
}

impl IDProvider for IDHolder {
    fn get_id(&mut self, path: &str) -> u32{
        let stat = self.ids.entry(path.to_string()).and_modify(|e| { *e += 1 }).or_insert(0);
        *stat
    }
}

impl IDHolder {
    fn new() -> IDHolder {
        IDHolder{ids: FnvHashMap::default()}
    }
}

#[test]
fn test_foreach() {

    let data = json!({
        "a": 1,
        "more": ["ok", "nice"],
        "objects": [{
            "stuff": "yii"
        },{
            "stuff": "yaa"
        }]
    });

    let mut opt = ForEachOpt {
        parent_pos_in_path:        0,
        current_parent_id_counter: 0,
        value_id_counter:          0,
    };
    let mut id_holder = IDHolder::new();

    let mut callback_text = |value: &str, path: &str, parent_val_id: u32, parent_val_sid: u32| {
        println!("path {} value {}",path, value);
    };
    let mut callback_ids = |path: &str, val_id: u32, parent_val_id: u32| {
        println!("IDS: path {} val_id {} parent_val_id {}",path, val_id, parent_val_id);
    };

    for_each_element(&data, &mut id_holder, &mut opt, &mut callback_text, &mut callback_ids);

    assert_eq!(2 + 2, 4);
}
