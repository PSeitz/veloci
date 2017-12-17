#![feature(plugin, custom_attribute)]
#![plugin(flamer)]
#![feature(test)]
#![feature(entry_and_modify)]
#[macro_use]
extern crate serde_json;
extern crate fnv;
extern crate test;
extern crate flame;

use fnv::FnvHashMap;
use serde_json::Value;
use std::str;

pub mod bench;

pub struct ForEachOpt {
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

#[flame]
pub fn for_each_element<F, F2>(data: &Value, id_provider: &mut IDProvider, opt: &mut ForEachOpt, cb_text: &mut F, cb_ids: &mut F2)
where
    F: FnMut(&str, &str, u32),
    F2: FnMut(&str, u32, u32)
{
    if let Some(arr) = data.as_array() {
        for el in arr {
            let root_id = id_provider.get_id("");
            for_each_elemento(el, id_provider, root_id, &mut "".to_owned(), "", opt, cb_text, cb_ids);
        }
    } else {
        let root_id = id_provider.get_id("");
        for_each_elemento(data, id_provider, root_id, &mut "".to_owned(), "", opt, cb_text, cb_ids);
    }

}

pub fn for_each_elemento<F, F2>(data: &Value, id_provider: &mut IDProvider, parent_id:u32, mut current_path:&mut String, current_el_name:&str, opt: &mut ForEachOpt, cb_text: &mut F, cb_ids: &mut F2)
where
    F: FnMut(&str, &str, u32),
    F2: FnMut(&str, u32, u32)
{
    
    if let Some(arr) = data.as_array() {
        let delimiter = if current_path.len() == 0 || current_path.ends_with(".") {""} else {"."};
        // current_path = current_path + delimiter + current_el_name + "[]";
        current_path.push_str(delimiter);
        current_path.push_str(current_el_name);
        current_path.push_str("[]");
        let prev_len = current_path.len();
        for el in arr {
            let id = id_provider.get_id(&current_path);
            cb_ids(&current_path, id, parent_id);
            for_each_elemento(el, id_provider, id, current_path, "", opt, cb_text, cb_ids);
            unsafe {current_path.as_mut_vec().truncate(prev_len); }
        }
    } else if let Some(obj) = data.as_object() {
        let delimiter = if current_path.len() == 0 || current_path.ends_with(".") {""} else {"."};
        // current_path = current_path + delimiter + current_el_name;
        current_path.push_str(delimiter);
        current_path.push_str(current_el_name);
        let prev_len = current_path.len();
        for (key, ref value) in obj.iter() {
            for_each_elemento(value, id_provider, parent_id, &mut current_path, key, opt, cb_text, cb_ids);
            unsafe {current_path.as_mut_vec().truncate(prev_len); }
        }
    } else {
        current_path.push_str(current_el_name);
        current_path.push_str(".textindex");
        cb_text(&convert_to_string(&data), &current_path , parent_id);
        // cb_text(data.as_str().unwrap(), &(current_path + current_el_name + ".textindex") , parent_id);
    }
}

pub trait IDProvider {
    fn get_id(&mut self, path: &str) -> u32;
}

#[derive(Debug)]
pub struct IDHolder {
    pub ids: FnvHashMap<String, u32>
}

impl IDProvider for IDHolder {
    fn get_id(&mut self, path: &str) -> u32{
        let stat = self.ids.entry(path.to_string()).and_modify(|e| { *e += 1 }).or_insert(0);
        *stat
    }
}

impl IDHolder {
    pub fn new() -> IDHolder {
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

    let mut opt = ForEachOpt {};
    let mut id_holder = IDHolder::new();

    let mut cb_text = |value: &str, path: &str, parent_val_id: u32| {
        println!("TEXT: path {} value {} parent_val_id {}",path, value, parent_val_id);
    };
    let mut callback_ids = |path: &str, val_id: u32, parent_val_id: u32| {
        println!("IDS: path {} val_id {} parent_val_id {}",path, val_id, parent_val_id);
    };


    let data = json!({
        "address": [
            {
                "line": [ "line1" ]
            }
        ]
    });


    for_each_element(&data, &mut id_holder, &mut opt, &mut cb_text, &mut callback_ids);

    assert_eq!(2 + 2, 4);
}
