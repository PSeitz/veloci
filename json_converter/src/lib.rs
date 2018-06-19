#![feature(plugin, custom_attribute)]
#![feature(test)]
#![feature(entry_and_modify)]
#[macro_use]
extern crate serde_json;
extern crate chashmap;
extern crate fnv;
extern crate rayon;
extern crate test;

use fnv::FnvHashMap;
use serde_json::Value;
use std::borrow::Cow;
use std::str;
// use rayon::prelude::*;
use chashmap::CHashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

pub mod bench;

#[inline(always)]
pub fn convert_to_string(value: &Value) -> Cow<str> {
    match *value {
        Value::String(ref s) => Cow::from(s.as_str()),
        Value::Number(ref i) if i.is_u64() => Cow::from(i.as_u64().unwrap().to_string()),
        Value::Number(ref i) if i.is_f64() => Cow::from(i.as_f64().unwrap().to_string()),
        Value::Bool(ref i) => Cow::from(i.to_string()),
        _ => Cow::from(""),
    }
}


pub fn for_each_element_and_doc<F, F2, F3, I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>>(
    data: I,
    id_provider: &mut IDProvider,
    cb_text: &mut F,
    cb_ids: &mut F2,
    cb_docs: &mut F3,
) where
    F: FnMut(u32, &str, &str, u32, bool),
    F2: FnMut(u32, &str, u32, u32),
    F3: FnMut(&Value),
{
    let mut path = String::with_capacity(25);
    let mut is_new_doc;
    for el in data {
        is_new_doc = true;
        // let root_id = id_provider.get_id("");
        // for_each_elemento(&el.unwrap(), root_id, id_provider, root_id, &mut path, "", cb_text, cb_ids);
        // path.clear();

        if let Some(arr) = el.as_ref().unwrap().as_array() {
            for el in arr.iter() {
                let root_id = id_provider.get_id("");
                for_each_elemento(el, root_id, id_provider, root_id, &mut path, "", cb_text, cb_ids, &mut is_new_doc);
                path.clear();
                cb_docs(el);
            }
        } else {
            let root_id = id_provider.get_id("");
            for_each_elemento(el.as_ref().unwrap(), root_id, id_provider, root_id, &mut path, "", cb_text, cb_ids, &mut is_new_doc);
            cb_docs(el.as_ref().unwrap());
        }
        path.clear();
    }
}

#[inline]
pub fn for_each_element<F, F2, I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>>(
    data: I,
    id_provider: &mut IDProvider,
    cb_text: &mut F,
    cb_ids: &mut F2,
) where
    F: FnMut(u32, &str, &str, u32, bool),
    F2: FnMut(u32, &str, u32, u32),
{
    let mut path = String::with_capacity(25);

    let mut is_new_doc;
    for el in data {
        is_new_doc = true;
        // let root_id = id_provider.get_id("");
        // for_each_elemento(&el.unwrap(), root_id, id_provider, root_id, &mut path, "", cb_text, cb_ids);
        // path.clear();

        if let Some(arr) = el.as_ref().unwrap().as_array() {
            //TODO code path invalid??, data format should always be line seperated
            for el in arr.iter() {
                let root_id = id_provider.get_id("");
                for_each_elemento(el, root_id, id_provider, root_id, &mut path, "", cb_text, cb_ids, &mut is_new_doc);
                path.clear();
            }
        } else {
            let root_id = id_provider.get_id("");
            for_each_elemento(el.as_ref().unwrap(), root_id, id_provider, root_id, &mut path, "", cb_text, cb_ids, &mut is_new_doc);
        }
        path.clear();
    }
}


pub fn for_each_elemento<F, F2>(
    data: &Value,
    anchor_id: u32,
    id_provider: &mut IDProvider,
    parent_id: u32,
    mut current_path: &mut String,
    current_el_name: &str,
    cb_text: &mut F,
    cb_ids: &mut F2,
    is_new_doc: &mut bool,
) where
    F: FnMut(u32, &str, &str, u32, bool),
    F2: FnMut(u32, &str, u32, u32),
{
    if let Some(arr) = data.as_array() {
        let delimiter: &'static str = if current_path.is_empty() || current_path.ends_with('.') { "" } else { "." };
        current_path.push_str(delimiter);
        current_path.push_str(current_el_name);
        current_path.push_str("[]");
        let prev_len = current_path.len();
        for el in arr {
            let id = id_provider.get_id(&current_path);
            cb_ids(anchor_id, &current_path, id, parent_id);
            for_each_elemento(el, anchor_id, id_provider, id, current_path, "", cb_text, cb_ids, is_new_doc);
            unsafe {
                current_path.as_mut_vec().truncate(prev_len);
            }
        }
    } else if let Some(obj) = data.as_object() {
        let delimiter: &'static str = if current_path.is_empty() || current_path.ends_with('.') { "" } else { "." };
        current_path.push_str(delimiter);
        current_path.push_str(current_el_name);
        let prev_len = current_path.len();
        for (key, ref value) in obj.iter() {
            for_each_elemento(value, anchor_id, id_provider, parent_id, &mut current_path, key, cb_text, cb_ids, is_new_doc);
            unsafe {
                current_path.as_mut_vec().truncate(prev_len);
            }
        }
    } else if !data.is_null() {
        current_path.push_str(current_el_name);
        current_path.push_str(".textindex");
        cb_text(anchor_id, convert_to_string(&data).as_ref(), &current_path, parent_id, *is_new_doc);
        *is_new_doc = false;
    }
}

pub trait IDProvider {
    fn get_id(&mut self, path: &str) -> u32;
}

#[derive(Debug, Default)]
pub struct ConcurrentIDHolder {
    pub ids: CHashMap<String, AtomicUsize>,
}

impl IDProvider for ConcurrentIDHolder {
    #[inline(always)]
    fn get_id(&mut self, path: &str) -> u32 {
        {
            if let Some(e) = self.ids.get_mut(path) {
                return e.fetch_add(1, Ordering::SeqCst) as u32;
            }
        }

        {
            self.ids.upsert(path.to_string(), || AtomicUsize::new(0), |_exisitng| {});
        }

        if let Some(e) = self.ids.get_mut(path) {
            return e.fetch_add(1, Ordering::SeqCst) as u32;
        }
        panic!("path not existing in id holder");
    }
}
impl ConcurrentIDHolder {
    pub fn new() -> ConcurrentIDHolder {
        ConcurrentIDHolder { ids: CHashMap::default() }
    }
}

#[derive(Debug, Clone, Default)]
pub struct IDHolder {
    pub ids: FnvHashMap<String, u32>,
}

impl IDProvider for IDHolder {
    #[inline(always)]
    fn get_id(&mut self, path: &str) -> u32 {
        {
            if let Some(e) = self.ids.get_mut(path) {
                *e += 1;
                return *e;
            }
        }

        self.ids.insert(path.to_string(), 0);
        0
    }
}

impl IDHolder {
    pub fn new() -> IDHolder {
        IDHolder { ids: FnvHashMap::default() }
    }
}

// #[test]
// fn test_foreach() {
//     let data = json!({
//         "a": 1,
//         "more": ["ok", "nice"],
//         "objects": [{
//             "stuff": "yii",
//             "nothing": null
//         },{
//             "stuff": "yaa"
//         }],
//         "address": [
//             {
//                 "line": [ "line1" ]
//             }
//         ]
//     });

//     let mut id_holder = IDHolder::new();

//     let mut cb_text = |_anchor_id: u32, value: &str, path: &str, parent_val_id: u32, _is_new_doc: bool| {
//         println!("TEXT: path {} value {} parent_val_id {}", path, value, parent_val_id);
//     };
//     let mut callback_ids = |_anchor_id: u32, path: &str, val_id: u32, parent_val_id: u32| {
//         println!("IDS: path {} val_id {} parent_val_id {}", path, val_id, parent_val_id);
//     };

//     let data_str = serde_json::to_string(&data).unwrap();
//     let mut stream = Deserializer::from_str(&data_str).into_iter::<Value>();

//     for_each_element(stream, &mut id_holder, &mut cb_text, &mut callback_ids);

// }
