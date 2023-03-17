use fnv::FnvHashMap;
use serde_json::Value;
use std::{borrow::Cow, str};

#[inline]
pub fn convert_to_string(value: &Value) -> Cow<'_, str> {
    match *value {
        Value::String(ref s) => Cow::from(s.as_str()),
        Value::Number(ref i) if i.is_u64() => Cow::from(i.as_u64().unwrap().to_string()),
        Value::Number(ref i) if i.is_f64() => Cow::from(i.as_f64().unwrap().to_string()),
        Value::Bool(ref i) => Cow::from(i.to_string()),
        _ => Cow::from(""),
    }
}

#[inline]
pub fn for_each_text<T, E, F, I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>>(data: I, cb_text: &mut F) -> Result<(), E>
where
    F: FnMut(&str, &str) -> Result<T, E>,
{
    let mut path = String::with_capacity(25);

    for el in data {
        for_each_texto(el.as_ref().unwrap(), &mut path, "", cb_text)?;
        path.clear();
    }
    Ok(())
}

#[inline]
pub fn for_each_texto<T, E, F>(data: &Value, mut current_path: &mut String, current_el_name: &str, cb_text: &mut F) -> Result<(), E>
where
    F: FnMut(&str, &str) -> Result<T, E>,
{
    if let Some(arr) = data.as_array() {
        if !(current_path.is_empty() || current_path.ends_with('.')) {
            current_path.push('.');
        }
        current_path.push_str(current_el_name);
        current_path.push('[');
        current_path.push(']');
        let prev_len = current_path.len();
        for el in arr {
            for_each_texto(el, current_path, "", cb_text)?;
            unsafe {
                current_path.as_mut_vec().truncate(prev_len);
            }
        }
    } else if let Some(obj) = data.as_object() {
        if !(current_path.is_empty() || current_path.ends_with('.')) {
            current_path.push('.');
        }
        current_path.push_str(current_el_name);
        let prev_len = current_path.len();
        for (key, ref value) in obj.iter() {
            for_each_texto(value, &mut current_path, key, cb_text)?;
            unsafe {
                current_path.as_mut_vec().truncate(prev_len);
            }
        }
    } else if !data.is_null() {
        current_path.push_str(current_el_name);
        cb_text(convert_to_string(&data).as_ref(), &current_path)?;
    }
    Ok(())
}

#[inline]
pub fn for_each_element<T, E, ID: IDProvider, F, F2, I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>>(
    data: I,
    id_provider: &mut ID,
    cb_text: &mut F,
    cb_ids: &mut F2,
) -> Result<(), E>
where
    E: std::convert::From<serde_json::error::Error>,
    F: FnMut(u32, &str, &str, u32) -> Result<T, E>,
    F2: FnMut(u32, &str, u32, u32) -> Result<T, E>,
{
    let mut path = String::with_capacity(25);

    for el in data {
        let root_id = id_provider.get_id("");
        for_each_elemento(&el?, root_id, id_provider, root_id, &mut path, "", cb_text, cb_ids)?;
        path.clear();
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn for_each_elemento<T, E, ID: IDProvider, F, F2>(
    data: &Value,
    anchor_id: u32,
    id_provider: &mut ID,
    parent_id: u32,
    mut current_path: &mut String,
    current_el_name: &str,
    cb_text: &mut F,
    cb_ids: &mut F2,
) -> Result<(), E>
where
    E: std::convert::From<serde_json::error::Error>,
    F: FnMut(u32, &str, &str, u32) -> Result<T, E>,
    F2: FnMut(u32, &str, u32, u32) -> Result<T, E>,
{
    if let Some(arr) = data.as_array() {
        current_path.push_str(current_el_name);
        current_path.push('[');
        current_path.push(']');
        let prev_len = current_path.len();
        for el in arr {
            let id = id_provider.get_id(&current_path);
            cb_ids(anchor_id, &current_path, id, parent_id)?;
            for_each_elemento(el, anchor_id, id_provider, id, current_path, "", cb_text, cb_ids)?;
            unsafe {
                current_path.as_mut_vec().truncate(prev_len);
            }
        }
    } else if let Some(obj) = data.as_object() {
        current_path.push_str(current_el_name);
        let is_root = current_path.is_empty();
        if !is_root {
            current_path.push('.');
        }
        let prev_len = current_path.len();
        for (key, ref value) in obj.iter() {
            for_each_elemento(value, anchor_id, id_provider, parent_id, &mut current_path, key, cb_text, cb_ids)?;
            unsafe {
                current_path.as_mut_vec().truncate(prev_len);
            }
        }
        current_path.pop();
    } else if !data.is_null() {
        current_path.push_str(current_el_name);
        cb_text(anchor_id, convert_to_string(&data).as_ref(), &current_path, parent_id)?;
    }
    Ok(())
}

pub trait IDProvider {
    fn get_id(&mut self, path: &str) -> u32;
}

#[derive(Debug, Clone, Default)]
pub struct IDHolder(FnvHashMap<String, u32>);

impl IDProvider for IDHolder {
    #[inline]
    fn get_id(&mut self, path: &str) -> u32 {
        {
            if let Some(e) = self.0.get_mut(path) {
                *e += 1;
                return *e;
            }
        }

        self.0.insert(path.to_string(), 0);
        0
    }
}

impl IDHolder {
    pub fn new() -> IDHolder {
        IDHolder(FnvHashMap::default())
    }
}

#[test]
fn test_foreach() {
    let mut id_holder = IDHolder::new();
    let mut callback_ids = |_anchor_id: u32, _path: &str, _val_id: u32, _parent_val_id: u32| -> Result<(), serde_json::error::Error> {
        // println!("IDS: path {} val_id {} parent_val_id {}", path, val_id, parent_val_id);
        Ok(())
    };

    let stream = r#"{"structure" : {"sub1" : "test"}}"#.lines().map(|line| serde_json::from_str(&line));
    for_each_element(
        stream,
        &mut id_holder,
        &mut |anchor_id: u32, value: &str, path: &str, _parent_val_id: u32| -> Result<(), serde_json::error::Error> {
            assert_eq!(path, "structure.sub1");
            assert_eq!(value, "test");
            assert_eq!(anchor_id, 0);
            assert_eq!(_parent_val_id, 0);
            Ok(())
        },
        &mut callback_ids,
    )
    .unwrap();

    let stream = r#"{"a" : "1"}"#.lines().map(|line| serde_json::from_str(&line));
    for_each_element(
        stream,
        &mut id_holder,
        &mut |_anchor_id: u32, value: &str, path: &str, _parent_val_id: u32| -> Result<(), serde_json::error::Error> {
            assert_eq!(path, "a");
            assert_eq!(value, "1");
            Ok(())
        },
        &mut callback_ids,
    )
    .unwrap();

    let stream = r#"{"meanings": {"ger" : ["karlo"]}}"#.lines().map(|line| serde_json::from_str(&line));
    for_each_element(
        stream,
        &mut id_holder,
        &mut |_anchor_id: u32, value: &str, path: &str, _parent_val_id: u32| -> Result<(), serde_json::error::Error> {
            assert_eq!(path, "meanings.ger[]");
            assert_eq!(value, "karlo");
            Ok(())
        },
        &mut callback_ids,
    )
    .unwrap();

    let stream = r#"{"meanings": {"ger" : ["karlo"]}}"#.lines().map(|line| serde_json::from_str(&line));
    for_each_text(stream, &mut |value: &str, path: &str| -> Result<(), serde_json::error::Error> {
        assert_eq!(path, "meanings.ger[]");
        assert_eq!(value, "karlo");
        Ok(())
    })
    .unwrap();
}
