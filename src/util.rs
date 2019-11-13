use std::path::PathBuf;
use crate::{error::VelociError, persistence::TEXTINDEX};
use itertools::Itertools;
use regex::Regex;
use std::{self, collections::HashMap, fs::File, io::prelude::*, path::Path};
use std::ffi::{OsString};

pub(crate) fn normalize_text(text: &str) -> String {
    lazy_static! {
        static ref REGEXES:Vec<(Regex, & 'static str)> = vec![
            (Regex::new(r"\([fmn\d]\)").expect("Could not create regex"), " "),
            (Regex::new(r"[\(\)]").expect("Could not create regex"), " "),  // remove braces
            (Regex::new(r#"[{}'"“]"#).expect("Could not create regex"), ""), // remove ' " {}
            (Regex::new(r"\s\s+").expect("Could not create regex"), " "), // replace tabs, newlines, double spaces with single spaces
            (Regex::new(r"[,.…;・’-]").expect("Could not create regex"), "")  // remove , .;・’-
        ];

    }
    let mut new_str = text.to_owned();
    // for tupl in &*RElet tupl = &&*REGEXES;
    for tupl in REGEXES.iter() {
        new_str = (tupl.0).replace_all(&new_str, tupl.1).into_owned();
    }

    new_str.to_lowercase().trim().to_owned()
}

pub fn open_file<P: AsRef<Path>>(path: P) -> Result<File, VelociError> {
    Ok(File::open(path.as_ref())
        .map_err(|err| VelociError::StringError(format!("Could not open {} {:?}", path.as_ref().to_str().expect("could not convert path to string"), err)))?)
}

#[derive(Debug)]
pub(crate) enum Ext {
    Indirect,
    Data,
}

pub(crate) trait SetExt {
    fn set_ext(&self, other: Ext) -> PathBuf;
}

impl SetExt for PathBuf {
    #[inline]
    fn set_ext(&self, other: Ext) -> PathBuf{
        self.as_path().set_ext(other)
    }
}

impl SetExt for Path {
    #[inline]
    fn set_ext(&self, other: Ext) -> PathBuf{
        let ext = match other {
            Ext::Indirect => "indirect",
            Ext::Data => "data",
        };
        let mut new_path = PathBuf::from(self);
        if !new_path.ends_with(ext){
            if let Some(curr_ext) = new_path.extension() {
                let mut new_ext = OsString::from(curr_ext);
                new_ext.push(".");
                new_ext.push(ext);
                new_path.set_extension(new_ext);
            }else{
                new_path.set_extension(ext);
            }
        }
        new_path
    }
}


// pub(crate) fn get_bit_at(input: u32, n: u8) -> bool {
//     if n < 32 {
//         input & (1 << n) != 0
//     } else {
//         false
//     }
// }

// pub(crate) fn load_flush_threshold_from_env() -> Result<Option<u32>, VelociError> {
//     if let Some(val) = env::var_os("FlushThreshold") {
//         let conv_env = val.clone()
//             .into_string()
//             .map_err(|_err| VelociError::StringError(format!("Could not convert LoadingType environment variable to utf-8: {:?}", val)))?;

//         let flush_threshold = conv_env
//             .parse::<u32>()
//             .map_err(|_err| format!("Expecting number for FlushThreshold, but got {:?}", conv_env))?;
//         Ok(Some(flush_threshold))
//     } else {
//         Ok(None)
//     }
// }

// #[inline]
// pub(crate) fn set_bit_at(input: &mut u32, n: u8) {
//     *input |= 1 << n
// }
// #[inline]
// pub(crate) fn is_bit_set_at(input: u32, n: u8) -> bool {
//     input & (1 << n) != 0
// }

const ONLY_HIGH_BIT_SET: u32 = (1 << 31);
const ALL_BITS_BUT_HIGHEST_SET: u32 = (1 << 31) - 1;

#[inline]
pub(crate) fn set_high_bit(input: &mut u32) {
    *input |= ONLY_HIGH_BIT_SET
}
#[inline]
pub(crate) fn unset_high_bit(input: &mut u32) {
    *input &= ALL_BITS_BUT_HIGHEST_SET
}

#[inline]
pub(crate) fn is_hight_bit_set(input: u32) -> bool {
    input & ONLY_HIGH_BIT_SET != 0
}

use std::ptr::copy_nonoverlapping;
#[inline]
pub(crate) fn get_u32_from_bytes(data: &[u8], pos: usize) -> u32 {
    let mut out: u32 = 0;
    unsafe {
        copy_nonoverlapping(data[pos..].as_ptr(), &mut out as *mut u32 as *mut u8, 4);
    }
    out
}

#[inline]
pub(crate) fn get_u64_from_bytes(data: &[u8], pos: usize) -> u64 {
    let mut out: u64 = 0;
    unsafe {
        copy_nonoverlapping(data[pos..].as_ptr(), &mut out as *mut u64 as *mut u8, 8);
    }
    out
}

pub(crate) trait StringAdd {
    fn add<O: AsRef<str>>(&self, other: O) -> String;
}
impl<S: AsRef<str>> StringAdd for S {
    #[inline]
    fn add<O: AsRef<str>>(&self, other: O) -> String {
        self.as_ref().to_string() + other.as_ref()
    }
}

pub(crate) fn get_file_path(folder: &str, path: &str) -> PathBuf {
    PathBuf::from(folder).join(path)
    // folder.to_string() + "/" + path
}

pub(crate) fn file_as_string<P: AsRef<Path> + std::fmt::Debug>(path: P) -> Result<String, VelociError> {
    info!("Loading File {:?}", path);
    let mut file = File::open(path.as_ref()).map_err(|err| VelociError::StringError(format!("Could not open {:?} {:?}", path, err)))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .map_err(|err| VelociError::StringError(format!("Could not read to string {:?} {:?}", path, err)))?;
    Ok(contents)
}

#[inline]
pub(crate) fn vec_with_size_uninitialized<T>(size: usize) -> Vec<T> {
    let mut buffer = vec![];
    buffer.reserve_exact(size);
    unsafe {
        buffer.set_len(size);
    }
    buffer
}
// #[inline]
// pub(crate) fn get_my_data_danger_zooone(start: u32, end: u32, data_file: &Mutex<fs::File>) -> Vec<u32> {
//     let mut data: Vec<u32> = vec_with_size_uninitialized(end as usize - start as usize);
//     {
//         let p = data.as_mut_ptr();
//         let len = data.len();
//         let cap = data.capacity();

//         unsafe {
//             // complete control of the allocation to which `p` points.
//             let ptr = p as *mut u8;
//             let mut data_bytes = Vec::from_raw_parts(ptr, len * 4, cap);

//             load_bytes_into(&mut data_bytes, &*data_file.lock(), u64::from(start) * 4); //READ directly into u32 data

//             // forget about temp data_bytes: no destructor run, so we can use data again
//             mem::forget(data_bytes);
//         }
//     }
//     data.retain(|el| *el != std::u32::MAX);
//     data
// }

#[inline]
pub(crate) fn extract_field_name(field: &str) -> String {
    field
        .chars()
        .take(field.chars().count() - 10) //remove .textindex
        .collect()
}

pub(crate) fn extract_prop_name(path: &str) -> &str {
    path.split('.')
        .map(|el| if el.ends_with("[]") { &el[0..el.len() - 2] } else { el })
        .filter(|el| *el != "textindex")
        .last()
        .unwrap_or_else(|| panic!("could not extract prop name from path {:?}", path))
}

#[inline]
pub(crate) fn get_steps_to_anchor(path: &str) -> Vec<String> {
    let mut paths = vec![];
    let mut current = vec![];
    let parts = path.split('.');

    for part in parts {
        current.push(part.to_string());
        if part.ends_with("[]") {
            let joined = current.join(".");
            paths.push(joined);
        }
    }

    paths.push(path.to_string() + TEXTINDEX); // last step is field.textindex
    paths
}

#[allow(unused_macros)]
macro_rules! print_json {
    ($e:expr) => {
        use serde_json;
        println!("{}", serde_json::to_string(&$e).unwrap());
    };
}

/// Also includes for e.g {"meaning":{"ger":["aye"]}}
/// the [meaning] and [meaning, ger] step, which is skipped in a search (not needed)
#[inline]
pub(crate) fn get_all_steps_to_anchor(path: &str) -> Vec<String> {
    let mut paths = vec![];
    let mut current = vec![];
    let parts = path.split('.');

    for part in parts {
        current.push(part.to_string());
        let joined = current.join(".");
        paths.push(joined);
    }

    paths
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum NodeTree {
    Map(HashMap<String, NodeTree>),
    IsLeaf,
}

impl NodeTree {
    pub fn new(map: HashMap<String, NodeTree>) -> NodeTree {
        NodeTree::Map(map)
    }
}

pub fn to_node_tree(mut paths: Vec<Vec<String>>) -> NodeTree {
    paths.sort_by_key(|el| el[0].clone()); // sort for group_by
    let mut next = HashMap::default();
    for (key, group) in &paths.into_iter().group_by(|el| el.get(0).cloned()) {
        let key = key.unwrap();
        let mut next_paths = group.collect_vec();

        let mut is_leaf = false;
        for el in &mut next_paths {
            el.remove(0);
            if el.is_empty() {
                //removing last part means it's a leaf
                is_leaf = true;
            }
        }

        next_paths.retain(|el| !el.is_empty()); //remove empty paths

        if next_paths.is_empty() {
            next.insert(key.to_string(), NodeTree::IsLeaf);
        } else {
            next_paths.sort_by_key(|el| el[0].clone());
            let sub_tree = if !is_leaf { to_node_tree(next_paths) } else { NodeTree::IsLeaf };
            next.insert(key.to_string(), sub_tree);
        }
    }
    NodeTree::new(next)
}
