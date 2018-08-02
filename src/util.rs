use persistence::TEXTINDEX;
use regex::Regex;
use search;
use std;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::Path;

pub(crate) fn normalize_text(text: &str) -> String {
    lazy_static! {
        static ref REGEXES:Vec<(Regex, & 'static str)> = vec![
            (Regex::new(r"\([fmn\d]\)").unwrap(), " "),
            (Regex::new(r"[\(\)]").unwrap(), " "),  // remove braces
            (Regex::new(r#"[{}'"“]"#).unwrap(), ""), // remove ' " {}
            (Regex::new(r"\s\s+").unwrap(), " "), // replace tabs, newlines, double spaces with single spaces
            (Regex::new(r"[,.…;・’-]").unwrap(), "")  // remove , .;・’-
        ];

    }
    let mut new_str = text.to_owned();
    // for tupl in &*RElet tupl = &&*REGEXES;
    for tupl in REGEXES.iter() {
        new_str = (tupl.0).replace_all(&new_str, tupl.1).into_owned();
    }

    new_str.to_lowercase().trim().to_owned()
}

pub fn open_file<P: AsRef<Path>>(path: P) -> Result<File, search::SearchError> {
    Ok(File::open(path.as_ref()).map_err(|err| search::SearchError::StringError(format!("Could not open {} {:?}", path.as_ref().to_str().unwrap(), err)))?)
}

// pub(crate) fn get_bit_at(input: u32, n: u8) -> bool {
//     if n < 32 {
//         input & (1 << n) != 0
//     } else {
//         false
//     }
// }

// pub(crate) fn load_flush_threshold_from_env() -> Result<Option<u32>, search::SearchError> {
//     if let Some(val) = env::var_os("FlushThreshold") {
//         let conv_env = val.clone()
//             .into_string()
//             .map_err(|_err| search::SearchError::StringError(format!("Could not convert LoadingType environment variable to utf-8: {:?}", val)))?;

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

// pub fn get_u32_from_bytes(data: &[u8], pos: usize) -> u32 {
//     let mut bytes: [u8; 4] = [0, 0, 0, 0];
//     bytes.copy_from_slice(&data[pos..pos + 4]);
//     unsafe { transmute(bytes) }
// }
use std::ptr::copy_nonoverlapping;
#[inline]
pub(crate) fn get_u32_from_bytes(data: &[u8], pos: usize) -> u32 {
    let mut out: u32 = 0;
    unsafe {
        copy_nonoverlapping(data[pos..].as_ptr(), &mut out as *mut u32 as *mut u8, 4);
    }
    out
}

// #[inline]
// pub(crate) fn unsafe_increase_len<T>(vec: &mut Vec<T>, add: usize) -> usize {
//     vec.reserve(1 + add);
//     let curr_pos = vec.len();
//     unsafe {
//         vec.set_len(curr_pos + add);
//     }
//     curr_pos
// }

// pub(crate) fn hits_map_to_vec(hits: &FnvHashMap<u32, f32>) -> Vec<Hit> {
//     hits.iter().map(|(id, score)| Hit { id: *id, score: *score }).collect()
// }

// pub(crate) fn hits_vec_to_map(vec_hits: Vec<Hit>) -> FnvHashMap<u32, f32> {
//     let mut hits: FnvHashMap<u32, f32> = FnvHashMap::default();
//     for hit in vec_hits {
//         hits.insert(hit.id, hit.score);
//     }
//     hits
// }

pub(crate) trait StringAdd {
    fn add<O: AsRef<str>>(&self, other: O) -> String;
}
impl<S: AsRef<str>> StringAdd for S {
    fn add<O: AsRef<str>>(&self, other: O) -> String {
        self.as_ref().to_string() + other.as_ref()
    }
}

pub(crate) fn get_file_path(folder: &str, path: &str) -> String {
    folder.to_string() + "/" + path
}

pub(crate) fn file_as_string<P: AsRef<Path> + std::fmt::Debug>(path: P) -> Result<(String), io::Error> {
    info!("Loading File {:?}", path);
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    Ok(contents)
}

// pub(crate) fn get_level(path: &str) -> u32 {
//     path.matches("[]").count() as u32
// }

// pub(crate) fn remove_array_marker(path: &str) -> String {
//     path.split('.')
//         .collect::<Vec<_>>()
//         .iter()
//         .map(|el| if el.ends_with("[]") { &el[0..el.len() - 2] } else { el })
//         .collect::<Vec<_>>()
//         .join(".")
// }

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

// #[inline]
// pub(crate) fn load_bytes_into(buffer: &mut [u8], mut file: &File, offset: u64) {
//     // @Temporary Use Result
//     file.seek(SeekFrom::Start(offset)).unwrap();
//     file.read_exact(buffer).unwrap();
// }

// #[inline]
// pub(crate) fn write_bytes_at(buffer: &[u8], mut file: &File, offset: u64) -> Result<(), io::Error> {
//     file.seek(SeekFrom::Start(offset))?;
//     file.write_all(buffer)
// }

#[inline]
pub(crate) fn extract_field_name(field: &str) -> String {
    field
    .chars()
    .take(field.chars().count() - 10) //remove .textindex
    .into_iter()
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

    paths.push(path.to_string() + TEXTINDEX); // add path to index
    paths
}

#[allow(unused_macros)]
macro_rules! print_json {
    ($e:expr) => {
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

    // paths.push(path.to_string() + TEXTINDEX); // add path to index
    paths
}

use itertools::Itertools;
use std::collections::HashMap;

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
