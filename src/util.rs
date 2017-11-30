
use regex::Regex;
use std::io::prelude::*;
use std::io;
use fnv::FnvHashMap;
// use std::mem;
use std::fs::File;
// use std;
#[allow(unused_imports)]
use std;

pub fn normalize_text(text: &str) -> String {
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
    for ref tupl in &*REGEXES {
        new_str = (tupl.0).replace_all(&new_str, tupl.1).into_owned();
    }

    new_str.to_lowercase().trim().to_owned()
}

use search::Hit;

pub fn hits_map_to_vec(hits: FnvHashMap<u32, f32>) -> Vec<Hit> {
    hits.iter().map(|(id, score)| Hit { id:    *id, score: *score }).collect()
}

pub fn hits_vec_to_map(vec_hits: Vec<Hit>) -> FnvHashMap<u32, f32> {
    let mut hits: FnvHashMap<u32, f32> = FnvHashMap::default();
    for hit in vec_hits {
        hits.insert(hit.id, hit.score);
    }
    hits
}

pub fn boost_path(path: &str) -> (String, String) {
    concat_tuple(path, ".boost.subObjId", ".boost.value")
}

pub fn concat(path: &str, suffix: &str) -> String {
    path.to_string() + suffix
}

pub fn get_file_path(folder: &str, path: &str) -> String {
    folder.to_string() + "/" + path
}

pub fn concat_tuple(path: &str, suffix: &str, suffix2: &str) -> (String, String) {
    (concat(path, suffix), concat(path, suffix2))
}

pub fn get_file_path_name(path_to_anchor: &str, is_text_index_part: bool) -> String {
    let suffix = if is_text_index_part { ".textindex" } else { "" };
    path_to_anchor.to_owned() + suffix
}

pub fn file_as_string(path: &str) -> Result<(String), io::Error> {
    info!("Loading File {}", path);
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    Ok(contents)
}

pub fn get_level(path: &str) -> u32 {
    path.matches("[]").count() as u32
}

pub fn remove_array_marker(path: &str) -> String {
    path.split(".")
        .collect::<Vec<_>>()
        .iter()
        .map(|el| if el.ends_with("[]") { &el[0..el.len() - 2] } else { el })
        .collect::<Vec<_>>()
        .join(".")
}

pub fn get_steps_to_anchor(path: &str) -> Vec<String> {
    let mut paths = vec![];
    let mut current = vec![];
    // let parts = path.split('.')
    let parts = path.split(".");

    for part in parts {
        current.push(part.to_string());
        if part.ends_with("[]") {
            let joined = current.join(".");
            paths.push(joined);
        }
    }

    paths.push(path.to_string()); // add complete path
    return paths;
}


// assert_eq!(re.replace("1078910", ""), " ");

//     text = text.replace(/ *\([^)]*\) */g, ' ') // remove everything in braces
//     text = text.replace(/[{}'"]/g, '') // remove ' " {}
//     text = text.replace(/\s\s+/g, ' ') // replace tabs, newlines, double spaces with single spaces
//     text = text.replace(/[,.]/g, '') // remove , .
//     text = text.replace(/[;・’-]/g, '') // remove ;・’-
//     text = text.toLowerCase()
//     return text.trim()
// }

//     text = text.replace(/ *\([fmn\d)]*\) */g, ' ') // remove (f)(n)(m)(1)...(9)
//     text = text.replace(/[\(\)]/g, ' ') // remove braces
//     text = text.replace(/[{}'"“]/g, '') // remove ' " {}
//     text = text.replace(/\s\s+/g, ' ') // replace tabs, newlines, double spaces with single spaces
//     text = text.replace(/[,.…]/g, '') // remove , .
//     text = text.replace(/[;・’-]/g, '') // remove ;・’-
//     text = text.toLowerCase()
//     return text.trim()
// }
