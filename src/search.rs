
use std::fs::File;
use std::io::prelude::*;
#[allow(unused_imports)]
use std::io::{self, BufRead};
// use std::io::Error;
#[allow(unused_imports)]
use std::path::Path;
use std::char;
use std::cmp;

use serde_json;
use std::time::Duration;

use futures_cpupool::CpuPool;
use tokio_timer::Timer;

#[allow(unused_imports)]
use futures::{Poll, Future, Sink};
#[allow(unused_imports)]
use futures::executor;
#[allow(unused_imports)]
use futures::future::{ok, err};
#[allow(unused_imports)]
use futures::stream::{iter, Peekable, BoxStream, Stream};
#[allow(unused_imports)]
use futures::sync::oneshot;
#[allow(unused_imports)]
use futures::sync::mpsc;
use std::str;
#[allow(unused_imports)]
use std::thread;
#[allow(unused_imports)]
use std::sync::mpsc::sync_channel;
use std::fs;

// use std::os::windows::fs::FileExt;
use std::io::SeekFrom;
#[allow(unused_imports)]
use std::collections::HashMap;
use util;
use util::get_file_path;
#[allow(unused_imports)]
use std::collections::hash_map::Entry;
use fnv::FnvHashMap;
#[allow(unused_imports)]
use std::time::Instant;

use std::collections::hash_map::Entry::{Occupied, Vacant};
// fn get_text_lines2() -> BoxStream<String, io::Error> {
//     let (mut tx, rx) = channel();
//     thread::spawn(move || {
//         for msg in &["one", "two", "three", "four"] {
//             thread::sleep(Duration::from_millis(500));
//             tx = tx.send(Ok(msg.to_string())).wait().unwrap();
//         }
//         // tx.send("line");
//         // let x = vec!["asdf", "asddddd"];
//         // for line in x {
//         //      tx.send(line).wait() {
//         //         Ok(s) => tx = s,
//         //         Err(_) => break,
//         //     }
//         // }

//     });
//     return rx.boxed();
// }


#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct Request {
    pub or : Option<Vec<Request>>,
    pub and : Option<Vec<Request>>,
    pub search: Option<RequestSearchPart>,
    pub boost: Option<Vec<RequestBoostPart>> // @FixMe // @Hack 
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct RequestSearchPart {
    pub path: String,
    pub term: String,
    pub levenshtein_distance: u32,
    pub starts_with: Option<String>,
    pub exact: Option<bool>,
    pub first_char_exact_match: Option<bool>
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct RequestBoostPart {
    pub path: String,
    pub boost_fun: BoostFunction,
    pub param: Option<f32>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum BoostFunction {
    Log10,
}

impl Default for BoostFunction {
    fn default() -> BoostFunction { BoostFunction::Log10 }
}


// pub enum CheckOperators {
//     All,
//     One
// }
// impl Default for CheckOperators {
//     fn default() -> CheckOperators { CheckOperators::All }
// }


#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Hit {
    pub id: u32,
    pub score: f32
}


pub fn main2() {
    let res = test_levenshtein("anschauen", 2);
    println!("{:?}", res);

    let pool = CpuPool::new_num_cpus();
    let timer = Timer::default();
    // a future that resolves to Err after a timeout
    let timeout = timer.sleep(Duration::from_millis(750))
        .then(|_| Err(()));

    // spawn our computation, getting back a *future* of the answer
    let prime_future = pool.spawn_fn(|| {
        let prime = true;
        // For reasons we'll see later, we need to return a Result here
        let res: Result<bool, ()> = Ok(prime);
        res
    });

    let winner = timeout.select(prime_future).map(|(win, _)| win);
    // now block until we have a winner, then print what happened
    match winner.wait() {
        Ok(true) => println!("Priwwwme"),
        Ok(false) => println!("Not wwww"),
        Err(_) => println!("Timed wwwout"),
    }

    let search_part = RequestSearchPart{
        levenshtein_distance: 0,
        exact: Some(true),
        first_char_exact_match: Some(true),
        path: "jmdict/menaings.ger[].text".to_string(),
        term:"haus".to_string(),
        .. Default::default()
    };

    // let hits = get_hits_in_field("jmdict/meanings.ger[].text", &options, "haus");
    // let search_part = RequestSearchPart{path: "jmdict/meanings.ger[].text".to_string(), term:"haus".to_string(), options:options};
    let request = Request{search:Some(search_part), or:None, and:None, boost:None};

    let res:Vec<Hit> = search("", request, 0, 10).unwrap();
    println!("{:?}", res[0].id);

}

use std::cmp::Ordering;

fn hits_to_array(hits:FnvHashMap<u32, f32>) -> Vec<Hit> {
    let mut res:Vec<Hit> = hits.iter().map(|(id, score)| Hit{id:*id, score:*score}).collect();
    res.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal)); // Add sort by id
    res
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DocWithHit {
    pub doc: String,
    pub hit: Hit
}

use std;
impl std::fmt::Display for DocWithHit {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "\n{}\t{}", self.hit.id, self.hit.score )?;
        let val:serde_json::Value = serde_json::from_str(&self.doc).unwrap(); // @Temporary // @Cleanup 
        write!(f, "\n{}", serde_json::to_string_pretty(&val).unwrap() )?;
        Ok(())
    }
}

use doc_loader;
pub fn to_documents(hits: &Vec<Hit>, folder:&str) -> Vec<DocWithHit> {
    let doc_loader = doc_loader::DocLoader::new(folder, "data");
    hits.iter().map(|ref hit| {
        let doc = doc_loader.get_doc(hit.id as usize).unwrap();
        DocWithHit{doc:doc, hit:*hit.clone()}
    }).collect::<Vec<_>>()
}

pub fn search(folder:&str, request: Request, skip:usize, mut top:usize) -> Result<Vec<Hit>, SearchError>{
    let res = hits_to_array(search_unrolled(folder, request)?);
    top = cmp::min(top + skip, res.len());
    Ok(res[skip..top].to_vec())
}

pub fn search_unrolled(folder:&str, request: Request) -> Result<FnvHashMap<u32, f32>, SearchError>{
    if request.or.is_some() {
        Ok(request.or.unwrap().iter()
            .fold(FnvHashMap::default(), |mut acc, x| -> FnvHashMap<u32, f32> {
                acc.extend(search_unrolled(folder, x.clone()).unwrap());
                acc
            }))
        // return Promise.all(request.or.map(req => search_unrolled(req)))
        // .then(results => results.reduce((p, c) => Object.assign(p, c)))
    }else if request.and.is_some(){ //TODO Implement
        let ands = request.and.unwrap();
        let and_results = ands.iter().map(|x| search_unrolled(folder, x.clone()).unwrap() ).collect::<Vec<FnvHashMap<u32, f32>>>(); // @Hack  unwrap forward errors

        let mut all_results:FnvHashMap<u32, f32> = FnvHashMap::default();
        for res in &and_results {
            all_results.extend(res); // merge all results
        }

        all_results.retain(|&k, _| and_results.iter().all(|ref x| x.contains_key(&k)) );
        Ok(all_results)
    }else if request.search.is_some(){
        Ok(search_raw(folder, request.search.unwrap())?)
    }else{
        Ok(FnvHashMap::default())
    }
}

fn add_boost(folder: &str, boost: &RequestBoostPart, hits : &mut FnvHashMap<u32, f32>) -> Result<(), SearchError> {
    let boostkv_store = IndexKeyValueStore::new(&get_file_path(folder, &boost.path, ".boost.subObjId") , &get_file_path(folder, &boost.path, ".boost.value"));
    let boost_param = boost.param.unwrap_or(0.0);
    for (value_id, score) in hits {
        if let Some(boost_value) = boostkv_store.get_value(*value_id) {
            match boost.boost_fun {
                BoostFunction::Log10 => {
                    *score += (boost_value  as f32 + boost_param).log10();
                }
            }
        }
    }
    Ok(())
    
}


#[derive(Debug)]
pub enum SearchError{
    Io(io::Error),
    MetaData(serde_json::Error),
    Utf8Error(std::str::Utf8Error)
}

impl From<io::Error> for SearchError { // Automatic Conversion
    fn from(err: io::Error) -> SearchError {
        SearchError::Io(err)
    }
}

impl From<serde_json::Error> for SearchError { // Automatic Conversion
    fn from(err: serde_json::Error) -> SearchError {
        SearchError::MetaData(err)
    }
}

impl From<std::str::Utf8Error> for SearchError { // Automatic Conversion
    fn from(err: std::str::Utf8Error) -> SearchError {
        SearchError::Utf8Error(err)
    }
}

pub fn search_raw(folder:&str, mut request: RequestSearchPart) -> Result<FnvHashMap<u32, f32>, SearchError> {

    // let ref path = request.path;
    let term = util::normalize_text(&request.term);

    let mut hits = get_hits_in_field(folder, &mut request, &term)?;
    add_token_results(folder, &request.path, &mut hits);

    let mut next_level_hits:FnvHashMap<u32, f32> = FnvHashMap::default();

    let paths = util::get_steps_to_anchor(&request.path);
    info!("Joining paths::: {:?}", paths);
    for i in (0..paths.len()).rev() {
        let is_text_index = i == (paths.len() -1);
        let path_name = util::get_path_name(&paths[i], is_text_index);
        let kv_store = IndexKeyValueStore::new(&get_file_path(folder, &path_name, ".valueIdToParent.valIds") , &get_file_path(folder, &path_name, ".valueIdToParent.mainIds"));
        trace!("kv_store: {:?}", kv_store);
        for (value_id, score) in &hits {
            let values = kv_store.get_values(*value_id);
            trace!("value_id: {:?}", value_id);
            trace!("values: {:?}", values);
            for parent_val_id in values {
                match next_level_hits.entry(parent_val_id as u32) {
                    Vacant(entry) => { entry.insert(*score); },
                    Occupied(entry) => { *entry.into_mut() = score.max(*entry.get()) + 0.1; },
                }
            }
        }
        trace!("next_level_hits: {:?}", next_level_hits);
        hits = next_level_hits;
        next_level_hits = FnvHashMap::default();
    }

    Ok(hits)
}

#[derive(Debug)]
struct OffsetInfo {
    byte_range_start: u64,
    byte_range_end: u64,
    line_offset: u64,
}

#[derive(Debug)]
struct CharOffset {
    path: String,
    chars: Vec<String>,
    byte_offsets_start: Vec<u64>,
    byte_offsets_end: Vec<u64>,
    line_offsets: Vec<u64>,
}


impl CharOffset {
    fn new(path:&str) -> Result<CharOffset, SearchError> {
        let char_offset = CharOffset {
            path: path.to_string(),
            chars: util::file_as_string(&(path.to_string()+".char_offsets.chars"))?.lines().collect::<Vec<_>>().iter().map(|el| el.to_string()).collect(), // @Cleanup // @Temporary  sinlge  collect
            byte_offsets_start: util::load_index_64(&(path.to_string()+".char_offsets.byteOffsetsStart"))?,
            byte_offsets_end: util::load_index_64(&(path.to_string()+".char_offsets.byteOffsetsEnd"))?,
            line_offsets: util::load_index_64(&(path.to_string()+".char_offsets.lineOffset"))?
        };
        trace!("Loaded CharOffset:{} ", path );
        trace!("{:?}", char_offset);
        Ok(char_offset)
    }
    fn get_char_offset_info(&self,character: &str) -> Result<OffsetInfo, usize>{
        match self.chars.binary_search(&character.to_string()) {
            Ok(index) => Ok(self.get_offset_info(index)),
            Err(nearest_index) => Ok(self.get_offset_info(nearest_index-1)),
        }
        // let char_index = self.chars.binary_search(&character.to_string()).unwrap(); // .unwrap() -> find closest offset
        // Ok(self.get_offset_info(char_index))
        // self.chars.binary_search(&character) { Ok(char_index) => this.get_offset_info(char_index),Err(_) => };
    }
    fn get_offset_info(&self, index: usize) -> OffsetInfo {
        trace!("get_offset_info path:{}\tindex:{}\toffsetSize: {}", self.path, index, self.byte_offsets_start.len());
        return OffsetInfo{byte_range_start: self.byte_offsets_start[index], byte_range_end: self.byte_offsets_end[index], line_offset: self.line_offsets[index]};
    }

}


//todo use cache
fn get_create_char_offset_info(folder:&str, path: &str,character: &str) -> Result<Option<OffsetInfo>, SearchError> { // @Temporary 
    let char_offset = CharOffset::new(&get_file_path(folder, &path, ""))?;
    return Ok(char_offset.get_char_offset_info(character).ok());
}

fn get_default_score(term1: &str, term2: &str) -> f32{
    return 2.0/(distance(term1, term2) as f32 + 0.2 )
}
fn get_default_score2(distance: u32) -> f32{
    return 2.0/(distance as f32 + 0.2 )
}

fn get_hits_in_field(folder:&str, mut options: &mut RequestSearchPart, term: &str) -> Result<FnvHashMap<u32, f32>, SearchError> {
    let mut hits:FnvHashMap<u32, f32> = FnvHashMap::default();

    // let checks:Vec<Fn(&str) -> bool> = Vec::new();
    let term_chars = term.chars().collect::<Vec<char>>();
    // options.first_char_exact_match = options.exact || options.levenshtein_distance == 0 || options.starts_with.is_some(); // TODO fix

    if options.levenshtein_distance == 0 {
        options.exact = Some(true);
    }

    let start_char = if options.exact.unwrap_or(false) || options.levenshtein_distance == 0 || options.starts_with.is_some() && term_chars.len() >= 2 {
        Some(term_chars[0].to_string() + &term_chars[1].to_string())
    }
    else if options.first_char_exact_match.unwrap_or(false) { Some(term_chars[0].to_string() )
    }
    else { None };

    let value = start_char.as_ref().map(String::as_ref);

    debug!("Will Check distance {:?}", options.levenshtein_distance != 0);
    debug!("Will Check exact {:?}", options.exact);
    debug!("Will Check starts_with {:?}", options.starts_with);
    {
        let teh_callback = |line: &str, line_pos: u32| {
            trace!("Checking {} with {}", line, term);
            let distance = if options.levenshtein_distance != 0 { Some(distance(term, line))} else { None };
            trace!("Exact match {}", (options.exact.unwrap_or(false) &&  line == term));
            trace!("Distance {}", (distance.is_some() && distance.unwrap() <= options.levenshtein_distance));
            trace!("starts_with {}", (options.starts_with.is_some() && line.starts_with(options.starts_with.as_ref().unwrap())));
            if (options.exact.unwrap_or(false) &&  line == term)
                || (distance.is_some() && distance.unwrap() <= options.levenshtein_distance)
                || (options.starts_with.is_some() && line.starts_with(options.starts_with.as_ref().unwrap())  )
                // || (options.customCompare.is_some() && options.customCompare.unwrap(line, term))
                {
                // let score = get_default_score(term, line);
                let score = if distance.is_some() {get_default_score2(distance.unwrap())} else {get_default_score(term, line)};
                trace!("Hit: {:?} score: {:?}", line, score);
                hits.insert(line_pos, score);
            }
        };
        let result = get_text_lines(folder, &options.path, value, teh_callback)?; // @Hack // @Cleanup // @FixMe Forward errors
        info!("{:?}", result); // TODO: Forward
    }
    trace!("hits: {:?}", hits);
    Ok(hits)

}

#[derive(Debug)]
struct IndexKeyValueStore {
    values1: Vec<u32>,
    values2: Vec<u32>,
}

impl IndexKeyValueStore {
    fn new(path1:&str, path2:&str) -> IndexKeyValueStore {
        IndexKeyValueStore { values1: util::load_index(path1).unwrap(), values2: util::load_index(path2).unwrap() }
    }
    fn get_value(&self, find: u32) -> Option<u32> {
        match self.values1.binary_search(&find) {
            Ok(pos) => { Some(self.values2[pos]) },
            Err(_) => {None},
        }
    }
    fn get_values(&self, find: u32) -> Vec<u32> {
        let mut result = Vec::new();
        match self.values1.binary_search(&find) {
            Ok(mut pos) => {
                let val_len = self.values1.len();
                while pos < val_len && self.values1[pos] == find{
                    result.push(self.values2[pos]);
                    pos+=1;
                }
            },Err(_) => {},
        }
        result
    }
}

trait TokensIndexKeyValueStore {
    fn new(path:&str) -> Self;
    fn get_parent_val_id(&self, find: u32) -> Option<u32>;
    fn get_parent_val_ids(&self, find: u32) -> Vec<u32>;
}

impl TokensIndexKeyValueStore for IndexKeyValueStore {
    fn new(path:&str) -> Self {
        IndexKeyValueStore { values1: util::load_index(&(path.to_string()+".tokens.tokenValIds")).unwrap(), values2: util::load_index(&(path.to_string()+".tokens.parentValId")).unwrap() }
    }
    fn get_parent_val_id(&self, find: u32) -> Option<u32>{  return self.get_value(find); }
    fn get_parent_val_ids(&self, find: u32) -> Vec<u32>{ return self.get_values(find); }
}


fn add_token_results(folder:&str, path:&str, hits: &mut FnvHashMap<u32, f32>){
    let complete_path = &get_file_path(folder, &path, ".tokens.parentValId");
    let has_tokens = fs::metadata(&complete_path);
    debug!("has_tokens {:?} {:?}", complete_path, has_tokens.is_ok());
    if has_tokens.is_err() { return; }

    // var hrstart = process.hrtime()
    let token_kvdata: IndexKeyValueStore = TokensIndexKeyValueStore::new(&get_file_path(folder, &path, ""));
    let value_lengths = util::load_index(&get_file_path(folder, &path, ".length")).unwrap();

    let mut token_hits:FnvHashMap<u32, f32> = FnvHashMap::default();
    for value_id in hits.keys() {
        let parent_ids_for_token = token_kvdata.get_parent_val_ids(*value_id);
        // trace!("value_id {:?}", value_id);
        // trace!("parent_ids_for_token {:?}", parent_ids_for_token);
        if parent_ids_for_token.len() > 0 {
            for token_parentval_id in parent_ids_for_token {
                let parent_text_length = value_lengths[token_parentval_id as usize];
                let token_text_length = value_lengths[*value_id as usize];
                let adjusted_score = 2.0/(parent_text_length as f32 - token_text_length as f32) + 0.2;
                // if (adjusted_score < 0) throw new Error('asdf')

                let the_score = token_hits.entry(token_parentval_id as u32)
                    .or_insert(*hits.get(&token_parentval_id).unwrap_or(&0.0));
                *the_score += adjusted_score;
            }
        }
    }
    hits.extend(token_hits);
}


#[inline(always)]
fn get_text_lines<F>(folder:&str, path: &str, character: Option<&str>, mut fun: F) -> Result<(), SearchError>
where F: FnMut(&str, u32) {

    if character.is_some() {
        trace!("Search CharOffset for: {:?}", character.unwrap());
        println!("{:?}", get_create_char_offset_info(folder, path, character.unwrap()));
        let char_offset_info_opt = get_create_char_offset_info(folder, path, character.unwrap())?;
        trace!("CharOffset: {:?}", char_offset_info_opt);
        if char_offset_info_opt.is_none() {
            return Ok(())
        }
        let mut char_offset_info = char_offset_info_opt.unwrap();
        let mut f = File::open(&get_file_path(folder, &path, ""))?;
        let mut buffer:Vec<u8> = Vec::with_capacity((char_offset_info.byte_range_end - char_offset_info.byte_range_start) as usize);
        unsafe { buffer.set_len(char_offset_info.byte_range_end as usize - char_offset_info.byte_range_start as usize); }

        f.seek(SeekFrom::Start(char_offset_info.byte_range_start as u64))?;
        f.read_exact(&mut buffer)?;
        // let s = unsafe {str::from_utf8_unchecked(&buffer)}; 
        let s = str::from_utf8(&buffer)?; // @Temporary  -> use unchecked if stable
        // trace!("Loaded Text: {}", s);
        let lines = s.lines();
        for line in lines{
            fun(&line, char_offset_info.line_offset as u32);
            char_offset_info.line_offset += 1
        }

    }else{
        let mut f = File::open(&get_file_path(folder, &path, ""))?;
        let mut s = String::new();
        f.read_to_string(&mut s)?;
        let lines = s.lines();
        for (line_pos, line) in lines.enumerate(){
            fun(&line, line_pos as u32)
        }
    }

    Ok(())
}


pub fn test_levenshtein(term:&str, max_distance:u32) -> Result<(Vec<String>), io::Error> {

    use std::time::SystemTime;
    
    let mut f = try!(File::open("de_full_2.txt"));
    let mut s = String::new();
    try!(f.read_to_string(&mut s));

    let now = SystemTime::now();

    let lines = s.lines();
    let mut hits = vec![];
    for line in lines{
        let distance = distance(term, line);
        if distance < max_distance {
            hits.push(line.to_string())
        }
    }
    
    let ms = match now.elapsed() {
        Ok(elapsed) => {(elapsed.as_secs() as f64) * 1_000.0 + (elapsed.subsec_nanos() as f64 / 1000_000.0)}
        Err(_e) => {-1.0}
    };

    let lines_checked = s.lines().count() as f64;
    println!("levenshtein ms: {}", ms);
    println!("Lines : {}", lines_checked );
    let ms_per_1000 = ((ms as f64) / lines_checked) * 1000.0;
    println!("ms per 1000 lookups: {}", ms_per_1000);
    Ok((hits))

}


fn distance(s1: &str, s2: &str) -> u32 {
    let len_s1 = s1.chars().count();

    let mut column: Vec<u32> = Vec::with_capacity(len_s1+1);
    unsafe { column.set_len(len_s1+1); }
    for x in 0..len_s1+1 {
        column[x] = x as u32;
    }

    for (x, current_char2) in s2.chars().enumerate() {
        column[0] = x as u32  + 1;
        let mut lastdiag = x as u32;
        for (y, current_char1) in s1.chars().enumerate() {
            if current_char1 != current_char2 { lastdiag+=1 }
            let olddiag = column[y+1];
            column[y+1] = cmp::min(column[y+1]+1, cmp::min(column[y]+1, lastdiag));
            lastdiag = olddiag;

        }
    }
    column[len_s1]

}

