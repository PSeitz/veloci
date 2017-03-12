
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
use futures::stream::{iter, Peekable, BoxStream, channel, Stream};
#[allow(unused_imports)]
use futures::sync::oneshot;
#[allow(unused_imports)]
use futures::sync::mpsc;
use std::str;
#[allow(unused_imports)]
use std::thread;
use std::fmt;
#[allow(unused_imports)]
use std::sync::mpsc::sync_channel;
use std::fs;

// use std::os::windows::fs::FileExt;
use std::io::SeekFrom;
#[allow(unused_imports)]
use std::collections::HashMap;
use util;
#[allow(unused_imports)]
use std::collections::hash_map::Entry;
use fnv::FnvHashMap;


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



fn call_twice<A, F>(val: A, mut f: F)
where F: FnMut(A) {
    f(val);
}

fn main3() {

    

    let mut num = 5;
    // let plus_num = |x: i32| -> i32 {  x + num;}

    let plus_one_v2 = |x: i32| { println!("Its: {}", num); num +=x };

    // let x = 12;
    // let plus_one = |x: i32| x + 1;
    // fn double(x: i32) -> i32 {x + x};
    call_twice(10, plus_one_v2);

    // println!("Res is {}", call_twice(10, plus_one_v2));
    // println!("Res is {}", call_twice(10, |x| x + x));
}


pub fn main2() {
    let res = test_levenshtein("anschauen", 2);
    println!("{:?}", res);

    main3();
    // let stream = get_text_lines2();
    // let mut stream = get_text_lines2().fuse().wait();
    
    // println!("msg {}", stream.next());
    // stream.map(move |msg| {
    //     println!("msg {}", msg);

    //     // unfortunate workaround needed since `send()` takes `self`
    //     // let mut tx = tx_opt.take().unwrap();
    //     // tx = tx.send(msg.clone()).wait().unwrap();
    //     // tx_opt = Some(tx);
    //     // msg
    // });


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



    // let char_offsets = CharOffset::new("jmdict/meanings.ger[].text");

    // let kv = IndexKeyValueStore::new("jmdict/meanings.ger[].text.textindex.value_idToParent.val_ids", "jmdict/meanings.ger[].text.textindex.value_idToParent.mainIds");
    // println!("kv.get_value(100) {}", kv.get_value(100).unwrap());
    // println!("kv.values1[100] {}", kv.values1[100]);
    // println!("kv.values2[100] {}", kv.values2[100]);

    let x = vec![1,2,3,6,7,8];
    let u =  x.binary_search(&4).unwrap_err();;
    println!("{:?}", u);
    let value = match x.binary_search(&4) { Ok(value) => value,Err(value) => value};
    println!("mjjaaa {}", value);

    // util::load_index("jmdict/meanings.ger[].text.textindex.value_idToParent.val_ids");
    // util::load_index("index11");

    // let teh_callback = |x: &str| { println!("Its: {}", x); };
    // let start_char = "a";
    // get_text_lines("jmdict/meanings.ger[].text", Some(start_char), teh_callback) ;

    let options = SearchOptions{
        levenshtein_distance: 0,
        exact: true,
        first_char_exact_match: true,
        .. Default::default()
    };

    // let hits = get_hits_in_field("jmdict/meanings.ger[].text", &options, "haus");
    let search_part = RequestSearchPart{path: "jmdict/meanings.ger[].text".to_string(), term:"haus".to_string(), options:&options};
    let request = Request{search:&search_part, or:None, and:None};

    let res:Vec<Hit> = search(request, 0, 10);
    println!("{:?}", res[0].id);

}

pub struct Request<'b> {
    or : Option<Vec<RequestSearchPart<'b>>>,
    and : Option<Vec<RequestSearchPart<'b>>>,
    search: & 'b RequestSearchPart<'b>,
    // boost: Vec<RequestBoostPart<'b>>
}

pub struct RequestSearchPart<'b> {
    path: String,
    term: String,
    options: & 'b SearchOptions
}
// pub struct RequestBoostPart<'b> {
//     path: String,
//     boost_function:&'b Fn(f64) -> f32
//     // values2: Vec<u32>
// }


// pub enum CheckOperators {
//     All,
//     One
// }
// impl Default for CheckOperators {
//     fn default() -> CheckOperators { CheckOperators::All }
// }

#[derive(Default)]
struct SearchOptions {
    // checks: Vec<&Fn(&str) -> bool>
    // checks: Vec<fn(&str) -> bool>,
    // check_operator: CheckOperators,
    levenshtein_distance: u32,
    starts_with: Option<String>,
    exact: bool,
    first_char_exact_match: bool
    // customCompare:Option<&'b Fn(&str, &str) -> bool>
    // customScore:&Fn(&str) -> bool
}

#[derive(Debug, Clone, Copy)]
pub struct Hit {
    id: u32,
    score: f32
}

use std::cmp::Ordering;

fn hits_to_array(hits:FnvHashMap<u32, f32>) -> Vec<Hit> {
    let mut res:Vec<Hit> = hits.iter().map(|(id, score)| Hit{id:*id, score:*score}).collect();
    res.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal));
    res
}


pub fn search(request: Request, skip:u32, top:u32) -> Vec<Hit>{
    // request.skip = request.skip || 0
    // request.top = request.top || 10

    let res = hits_to_array(search_unrolled(request));
    // res.resize(skip + top, 0);
    res[skip as usize..top as usize].to_vec()
        // .then(res => res.slice(request.skip, request.top))
}

pub fn search_unrolled(request: Request) -> FnvHashMap<u32, f32>{
    if request.or.is_some() {
        // search_raw(request)
        // let request2 = request;
        request.or.unwrap().iter()
            .fold(FnvHashMap::default(), |mut acc, x| -> FnvHashMap<u32, f32> {
                let requesto = Request{search: x, or:None, and: None}; // TODO :BOOST
                acc.extend(search_raw(requesto));
                acc
            })
        // return Promise.all(request.or.map(req => search_unrolled(req)))
        // .then(results => results.reduce((p, c) => Object.assign(p, c)))
    }else if request.and.is_some(){
        search_raw(request)
        // return Promise.all(request.and.map(req => search_unrolled(req)))
        // .then(results => results
        //     .reduce((p, c) => intersection(p, c)
        //     .map(commonKey => ((p[commonKey].score > c[commonKey].score) ? p[commonKey] : c[commonKey]))))
    }else{
        search_raw(request)
    }
}


pub fn search_raw(request: Request) -> FnvHashMap<u32, f32> {

    let ref path = request.search.path;
    let term = util::normalize_text(&request.search.term);


    let mut hits = get_hits_in_field(&path, request.search.options, &term);
    add_token_results(&mut hits, &path);

    let mut next_level_hits:FnvHashMap<u32, f32> = FnvHashMap::default();

    let paths = util::get_steps_to_anchor(&path);
    for i in (paths.len()-1)..0 {
        let ref path = paths[i];
        let is_last = i == (paths.len() -1);

        let kv_store = IndexKeyValueStore::new(&(path.to_string()+".value_idToParent.val_ids"), &(path.to_string()+".value_idToParent.mainIds"));
        for (value_id, score) in &hits {
            let values = kv_store.get_values(*value_id);
            for parent_val_id in values {
                match next_level_hits.entry(parent_val_id as u32) {
                    Vacant(entry) => {entry.insert(*score);},
                    Occupied(entry) => { *entry.into_mut() = score.max(*entry.get()) + 0.1;},
                }
            }
        }
        hits = next_level_hits;
        next_level_hits = FnvHashMap::default();
    }

    next_level_hits
}

// struct ByteRange {
//     byte_offsets_start: u32,
//     byte_offsets_end: u32,
// }
struct OffsetInfo {
    byte_range_start: u32,
    byte_range_end: u32,
    line_offset: u32,
}

struct CharOffset {
    chars: Vec<String>,
    byte_offsets_start: Vec<u32>,
    byte_offsets_end: Vec<u32>,
    line_offsets: Vec<u32>,
}

// impl fmt::Debug for OffsetInfo {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         writeln!(f, "byte_range_start: {}", self.byte_range_start);
//         writeln!(f, "byte_range_end: {}", self.byte_range_end);
//         writeln!(f, "line_offset: {}", self.line_offset)
//     }
// }

impl CharOffset {
    fn new(path:&str) -> CharOffset {
        CharOffset {
            chars: serde_json::from_str(&file_as_string(&(path.to_string()+".char_offsets.chars"))).unwrap(),
            byte_offsets_start: util::load_index(&(path.to_string()+".char_offsets.byte_offsets_start")).unwrap(),
            byte_offsets_end: util::load_index(&(path.to_string()+".char_offsets.byte_offsets_end")).unwrap(),
            line_offsets: util::load_index(&(path.to_string()+".char_offsets.line_offset")).unwrap()
        }
    }

    // fn getClosestOffset(&self, line_pos: u32) -> OffsetInfo {
    //     let line_offset = match self.line_offsets.binary_search(&line_pos){ Ok(value) => value,Err(value) => value};
    //     return self.get_offset_info(line_offset);
    // }
    fn get_char_offset_info(&self,character: &str) -> Result<OffsetInfo, usize>{
        let char_index = try!(self.chars.binary_search(&character.to_string()));
        return Ok(self.get_offset_info(char_index))
        // self.chars.binary_search(&character) { Ok(char_index) => this.get_offset_info(char_index),Err(_) => };
    }
    fn get_offset_info(&self, index: usize) -> OffsetInfo {
        // let byteRange = {start: this.byte_offsets_start[index], end:this.byte_offsets_end[index]-1}; // -1 For the linebreak
        return OffsetInfo{byte_range_start: self.byte_offsets_start[index], byte_range_end: self.byte_offsets_end[index]-1, line_offset: self.line_offsets[index]}; // -1 For the linebreak
    }

}


//todo use cache
fn get_create_char_offset_info(path: &str,character: &str) -> Result<OffsetInfo, usize> {
    let char_offset = CharOffset::new(path);
    return char_offset.get_char_offset_info(character);
    // char_offsetCache[path] = char_offsetCache[path] || new CharOffset(path)
    // return char_offsetCache[path]
}

// fn levenshteinCheck(text: &str, term: &str) -> bool {
//     distance(text, term) <=
// }

fn get_default_score(term1: &str, term2: &str) -> f32{
    return 2.0/(distance(term1, term2) as f32 + 0.2 )
}
fn get_default_score2(distance: u32) -> f32{
    return 2.0/(distance as f32 + 0.2 )
}

fn get_hits_in_field(path: &str, options: &SearchOptions, term: &str) -> FnvHashMap<u32, f32> {
    let mut hits:FnvHashMap<u32, f32> = FnvHashMap::default();
    // let mut hits:HashMap<u32, f32> = HashMap::new(); // id:score

    // let checks:Vec<Fn(&str) -> bool> = Vec::new();
    let term_chars = term.chars().collect::<Vec<char>>();

    // options.first_char_exact_match = options.exact || options.levenshtein_distance == 0 || options.starts_with.is_some(); // TODO fix

    let start_char = if options.exact || options.levenshtein_distance == 0 || options.starts_with.is_some() && term_chars.len() >= 2 {
        Some(term_chars[0].to_string() + &term_chars[1].to_string())
    }
    else if options.first_char_exact_match {
        Some(term_chars[0].to_string())
    }
    else {
        None
    };

    let value = start_char.as_ref().map(String::as_ref);

    {
        let teh_callback = |line: &str, line_pos: u32| {
            let distance = if options.levenshtein_distance != 0 { Some(distance(term, line))} else { None };
            if (options.exact &&  line == term)
                || (distance.is_some() && distance.unwrap() >= options.levenshtein_distance)
                || (options.starts_with.is_some() && line.starts_with(options.starts_with.as_ref().unwrap())  )
                // || (options.customCompare.is_some() && options.customCompare.unwrap(line, term))
                {
                // let score = get_default_score(term, line);
                let score = if distance.is_some() {get_default_score2(distance.unwrap())} else {get_default_score(term, line)};
                hits.insert(line_pos, score);
            }
        };
        get_text_lines(path, value, teh_callback);
    }
    hits

}


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
            Ok(value) => { Some(self.values2[value]) },
            Err(_) => {None},
        }
    }
    fn get_values(&self, find: u32) -> Vec<u32> {
        let mut result = Vec::new();
        match self.values1.binary_search(&find) {
            Ok(value) => {
                result.push(self.values2[value]);
                let mut i = value;
                while self.values1[i] == find{
                    result.push(self.values2[i]);
                    i+=1;
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
        IndexKeyValueStore { values1: util::load_index(&(path.to_string()+".tokens.tokenValIds")).unwrap(), values2: util::load_index(&(path.to_string()+".tokens.parent_val_id")).unwrap() }
    }
    fn get_parent_val_id(&self, find: u32) -> Option<u32>{  return self.get_value(find); }
    fn get_parent_val_ids(&self, find: u32) -> Vec<u32>{ return self.get_values(find); }
}


fn add_token_results(hits: &mut FnvHashMap<u32, f32>, path:&str){

    let has_tokens = fs::metadata("/some/file/path.txt");
    if has_tokens.is_err() {
        return;
    }

    // var hrstart = process.hrtime()
    let token_kvdata: IndexKeyValueStore = TokensIndexKeyValueStore::new(path);
    let value_lengths = util::load_index(&(path.to_string()+".length")).unwrap();

    let mut token_hits:FnvHashMap<u32, f32> = FnvHashMap::default();
    for value_id in hits.keys() {
        let parent_ids_for_token = token_kvdata.get_parent_val_ids(*value_id);
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
fn get_text_lines<F>(path: &str,character: Option<&str>, mut fun: F)
where F: FnMut(&str, u32) {

    let char_offset_info_opt = if character.is_some() { Some(get_create_char_offset_info(path, character.unwrap())) } else { None };
    if char_offset_info_opt.is_some() {
        let mut char_offset_info = char_offset_info_opt.unwrap().unwrap();
        let mut f = File::open(path).unwrap();
        let mut buffer:Vec<u8> = Vec::with_capacity((char_offset_info.byte_range_end - char_offset_info.byte_range_start) as usize);
        unsafe { buffer.set_len(char_offset_info.byte_range_end as usize - char_offset_info.byte_range_start as usize); }

        f.seek(SeekFrom::Start(char_offset_info.byte_range_start as u64));
        f.read_exact(&mut buffer).unwrap();
        let s = unsafe {str::from_utf8_unchecked(&buffer)};

        let lines = s.lines();
        for line in lines{
            fun(&line, char_offset_info.line_offset as u32);
            char_offset_info.line_offset += 1
        }

    }else{
        let mut f = File::open(path).unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();

        let lines = s.lines();

        for (line_pos, line) in lines.enumerate(){
            fun(&line, line_pos as u32)
        }
    }
}



fn file_as_string(path:&str) -> String {
    let mut file = File::open(path).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    contents
}

fn test_levenshtein(term:&str, max_distance:u32) -> Result<(), io::Error> {

    use std::time::SystemTime;
    let now = SystemTime::now();

    let mut f = try!(File::open("de_full_2.txt"));

    let mut s = String::new();
    try!(f.read_to_string(&mut s));

    let lines = s.lines();
    let mut hits = vec![];
    for line in lines{
        let distance = distance(term, line);
        if distance < max_distance {
            hits.push(line)
        }
    }
    
    let ms = match now.elapsed() {
        Ok(elapsed) => {(elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000.0)}
        Err(_e) => {-1.0}
    };

    let lines_checked = s.lines().count() as f64;
    println!("levenshtein ms: {}", ms);
    println!("Lines : {}", lines_checked );
    let ms_per_1000 = ((ms as f64) / lines_checked) * 1000.0;
    println!("ms per 1000 lookups: {}", ms_per_1000);
    Ok(())

}


fn distance(s1: &str, s2: &str) -> u32 {
    // if s1.len() > s2.len(){
    //     return distance(s2, s1);
    // }

    let len_s1 = s1.chars().count();
    // let len_s2 = s2.chars().count();

    // let s1chars_vec = s1.chars().collect::<Vec<char>>();
    // let s2chars_vec = s2.chars().collect::<Vec<char>>();

    // let len_s1 = s1chars_vec.len();
    // let len_s2 = s2chars_vec.len();

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



