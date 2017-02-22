
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufRead};
// use std::io::Error;
use std::path::Path;
use std::char;
use std::cmp;
use std::mem;
use serde_json;
use std::time::Duration;

use futures_cpupool::CpuPool;
use tokio_timer::Timer;

use futures::{Poll, Future, Sink};
use futures::executor;
use futures::future::{ok, err};
use futures::stream::{iter, Peekable, BoxStream, channel, Stream};
use futures::sync::oneshot;
use futures::sync::mpsc;
use std::str;
use std::thread;
use std::fmt;
use std::sync::mpsc::sync_channel;

// use std::os::windows::fs::FileExt;
use std::io::SeekFrom;
use std::collections::HashMap;

fn getTextLines2() -> BoxStream<String, io::Error> {
    let (mut tx, rx) = channel();
    thread::spawn(move || {
        for msg in &["one", "two", "three", "four"] {
            thread::sleep(Duration::from_millis(500));
            tx = tx.send(Ok(msg.to_string())).wait().unwrap();
        }
        // tx.send("line");
        // let x = vec!["asdf", "asddddd"];
        // for line in x {
        //      tx.send(line).wait() {
        //         Ok(s) => tx = s,
        //         Err(_) => break,
        //     }
        // }

    });
    return rx.boxed();
}

fn stdin() -> BoxStream<String, io::Error> {
    let (mut tx, rx) = channel();
    thread::spawn(move || {
        let input = io::stdin();
        for line in input.lock().lines() {
            match tx.send(line).wait() {
                Ok(s) => tx = s,
                Err(_) => break,
            }
        }
    });
    return rx.boxed();
}


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
    main3();
    // let stream = getTextLines2();
    // let mut stream = getTextLines2().fuse().wait();
    
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

    // println!("distance(jaa, jaar){}", distance("jaa", "jaar"));
    // println!("distance(jaa, naar){}", distance("jaa", "naar"));
    // println!("distance(jaa, m){}", distance("jaa", "m"));
    // println!("distance(m, jaa){}", distance("m", "jaa"));

    // println!("distance(j, craaa){}", distance("j", "craaa"));
    use std::time::SystemTime;
    let now = SystemTime::now();

    // let path = "jmdict/meanings.ger[].text";
    // let test = file_as_string(&(path.to_string()+".charOffsets.chars"));
    // test_levenshtein();

    let charOffsets = CharOffset::new("jmdict/meanings.ger[].text");

    // let kv = IndexKeyValueStore::new("jmdict/meanings.ger[].text.textindex.valueIdToParent.valIds", "jmdict/meanings.ger[].text.textindex.valueIdToParent.mainIds");
    // println!("kv.getValue(100) {}", kv.getValue(100).unwrap());
    // println!("kv.values1[100] {}", kv.values1[100]);
    // println!("kv.values2[100] {}", kv.values2[100]);
    

    let x = vec![1,2,3,6,7,8];
    let u =  x.binary_search(&4).unwrap_err();;
    println!("{:?}", u);

    let value = match x.binary_search(&4) { Ok(value) => value,Err(value) => value};

    println!("mjjaaa {}", value);

    // load_index("jmdict/meanings.ger[].text.textindex.valueIdToParent.valIds");
    // load_index("index11");

    let tehCallback = |x: &str| { println!("Its: {}", x); };

    let startChar = "a";
    // getTextLines("jmdict/meanings.ger[].text", Some(startChar), tehCallback) ;


}

struct IndexKeyValueStore {
    values1: Vec<u32>,
    values2: Vec<u32>,
}

impl IndexKeyValueStore {
    fn new(path1:&str, path2:&str) -> IndexKeyValueStore {
        IndexKeyValueStore { values1: load_index(path1).unwrap(), values2: load_index(path2).unwrap() }
    }
    fn getValue(&self, find: u32) -> Option<u32> {
        match self.values1.binary_search(&find) {
            Ok(value) => { Some(self.values2[value]) },
            Err(_) => {None},
        }
    }
    fn getValues(&self, find: u32) -> Vec<u32> {
        let mut result = Vec::new();
        match self.values1.binary_search(&find) {
            Ok(value) => {
                result.push(self.values2[value]);
                let mut i = value;
                while(self.values1[i] == find){
                    result.push(self.values2[i]);
                    i+=1;
                }
            },Err(_) => {},
        }
        result
    }
}


// struct ByteRange {
//     byteOffsetsStart: u32,
//     byteOffsetsEnd: u32,
// }
struct OffsetInfo {
    byteRangeStart: u32,
    byteRangeEnd: u32,
    lineOffset: u32,
}

struct CharOffset {
    chars: Vec<String>,
    byteOffsetsStart: Vec<u32>,
    byteOffsetsEnd: Vec<u32>,
    lineOffsets: Vec<u32>,
}

impl fmt::Debug for OffsetInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "byteRangeStart: {}", self.byteRangeStart);
        writeln!(f, "byteRangeEnd: {}", self.byteRangeEnd);
        writeln!(f, "lineOffset: {}", self.lineOffset)
    }
}

impl CharOffset {
    fn new(path:&str) -> CharOffset {
        CharOffset { 
            chars: serde_json::from_str(&file_as_string(&(path.to_string()+".charOffsets.chars"))).unwrap(), 
            byteOffsetsStart: load_index(&(path.to_string()+".charOffsets.byteOffsetsStart")).unwrap(),
            byteOffsetsEnd: load_index(&(path.to_string()+".charOffsets.byteOffsetsEnd")).unwrap(),
            lineOffsets: load_index(&(path.to_string()+".charOffsets.lineOffset")).unwrap() 
        }
    }

    // fn getClosestOffset(&self, linePos: u32) -> OffsetInfo {
    //     let lineOffset = match self.lineOffsets.binary_search(&linePos){ Ok(value) => value,Err(value) => value};
    //     return self.getOffsetInfo(lineOffset);
    // }
    fn getCharOffsetInfo(&self,character: &str) -> Result<OffsetInfo, usize>{
        let charIndex = try!(self.chars.binary_search(&character.to_string()));
        return Ok(self.getOffsetInfo(charIndex))
        // self.chars.binary_search(&character) { Ok(charIndex) => this.getOffsetInfo(charIndex),Err(_) => };
    }
    fn getOffsetInfo(&self, index: usize) -> OffsetInfo {
        // let byteRange = {start: this.byteOffsetsStart[index], end:this.byteOffsetsEnd[index]-1}; // -1 For the linebreak
        return OffsetInfo{byteRangeStart: self.byteOffsetsStart[index], byteRangeEnd: self.byteOffsetsEnd[index]-1, lineOffset: self.lineOffsets[index]}; // -1 For the linebreak
    }

}



fn call_twice2<A, F>(val: A, mut f: F)
where F: FnMut(&str) {
    f("val");
}

fn main4() {
    let mut num = 5;
    // let plus_num = |x: i32| -> i32 {  x + num;}

    let plus_one_v2 = |x: &str| { println!("Its: {}", x); };

    // let x = 12;
    // let plus_one = |x: i32| x + 1;
    // fn double(x: i32) -> i32 {x + x};
    call_twice2(10, plus_one_v2);

    // println!("Res is {}", call_twice(10, plus_one_v2));
    // println!("Res is {}", call_twice(10, |x| x + x));
}

enum CheckOperators {
    All,
    One
}
struct SearchOptions<'b, 'a> {
    // checks: Vec<&Fn(&str) -> bool>
    // checks: Vec<fn(&str) -> bool>,
    checkOperator: CheckOperators,
    levenshtein_distance: u32,
    startsWith: Option<&'a str>,
    exact: bool,
    firstCharExactMatch: bool,
    customCompare:Option<&'b Fn(&str, &str) -> bool>
    // customScore:&Fn(&str) -> bool
}

//todo use cache
fn getCreateCharOffsetInfo(path: &str,character: &str) -> Result<OffsetInfo, usize> {
    let charOffset = CharOffset::new(path);
    return charOffset.getCharOffsetInfo(character);
    // charOffsetCache[path] = charOffsetCache[path] || new CharOffset(path)
    // return charOffsetCache[path]
}

struct Hit{
    id: u32,
    score: f32
}
// fn levenshteinCheck(text: &str, term: &str) -> bool {
//     distance(text, term) <=
// }

fn getDefaultScore(term1: &str, term2: &str) -> f32{
    return 2.0/(distance(term1, term2) as f32 + 0.2 )
}
fn getDefaultScore2(distance: u32) -> f32{
    return 2.0/(distance as f32 + 0.2 )
}

fn getHitsInField(path: &str, mut options: SearchOptions, term: &str)/* -> HashMap<u32, f32>*/ {
    let mut hits:HashMap<u32, f32> = HashMap::new(); // id:score

    // let checks:Vec<Fn(&str) -> bool> = Vec::new();

    let term_chars = term.chars().collect::<Vec<char>>();

    options.firstCharExactMatch = options.exact || options.levenshtein_distance == 0 || options.startsWith.is_some();

    let startChar = if options.exact || options.levenshtein_distance == 0 || options.startsWith.is_some() && term_chars.len() >= 2 {
        Some(term_chars[0].to_string() + &term_chars[1].to_string())
    }
    else if options.firstCharExactMatch {
        Some(term_chars[0].to_string())
    }
    else {
        None
    };

    let value = startChar.as_ref().map(String::as_ref);

    let tehCallback = |line: &str| {

        let distance = if options.levenshtein_distance != 0 { Some(distance(term, line))} else { None };

        if (options.exact &&  line == term)
            || (distance.is_some() && distance.unwrap() >= options.levenshtein_distance)
            || (options.startsWith.is_some() && line.starts_with(options.startsWith.unwrap())  )
            // || (options.customCompare.is_some() && options.customCompare.unwrap(line, term))
            {
            // let score = getDefaultScore(term, line);
            let score = if distance.is_some() {getDefaultScore2(distance.unwrap())} else {getDefaultScore(term, line)};
            hits.insert(10, score);
        }
    };

    getTextLines(path, value, tehCallback);
    // hits

}


#[inline(always)]
fn getTextLines<F>(path: &str,character: Option<&str>, mut fun: F)
where F: FnMut(&str) {

    let charOffsetInfoOpt = if character.is_some() { Some(getCreateCharOffsetInfo(path, character.unwrap())) } else { None };
    if charOffsetInfoOpt.is_some() {
        let charOffsetInfo = charOffsetInfoOpt.unwrap().unwrap();
        let mut f = File::open(path).unwrap();
        let mut buffer:Vec<u8> = Vec::with_capacity((charOffsetInfo.byteRangeEnd - charOffsetInfo.byteRangeStart) as usize);
        unsafe { buffer.set_len(charOffsetInfo.byteRangeEnd as usize - charOffsetInfo.byteRangeStart as usize); }

        f.seek(SeekFrom::Start(charOffsetInfo.byteRangeStart as u64));
        f.read_exact(&mut buffer).unwrap();
        let s = unsafe {str::from_utf8_unchecked(&buffer)};

        let lines = s.lines();
        for line in lines{
            fun(&line)
        }

    }else{
        let mut f = File::open(path).unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();

        let lines = s.lines();
        for line in lines{
            fun(&line)
        }
    }
}

fn load_index(s1: &str) -> Result<(Vec<u32>), io::Error> {
    let mut f = try!(File::open(s1));
    let mut buffer = Vec::new();
    try!(f.read_to_end(&mut buffer));
    buffer.shrink_to_fit();
    let buf_len = buffer.len();

    let mut read: Vec<u32> = unsafe { mem::transmute(buffer) };
    unsafe { read.set_len(buf_len/4); }
    // println!("100: {}", data[100]);
    Ok(read)
    // let v_from_raw = unsafe {
    // Vec::from_raw_parts(buffer.as_mut_ptr(),
    //                     buffer.len(),
    //                     buffer.capacity())
    // };
    // println!("100: {}", v_from_raw[100]);


}

fn file_as_string(path:&str) -> String {
    let mut file = File::open(path).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    contents
}

fn test_levenshtein() -> Result<(), io::Error> {

    use std::time::SystemTime;
    let now = SystemTime::now();

    let mut f = try!(File::open("words.txt"));

    let mut s = String::new();
    try!(f.read_to_string(&mut s));

    let lines = s.lines();

    for line in lines{
        let distance = distance("test123", line);
    }
    
    let ms = match now.elapsed() {
        Ok(elapsed) => {(elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000.0)}
        Err(_e) => {-1.0}
    };

    let lines_checked = s.lines().count() as f64;
    println!("levenshtein ms: {}", ms);
    println!("Lines : {}", lines_checked );
    let ms_per_1000 = ( ((ms as f64) / lines_checked) * 1000.0);
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
