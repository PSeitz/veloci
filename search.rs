
// use std::io::BufferedReader;
// use std::io::File;
// use std::from_str::from_str;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::char;
use std::cmp;
// This is the main function
fn main() {
    // The statements here will be executed when the compiled binary is called

    // Print text to the console
    println!("Hello World!");

    // let fname = "in.txt";
    // let path = Path::new("jmdict/meanings.ger[].text");
    // let mut file = BufferedReader::new(File::open(&path));

    // for line_iter in file.lines() {
    //     let line : ~str = match line_iter { Ok(x) => x, Err(e) => fail!(e) };
    //     // preprocess line for further processing, say split int chunks separated by spaces
    //     let chunks: ~[&str] = line.split_terminator(|c: char| c.is_whitespace()).collect();
    //     // then parse chunks
    //     let terms: ~[int] = vec::from_fn(nterms, |i: uint| parse_str::<int>(chunks[i+1]));
    // }

    let charo = 'a';
    
    println!("{}", charo.to_string());

    let path = Path::new("words.txt");
    let display = path.display();

    // Open the path in read-only mode, returns `io::Result<File>`
    let mut file = match File::open(&path) {
        Err(why) => panic!("couldn't open {}: {}", display, why.description()),
        Ok(file) => file,
    };

    // Read the file contents into a string, returns `io::Result<usize>`
    let mut s = String::new();
    // match file.read_to_string(&mut s) {
    //     Err(why) => panic!("couldn't read {}: {}", display,
    //                                                why.description()),
    //     Ok(_) => , //print!("{} contains:\n{}", display, s),
    // }

    file.read_to_string(&mut s);

    let split = s.lines();
    let vec = split.collect::<Vec<&str>>();
    println!("{}", vec[1000]);

    println!("distance(jaa, jaar){}", distance("jaa", "jaar"));
    println!("distance(jaa, naar){}", distance("jaa", "naar"));
    println!("distance(jaa, m){}", distance("jaa", "m"));
    // for word in &vec {
    //     println!("A reference to {}", i);
    // }

    // let mut iterator = vec.iter();
    // iterator.next();
    // for (index, value) in iterator.enumerate() {
    //     let distance = distance(vec[index-1], vec[index]);
    // }

    use std::time::SystemTime;
    let now = SystemTime::now();
    let end = vec.len();
    for index in 1..end{
        let distance = distance(vec[index-1], vec[index]);
    }
    
    let sec = match now.elapsed() {
        Ok(elapsed) => {(elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000_000.0)}
        Err(_e) => {-1.0}
    };
    println!("Seconds: {}", sec);

}


fn distance(s1: &str, s2: &str) -> i32 {
    let (mut cost, mut lastdiag, mut olddiag): (i32, i32, i32);

    // let mut s2chars = s2.chars();

    let len_s1 = s1.chars().count();
    // let len_s2 = s2.chars().count();

    // let s1chars_vec = s1.chars().collect::<Vec<char>>();
    // let s2chars_vec = s2.chars().collect::<Vec<char>>();

    // let len_s1 = s1chars_vec.len();
    // let len_s2 = s2chars_vec.len();

    // let len_s1 = s1.chars().count();
    // let len_s2 = s1.chars().count();

    let mut column: Vec<i32> = vec![0; len_s1+1]; 

    for x in 0..len_s1+1 {
        column[x] = x as i32;
    }

    for (x, currentChar2) in s2.chars().enumerate() {
    //     println!("index = {} and value = {}", index, value);
    // }

    // for x in 1..len_s2+1 {
        // let currentChar2 = s2chars.next().unwrap();
        let mut s1chars = s1.chars();
        column[0] = x as i32  + 1;
        lastdiag = (x as i32) ;
        for (y, currentChar1) in s1.chars().enumerate() {
        // for y in 1..len_s1+1 {
            // let currentChar1 = s1chars.next().unwrap();
            olddiag = column[y+1];
            cost = 0;
            // println!("currentChar1: {}", currentChar1);
            // println!("currentChar2: {}", currentChar2);
            if currentChar1 != currentChar2 {
                cost = 1
            }
            // if s1chars_vec[y-1] != s2chars_vec[x-1] {
            //     cost = 1
            // }

            column[y+1] = cmp::min(column[y+1]+1, cmp::min(column[y]+1, lastdiag+cost));
            // column[y+1] = mini(
            //     column[y+1]+1,
            //     column[y]+1,
            //     lastdiag+cost);
            lastdiag = olddiag;

        }
    }
    column[len_s1]

}


fn mini(a:i32, b:i32, c:i32) -> i32 {
    if a < b {
        if a < c {
            return a
        }
    } else {
        if b < c {
            return b
        }
    }
    return c
}
