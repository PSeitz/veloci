#![recursion_limit = "128"]

extern crate flexi_logger;
extern crate search_lib;
extern crate cpuprofiler;

extern crate clap;
use clap::{App, Arg};

use std::fs::File;
use std::io::prelude::*;

fn main() {
    search_lib::trace::enable_log();

    let matches = App::new("Index Creator")
        .version("1.0")
        .author("Pascal Seitz <pascal.seitz@gmail.com>")
        .about("creates an index from json data")
        .arg(Arg::with_name("data").short("d").help("Sets the input data file to use").required(true).takes_value(true))
        .arg(Arg::with_name("target").short("t").help("sets target folder").required(true).takes_value(true))
        .arg(Arg::with_name("config").short("c").long("config").help("Sets a custom config file").takes_value(true))
        .get_matches();

    let config: String = matches
        .value_of("config")
        .map(|path| {
            let mut f = File::open(path).expect("file not found");
            let mut contents = String::new();
            f.read_to_string(&mut contents).expect("something went wrong reading the file");
            contents
        }).unwrap_or_else(|| "{}".to_string());
    //     let mut f = File::open(path).expect("file not found");

    //     let mut contents = String::new();
    //     f.read_to_string(&mut contents)
    //         .expect("something went wrong reading the file");
    //     contents
    // }else{
    //     "{}".to_string()
    // };

    let file = matches.value_of("data").unwrap();
    let target = matches.value_of("target").unwrap();

    use cpuprofiler::PROFILER;
    PROFILER.lock().unwrap().start("./create-prof.profile").unwrap();
    search_lib::create::create_indices_from_file(&mut search_lib::persistence::Persistence::create(target.to_string()).unwrap(), file, &config, None, false).unwrap();
    PROFILER.lock().unwrap().stop().unwrap();
    // std::env::args().nth(1).expect("require command line parameter");

    // let args: Vec<_> = std::env::args().collect();

    // let file = &args[1];
    // let target = &args[2];

    // let config:String = if let Some(path) = args.get(3) {
    //     let mut f = File::open(path).expect("file not found");

    //     let mut contents = String::new();
    //     f.read_to_string(&mut contents)
    //         .expect("something went wrong reading the file");
    //     contents
    // }else{
    //     "{}".to_string()
    // };

    // search_lib::create::create_indices_from_file(
    //     &mut search_lib::persistence::Persistence::create(target.to_string()).unwrap(),
    //     file,
    //     &config,
    //     None,
    //     false,
    // ).unwrap();
}
