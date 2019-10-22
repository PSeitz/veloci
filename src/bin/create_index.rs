#![recursion_limit = "128"]

use search_lib;

#[cfg(feature = "enable_cpuprofiler")]
extern crate cpuprofile;

use clap::{App, Arg};

use std::{fs::File, io::prelude::*};

fn main() {
    search_lib::trace::enable_log();

    let matches = App::new("Veloci Index Creator")
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
        })
        .unwrap_or_else(|| "{}".to_string());

    let file = matches.value_of("data").unwrap();
    let target = matches.value_of("target").unwrap();

    start_profiler("./create-prof.profile");
    search_lib::create::create_indices_from_file(&mut search_lib::persistence::Persistence::create(target.to_string()).unwrap(), file, &config, false).unwrap();
    stop_profiler();
}

#[cfg(not(enable_cpuprofiler))]
fn start_profiler(_: &str) {}
#[cfg(not(enable_cpuprofiler))]
fn stop_profiler() {}

#[cfg(feature = "enable_cpuprofiler")]
fn start_profiler(name: &str) {
    use cpuprofiler::PROFILER;
    PROFILER.lock().unwrap().start(name).unwrap();
}

#[cfg(feature = "enable_cpuprofiler")]
fn stop_profiler() {
    use cpuprofiler::PROFILER;
    PROFILER.lock().unwrap().stop().unwrap();
}
