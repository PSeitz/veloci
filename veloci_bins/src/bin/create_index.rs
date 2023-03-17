#![recursion_limit = "128"]

#[cfg(feature = "enable_cpuprofiler")]
extern crate cpuprofile;

use argh::FromArgs;
use std::{fs::File, io::prelude::*};

#[derive(FromArgs)]
/// Veloci Index Creator
struct Opt {
    /// sets the input data file to use
    #[argh(option, short = 'd')]
    data: String,

    /// sets target folder, will be deleted first if it exist
    #[argh(option, short = 't')]
    target: String,

    /// path to config file
    #[argh(option, short = 'c')]
    config: Option<String>,
}

fn main() {
    veloci::trace::enable_log();

    let matches: Opt = argh::from_env();
    let config: String = matches
        .config
        .map(|path| {
            let mut f = File::open(path).expect("file not found");
            let mut contents = String::new();
            f.read_to_string(&mut contents).expect("something went wrong reading the file");
            contents
        })
        .unwrap_or_else(|| "{}".to_string());

    start_profiler("./create-prof.profile");
    veloci::create::create_indices_from_file(&mut veloci::persistence::Persistence::create(matches.target).unwrap(), &matches.data, &config, false).unwrap();
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
