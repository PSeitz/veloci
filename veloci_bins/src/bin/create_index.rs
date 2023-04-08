#![recursion_limit = "128"]

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

    veloci::create::create_indices_from_file(&mut veloci::persistence::Persistence::create_mmap(matches.target).unwrap(), &matches.data, &config, false).unwrap();
}
