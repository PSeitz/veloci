#![recursion_limit = "128"]

extern crate flexi_logger;
extern crate search_lib;

use std::fs::File;
use std::io::prelude::*;

fn main() {
    search_lib::trace::enable_log();
    std::env::args().nth(1).expect("require command line parameter");

    let args: Vec<_> = std::env::args().collect();

    let file = &args[1];
    let target = &args[2];

    let indices:String = if let Some(path) = args.get(3) {
        let mut f = File::open(path).expect("file not found");

        let mut contents = String::new();
        f.read_to_string(&mut contents)
            .expect("something went wrong reading the file");
        contents
    }else{
        "{}".to_string()
    };

    search_lib::create::create_indices_from_file(
        &mut search_lib::persistence::Persistence::create(target.to_string()).unwrap(),
        file,
        &indices,
        None,
        false,
    ).unwrap();

}
