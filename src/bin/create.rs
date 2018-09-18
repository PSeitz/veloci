#![recursion_limit = "128"]

extern crate flexi_logger;
extern crate search_lib;

fn main() {
    search_lib::trace::enable_log();
    std::env::args().nth(1).expect("require command line parameter");

    let args: Vec<_> = std::env::args().collect();

    let file = &args[1];
    let target = &args[2];

    search_lib::create::create_indices_from_file(
        &mut search_lib::persistence::Persistence::create(target.to_string()).unwrap(),
        file,
        "[]",
        None,
        false,
    ).unwrap();

}
