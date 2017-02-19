#[macro_use]
extern crate serde_json;

extern crate serde;
extern crate rand;
extern crate futures;
extern crate futures_cpupool;
extern crate tokio_timer;
// extern crate rustc_serialize;
mod search;

fn main() {
	
    println!("Hello, world!");

    search::main2();
}
