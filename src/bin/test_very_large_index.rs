#![recursion_limit = "128"]

// #[macro_use]
// extern crate measure_time;

use search_lib;

#[allow(unused_imports)]
#[macro_use]
extern crate serde_json;

#[allow(unused_imports)]
use search_lib::*;
use std::io;

#[allow(unused_imports)]
use rayon::prelude::*;

use buffered_index_writer::BufferedIndexWriter;

fn main() -> Result<(), io::Error> {
    search_lib::trace::enable_log();
    let mut buffered_index_writer = BufferedIndexWriter::<u32, (u32, u32)>::new_unstable_sorted("./".to_string());

    for i in 0..40_000_000 {
        // Write a lot of data
        buffered_index_writer.add(i, (i, 50_000))?;
    }

    println!("{:?}", buffered_index_writer.bytes_written());

    search_lib::create::add_anchor_score_flush("test_u64", "check".to_string(), buffered_index_writer, &mut vec![]).unwrap();

    Ok(())
}
