use num::{self, cast::ToPrimitive};
use std::io::{self, Write};

pub mod direct;
pub mod indirect;
pub mod metadata;
pub mod persistence_data_binary_search;
pub mod persistence_score;

pub(crate) use direct::*;
pub(crate) use indirect::*;
pub(crate) use metadata::*;
pub(crate) use persistence_data_binary_search::*;
pub(crate) use persistence_score::*;

pub static EMPTY_BUCKET: u32 = 0;
pub static EMPTY_BUCKET_USIZE: usize = 0;
pub static VALUE_OFFSET: u32 = 1; // because 0 is reserved for EMPTY_BUCKET

pub(crate) fn calc_avg_join_size(num_values: u64, num_ids: u32) -> f32 {
    num_values as f32 / std::cmp::max(1, num_ids).to_f32().unwrap()
}

pub(crate) fn flush_to_file_indirect(indirect_path: &str, data_path: &str, indirect_data: &[u8], data: &[u8]) -> Result<(), io::Error> {
    let mut indirect = std::fs::OpenOptions::new().read(true).write(true).append(true).create(true).open(&indirect_path).unwrap();
    let mut data_cache = std::fs::OpenOptions::new().read(true).write(true).append(true).create(true).open(&data_path).unwrap();

    indirect.write_all(indirect_data)?;
    data_cache.write_all(data)?;

    Ok(())
}
