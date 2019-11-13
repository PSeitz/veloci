use memmap::{Mmap, MmapOptions};
use std::fs::File;

use crate::{error::VelociError, util::open_file};
use num::{self, cast::ToPrimitive};
use std::{
    io::{self, Write},
    path::Path,
};

pub mod direct;
#[macro_use]
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

pub(crate) fn flush_to_file_indirect<P: AsRef<Path>>(indirect_path: P, data_path: P, indirect_data: &[u8], data: &[u8]) -> Result<(), io::Error> {
    let mut indirect = std::fs::OpenOptions::new().read(true).write(true).append(true).create(true).open(&indirect_path).unwrap();
    let mut data_cache = std::fs::OpenOptions::new().read(true).write(true).append(true).create(true).open(&data_path).unwrap();

    indirect.write_all(indirect_data)?;
    data_cache.write_all(data)?;

    Ok(())
}

pub fn mmap_from_path<P: AsRef<Path>>(path: P) -> Result<Mmap, VelociError> {
    mmap_from_file(&open_file(path)?)
}

pub fn mmap_from_file(file: &File) -> Result<Mmap, VelociError> {
    Ok(unsafe { MmapOptions::new().map(file).unwrap() })
}
