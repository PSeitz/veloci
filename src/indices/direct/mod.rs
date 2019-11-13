#[cfg(feature = "create")]
mod create_direct;
mod single_array_im;
mod single_array_mmap;

#[cfg(feature = "create")]
pub(crate) use create_direct::*;
pub(crate) use single_array_im::*;
pub(crate) use single_array_mmap::*;
pub(crate) use std::path::PathBuf;
