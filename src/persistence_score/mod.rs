use half::f16;
use persistence::*;
use std::fs;
use std::path::Path;
use type_info::TypeInfo;

use heapsize::HeapSizeOf;

use memmap::Mmap;
use memmap::MmapOptions;

pub mod token_to_anchor_score_vint;

pub(crate) use self::token_to_anchor_score_vint::*;

#[repr(packed)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AnchorScore {
    pub id: u32,
    pub score: f16,
}
impl AnchorScore {
    pub fn new(id: u32, score: f16) -> AnchorScore {
        AnchorScore { id, score }
    }
}

// #[repr(packed)]
// #[derive(Debug, Clone, Copy, PartialEq)]
// pub struct AnchorScoreSerialize {
//     pub id: u32,
//     pub score: u16,
// }

// impl AnchorScoreSerialize {
//     pub fn new(id: u32, score: u16) -> AnchorScoreSerialize {
//         AnchorScoreSerialize { id, score }
//     }
// }
