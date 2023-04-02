use crate::{persistence::*, type_info::TypeInfo};
use half::f16;
use std::path::Path;

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
