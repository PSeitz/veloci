use util::*;

use super::*;
use vint::vint_encode_most_common::*;

use std;
use std::fs::File;
use std::io;
use std::io::prelude::*;

use super::U31_MAX;
use itertools::Itertools;

impl_type_info!(TokenToAnchorScoreVint, TokenToAnchorScoreVintMmap);

#[derive(Serialize, Deserialize, Debug, Clone, Default, HeapSizeOf)]
pub struct TokenToAnchorScoreVint {
    pub start_pos: Vec<u32>,
    pub data: Vec<u8>,
}

pub fn get_serialized_most_common_encoded(data: &mut Vec<(u32, u32)>) -> Vec<u8> {
    let mut vint = VIntArrayEncodeMostCommon::default();

    let mut last = 0;
    for el in data.iter_mut() {
        let actual_val = el.0;
        el.0 -= last;
        last = actual_val;
    }

    let values: Vec<u32> = data.iter().flat_map(|(el1, el2)| vec![*el1, *el2]).collect();
    vint.encode_vals(&values);
    vint.serialize()
}

impl TokenToAnchorScoreVint {
    pub fn set_scores(&mut self, id: u32, mut add_data: Vec<(u32, u32)>) {
        //TODO INVALIDATE OLD DATA IF SET TWICE?

        let pos: usize = id as usize;
        let required_size = pos + 1;
        if self.start_pos.len() < required_size {
            self.start_pos.resize(required_size, U31_MAX);
        }

        let byte_offset = self.data.len() as u32;
        self.start_pos[pos] = byte_offset;

        // use mayda::{Encode, Monotone, Uniform};
        // let mut bits = Monotone::new();
        // bits.encode(&add_data.iter().map(|(el1, _)| *el1).collect::<Vec<u32>>()).unwrap();
        // let bytes = vec_to_bytes_u32(bits.storage());
        // self.data.extend(bytes);

        // let mut bits = Uniform::new();
        // bits.encode(&add_data.iter().map(|(_, el2)| *el2).collect::<Vec<u32>>()).unwrap();
        // let bytes = vec_to_bytes_u32(bits.storage());
        // self.data.extend(bytes);

        // let num_elements: [u8; 4] = unsafe { transmute(vint.data.len() as u32) };
        // self.data.extend(num_elements.iter());
        // self.data.extend(vint.data.iter());

        self.data.extend(get_serialized_most_common_encoded(&mut add_data));
    }

    #[inline]
    fn get_size(&self) -> usize {
        self.start_pos.len()
    }

    pub fn write<P: AsRef<Path> + std::fmt::Debug>(&self, path_indirect: P, path_data: P) -> Result<(), io::Error> {
        File::create(path_indirect)?.write_all(&vec_to_bytes_u32(&self.start_pos))?;
        File::create(path_data)?.write_all(&self.data)?;
        Ok(())
    }

    pub fn read<P: AsRef<Path> + std::fmt::Debug>(&mut self, path_indirect: P, path_data: P) -> Result<(), io::Error> {
        self.start_pos = load_index_u32(&path_indirect)?;
        self.data = file_to_bytes(&path_data)?;
        Ok(())
    }
}

#[inline]
fn recreate_vec(data: &[u8], pos: usize) -> Vec<AnchorScore> {
    let vint = VintArrayMostCommonIterator::from_slice(&data[pos..]);

    let mut current = 0;
    let data: Vec<AnchorScore> = vint.tuples()
        .map(|(mut id, score)| {
            id += current;
            current = id;
            AnchorScore::new(id, f16::from_f32(score as f32))
        })
        .collect();
    data
}

impl TokenToAnchorScore for TokenToAnchorScoreVint {
    #[inline]
    fn get_scores(&self, id: u32) -> Option<Vec<AnchorScore>> {
        if id as usize >= self.get_size() {
            return None;
        }

        let pos = self.start_pos[id as usize];
        if pos == U31_MAX {
            return None;
        }

        Some(recreate_vec(&self.data, pos as usize))
    }

    #[inline]
    fn get_max_id(&self) -> usize {
        //TODO REMOVE METHOD
        self.get_size()
    }
}

#[derive(Debug)]
pub struct TokenToAnchorScoreVintMmap {
    pub start_pos: Mmap,
    pub data: Mmap,
    pub max_value_id: u32,
}

impl TokenToAnchorScoreVintMmap {
    pub fn new(start_and_end_file: &fs::File, data_file: &fs::File) -> Self {
        let start_and_end_file = unsafe { MmapOptions::new().map(&start_and_end_file).unwrap() };
        let data_file = unsafe { MmapOptions::new().map(&data_file).unwrap() };
        TokenToAnchorScoreVintMmap {
            start_pos: start_and_end_file,
            data: data_file,
            max_value_id: 0,
        }
    }
}

impl HeapSizeOf for TokenToAnchorScoreVintMmap {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}

impl TokenToAnchorScore for TokenToAnchorScoreVintMmap {
    #[inline]
    fn get_scores(&self, id: u32) -> Option<Vec<AnchorScore>> {
        if id as usize >= self.start_pos.len() / 4 {
            return None;
        }
        let pos = get_u32_from_bytes(&self.start_pos, id as usize * 4);
        if pos == U31_MAX {
            return None;
        }
        Some(recreate_vec(&self.data, pos as usize))
    }

    #[inline]
    fn get_max_id(&self) -> usize {
        self.start_pos.len() / 4
    }
}

#[test]
fn test_token_to_anchor_score_vint() {
    use tempfile::tempdir;

    let mut yeps = TokenToAnchorScoreVint::default();

    yeps.set_scores(1, vec![(1, 1)]);

    assert_eq!(yeps.get_scores(0), None);
    assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0))]));
    assert_eq!(yeps.get_scores(2), None);

    yeps.set_scores(5, vec![(1, 1), (2, 3)]);
    assert_eq!(yeps.get_scores(4), None);
    assert_eq!(
        yeps.get_scores(5),
        Some(vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))])
    );
    assert_eq!(yeps.get_scores(6), None);

    let dir = tempdir().unwrap();
    let data = dir.path().join("TokenToAnchorScoreVintTestData");
    let indirect = dir.path().join("TokenToAnchorScoreVintTestIndirect");
    yeps.write(indirect.to_str().unwrap(), data.to_str().unwrap()).unwrap();

    // IM loaded from File
    let mut yeps = TokenToAnchorScoreVint::default();
    yeps.read(indirect.to_str().unwrap(), data.to_str().unwrap()).unwrap();
    assert_eq!(yeps.get_scores(0), None);
    assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0))]));
    assert_eq!(yeps.get_scores(2), None);

    assert_eq!(yeps.get_scores(4), None);
    assert_eq!(
        yeps.get_scores(5),
        Some(vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))])
    );
    assert_eq!(yeps.get_scores(6), None);

    // Mmap from File
    let start_and_end_file = File::open(indirect).unwrap();
    let data_file = File::open(data).unwrap();
    let yeps = TokenToAnchorScoreVintMmap::new(&start_and_end_file, &data_file);
    assert_eq!(yeps.get_scores(0), None);
    assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0))]));
    assert_eq!(yeps.get_scores(2), None);

    assert_eq!(yeps.get_scores(4), None);
    assert_eq!(
        yeps.get_scores(5),
        Some(vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))])
    );
    assert_eq!(yeps.get_scores(6), None);
}
