use util::*;

use super::*;
use vint::vint_encode_most_common::*;

use std;
use std::fs::File;
use std::io;
use std::io::prelude::*;

use super::U31_MAX;
use itertools::Itertools;

impl_type_info!(TokenToAnchorScoreVintDelta);
// impl_type_info!(TokenToAnchorScoreVintDelta, TokenToAnchorScoreVintDeltaMmap);

#[derive(Serialize, Deserialize, Debug, Clone, Default, HeapSizeOf)]
pub struct TokenToAnchorScoreVintDelta {
    pub start_pos: Vec<u32>,
    pub free_blocks: Vec<FreeBlock>,
    pub data: Vec<u8>,
}

// pub fn get_serialized_most_common_encoded(data: &mut Vec<(u32, u32)>) -> Vec<u8> {
//     let mut vint = VIntArrayEncodeMostCommon::default();

//     let mut last = 0;
//     for el in data.iter_mut() {
//         let actual_val = el.0;
//         el.0 -= last;
//         last = actual_val;
//     }

//     let values: Vec<u32> = data.iter().flat_map(|(el1, el2)| vec![*el1, *el2]).collect();
//     vint.encode_vals(&values);
//     vint.serialize()
// }

// pub fn get_serialized_most_common_encoded(data: &[u32]) -> Vec<u8> {
//     let mut vint = VIntArrayEncodeMostCommon::default();
//     vint.encode_vals(&data);
//     vint.serialize()
// }

pub fn resize_to_power_of_two(data: &mut Vec<u8>) {
    let size = data.len() as u32;
    data.resize(get_next_to_power_of_two(size) as usize, 0);
}

pub fn get_next_to_power_of_two(size: u32) -> u32 {
    let mut n = 0;
    while size > 2u32.pow(n) {
        n += 1;
    }
    2u32.pow(n)
}

pub fn get_serialized_most_common_encoded(data: &mut Vec<u32>) -> Vec<u8> {
    let mut vint = VIntArrayEncodeMostCommon::default();

    // data.sort_unstable_by_key(|a| a.valid);
    let mut last = 0;
    for (id, _) in data.iter_mut().tuples() {
        let actual_val = *id;
        *id -= last;
        last = actual_val;
    }

    // let values: Vec<u32> = data.iter().flat_map(|(el1, el2)| vec![*el1, *el2]).collect();
    vint.encode_vals(&data);
    let mut data = vint.serialize();
    resize_to_power_of_two(&mut data);
    data
}


// pub fn get_serialized_most_common_encoded(data: &[u32]) -> Vec<u8> {
//     let mut vint = VIntArrayEncodeMostCommon::default();
//     vint.encode_vals(&data);
//     vint.serialize()
// }

#[derive(Serialize, Deserialize, Debug, Clone, Default, HeapSizeOf)]
pub struct FreeBlock {
    pub start: u32,
    pub length: u32
}
impl FreeBlock {
    fn new(start: u32, length:u32) -> Self {
        FreeBlock{start, length}
    }
}

impl TokenToAnchorScoreVintDelta {
    pub fn read<P: AsRef<Path> + std::fmt::Debug>(&mut self, path_indirect: P, path_data: P) -> Result<(), io::Error> {
        self.start_pos = load_index_u32(&path_indirect)?;
        self.data = file_to_bytes(&path_data)?;
        Ok(())
    }

    pub fn write<P: AsRef<Path> + std::fmt::Debug>(&self, path_indirect: P, path_data: P, path_free_blocks: P) -> Result<(), io::Error> {
        File::create(path_indirect)?.write_all(&vec_to_bytes_u32(&self.start_pos))?;

        let free_blocks_vec:Vec<u32> = self.free_blocks.iter().flat_map(|block| vec![block.start, block.length]).collect();
        File::create(path_free_blocks)?.write_all(&vec_to_bytes_u32(&free_blocks_vec))?;
        File::create(path_data)?.write_all(&self.data)?;
        Ok(())
    }

    #[inline]
    fn get_size(&self) -> usize {
        self.start_pos.len()
    }

    fn get_free_block(&mut self, size: u32) -> Option<u32> {
        for block in self.free_blocks.iter_mut() {
            if block.length >= size{
                block.length -= size;
                let pos = block.start;
                block.start += size;
                return Some(pos);
            }
        }
        None
    }

    pub fn add_data(&mut self, add_data: &[u8]) -> u32{
        if let Some(free_block_pos) = self.get_free_block(add_data.len() as u32) {
            self.data[free_block_pos as usize..free_block_pos as usize +add_data.len()].copy_from_slice(&add_data);
            // self.start_pos[pos] = free_block_pos;
            free_block_pos
        }else{
            let byte_offset = self.data.len() as u32;
            // self.start_pos[pos] = byte_offset;
            self.data.extend(add_data);
            byte_offset
        }

    }

    pub fn add_values(&mut self, id: u32, mut add_data: Vec<u32>) {
        //TODO INVALIDATE OLD DATA IF SET TWICE?

        let pos: usize = id as usize;
        let required_size = pos + 1;
        if self.start_pos.len() < required_size {
            self.start_pos.resize(required_size, U31_MAX);
        }

        if self.start_pos[pos] != U31_MAX { //Merge Move existing data
            let (mut data, size) = VIntArrayEncodeMostCommon::decode_from_slice(&self.data[self.start_pos[pos] as usize ..]);
            let mut current = 0;
            for (mut id, _) in data.iter_mut().tuples() {
                *id += current;
                current = *id;
            }
            let old_size = get_next_to_power_of_two(size);
            data.extend(add_data);
            // self.free_blocks.push(FreeBlock::new(self.start_pos[pos], size));
            let new_data = get_serialized_most_common_encoded(&mut data);
            // let new_size = get_next_to_power_of_two(size);
            let new_size = new_data.len() as u32;
            if old_size != new_size {
                self.free_blocks.push(FreeBlock::new(self.start_pos[pos], old_size));
                let pos_in_data = self.add_data(&new_data);
                self.start_pos[pos] = pos_in_data;
            }else{
                self.data[self.start_pos[pos] as usize..self.start_pos[pos] as usize +new_size as usize].copy_from_slice(&new_data);
            }
        }else{
            let pos_in_data = self.add_data(&get_serialized_most_common_encoded(&mut add_data));
            self.start_pos[pos] = pos_in_data;
        }

    }
}


// #[inline]
// fn recreate_vec(data: &[u8], pos: usize) -> Vec<u32> {
//     let vint = VintArrayMostCommonIterator::from_slice(&data[pos..]);
//     lervint.data.len()
//     let data: Vec<AnchorScore> = vint.collect();
//     data
// }

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

impl TokenToAnchorScore for TokenToAnchorScoreVintDelta {
    #[inline]
    fn get_max_id(&self) -> usize {
        //TODO REMOVE METHOD
        self.get_size()
    }

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
}

// #[derive(Debug)]
// pub struct TokenToAnchorScoreVintDeltaMmap {
//     pub start_pos: Mmap,
//     pub data: Mmap,
//     pub max_value_id: u32,
// }

// impl TokenToAnchorScoreVintDeltaMmap {
//     pub fn new(start_and_end_file: &fs::File, data_file: &fs::File) -> Self {
//         let start_and_end_file = unsafe { MmapOptions::new().map(&start_and_end_file).unwrap() };
//         let data_file = unsafe { MmapOptions::new().map(&data_file).unwrap() };
//         TokenToAnchorScoreVintDeltaMmap {
//             start_pos: start_and_end_file,
//             data: data_file,
//             max_value_id: 0,
//         }
//     }
// }

// impl HeapSizeOf for TokenToAnchorScoreVintDeltaMmap {
//     fn heap_size_of_children(&self) -> usize {
//         0
//     }
// }

// impl TokenToAnchorScore for TokenToAnchorScoreVintDeltaMmap {
//     #[inline]
//     fn get_max_id(&self) -> usize {
//         self.start_pos.len() / 4
//     }

//     #[inline]
//     fn get_scores(&self, id: u32) -> Option<Vec<AnchorScore>> {
//         if id as usize >= self.start_pos.len() / 4 {
//             return None;
//         }
//         let pos = get_u32_from_bytes(&self.start_pos, id as usize * 4);
//         if pos == U31_MAX {
//             return None;
//         }
//         Some(recreate_vec(&self.data, pos as usize))
//     }
// }

#[test]
fn test_token_to_anchor_score_vinto() {
    // use tempfile::tempdir;

    let mut yeps = TokenToAnchorScoreVintDelta::default();

    // yeps.set_scores(1, vec![(1, 1)]);
    yeps.add_values(1, vec![1, 1]);

    assert_eq!(yeps.get_scores(0), None);
    assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0))]));
    assert_eq!(yeps.get_scores(2), None);

    yeps.add_values(1, vec![2, 1]);

    assert_eq!(yeps.get_scores(0), None);
    assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(1.0))]));
    assert_eq!(yeps.get_scores(2), None);

     yeps.add_values(3, vec![2, 1, 5, 1]);

     assert_eq!(yeps.get_scores(3), Some(vec![AnchorScore::new(2, f16::from_f32(1.0)), AnchorScore::new(5, f16::from_f32(1.0))]));;

    // yeps.set_scores(5, vec![(1, 1), (2, 3)]);
    // assert_eq!(yeps.get_scores(4), None);
    // assert_eq!(
    //     yeps.get_scores(5),
    //     Some(vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))])
    // );
    // assert_eq!(yeps.get_scores(6), None);

    // let dir = tempdir().unwrap();
    // let data = dir.path().join("TokenToAnchorScoreVintTestData");
    // let indirect = dir.path().join("TokenToAnchorScoreVintTestIndirect");
    // yeps.write(indirect.to_str().unwrap(), data.to_str().unwrap()).unwrap();

    // // IM loaded from File
    // let mut yeps = TokenToAnchorScoreVintDelta::default();
    // yeps.read(indirect.to_str().unwrap(), data.to_str().unwrap()).unwrap();
    // assert_eq!(yeps.get_scores(0), None);
    // assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0))]));
    // assert_eq!(yeps.get_scores(2), None);

    // assert_eq!(yeps.get_scores(4), None);
    // assert_eq!(
    //     yeps.get_scores(5),
    //     Some(vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))])
    // );
    // assert_eq!(yeps.get_scores(6), None);

    // // Mmap from File
    // let start_and_end_file = File::open(indirect).unwrap();
    // let data_file = File::open(data).unwrap();
    // let yeps = TokenToAnchorScoreVintDeltaMmap::new(&start_and_end_file, &data_file);
    // assert_eq!(yeps.get_scores(0), None);
    // assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0))]));
    // assert_eq!(yeps.get_scores(2), None);

    // assert_eq!(yeps.get_scores(4), None);
    // assert_eq!(
    //     yeps.get_scores(5),
    //     Some(vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))])
    // );
    // assert_eq!(yeps.get_scores(6), None);
}
