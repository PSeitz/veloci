
use super::*;
use vint::vint_encode_most_common::*;

use std::fs::OpenOptions;
use memmap::MmapMut;
use memmap::MmapOptions;

use std;
use std::fs::File;
use std::io;
use std::io::prelude::*;

use itertools::Itertools;

use byteorder::{LittleEndian, ReadBytesExt};

use std::mem::transmute;

const NO_DATA: u32 = 0;

impl_type_info!(TokenToAnchorScoreVintDelta);
// impl_type_info!(TokenToAnchorScoreVintDelta, TokenToAnchorScoreVintDeltaMmap);

#[derive(Clone, Default, Debug)]
pub struct TokenToAnchorScoreVintDelta {
    // pub start_pos: Vec<u32>,
    pub free_blocks: Vec<FreeBlock>,
    // pub data: Vec<u8>,
    pub data: Vec<Option<Vec<u32>>>,
    pub num_values_added: u32,
    pub start_pos_file_path: String,
    pub data_file_path: String,
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

use num::ToPrimitive;

fn get_u32_from_mmap(data: &[u8], index: usize) -> u32 {
    let mmap_index = index as usize * 4;
    if data.len() < mmap_index + 4{
        return NO_DATA;
    }
    let data_start_pos = (&data[mmap_index..mmap_index + 4]).read_u32::<LittleEndian>().unwrap();
    data_start_pos.to_u32().unwrap()
}

fn write_u32_to_mmap(data: &mut MmapMut, index: usize, value: u32) {
    // let data_start_pos = (&data[index..index + 4]).read_u32::<LittleEndian>().unwrap();
    // data_start_pos.to_u32().unwrap()
    let mmap_index = index as usize * 4;
    let num_elements: [u8; 4] = unsafe { transmute(value) };
    // data[mmap_index..mmap_index + 4] = num_elements;

    data[mmap_index..mmap_index + 4].copy_from_slice(&num_elements);
}


impl HeapSizeOf for TokenToAnchorScoreVintDelta {
    fn heap_size_of_children(&self) -> usize {
        0 //FIXME
    }
}

impl TokenToAnchorScoreVintDelta {
    pub fn new(start_pos_file_path: String, data_file_path: String) -> Self {
        let start_pos_file = File::create(start_pos_file_path.to_string()).unwrap();
        let data_file = File::create(data_file_path.to_string()).unwrap();
        start_pos_file.set_len(1).unwrap();
        data_file.set_len(1).unwrap();
        TokenToAnchorScoreVintDelta{
            free_blocks:vec![],
            data:vec![],
            num_values_added:0,
            start_pos_file_path,
            data_file_path,
        }
    }
    pub fn read<P: AsRef<Path> + std::fmt::Debug>(&mut self, _path_indirect: P, _path_data: P) -> Result<(), io::Error> {
        // self.start_pos = load_index_u32(&_path_indirect)?;
        // self.data = file_to_bytes(&_path_data)?;
        Ok(())
    }

    pub fn write<P: AsRef<Path> + std::fmt::Debug>(&self, _path_indirect: P, _path_data: P, path_free_blocks: P) -> Result<(), io::Error> {
        // File::create(path_indirect)?.write_all(&vec_to_bytes_u32(&self.start_pos))?;

        let free_blocks_vec:Vec<u32> = self.free_blocks.iter().flat_map(|block| vec![block.start, block.length]).collect();
        File::create(path_free_blocks)?.write_all(&vec_to_bytes_u32(&free_blocks_vec))?;
        // File::create(path_data)?.write_all(&self.data)?;
        // File::create(path_data)?.write_all(&vec_to_bytes_u32(self.data))?;
        Ok(())
    }

    #[inline]
    fn get_size(&self) -> usize {
        0
        // self.start_pos.len()
    }

    fn get_free_block(free_blocks: &mut Vec<FreeBlock>, size: u32) -> Option<u32> {
        for block in free_blocks.iter_mut() {
            if block.length >= size{
                let pos = block.start;
                block.length -= size;
                block.start += size;
                return Some(pos);
            }
        }
        None
    }

    // pub fn add_data(&mut self, add_data: &[u8]) -> u32{
    //     if let Some(free_block_pos) = self.get_free_block(add_data.len() as u32) {
    //         self.data[free_block_pos as usize..free_block_pos as usize +add_data.len()].copy_from_slice(&add_data);
    //         // self.start_pos[pos] = free_block_pos;
    //         free_block_pos
    //     }else{
    //         let byte_offset = self.data.len() as u32;
    //         // self.start_pos[pos] = byte_offset;
    //         self.data.extend(add_data);
    //         byte_offset
    //     }

    // }

    pub fn add_values(&mut self, id: u32, add_data: Vec<u32>) {

        let pos: usize = id as usize;
        let required_size = pos + 1;
        if self.data.len() < required_size {
            self.data.resize(required_size, None);
        }
        self.num_values_added += add_data.len() as u32;
        if self.data[pos].is_none() {
            self.data[pos] = Some(add_data);
        }else{
            self.data[pos].as_mut().unwrap().extend(add_data);
        }

        if self.num_values_added > 20 {
            self.flush_to_disk().unwrap();
            self.num_values_added = 0;
        }

    }

    pub fn flush_to_disk(&mut self) -> Result<(), io::Error> {

        let start_pos_file = OpenOptions::new().read(true).write(true).create(true).open(self.start_pos_file_path.to_string()).unwrap();
        let data_file = OpenOptions::new().read(true).write(true).create(true).open(self.data_file_path.to_string()).unwrap();

        let mut start_pos_mmap = unsafe{MmapOptions::new().map_mut(&start_pos_file).unwrap()};
        let mut data_mmap = unsafe{MmapOptions::new().map_mut(&data_file).unwrap()};

        let mut append_data = vec![];
        let mut id = 0;
        for el in self.data.iter_mut() {
            if let Some(el) = el {
                Self::add_values_to_disk(&mut self.free_blocks, id, el, &mut start_pos_mmap, &mut data_mmap, &mut append_data);
            }
            id += 1;
        }

        for el in self.data.iter_mut() {
            *el = None;
        }

        if append_data.is_empty() {
            return Ok(());
        }

        let mut curr_size = data_file.metadata()?.len();
        let el_with_max_id = append_data.iter().max_by_key(|el|el.0).unwrap();

        //resize indirect file to fit max id
        let required_size = (el_with_max_id.0 + 1) as u64 * 4;
        if start_pos_file.metadata()?.len() < required_size {
            start_pos_file.set_len(required_size)?;
        }
        let total_byte_size:u64 = append_data.iter().map(|el|el.1.len() as u64).sum();

        data_file.set_len(curr_size + total_byte_size + 4)?;

        let mut start_pos_mmap = unsafe{MmapOptions::new().map_mut(&start_pos_file).unwrap()};
        let mut data_mmap = unsafe{MmapOptions::new().map_mut(&data_file).unwrap()};
        for el in append_data.iter() {
            let pos = el.0;
            let add_data = &el.1;

            let (mut data_adding_now, _) = VIntArrayEncodeMostCommon::decode_from_slice(&add_data);
            println!("data_adding_now {:?}", data_adding_now);
            write_u32_to_mmap(&mut start_pos_mmap, pos, curr_size as u32);
            data_mmap[curr_size as usize..curr_size as usize  + add_data.len()].copy_from_slice(&add_data);
            curr_size += add_data.len() as u64;
        }

        start_pos_mmap.flush()?;
        data_mmap.flush()?;

        Ok(())

    }

    pub fn add_values_to_disk(free_blocks: &mut Vec<FreeBlock>, id: u32, mut add_data: &mut Vec<u32>, start_pos_mmap: &mut MmapMut, data_mmap: &mut MmapMut, append_data: &mut Vec<(usize, Vec<u8>)>) {

        let pos: usize = id as usize;
        let required_size = (pos + 1) * 4;
        // if start_pos_mmap.len() < required_size {
        //     // start_pos_mmap.resize(required_size, NO_DATA);
        // }

        if start_pos_mmap.len() < required_size || get_u32_from_mmap(&start_pos_mmap, pos) == NO_DATA  {
            let new_data = get_serialized_most_common_encoded(&mut add_data);
            append_data.push((pos, new_data));
        }else{

            let pos_in_data = get_u32_from_mmap(&start_pos_mmap, pos);

            let (mut data_old, size) = VIntArrayEncodeMostCommon::decode_from_slice(&data_mmap[pos_in_data as usize ..]);
            let mut current = 0;
            for (mut id, _) in data_old.iter_mut().tuples() {
                *id += current;
                current = *id;
            }
            let old_size = get_next_to_power_of_two(size);
            println!("data_old {:?}", data_old);
            data_old.extend(add_data.iter());
            println!("data_old + new {:?}", data_old);
            let new_data = get_serialized_most_common_encoded(&mut data_old);
            let new_size = new_data.len() as u32;
            if old_size != new_size {
                free_blocks.push(FreeBlock::new(pos_in_data, old_size));
                if free_blocks.len() % 100 == 0 {
                    free_blocks.retain(|ref block| block.length != 0);
                }
                let new_data = get_serialized_most_common_encoded(&mut add_data);
                if let Some(free_block_pos) = Self::get_free_block(free_blocks, new_data.len() as u32) {
                    data_mmap[free_block_pos as usize..free_block_pos as usize + new_data.len()].copy_from_slice(&new_data);
                    write_u32_to_mmap(start_pos_mmap, pos, free_block_pos);
                }else{ // append data_mmap to end of file
                    append_data.push((pos, new_data));
                }
            }else{
                data_mmap[start_pos_mmap[pos] as usize..start_pos_mmap[pos] as usize + new_size as usize].copy_from_slice(&new_data);
            }
        }
    }

    fn get_scores_disk(&self, id: u32) -> Option<Vec<AnchorScore>> {
        let start_pos_file = OpenOptions::new().read(true).open(self.start_pos_file_path.to_string()).unwrap();
        let data_file = OpenOptions::new().read(true).open(self.data_file_path.to_string()).unwrap();

        let start_pos_mmap = unsafe{MmapOptions::new()
                .map(&start_pos_file)
                .unwrap()};

        let data_mmap = unsafe{MmapOptions::new()
                .map(&data_file)
                .unwrap()};

        let pos_in_data = get_u32_from_mmap(&start_pos_mmap, id as usize);
        if pos_in_data == NO_DATA {
            return None;
        }
        Some(recreate_vec(&data_mmap, pos_in_data as usize))

    }
    fn get_scores_im(&self, id: u32) -> Option<Vec<AnchorScore>> {
        // println!("Waaa {:?}", self.data);
        if self.data.len() <= id as usize{
            return None;
        }
        if self.data[id as usize].is_none(){
            None
        }else{
            Some(self.data[id as usize].as_ref().unwrap().iter().tuples()
            .map(|(id, score)| AnchorScore::new(*id, f16::from_f32(*score as f32)))
            .collect())
        }
        // self.data[id as usize].map(|vec|
        //     vec.iter().tuples()
        //     .map(|(id, score)| AnchorScore::new(*id, f16::from_f32(*score as f32)))
        //     .collect()
        // )

        // if self.data.len() < id){
        //     None
        // }else{
        // }

    }

    // pub fn add_values(&mut self, id: u32, mut add_data: Vec<u32>) {
    //     //TODO INVALIDATE OLD DATA IF SET TWICE?

    //     let pos: usize = id as usize;
    //     let required_size = pos + 1;
    //     if self.start_pos.len() < required_size {
    //         self.start_pos.resize(required_size, U31_MAX);
    //     }

    //     if self.start_pos[pos] != U31_MAX { //Merge Move existing data
    //         let (mut data, size) = VIntArrayEncodeMostCommon::decode_from_slice(&self.data[self.start_pos[pos] as usize ..]);
    //         let mut current = 0;
    //         for (mut id, _) in data.iter_mut().tuples() {
    //             *id += current;
    //             current = *id;
    //         }
    //         let old_size = get_next_to_power_of_two(size);
    //         data.extend(add_data);
    //         // self.free_blocks.push(FreeBlock::new(self.start_pos[pos], size));
    //         let new_data = get_serialized_most_common_encoded(&mut data);
    //         // let new_size = get_next_to_power_of_two(size);
    //         let new_size = new_data.len() as u32;
    //         if old_size != new_size {
    //             self.free_blocks.push(FreeBlock::new(self.start_pos[pos], old_size));
    //             if self.free_blocks.len() % 100 == 0 {
    //                 // println!("{:?}", self.free_blocks.len());
    //                 self.free_blocks.retain(|ref block| block.length != 0);
    //             }
    //             let pos_in_data = self.add_data(&new_data);
    //             self.start_pos[pos] = pos_in_data;
    //         }else{
    //             self.data[self.start_pos[pos] as usize..self.start_pos[pos] as usize +new_size as usize].copy_from_slice(&new_data);
    //         }
    //     }else{
    //         let pos_in_data = self.add_data(&get_serialized_most_common_encoded(&mut add_data));
    //         self.start_pos[pos] = pos_in_data;
    //     }
    // }

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
        // if id as usize >= self.get_size() {
        //     return None;
        // }

        // let pos = self.start_pos[id as usize];
        // if pos == NO_DATA {
        //     return None;
        // }

        // Some(recreate_vec(&self.data, pos as usize))

        if let Some(mut data_disk) = self.get_scores_disk(id) {
            if let Some(mut data_im) = self.get_scores_im(id) {
                data_disk.extend(data_im);
            }
            Some(data_disk)
        }else{
            self.get_scores_im(id)
        }
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
fn test_token_to_anchor_score_delta() {
    let start_pos_file_path = "token_to_anchor_score_delta_indirect";
    let data_file_path = "token_to_anchor_score_delta_indirect_data";
    let start_pos_file = File::create(start_pos_file_path).unwrap();
    let data_file = File::create(data_file_path).unwrap();
    start_pos_file.set_len(1).unwrap();
    data_file.set_len(1).unwrap();
    let mut yeps = TokenToAnchorScoreVintDelta{
        free_blocks: vec![],
        data: vec![],
        num_values_added: 0,
        start_pos_file_path: start_pos_file_path.to_string(),
        data_file_path: data_file_path.to_string(),
    };

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

}

// #[test]
// fn test_token_to_anchor_score_delta_flush() {
//     let start_pos_file_path = "token_to_anchor_score_delta_indirect";
//     let data_file_path = "token_to_anchor_score_delta_indirect_data";
//     let start_pos_file = File::create(start_pos_file_path).unwrap();
//     let data_file = File::create(data_file_path).unwrap();
//     start_pos_file.set_len(1).unwrap();
//     data_file.set_len(1).unwrap();
//     let mut yeps = TokenToAnchorScoreVintDelta{
//         free_blocks: vec![],
//         data: vec![],
//         num_values_added: 0,
//         start_pos_file_path: start_pos_file_path.to_string(),
//         data_file_path: data_file_path.to_string(),
//     };

//     yeps.add_values(1, vec![1, 1]);

//     assert_eq!(yeps.get_scores(0), None);
//     assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0))]));
//     assert_eq!(yeps.get_scores(2), None);

//     yeps.flush_to_disk().unwrap();

//     yeps.add_values(1, vec![2, 1]);

//     yeps.flush_to_disk().unwrap();

//     assert_eq!(yeps.get_scores(0), None);
//     assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(1.0))]));
//     assert_eq!(yeps.get_scores(2), None);

//     yeps.add_values(3, vec![2, 1, 5, 1]);
//     yeps.flush_to_disk().unwrap();

//     assert_eq!(yeps.get_scores(3), Some(vec![AnchorScore::new(2, f16::from_f32(1.0)), AnchorScore::new(5, f16::from_f32(1.0))]));;

// }

#[test]
fn test_token_to_anchor_score_delta_write() {
    let start_pos_file_path = "token_to_anchor_score_delta_indirect";
    let data_file_path = "token_to_anchor_score_delta_indirect_data";
    let start_pos_file = File::create(start_pos_file_path).unwrap();
    let data_file = File::create(data_file_path).unwrap();
    start_pos_file.set_len(1).unwrap();
    data_file.set_len(1).unwrap();
    let mut yeps = TokenToAnchorScoreVintDelta{
        free_blocks: vec![],
        data: vec![],
        num_values_added: 0,
        start_pos_file_path: start_pos_file_path.to_string(),
        data_file_path: data_file_path.to_string(),
    };

    yeps.add_values(1, vec![1, 1]);
    yeps.add_values(1, vec![2, 1]);
    yeps.add_values(3, vec![2, 1, 5, 1]);

    yeps.flush_to_disk().unwrap();
}
