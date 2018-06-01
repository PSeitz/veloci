use util::*;

use super::*;
use vint::vint_encode_most_common::*;

use std;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use search;
use super::U31_MAX;
use itertools::Itertools;

impl_type_info!(TokenToAnchorScoreVintIM, TokenToAnchorScoreVintMmap);

#[derive(Serialize, Deserialize, Debug, Clone, Default, HeapSizeOf)]
pub struct TokenToAnchorScoreVintIM {
    pub start_pos: Vec<u32>,
    pub data: Vec<u8>,
}

///
/// Datastructure to cache and flush changes to file
///
#[derive(Serialize, Deserialize, Debug, Clone, Default, HeapSizeOf)]
pub struct TokenToAnchorScoreVint {
    pub cache: Vec<(u32, Vec<u32>)>,
    pub path: String,
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

pub fn get_serialized_most_common_encoded_2(data: &mut Vec<u32>) -> Vec<u8> {
    let mut vint = VIntArrayEncodeMostCommon::default();

    let mut last = 0;
    for (el, _score) in data.iter_mut().tuples() {
        let actual_val = *el;
        *el -= last;
        last = actual_val;
    }

    vint.encode_vals(&data);
    vint.serialize()
}

impl TokenToAnchorScoreVint {
    pub fn set_scores(&mut self, id: u32, add_data: Vec<u32>) -> Result<(), io::Error> {
        self.cache.push((id, add_data));

        // let pos: usize = id as usize;
        // let required_size = pos + 1;
        // if self.start_pos.len() < required_size {
        //     self.start_pos.resize(required_size, U31_MAX);
        // }

        // let byte_offset = self.data.len() as u32;
        // self.start_pos[pos] = byte_offset;
        // self.data.extend(get_serialized_most_common_encoded_2(&mut add_data));

        self.flush()
    }

    #[inline]
    fn flush(&mut self) -> Result<(), io::Error> {
        // let mut indirect = File::open(self.path.to_string() + ".indirect")?;
        // let mut data = File::open(self.path.to_string() + ".data")?;
        // let mut data_pos = data.metadata()?.len();

        // let mut positions = vec![];
        // let mut all_bytes = vec![];
        // positions.push(data_pos as u32);
        // for (_, add_data) in self.cache.iter_mut() {
        //     let add_bytes = get_serialized_most_common_encoded_2(add_data);
        //     data_pos += add_bytes.len() as u64;
        //     positions.push(data_pos as u32);
        //     all_bytes.extend(add_bytes);
        // }
        // data.write_all(&all_bytes)?;
        // indirect.write_all(&vec_to_bytes_u32(&positions))?;

        let mut indirect = File::open(self.path.to_string() + ".indirect")?;
        let mut data = File::open(self.path.to_string() + ".data")?;
        let all_data = self.cache.iter_mut().map(|(id, add_data)|(*id, get_serialized_most_common_encoded_2(add_data))).collect();
        flush_data_to_indirect_index(&mut indirect, &mut data, all_data);
        self.cache.clear();
        Ok(())
    }

}


#[inline]
fn flush_data_to_indirect_index(indirect: &mut File, data: &mut File, cache: Vec<(u32, Vec<u8>)> ) -> Result<(), io::Error> {

    let mut data_pos = data.metadata()?.len();
    let mut positions = vec![];
    let mut all_bytes = vec![];
    positions.push(data_pos as u32);
    for (_, add_bytes) in cache.iter() {
        data_pos += add_bytes.len() as u64;
        positions.push(data_pos as u32);
        all_bytes.extend(add_bytes);
    }
    data.write_all(&all_bytes)?;
    // TODO write_bytes_at for indirect
    Ok(())
}



impl TokenToAnchorScoreVintIM {
    pub fn set_scores(&mut self, id: u32, mut add_data: &mut Vec<u32>) {
        //TODO INVALIDATE OLD DATA IF SET TWICE?

        let pos: usize = id as usize;
        let required_size = pos + 1;
        if self.start_pos.len() < required_size {
            self.start_pos.resize(required_size, U31_MAX);
        }

        let byte_offset = self.data.len() as u32;
        self.start_pos[pos] = byte_offset;
        self.data.extend(get_serialized_most_common_encoded_2(&mut add_data));
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

    pub fn read<P: AsRef<Path> + std::fmt::Debug>(&mut self, path_indirect: P, path_data: P) -> Result<(), search::SearchError> {
        self.start_pos = load_index_u32(&path_indirect)?;
        self.data = file_path_to_bytes(&path_data)?;
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

impl TokenToAnchorScore for TokenToAnchorScoreVintIM {
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

// #[test]
// fn test_token_to_anchor_score_vint() {
//     use tempfile::tempdir;

//     let mut yeps = TokenToAnchorScoreVintIM::default();

//     yeps.set_scores(1, vec![(1, 1)]);

//     assert_eq!(yeps.get_scores(0), None);
//     assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0))]));
//     assert_eq!(yeps.get_scores(2), None);

//     yeps.set_scores(5, vec![(1, 1), (2, 3)]);
//     assert_eq!(yeps.get_scores(4), None);
//     assert_eq!(
//         yeps.get_scores(5),
//         Some(vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))])
//     );
//     assert_eq!(yeps.get_scores(6), None);

//     let dir = tempdir().unwrap();
//     let data = dir.path().join("TokenToAnchorScoreVintTestData");
//     let indirect = dir.path().join("TokenToAnchorScoreVintTestIndirect");
//     yeps.write(indirect.to_str().unwrap(), data.to_str().unwrap()).unwrap();

//     // IM loaded from File
//     let mut yeps = TokenToAnchorScoreVintIM::default();
//     yeps.read(indirect.to_str().unwrap(), data.to_str().unwrap()).unwrap();
//     assert_eq!(yeps.get_scores(0), None);
//     assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0))]));
//     assert_eq!(yeps.get_scores(2), None);

//     assert_eq!(yeps.get_scores(4), None);
//     assert_eq!(
//         yeps.get_scores(5),
//         Some(vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))])
//     );
//     assert_eq!(yeps.get_scores(6), None);

//     // Mmap from File
//     let start_and_end_file = File::open(indirect).unwrap();
//     let data_file = File::open(data).unwrap();
//     let yeps = TokenToAnchorScoreVintMmap::new(&start_and_end_file, &data_file);
//     assert_eq!(yeps.get_scores(0), None);
//     assert_eq!(yeps.get_scores(1), Some(vec![AnchorScore::new(1, f16::from_f32(1.0))]));
//     assert_eq!(yeps.get_scores(2), None);

//     assert_eq!(yeps.get_scores(4), None);
//     assert_eq!(
//         yeps.get_scores(5),
//         Some(vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))])
//     );
//     assert_eq!(yeps.get_scores(6), None);
// }
