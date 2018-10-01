use util::*;

use super::*;
use vint::vint_encode_most_common::*;

use itertools::Itertools;
use search;
use std;
use std::io;
use std::iter::FusedIterator;

use persistence_data_indirect;
use num::Integer;
use num;
use std::ops;

impl_type_info!(TokenToAnchorScoreVintIM, TokenToAnchorScoreVintMmap);

const EMPTY_BUCKET: u32 = 0;

#[derive(Serialize, Deserialize, Debug, Clone, Default, HeapSizeOf)]
pub struct TokenToAnchorScoreVintIM {
    pub start_pos: Vec<u32>,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct TokenToAnchorScoreVintMmap {
    pub start_pos: Mmap,
    pub data: Mmap,
    pub max_value_id: u32,
}

///
/// Datastructure to cache and flush changes to file
///
#[derive(Serialize, Deserialize, Debug, Clone, HeapSizeOf)]
pub struct TokenToAnchorScoreVintFlushing<T: Integer + num::NumCast + Clone + Copy + ops::AddAssign + ops::Add + num::Zero> {
    pub id_to_data_pos: Vec<T>,
    pub data_cache: Vec<u8>,
    pub current_data_offset: T,
    /// Already written id_to_data_pos
    pub current_id_offset: u32,
    pub indirect_path: String,
    pub data_path: String,
    pub metadata: IndexMetaData,
}

fn get_serialized_most_common_encoded(data: &mut [u32]) -> Vec<u8> {
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

impl<T: Integer + num::NumCast + Clone + Copy + ops::AddAssign + ops::Add + num::Zero> Default for TokenToAnchorScoreVintFlushing<T> {
    fn default() -> TokenToAnchorScoreVintFlushing<T> {
        TokenToAnchorScoreVintFlushing::new("".to_string(), "".to_string())
    }
}

impl<T: Integer + num::NumCast + Clone + Copy + ops::AddAssign + ops::Add + num::Zero> TokenToAnchorScoreVintFlushing<T> {
    pub fn new(indirect_path: String, data_path: String) -> Self {
        let mut data_cache = vec![];
        data_cache.resize(1, 0); // resize data by one, because 0 is reserved for the empty buckets
        TokenToAnchorScoreVintFlushing {
            id_to_data_pos: vec![],
            data_cache,
            current_data_offset: T::zero(),
            current_id_offset: 0,
            indirect_path,
            data_path,
            metadata: IndexMetaData::default(),
        }
    }

    pub fn set_scores(&mut self, id: u32, mut add_data: &mut [u32]) -> Result<(), io::Error> {
        let id_pos = (id - self.current_id_offset) as usize;

        if self.id_to_data_pos.len() <= id_pos {
            //TODO this could become very big, check memory consumption upfront, and flush directly to disk, when a resize would step over a certain threshold @Memory
            self.id_to_data_pos.resize(id_pos + 1, num::cast(EMPTY_BUCKET).unwrap());
        }

        self.metadata.num_values += add_data.len() as u32 / 2;
        self.metadata.num_ids += 1;
        // self.id_to_data_pos[id_pos] = self.current_data_offset + self.data_cache.len() as u32;

        self.id_to_data_pos[id_pos] = self.current_data_offset + num::cast(self.data_cache.len()).unwrap();
        self.data_cache.extend(get_serialized_most_common_encoded(&mut add_data));

        if self.id_to_data_pos.len() + self.data_cache.len() >= 1_000_000 {
            self.flush()?;
        }
        Ok(())
    }

    #[inline]
    pub fn is_in_memory(&self) -> bool {
        self.current_id_offset == 0
    }

    pub fn into_store(mut self) -> Result<Box<TokenToAnchorScore>, search::SearchError> {
        if self.is_in_memory() {
            Ok(Box::new(self.into_im_store()))
        } else {
            self.flush()?;
            Ok(Box::new(self.into_mmap()?))
        }
    }

    pub fn into_im_store(self) -> TokenToAnchorScoreVintIM {
        TokenToAnchorScoreVintIM {
            start_pos: self.id_to_data_pos.iter().map(|el|num::cast(*el).unwrap()).collect(), //TODO
            data: self.data_cache,
        }
    }

    pub fn into_mmap(self) -> Result<(TokenToAnchorScoreVintMmap), search::SearchError> {
        //TODO MAX VALUE ID IS NOT SET
        Ok(TokenToAnchorScoreVintMmap::from_path(&self.indirect_path, &self.data_path)?)
    }

    #[inline]
    pub fn flush(&mut self) -> Result<(), io::Error> {
        if self.id_to_data_pos.is_empty() {
            return Ok(());
        }

        self.current_id_offset += self.id_to_data_pos.len() as u32;
        self.current_data_offset += num::cast(self.data_cache.len()).unwrap();

        use std::slice;
        use std::mem;
        let id_to_data_pos_bytes = unsafe {
            slice::from_raw_parts(self.id_to_data_pos.as_ptr() as *const u8, self.id_to_data_pos.len() * mem::size_of::<T>())
        };

        // persistence_data_indirect::flush_to_file_indirect(&self.indirect_path, &self.data_path, &vec_to_bytes_u32(&self.id_to_data_pos), &self.data_cache)?;
        persistence_data_indirect::flush_to_file_indirect(&self.indirect_path, &self.data_path, id_to_data_pos_bytes, &self.data_cache)?;

        self.data_cache.clear();
        self.id_to_data_pos.clear();

        self.metadata.avg_join_size = persistence_data_indirect::calc_avg_join_size(self.metadata.num_values, self.metadata.num_ids);

        Ok(())
    }
}

impl TokenToAnchorScoreVintIM {
    #[inline]
    fn get_size(&self) -> usize {
        self.start_pos.len()
    }

    pub(crate) fn read<P: AsRef<Path> + std::fmt::Debug>(&mut self, path_indirect: P, path_data: P) -> Result<(), search::SearchError> {
        self.start_pos = load_index_u32(&path_indirect)?;
        self.data = file_path_to_bytes(&path_data)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AnchorScoreIter<'a> {
    /// the current rolling value
    pub current: u32,
    pub vint_iter: VintArrayMostCommonIterator<'a>,
}
impl<'a> AnchorScoreIter<'a> {
    pub fn new(data: &'a [u8]) -> AnchorScoreIter<'a> {
        AnchorScoreIter {
            current: 0,
            vint_iter: VintArrayMostCommonIterator::from_slice(&data),
        }
    }
}
impl<'a> Iterator for AnchorScoreIter<'a> {
    type Item = AnchorScore;

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.vint_iter.size_hint()
    }

    #[inline]
    fn next(&mut self) -> Option<AnchorScore> {
        if let Some(mut id) = self.vint_iter.next() {
            let score = self.vint_iter.next().unwrap();
            id += self.current;
            self.current = id;
            Some(AnchorScore::new(id, f16::from_f32(score as f32)))
        } else {
            None
        }
    }
}

impl<'a> FusedIterator for AnchorScoreIter<'a> {}

impl TokenToAnchorScore for TokenToAnchorScoreVintIM {
    fn get_score_iter(&self, id: u32) -> AnchorScoreIter {
        if id as usize >= self.get_size() {
            return AnchorScoreIter::new(&[]);
        }
        let pos = self.start_pos[id as usize];
        if pos == EMPTY_BUCKET {
            return AnchorScoreIter::new(&[]);
        }
        AnchorScoreIter::new(&self.data[pos as usize..])
    }
}

use util::open_file;
impl TokenToAnchorScoreVintMmap {
    pub fn from_path(start_and_end_file: &str, data_file: &str) -> Result<Self, search::SearchError> {
        let start_and_end_file = unsafe { MmapOptions::new().map(&open_file(start_and_end_file)?).unwrap() };
        let data_file = unsafe { MmapOptions::new().map(&open_file(data_file)?).unwrap() };
        Ok(TokenToAnchorScoreVintMmap {
            start_pos: start_and_end_file,
            data: data_file,
            max_value_id: 0,
        })
    }
}

impl HeapSizeOf for TokenToAnchorScoreVintMmap {
    fn heap_size_of_children(&self) -> usize {
        8
    }
}

impl TokenToAnchorScore for TokenToAnchorScoreVintMmap {
    fn get_score_iter(&self, id: u32) -> AnchorScoreIter {
        if id as usize >= self.start_pos.len() / 4 {
            return AnchorScoreIter::new(&[]);
        }
        let pos = get_u32_from_bytes(&self.start_pos, id as usize * 4);
        if pos == EMPTY_BUCKET {
            return AnchorScoreIter::new(&[]);
        }
        AnchorScoreIter::new(&self.data[pos as usize..])
    }
}

#[test]
fn test_token_to_anchor_score_vint() {
    use tempfile::tempdir;

    let mut store = TokenToAnchorScoreVintFlushing::<u32>::default();

    store.set_scores(1, &mut vec![1, 1]).unwrap();
    let store = store.into_im_store();
    assert_eq!(store.get_score_iter(0).collect::<Vec<_>>(), vec![]);
    assert_eq!(store.get_score_iter(1).collect::<Vec<_>>(), vec![AnchorScore::new(1, f16::from_f32(1.0))]);
    assert_eq!(store.get_score_iter(2).collect::<Vec<_>>(), vec![]);

    let mut store = TokenToAnchorScoreVintFlushing::<u32>::default();
    store.set_scores(5, &mut vec![1, 1, 2, 3]).unwrap();
    let store = store.into_im_store();
    assert_eq!(store.get_score_iter(4).collect::<Vec<_>>(), vec![]);
    assert_eq!(
        store.get_score_iter(5).collect::<Vec<_>>(),
        vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))]
    );
    assert_eq!(store.get_score_iter(6).collect::<Vec<_>>(), vec![]);

    let dir = tempdir().unwrap();
    let data = dir.path().join("TokenToAnchorScoreVintTestData").to_str().unwrap().to_string();
    let indirect = dir.path().join("TokenToAnchorScoreVintTestIndirect").to_str().unwrap().to_string();

    let mut store = TokenToAnchorScoreVintFlushing::<u32>::new(indirect, data);
    store.set_scores(1, &mut vec![1, 1]).unwrap();
    store.flush().unwrap();
    store.set_scores(5, &mut vec![1, 1, 2, 3]).unwrap();
    store.flush().unwrap();
    store.flush().unwrap(); // double flush test

    let store = store.into_mmap().unwrap();
    assert_eq!(store.get_score_iter(0).collect::<Vec<_>>(), vec![]);
    assert_eq!(store.get_score_iter(1).collect::<Vec<_>>(), vec![AnchorScore::new(1, f16::from_f32(1.0))]);
    assert_eq!(store.get_score_iter(2).collect::<Vec<_>>(), vec![]);
    assert_eq!(
        store.get_score_iter(5).collect::<Vec<_>>(),
        vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))]
    );
}
