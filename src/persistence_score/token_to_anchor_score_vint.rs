use util::*;

use super::*;
use vint::vint_encode_most_common::*;

use itertools::Itertools;
use search;
use std;
use std::io;
use std::iter::FusedIterator;

use persistence_data_indirect;

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
pub struct TokenToAnchorScoreVintFlushing {
    pub ids_cache: Vec<u32>,
    pub data_cache: Vec<u8>,
    pub current_data_offset: u32,
    /// Already written ids_cache
    pub current_id_offset: u32,
    pub indirect_path: String,
    pub data_path: String,
    // pub avg_join_size: f32,
    // pub num_values: u32,
    // pub num_ids: u32,
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

impl Default for TokenToAnchorScoreVintFlushing {
    fn default() -> TokenToAnchorScoreVintFlushing {
        TokenToAnchorScoreVintFlushing::new("".to_string(), "".to_string())
    }
}

impl TokenToAnchorScoreVintFlushing {
    pub fn new(indirect_path: String, data_path: String) -> Self {
        let mut data_cache = vec![];
        data_cache.resize(1, 0); // resize data by one, because 0 is reserved for the empty buckets
        TokenToAnchorScoreVintFlushing {
            ids_cache: vec![],
            data_cache,
            current_data_offset: 0,
            current_id_offset: 0,
            indirect_path,
            data_path,
            metadata: IndexMetaData::default(),
        }
    }

    pub fn set_scores(&mut self, id: u32, mut add_data: &mut [u32]) -> Result<(), io::Error> {
        let id_pos = (id - self.current_id_offset) as usize;

        if self.ids_cache.len() <= id_pos {
            //TODO this could become very big, check memory consumption upfront, and flush directly to disk, when a resize would step over a certain threshold @Memory
            self.ids_cache.resize(id_pos + 1, EMPTY_BUCKET);
        }

        self.metadata.num_values += add_data.len() as u32 / 2;
        self.metadata.num_ids += 1;
        self.ids_cache[id_pos] = self.current_data_offset + self.data_cache.len() as u32;

        self.ids_cache[id_pos] = self.current_data_offset + self.data_cache.len() as u32;
        self.data_cache.extend(get_serialized_most_common_encoded(&mut add_data));

        if self.ids_cache.len() + self.data_cache.len() >= 1_000_000 {
            self.flush()?;
        }
        Ok(())
    }

    #[inline]
    pub fn is_in_memory(&self) -> bool {
        self.current_id_offset == 0
    }

    pub fn into_im_store(self) -> TokenToAnchorScoreVintIM {
        TokenToAnchorScoreVintIM {
            start_pos: self.ids_cache,
            data: self.data_cache,
        }
    }

    pub fn into_mmap(self) -> Result<(TokenToAnchorScoreVintMmap), search::SearchError> {
        //TODO MAX VALUE ID IS NOT SET
        Ok(TokenToAnchorScoreVintMmap::from_path(&self.indirect_path, &self.data_path)?)
    }

    #[inline]
    pub fn flush(&mut self) -> Result<(), io::Error> {
        if self.ids_cache.is_empty() {
            return Ok(());
        }

        self.current_id_offset += self.ids_cache.len() as u32;
        self.current_data_offset += self.data_cache.len() as u32;

        persistence_data_indirect::flush_to_file_indirect(&self.indirect_path, &self.data_path, &vec_to_bytes_u32(&self.ids_cache), &self.data_cache)?;

        self.data_cache.clear();
        self.ids_cache.clear();

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

    let mut store = TokenToAnchorScoreVintFlushing::default();

    store.set_scores(1, &mut vec![1, 1]).unwrap();
    let store = store.into_im_store();
    assert_eq!(store.get_score_iter(0).collect::<Vec<_>>(), vec![]);
    assert_eq!(store.get_score_iter(1).collect::<Vec<_>>(), vec![AnchorScore::new(1, f16::from_f32(1.0))]);
    assert_eq!(store.get_score_iter(2).collect::<Vec<_>>(), vec![]);

    let mut store = TokenToAnchorScoreVintFlushing::default();
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

    let mut store = TokenToAnchorScoreVintFlushing::new(indirect, data);
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
