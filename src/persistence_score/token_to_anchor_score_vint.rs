use crate::util::*;

use super::*;
use vint::vint_encode_most_common::*;

use crate::error::VelociError;
use itertools::Itertools;
use std;
use std::io;
use std::iter::FusedIterator;
use std::marker::PhantomData;
use std::mem;

use crate::persistence_data_indirect;
use num;
use std::ops;

// impl_type_info_single_templ!(TokenToAnchorScoreVintMmap);
// impl_type_info!(TokenToAnchorScoreVintIM);

const EMPTY_BUCKET: u32 = 0;
const EMPTY_BUCKET_USIZE: usize = 0;

pub trait AnchorScoreDataSize: IndexIdToParentData + ops::AddAssign + ops::Add + num::Zero {}
impl<T> AnchorScoreDataSize for T where T: IndexIdToParentData + ops::AddAssign + ops::Add + num::Zero {}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TokenToAnchorScoreVintIM<T> {
    pub start_pos: Vec<T>,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct TokenToAnchorScoreVintMmap<T> {
    pub start_pos: Mmap,
    pub data: Mmap,
    pub max_value_id: u32,
    pub ok: PhantomData<T>,
}

impl<T: AnchorScoreDataSize> TypeInfo for TokenToAnchorScoreVintIM<T> {
    fn type_name(&self) -> String {
        unsafe { std::intrinsics::type_name::<Self>().to_string() }
    }
}

impl<T: AnchorScoreDataSize> TypeInfo for TokenToAnchorScoreVintMmap<T> {
    fn type_name(&self) -> String {
        unsafe { std::intrinsics::type_name::<Self>().to_string() }
    }
}

///
/// Datastructure to cache and flush changes to file
///
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenToAnchorScoreVintFlushing<T: AnchorScoreDataSize> {
    pub id_to_data_pos: Vec<T>,
    pub data_cache: Vec<u8>,
    pub current_data_offset: T,
    /// Already written id_to_data_pos
    pub current_id_offset: u32,
    pub indirect_path: String,
    pub data_path: String,
    pub metadata: IndexValuesMetadata,
}

fn compress_data_block(data: &mut [u32]) -> Vec<u8> {

    // if data.len() > 128 {
    //     let out:Vec<u8> = vec![];
    //     push_compact(data.len() as u32, &mut out);

    // }else{
        let mut last = 0;
        for (el, _score) in data.iter_mut().tuples() {
            let actual_val = *el;
            *el -= last;
            last = actual_val;
        }

        let mut vint = VIntArrayEncodeMostCommon::default();
        vint.encode_vals(&data);
        vint.serialize()
    // }

}

impl<T: AnchorScoreDataSize> Default for TokenToAnchorScoreVintFlushing<T> {
    fn default() -> TokenToAnchorScoreVintFlushing<T> {
        TokenToAnchorScoreVintFlushing::new("".to_string(), "".to_string())
    }
}

impl<T: AnchorScoreDataSize> TokenToAnchorScoreVintFlushing<T> {
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
            metadata: IndexValuesMetadata::default(),
        }
    }

    pub fn set_scores(&mut self, id: u32, mut add_data: &mut [u32]) -> Result<(), io::Error> {
        let id_pos = id as usize - self.current_id_offset as usize;

        if self.id_to_data_pos.len() <= id_pos {
            //TODO this could become very big, check memory consumption upfront, and flush directly to disk, when a resize would step over a certain threshold @Memory
            self.id_to_data_pos.resize(id_pos + 1, num::cast(EMPTY_BUCKET).unwrap());
        }

        self.metadata.num_values += add_data.len() as u64 / 2; // 1/2 because the array is docid/score tuples
        self.metadata.num_ids += 1;
        // self.id_to_data_pos[id_pos] = self.current_data_offset + self.data_cache.len() as u32;

        self.id_to_data_pos[id_pos] = self.current_data_offset + num::cast(self.data_cache.len()).unwrap();
        self.data_cache.extend(compress_data_block(&mut add_data));

        if self.id_to_data_pos.len() + self.data_cache.len() >= 1_000_000 {
            self.flush()?;
        }
        Ok(())
    }

    #[inline]
    pub fn is_in_memory(&self) -> bool {
        self.current_id_offset == 0
    }

    pub fn into_store(mut self) -> Result<Box<dyn TokenToAnchorScore>, VelociError> {
        if self.is_in_memory() {
            Ok(Box::new(self.into_im_store()))
        } else {
            self.flush()?;
            Ok(Box::new(self.into_mmap()?))
        }
    }

    pub fn into_im_store(self) -> TokenToAnchorScoreVintIM<T> {
        TokenToAnchorScoreVintIM {
            start_pos: self.id_to_data_pos, //TODO
            data: self.data_cache,
        }
    }

    pub fn into_mmap(self) -> Result<(TokenToAnchorScoreVintMmap<T>), VelociError> {
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
        let id_to_data_pos_bytes = unsafe { slice::from_raw_parts(self.id_to_data_pos.as_ptr() as *const u8, self.id_to_data_pos.len() * mem::size_of::<T>()) };

        // persistence_data_indirect::flush_to_file_indirect(&self.indirect_path, &self.data_path, &vec_to_bytes_u32(&self.id_to_data_pos), &self.data_cache)?;
        persistence_data_indirect::flush_to_file_indirect(&self.indirect_path, &self.data_path, id_to_data_pos_bytes, &self.data_cache)?;

        self.data_cache.clear();
        self.id_to_data_pos.clear();

        self.metadata.avg_join_size = persistence_data_indirect::calc_avg_join_size(self.metadata.num_values, self.metadata.num_ids);

        Ok(())
    }
}

impl<T: AnchorScoreDataSize> TokenToAnchorScoreVintIM<T> {
    #[inline]
    fn get_size(&self) -> usize {
        self.start_pos.len()
    }

    pub(crate) fn read<P: AsRef<Path> + std::fmt::Debug>(&mut self, path_indirect: P, path_data: P) -> Result<(), VelociError> {
        //TODO THIS IS WEIRD
        if mem::size_of::<T>() == mem::size_of::<u32>() {
            self.start_pos = load_index_u32(&path_indirect)?.into_iter().map(|el| num::cast(el).unwrap()).collect(); //TODO REPLACE WITH SPECIALIZATION
        } else {
            self.start_pos = load_index_u64(&path_indirect)?.into_iter().map(|el| num::cast(el).unwrap()).collect(); //TODO REPLACE WITH SPECIALIZATION
        };
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

impl<T: AnchorScoreDataSize> TokenToAnchorScore for TokenToAnchorScoreVintIM<T> {
    fn get_score_iter(&self, id: u32) -> AnchorScoreIter<'_> {
        if id as usize >= self.get_size() {
            return AnchorScoreIter::new(&[]);
        }
        let pos = self.start_pos[id as usize];
        if pos.to_usize().unwrap() == EMPTY_BUCKET_USIZE {
            return AnchorScoreIter::new(&[]);
        }
        AnchorScoreIter::new(&self.data[num::cast(pos).unwrap()..])
    }
}

use crate::util::open_file;
impl<T: AnchorScoreDataSize> TokenToAnchorScoreVintMmap<T> {
    pub fn from_path(start_and_end_file: &str, data_file: &str) -> Result<Self, VelociError> {
        let start_and_end_file = unsafe { MmapOptions::new().map(&open_file(start_and_end_file)?).unwrap() };
        let data_file = unsafe { MmapOptions::new().map(&open_file(data_file)?).unwrap() };
        Ok(TokenToAnchorScoreVintMmap {
            start_pos: start_and_end_file,
            data: data_file,
            max_value_id: 0,
            ok: std::marker::PhantomData,
        })
    }
}

// impl<T: AnchorScoreDataSize> HeapSizeOf for TokenToAnchorScoreVintMmap<T> {
//     fn heap_size_of_children(&self) -> usize {
//         8
//     }
// }

impl<T: AnchorScoreDataSize> TokenToAnchorScore for TokenToAnchorScoreVintMmap<T> {
    fn get_score_iter(&self, id: u32) -> AnchorScoreIter<'_> {
        if id as usize >= self.start_pos.len() / mem::size_of::<u32>() {
            return AnchorScoreIter::new(&[]);
        }
        let pos = if mem::size_of::<T>() == mem::size_of::<u32>() {
            get_u32_from_bytes(&self.start_pos, id as usize * 4) as usize
        } else {
            get_u64_from_bytes(&self.start_pos, id as usize * 8) as usize
        };
        // let pos = get_u32_from_bytes(&self.start_pos, id as usize * 4);
        if pos == EMPTY_BUCKET_USIZE {
            return AnchorScoreIter::new(&[]);
        }
        AnchorScoreIter::new(&self.data[pos..])
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
