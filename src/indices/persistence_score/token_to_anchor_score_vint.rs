use super::{
    super::{EMPTY_BUCKET, EMPTY_BUCKET_USIZE},
    *,
};
use ownedbytes::OwnedBytes;
use std::path::PathBuf;
use vint32::common_encode::{VIntArrayEncodeMostCommon, VintArrayMostCommonIterator};

use crate::{
    directory::Directory,
    error::VelociError,
    indices::{calc_avg_join_size, flush_to_file_indirect, *},
    util::*,
};
use itertools::Itertools;

use std::{self, io, iter::FusedIterator, marker::PhantomData, mem, ops};

pub trait AnchorScoreDataSize: IndexIdToParentData + ops::AddAssign + ops::Add + num::Zero {}
impl<T> AnchorScoreDataSize for T where T: IndexIdToParentData + ops::AddAssign + ops::Add + num::Zero {}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TokenToAnchorScoreVintIM<T> {
    pub start_pos: Vec<T>,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct TokenToAnchorScoreVint<T> {
    pub start_pos: OwnedBytes,
    pub data: OwnedBytes,
    pub max_value_id: u32,
    pub ok: PhantomData<T>,
}

impl<T: AnchorScoreDataSize> TypeInfo for TokenToAnchorScoreVint<T> {
    fn type_name(&self) -> String {
        std::any::type_name::<Self>().to_string()
    }
}

///
/// Datastructure to cache and flush changes to file
///
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenToAnchorScoreVintFlushing<T: AnchorScoreDataSize> {
    pub field_path: PathBuf,
    pub id_to_data_pos: Vec<T>,
    pub data_cache: Vec<u8>,
    pub current_data_offset: T,
    /// Already written id_to_data_pos
    pub current_id_offset: u32,
    pub indirect_path: PathBuf,
    pub data_path: PathBuf,
    pub metadata: IndexValuesMetadata,
}

fn delta_compress_data_block(data: &mut [u32]) -> Vec<u8> {
    let mut last = 0;
    for (el, _score) in data.iter_mut().tuples() {
        let actual_val = *el;
        *el -= last;
        last = actual_val;
    }

    let mut vint = VIntArrayEncodeMostCommon::default();
    vint.encode_vals(data);
    vint.serialize()
}

impl<T: AnchorScoreDataSize> TokenToAnchorScoreVintFlushing<T> {
    pub fn new2(field_path: String, indirect_path: PathBuf, data_path: PathBuf) -> Self {
        let mut data_cache = vec![];
        data_cache.resize(1, 0); // resize data by one, because 0 is reserved for the empty buckets
        TokenToAnchorScoreVintFlushing {
            field_path: PathBuf::from(field_path),
            id_to_data_pos: vec![],
            data_cache,
            current_data_offset: T::zero(),
            current_id_offset: 0,
            indirect_path,
            data_path,
            metadata: IndexValuesMetadata::default(),
        }
    }

    pub fn set_scores(&mut self, id: u32, add_data: &mut [u32]) -> Result<(), io::Error> {
        let id_pos = id as usize - self.current_id_offset as usize;

        if self.id_to_data_pos.len() <= id_pos {
            //TODO this could become very big, check memory consumption upfront, and flush directly to disk, when a resize would step over a certain threshold @Memory
            self.id_to_data_pos.resize(id_pos + 1, num::cast(EMPTY_BUCKET).unwrap());
        }

        self.metadata.num_values += add_data.len() as u64 / 2; // 1/2 because the array is docid/score tuples
        self.metadata.num_ids += 1;
        // self.id_to_data_pos[id_pos] = self.current_data_offset + self.data_cache.len() as u32;

        self.id_to_data_pos[id_pos] = self.current_data_offset + num::cast(self.data_cache.len()).unwrap();
        self.data_cache.extend(delta_compress_data_block(add_data));

        if self.id_to_data_pos.len() + self.data_cache.len() >= 1_000_000 {
            self.flush()?;
        }
        Ok(())
    }

    #[inline]
    pub fn is_in_memory(&self) -> bool {
        self.current_id_offset == 0
    }

    pub fn into_store(mut self, directory: &Box<dyn Directory>) -> Result<Box<dyn TokenToAnchorScore>, VelociError> {
        self.flush()?;
        Ok(Box::new(self.load_from_disk(directory)?))
    }

    pub fn load_from_disk(self, directory: &Box<dyn Directory>) -> Result<TokenToAnchorScoreVint<T>, VelociError> {
        //TODO MAX VALUE ID IS NOT SET
        let data_path = self.field_path.set_ext(Ext::Data);
        let indirect_path = self.field_path.set_ext(Ext::Indirect);
        TokenToAnchorScoreVint::from_data(directory.get_file_bytes(&indirect_path)?, directory.get_file_bytes(&data_path)?)
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
        flush_to_file_indirect(&self.indirect_path, &self.data_path, id_to_data_pos_bytes, &self.data_cache)?;

        self.data_cache.clear();
        self.id_to_data_pos.clear();

        self.metadata.avg_join_size = calc_avg_join_size(self.metadata.num_values, self.metadata.num_ids);

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
            vint_iter: VintArrayMostCommonIterator::from_slice(data),
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

impl<T: AnchorScoreDataSize> TokenToAnchorScoreVint<T> {
    pub fn from_data(start_pos: OwnedBytes, data: OwnedBytes) -> Result<Self, VelociError> {
        Ok(TokenToAnchorScoreVint {
            start_pos,
            data,
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

impl<T: AnchorScoreDataSize> TokenToAnchorScore for TokenToAnchorScoreVint<T> {
    fn get_score_iter(&self, id: u32) -> AnchorScoreIter<'_> {
        if id as usize >= self.start_pos.len() / mem::size_of::<T>() {
            return AnchorScoreIter::new(&[]);
        }
        let pos = if mem::size_of::<T>() == mem::size_of::<u32>() {
            get_u32_from_bytes(&self.start_pos, id as usize * mem::size_of::<T>()) as usize
        } else {
            get_u64_from_bytes(&self.start_pos, id as usize * mem::size_of::<T>()) as usize
        };
        // let pos = get_u32_from_bytes(&self.start_pos, id as usize * 4);
        if pos == EMPTY_BUCKET_USIZE {
            return AnchorScoreIter::new(&[]);
        }
        AnchorScoreIter::new(&self.data[pos..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    //fn test_token_to_anchor_score_vint<T: AnchorScoreDataSize>(store: TokenToAnchorScoreVintFlushing<T>) {

    //let dir: Box<dyn Directory> = Box::new(crate::directory::MmapDirectory::open(dir.path()).unwrap());

    //// test im
    //store.set_scores(1, &mut [1, 1]).unwrap();
    //let store = store.into_store(&dir);
    //assert_eq!(store.get_score_iter(0).collect::<Vec<_>>(), vec![]);
    //assert_eq!(store.get_score_iter(1).collect::<Vec<_>>(), vec![AnchorScore::new(1, f16::from_f32(1.0))]);
    //assert_eq!(store.get_score_iter(2).collect::<Vec<_>>(), vec![]);

    //let mut store = <$type1>::default();
    //store.set_scores(5, &mut [1, 1, 2, 3]).unwrap();
    //let store = store.into_store();
    //assert_eq!(store.get_score_iter(4).collect::<Vec<_>>(), vec![]);
    //assert_eq!(
    //store.get_score_iter(5).collect::<Vec<_>>(),
    //vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))]
    //);
    //for i in 6..18 {
    //assert_eq!(store.get_score_iter(i).collect::<Vec<_>>(), vec![]);
    //}

    //// test flush to file
    //let data = dir.path().join("TokenToAnchorScoreVintTestData");
    //let indirect = dir.path().join("TokenToAnchorScoreVintTestIndirect");

    //let mut store = <$type1>::new(indirect.clone(), data.clone());
    //store.set_scores(1, &mut [1, 1]).unwrap();
    //store.flush().unwrap();
    //store.set_scores(5, &mut [1, 1, 2, 3]).unwrap();
    //store.flush().unwrap();
    //store.flush().unwrap(); // double flush test

    //TODO Reenable
    //let dir: Box<dyn Directory> = Box::new(crate::directory::MmapDirectory::open(dir.path()).unwrap());

    //let store = store.into_mmap(&dir).unwrap();
    //assert_eq!(store.get_score_iter(0).collect::<Vec<_>>(), vec![]);
    //assert_eq!(store.get_score_iter(1).collect::<Vec<_>>(), vec![AnchorScore::new(1, f16::from_f32(1.0))]);
    //assert_eq!(store.get_score_iter(2).collect::<Vec<_>>(), vec![]);
    //assert_eq!(
    //store.get_score_iter(5).collect::<Vec<_>>(),
    //vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))]
    //);
    //for i in 6..18 {
    //assert_eq!(store.get_score_iter(i).collect::<Vec<_>>(), vec![]);
    //}

    //let store = <$type2>::from_path(&indirect, &data).unwrap();
    //assert_eq!(store.get_score_iter(0).collect::<Vec<_>>(), vec![]);
    //assert_eq!(store.get_score_iter(1).collect::<Vec<_>>(), vec![AnchorScore::new(1, f16::from_f32(1.0))]);
    //assert_eq!(store.get_score_iter(2).collect::<Vec<_>>(), vec![]);
    //assert_eq!(
    //store.get_score_iter(5).collect::<Vec<_>>(),
    //vec![AnchorScore::new(1, f16::from_f32(1.0)), AnchorScore::new(2, f16::from_f32(3.0))]
    //);
    //for i in 6..18 {
    //assert_eq!(store.get_score_iter(i).collect::<Vec<_>>(), vec![]);
    //}
    //}

    //#[test]
    //fn test_token_to_anchor_score_vint_u32() {
    //use tempfile::tempdir;
    //let dir = tempdir().unwrap();

    //test_token_to_anchor_score_vint(TokenToAnchorScoreVintFlushing::<u32>);
    //}
    //#[test]
    //fn test_token_to_anchor_score_vint_u64() {
    //use tempfile::tempdir;
    //let dir = tempdir().unwrap();

    //test_token_to_anchor_score_vint(TokenToAnchorScoreVintFlushing::<u64>);
    //}
}
