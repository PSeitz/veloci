use vint32::vint_array::VIntArray;
use super::super::{calc_avg_join_size, flush_to_file_indirect};
use crate::{error::VelociError, indices::*, persistence::*, util::*};
use num::{self, cast::ToPrimitive};
use std::{self, io, path::PathBuf, u32};

fn to_serialized_vint_array(add_data: Vec<u32>) -> Vec<u8> {
    let mut vint = VIntArray::default();
    for el in add_data {
        vint.encode(el);
    }
    vint.serialize()
}

/// This data structure assumes that a set is only called once for a id, and ids are set in order.
#[derive(Debug, Clone)]
pub(crate) struct IndirectIMFlushingInOrderVint {
    pub(crate) ids_cache: Vec<u32>,
    pub(crate) data_cache: Vec<u8>,
    pub(crate) current_data_offset: u32,
    /// Already written ids_cache
    pub(crate) current_id_offset: u32,
    pub(crate) path: PathBuf,
    pub(crate) metadata: IndexValuesMetadata,
}

// use vint for indirect, use not highest bit in indirect, but the highest unused bit. Max(value_id, single data_id, which would be encoded in the valueid index)
//
impl IndirectIMFlushingInOrderVint {
    pub(crate) fn new(path: PathBuf, max_value_id: u32) -> Self {
        let mut data_cache = vec![];
        data_cache.resize(1, 0); // resize data by one, because 0 is reserved for the empty buckets
        IndirectIMFlushingInOrderVint {
            ids_cache: vec![],
            data_cache,
            current_data_offset: 0,
            current_id_offset: 0,
            path,
            metadata: IndexValuesMetadata::new(max_value_id),
        }
    }

    pub(crate) fn into_im_store(mut self) -> IndirectIM<u32> {
        let mut store = IndirectIM::default();
        self.metadata.avg_join_size = calc_avg_join_size(self.metadata.num_values, self.metadata.num_ids);
        store.start_pos = self.ids_cache;
        store.metadata = self.metadata;
        store.data = self.data_cache;
        store
    }

    pub(crate) fn into_store(mut self) -> Result<Box<dyn IndexIdToParent<Output = u32>>, VelociError> {
        if self.is_in_memory() {
            Ok(Box::new(self.into_im_store()))
        } else {
            self.flush()?;
            let store = IndirectMMap::from_path(&self.path, self.metadata)?;
            Ok(Box::new(store))
        }
    }

    #[inline]
    pub(crate) fn add(&mut self, id: u32, add_data: Vec<u32>) -> Result<(), io::Error> {
        self.metadata.num_values += 1;
        self.metadata.num_ids += add_data.len() as u32;

        let id_pos = (id - self.current_id_offset) as usize;
        if self.ids_cache.len() <= id_pos {
            //TODO this could become very big, check memory consumption upfront, and flush directly to disk, when a resize would step over a certain threshold @Memory
            self.ids_cache.resize(id_pos + 1, EMPTY_BUCKET);
        }

        if add_data.len() == 1 {
            let mut val: u32 = add_data[0].to_u32().unwrap();
            set_high_bit(&mut val); // encode directly in indirect index, much wow, much compression, gg memory consumption
            self.ids_cache[id_pos] = val;
        } else if let Some(pos_in_data) = (self.current_data_offset as usize + self.data_cache.len()).to_u32() {
            self.ids_cache[id_pos] = pos_in_data;
            self.data_cache.extend(to_serialized_vint_array(add_data));
        } else {
            //Handle Overflow
            panic!("Too much data, can't adress with u32");
        }

        if self.ids_cache.len() * std::mem::size_of::<u32>() + self.data_cache.len() >= 4_000_000 {
            self.flush()?;
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn is_in_memory(&self) -> bool {
        self.current_id_offset == 0
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.ids_cache.is_empty() && self.current_id_offset == 0
    }

    pub(crate) fn flush(&mut self) -> Result<(), io::Error> {
        if self.ids_cache.is_empty() {
            return Ok(());
        }

        self.current_id_offset += self.ids_cache.len() as u32;
        self.current_data_offset += self.data_cache.len() as u32;

        flush_to_file_indirect(
            &(self.path.set_ext(Ext::Indirect)),
            &(self.path.set_ext(Ext::Data)),
            &vec_to_bytes(&self.ids_cache),
            &self.data_cache,
        )?;

        self.data_cache.clear();
        self.ids_cache.clear();

        self.metadata.avg_join_size = calc_avg_join_size(self.metadata.num_values, self.metadata.num_ids);

        Ok(())
    }
}
