use super::*;
use crate::{error::VelociError, indices::*, persistence::*, type_info::TypeInfo};
use byteorder::{LittleEndian, ReadBytesExt};
use num::{self};
use ownedbytes::OwnedBytes;
use std::{self, marker::PhantomData, u32, usize};
use vint32::iterator::VintArrayIterator;

impl_type_info_single_templ!(Indirect);

#[derive(Debug)]
pub(crate) struct Indirect<T: IndexIdToParentData> {
    pub(crate) start_pos: OwnedBytes,
    pub(crate) data: OwnedBytes,
    pub(crate) size: usize,
    pub(crate) ok: PhantomData<T>,
    pub(crate) metadata: IndexValuesMetadata,
}

impl<T: IndexIdToParentData> Indirect<T> {
    #[inline]
    fn get_size(&self) -> usize {
        self.size
    }

    pub fn from_data(start_pos: OwnedBytes, data: OwnedBytes, metadata: IndexValuesMetadata) -> Result<Self, VelociError> {
        let size = start_pos.len() / std::mem::size_of::<T>();
        Ok(Indirect {
            start_pos,
            data,
            size,
            ok: PhantomData,
            metadata,
        })
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for Indirect<T> {
    type Output = T;

    fn get_index_meta_data(&self) -> &IndexValuesMetadata {
        &self.metadata
    }

    fn get_values_iter(&self, id: u64) -> VintArrayIteratorOpt<'_> {
        if id >= self.get_size() as u64 {
            VintArrayIteratorOpt::empty()
        } else {
            let data_start_pos = (&self.start_pos[id as usize * std::mem::size_of::<T>()..id as usize * std::mem::size_of::<T>() + std::mem::size_of::<T>()])
                .read_u32::<LittleEndian>()
                .unwrap();
            let data_start_pos_or_data = data_start_pos.to_u32().unwrap();
            if let Some(val) = get_encoded(data_start_pos_or_data) {
                // TODO handle u64 indices
                return VintArrayIteratorOpt {
                    single_value: i64::from(val),
                    iter: Box::new(VintArrayIterator::from_serialized_vint_array(&[])),
                };
            }
            if data_start_pos_or_data == EMPTY_BUCKET {
                return VintArrayIteratorOpt::empty();
            }
            VintArrayIteratorOpt::from_slice(&self.data[data_start_pos.to_usize().unwrap()..])
        }
    }

    fn get_values(&self, id: u64) -> Option<Vec<T>> {
        if id >= self.get_size() as u64 {
            None
        } else {
            debug_assert_eq!(std::mem::size_of::<T>(), std::mem::size_of::<u32>());
            let data_start_pos_or_data = u32::from_le_bytes(
                self.start_pos[id as usize * std::mem::size_of::<T>()..id as usize * std::mem::size_of::<T>() + std::mem::size_of::<T>()]
                    .try_into()
                    .unwrap(),
            );
            if let Some(val) = get_encoded(data_start_pos_or_data) {
                return Some(vec![num::cast(val).unwrap()]);
            }
            if data_start_pos_or_data == EMPTY_BUCKET {
                return None;
            }

            let iter = VintArrayIterator::from_serialized_vint_array(&self.data[data_start_pos_or_data as usize..]);
            let decoded_data: Vec<u32> = iter.collect();
            Some(decoded_data.iter().map(|el| num::cast(*el).unwrap()).collect())
        }
    }
}
