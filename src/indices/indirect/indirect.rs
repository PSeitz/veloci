use super::*;
use crate::{error::VelociError, indices::*, persistence::*, type_info::TypeInfo};
use byteorder::{LittleEndian, ReadBytesExt};
use num::{self, cast::ToPrimitive};
use ownedbytes::OwnedBytes;
use std::{self, marker::PhantomData, u32};
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
        get_values_iter!(self, id, self.data, {
            (&self.start_pos[id as usize * std::mem::size_of::<T>()..id as usize * std::mem::size_of::<T>() + std::mem::size_of::<T>()])
                .read_u32::<LittleEndian>()
                .unwrap()
        })
    }

    fn get_values(&self, id: u64) -> Option<Vec<T>> {
        get_values!(self, id, self.data, {
            (&self.start_pos[id as usize * std::mem::size_of::<T>()..id as usize * std::mem::size_of::<T>() + std::mem::size_of::<T>()])
                .read_u32::<LittleEndian>()
                .unwrap()
        })
    }
}
