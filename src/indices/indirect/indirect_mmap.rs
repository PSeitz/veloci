use super::*;
use crate::{error::VelociError, indices::*, persistence::*, type_info::TypeInfo, util::*};
use byteorder::{LittleEndian, ReadBytesExt};
use memmap2::Mmap;
use num::{self, cast::ToPrimitive};
use std::{self, fs::File, marker::PhantomData, u32};
use vint32::iterator::VintArrayIterator;

impl_type_info_single_templ!(IndirectMMap);

#[derive(Debug)]
pub(crate) struct IndirectMMap<T: IndexIdToParentData> {
    pub(crate) start_pos: Mmap,
    pub(crate) data: Mmap,
    pub(crate) size: usize,
    pub(crate) ok: PhantomData<T>,
    pub(crate) metadata: IndexValuesMetadata,
}

impl<T: IndexIdToParentData> IndirectMMap<T> {
    #[inline]
    fn get_size(&self) -> usize {
        self.size
    }

    pub(crate) fn from_path<P: AsRef<Path>>(path: P, metadata: IndexValuesMetadata) -> Result<Self, VelociError> {
        Ok(IndirectMMap {
            start_pos: mmap_from_path(path.as_ref().set_ext(Ext::Indirect))?,
            data: mmap_from_path(path.as_ref().set_ext(Ext::Data))?,
            size: File::open(path.as_ref().set_ext(Ext::Indirect))?.metadata()?.len() as usize / std::mem::size_of::<T>(),
            ok: PhantomData,
            metadata,
        })
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for IndirectMMap<T> {
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
