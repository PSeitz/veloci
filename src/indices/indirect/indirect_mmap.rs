use super::*;
use crate::{error::VelociError, indices::*, persistence::*, type_info::TypeInfo};
use byteorder::{LittleEndian, ReadBytesExt};
use memmap::{Mmap};
use num::{self, cast::ToPrimitive};
use std::{self, fs::File, marker::PhantomData, u32};
use vint::vint::*;
use crate::util::*;

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

// impl<T: IndexIdToParentData> HeapSizeOf for IndirectMMap<T> {
//     fn heap_size_of_children(&self) -> usize {
//         0
//     }
// }

impl<T: IndexIdToParentData> IndexIdToParent for IndirectMMap<T> {
    type Output = T;

    // fn get_keys(&self) -> Vec<T> {
    //     (num::cast(0).unwrap()..num::cast(self.get_size()).unwrap()).collect()
    // }

    fn get_index_meta_data(&self) -> &IndexValuesMetadata {
        &self.metadata
    }

    fn get_values_iter(&self, id: u64) -> VintArrayIteratorOpt<'_> {
        if id >= self.get_size() as u64 {
            VintArrayIteratorOpt {
                single_value: -2,
                iter: Box::new(VintArrayIterator::from_serialized_vint_array(&[])),
            }
        } else {
            let start_index = id as usize * std::mem::size_of::<T>();
            let data_start_pos = (&self.start_pos[start_index as usize..start_index + 4]).read_u32::<LittleEndian>().unwrap();
            let data_start_pos_or_data = data_start_pos.to_u32().unwrap();
            if let Some(val) = get_encoded(data_start_pos_or_data) {
                return VintArrayIteratorOpt {
                    single_value: i64::from(val),
                    iter: Box::new(VintArrayIterator::from_serialized_vint_array(&[])),
                };
            }
            if data_start_pos_or_data == EMPTY_BUCKET {
                return VintArrayIteratorOpt {
                    single_value: -2,
                    iter: Box::new(VintArrayIterator::from_serialized_vint_array(&[])),
                };
            }
            VintArrayIteratorOpt {
                single_value: -1,
                iter: Box::new(VintArrayIterator::from_serialized_vint_array(&self.data[data_start_pos.to_usize().unwrap()..])),
            }
        }
    }

    default fn get_values(&self, id: u64) -> Option<Vec<T>> {
        get_u32_values_from_pointing_mmap_file_vint(
            //FIXME BUG BUG if file is not u32
            id,
            self.get_size(),
            &self.start_pos,
            &self.data,
        )
        .map(|el| el.iter().map(|el| num::cast(*el).unwrap()).collect())
    }
}

#[inline(always)]
fn get_u32_values_from_pointing_mmap_file_vint(id: u64, size: usize, start_pos: &Mmap, data: &Mmap) -> Option<Vec<u32>> {
    if id >= size as u64 {
        None
    } else {
        let start_index = id as usize * std::mem::size_of::<u32>();
        let data_start_pos = (&start_pos[start_index as usize..start_index + std::mem::size_of::<u32>()])
            .read_u32::<LittleEndian>()
            .unwrap();

        let data_start_pos_or_data = data_start_pos.to_u32().unwrap();
        if let Some(val) = get_encoded(data_start_pos_or_data) {
            return Some(vec![num::cast(val).unwrap()]);
        }
        if data_start_pos_or_data == EMPTY_BUCKET {
            return None;
        }

        let iter = VintArrayIterator::from_serialized_vint_array(&data[data_start_pos as usize..]);
        let decoded_data: Vec<u32> = iter.collect();
        Some(decoded_data)
    }
}
