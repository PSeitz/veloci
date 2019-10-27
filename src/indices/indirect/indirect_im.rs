use super::*;
use crate::{facet::*, indices::*, persistence::*, type_info::TypeInfo};
use fnv::FnvHashMap;
use itertools::Itertools;
use lru_time_cache::LruCache;
use num;
use std::{self, fmt, u32};
use vint::vint::*;

impl_type_info_single_templ!(IndirectIM);

#[derive(Clone)]
pub(crate) struct IndirectIM<T: IndexIdToParentData> {
    pub(crate) start_pos: Vec<T>,
    pub(crate) cache: LruCache<Vec<T>, u32>,
    pub(crate) data: Vec<u8>,
    pub(crate) metadata: IndexValuesMetadata,
}
// impl<T: IndexIdToParentData> HeapSizeOf for IndirectIM<T> {
//     fn heap_size_of_children(&self) -> usize {
//         self.start_pos.heap_size_of_children() + self.data.heap_size_of_children() + self.metadata.heap_size_of_children()
//     }
// }

impl<T: IndexIdToParentData> fmt::Debug for IndirectIM<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IndirectIM {{ start_pos: {:?}, data: {:?} }}", self.start_pos, self.data)
    }
}

impl<T: IndexIdToParentData> Default for IndirectIM<T> {
    fn default() -> IndirectIM<T> {
        let mut data = vec![];
        data.resize(1, 1); // resize data by one, because 0 is reserved for the empty buckets
        IndirectIM {
            start_pos: vec![],
            cache: LruCache::with_capacity(250),
            data,
            metadata: IndexValuesMetadata::new(0),
        }
    }
}

impl<T: IndexIdToParentData> IndirectIM<T> {
    #[inline]
    fn get_size(&self) -> usize {
        self.start_pos.len()
    }

    #[inline]
    fn count_values_for_ids_for_agg<C: AggregationCollector<T>>(&self, ids: &[u32], top: Option<u32>, mut coll: C) -> FnvHashMap<T, usize> {
        let size = self.get_size();

        let mut positions_vec = Vec::with_capacity(8);
        for id_chunk in &ids.iter().chunks(8) {
            for id in id_chunk {
                if *id >= size as u32 {
                    continue;
                }
                let pos = *id as usize;
                let data_start_pos = self.start_pos[pos];
                let data_start_pos_or_data = data_start_pos.to_u32().unwrap();
                if let Some(val) = get_encoded(data_start_pos_or_data) {
                    coll.add(num::cast(val).unwrap());
                    continue;
                }
                if data_start_pos_or_data != EMPTY_BUCKET {
                    positions_vec.push(data_start_pos_or_data);
                }
            }

            for position in &positions_vec {
                let iter = VintArrayIterator::from_serialized_vint_array(&self.data[*position as usize..]);
                for el in iter {
                    coll.add(num::cast(el).unwrap());
                }
            }
            positions_vec.clear();
        }
        Box::new(coll).to_map(top)
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for IndirectIM<T> {
    type Output = T;

    fn get_index_meta_data(&self) -> &IndexValuesMetadata {
        &self.metadata
    }

    #[inline]
    fn count_values_for_ids(&self, ids: &[u32], top: Option<u32>) -> FnvHashMap<T, usize> {
        if should_prefer_vec(ids.len() as u32, self.metadata.avg_join_size, self.metadata.max_value_id) {
            let mut dat = vec![];
            dat.resize(self.metadata.max_value_id as usize + 1, T::zero());
            self.count_values_for_ids_for_agg(ids, top, dat)
        } else {
            let map = FnvHashMap::default();
            // map.reserve((ids.len() as f32 * self.metadata.avg_join_size) as usize);  TODO TO PROPERLY RESERVE HERE, NUMBER OF DISTINCT VALUES IS NEEDED IN THE INDEX
            self.count_values_for_ids_for_agg(ids, top, map)
        }
    }

    // fn get_keys(&self) -> Vec<T> {
    //     (num::cast(0).unwrap()..num::cast(self.get_size()).unwrap()).collect()
    // }

    fn get_values_iter(&self, id: u64) -> VintArrayIteratorOpt<'_> {
        get_values_iter!(self, id, self.data, {self.start_pos[id as usize]})
    }

    #[inline]
    default fn get_values(&self, id: u64) -> Option<Vec<T>> {
        get_values!(self, id, self.data, {self.start_pos[id as usize]})
    }
}
