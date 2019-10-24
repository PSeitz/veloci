use crate::{
    facet::*,
    indices::{metadata::IndexValuesMetadata, EMPTY_BUCKET},
    persistence::*,
    type_info::TypeInfo,
};
use fnv::FnvHashMap;
use num;
use std::{self, marker::PhantomData, u32};

#[derive(Debug, Default)]
pub(crate) struct SingleArrayIM<T: IndexIdToParentData, K: IndexIdToParentData> {
    pub(crate) data: Vec<K>,
    pub(crate) ok: PhantomData<T>,
    pub(crate) metadata: IndexValuesMetadata,
}

impl<T: IndexIdToParentData, K: IndexIdToParentData> TypeInfo for SingleArrayIM<T, K> {
    fn type_name(&self) -> String {
        std::intrinsics::type_name::<Self>().to_string()
    }
}

impl<T: IndexIdToParentData, K: IndexIdToParentData> IndexIdToParent for SingleArrayIM<T, K> {
    type Output = T;

    fn get_index_meta_data(&self) -> &IndexValuesMetadata {
        &self.metadata
    }

    #[inline]
    fn count_values_for_ids(&self, ids: &[u32], top: Option<u32>) -> FnvHashMap<T, usize> {
        count_values_for_ids(ids, top, &self.metadata, |id: u64| self.get_value(id))
    }

    // fn get_keys(&self) -> Vec<T> {
    //     (num::cast(0).unwrap()..num::cast(self.data.len()).unwrap()).collect()
    // }

    #[inline]
    fn get_values_iter(&self, id: u64) -> VintArrayIteratorOpt<'_> {
        if let Some(val) = self.get_value(id) {
            VintArrayIteratorOpt::from_single_val(num::cast(val).unwrap())
        } else {
            VintArrayIteratorOpt::empty()
        }
    }

    fn get_value(&self, id: u64) -> Option<T> {
        let val = self.data.get(id as usize);
        match val {
            Some(val) => {
                if val.to_u32().unwrap_or_else(|| panic!("could not cast to u32 {:?}", val)) == EMPTY_BUCKET {
                    None
                } else {
                    Some(num::cast(*val - K::one()).unwrap_or_else(|| panic!("could not cast to u32 {:?}", *val - K::one())))
                }
            }
            None => None,
        }
    }

    #[inline]
    fn get_values(&self, id: u64) -> Option<Vec<T>> {
        self.get_value(id).map(|el| vec![el])
    }

    // #[inline]
    // fn get_num_keys(&self) -> usize {
    //     self.data.len()
    // }
}

#[inline]
fn count_values_for_ids_for_agg<C: AggregationCollector<T>, T: IndexIdToParentData, F>(ids: &[u32], top: Option<u32>, mut coll: C, get_value: F) -> FnvHashMap<T, usize>
where
    F: Fn(u64) -> Option<T>,
{
    for id in ids {
        if let Some(hit) = get_value(u64::from(*id)) {
            coll.add(hit);
        }
    }
    Box::new(coll).to_map(top)
}

#[inline]
fn count_values_for_ids<F, T: IndexIdToParentData>(ids: &[u32], top: Option<u32>, metadata: &IndexValuesMetadata, get_value: F) -> FnvHashMap<T, usize>
where
    F: Fn(u64) -> Option<T>,
{
    if should_prefer_vec(ids.len() as u32, metadata.avg_join_size, metadata.max_value_id) {
        let mut dat = vec![];
        dat.resize(metadata.max_value_id as usize + 1, T::zero());
        count_values_for_ids_for_agg(ids, top, dat, get_value)
    } else {
        let map = FnvHashMap::default();
        // map.reserve((ids.len() as f32 * avg_join_size) as usize); TODO TO PROPERLY RESERVE HERE, NUMBER OF DISTINCT VALUES IS NEEDED IN THE INDEX
        count_values_for_ids_for_agg(ids, top, map, get_value)
    }
}
