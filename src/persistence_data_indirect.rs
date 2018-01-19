use std;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::cmp::Ordering;

#[allow(unused_imports)]
use heapsize::{heap_size_of, HeapSizeOf};
#[allow(unused_imports)]
use bincode::{deserialize, serialize, Infinite};

#[allow(unused_imports)]
use util::*;

use persistence::*;
use persistence_data::TypeInfo;
#[allow(unused_imports)]
use persistence;
use create;
use mayda;
use snap;
#[allow(unused_imports)]
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[allow(unused_imports)]
use mayda::{Access, AccessInto, Encode, Uniform};
use parking_lot::Mutex;
use lru_cache::LruCache;

use std::io::Cursor;
use std::fs;
#[allow(unused_imports)]
use std::fmt::Debug;
use num::cast::ToPrimitive;
use num::{Integer, NumCast};
use std::marker::PhantomData;

#[allow(unused_imports)]
use fnv::FnvHashMap;
#[allow(unused_imports)]
use fnv::FnvHashSet;


macro_rules! mut_if {
    ($name:ident = $value:expr, $($any:expr)+) => (let mut $name = $value;);
    ($name:ident = $value:expr,) => (let $name = $value;);
}

macro_rules! impl_type_info_single_templ {
    ($name:ident$(<$($T:ident),+>)*) => {
        impl<D: IndexIdToParentData>$(<$($T: TypeInfo),*>)* TypeInfo for $name<D>$(<$($T),*>)* {
            fn type_name(&self) -> String {
                mut_if!(res = String::from(stringify!($name)), $($($T)*)*);
                $(
                    res.push('<');
                    $(
                        res.push_str(&$T::type_name(&self));
                        res.push(',');
                    )*
                    res.pop();
                    res.push('>');
                )*
                res
            }
            fn type_of(&self) -> String {
                $name$(::<$($T),*>)*::type_name(&self)
            }
        }
    }
}

impl_type_info_single_templ!(IndexIdToMultipleParentCompressedMaydaINDIRECTOne);
impl_type_info_single_templ!(IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse);
impl_type_info_single_templ!(IndexIdToMultipleParentIndirect);
impl_type_info_single_templ!(PointingArrayFileReader);


#[derive(Serialize, Deserialize, Debug, HeapSizeOf)]
pub struct IndexIdToMultipleParentIndirect<T: IndexIdToParentData> {
    pub start_and_end: Vec<T>,
    pub data: Vec<T>,
}
impl<T: IndexIdToParentData> IndexIdToMultipleParentIndirect<T> {
    #[allow(dead_code)]
    pub fn new(data: &IndexIdToParent<Output = T>) -> IndexIdToMultipleParentIndirect<T> {
        IndexIdToMultipleParentIndirect::new_sort_and_dedup(data, false)
    }
    #[allow(dead_code)]
    pub fn new_sort_and_dedup(data: &IndexIdToParent<Output = T>, sort_and_dedup: bool) -> IndexIdToMultipleParentIndirect<T> {
        let (start_and_end_pos, data) = to_indirect_arrays_dedup(data, 0, sort_and_dedup);
        IndexIdToMultipleParentIndirect {
            start_and_end: start_and_end_pos,
            data,
        }
    }
    #[allow(dead_code)]
    pub fn from_data(start_and_end: Vec<T>, data: Vec<T>) -> IndexIdToMultipleParentIndirect<T> {
        IndexIdToMultipleParentIndirect {
            start_and_end,
            data,
        }
    }
    fn get_size(&self) -> usize {
        self.start_and_end.len() / 2
    }
}
use itertools::Itertools;
impl<T: IndexIdToParentData> IndexIdToParent for IndexIdToMultipleParentIndirect<T> {
    type Output = T;

    #[inline]
    default fn get_values(&self, id: u64) -> Option<Vec<T>> {
        if id >= self.get_size() as u64 {
            None
        } else {
            let positions = &self.start_and_end[(id * 2) as usize..=((id * 2) as usize + 1)];
            if positions[0] == positions[1] {
                return None;
            }
            Some(
                self.data[NumCast::from(positions[0]).unwrap()..NumCast::from(positions[1]).unwrap()]
                    .iter()
                    .map(|el| NumCast::from(*el).unwrap())
                    .collect(),
            )
        }
    }
    fn get_keys(&self) -> Vec<T> {
        (NumCast::from(0).unwrap()..NumCast::from(self.get_size()).unwrap()).collect()
    }

    #[inline]
    fn count_values_for_ids(&self, ids: &[u32], hits: &mut FnvHashMap<T, usize>){
        let size = self.get_size();

        let mut positions_vec = Vec::with_capacity(8);
        for id_chunk in &ids.into_iter().chunks(8) {
            // println!("id {:?}", id);
            for id in id_chunk {
                if *id >= size as u32 {
                    continue;
                } else {
                    let pos = (*id * 2) as usize;
                    let positions = &self.start_and_end[pos..=pos + 1];
                    if positions[0] != positions[1]{
                        positions_vec.push(positions);
                    }
                }
            }

            for position in positions_vec.iter() {
                for hit_id in &self.data[NumCast::from(position[0]).unwrap()..NumCast::from(position[1]).unwrap()] {
                    let stat = hits.entry(*hit_id).or_insert(0);
                    *stat += 1;
                }
            }
            positions_vec.clear();
        }
    }

}



#[derive(Debug, HeapSizeOf)]
#[allow(dead_code)]
pub struct IndexIdToMultipleParentCompressedMaydaINDIRECTOne<T: IndexIdToParentData> {
    pub start_and_end: mayda::Monotone<T>,
    pub data: mayda::Uniform<T>,
    pub size: usize,
}

impl<T: IndexIdToParentData> IndexIdToMultipleParentCompressedMaydaINDIRECTOne<T> {
    #[allow(dead_code)]
    pub fn new(store: &IndexIdToParent<Output = T>) -> IndexIdToMultipleParentCompressedMaydaINDIRECTOne<T> {
        let (size, start_and_end, data) = id_to_parent_to_array_of_array_mayda_indirect_one(store);
        info!(
            "start_and_end {}",
            get_readable_size(start_and_end.heap_size_of_children())
        );
        info!("data {}", get_readable_size(data.heap_size_of_children()));
        IndexIdToMultipleParentCompressedMaydaINDIRECTOne {
            start_and_end,
            data,
            size,
        }
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for IndexIdToMultipleParentCompressedMaydaINDIRECTOne<T> {
    type Output = T;
    #[inline]
    fn get_values(&self, id: u64) -> Option<Vec<T>> {
        let mut positions:Vec<T> = vec![];
        positions.resize(2, T::zero());
        get_values_indirect_generic(id, self.size as u64, &self.start_and_end, &self.data, &mut positions)
    }

    fn get_keys(&self) -> Vec<T> {
        (NumCast::from(0).unwrap()..NumCast::from(self.start_and_end.len() / 2).unwrap()).collect()
    }

    #[inline]
    fn append_values_for_ids(&self, ids: &[u32], vec: &mut Vec<T>){
        let mut positions:Vec<T> = vec![];
        positions.resize(2, T::zero());
        for id in ids {

            if *id >= self.size as u32 {
                continue;
            } else {
                self.start_and_end.access_into((*id * 2) as usize..=((*id * 2) as usize + 1), &mut positions[0..=1]);
                if positions[0] == positions[1] {
                    continue;
                }
                let current_len = vec.len();
                vec.resize(current_len + positions[1].to_usize().unwrap() - positions[0].to_usize().unwrap(), T::zero());
                let new_len = vec.len();

                self.data.access_into(NumCast::from(positions[0]).unwrap()..NumCast::from(positions[1]).unwrap(), &mut vec[current_len .. new_len]);
            }

        }
    }

    #[inline]
    fn count_values_for_ids(&self, ids: &[u32], hits: &mut FnvHashMap<T, usize>){
        // let mut data_cache:Vec<T> = vec![];
        // let chunk_size = 8;
        // let mut positions_vec = Vec::with_capacity(chunk_size * 2);
        // positions_vec.resize(chunk_size * 2, T::zero());
        // let mut current_pos = 0;
        // // for id_chunk in &ids.into_iter().chunks(chunk_size) {
        // for mut x in (0..ids.len()).step_by(chunk_size) {
        //     // println!("id {:?}", id);
        //     // for id in &ids[x..x+chunk_size] {
        //     let ende = std::cmp::min(x+chunk_size, ids.len());
        //     for mut id_pos in x..ende {
        //         let id = ids[id_pos];
        //         if id >= self.size as u32 {
        //             continue;
        //         } else {
        //             let start = (id * 2) as usize;
        //             let mut end = start + 1;
        //             let mut next_continuous_id = id+1;

        //             while next_continuous_id < ids.len() as u32
        //                 && next_continuous_id < self.size as u32
        //                 && id_pos < ende
        //                 && next_continuous_id == ids[id_pos+1]
        //             {
        //                 id_pos += 1;
        //                 end = next_continuous_id as usize * 2 + 1;
        //                 next_continuous_id+=1;
        //             }

        //             if start + 1 == end {
        //                 self.start_and_end.access_into(start ..= end, &mut positions_vec[current_pos ..= current_pos+1]);
        //             }else{
        //                 let start_pos_in_data = self.start_and_end.access(start);
        //                 let end_pos_in_data = self.start_and_end.access(end);
        //                 positions_vec[current_pos] = start_pos_in_data;
        //                 positions_vec[current_pos+1] = end_pos_in_data;
        //                 print!("start_pos_in_data {:?}", start_pos_in_data);
        //                 print!("end_pos_in_data {:?}", end_pos_in_data);
        //             }

        //             if positions_vec[current_pos] != positions_vec[current_pos+1]{ // skip data with no values
        //                 current_pos += 2;
        //             }
        //         }
        //     }

        //     for x in (0..current_pos).step_by(2) {
        //         let end_pos_data = positions_vec[x+1].to_usize().unwrap();
        //         let start_pos_data = positions_vec[x].to_usize().unwrap();
        //         data_cache.resize(end_pos_data - start_pos_data, T::zero());
        //         let new_len = data_cache.len();

        //         self.data.access_into(start_pos_data..end_pos_data, &mut data_cache[0 .. new_len]);

        //         for id in data_cache.iter() {
        //             let stat = hits.entry(*id).or_insert(0);
        //             *stat += 1;
        //         }
        //     }
        //     current_pos=0;
        //     // x+=8;
        // }


        // let mut agg_hits = vec![];
        // agg_hits.resize(256, 0);

        // let mut positions:Vec<T> = vec![];
        // positions.resize(2, T::zero());
        // let mut data_cache:Vec<T> = vec![];
        // let mut iter = ids.iter().peekable();
        // while let Some(id) = iter.next(){

        //     if *id >= self.size as u32 {
        //         continue;
        //     } else {

        //         let mut end_id = *id;
        //         let mut continuous_id = end_id+1;
        //         loop{
        //             if Some(&&continuous_id) == iter.peek(){
        //                 let next = iter.next().unwrap() + 1;
        //                 if next >= self.size as u32 {
        //                     continue;
        //                 }
        //                 end_id = next;
        //                 continuous_id = end_id+1;
        //             }
        //             else{
        //                 break;
        //             }
        //             if end_id - *id > 64 {
        //                 break; //group max 64 items
        //             }
        //         }

        //         if *id == end_id {
        //             self.start_and_end.access_into((*id * 2) as usize..=((*id * 2) as usize + 1), &mut positions[0..=1]);
        //         }else{
        //             let start_pos_in_data = self.start_and_end.access((*id * 2) as usize);
        //             let end_pos_in_data = self.start_and_end.access((end_id * 2) as usize + 1);
        //             positions[0] = start_pos_in_data;
        //             positions[1] = end_pos_in_data;
        //         }

        //         if positions[0] == positions[1] {
        //             continue;
        //         }

        //         // let current_len = data_cache.len();
        //         data_cache.resize(positions[1].to_usize().unwrap() - positions[0].to_usize().unwrap(), T::zero());
        //         let new_len = data_cache.len();

        //         self.data.access_into(NumCast::from(positions[0]).unwrap()..NumCast::from(positions[1]).unwrap(), &mut data_cache[0 .. new_len]);
        //         for id in data_cache.iter() {
        //             // let stat = hits.entry(*id).or_insert(0);
        //             // *stat += 1;
        //             agg_hits[id.to_usize().unwrap()] += 1;
        //         }

        //     }

        // }






        let mut positions:Vec<T> = vec![];
        positions.resize(2, T::zero());
        let mut data_cache:Vec<T> = vec![];
        for id in ids {

            if *id >= self.size as u32 {
                continue;
            } else {
                self.start_and_end.access_into((*id * 2) as usize..=((*id * 2) as usize + 1), &mut positions[0..=1]);
                if positions[0] == positions[1] {
                    continue;
                }

                // let current_len = data_cache.len();
                data_cache.resize(positions[1].to_usize().unwrap() - positions[0].to_usize().unwrap(), T::zero());
                let new_len = data_cache.len();

                self.data.access_into(NumCast::from(positions[0]).unwrap()..NumCast::from(positions[1]).unwrap(), &mut data_cache[0 .. new_len]);
                for id in data_cache.iter() {
                    let stat = hits.entry(*id).or_insert(0);
                    *stat += 1;

                    // agg_hits[id.to_usize().unwrap()] += 1;
                }

            }

        }
    }

    #[inline]
    fn append_values(&self, id: u64, vec: &mut Vec<T>){
        if let Some(vals) = self.get_values(id) {
            for id in vals {
                vec.push(id);
            }
        }
    }

    #[inline]
    fn get_count_for_id(&self, id: u64) -> Option<usize> {
        if id >= self.size as u64 {
            None
        } else {
            let positions = self.start_and_end.access((id * 2) as usize..=((id * 2) as usize + 1));
            (positions[1] - positions[0]).to_usize()
        }
    }
}

// impl IndexIdToParent for IndexIdToMultipleParentCompressedMaydaINDIRECTOne<u32> {
//     type Output = u32;
//     fn get_values(&self, id: u64) -> Option<Vec<u32>> {
//         get_values_indirect(id, self.size as u64, &self.start_and_end, &self.data)
//     }
// }

// #[inline(always)]
// fn get_values_indirect<T, K>(id: u64, size:u64, start_and_end: &T, data: &K) -> Option<Vec<u32>> where
//     T: mayda::utility::Access<std::ops::RangeInclusive<usize>, Output=Vec<u32>> + mayda::utility::Access<std::ops::Range<usize>, Output=Vec<u32>>,
//     K: mayda::utility::Access<std::ops::RangeInclusive<usize>, Output=Vec<u32>> + mayda::utility::Access<std::ops::Range<usize>, Output=Vec<u32>>
//     {
//     if id >= size { None }
//     else {
//         let positions = start_and_end.access((id * 2) as usize..=((id * 2) as usize + 1));
//         if positions[0] == positions[1] {return None}

//         Some(data.access(positions[0] as usize .. positions[1] as usize))
//     }
// }

#[inline]
fn get_values_indirect_generic<T, K, M>(id: u64, size: u64, start_and_end: &T, data: &K, positions: &mut Vec<M>) -> Option<Vec<M>>
where
    T: mayda::utility::Access<std::ops::RangeInclusive<usize>, Output = Vec<M>> + mayda::utility::Access<std::ops::Range<usize>, Output = Vec<M>> + mayda::utility::AccessInto<std::ops::RangeInclusive<usize>, M> + mayda::utility::AccessInto<std::ops::Range<usize>, M>,
    K: mayda::utility::Access<std::ops::RangeInclusive<usize>, Output = Vec<M>> + mayda::utility::Access<std::ops::Range<usize>, Output = Vec<M>> + mayda::utility::AccessInto<std::ops::RangeInclusive<usize>, M> + mayda::utility::AccessInto<std::ops::Range<usize>, M>,
    M: IndexIdToParentData,
{
    if id >= size as u64 {
        None
    } else {
        start_and_end.access_into((id * 2) as usize..=((id * 2) as usize + 1), &mut positions[0..=1]);
        if positions[0] == positions[1] {
            return None;
        }

        let dat = data.access(NumCast::from(positions[0]).unwrap()..NumCast::from(positions[1]).unwrap());
        Some(dat)
    }
}

#[derive(Debug, HeapSizeOf)]
#[allow(dead_code)]
pub struct IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse<T: IndexIdToParentData> {
    start_and_end: mayda::Uniform<T>,
    data: mayda::Uniform<T>,
    size: usize,
}
impl<T: IndexIdToParentData> IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse<T> {
    #[allow(dead_code)]
    pub fn new(store: &IndexIdToParent<Output = T>) -> IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse<T> {
        let (size, start_and_end, data) = id_to_parent_to_array_of_array_mayda_indirect_one_reuse_existing(store);

        info!(
            "start_and_end {}",
            get_readable_size(start_and_end.heap_size_of_children())
        );
        info!("data {}", get_readable_size(data.heap_size_of_children()));

        IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse {
            start_and_end,
            data,
            size,
        }
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for IndexIdToMultipleParentCompressedMaydaINDIRECTOneReuse<T> {
    type Output = T;
    #[inline]
    fn get_values(&self, id: u64) -> Option<Vec<T>> {
        let mut positions = vec![];
        positions.resize(2, T::zero());
        get_values_indirect_generic(id, self.size as u64, &self.start_and_end, &self.data, &mut positions)
    }

    fn get_keys(&self) -> Vec<T> {
        (NumCast::from(0).unwrap()..NumCast::from(self.start_and_end.len() / 2).unwrap()).collect()
    }
}


#[derive(Debug)]
pub struct PointingArrayFileReader<T: IndexIdToParentData> {
    pub start_and_end_file: Mutex<fs::File>,
    pub data_file: Mutex<fs::File>,
    pub data_metadata: Mutex<fs::Metadata>,
    pub ok: PhantomData<T>,
}

impl<T: IndexIdToParentData> PointingArrayFileReader<T> {
    pub fn new(start_and_end_file: fs::File, data_file: fs::File, data_metadata: fs::Metadata) -> Self {
        PointingArrayFileReader {
            start_and_end_file: Mutex::new(start_and_end_file),
            data_file: Mutex::new(data_file),
            data_metadata: Mutex::new(data_metadata),
            ok: PhantomData,
        }
    }
    fn get_size(&self) -> usize {
        self.data_metadata.lock().len() as usize / 8
    }
}

impl<T: IndexIdToParentData> IndexIdToParent for PointingArrayFileReader<T> {
    type Output = T;
    default fn get_values(&self, find: u64) -> Option<Vec<T>> {
        get_u32_values_from_pointing_file(
            //FIXME BUG BUG
            find,
            self.get_size(),
            &self.start_and_end_file,
            &self.data_file,
        ).map(|el| el.iter().map(|el| NumCast::from(*el).unwrap()).collect())
    }
    fn get_keys(&self) -> Vec<T> {
        (NumCast::from(0).unwrap()..NumCast::from(self.get_size()).unwrap()).collect()
    }
}
impl<T: IndexIdToParentData> HeapSizeOf for PointingArrayFileReader<T> {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}

impl IndexIdToParent for PointingArrayFileReader<u32> {
    fn get_values(&self, find: u64) -> Option<Vec<u32>> {
        get_u32_values_from_pointing_file(
            find,
            self.get_size(),
            &self.start_and_end_file,
            &self.data_file,
        )
    }
}

#[inline(always)]
fn get_u32_values_from_pointing_file(find: u64, size: usize, start_and_end_file: &Mutex<fs::File>, data_file: &Mutex<fs::File>) -> Option<Vec<u32>> {
    if find >= size as u64 {
        return None;
    }
    let mut offsets: Vec<u8> = Vec::with_capacity(8);
    offsets.resize(8, 0);
    load_bytes_into(&mut offsets, &*start_and_end_file.lock(), find as u64 * 8);

    let mut rdr = Cursor::new(offsets);

    let start = rdr.read_u32::<LittleEndian>().unwrap() * 4;
    let end = rdr.read_u32::<LittleEndian>().unwrap() * 4;

    if start == end {
        return None;
    }

    let mut data_bytes: Vec<u8> = Vec::with_capacity(end as usize - start as usize);
    data_bytes.resize(end as usize - start as usize, 0);
    load_bytes_into(&mut data_bytes, &*data_file.lock(), start as u64);

    Some(bytes_to_vec_u32(&data_bytes))
}

pub fn id_to_parent_to_array_of_array<T: IndexIdToParentData>(store: &IndexIdToParent<Output = T>) -> Vec<Vec<T>> {
    let mut data: Vec<Vec<T>> = prepare_data_for_array_of_array(store, &Vec::new);
    let valids = store.get_keys();

    for valid in valids {
        if let Some(vals) = store.get_values(NumCast::from(valid).unwrap()) {
            data[valid.to_usize().unwrap()] = vals.iter().map(|el| NumCast::from(*el).unwrap()).collect();
            // vals.sort(); // WHY U SORT ?
        }
    }
    data
}

fn prepare_data_for_array_of_array<T: Clone, K: IndexIdToParentData>(store: &IndexIdToParent<Output = K>, f: &Fn() -> T) -> Vec<T> {
    let mut data = vec![];
    let mut valids = store.get_keys();
    valids.dedup();
    if valids.len() == 0 {
        return data;
    }
    data.resize(valids.last().unwrap().to_usize().unwrap() + 1, f());
    data
}

// fn prepare_data_for_array_of_array<T:IndexIdToParentData, K:>(store: &IndexIdToParent<Output=T>, f: &Fn() -> Vec<T>) -> Vec<Vec<T>> {
//     let mut data = vec![];
//     let mut valids = store.get_keys();
//     valids.dedup();
//     if valids.len() == 0 {
//         return data;
//     }
//     data.resize(*valids.last().unwrap() as usize + 1, f());
//     data

// }

//TODO TRY WITH FROM ITERATOR oder so
pub fn to_uniform<T: mayda::utility::Bits>(data: &Vec<T>) -> mayda::Uniform<T> {
    let mut uniform = mayda::Uniform::new();
    uniform.encode(&data).unwrap();
    uniform
}
pub fn to_monotone<T: mayda::utility::Bits>(data: &Vec<T>) -> mayda::Monotone<T> {
    let mut uniform = mayda::Monotone::new();
    uniform.encode(&data).unwrap();
    uniform
}

// pub fn id_to_parent_to_array_of_array_mayda_indirect(store: &IndexIdToParent) -> (usize, mayda::Uniform<u32>, mayda::Uniform<u32>, mayda::Uniform<u32>) { //start, end, data
//     let mut data = vec![];
//     let mut valids = store.get_keys();
//     valids.dedup();
//     if valids.len() == 0 {
//         return (0, mayda::Uniform::default(), mayda::Uniform::default(), mayda::Uniform::default());
//     }
//     let mut start_pos = vec![];
//     let mut end_pos = vec![];
//     start_pos.resize(*valids.last().unwrap() as usize + 1, 0);
//     end_pos.resize(*valids.last().unwrap() as usize + 1, 0);

//     let mut offset = 0;
//     // debug_time!("convert key_value_store to vec vec");

//     for valid in valids {
//         let mut vals = store.get_values(valid as u64).unwrap();
//         let start = offset;
//         data.extend(&vals);
//         offset += vals.len() as u32;

//         start_pos[valid as usize] = start;
//         end_pos[valid as usize] = offset;
//     }

//     data.shrink_to_fit();

//     (start_pos.len(), to_uniform(&start_pos), to_uniform(&end_pos), to_uniform(&data))
// }
use num;

fn to_indirect_arrays<T: Integer + Clone + NumCast + mayda::utility::Bits + Copy, K: IndexIdToParentData>(
    store: &IndexIdToParent<Output = K>,
    cache_size: usize,
) -> (Vec<T>, Vec<T>) {
    to_indirect_arrays_dedup(store, cache_size, false)
}

fn to_indirect_arrays_dedup<T: Integer + Clone + NumCast + mayda::utility::Bits + Copy, K: IndexIdToParentData>(
    store: &IndexIdToParent<Output = K>,
    cache_size: usize,
    sort_and_dedup: bool,
) -> (Vec<T>, Vec<T>) {
    let mut data = vec![];
    let mut valids = store.get_keys();
    valids.dedup();
    if valids.len() == 0 {
        return (vec![], vec![]);
    }
    let mut start_and_end_pos = vec![];
    let last_id = *valids.last().unwrap();
    start_and_end_pos.resize(
        (valids.last().unwrap().to_usize().unwrap() + 1) * 2,
        T::zero(),
    );

    let mut offset = 0;

    let mut cache = LruCache::new(cache_size);

    for valid in 0..=num::cast(last_id).unwrap() {
        let start = offset;
        if let Some(mut vals) = store.get_values(valid as u64) {
            if sort_and_dedup {
                vals.sort();
                vals.dedup();
            }

            if let Some(&mut (start, offset)) = cache.get_mut(&vals) {
                //reuse and reference existing data
                start_and_end_pos[valid as usize * 2] = start;
                start_and_end_pos[(valid as usize * 2) + 1] = offset;
            } else {
                let start = offset;

                for val in &vals {
                    data.push(num::cast(*val).unwrap());
                }
                offset += vals.len() as u64;

                if cache_size > 0 {
                    cache.insert(
                        vals,
                        (num::cast(start).unwrap(), num::cast(offset).unwrap()),
                    );
                }
                start_and_end_pos[valid as usize * 2] = num::cast(start).unwrap();
                start_and_end_pos[(valid as usize * 2) + 1] = num::cast(offset).unwrap();
            }
        } else {
            // add latest offsets, so the data is monotonically increasing -> better compression
            start_and_end_pos[valid as usize * 2] = num::cast(start).unwrap();
            start_and_end_pos[(valid as usize * 2) + 1] = num::cast(offset).unwrap();
        }
    }
    data.shrink_to_fit();

    (start_and_end_pos, data)
}

pub fn id_to_parent_to_array_of_array_mayda_indirect_one<T: Integer + Clone + NumCast + mayda::utility::Bits + Copy, K: IndexIdToParentData>(
    store: &IndexIdToParent<Output = K>,
) -> (usize, mayda::Monotone<T>, mayda::Uniform<T>) {
    //start, end, data
    let (start_and_end_pos, data) = to_indirect_arrays(store, 0);
    (
        start_and_end_pos.len() / 2,
        to_monotone(&start_and_end_pos),
        to_uniform(&data),
    )
}

pub fn id_to_parent_to_array_of_array_mayda_indirect_one_reuse_existing<T: Integer + Clone + NumCast + mayda::utility::Bits + Copy, K: IndexIdToParentData>(
    store: &IndexIdToParent<Output = K>,
) -> (usize, mayda::Uniform<T>, mayda::Uniform<T>) {
    //start, end, data
    let (start_and_end_pos, data) = to_indirect_arrays(store, 250);
    (
        start_and_end_pos.len() / 2,
        to_uniform(&start_and_end_pos),
        to_uniform(&data),
    )
}

use std::u32;


fn load_bytes(file: &File, offset: u64, num_bytes: usize) -> Vec<u8> {
    let mut data = vec![];
    data.resize(num_bytes, 0);
    load_bytes_into(&mut data, file, offset);
    data
}

fn load_bytes_into(buffer: &mut Vec<u8>, mut file: &File, offset: u64) {
    // @Temporary Use Result
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read_exact(buffer).unwrap();
}



#[cfg(test)]
mod tests {
    use test;
    use super::*;
    use rand;
    use persistence_data::*;

    fn get_test_data_1_to_1() -> IndexIdToOneParent<u64> {
        let values = vec![5, 6, 9, 9, 9, 50000];
        IndexIdToOneParent { data: values }
    }

    fn check_test_data_1_to_1(store: &IndexIdToParent<Output = u64>) {
        assert_eq!(store.get_keys(), vec![0, 1, 2, 3, 4, 5]);
        assert_eq!(store.get_value(0).unwrap(), 5);
        assert_eq!(store.get_value(1).unwrap(), 6);
        assert_eq!(store.get_value(2).unwrap(), 9);
        assert_eq!(store.get_value(3).unwrap(), 9);
        assert_eq!(store.get_value(4).unwrap(), 9);
        assert_eq!(store.get_value(5).unwrap(), 50000);
        assert_eq!(store.get_value(6), None);
    }

    fn get_test_data_1_to_n() -> ParallelArrays<u32> {
        let keys =   vec![0, 0, 1, 2, 3, 3];
        let values = vec![5, 6, 9, 9, 9, 50000];

        let store = ParallelArrays {
            values1: keys.clone(),
            values2: values.clone(),
        };
        store
    }

    fn check_test_data_1_to_n(store: &IndexIdToParent<Output = u32>) {
        assert_eq!(store.get_keys(), vec![0, 1, 2, 3]);
        assert_eq!(store.get_values(0).unwrap(), vec![5, 6]);
        assert_eq!(store.get_values(1).unwrap(), vec![9]);
        assert_eq!(store.get_values(2).unwrap(), vec![9]);
        assert_eq!(store.get_values(3).unwrap(), vec![9, 50000]);
        assert_eq!(store.get_values(4), None);

        let mut vec = vec![];
        store.append_values_for_ids(&[0, 1, 2, 3, 4, 5], &mut vec);
        assert_eq!(vec, vec![5, 6, 9, 9, 9, 50000]);

        let mut map = FnvHashMap::default();

        store.count_values_for_ids(&[0, 1, 2, 3, 4, 5], &mut map);
        assert_eq!(map.get(&5).unwrap(), &1);
        assert_eq!(map.get(&9).unwrap(), &3);
    }

    #[test]
    fn test_index_id_to_multiple_vec_vec_flat() {
        let data = get_test_data_1_to_n();
        let store = IndexIdToMultipleParent::new(&data);
        check_test_data_1_to_n(&store);
    }

    #[test]
    fn test_testdata() {
        let data = get_test_data_1_to_n();
        check_test_data_1_to_n(&data);
    }

    mod test_direct_1_to_1 {
        use super::*;

        #[test]
        fn test_single_file_array() {
            let store = get_test_data_1_to_1();

            fs::create_dir_all("test_single_file_array").unwrap();
            File::create("test_single_file_array/data")
                .unwrap()
                .write_all(&vec_to_bytes_u64(&store.data))
                .unwrap();

            let data_file = File::open(&get_file_path("test_single_file_array", "data")).unwrap();
            let data_metadata = fs::metadata(&get_file_path("test_single_file_array", "data")).unwrap();
            let store = SingleArrayFileReader::<u64>::new(data_file, data_metadata);
            check_test_data_1_to_1(&store);
        }

    }

    mod test_indirect {
        use super::*;
        use rand::distributions::{IndependentSample, Range};
        #[test]
        fn test_pointing_file_array() {
            let store = get_test_data_1_to_n();
            let (keys, values) = to_indirect_arrays(&store, 0);

            fs::create_dir_all("test_pointing_file_array").unwrap();
            File::create("test_pointing_file_array/indirect")
                .unwrap()
                .write_all(&vec_to_bytes_u32(&keys))
                .unwrap();
            File::create("test_pointing_file_array/data")
                .unwrap()
                .write_all(&vec_to_bytes_u32(&values))
                .unwrap();

            let start_and_end_file = File::open(&get_file_path("test_pointing_file_array", "indirect")).unwrap();
            let data_file = File::open(&get_file_path("test_pointing_file_array", "data")).unwrap();
            let data_metadata = fs::metadata(&get_file_path("test_pointing_file_array", "indirect")).unwrap();
            let store = PointingArrayFileReader::new(start_and_end_file, data_file, data_metadata);
            check_test_data_1_to_n(&store);
        }

        #[test]
        fn test_pointing_array_index_id_to_multiple_parent_indirect() {
            let store = get_test_data_1_to_n();
            let store = IndexIdToMultipleParentIndirect::new(&store);
            check_test_data_1_to_n(&store);
        }

        #[test]
        fn test_mayda_compressed_one() {
            let store = get_test_data_1_to_n();
            let mayda = IndexIdToMultipleParentCompressedMaydaINDIRECTOne::<u32>::new(&store);
            // let yep = to_uniform(&values);
            // assert_eq!(yep.access(0..=1), vec![5, 6]);
            check_test_data_1_to_n(&mayda);
        }

        #[inline(always)]
        fn pseudo_rand(num: u32) -> u32 {
            num * (num % 8) as u32
        }

        fn get_test_data_large(num_ids: usize, max_num_values_per_id: usize) -> ParallelArrays<u32> {
            let mut rng = rand::thread_rng();
            let between = Range::new(0, max_num_values_per_id);

            let mut keys = vec![];
            let mut values = vec![];

            for x in 0..num_ids {
                let num_values = between.ind_sample(&mut rng) as u64;

                for i in 0..num_values {
                    keys.push(x as u32);
                    // values.push(pseudo_rand((x as u32 * i as u32) as u32));
                    values.push(between.ind_sample(&mut rng) as u32);
                }
            }
            ParallelArrays {
                values1: keys,
                values2: values,
            }
        }

        fn prepare_indirect_pointing_file_array(folder: &str, store: &IndexIdToParent<Output=u32>) -> PointingArrayFileReader<u32> {
            let (keys, values) = to_indirect_arrays(store, 0);

            fs::create_dir_all(folder).unwrap();
            let data_path = get_file_path(folder, "data");
            let indirect_path = get_file_path(folder, "indirect");
            File::create(&data_path).unwrap().write_all(&vec_to_bytes_u32(&keys)).unwrap();
            File::create(&indirect_path).unwrap().write_all(&vec_to_bytes_u32(&values)).unwrap();

            let start_and_end_file = File::open(&data_path).unwrap();
            let data_file = File::open(&data_path).unwrap();
            let data_metadata = fs::metadata(&data_path).unwrap();
            let store: PointingArrayFileReader<u32> = PointingArrayFileReader::new(start_and_end_file, data_file, data_metadata);
            store
        }


        // #[bench]
        // fn indirect_pointing_file_array(b: &mut test::Bencher) {
        //     let store = get_test_data_large(40_000, 15);
        //     let mut rng = rand::thread_rng();
        //     let between = Range::new(0, 40_000);

        //     let store = prepare_indirect_pointing_file_array("test_pointing_file_array_perf", &store);// PointingArrayFileReader::new(start_and_end_file, data_file, data_metadata);

        //     b.iter(|| store.get_values(between.ind_sample(&mut rng)))
        // }

        #[bench]
        fn indirect_pointing_mayda(b: &mut test::Bencher) {
            let mut rng = rand::thread_rng();
            let between = Range::new(0, 40_000);
            let store = get_test_data_large(40_000, 15);
            let mayda = IndexIdToMultipleParentCompressedMaydaINDIRECTOne::<u32>::new(&store);

            b.iter(|| mayda.get_values(between.ind_sample(&mut rng)))
        }

        // #[bench]
        // fn indirect_pointing_mayda_large_array_700k_sorted_reads(b: &mut test::Bencher) {
        //     let mut rng = rand::thread_rng();
        //     // let between = Range::new(0, 40_000_000);
        //     let store_tmp = get_test_data_large(40_000_000, 15);

        //     let store = prepare_indirect_pointing_file_array("test_pointing_file_array_perf", &store_tmp);// PointingArrayFileReader::new(start_and_end_file, data_file, data_metadata);

        //     // let store = IndexIdToMultipleParentCompressedMaydaINDIRECTOne::<u32>::new(&store_tmp);
        //     let ids:Vec<u32> = (0 .. 7).collect();

        //     b.iter(|| {
        //         let mut hits = FnvHashMap::default();
        //         {
        //             store.count_values_for_ids(&ids, &mut hits);
        //         }
        //     })
        // }

        #[bench]
        fn indirect_pointing_uncompressed_im(b: &mut test::Bencher) {
            let mut rng = rand::thread_rng();
            let between = Range::new(0, 40_000);
            let store = get_test_data_large(40_000, 15);
            let mayda = IndexIdToMultipleParent::<u32>::new(&store);

            b.iter(|| mayda.get_values(between.ind_sample(&mut rng)))
        }

    }

}
