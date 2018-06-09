extern crate itertools;
extern crate memmap;
extern crate tempfile;
extern crate byteorder;

use itertools::Itertools;
use memmap::MmapOptions;

// use byteorder::{ByteOrder, LittleEndian};
use std::fs::File;
use std::io;
use std::iter::FusedIterator;
use std::mem;
use std::io::prelude::*;
use std::io::BufWriter;
use std::ptr::copy_nonoverlapping;

#[macro_use]
extern crate measure_time;
#[macro_use]
extern crate log;

pub trait GetValue {
    fn get_value(&self) -> u32;
}

impl GetValue for u32 {
    #[inline]
    fn get_value(&self) -> u32 {
        *self
    }
}

impl GetValue for (u32,u32) {
    #[inline]
    fn get_value(&self) -> u32 {
        self.0
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct KeyValue<T:GetValue> {
    pub key: u32,
    pub value: T,
}

impl<T:GetValue> KeyValue<T> {
    fn new(key: u32, value: T) -> Self {
        KeyValue{key, value}
    }
}

#[derive(Debug, Clone)]
struct Part {
    offset: u32,
    len: u32,
}

#[derive(Debug, Default)]
///
/// Order is not guaranteed to be kept the same for same ids -> insert (0, 1)..(0,2)   --> Output could be (0,2),(0,1) with BufferedIndexWriter::default()
///
pub struct BufferedIndexWriter<T:GetValue = u32> {
    pub cache: Vec<KeyValue<T>>,
    pub temp_file: Option<File>,
    pub max_value_id: u32,
    pub num_values: u32,
    /// keep order of values
    stable_sort: bool,
    /// Ids are already sorted inserted, so there is no need to sort them
    ids_are_sorted: bool,
    last_id: u32,
    /// written parts offsets in the file
    parts: Vec<Part>,
}

impl<T:GetValue + Default> BufferedIndexWriter<T> {
    pub fn new_with_opt(stable_sort: bool, ids_are_sorted: bool) -> Self {
        BufferedIndexWriter {
            cache: vec![],
            temp_file: None,
            max_value_id: 0,
            num_values: 0,
            stable_sort,
            ids_are_sorted,
            last_id: std::u32::MAX,
            parts: vec![],
        }
    }

    //TODO REPLACE TRANSACTION WITH CHANGE DETECTION FOR FLUSHING
    pub fn new_for_sorted_id_insertion() -> Self {
        BufferedIndexWriter::new_with_opt(false, true)
    }
    pub fn new_stable_sorted() -> Self {
        BufferedIndexWriter::new_with_opt(true, false)
    }

    #[inline]
    pub fn add_all(&mut self, id: u32, values: Vec<T>) -> Result<(), io::Error> {
        self.num_values += values.len() as u32;

        //To ensure ordering we flush only, when ids change
        let id_has_changed = self.last_id != id;
        self.last_id = id;

        for value in values {
            self.max_value_id = std::cmp::max(value.get_value(), self.max_value_id);
            self.cache.push(KeyValue {
                key: id,
                value: value,
            });
        }

        if id_has_changed && self.cache.len() >= 1_000_000 { // flush after 1_000_000 * 8 byte values = 8Megadolonbytes
            self.flush()?;
        }

        Ok(())
    }

    #[inline]
    pub fn add(&mut self, id: u32, value: T) -> Result<(), io::Error> {
        self.max_value_id = std::cmp::max(value.get_value(), self.max_value_id);
        self.num_values += 1;

        //To ensure ordering we flush only, when ids change
        let id_has_changed = self.last_id != id;
        self.last_id = id;

        self.cache.push(KeyValue {
            key: id,
            value: value,
        });

        if id_has_changed && self.cache.len() >= 500_000 { // flush after 500_000 * 8 byte values = 4Megadolonbytes
            self.flush()?;
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        if self.cache.is_empty() {
            return Ok(());
        }

        let mut data_file = BufWriter::new(self.temp_file.get_or_insert_with(|| tempfile::tempfile().unwrap()));
        let prev_part = self
            .parts
            .last()
            .cloned()
            .unwrap_or(Part { offset: 0, len: 0 });

        if !self.ids_are_sorted{
            if self.stable_sort {
                self.cache.sort_by_key(|el| el.key);
            }else{
                self.cache.sort_unstable_by_key(|el| el.key);
            }
        }

        //Maximum speed, Maximum unsafe
        use std::slice;
        unsafe {
            let slice =
                slice::from_raw_parts(self.cache.as_ptr() as *const u8, self.cache.len() * mem::size_of::<KeyValue<T>>());
            data_file.write(&slice)?;
        }

        // data_file.write(&vec_to_bytes_u32(&self.cache.map()))?;

        self.parts.push(Part {
            offset: prev_part.offset + prev_part.len,
            len: self.cache.len() as u32,
        });
        self.cache.clear();
        Ok(())
    }

    pub fn multi_iter(&mut self) -> Result<(Vec<MMapIter<T>>), io::Error> {
        let mut vecco = vec![];

        // let file = File::open(&self.data_path)?;
        if let Some(file) = &self.temp_file {
            for part in self.parts.iter() {
                let mmap = unsafe {
                    MmapOptions::new()
                        .offset(part.offset as usize * mem::size_of::<KeyValue<T>>())
                        .len(part.len as usize * mem::size_of::<KeyValue<T>>())
                        .map(&file)?
                };
                vecco.push(MMapIter::<T>::new(mmap));
            }
            Ok(vecco)

        }else{
            Ok(vec![])
        }
    }

    #[inline]
    pub fn is_in_memory(&self) -> bool {
        self.parts.is_empty()
    }

    /// inmemory version for very small indices, where it's inefficient to write and then read from disk - data on disk will be ignored!
    #[inline]
    pub fn iter_inmemory<'a>(&'a mut self) -> impl Iterator<Item = &'a KeyValue<T>> {
        if !self.ids_are_sorted{
            if self.stable_sort {
                self.cache.sort_by_key(|el| el.key);
            }else{
                self.cache.sort_unstable_by_key(|el| el.key);
            }
        }
        self.cache.iter()
    }

    /// flushed changes on disk and returns iterator over sorted elements
    #[inline]
    pub fn kmerge_sorted_iter(&mut self) -> Result<(impl Iterator<Item = KeyValue<T>>), io::Error> {
        self.flush()?;

        let iters = self.multi_iter().unwrap();
        let mergo = iters.into_iter().kmerge_by(|a, b| (*a).key < (*b).key);

        Ok(mergo)
    }
}


use std::fmt;
impl<T:GetValue> fmt::Display for BufferedIndexWriter<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for el in self.cache.iter() {
            write!(f, "{}\t{}\n", el.key, el.value.get_value())?;
        }
        Ok(())
    }
}
// impl fmt::Debug for BufferedIndexWriter {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {

//         for el in self.cache {
//             write!(f, "({}, {})", el.key, el.value);
//         }
//         f
//     }
// }


// pub fn vec_to_bytes_u32(data: &[u32]) -> Vec<u8> {
//     let mut wtr: Vec<u8> = vec_with_size_uninitialized(data.len() * std::mem::size_of::<u32>());
//     LittleEndian::write_u32_into(data, &mut wtr);
//     wtr
// }

use std::marker::PhantomData;
#[derive(Debug)]
pub struct MMapIter<T:GetValue> {
    mmap: memmap::Mmap,
    pos: u32,
    phantom: PhantomData<T>,
}

impl<T:GetValue> MMapIter<T> {
    fn new(mmap: memmap::Mmap) -> Self {
        MMapIter { mmap, pos: 0, phantom:PhantomData }
    }
}

#[inline]
// Maximum speed, Maximum unsafe
fn read_pair_very_raw_p<T:GetValue + Default>(p: *const u8) -> KeyValue<T> {
    // let mut out: (u32, u32) = (0, 0);
    let mut out: KeyValue<T> = KeyValue::default();
    unsafe {
        copy_nonoverlapping(p, &mut out as *mut KeyValue<T> as *mut u8, mem::size_of::<KeyValue<T>>());
    }
    out
}

impl<T:GetValue + Default> Iterator for MMapIter<T> {
    type Item = KeyValue<T>;

    #[inline]
    fn next(&mut self) -> Option<KeyValue<T>> {
        if self.mmap.len() <= self.pos as usize {
            return None;
        }
        let pair = read_pair_very_raw_p((&self.mmap[self.pos as usize..]).as_ptr());
        self.pos += mem::size_of::<KeyValue<T>>() as u32;
        Some(pair)
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining_els = (self.mmap.len() as u32 - self.pos) / mem::size_of::<KeyValue<T>>() as u32;
        (remaining_els as usize, Some(remaining_els as usize))
    }
}

impl<T:GetValue + Default>  ExactSizeIterator for MMapIter<T> {
    #[inline]
    fn len(&self) -> usize {
        let remaining_els = (self.mmap.len() as u32 - self.pos) / mem::size_of::<KeyValue<T>>() as u32;
        remaining_els as usize
    }
}

impl<T:GetValue + Default>  FusedIterator for MMapIter<T> {}

#[test]
fn test_buffered_index_writer() {
    let mut ind = BufferedIndexWriter::default();

    ind.add(2, 2).unwrap();
    ind.flush().unwrap();

    let mut iters = ind.multi_iter().unwrap();
    assert_eq!(iters[0].next(), Some(KeyValue::new(2, 2)));
    assert_eq!(iters[0].next(), None);

    let mut iters = ind.multi_iter().unwrap();
    assert_eq!(iters[0].next(), Some(KeyValue::new(2, 2)));
    assert_eq!(iters[0].next(), None);

    ind.add(1, 3).unwrap();
    ind.flush().unwrap();
    ind.add(4, 4).unwrap();
    ind.flush().unwrap();

    let mut iters = ind.multi_iter().unwrap();
    assert_eq!(iters[1].next(), Some(KeyValue::new(1, 3)));
    assert_eq!(iters[1].next(), None);

    let mut mergo = ind.kmerge_sorted_iter().unwrap();
    assert_eq!(mergo.next(), Some(KeyValue::new(1, 3)));
    assert_eq!(mergo.next(), Some(KeyValue::new(2, 2)));
    assert_eq!(mergo.next(), Some(KeyValue::new(4, 4)));
}
