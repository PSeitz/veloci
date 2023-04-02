use itertools::Itertools;
use memmap2::{Mmap, MmapOptions};
use std::fmt::Display;

use std::{
    boxed::Box,
    cmp::{Ord, PartialOrd},
    default::Default,
    env, fmt,
    io::{self, prelude::*, BufWriter},
    iter::{FusedIterator, Iterator},
    marker::PhantomData,
    mem,
};
use vint32::*;

pub trait SerializeInto {
    fn serialize_into(&self, sink: &mut Vec<u8>);
}

pub trait DeserializeFrom {
    fn deserialize_from_slice(source: &[u8], pos: &mut usize) -> Option<Self>
    where
        Self: std::marker::Sized;
}

pub trait GetValue {
    fn get_value(&self) -> u32;
}

impl GetValue for u32 {
    #[inline]
    fn get_value(&self) -> u32 {
        *self
    }
}

impl GetValue for (u32, u32) {
    #[inline]
    fn get_value(&self) -> u32 {
        self.0
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct KeyValue<K: PartialOrd + Ord + Default + Copy + SerializeInto + DeserializeFrom, T: GetValue + SerializeInto + DeserializeFrom> {
    pub key: K,
    pub value: T,
}

impl<K: PartialOrd + Ord + Default + Copy + SerializeInto + DeserializeFrom, T: GetValue + SerializeInto + DeserializeFrom> SerializeInto for KeyValue<K, T> {
    #[inline]
    fn serialize_into(&self, sink: &mut Vec<u8>) {
        self.key.serialize_into(sink);
        self.value.serialize_into(sink);
    }
}

const DESER_ERROR: &str = "Could not deserialize from map in buffered index writer";

impl<K: PartialOrd + Ord + Default + Copy + SerializeInto + DeserializeFrom, T: GetValue + SerializeInto + DeserializeFrom> DeserializeFrom for KeyValue<K, T> {
    #[inline]
    fn deserialize_from_slice(source: &[u8], pos: &mut usize) -> Option<Self>
    where
        Self: std::marker::Sized,
    {
        if let Some(key) = K::deserialize_from_slice(source, pos) {
            let value = T::deserialize_from_slice(source, pos).expect(DESER_ERROR);
            Some(KeyValue { key, value })
        } else {
            None
        }
    }
}

impl SerializeInto for u32 {
    #[inline(always)]
    fn serialize_into(&self, sink: &mut Vec<u8>) {
        encode_varint_into(sink, *self);
    }
}

impl DeserializeFrom for u32 {
    #[inline(always)]
    fn deserialize_from_slice(source: &[u8], pos: &mut usize) -> Option<Self>
    where
        Self: std::marker::Sized,
    {
        decode_varint_slice(source, pos)
    }
}

impl SerializeInto for (u32, u32) {
    #[inline(always)]
    fn serialize_into(&self, sink: &mut Vec<u8>) {
        encode_varint_into(sink, self.0);
        encode_varint_into(sink, self.1);
    }
}
impl DeserializeFrom for (u32, u32) {
    #[inline(always)]
    fn deserialize_from_slice(source: &[u8], pos: &mut usize) -> Option<Self>
    where
        Self: std::marker::Sized,
    {
        decode_varint_slice(source, pos).map(|first| (first, decode_varint_slice(source, pos).expect(DESER_ERROR)))
    }
}

#[derive(Debug, Clone)]
struct Part {
    offset: u64,
    len: u32,
}

#[derive(Debug)]
///
/// Order is not guaranteed to be kept the same for same ids -> insert (0, 1)..(0,2)   --> Output could be (0,2),(0,1) with BufferedIndexWriter::default()
/// stable_sort with add_all fn keeps insertion order
///
pub struct BufferedIndexWriter<K: PartialOrd + Ord + Default + Copy + SerializeInto + DeserializeFrom = u32, T: GetValue + SerializeInto + DeserializeFrom = u32> {
    pub cache: Vec<KeyValue<K, T>>,
    pub max_value_id: u32,
    pub num_values: u32,
    last_id: Option<K>,
    flush_data: Box<FlushStruct>,
}

#[derive(Debug)]
pub(crate) struct FlushStruct {
    bytes_written: u64,
    /// flush to disk in bytes after threshold
    flush_threshold: usize,
    /// keep order of values
    stable_sort: bool,
    /// Ids are already sorted inserted, so there is no need to sort them
    ids_are_sorted: bool,
    // flush_threshold: usize,
    /// written parts offsets in the file
    parts: Vec<Part>,
    temp_file_folder: String,
    // temp_file: Option<File>,
    temp_file: Option<tempfile::NamedTempFile>,
}

impl<
        K: PartialOrd + Ord + Default + Copy + Send + Sync + SerializeInto + DeserializeFrom,
        T: GetValue + Default + Clone + Copy + Send + Sync + SerializeInto + DeserializeFrom,
    > BufferedIndexWriter<K, T>
{
    pub fn bytes_written(&self) -> u64 {
        self.flush_data.bytes_written
    }

    pub fn new_with_opt(stable_sort: bool, ids_are_sorted: bool, temp_file_folder: String) -> Self {
        let flush_threshold = env::var_os("FlushThreshold")
            .map(|el| el.into_string().unwrap().parse::<usize>().unwrap())
            .unwrap_or(4_000_000);

        let flush_data = Box::new(FlushStruct {
            bytes_written: 0,
            flush_threshold,
            temp_file: None,
            parts: vec![],
            stable_sort,
            ids_are_sorted,
            temp_file_folder,
        });
        BufferedIndexWriter {
            cache: vec![],
            max_value_id: 0,
            num_values: 0,
            last_id: None,
            flush_data,
        }
    }

    pub fn new_for_sorted_id_insertion(temp_file_folder: String) -> Self {
        BufferedIndexWriter::new_with_opt(false, true, temp_file_folder)
    }

    pub fn new_stable_sorted(temp_file_folder: String) -> Self {
        BufferedIndexWriter::new_with_opt(true, false, temp_file_folder)
    }

    pub fn new_unstable_sorted(temp_file_folder: String) -> Self {
        BufferedIndexWriter::new_with_opt(false, false, temp_file_folder)
    }

    #[inline]
    pub fn flush_threshold(&mut self) -> usize {
        self.flush_data.flush_threshold
    }

    #[inline]
    pub fn add_all(&mut self, id: K, values: &[T]) -> Result<(), io::Error> {
        self.num_values += values.len() as u32;

        //To ensure ordering we flush only, when ids change
        let id_has_changed = self.last_id != Some(id);
        self.last_id = Some(id);

        for value in values {
            self.max_value_id = std::cmp::max(value.get_value(), self.max_value_id);
            self.cache.push(KeyValue { key: id, value: *value });
        }

        self.check_flush(id_has_changed)?;

        Ok(())
    }

    #[inline]
    pub fn check_flush(&mut self, id_has_changed: bool) -> Result<(), io::Error> {
        if id_has_changed && self.cache.len() * mem::size_of::<KeyValue<K, T>>() >= self.flush_data.flush_threshold {
            self.flush()?;
        }
        Ok(())
    }

    #[inline]
    pub fn add(&mut self, id: K, value: T) -> Result<(), io::Error> {
        self.max_value_id = std::cmp::max(value.get_value(), self.max_value_id);
        self.num_values += 1;

        //To ensure ordering we flush only, when ids change
        let id_has_changed = self.last_id != Some(id);
        self.last_id = Some(id);

        self.cache.push(KeyValue { key: id, value });

        self.check_flush(id_has_changed)?;

        Ok(())
    }

    #[cold]
    pub fn flush(&mut self) -> Result<(), io::Error> {
        if self.cache.is_empty() {
            return Ok(());
        }

        self.sort_cache();
        let prev_part = self.flush_data.parts.last().cloned().unwrap_or(Part { offset: 0, len: 0 });
        let serialized_len = {
            let temp_folder = &self.flush_data.temp_file_folder;
            let mut data_file = BufWriter::new(
                self.flush_data
                    .temp_file
                    .get_or_insert_with(|| tempfile::NamedTempFile::new_in(temp_folder).unwrap_or_else(|e| panic!("could not create temp file {:?}, {:?}", temp_folder, e))),
            );
            let mut sink = Vec::with_capacity(self.cache.len() * mem::size_of::<KeyValue<K, T>>());
            for value in self.cache.iter() {
                value.serialize_into(&mut sink);
            }

            data_file.write_all(&sink)?;
            sink.len()
        };

        self.flush_data.bytes_written += u64::from(prev_part.len);

        self.flush_data.parts.push(Part {
            offset: prev_part.offset + u64::from(prev_part.len),
            len: serialized_len as u32,
        });
        self.cache.clear();
        Ok(())
    }

    fn sort_cache(&mut self) {
        if !self.flush_data.ids_are_sorted {
            if self.flush_data.stable_sort {
                self.cache.sort_by_key(|el| el.key);
            } else {
                self.cache.sort_unstable_by_key(|el| el.key);
            }
        }
    }

    pub fn multi_iter(&self) -> Result<Vec<MMapIter<K, T>>, io::Error> {
        let mut vecco = vec![];

        if let Some(file) = &self.flush_data.temp_file {
            for part in &self.flush_data.parts {
                let mmap = unsafe { MmapOptions::new().offset(part.offset).len(part.len as usize).map(file.as_file())? };
                vecco.push(MMapIter::<K, T>::new(mmap));
            }
            Ok(vecco)
        } else {
            Ok(vec![])
        }
    }

    #[inline]
    pub fn is_in_memory(&self) -> bool {
        self.flush_data.parts.is_empty()
    }

    /// inmemory version for very small indices, where it's inefficient to write and then read from disk - data on disk will be ignored!
    #[inline]
    pub fn into_iter_inmemory(mut self) -> impl Iterator<Item = KeyValue<K, T>> {
        self.sort_cache();
        self.cache.into_iter()
    }

    /// flushed changes on disk and returns iterator over sorted elements
    #[inline]
    pub fn flush_and_kmerge(&mut self) -> Result<impl Iterator<Item = KeyValue<K, T>>, io::Error> {
        self.flush()?;

        Ok(self.kmerge())
    }

    /// returns iterator over sorted elements
    #[inline]
    fn kmerge(&self) -> impl Iterator<Item = KeyValue<K, T>> {
        let iters = self.multi_iter().unwrap();
        iters.into_iter().kmerge_by(|a, b| a.key < b.key)
    }
}

impl<K: Display + PartialOrd + Ord + Default + Copy + SerializeInto + DeserializeFrom, T: GetValue + Default + SerializeInto + DeserializeFrom> fmt::Display
    for BufferedIndexWriter<K, T>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for el in &self.cache {
            writeln!(f, "{}\t{}", el.key, el.value.get_value())?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MMapIter<K: PartialOrd + Ord + Default + Copy, T: GetValue> {
    mmap: Mmap,
    pos: usize,
    #[allow(dead_code)]
    finished: bool,
    phantom: PhantomData<T>,
    menace: PhantomData<K>,
}

impl<K: PartialOrd + Ord + Default + Copy, T: GetValue> MMapIter<K, T> {
    fn new(mmap: Mmap) -> Self {
        MMapIter {
            mmap,
            finished: false,
            pos: 0,
            phantom: PhantomData,
            menace: PhantomData,
        }
    }
}

impl<K: PartialOrd + Ord + Default + Copy + SerializeInto + DeserializeFrom, T: GetValue + Default + SerializeInto + DeserializeFrom> Iterator for MMapIter<K, T> {
    type Item = KeyValue<K, T>;

    #[inline]
    fn next(&mut self) -> Option<KeyValue<K, T>> {
        KeyValue::deserialize_from_slice(&self.mmap, &mut self.pos)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let lower_bound = (self.mmap.len() - self.pos) / mem::size_of::<KeyValue<K, T>>();
        let upper_bound = self.mmap.len() - self.pos;
        (lower_bound, Some(upper_bound))
    }
}

impl<K: PartialOrd + Ord + Default + Copy + SerializeInto + DeserializeFrom, T: GetValue + Default + SerializeInto + DeserializeFrom> FusedIterator for MMapIter<K, T> {}

#[test]
fn test_buffered_index_writer() {
    use std::env;
    let mut ind = BufferedIndexWriter::new_unstable_sorted(env::temp_dir().to_str().unwrap().to_string());

    ind.add(2_u32, 2).unwrap();
    ind.flush().unwrap();

    let mut iters = ind.multi_iter().unwrap();
    assert_eq!(iters[0].next(), Some(KeyValue { key: 2, value: 2 }));
    assert_eq!(iters[0].next(), None);

    let mut iters = ind.multi_iter().unwrap();
    assert_eq!(iters[0].next(), Some(KeyValue { key: 2, value: 2 }));
    assert_eq!(iters[0].next(), None);

    ind.add(1, 3).unwrap();
    ind.flush().unwrap();
    ind.add(4, 4).unwrap();
    ind.flush().unwrap();
    ind.flush().unwrap(); // double flush test

    let mut iters = ind.multi_iter().unwrap();
    assert_eq!(iters[1].next(), Some(KeyValue { key: 1, value: 3 }));
    assert_eq!(iters[1].next(), None);

    let mut mergo = ind.flush_and_kmerge().unwrap();
    assert_eq!(mergo.next(), Some(KeyValue { key: 1, value: 3 }));
    assert_eq!(mergo.next(), Some(KeyValue { key: 2, value: 2 }));
    assert_eq!(mergo.next(), Some(KeyValue { key: 4, value: 4 }));

    let mut ind = BufferedIndexWriter::new_unstable_sorted(env::temp_dir().to_str().unwrap().to_string());
    ind.add_all(2_u32, &[2, 2000]).unwrap();
    ind.flush().unwrap();
    let mut iters = ind.multi_iter().unwrap();
    assert_eq!(iters[0].next(), Some(KeyValue { key: 2, value: 2 }));
    assert_eq!(iters[0].next(), Some(KeyValue { key: 2, value: 2000 }));

    let mut ind = BufferedIndexWriter::new_unstable_sorted(env::temp_dir().to_str().unwrap().to_string());
    ind.add_all(2_u32, &[2, 2000]).unwrap();
    let mut iter = ind.into_iter_inmemory();
    assert_eq!(iter.next(), Some(KeyValue { key: 2, value: 2 }));
    assert_eq!(iter.next(), Some(KeyValue { key: 2, value: 2000 }));
}

#[test]
fn test_buffered_index_writer_pairs() {
    use std::env;
    let mut ind = BufferedIndexWriter::new_unstable_sorted(env::temp_dir().to_str().unwrap().to_string());

    ind.add((2_u32, 3_u32), 2).unwrap();
    ind.flush().unwrap();

    let mut iters = ind.multi_iter().unwrap();
    assert_eq!(iters[0].next(), Some(KeyValue { key: (2_u32, 3_u32), value: 2 }));
    assert_eq!(iters[0].next(), None);

    let mut iters = ind.multi_iter().unwrap();
    assert_eq!(iters[0].next(), Some(KeyValue { key: (2_u32, 3_u32), value: 2 }));
    assert_eq!(iters[0].next(), None);

    ind.add((1, 2), 3).unwrap();
    ind.flush().unwrap();
    ind.add((4, 4), 4).unwrap();
    ind.flush().unwrap();
    ind.flush().unwrap(); // double flush test

    let mut iters = ind.multi_iter().unwrap();
    assert_eq!(iters[1].next(), Some(KeyValue { key: (1, 2), value: 3 }));
    assert_eq!(iters[1].next(), None);

    let mut mergo = ind.flush_and_kmerge().unwrap();
    assert_eq!(mergo.next(), Some(KeyValue { key: (1, 2), value: 3 }));
    assert_eq!(mergo.next(), Some(KeyValue { key: (2_u32, 3_u32), value: 2 }));
    assert_eq!(mergo.next(), Some(KeyValue { key: (4, 4), value: 4 }));

    let mut ind = BufferedIndexWriter::new_unstable_sorted(env::temp_dir().to_str().unwrap().to_string());
    ind.add_all((2_u32, 1500_u32), &[2, 2000]).unwrap();
    ind.flush().unwrap();
    let mut iters = ind.multi_iter().unwrap();
    assert_eq!(iters[0].next(), Some(KeyValue { key: (2_u32, 1500_u32), value: 2 }));
    assert_eq!(
        iters[0].next(),
        Some(KeyValue {
            key: (2_u32, 1500_u32),
            value: 2000
        })
    );
}

#[test]
fn test_buffered_index_iter_im() {
    use std::env;

    let mut ind = BufferedIndexWriter::new_unstable_sorted(env::temp_dir().to_str().unwrap().to_string());
    if ind.flush_threshold() > 10_000 {
        ind.add_all((2_u32, 1500_u32), &[2, 2000]).unwrap();
        let mut iter = ind.into_iter_inmemory();
        assert_eq!(iter.next(), Some(KeyValue { key: (2_u32, 1500_u32), value: 2 }));
        assert_eq!(
            iter.next(),
            Some(KeyValue {
                key: (2_u32, 1500_u32),
                value: 2000
            })
        );
    }
}
