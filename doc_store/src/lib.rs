#![feature(test)]

use std::mem;
use std::io;
use std::io::prelude::*;
use std::io::Seek;
use std::io::SeekFrom;
use std::cmp::Ordering::Greater;

use vint::vint::*;

#[cfg(feature = "lz4_linked")]
use lz4::{Decoder, EncoderBuilder};

#[cfg(feature = "lz4_rust")]
use lz4_compress::{compress, decompress};

#[cfg(feature = "lz4_flexx")]
use lz4_flex::{compress_into, decompress};

const FLUSH_THRESHOLD: usize = 65535;
// const VALUE_OFFSET: usize = 1;

#[derive(Debug)]
pub struct DocLoader {}
impl DocLoader {

    #[cfg(feature = "lz4_linked")]
    fn decompress(buffer: &[u8]) -> Result<Vec<u8>, io::Error> {
        let mut output:Vec<u8> = vec![];
        let mut decoder = Decoder::new(buffer)?;
        io::copy(&mut decoder, &mut output)?;
        Ok(output)
    }

    #[cfg(any(feature = "lz4_rust", feature = "lz4_flexx"))]
    fn decompress(buffer: &[u8]) -> Result<Vec<u8>, io::Error> {
        Ok(decompress(&buffer).unwrap())
    }

    pub fn get_doc<R: Read + Seek>(mut data_reader: R, offsets:&[u8], pos: usize) -> Result<String, io::Error> {
        let size = offsets.len() / mem::size_of::<(u32, u64)>();
        let hit = binary_search_slice::<u32, u64>(size, pos as u32, &offsets);
        let start = hit.lower.1;
        let end = hit.upper.1;
        let mut buffer: Vec<u8> = vec![0;(end - start) as usize];
        data_reader.seek(SeekFrom::Start(start as u64))?;
        data_reader.read_exact(&mut buffer)?;
        let mut output:Vec<u8> = DocLoader::decompress(&buffer as &[u8])?;

        let mut arr = VintArrayIterator::new(&output);
        let arr_size = arr.next().unwrap();

        let mut data_start = arr.pos;
        let mut arr = VintArrayIterator::new(&output[arr.pos .. arr.pos + arr_size as usize]);
        let first_id_in_block = arr.next().unwrap();

        let mut doc_offsets_in_block:Vec<u32> = vec![];
        while let Some(off) = arr.next() {
            doc_offsets_in_block.push(off);
        }
        data_start += arr.pos;
        let pos_in_block = pos - first_id_in_block as usize;

        let doc = output[(data_start + doc_offsets_in_block[pos_in_block]  as usize) .. (data_start + doc_offsets_in_block[pos_in_block + 1] as usize)].to_vec();
        let s = unsafe { String::from_utf8_unchecked(doc) };
        Ok(s)
    }
}


#[test]
fn test_minimal() {
    color_backtrace::install();
    let mut writer = DocWriter::new(0);

    let mut sink = vec![];

    let doc1 = "a";
    writer.add_doc(&doc1, &mut sink).unwrap();
    writer.finish(&mut sink).unwrap();
    let ret_doc = DocLoader::get_doc(io::Cursor::new(&sink), &writer.get_offsets_as_byte_slice(), 0).unwrap();
    assert_eq!(doc1.to_string(), ret_doc);

}

#[test]
fn test_large_doc_store() {
    let mut writer = DocWriter::new(0);

    let mut sink = vec![];

    let doc1 = r#"{"category": "superb", "tags": ["nice", "cool"] }"#;
    for _ in 0..64 {
        writer.add_doc(doc1, &mut sink).unwrap();
    }

    writer.finish(&mut sink).unwrap();

    use std::slice;
    let offset_bytes = unsafe {
        slice::from_raw_parts(writer.offsets.as_ptr() as *const u8, writer.offsets.len() * mem::size_of::<(u32, u64)>())
    };

    assert_eq!(doc1.to_string(), DocLoader::get_doc(io::Cursor::new(&sink), &offset_bytes, 0).unwrap());

}

#[derive(Debug)]
pub struct DocWriter {
    pub curr_id: u32,
    pub bytes_indexed: u64,
    pub offsets: Vec<(u32,u64)>, // tuples of (first_id_in_block, byte_offset)
    pub current_offset: u64,
    current_block: DocWriterBlock,
}

#[derive(Debug, Default)]
struct DocWriterBlock {
    data: Vec<u8>,
    doc_offsets_in_cache: Vec<u32>,
    first_id_in_block: u32,
}

impl DocWriter {
    pub fn new( current_offset:u64) -> Self {
        DocWriter {
            curr_id: 0,
            bytes_indexed: 0,
            offsets: vec![],
            current_offset,
            current_block: Default::default(),
        }
    }
    pub fn get_offsets_as_byte_slice(&self) -> &[u8]{
        use std::slice;
        let slice = unsafe { slice::from_raw_parts(self.offsets.as_ptr() as *const u8, self.offsets.len() * mem::size_of::<(u32, u64)>()) };
        slice
    }
    pub fn add_doc<W:Write>(&mut self, doc:&str, out: W)-> Result<(), io::Error>{
        self.bytes_indexed += doc.as_bytes().len() as u64;
        let new_block = self.current_block.data.is_empty();
        if new_block {
            self.current_block.first_id_in_block = self.curr_id;
            self.current_block.doc_offsets_in_cache.push(self.current_block.data.len() as u32);
        }
        self.current_block.data.extend(doc.as_bytes());
        self.current_block.doc_offsets_in_cache.push(self.current_block.data.len() as u32);

        if self.current_block.data.len() > FLUSH_THRESHOLD {
            self.flush(out)?;
        }
        self.curr_id +=1;
        Ok(())
    }

    #[cfg(feature = "lz4_linked")]
    fn compress<W:Write>(mut slice1: &[u8], mut slice2: &[u8], mut out: W) -> usize {
        let mut cache = vec![];
        let mut encoder = EncoderBuilder::new().level(2).build(&mut cache).unwrap();
        io::copy(&mut slice1, &mut encoder).unwrap();
        io::copy(&mut slice2, &mut encoder).unwrap();
        let (output, _result) = encoder.finish();
        out.write_all(&output).unwrap();
        output.len()
    }

    #[cfg(feature = "lz4_flexx")]
    fn compress<W:Write>(mut slice1: &[u8], mut slice2: &[u8], mut out: W) -> usize {
        let both:Vec<u8> = [slice1,slice2].concat();
        compress_into(&both, out).unwrap()
        // let comp = compress(&both);
        // out.write_all(&comp).unwrap();
        // comp.len()
    }
    #[cfg(feature = "lz4_rust")]
    fn compress<W:Write>(mut slice1: &[u8], mut slice2: &[u8], mut out: W) -> usize {
        let both:Vec<u8> = [slice1,slice2].concat();
        let comp = compress(&both);
        out.write_all(&comp).unwrap();
        comp.len()
    }

    fn flush<W:Write>(&mut self, mut out: W) -> Result<(), io::Error>{
        let mut arr = VIntArray::default();
        arr.encode_val(self.current_block.first_id_in_block);
        arr.encode_vals(&self.current_block.doc_offsets_in_cache);

        let bytes_written = DocWriter::compress(arr.serialize().as_slice(), self.current_block.data.as_slice(), &mut out);

        self.offsets.push((self.current_block.first_id_in_block as u32, self.current_offset));
        self.current_offset += bytes_written as u64;
        self.current_block.data.clear();
        self.current_block.doc_offsets_in_cache.clear();
        out.flush()?;
        Ok(())
    }

    pub fn finish<W:Write>(&mut self, out: W) -> Result<(), io::Error> {
        self.flush(out)?;
        self.offsets.push((self.curr_id, self.current_offset));
        Ok(())
    }
}

#[test]
fn test_doc_store() {
    color_backtrace::install();
    let mut writer = DocWriter::new(0);

    let mut sink = vec![];
    let doc1 = r#"{"test":"ok"}"#;
    let doc2 = r#"{"test2":"ok"}"#;
    let doc3 = r#"{"test3":"ok"}"#;
    writer.add_doc(doc1, &mut sink).unwrap();
    writer.add_doc(doc2, &mut sink).unwrap();
    writer.add_doc(doc3, &mut sink).unwrap();
    writer.finish(&mut sink).unwrap();

    use std::slice;
    let offset_bytes = unsafe {
        slice::from_raw_parts(writer.offsets.as_ptr() as *const u8, writer.offsets.len() * mem::size_of::<(u32, u64)>())
    };

    assert_eq!(doc1.to_string(), DocLoader::get_doc(io::Cursor::new(&sink), &offset_bytes, 0).unwrap());
    assert_eq!(doc2.to_string(), DocLoader::get_doc(io::Cursor::new(&sink), &offset_bytes, 1).unwrap());
    assert_eq!(doc3.to_string(), DocLoader::get_doc(io::Cursor::new(&sink), &offset_bytes, 2).unwrap());
}

#[cfg(test)]
extern crate test;

#[bench]
fn bench_creation(b: &mut test::Bencher) {
    b.iter(|| {
        let mut writer = DocWriter::new(0);
        let mut sink = vec![];
        for _ in 0..10_000 {
            writer.add_doc(r#"{"test":"ok"}"#, &mut sink).unwrap();
            writer.add_doc(r#"{"test2":"ok"}"#, &mut sink).unwrap();
            writer.add_doc(r#"{"test3":"ok"}"#, &mut sink).unwrap();
        }
        writer.finish(&mut sink).unwrap();
    })
}

#[bench]
fn bench_reading(b: &mut test::Bencher) {
    let mut writer = DocWriter::new(0);
    let mut sink = vec![];
    for _ in 0..10_000 {
        writer.add_doc(r#"{"test":"ok"}"#, &mut sink).unwrap();
        writer.add_doc(r#"{"test2":"ok"}"#, &mut sink).unwrap();
        writer.add_doc(r#"{"test3":"ok"}"#, &mut sink).unwrap();
    }
    writer.finish(&mut sink).unwrap();

    use std::slice;
    let offset_bytes = unsafe {
        slice::from_raw_parts(writer.offsets.as_ptr() as *const u8, writer.offsets.len() * mem::size_of::<(u32, u64)>())
    };

    b.iter(|| {
        for i in 1..1_000 {
            DocLoader::get_doc(io::Cursor::new(&sink), &offset_bytes, i).unwrap();
        }
    })
}

#[inline]
fn decode_pos<T: Copy + Default, K: Copy + Default>(pos: usize, slice:&[u8]) -> (T, K) {
    let mut out: (T, K) = Default::default();
    let byte_pos = mem::size_of::<(T, K)>() * pos;
    unsafe {
        slice[byte_pos as usize..]
            .as_ptr()
            .copy_to_nonoverlapping(&mut out as *mut (T, K) as *mut u8, mem::size_of::<(T, K)>());
    }
    out
}

#[derive(Debug)]
struct SearchHit<T, K> {
    found: bool,
    lower: (T, K),
    upper: (T, K),
}

#[inline]
fn binary_search_slice<T: Ord + Copy + Default + std::fmt::Debug, K: Copy + Default + std::fmt::Debug>(mut size: usize, id: T, slice:&[u8]) -> SearchHit<T, K> {
    // if size == 0 {
    //     return None;
    // }
    let mut base = 0usize;
    while size > 1 {
        let half = size / 2;
        let mid = base + half;
        // mid is always in [0, size), that means mid is >= 0 and < size.
        // mid >= 0: by definition
        // mid < size: mid = size / 2 + size / 4 + size / 8 ...
        let cmp = decode_pos::<T, K>(mid, &slice).0.cmp(&id);
        base = if cmp == Greater { base } else { mid };
        size -= half;
    }

    let hit = decode_pos(base, &slice); // TODO HANDLE OUT OF BOUNDS
    let hit_next = decode_pos(base + 1, &slice);
    SearchHit {
        lower: hit,
        upper: hit_next,
        found: id == hit.0
    }
}
