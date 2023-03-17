use std::{cmp::Ordering::Greater, io, io::prelude::*, mem};
use vint32::{iterator::VintArrayIterator, vint_array::VIntArray};

const FLUSH_THRESHOLD: usize = 16_384;
const VALUE_OFFSET: u64 = 1;

#[derive(Debug)]
pub struct DocLoader {}
impl DocLoader {
    /// offsets are the offsets produced by the `DocWriter`
    pub fn get_doc(data_reader: &[u8], offsets: &[u8], pos: usize) -> Result<String, io::Error> {
        let size = offsets.len() / mem::size_of::<(u32, u64)>();

        // binary search on the slice to find the correct block where the document resides
        // returns the start and end boundaries of the block
        let hit = binary_search_slice::<u32, u64>(size, pos as u32, &offsets);

        let start = hit.lower.1 - VALUE_OFFSET;
        let end = hit.upper.1 - VALUE_OFFSET;

        // load compressed block data into buffer
        let mut output = lz4_flex::decompress_size_prepended(&data_reader[start as usize..end as usize]).unwrap();

        let mut arr = VintArrayIterator::new(&output);
        let arr_size = arr.next().unwrap();

        let mut data_start = arr.pos;
        let mut arr = VintArrayIterator::new(&output[arr.pos..arr.pos + arr_size as usize]);
        let first_id_in_block = arr.next().unwrap();

        let mut doc_offsets_in_block: Vec<u32> = vec![];
        while let Some(off) = arr.next() {
            doc_offsets_in_block.push(off);
        }
        data_start += arr.pos;
        let pos_in_block = pos - first_id_in_block as usize;

        // get the document from the decompressed data
        let document_start_pos = data_start + doc_offsets_in_block[pos_in_block + 1] as usize;
        let document_end_pos = data_start + doc_offsets_in_block[pos_in_block] as usize;
        output.resize(document_start_pos, 0);
        let doc = output.split_off(document_end_pos);
        let s = unsafe { String::from_utf8_unchecked(doc) };
        Ok(s)
    }
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
    let offset_bytes = unsafe { slice::from_raw_parts(writer.offsets.as_ptr() as *const u8, writer.offsets.len() * mem::size_of::<(u32, u64)>()) };

    assert_eq!(doc1.to_string(), DocLoader::get_doc(&sink, &offset_bytes, 0).unwrap());
}

#[derive(Debug)]
pub struct DocWriter {
    pub curr_id: u32,
    pub bytes_indexed: u64,
    /// the offsets holds metadata for the block
    /// the tuple consists of (the first id in the block, the position of the block in the data)
    pub offsets: Vec<(u32, u64)>,
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
    pub fn new(current_offset: u64) -> Self {
        DocWriter {
            curr_id: 0,
            bytes_indexed: 0,
            offsets: vec![],
            current_offset,
            current_block: Default::default(),
        }
    }

    /// add a document to the current block
    pub fn add_doc<W: Write>(&mut self, doc: &str, out: W) -> Result<(), io::Error> {
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
        self.curr_id += 1;
        Ok(())
    }

    /// flushes the current block to out
    fn flush<W: Write>(&mut self, mut out: W) -> Result<(), io::Error> {
        // write first_id_in_block
        let mut arr = VIntArray::default();
        arr.encode(self.current_block.first_id_in_block);
        arr.encode_vals(&self.current_block.doc_offsets_in_cache);

        let mut data = arr.serialize();
        data.extend(self.current_block.data.as_slice());
        let output = lz4_flex::compress_prepend_size(&data);

        // println!("CHECKO cache[data_start] {:?}", char::from(cache[129]));
        out.write_all(&output).unwrap();

        self.offsets.push((self.current_block.first_id_in_block as u32, self.current_offset + VALUE_OFFSET as u64));
        self.current_offset += output.len() as u64;
        self.current_block.data.clear();
        self.current_block.doc_offsets_in_cache.clear();
        out.flush()?;
        Ok(())
    }

    pub fn finish<W: Write>(&mut self, out: W) -> Result<(), io::Error> {
        self.flush(out)?;
        self.offsets.push((self.curr_id as u32 + 1, self.current_offset + VALUE_OFFSET as u64));
        Ok(())
    }
}

#[test]
fn test_doc_store() {
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
    let offset_bytes = unsafe { slice::from_raw_parts(writer.offsets.as_ptr() as *const u8, writer.offsets.len() * mem::size_of::<(u32, u64)>()) };

    assert_eq!(doc1.to_string(), DocLoader::get_doc(&sink, &offset_bytes, 0).unwrap());
    assert_eq!(doc2.to_string(), DocLoader::get_doc(&sink, &offset_bytes, 1).unwrap());
    assert_eq!(doc3.to_string(), DocLoader::get_doc(&sink, &offset_bytes, 2).unwrap());
}

#[inline]
fn decode_pos<T: Copy + Default, K: Copy + Default>(pos: usize, slice: &[u8]) -> (T, K) {
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
    #[allow(dead_code)]
    found: bool,
    lower: (T, K),
    upper: (T, K),
}

#[inline]
fn binary_search_slice<T: Ord + Copy + Default + std::fmt::Debug, K: Copy + Default + std::fmt::Debug>(mut size: usize, id: T, slice: &[u8]) -> SearchHit<T, K> {
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
        found: id == hit.0,
    }
}
