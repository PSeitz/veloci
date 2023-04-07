use std::convert::TryInto;

use std::{cmp::Ordering::Greater, cmp::Ordering::Less, io, io::prelude::*, mem};
use vint32::iterator::VintArrayIterator;
use vint32::vint_array::VIntArray;

const FLUSH_THRESHOLD: usize = 16_384;
const VALUE_OFFSET: u32 = 1;

#[derive(Debug)]
pub struct DocLoader<'a> {
    blocks: &'a [u8],
    block_index: &'a [u8],
}
impl<'a> DocLoader<'a> {
    pub fn open(data: &'a [u8]) -> Self {
        let index_size = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap());
        let block_index = &data[..data.len() - 4];
        Self {
            blocks: data,
            block_index: &block_index[block_index.len() - index_size as usize..],
        }
    }

    /// offsets are the offsets produced by the `DocWriter`
    pub fn get_doc(&self, doc_id: u32) -> Result<String, io::Error> {
        let offsets = self.block_index;
        let size = offsets.len() / mem::size_of::<(u32, u32)>();

        // binary search on the slice to find the correct block where the document resides
        // returns the start and end boundaries of the block
        let hit = binary_search_slice(size, doc_id, offsets);

        let start = hit.lower.1 - VALUE_OFFSET;
        let end = hit.upper.1 - VALUE_OFFSET;

        // load compressed block data into buffer
        let mut output = lz4_flex::decompress_size_prepended(&self.blocks[start as usize..end as usize]).unwrap();

        let mut arr = VintArrayIterator::new(&output);
        let arr_size = arr.next().unwrap();

        let mut data_start = arr.pos;
        let mut arr = VintArrayIterator::new(&output[arr.pos..arr.pos + arr_size as usize]);
        let first_id_in_block = arr.next().unwrap();

        let mut doc_offsets_in_block: Vec<u32> = vec![];
        for off in arr.by_ref() {
            doc_offsets_in_block.push(off);
        }
        data_start += arr.pos;
        let pos_in_block = doc_id as usize - first_id_in_block as usize;

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
    let mut writer = DocStoreWriter::new(0);

    let mut sink = vec![];

    let doc1 = r#"{"category": "superb", "tags": ["nice", "cool"] }"#;
    for _ in 0..2640 {
        writer.add_doc(doc1, &mut sink).unwrap();
    }

    writer.finish(&mut sink).unwrap();

    let doc_loader = DocLoader::open(&sink);
    for i in 0..2640 {
        assert_eq!(doc1.to_string(), doc_loader.get_doc(i as u32).unwrap());
    }
}

#[derive(Debug)]
pub struct DocStoreWriter {
    pub curr_id: u32,
    pub bytes_indexed: u64,
    /// the offsets holds metadata for the block
    /// the tuple consists of (the first doc id in the block, the start byte of the block in the data)
    pub offsets: Vec<(u32, u32)>,
    pub current_offset: u32,
    current_block: DocWriterBlock,
}

#[derive(Debug, Default)]
struct DocWriterBlock {
    data: Vec<u8>,
    doc_offsets_in_cache: Vec<u32>,
    first_id_in_block: u32,
}

impl DocStoreWriter {
    pub fn new(current_offset: u32) -> Self {
        DocStoreWriter {
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
            self.flush_block(out)?;
        }
        self.curr_id += 1;
        Ok(())
    }

    /// flushes the current block to out
    fn flush_block<W: Write>(&mut self, mut out: W) -> Result<(), io::Error> {
        // write first_id_in_block
        let mut arr = VIntArray::default();
        arr.encode(self.current_block.first_id_in_block);
        arr.encode_vals(&self.current_block.doc_offsets_in_cache);

        let mut data = arr.serialize();
        data.extend(self.current_block.data.as_slice());
        let output = lz4_flex::compress_prepend_size(&data);

        out.write_all(&output).unwrap();

        self.offsets.push((self.current_block.first_id_in_block, self.current_offset + VALUE_OFFSET));
        self.current_offset += output.len() as u32;
        self.current_block.data.clear();
        self.current_block.doc_offsets_in_cache.clear();
        out.flush()?;
        Ok(())
    }

    pub fn finish<W: Write>(&mut self, mut out: W) -> Result<(), io::Error> {
        self.flush_block(&mut out)?;
        self.offsets.push((self.curr_id + 1, self.current_offset + VALUE_OFFSET));

        let mut bytes_written = 0;
        for (id, current_offset) in &self.offsets {
            out.write_all(&id.to_le_bytes())?;
            out.write_all(&current_offset.to_le_bytes())?;
            bytes_written += 8;
        }
        let index_size = self.offsets.len() * std::mem::size_of_val(&self.offsets[0]);

        out.write_all(&(index_size as u32).to_le_bytes())?;
        out.flush()?;

        Ok(())
    }
}

#[test]
fn test_doc_store() {
    let mut writer = DocStoreWriter::new(0);

    let mut sink = vec![];
    let doc1 = r#"{"test":"ok"}"#;
    let doc2 = r#"{"test2":"ok"}"#;
    let doc3 = r#"{"test3":"ok"}"#;
    writer.add_doc(doc1, &mut sink).unwrap();
    writer.add_doc(doc2, &mut sink).unwrap();
    writer.add_doc(doc3, &mut sink).unwrap();
    writer.finish(&mut sink).unwrap();

    let doc_loader = DocLoader::open(&sink);
    assert_eq!(doc1.to_string(), doc_loader.get_doc(0_u32).unwrap());
    assert_eq!(doc2.to_string(), doc_loader.get_doc(1_u32).unwrap());
    assert_eq!(doc3.to_string(), doc_loader.get_doc(2_u32).unwrap());
}

#[inline]
fn decode_pos(pos: usize, slice: &[u8]) -> (u32, u32) {
    let start_offset = pos * 8;
    let slice = &slice[start_offset..];
    let id = u32::from_le_bytes(slice[..4].try_into().unwrap());
    let offset = u32::from_le_bytes(slice[4..4 + 4].try_into().unwrap());
    (id, offset)
}

#[derive(Debug)]
struct SearchHit {
    #[allow(dead_code)]
    found: bool,
    lower: (u32, u32),
    upper: (u32, u32),
}

#[inline]
fn binary_search_slice(mut size: usize, id: u32, slice: &[u8]) -> SearchHit {
    let mut left = 0;
    let mut right = size;
    while left < right {
        let mid = left + size / 2;

        let cmp = decode_pos(mid, slice).0.cmp(&id);

        if cmp == Less {
            left = mid + 1;
        } else if cmp == Greater {
            right = mid;
        } else {
            left = mid;
            let hit = decode_pos(left, slice);
            let hit_next = decode_pos(left + 1, slice);
            return SearchHit {
                lower: hit,
                upper: hit_next,
                found: id == hit.0,
            };

            // SAFETY: same as the `get_unchecked` above
        }

        size = right - left;
    }

    let hit = decode_pos(left - 1, slice);
    let hit_next = decode_pos(left, slice);
    SearchHit {
        lower: hit,
        upper: hit_next,
        found: id == hit.0,
    }
}
