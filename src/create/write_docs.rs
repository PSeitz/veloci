use crate::{error::VelociError, persistence::Persistence};
use doc_store::DocWriter;
use std::{mem, str};

#[derive(Debug)]
pub(crate) struct DocWriteRes {
    pub(crate) num_doc_ids: u32,
    pub(crate) bytes_indexed: u64,
    pub(crate) offset: u64,
}

pub(crate) fn write_docs<K, S: AsRef<str>>(persistence: &mut Persistence, stream3: K) -> Result<DocWriteRes, VelociError>
where
    K: Iterator<Item = S>,
{
    info_time!("write_docs");
    let mut file_out = persistence.get_buffered_writer("data")?;

    let mut doc_store = DocWriter::new(0);
    for doc in stream3 {
        doc_store.add_doc(doc.as_ref(), &mut file_out)?;
    }
    doc_store.finish(&mut file_out)?;
    // create_cache.term_data.current_offset = doc_store.current_offset;
    use std::slice;
    let slice = unsafe { slice::from_raw_parts(doc_store.offsets.as_ptr() as *const u8, doc_store.offsets.len() * mem::size_of::<(u32, u64)>()) };
    persistence.write_data_offset(slice, &doc_store.offsets)?;
    persistence.metadata.num_docs = doc_store.curr_id.into();
    persistence.metadata.bytes_indexed = doc_store.bytes_indexed;
    Ok(DocWriteRes {
        num_doc_ids: doc_store.curr_id,
        bytes_indexed: doc_store.bytes_indexed,
        offset: doc_store.current_offset,
    })
}
