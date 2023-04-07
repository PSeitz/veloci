use crate::{error::VelociError, persistence::Persistence};
use doc_store::DocStoreWriter;
use std::{io::Write, path::Path, str};

#[derive(Debug)]
pub(crate) struct DocWriteRes {
    pub(crate) num_doc_ids: u32,
    #[allow(dead_code)]
    pub(crate) bytes_indexed: u64,
}

pub(crate) fn write_docs<K, S: AsRef<str>>(persistence: &mut Persistence, stream3: K) -> Result<DocWriteRes, VelociError>
where
    K: Iterator<Item = S>,
{
    info_time!("write_docs");
    let path = Path::new("data");
    let mut file_out = persistence.directory.open_append(path)?;

    let mut doc_store = DocStoreWriter::new(0);
    for doc in stream3 {
        doc_store.add_doc(doc.as_ref(), &mut file_out)?;
    }
    doc_store.finish(&mut file_out)?;
    file_out.flush()?;

    // create_cache.term_data.current_offset = doc_store.current_offset;
    persistence.metadata.num_docs = doc_store.curr_id.into();
    persistence.metadata.bytes_indexed = doc_store.bytes_indexed;
    Ok(DocWriteRes {
        num_doc_ids: doc_store.curr_id,
        bytes_indexed: doc_store.bytes_indexed,
    })
}
