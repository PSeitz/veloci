#![feature(test)]
extern crate test;

use doc_store::DocLoader;
use std::mem;
use doc_store::DocWriter;

#[bench]
fn bench_creation_im(b: &mut test::Bencher) {
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
fn bench_reading_im(b: &mut test::Bencher) {
    let mut writer = DocWriter::new(0);
    let mut sink = vec![];
    for _ in 0..10_000 {
        writer.add_doc(r#"{"test":"ok"}"#, &mut sink).unwrap();
        writer.add_doc(r#"{"test2":"ok"}"#, &mut sink).unwrap();
        writer.add_doc(r#"{"test3":"ok"}"#, &mut sink).unwrap();
    }
    writer.finish(&mut sink).unwrap();

    use std::slice;
    let offset_bytes = unsafe { slice::from_raw_parts(writer.offsets.as_ptr() as *const u8, writer.offsets.len() * mem::size_of::<(u32, u64)>()) };

    b.iter(|| {
        let mut total_len = 0;
        for _ in 0..10 {
            let doc = DocLoader::get_doc(&sink, &offset_bytes, 0).unwrap();
            total_len += doc.len();
        }
        total_len
    })
}
