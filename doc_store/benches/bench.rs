#![feature(test)]
extern crate test;

use doc_store::DocLoader;
use doc_store::DocStoreWriter;

#[bench]
fn bench_creation_im(b: &mut test::Bencher) {
    b.iter(|| {
        let mut writer = DocStoreWriter::new(0);
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
    let mut writer = DocStoreWriter::new(0);
    let mut sink = vec![];
    for _ in 0..10_000 {
        writer.add_doc(r#"{"test":"ok"}"#, &mut sink).unwrap();
        writer.add_doc(r#"{"test2":"ok"}"#, &mut sink).unwrap();
        writer.add_doc(r#"{"test3":"ok"}"#, &mut sink).unwrap();
    }
    writer.finish(&mut sink).unwrap();

    let doc_loader = DocLoader::open(&sink);
    b.iter(|| {
        let mut total_len = 0;
        for i in 0..1_000 {
            let doc = doc_loader.get_doc(i as u32);
            total_len += doc.len();
        }
        total_len
    })
}
