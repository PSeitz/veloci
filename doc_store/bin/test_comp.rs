extern crate doc_store;

fn main() {

	// let input: String = std::fs::read_to_string("../../jmdict_search/jmdict_split.json").unwrap();

 //    let compressed = lz4_flex::compress(COMPRESSION10MB as &[u8]);
 //    for _ in 0..100000 {
 //        decompress(&compressed).unwrap();
 //    }

    let input: String = std::fs::read_to_string("../../jmdict_search/jmdict_split.json").unwrap();
    let mut writer = doc_store::DocWriter::new(0);
    let mut sink = vec![];
    for line in input.lines() {
        writer.add_doc(line, &mut sink).unwrap();
    }
    writer.finish(&mut sink).unwrap();

    println!("{:?}", sink.len());
    
}

