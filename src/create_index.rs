extern crate flexi_logger;
extern crate env_logger;
extern crate fst;
extern crate fst_levenshtein;
extern crate search_lib;
#[macro_use]
extern crate serde_json;

use fst::{IntoStreamer, MapBuilder, Set};
use fst_levenshtein::Levenshtein;
use std::fs::File;
use std::io::prelude::*;
use std::io;

use std::time::Instant;
use std::str;


fn main() {
    // env_logger::init().unwrap();
    search_lib::trace::enable_log();
    let e = std::env::args().nth(1).expect("require command line parameter");

    for jeppo in std::env::args().skip(1){
        match jeppo.as_ref() {
            "healthcare" => println!("{:?}",create_healtcare()),
            "jmdict" => println!("{:?}",create_jmdict_index()),
            "gutenberg" => println!("{:?}",create_book_index()),
            _ => {}
        };
    }

    // println!("{:?}",create_healtcare());

    // create_thalia_index();
    // println!("{:?}",create_jmdict_index());
    // println!("{:?}",create_book_index());

    // {
    //     let my_time = util::MeasureTime::new("jmdict load time", util::MeasureTimeLogLevel::Print);
    //     let mut _pers:persistence::Persistence = persistence::Persistence::load("jmdict".to_string()).expect("could not load jmdict");
    // }

    // let doc_loader = doc_loader::DocLoader::new("jmdict", "data");
    // let now = Instant::now();
    // println!("{:?}", doc_loader.get_doc(1000).unwrap());
    // println!("Load Time: {}", (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));

    // println!("{:?}",test_build_fst());

    // server::start_server();
}


#[allow(dead_code)]
fn create_thalia_index() {
    // let all_terms = loadcsv("./data.csv", 0);
    // println!("{:?}", all_terms.len());

    // File::create("MATNR").unwrap().write_all(all_terms.join("\n").as_bytes()).unwrap();
    let indices = r#"
    [
        { "fulltext":"MATNR", "attr_pos" : 0 },
        { "fulltext":"ISMTITLE", "attr_pos" : 1, "options":{"tokenize":true}},
        { "fulltext":"ISMORIGTITLE", "attr_pos" : 2, "options":{"tokenize":true}},
        { "fulltext":"ISMSUBTITLE1", "attr_pos" : 3, "options":{"tokenize":true}},
        { "fulltext":"ISMSUBTITLE2", "attr_pos" : 4, "options":{"tokenize":true}},
        { "fulltext":"ISMSUBTITLE3", "attr_pos" : 5, "options":{"tokenize":true}},
        { "fulltext":"ISMARTIST", "attr_pos" : 6, "options":{"tokenize":true}},
        { "fulltext":"ISMLANGUAGES", "attr_pos" : 7},
        { "fulltext":"ISMPUBLDATE", "attr_pos" : 8},
        { "fulltext":"EAN11", "attr_pos" : 9},
        { "fulltext":"ISMORIDCODE", "attr_pos" : 10}
    ]
    "#;

    // println!("{:?}", search_lib::create::create_indices_csv("csv_test", "./data.csv", indices));
}


#[allow(dead_code)]
fn create_jmdict_index() -> Result<(), io::Error> {
    let indices = r#"
    [
    {
        "boost": "commonness",
        "options": { "boost_type": "int" }
    },
    { "fulltext": "kanji[].text" },
    { "fulltext": "kana[].text" },
    {
        "fulltext": "meanings.ger[].text",
        "options": { "tokenize": true  }
    },
    {
        "boost": "meanings.ger[].rank",
        "options": { "boost_type": "int" }
    },
    {
        "fulltext": "meanings.eng[]",
        "options": { "tokenize": true  }
    },
    {
        "boost": "kanji[].commonness",
        "options": { "boost_type": "int" }
    },
    {
        "boost": "kana[].commonness",
        "options": { "boost_type": "int" }
    }
    ]
    "#;
    let mut f = File::open("jmdict.json")?;
    let mut s = String::new();
    f.read_to_string(&mut s)?;
    println!("{:?}", search_lib::create::create_indices("jmdict", &s, indices));
    Ok(())
}

#[allow(dead_code)]
fn create_book_index() -> Result<(), io::Error> {
    let indices = "[]";
    let mut f = File::open("1342-0.txt")?;
    let mut s = String::new();
    f.read_to_string(&mut s)?;


    let books = (0..100).map(|el| json!({"title":"PRIDE AND PREJUDICE", "content":s})).collect::<Vec<_>>();

    println!("{:?}", search_lib::create::create_indices("gutenberg", &serde_json::to_string_pretty(&books).unwrap(), indices));
    // println!("{:?}", search_lib::create::create_indices("gutenberg", &json!({"title":"PRIDE AND PREJUDICE", "content":s}).to_string(), indices));
    Ok(())
}


#[allow(dead_code)]
pub fn testfst(term: &str, max_distance: u32) -> Result<(Vec<String>), fst_levenshtein::Error> {
    let mut f = File::open("de_full_2.txt").unwrap();
    let mut s = String::new();
    f.read_to_string(&mut s).unwrap();
    let lines = s.lines().collect::<Vec<&str>>();
    // lines.sort();

    println!("{:?}", lines.len());
    // A convenient way to create sets in memory.
    // let set = try!(Set::from_iter(lines));

    let keys = vec!["寿司は焦げられない"];
    let set = Set::from_iter(keys).unwrap();

    let now = Instant::now();

    let lev = Levenshtein::new(term, max_distance).unwrap();
    let stream = set.search(lev).into_stream();
    let hits = stream.into_strs().unwrap();

    println!("fst ms: {}", (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));

    // assert_eq!(hits, vec!["fo", "fob", "foo", "food"]);

    Ok((hits))
}

// fn split_at_first()  {

//     lines.sort();
//     let firsts = lines.into_iter().map(|line: &str| {
//         let splits = line.split(" ").collect::<Vec<&str>>();
//         splits[0].to_string()

//     }).collect::<Vec<String>>();
//     File::create("de_full_2.txt")?.write_all(firsts.join("\n").as_bytes());
// }

#[allow(dead_code)]
fn test_build_fst() -> Result<(), fst::Error> {
    let now = Instant::now();

    let mut f = File::open("de_full_2.txt")?;
    let mut s = String::new();
    f.read_to_string(&mut s)?;
    let lines = s.lines().collect::<Vec<&str>>();
    println!("lines: {:?}", lines.len());


    let wtr = io::BufWriter::new(File::create("map.fst")?);
    // Create a builder that can be used to insert new key-value pairs.
    let mut build = MapBuilder::new(wtr)?;

    let mut i = 0;
    for line in lines {
        build.insert(line, i).unwrap();
        i += 1;
    }

    // println!("mapsize: {:?}", build.len());
    // println!("lines: {:?}", lines.len());
    // println(dupl_terms_checker.len())
    // Finish construction of the map and flush its contents to disk.
    build.finish()?;

    println!("test_build_fst ms: {}", (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));


    Ok(())
}

// use std::collections::BTreeMap;
// use fst::raw::{Builder, Fst, Output};

// #[cfg(test)]
// mod testero {

// use test::Bencher;
// use super::*;

//     #[bench]
//     fn bench_teh_stuff_btree(b: &mut Bencher) {
//         let mut map = BTreeMap::new();
//         for n in 1..15555555 {
//             map.insert(n, n * 30);
//         }
//         b.iter(|| {
//             // let mut hits = vec![];
//             // for i in 100000..200000 {
//             //     hits.push(map.get(&(i*50)));
//             // }
//             map.get(&1_234_567);
//             map.get(&60_000_000);
//             map.get(&12_345_678);
//             map.get(&80_345_678);
//             map.get(&123_456_789);
//         });
//     }

//     #[bench]
//     fn bench_teh_stuff_hashmap(b: &mut Bencher) {
//         let mut map = FnvHashMap::default();
//         for n in 1..15555555 {
//             map.insert(n, n * 30);
//         }
//         b.iter(|| {
//             // let mut hits = vec![];
//             // for i in 100000..200000 {
//             //     hits.push(map.get(&(i*50)));
//             // }
//             map.get(&1_234_567);
//             map.get(&60_000_000);
//             map.get(&12_345_678);
//             map.get(&80_345_678);
//             map.get(&123_456_789);
//         });
//     }

//     #[bench]
//     fn bench_teh_stuff_fst(b: &mut Bencher) {
//         let wtr = io::BufWriter::new(File::create("bencho.fst").unwrap());
//         let mut build2 = MapBuilder::new(wtr).unwrap();
//         let mut ids = vec![];
//         let mut build = Builder::memory();
//         for n in 1..15555555 {
//             // map.insert(n, n);
//             let raw_bytes : [u8; 8] = unsafe {std::mem::transmute(n as u64)};
//             // build.insert(raw_bytes, n).unwrap();
//             // build.insert(n.to_string(), n).unwrap();
//             // ids.push(format!("{:09}", n));
//             // ids.push(n.to_string());
//             ids.push((raw_bytes, n * 30));
//         }
//         // ids.sort();
//         ids.sort_by_key(|&(k, ref v)| k);
//         for (ref el, ref v) in ids {
//             build.insert(el.clone(), *v  ).unwrap();
//             build2.insert(el.clone(), *v ).unwrap();
//         }
//         let fst_bytes = build.into_inner().unwrap();
//         build2.finish().unwrap();
//         // Create an Fst that we can query.
//         let fst = Fst::from_bytes(fst_bytes).unwrap();
//         let raw_bytes1 : [u8; 8] = unsafe {std::mem::transmute(1_234_567 as u64)};
//         let raw_bytes2 : [u8; 8] = unsafe {std::mem::transmute(60_000_000 as u64)};
//         let raw_bytes3 : [u8; 8] = unsafe {std::mem::transmute(12_345_678 as u64)};
//         let raw_bytes4 : [u8; 8] = unsafe {std::mem::transmute(80_345_678 as u64)};
//         let raw_bytes5 : [u8; 8] = unsafe {std::mem::transmute(123_456_789 as u64)};
//         b.iter(|| {
//             // for i in 100000..200000 {
//             //     // fst.get(format!("{:09}", i*50));
//             //     // fst.get((i*50).to_string());
//             //     let raw_bytes : [u8; 8] = unsafe {std::mem::transmute((i*50) as u32)};
//             //     fst.get(raw_bytes);
//             // }
//             fst.get(raw_bytes1);
//             fst.get(raw_bytes2);
//             fst.get(raw_bytes3);
//             fst.get(raw_bytes4);
//             fst.get(raw_bytes5);
//         });
//     }

//     #[bench]
//     fn bench_teh_stuff_fst_string_based(b: &mut Bencher) {
//         let mut ids = vec![];
//         let mut build = Builder::memory();
//         for n in 1..15555555 {
//             ids.push((format!("{:09}", n), n * 30));
//         }
//         ids.sort_by_key(|&(ref k, ref v)| k.clone());
//         for (ref el, ref v) in ids {
//             build.insert(el.clone(),  *v ).unwrap();
//         }
//         let fst_bytes = build.into_inner().unwrap();
//         // Create an Fst that we can query.
//         let fst = Fst::from_bytes(fst_bytes).unwrap();
//         b.iter(|| {
//             // for i in 100000..200000 {
//             //     // fst.get(format!("{:09}", i*50));
//             //     // fst.get((i*50).to_string());
//             //     let raw_bytes : [u8; 8] = unsafe {std::mem::transmute((i*50) as u32)};
//             //     fst.get(raw_bytes);
//             // }
//             fst.get(format!("{:09}", 1_234_567));
//             fst.get(format!("{:09}", 60_000_000));
//             fst.get(format!("{:09}", 12_345_678));
//             fst.get(format!("{:09}", 80_345_678));
//             fst.get(format!("{:09}", 123_456_789));
//         });
//     }

//     #[bench]
//     fn bench_teh_stuff_vec(b: &mut Bencher) {
//         let mut vec = vec![];
//         vec.resize(15555555, 0);
//         for n in 1..15555555 {
//             // vec.insert(n, n * 30);
//             vec[n] = n * 30;
//         }
//         b.iter(|| {
//             // let mut hits = vec![];
//             // for i in 100000..200000 {
//             //     hits.push(vec.get(i*50));
//             // }
//             vec.get(1_234_567);
//             vec.get(60_000_000);
//             vec.get(12_345_678);
//             vec.get(80_345_678);
//             vec.get(123_456_789);
//         });
//     }

//     #[bench]
//     fn bench_teh_stuff_vec_binary_search(b: &mut Bencher) {
//         let mut vec1 = vec![];
//         let mut vec2 = vec![];
//         // vec.resize(15555555, 0);
//         for n in 1..15555555 {
//             vec1.push(n);
//             vec2.push(n * 30);
//         }
//         b.iter(|| {
//             // let mut hits = vec![];
//             // for i in 100000..200000 {
//             //     hits.push(vec.get(i*50));
//             // }
//             match vec1.binary_search(&1_234_567) {
//                 Ok(mut pos) => {
//                     vec2[pos];
//                 },Err(_) => {},
//             }
//             match vec1.binary_search(&60_000_000) {
//                 Ok(mut pos) => {
//                     vec2[pos];
//                 },Err(_) => {},
//             }
//             match vec1.binary_search(&12_345_678) {
//                 Ok(mut pos) => {
//                     vec2[pos];
//                 },Err(_) => {},
//             }
//             match vec1.binary_search(&80_345_678) {
//                 Ok(mut pos) => {
//                     vec2[pos];
//                 },Err(_) => {},
//             }
//             match vec1.binary_search(&123_456_789) {
//                 Ok(mut pos) => {
//                     vec2[pos];
//                 },Err(_) => {},
//             }
//             // vec1.binary_search(&1_234_567);
//             // vec1.binary_search(&60_000_000);
//             // vec1.binary_search(&12_345_678);
//             // vec1.binary_search(&80_345_678);
//             // vec1.binary_search(&123_456_789);
//         });
//     }

// }

// #[test]
// fn it_works() {

//     assert_eq!(util::normalize_text("Hello"), "hello");
//     assert_eq!(util::normalize_text("(Hello)"), "hello");
//     assert_eq!(util::normalize_text("\"H,ell-;o"), "hello");
//     assert_eq!(util::normalize_text("Hello(f)"), "hello");
//     assert_eq!(util::normalize_text("Hello(2)"), "hello");

//     assert_eq!(util::normalize_text("majestätisches Aussehen (n)"), "majestätisches aussehen");

//     assert_eq!(util::remove_array_marker("Hello[]"), "hello");
//     assert_eq!(util::remove_array_marker("Hello[].ja"), "hello.ja");

// }











fn create_healtcare() -> Result<(), io::Error> {
    let indices = r#"
    [
        {"fulltext": "diagnosticreport[].result[].reference", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].result[].display", "options":{"tokenize":true}},
        {"fulltext": "address[].country", "options":{"tokenize":true}},
        {"fulltext": "address[].city", "options":{"tokenize":true}},
        {"fulltext": "address[].postalCode", "options":{"tokenize":true}},
        {"fulltext": "address[].state", "options":{"tokenize":true}},
        {"fulltext": "patientname[].given[]", "options":{"tokenize":true}},
        {"fulltext": "condition[].code.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "condition[].code.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "condition[].code.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component.code.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component.code.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component.code.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "patientname[].prefix[]", "options":{"tokenize":true}},
        {"fulltext": "procedure[].code.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "procedure[].code.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "procedure[].code.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueCodeableConcept.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueCodeableConcept.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueCodeableConcept.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "communication.language.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "communication.language.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "communication.language.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].verificationStatus", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].allergyintoleranceType", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].patient_reference", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "condition[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "condition[].subject_reference", "options":{"tokenize":true}},
        {"fulltext": "condition[].id", "options":{"tokenize":true}},
        {"fulltext": "condition[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "encounter[].period_end", "options":{"tokenize":true}},
        {"fulltext": "encounter[].serviceProvider_reference", "options":{"tokenize":true}},
        {"fulltext": "encounter[].period_start", "options":{"tokenize":true}},
        {"fulltext": "encounter[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "encounter[].id", "options":{"tokenize":true}},
        {"fulltext": "encounter[].patient_reference", "options":{"tokenize":true}},
        {"fulltext": "encounter[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "encounter[].status", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueQuantity_unit", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].encounter_reference", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].code_text", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueQuantity_code", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueQuantity_system", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].effectiveDateTime", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].subject_reference", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueQuantity_value", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].id", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].issued", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].valueCodeableConcept_text", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].status", "options":{"tokenize":true}},
        {"fulltext": "procedure[].extension.valueCodeableConcept.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "procedure[].extension.valueCodeableConcept.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "procedure[].extension.valueCodeableConcept.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "careplanCategory.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "careplanCategory.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "careplanCategory.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "encounter[].reason.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "encounter[].reason.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "encounter[].reason.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "maritalStatus.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "maritalStatus.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "address[].extension.extension[].valueDecimal", "options":{"tokenize":true}},
        {"fulltext": "address[].extension.extension[].url", "options":{"tokenize":true}},
        {"fulltext": "immunization.vaccineCode.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "immunization.vaccineCode.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "immunization.vaccineCode.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dispenseRequest_expectedSupplyDuration_system", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dispenseRequest_numberOfRepeatsAllowed", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dispenseRequest_quantity_unit", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dispenseRequest_expectedSupplyDuration_unit", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dispenseRequest_expectedSupplyDuration_value", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dispenseRequest_quantity_value", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].patient_reference", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dispenseRequest_expectedSupplyDuration_code", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].category[].text", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].code.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].code.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].code.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].effectiveDateTime", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].subject_reference", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].encounter_reference", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].id", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].issued", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].status", "options":{"tokenize":true}},
        {"fulltext": "patientname[].suffix[]", "options":{"tokenize":true}},
        {"fulltext": "type.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "type.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "type.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].additionalInstructions.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].additionalInstructions.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].additionalInstructions.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "patientname[].use", "options":{"tokenize":true}},
        {"fulltext": "patientname[].family", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component[].valueQuantity_system", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component[].valueQuantity_unit", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component[].valueQuantity_value", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component[].code_text", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].component[].valueQuantity_code", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].gender", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].type_text", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].maritalStatus_text", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].birthDate", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].context_reference", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].period_end", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].period_start", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].multipleBirthInteger", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].organizationname", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].deceasedDateTime", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].subject_reference", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].id", "options":{"tokenize":true}},
        {"fulltext": "healthcare[]._id", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "healthcare[].status", "options":{"tokenize":true}},
        {"fulltext": "suggest[].input", "options":{"tokenize":true}},
        {"fulltext": "suggest[].weight", "options":{"tokenize":true}},
        {"fulltext": "address[].line[]", "options":{"tokenize":true}},
        {"fulltext": "procedure[].extension[].url", "options":{"tokenize":true}},
        {"fulltext": "procedure[].extension[].valueCodeableConcept_text", "options":{"tokenize":true}},
        {"fulltext": "addresses[].reference", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].code.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].code.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].code.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "diagnosticreport[].performer[].display", "options":{"tokenize":true}},
        {"fulltext": "procedure[].reasonReference_reference", "options":{"tokenize":true}},
        {"fulltext": "procedure[].performedPeriod_end", "options":{"tokenize":true}},
        {"fulltext": "procedure[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "procedure[].subject_reference", "options":{"tokenize":true}},
        {"fulltext": "procedure[].encounter_reference", "options":{"tokenize":true}},
        {"fulltext": "procedure[].performedDateTime", "options":{"tokenize":true}},
        {"fulltext": "procedure[].performedPeriod_start", "options":{"tokenize":true}},
        {"fulltext": "procedure[].code_text", "options":{"tokenize":true}},
        {"fulltext": "procedure[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "procedure[].status", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].medicationCodeableConcept.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].medicationCodeableConcept.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].medicationCodeableConcept.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].category.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].category.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "activity[].detail_status", "options":{"tokenize":true}},
        {"fulltext": "address[].extension[].url", "options":{"tokenize":true}},
        {"fulltext": "goal[].reference", "options":{"tokenize":true}},
        {"fulltext": "allergyintolerance[].allergyintoleranceCategory[]", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].code.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].code.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "encounter[].observation[].code.coding[].display", "options":{"tokenize":true}},
        {"fulltext": "immunization[].date", "options":{"tokenize":true}},
        {"fulltext": "immunization[].primarySource", "options":{"tokenize":true}},
        {"fulltext": "immunization[].securityLabel", "options":{"tokenize":true}},
        {"fulltext": "immunization[].vaccineCode_text", "options":{"tokenize":true}},
        {"fulltext": "immunization[].encounter_reference", "options":{"tokenize":true}},
        {"fulltext": "immunization[].patient_reference", "options":{"tokenize":true}},
        {"fulltext": "immunization[].resourceType", "options":{"tokenize":true}},
        {"fulltext": "immunization[].status", "options":{"tokenize":true}},
        {"fulltext": "immunization[].wasNotGiven", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].doseQuantity_value", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].sequence", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].timing_repeat_period", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].timing_repeat_periodUnit", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].timing_repeat_frequency", "options":{"tokenize":true}},
        {"fulltext": "medicationrequest[].dosageInstruction[].asNeededBoolean", "options":{"tokenize":true}},
        {"fulltext": "activity[].detail.code.coding[].code", "options":{"tokenize":true}},
        {"fulltext": "activity[].detail.code.coding[].system", "options":{"tokenize":true}},
        {"fulltext": "activity[].detail.code.coding[].display", "options":{"tokenize":true}}
    ]
    "#;

    let mut f = File::open("healthcare.json")?;
    let mut s = String::new();
    f.read_to_string(&mut s)?;
    println!("{:?}", search_lib::create::create_indices("healthcare", &s, indices));
    Ok(())
}






