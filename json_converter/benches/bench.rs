#[macro_use]
extern crate criterion;
extern crate json_converter;
#[macro_use]
extern crate serde_json;

use criterion::Criterion;

use json_converter::ForEachOpt;
use json_converter::IDHolder;
use json_converter::for_each_element;
use serde_json::{Deserializer, Value};

fn criterion_benchmark(c: &mut Criterion) {

    let long_string :Vec<serde_json::Value> = (0..50000).map(|_|
        json!({
            "a": 1,
            "more": ["ok", "nice"],
            "objects": [{
                "stuff": "yii"
            },{
                "stuff": "yaa"
            }]
        })
    ).collect();

    let mut opt = ForEachOpt {};
    let mut id_holder = IDHolder::new();

    let data = json!(long_string);
    let data_str = serde_json::to_string(&data).unwrap();

    Criterion::default()
        .bench_function("walk json", |b| b.iter(|| {
            let mut cb_text = |_anchor_id: u32, _value: &str, _path: &str, _parent_val_id: u32| {
                // println!("TEXT: path {} value {} parent_val_id {}",path, value, parent_val_id);
            };
            let mut callback_ids = |_anchor_id: u32, _path: &str, _val_id: u32, _parent_val_id: u32| {
                // println!("IDS: path {} val_id {} parent_val_id {}",path, val_id, parent_val_id);
            };

            let stream = Deserializer::from_str(&data_str).into_iter::<Value>();
            for_each_element(stream, &mut id_holder, &mut opt, &mut cb_text, &mut callback_ids);
        }));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
