#[cfg(test)]
mod bencho {
    use for_each_element;
    use serde_json;
    use serde_json::{Deserializer, Value};
    use IDHolder;

    use test::Bencher;

    #[bench]
    fn name(b: &mut Bencher) {
        let data: Vec<serde_json::Value> = (0..20000)
            .map(|_| {
                json!({
                    "a": 1,
                    "commonness": 35,
                    "ent_seq": "1259290",
                    "romaji": "Miru",
                    "text": "みる",
                    "more": ["ok", "nice"],
                    "objects": [{
                        "stuff": "yii"
                    },{
                        "stuff": "yaa"
                    }]
                })
            })
            .collect();

        let mut id_holder = IDHolder::new();

        // let data = json!(long_string);
        // let data_str = serde_json::to_string(&data).unwrap();

                // let data = json!(long_string);
        let mut json_string_line_seperatred = String::new();
        for val in data {
            json_string_line_seperatred.push_str(&serde_json::to_string(&val).unwrap());
            json_string_line_seperatred.push_str("\n");
        }

        b.iter(|| {
            // let texts = vec![];
            // texts.reserve(5000);
            let mut cb_text = |_anchor_id: u32, _value: &str, _path: &str, _parent_val_id: u32| {
                // println!("TEXT: path {} value {} parent_val_id {}",path, value, parent_val_id);
            };
            let mut callback_ids = |_anchor_id: u32, _path: &str, _val_id: u32, _parent_val_id: u32| {
                // println!("IDS: path {} val_id {} parent_val_id {}",path, val_id, parent_val_id);
            };

            // let stream = Deserializer::from_str(&json_string_line_seperatred).into_iter::<Value>();
            let stream = json_string_line_seperatred.lines().map(|line| serde_json::from_str(&line));
            for_each_element(stream, &mut id_holder, &mut cb_text, &mut callback_ids);
        })
    }

}
