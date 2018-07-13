use csv;

use serde_json;

pub fn convert_to_json(csv_path: &str, headers: Vec<String>) -> serde_json::Value {
    let mut rdr = csv::Reader::from_file(csv_path).unwrap().has_headers(false).escape(Some(b'\\'));
    let mut data = vec![];
    for record in rdr.decode() {
        let els: Vec<Option<String>> = record.unwrap();
        let mut entry = json!({});
        for (i, el) in els.iter().enumerate() {
            if let &Some(ref text) = el {
                entry[headers[i].clone()] = json!(text);
            }
        }
        data.push(entry);
    }
    json!(data)
}
