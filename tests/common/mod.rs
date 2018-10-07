
use search_lib::*;
// use serde_json::Value;

pub fn create_test_persistence(folder: &str, indices: &str, test_data:&[u8], token_values: Option<(String, serde_json::Value)>) -> persistence::Persistence {
    trace::enable_log();

    let mut persistence_type = persistence::PersistenceType::Transient;
    if let Some(val) = std::env::var_os("PersistenceType") {
        if val.clone().into_string().unwrap() == "Transient" {
            persistence_type = persistence::PersistenceType::Transient;
        } else if val.clone().into_string().unwrap() == "Persistent" {
            persistence_type = persistence::PersistenceType::Persistent;
        } else {
            panic!("env PersistenceType needs to be Transient or Persistent");
        }
    }

    let path = "test_files/".to_string() + folder;
    let mut pers = persistence::Persistence::create_type(path.to_string(), persistence_type.clone()).unwrap();

    let mut out: Vec<u8> = vec![];
    search_lib::create::convert_any_json_data_to_line_delimited(test_data, &mut out).unwrap();
    println!("{:?}", create::create_indices_from_str(&mut pers, std::str::from_utf8(&out).unwrap(), indices, None, true));

    {
        if let Some(token_values) = token_values {
            create::add_token_values_to_tokens(&mut pers, &token_values.0, &token_values.1.to_string()).expect("Could not add token values");
        }
    }

    if persistence_type == persistence::PersistenceType::Persistent {
        pers = persistence::Persistence::load(path.to_string()).expect("Could not load persistence");
    }
    pers
}


#[allow(unused_macros)]
macro_rules! search_to_hits {
    ($x:expr) => {{
        let pers = &TEST_PERSISTENCE;
        search::search($x, &pers)
    }}
}

#[allow(unused_macros)]
macro_rules! search_testo_to_doc {
    ($x:expr) => {{
        let requesto: search::Request = serde_json::from_str(&$x.to_string()).expect("Can't parse json");
        let pers = &TEST_PERSISTENCE;
        search::to_search_result(&pers, search::search(requesto.clone(), &pers).expect("search error"), &requesto.select)
    }}
}

#[allow(unused_macros)]
macro_rules! search_testo_to_doco_qp {
    ($x:expr) => {{
        let pers = &TEST_PERSISTENCE;
        let requesto = query_generator::search_query(&pers, $x).unwrap();
        search::to_search_result(&pers, search::search(requesto.clone(), &pers).expect("search error"), &requesto.select)
    }}
}