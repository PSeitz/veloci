
use csv;


fn get_allterms_csv(csv_path: &str, attr_pos: usize, options: &FulltextIndexOptions) -> FnvHashMap<String, TermInfo> {
    // char escapeChar = 'a';
    // MATNR, ISMTITLE, ISMORIGTITLE, ISMSUBTITLE1, ISMSUBTITLE2, ISMSUBTITLE3, ISMARTIST, ISMLANGUAGES, ISMPUBLDATE, EAN11, ISMORIDCODE
    info_time!("get_allterms_csv total");
    let mut terms: FnvHashMap<String, TermInfo> = FnvHashMap::default();
    let mut rdr = csv::Reader::from_file(csv_path).unwrap().has_headers(false).escape(Some(b'\\'));
    for record in rdr.decode() {
        let els: Vec<Option<String>> = record.unwrap();
        if els[attr_pos].is_none() {
            continue;
        }
        let normalized_text = util::normalize_text(els[attr_pos].as_ref().unwrap());

        if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&normalized_text) {
            continue;
        }
        // terms.insert(els[attr_pos].as_ref().unwrap().clone());
        // terms.insert(normalized_text.clone());
        {
            let stat = terms.entry(normalized_text.clone()).or_insert(TermInfo::default());
            stat.num_occurences += 1;
        }
        if options.tokenize && normalized_text.split(" ").count() > 1 {
            for token in normalized_text.split(" ") {
                let token_str = token.to_string();
                if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&token_str) {
                    continue;
                }
                // terms.insert(token_str);
                let stat = terms.entry(token_str.clone()).or_insert(TermInfo::default());
                stat.num_occurences += 1;
            }
        }
    }
    info_time!("get_allterms_csv sort");
    set_ids(&mut terms);
    terms
}

pub fn create_fulltext_index_csv(
    csv_path: &str, attr_name: &str, attr_pos: usize, options: FulltextIndexOptions, mut persistence: &mut Persistence
) -> Result<(), io::Error> {
    let now = Instant::now();
    let all_terms = get_allterms_csv(csv_path, attr_pos, &options);
    println!("all_terms {} {}ms", csv_path, (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));

    let mut tuples: Vec<ValIdPair> = vec![];
    let mut tokens: Vec<ValIdPair> = vec![];
    let mut row: i64 = -1;

    let mut rdr = csv::Reader::from_file(csv_path).unwrap().has_headers(false).escape(Some(b'\\'));
    for record in rdr.decode() {
        row += 1;
        let els: Vec<Option<String>> = record.unwrap();
        if els[attr_pos].is_none() {
            continue;
        }
        let normalized_text = util::normalize_text(els[attr_pos].as_ref().unwrap());
        if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&normalized_text) {
            continue;
        }

        // let val_id = all_terms.binary_search(&normalized_text).unwrap();
        let val_id = all_terms.get(&normalized_text).unwrap().id;
        tuples.push(ValIdPair { valid:         val_id as u32, parent_val_id: row as u32 });
        trace!("Found id {:?} for {:?}", val_id, normalized_text);
        if options.tokenize && normalized_text.split(" ").count() > 1 {
            for token in normalized_text.split(" ") {
                let token_str = token.to_string();
                if options.stopwords.is_some() && options.stopwords.as_ref().unwrap().contains(&token_str) {
                    continue;
                }
                // let tolen_val_id = all_terms.binary_search(&token_str).unwrap();
                let tolen_val_id = all_terms.get(&token_str).unwrap().id;
                trace!("Adding to tokens {:?} : {:?}", token, tolen_val_id);
                tokens.push(ValIdPair { valid:         tolen_val_id as u32, parent_val_id: val_id as u32 });
            }
        }
    }

    let is_text_index = true;
    let path_name = util::get_file_path_name(attr_name, is_text_index);
    persistence.write_tuple_pair(&mut tuples, &concat(&path_name, ".valueIdToParent"))?;

    if options.tokenize {
        persistence.write_tuple_pair(&mut tokens, &concat(&path_name, ".tokens"))?;
    }

    store_full_text_info(&mut persistence, all_terms, &attr_name, &options)?;

    println!("createIndexComplete {} {}ms", attr_name, (now.elapsed().as_secs() as f64 * 1_000.0) + (now.elapsed().subsec_nanos() as f64 / 1000_000.0));

    Ok(())
}



pub fn create_indices_csv(folder: &str, csv_path: &str, indices: &str) -> Result<(), CreateError> {
    // let indices_json:Result<Vec<CreateIndex>> = serde_json::from_str(indices);
    // println!("{:?}", indices_json);
    let indices_json: Vec<CreateIndex> = serde_json::from_str(indices)?;
    let mut persistence = Persistence::create(folder.to_string())?;
    for el in indices_json {
        match el {
            CreateIndex::FulltextInfo(el)/*{ fulltext: path, options, attr_pos : _ }*/ => {
                create_fulltext_index_csv(csv_path, &el.fulltext, el.attr_pos.unwrap(), el.options.unwrap_or(Default::default()), &mut persistence)?
            },
            CreateIndex::BoostInfo(_) => {} // @Temporary // @FixMe
        }
    }

    let json = create_json_from_c_s_v(csv_path);
    persistence.write_json_to_disk(&json, "data")?;

    persistence.write_meta_data()?;

    Ok(())
}

fn create_json_from_c_s_v(csv_path: &str) -> Vec<Value> {
    let mut res = vec![];
    // let mut row: i64 = -1;

    let mut rdr = csv::Reader::from_file(csv_path).unwrap().has_headers(false).escape(Some(b'\\'));
    for record in rdr.decode() {
        // row+=1;
        let els: Vec<Option<String>> = record.unwrap();
        let mut map = FnvHashMap::default();
        // if els[attr_pos].is_none() { continue;}

        map.insert("MATNR".to_string(), els[0].clone().unwrap());
        let v: Value = serde_json::from_str(&serde_json::to_string(&map).unwrap()).unwrap();
        res.push(v);
    }
    res
}
