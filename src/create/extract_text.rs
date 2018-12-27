use crate::create::CreateCache;
use crate::error::VelociError;
use crate::persistence;
use crate::search::search_field;
use crate::persistence::TEXTINDEX;
use crate::persistence::Persistence;
use crate::persistence::TextIndexMetaData;
use super::fields_config::FieldsConfig;
use super::fields_config::FulltextIndexOptions;

use buffered_index_writer::BufferedIndexWriter;
use std::io;
use std::{self, str};
use num::ToPrimitive;
use crate::tokenizer::*;
use fnv::FnvHashMap;
use json_converter;
use serde_json::{self};
use crate::util::StringAdd;
use fst::{self, MapBuilder, Map};
use super::TermMap;
use super::TermInfo;
use rayon::prelude::*;

#[derive(Debug, Default)]
pub struct AllTermData {
    pub(crate) offsets: Vec<u64>,
    pub(crate) current_offset: u64,
    pub(crate) id_holder: json_converter::IDHolder,
    pub(crate) terms_in_path: FnvHashMap<String, TermDataInPath>,
}

#[derive(Debug, Default)]
pub(crate) struct TermDataInPath {
    pub(crate) not_lower_terms: TermMap,
    pub(crate) lower_terms: TermMap,
    /// does not store texts longer than this in the fst in bytes
    pub(crate) do_not_store_text_longer_than: usize,
    pub(crate) id_counter_for_large_texts: u32,
}

use super::NUM_TERM_LIMIT_ERR;

impl TermDataInPath {

    /// Text may be a large size text, which is not stored and has not an id associated. In this case a id will be generated
    pub fn get_text_info(&mut self, text: &str) -> TermInfo {
        if self.do_not_store_text_longer_than < text.len() {
            // *self.long_terms.get(text).expect("did not found term")
            self.id_counter_for_large_texts = self.id_counter_for_large_texts.checked_add(1).expect(NUM_TERM_LIMIT_ERR);
            let large_text_id = 
                self
                .not_lower_terms
                .len()
                .to_u32()
                .expect(NUM_TERM_LIMIT_ERR)
                .checked_add(self
                    .lower_terms
                    .len()
                    .to_u32()
                    .expect(NUM_TERM_LIMIT_ERR)
                )
                .expect(NUM_TERM_LIMIT_ERR)
                .checked_add(1)
                .expect(NUM_TERM_LIMIT_ERR)
                .checked_add(self.id_counter_for_large_texts)
                .expect(NUM_TERM_LIMIT_ERR) ;

            TermInfo {
                id: large_text_id,
                num_occurences: 1, // This may be incorrect, but should not have major impact on score calculation
            }
        } else {
            // *self.terms.get(value).expect("did not found term")
            *self.get_text_normal_size_info(text)
        }
    }


    pub fn get_text_normal_size_info<'a>(&'a self, text: &str) -> &'a TermInfo {
        let is_lowercase = text.chars().all(|c|c.is_lowercase());
        let token_info = if is_lowercase {
            self.lower_terms.get(text).expect("did not found text")
        }else {
            self.not_lower_terms.get(text).expect("did not found text")
        };
        token_info
    }


}

#[inline]
fn add_count_text(not_lower_terms: &mut TermMap, lower_terms: &mut TermMap, text: &str) {
    if text.chars().all(|c|c.is_lowercase()) {

        let stat = lower_terms.get_or_insert(text, TermInfo::default);
        stat.num_occurences = stat.num_occurences.saturating_add(1);
    }else {
            
        let stat = lower_terms.get_or_insert(&text.to_lowercase(), TermInfo::default);
        stat.num_occurences = stat.num_occurences.saturating_add(1);

        let stat = not_lower_terms.get_or_insert(text, TermInfo::default);
        stat.num_occurences = stat.num_occurences.saturating_add(1);
    }


}

#[inline]
fn add_text<T: Tokenizer>(text: &str, term_data: &mut TermDataInPath, options: &FulltextIndexOptions, tokenizer: &T) {
    trace!("text: {:?}", text);

    if term_data.do_not_store_text_longer_than < text.len() {
        term_data.id_counter_for_large_texts += 1;
    // add_count_text(&mut term_data.long_terms, text); //TODO handle no tokens case or else the text can't be reconstructed
    } else {
        add_count_text(&mut term_data.not_lower_terms,&mut term_data.lower_terms, text); //TODO handle no tokens case or else the text can't be reconstructed
    }

    if options.tokenize && tokenizer.has_tokens(&text) {
        for (token, _is_seperator) in text.iter_tokens() {
            add_count_text(&mut term_data.not_lower_terms,&mut term_data.lower_terms, token);
        }
        // tokenizer.get_tokens(&text, &mut |token: &str, _is_seperator: bool| {
        //     // debug_assert!(!_is_seperator && text.contains(" "));

        // });
    }
}



/// This will extract terms into Hashmaps and generate ids for them. Text is put in one of 2 containers, lower_case and other.
pub(crate) fn get_and_store_terms<I>(
    stream: I,
    mut persistence: &mut Persistence,
    indices_json: &FieldsConfig,
    term_data: &mut AllTermData,
) -> Result<(), VelociError>
where
    I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>
{
    get_allterms_per_path(stream, &indices_json, term_data)?;

    let default_fulltext_options = FulltextIndexOptions::new_with_tokenize();
    {
        info_time!("set term ids and write fst");
        let reso: Result<FnvHashMap<String, TextIndexMetaData>, io::Error> = term_data
            .terms_in_path
            .par_iter_mut()
            .map(|(path, mut terms_data)| {
                let mut fulltext_index_metadata = TextIndexMetaData::default();
                let options: &FulltextIndexOptions = indices_json.get(&path).fulltext.as_ref().unwrap_or_else(|| &default_fulltext_options);
                let path = path.to_string() + TEXTINDEX;
                fulltext_index_metadata.options = options.clone();
                store_full_text_info_and_set_ids(&persistence, &mut terms_data, &path, &options, &mut fulltext_index_metadata)?;
                Ok((path.to_string(), fulltext_index_metadata))
            })
            .collect();
        persistence.meta_data.fulltext_indices = reso?;
        persistence.load_all_fst()?;

        info!(
            "All text memory {}",
            persistence::get_readable_size(term_data.terms_in_path.iter().map(|el| el.1.not_lower_terms.memory_footprint() + el.1.lower_terms.memory_footprint()).sum())
        );
        info!(
            "All raw text data memory {}",
            persistence::get_readable_size(term_data.terms_in_path.iter().map(|el| el.1.not_lower_terms.total_size_of_text_data() + el.1.lower_terms.total_size_of_text_data()).sum())
        );
    }
    Ok(())
}


fn get_allterms_per_path<I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>>(
    stream: I,
    // persistence: &mut Persistence,
    fulltext_info_for_path: &FieldsConfig,
    data: &mut AllTermData,
) -> Result<(), io::Error> {
    info_time!("get_allterms_per_path");

    let tokenizer = SimpleTokenizerCharsIterateGroupTokens {};
    let default_fulltext_options = FulltextIndexOptions::new_with_tokenize();

    // let mut id_holder = ;
    {
        let mut cb_text = |_anchor_id: u32, value: &str, path: &str, _parent_val_id: u32| -> Result<(), io::Error> {
            let options: &FulltextIndexOptions = fulltext_info_for_path.get(path).fulltext.as_ref().unwrap_or(&default_fulltext_options);

            let mut terms_data = super::get_or_insert_prefer_get(&mut data.terms_in_path as *mut FnvHashMap<_, _>, path, || TermDataInPath {
                do_not_store_text_longer_than: options.do_not_store_text_longer_than,
                ..Default::default()
            });

            add_text(value, &mut terms_data, &options, &tokenizer);
            Ok(())
        };
        let mut callback_ids = |_anchor_id: u32, _path: &str, _value_id: u32, _parent_val_id: u32| -> Result<(), io::Error> { Ok(()) };

        json_converter::for_each_element(stream, &mut json_converter::IDHolder::new(), &mut cb_text, &mut callback_ids)?;
    }

    for map in data.terms_in_path.values_mut() {
        map.not_lower_terms.shrink_to_fit();
    }
    for map in data.terms_in_path.values_mut() {
        map.lower_terms.shrink_to_fit();
    }

    // std::mem::swap(&mut data.id_holder, &mut id_holder);

    Ok(())
}


//Set the global text/token_ids for a field, monotonically increasing. Therefore the offset is added from previous blocks (lower_terms currently)
pub(crate) fn set_term_ids(all_terms: &mut TermMap, offset: u32) -> Vec<(&str, &TermInfo)> {
    let mut term_and_mut_val: Vec<(&str, &mut TermInfo)> = all_terms.iter_mut().collect();
    // let mut term_and_mut_val: Vec<(&String, &mut TermInfo)> = all_terms.iter_mut().collect();
    term_and_mut_val.sort_unstable_by_key(|el| el.0);

    for (i, term_and_info) in term_and_mut_val.iter_mut().enumerate() {
        // term_and_info.1.id = i.to_u32().expect(super::NUM_TERM_LIMIT_ERR).checked_add(offset).expect(super::NUM_TERM_LIMIT_ERR);
        term_and_info.1.id = i.to_u32().expect(super::NUM_TERM_LIMIT_ERR).checked_add(offset).expect(super::NUM_TERM_LIMIT_ERR);
    }

    // term_and_mut_val

    let mut dat:Vec<_> = all_terms.iter().collect();
    dat.sort_unstable_by_key(|el| el.0);
    dat
}


// We we will search in the lower_case fst and extrapolate non lower_case search_results from it. For this we will need to associate the lower_case id to the others 
pub(crate) fn associate_lower_with_other(lower_terms: &TermMap, sorted_lower_terms_and_val: &Vec<(&str, &TermInfo)>, temp_dir: String) -> Result<BufferedIndexWriter, io::Error> {
    let mut buf = BufferedIndexWriter::new_for_sorted_id_insertion(temp_dir.to_string());
    // let mut assoc: Vec<(u32, u32)> = vec![];
    for (term, term_info) in sorted_lower_terms_and_val.iter() {
        let info = lower_terms.get(&term.to_lowercase()).unwrap_or_else(|| panic!("could not find lower_case version of string in hashmap {:?}", term));
        // assoc.push((term_info.id, info.id));
        buf.add(term_info.id, info.id)?;
    }
    // assoc
    Ok(buf)
}



fn store_full_text_info_and_set_ids(
    persistence: &Persistence,
    terms_data: &mut TermDataInPath,
    path: &str,
    options: &FulltextIndexOptions,
    fulltext_indices: &mut TextIndexMetaData,
) -> Result<(), io::Error> {
    debug_time!("store_fst strings and string offsets {:?}", path);

    if log_enabled!(log::Level::Trace) {
        let mut all_text: Vec<_> = terms_data.lower_terms.keys().collect();
        all_text.sort_unstable();
        trace!("{:?} LowerTerms: {:?}", path, all_text);
        let mut all_text: Vec<_> = terms_data.not_lower_terms.keys().collect();
        all_text.sort_unstable();
        trace!("{:?} OtherTerms: {:?}", path, all_text);
    }


    fulltext_indices.num_text_ids = terms_data.lower_terms.len() + terms_data.not_lower_terms.len();

    let term_and_info = set_term_ids(&mut terms_data.not_lower_terms, terms_data.lower_terms.len() as u32);
    store_fst(persistence, &term_and_info, &path.add(".other"), options.do_not_store_text_longer_than, terms_data.lower_terms.len() as u32).expect("Could not store fst other");

    let term_and_info = set_term_ids(&mut terms_data.lower_terms, 0);
    store_fst(persistence, &term_and_info, &path.add(".lower"), options.do_not_store_text_longer_than, 0).expect("Could not store fst lower");

    let associate_lower_with_other_buf = associate_lower_with_other(&terms_data.not_lower_terms, &term_and_info, persistence.temp_dir())?;

    Ok(())
}

//Setting the fst local term_ids. The offset is subtracted to be able to find the text later by its ordinal value in the fst
pub(crate) fn store_fst(persistence: &Persistence, sorted_terms: &[(&str, &TermInfo)], path: &str, ignore_text_longer_than: usize, offset:u32) -> Result<(), fst::Error> {
    debug_time!("store_fst {:?}", path);
    let wtr = persistence.get_buffered_writer(&path.add(".fst"))?;
    // Create a builder that can be used to insert new key-value pairs.
    let mut build = MapBuilder::new(wtr)?;
    for (term, info) in sorted_terms.iter() {
        if term.len() <= ignore_text_longer_than {
            build.insert(term, u64::from(info.id - offset)).expect("could not insert into fst");
        }
    }

    build.finish()?;

    Ok(())
}


#[test]
fn test_extrac_text_into_fest() {
    let tokenizer = SimpleTokenizerCharsIterateGroupTokens {};

    let mut terms_data = TermDataInPath {
        do_not_store_text_longer_than: 100,
        ..Default::default()
    };

    let fulltext_options = FulltextIndexOptions::new_with_tokenize();

    add_text("test", &mut terms_data, &fulltext_options, &tokenizer);
    add_text("zest", &mut terms_data, &fulltext_options, &tokenizer);
    add_text("Test", &mut terms_data, &fulltext_options, &tokenizer);
    add_text("Fest", &mut terms_data, &fulltext_options, &tokenizer);

    // let pers = Persistence::create("../../test_files".to_string()).unwrap();
    let pers = Persistence::create("test_files".to_string()).unwrap();

    let mut fulltext_index_metadata = TextIndexMetaData::default();
    store_full_text_info_and_set_ids(&pers, &mut terms_data, "test_field", &fulltext_options, &mut fulltext_index_metadata).unwrap();
    // pers.write_meta_data();

    // let pers = Persistence::load("test_files").expect("Could not load persistence");
    // let map = persistence
    //     .indices
    //     .fst
    //     .get("test_files/test_field.lower.fst")
    //     .ok_or_else(|| VelociError::FstNotFound(options.path.to_string()))?;

    // let pers = Persistence::create("test_files".to_string()).unwrap();

    let fst_lower = unsafe{Map::from_path("test_files/test_field.lower.fst").unwrap()};
    let fst_other = unsafe{Map::from_path("test_files/test_field.other.fst").unwrap()};

    assert_eq!(search_field::ord_to_term_to_string(fst_lower.as_fst(), 0), "fest");
    assert_eq!(search_field::ord_to_term_to_string(fst_lower.as_fst(), 1), "test");
    assert_eq!(search_field::ord_to_term_to_string(fst_lower.as_fst(), 2), "zest");
    assert_eq!(search_field::ord_to_term_to_string(fst_other.as_fst(), 0), "Fest");
    assert_eq!(search_field::ord_to_term_to_string(fst_other.as_fst(), 1), "Test");
}