use self::fields_config::FieldsConfig;
use crate::{
    create::write_docs::DocWriteRes,
    metadata::FulltextIndexOptions,
    persistence::{Persistence, *},
    tokenizer::*,
    util::StringAdd,
};
use fnv::FnvHashMap;
use fst::{self, MapBuilder};

use num::ToPrimitive;

use std::{self, io, str, sync::Arc};

use super::*;

#[derive(Debug, Default)]
pub struct AllTermsAndDocumentBuilder {
    offsets: Vec<u64>,
    current_offset: u64,
    pub(crate) id_holder: json_converter::IDHolder,
    pub(crate) terms_in_path: FnvHashMap<String, TermDataInPath>,
}

pub(crate) fn store_full_text_info_and_set_ids(
    persistence: &Persistence,
    terms_data: &mut TermDataInPath,
    path: &str,
    options: &FulltextIndexOptions,
    col_info: &mut FieldInfo,
    doc_write_res: &DocWriteRes,
) -> Result<(), io::Error> {
    debug_time!("store_fst strings and string offsets {:?}", path);

    let id_column = !path.contains("[]") && doc_write_res.num_doc_ids as usize == terms_data.terms.len() && terms_data.terms.iter().all(|(_term, info)| info.num_occurences == 1);
    col_info.is_anchor_identity_column = id_column;

    if log_enabled!(log::Level::Trace) {
        let mut all_text: Vec<_> = terms_data.terms.keys().collect();
        all_text.sort_unstable();
        trace!("{:?} Terms: {:?}", path, all_text);
    }
    col_info.textindex_metadata.num_text_ids = terms_data.terms.len();
    let term_and_mut_val = set_ids(&mut terms_data.terms, 0);
    store_fst(persistence, &term_and_mut_val, path, options.do_not_store_text_longer_than).expect("Could not store fst");

    Ok(())
}

fn store_fst(persistence: &Persistence, sorted_terms: &[(&str, &mut TermInfo)], path: &str, ignore_text_longer_than: usize) -> Result<(), fst::Error> {
    debug_time!("store_fst {:?}", path);
    let wtr = persistence.get_buffered_writer(&path.add(".fst"))?;
    // Create a builder that can be used to insert new key-value pairs.
    let mut build = MapBuilder::new(wtr)?;
    for (term, info) in sorted_terms.iter() {
        if term.len() <= ignore_text_longer_than {
            build.insert(term, u64::from(info.id)).expect("could not insert into fst");
        }
    }

    build.finish()?;

    Ok(())
}

fn set_ids(all_terms: &mut TermMap, offset: u32) -> Vec<(&str, &mut TermInfo)> {
    let mut term_and_mut_val: Vec<(&str, &mut TermInfo)> = all_terms.iter_mut().collect();
    term_and_mut_val.sort_unstable_by_key(|el| el.0);

    for (i, term_and_info) in term_and_mut_val.iter_mut().enumerate() {
        term_and_info.1.id = i.to_u32().expect(NUM_TERM_LIMIT_MSG).checked_add(offset).expect(NUM_TERM_LIMIT_MSG);
    }

    term_and_mut_val
}

//TODO: Detect id columns and store text directly in fst
#[inline]
fn add_count_text(terms: &mut TermMap, text: &str) {
    let stat = terms.get_or_create(text, TermInfo::default());
    stat.num_occurences = stat.num_occurences.saturating_add(1);

    // if let Some(stat) = terms.get_mut(text) {
    //     stat.num_occurences = stat.num_occurences.saturating_add(1);
    // }else{
    //     terms.insert(text.to_string(), TermInfo{
    //         id: 0, // id will be generated later
    //         num_occurences: 1,
    //     });
    // };
}

#[inline]
fn add_text(text: &str, term_data: &mut TermDataInPath, options: &FulltextIndexOptions, tokenizer: &Arc<dyn Tokenizer>) {
    trace!("text: {:?}", text);

    if term_data.do_not_store_text_longer_than < text.len() {
        term_data.id_counter_for_large_texts += 1;
    // add_count_text(&mut term_data.long_terms, text); //TODO handle no tokens case or else the text can't be reconstructed
    } else {
        add_count_text(&mut term_data.terms, text); //TODO handle no tokens case or else the text can't be reconstructed
    }

    if options.tokenize && tokenizer.has_tokens(text) {
        for (token, _is_seperator) in tokenizer.iter(text) {
            add_count_text(&mut term_data.terms, token);
        }
    }
}

pub(crate) fn get_allterms_per_path<I: Iterator<Item = Result<serde_json::Value, serde_json::Error>>>(
    stream: I,
    // persistence: &mut Persistence,
    fulltext_info_for_path: &FieldsConfig,
    data: &mut AllTermsAndDocumentBuilder,
) -> Result<(), io::Error> {
    info_time!("get_allterms_per_path");

    let default_fulltext_options = FulltextIndexOptions::new_with_tokenize();
    let default_tokenizer: Arc<dyn Tokenizer> = Arc::new(SimpleTokenizerCharsIterateGroupTokens::default());

    let mut id_holder = json_converter::IDHolder::new();
    {
        let mut cb_text = |_anchor_id: u32, value: &str, path: &str, _parent_val_id: u32| -> Result<(), io::Error> {
            let options: &FulltextIndexOptions = fulltext_info_for_path.get(path).fulltext.as_ref().unwrap_or(&default_fulltext_options);

            let terms_data = get_or_insert_prefer_get(&mut data.terms_in_path, path, || TermDataInPath {
                do_not_store_text_longer_than: options.do_not_store_text_longer_than,
                ..Default::default()
            });

            add_text(value, terms_data, options, options.tokenizer.as_ref().unwrap_or(&default_tokenizer));
            Ok(())
        };
        let mut callback_ids = |_anchor_id: u32, _path: &str, _value_id: u32, _parent_val_id: u32| -> Result<(), io::Error> { Ok(()) };

        json_converter::for_each_element(stream, &mut id_holder, &mut cb_text, &mut callback_ids)?;
    }

    for map in data.terms_in_path.values_mut() {
        map.terms.shrink_to_fit();
    }

    std::mem::swap(&mut data.id_holder, &mut id_holder);

    Ok(())
}
