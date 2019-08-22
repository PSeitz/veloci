use fnv::FnvHashMap;
use crate::util::extract_field_name;
use crate::{
    error::VelociError,
    persistence::{self, Persistence, *},
    search::*,
    tokenizer::*,
    util::StringAdd,
};
use std::{self, cmp, str};

#[allow(unused_imports)]
use fst::{IntoStreamer, Map, MapBuilder, Set};

// use heapsize::HeapSizeOf;
use itertools::Itertools;

use fnv::FnvHashSet;

pub fn group_hit_positions_for_snippet(hit_pos_of_tokens_in_doc: &[usize], opt: &SnippetInfo) -> Vec<Vec<i64>> {

    let num_tokens = opt.num_words_around_snippet * 2; // token seperator token seperator

    //group near tokens
    let mut grouped: Vec<Vec<i64>> = vec![];
    {
        trace_time!("group near tokens");
        let mut previous_token_pos = -num_tokens;
        for token_pos in hit_pos_of_tokens_in_doc {
            if *token_pos as i64 - previous_token_pos >= num_tokens {
                grouped.push(vec![]);
            }
            previous_token_pos = *token_pos as i64;
            grouped.last_mut().unwrap().push(*token_pos as i64);
        }
    }

    grouped

}


pub fn grouped_to_positions_for_snippet<'a, T>(vec: &Vec<i64>, tokens: &'a [T], num_tokens: i64) -> (i64, i64, &'a [T]) {

    let start_index = cmp::max(*vec.first().unwrap() as i64 - num_tokens, 0);
    let end_index = cmp::min(*vec.last().unwrap() as i64 + num_tokens + 1, tokens.len() as i64);
    (start_index, end_index, &tokens[start_index as usize..end_index as usize])

}


pub fn build_snippet<'a, T: 'static, I: Iterator<Item = (i64, i64, &'a[T])>>(windows: I, opt: &SnippetInfo) -> String {

    windows
    .map(|group| {
        group.2.iter().fold(String::with_capacity(group.2.len() * 10), |snippet_part_acc, token_id| {
            // if token_ids.contains(token_id) {
            //     snippet_part_acc + &opt.snippet_start_tag + &id_to_text[token_id] + &opt.snippet_end_tag // TODO store token and add
            // } else {
            //     snippet_part_acc + &id_to_text[token_id]
            // }

            snippet_part_acc + "&id_to_text[token_id]"
        })
    })
    .take(opt.max_snippets as usize)
    .intersperse(opt.snippet_connector.to_string())
    .fold(String::with_capacity(10 as usize), |snippet, snippet_part| snippet + &snippet_part)

}


/// Highlights text
/// * `text` - The text to hightlight.
/// * `set` - The tokens to hightlight in the text.
pub fn highlight_text(text: &str, set: &FnvHashSet<String>, opt: &SnippetInfo) -> Option<String> {
    let mut contains_any_token = false;
    let mut highlighted = String::with_capacity(text.len() + 10);

    // let mut tokens = vec![];
    // let mut hit_pos_of_tokens_in_doc = vec![];
    // let tokenizer = SimpleTokenizerCharsIterateGroupTokens {};
    // let mut pos = 0;
    // tokenizer.get_tokens(text, &mut |token: &str, _is_seperator: bool| {
    //     tokens.push(token);
    //     hit_pos_of_tokens_in_doc.push(pos);
    //     pos+=1;
    // });

    // let num_tokens = opt.num_words_around_snippet * 2; // token seperator token seperator

    // //group near tokens
    // let grouped = group_hit_positions_for_snippet(&hit_pos_of_tokens_in_doc, opt);

    // let get_document_windows = &(|vec: &Vec<i64>| {
    //     grouped_to_positions_for_snippet(vec, &tokens, num_tokens)
    // });
    // //get all required tokenids and their text
    // let mut all_tokens = grouped.iter().map(get_document_windows).flat_map(|el| el.2).cloned().collect_vec();
    // all_tokens.sort();
    // all_tokens = all_tokens.into_iter().dedup().collect_vec();

    // opt.num_words_around_snippet;
    let tokenizer = SimpleTokenizerCharsIterateGroupTokens {};
    tokenizer.get_tokens(text, &mut |token: &str, _is_seperator: bool| {
        if set.contains(token) {
            contains_any_token = true;
            highlighted.push_str(&opt.snippet_start_tag);
            highlighted.push_str(token);
            highlighted.push_str(&opt.snippet_end_tag);
        } else {
            highlighted.push_str(token);
        }
    });

    if contains_any_token {
        Some(highlighted)
    } else {
        None
    }
}

#[test]
fn test_highlight_text() {
    assert_eq!(highlight_text("mein treffer", &vec!["treffer"].iter().map(|el|el.to_string()).collect(), &DEFAULT_SNIPPETINFO).unwrap(), "mein <b>treffer</b>");
    assert_eq!(highlight_text("mein treffer treffers", &vec!["treffers", "treffer"].iter().map(|el|el.to_string()).collect(), &DEFAULT_SNIPPETINFO).unwrap(), "mein <b>treffer</b> <b>treffers</b>");
    assert_eq!(highlight_text("Schön-Hans", &vec!["Hans"].iter().map(|el|el.to_string()).collect(), &DEFAULT_SNIPPETINFO).unwrap(), "Schön-<b>Hans</b>");
    assert_eq!(highlight_text("Schön-Hans", &vec!["Haus"].iter().map(|el|el.to_string()).collect(), &DEFAULT_SNIPPETINFO), None);
}

pub(crate) fn highlight_on_original_document(doc: &str, why_found_terms: &FnvHashMap<String, FnvHashSet<String>>) -> FnvHashMap<String, Vec<String>> {
    let mut highlighted_texts: FnvHashMap<_, Vec<_>> = FnvHashMap::default();
    let stream = serde_json::Deserializer::from_str(&doc).into_iter::<serde_json::Value>();

    let mut id_holder = json_converter::IDHolder::new();
    {
        let mut cb_text = |_anchor_id: u32, value: &str, path: &str, _parent_val_id: u32| -> Result<(), serde_json::error::Error> {
            let path = path.add(TEXTINDEX);
            if let Some(terms) = why_found_terms.get(&path) {
                if let Some(highlighted) = highlight_text(value, &terms, &DEFAULT_SNIPPETINFO) {
                    let field_name = extract_field_name(&path); // extract_field_name removes .textindex
                    let jepp = highlighted_texts.entry(field_name).or_default();
                    jepp.push(highlighted);
                }
            }
            Ok(())
        };

        let mut callback_ids = |_anchor_id: u32, _path: &str, _value_id: u32, _parent_val_id: u32| -> Result<(), serde_json::error::Error> { Ok(()) };

        json_converter::for_each_element(stream, &mut id_holder, &mut cb_text, &mut callback_ids).unwrap(); // unwrap is ok here
    }
    highlighted_texts
}

pub fn highlight_document(persistence: &Persistence, path: &str, value_id: u64, token_ids: &[u32], opt: &SnippetInfo) -> Result<Option<String>, VelociError> {
    let text_id_to_token_ids = persistence.get_valueid_to_parent(path.add(TEXT_ID_TO_TOKEN_IDS))?;
    trace_time!("highlight_document id {}", value_id);

    // get document as list of token ids or return early
    let documents_token_ids: Vec<u32> = {
        trace_time!("get documents_token_ids");
        persistence::trace_index_id_to_parent(text_id_to_token_ids);

        let vals = text_id_to_token_ids.get_values(value_id);
        if let Some(vals) = vals {
            vals
        } else if token_ids.contains(&(value_id as u32)) { // highlight whole text
            return Ok(Some(
                opt.snippet_start_tag.to_string() + &get_text_for_id(persistence, path, value_id as u32) + &opt.snippet_end_tag,
            ));
        } else {
            return Ok(None); //No hits
        }
    };
    // trace!("documents_token_ids {}", get_readable_size(documents_token_ids.heap_size_of_children()));
    trace!("documents_token_ids {}", get_readable_size(documents_token_ids.len() * 4));

    let token_ids: FnvHashSet<u32> = token_ids.iter().cloned().collect(); // TOOD: Performance

    let to = std::cmp::min(documents_token_ids.len(), 100);
    trace!("documents_token_ids {:?}", &documents_token_ids[0..to]);

    let mut hit_pos_of_tokens_in_doc = vec![];
    {
        trace_time!("collect hit_pos_of_tokens_in_doc");
        //collect hit_pos_of_tokens_in_doc
        for token_id in &token_ids {
            let mut last_pos = 0;
            let mut iter = documents_token_ids.iter();
            while let Some(pos) = iter.position(|x| *x == *token_id) {
                // TODO: Maybe Performance just walk once over data
                last_pos += pos;
                hit_pos_of_tokens_in_doc.push(last_pos);
                last_pos += 1;
            }
        }
    }
    if hit_pos_of_tokens_in_doc.is_empty() {
        return Ok(None); //No hits
    }
    hit_pos_of_tokens_in_doc.sort();

    let num_tokens = opt.num_words_around_snippet * 2; // token seperator token seperator

    //group near tokens
    let grouped = group_hit_positions_for_snippet(&hit_pos_of_tokens_in_doc, opt);

    let get_document_windows = &(|vec: &Vec<i64>| {
        grouped_to_positions_for_snippet(vec, &documents_token_ids, num_tokens)
        // let start_index = cmp::max(*vec.first().unwrap() as i64 - num_tokens, 0);
        // let end_index = cmp::min(*vec.last().unwrap() as i64 + num_tokens + 1, documents_token_ids.len() as i64);
        // (start_index, end_index, &documents_token_ids[start_index as usize..end_index as usize])
    });

    //get all required tokenids and their text
    let mut all_tokens = grouped.iter().map(get_document_windows).flat_map(|el| el.2).cloned().collect_vec();
    all_tokens.sort();
    all_tokens = all_tokens.into_iter().dedup().collect_vec();
    let id_to_text = get_id_text_map_for_ids(persistence, path, all_tokens.as_slice());

    let estimated_snippet_size = std::cmp::min(u64::from(opt.max_snippets) * 100, documents_token_ids.len() as u64 * 10);

    trace_time!("create snippet string");
    let mut snippet = grouped
        .iter()
        .map(get_document_windows)
        .map(|group:(i64, i64, &[u32])| {
            group.2.iter().fold(String::with_capacity(group.2.len() * 10), |snippet_part_acc, token_id| {
                if token_ids.contains(token_id) {
                    snippet_part_acc + &opt.snippet_start_tag + &id_to_text[token_id] + &opt.snippet_end_tag // TODO store token and add
                } else {
                    snippet_part_acc + &id_to_text[token_id]
                }
            })
        })
        .take(opt.max_snippets as usize)
        .intersperse(opt.snippet_connector.to_string())
        .fold(String::with_capacity(estimated_snippet_size as usize), |snippet, snippet_part| snippet + &snippet_part);

    if !hit_pos_of_tokens_in_doc.is_empty() {
        let first_index = *hit_pos_of_tokens_in_doc.first().unwrap() as i64;
        let last_index = *hit_pos_of_tokens_in_doc.last().unwrap() as i64;
        if first_index > num_tokens {
            // add ... add the beginning
            snippet.insert_str(0, &opt.snippet_connector);
        }

        if last_index < documents_token_ids.len() as i64 - num_tokens {
            // add ... add the end
            snippet.push_str(&opt.snippet_connector);
        }
    }

    Ok(Some(snippet))
}
