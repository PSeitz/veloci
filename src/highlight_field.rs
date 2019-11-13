use crate::{
    error::VelociError,
    persistence::{self, Persistence, *},
    search::*,
    tokenizer::*,
    util::StringAdd,
};
use fnv::FnvHashMap;
use std::{self, cmp, str, sync::Arc};

use fnv::FnvHashSet;
#[allow(unused_imports)]
use fst::{IntoStreamer, Map, MapBuilder, Set};
use itertools::Itertools;

//TODO merge with grouped_to_positions_for_snippet
pub fn group_hit_positions_for_snippet(hit_pos_of_tokens_in_doc: &[usize], opt: &SnippetInfo) -> Vec<Vec<i64>> {
    let token_around_snippets = opt.num_words_around_snippet * 2; // double token_around_snippets, because seperator is every 2. token. token seperator token seperator

    //group near tokens
    let mut grouped: Vec<Vec<i64>> = vec![];
    {
        trace_time!("group near tokens");
        let mut previous_token_pos = -token_around_snippets;
        for token_pos in hit_pos_of_tokens_in_doc {
            if *token_pos as i64 - previous_token_pos >= token_around_snippets {
                grouped.push(vec![]);
            }
            previous_token_pos = *token_pos as i64;
            grouped.last_mut().unwrap().push(*token_pos as i64);
        }
    }

    grouped
}

pub fn grouped_to_positions_for_snippet(vec: &[i64], token_len: usize, token_around_snippets: i64) -> (usize, usize) {
    let start_index = cmp::max(*vec.first().unwrap() as i64 - token_around_snippets, 0) as usize;
    let end_index = cmp::min((*vec.last().unwrap() + token_around_snippets + 1) as usize, token_len);
    (start_index, end_index)
}

pub fn build_snippet<'a, 'b, F1, F2, I: Iterator<Item = (usize, usize)>>(windows: I, is_hit: &mut F1, get_text: &mut F2, opt: &SnippetInfo) -> String
where
    F1: FnMut(usize) -> bool,
    F2: Fn(usize) -> &'b str,
{
    windows
        .map(|group| {
            let mut snippet = String::with_capacity((group.1 - group.0) * 10);
            for i in group.0..group.1 {
                if is_hit(i) {
                    snippet += &opt.snippet_start_tag;
                    snippet += get_text(i);
                    snippet += &opt.snippet_end_tag; // TODO store token and add
                } else {
                    snippet += get_text(i);
                }
            }
            snippet
        })
        .take(opt.max_snippets as usize)
        .intersperse(opt.snippet_connector.to_string())
        .fold(String::with_capacity(10 as usize), |snippet, snippet_part| snippet + &snippet_part)
}

/// Adds ... at the beginning and end.
pub fn ellipsis_snippet(snippet: &mut String, hit_pos_of_tokens_in_doc: &[usize], token_len: usize, opt: &SnippetInfo) {
    let token_around_snippets = opt.num_words_around_snippet * 2; // token seperator token seperator
    if !hit_pos_of_tokens_in_doc.is_empty() {
        let first_index = *hit_pos_of_tokens_in_doc.first().unwrap() as i64;
        let last_index = *hit_pos_of_tokens_in_doc.last().unwrap() as i64;
        if first_index > token_around_snippets {
            // add ... add the beginning
            snippet.insert_str(0, &opt.snippet_connector);
        }

        if last_index < token_len as i64 - token_around_snippets {
            // add ... add the end
            snippet.push_str(&opt.snippet_connector);
        }
    }
}

/// Highlights text
/// * `text` - The text to hightlight.
/// * `set` - The tokens to hightlight in the text. They need to be properly tokenized for that field
pub fn highlight_text(text: &str, set: &FnvHashSet<String>, opt: &SnippetInfo, tokenizer: &Arc<dyn Tokenizer>) -> Option<String> {
    let mut contains_any_token = false;

    // hit complete text
    if set.contains(text) {
        return Some(opt.snippet_start_tag.to_string() + text + &opt.snippet_end_tag);
    }

    let mut tokens = vec![];
    let mut hit_pos_of_tokens_in_doc = vec![];
    for (pos, (token, _)) in tokenizer.iter(text).enumerate() {
        tokens.push(token);
        if set.contains(token) {
            hit_pos_of_tokens_in_doc.push(pos);
        }
    }

    let token_around_snippets = opt.num_words_around_snippet * 2; // token seperator token seperator

    // //group near tokens
    let grouped = group_hit_positions_for_snippet(&hit_pos_of_tokens_in_doc, opt);

    let get_document_windows = &(|vec: &Vec<i64>| grouped_to_positions_for_snippet(vec, tokens.len(), token_around_snippets));

    let window_iter = grouped.iter().map(get_document_windows);
    let mut snippet = build_snippet(
        window_iter,
        &mut |pos: usize| {
            if set.contains(tokens[pos]) {
                contains_any_token = true;
                true
            } else {
                false
            }
        },
        &mut |pos: usize| &tokens[pos],
        &opt,
    );

    ellipsis_snippet(&mut snippet, &hit_pos_of_tokens_in_doc, tokens.len(), &opt);

    if contains_any_token {
        Some(snippet)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_test_tokenizer() -> Arc<dyn Tokenizer> {
        Arc::new(SimpleTokenizerCharsIterateGroupTokens::default())
    }

    #[test]
    fn test_highlight_text() {
        assert_eq!(
            highlight_text(
                "mein treffer",
                &vec!["treffer"].iter().map(|el| el.to_string()).collect(),
                &DEFAULT_SNIPPETINFO,
                &get_test_tokenizer()
            )
            .unwrap(),
            "mein <b>treffer</b>"
        );
        assert_eq!(
            highlight_text(
                "mein treffer treffers",
                &vec!["treffers", "treffer"].iter().map(|el| el.to_string()).collect(),
                &DEFAULT_SNIPPETINFO,
                &get_test_tokenizer()
            )
            .unwrap(),
            "mein <b>treffer</b> <b>treffers</b>"
        );
        assert_eq!(
            highlight_text(
                "Schön-Hans",
                &vec!["Hans"].iter().map(|el| el.to_string()).collect(),
                &DEFAULT_SNIPPETINFO,
                &get_test_tokenizer()
            )
            .unwrap(),
            "Schön-<b>Hans</b>"
        );
        assert_eq!(
            highlight_text(
                "Schön-Hans",
                &vec!["Haus"].iter().map(|el| el.to_string()).collect(),
                &DEFAULT_SNIPPETINFO,
                &get_test_tokenizer()
            ),
            None
        );
    }
}

pub(crate) fn highlight_on_original_document(persistence: &Persistence, doc: &str, why_found_terms: &FnvHashMap<String, FnvHashSet<String>>) -> FnvHashMap<String, Vec<String>> {
    let mut highlighted_texts: FnvHashMap<_, Vec<_>> = FnvHashMap::default();
    let stream = serde_json::Deserializer::from_str(&doc).into_iter::<serde_json::Value>();

    let mut id_holder = json_converter::IDHolder::new();
    {
        let mut cb_text = |_anchor_id: u32, value: &str, field_name: &str, _parent_val_id: u32| -> Result<(), serde_json::error::Error> {
            let path_text = field_name.add(TEXTINDEX);
            if let Some(terms) = why_found_terms.get(&path_text) {
                if let Some(highlighted) = highlight_text(
                    value,
                    &terms,
                    &DEFAULT_SNIPPETINFO,
                    &persistence
                        .metadata
                        .columns
                        .get(field_name)
                        .unwrap_or_else(|| panic!("could not find metadata for {:?}", field_name))
                        .textindex_metadata
                        .options
                        .tokenizer
                        .as_ref()
                        .unwrap(),
                ) {
                    let jepp = highlighted_texts.entry(field_name.to_string()).or_default();
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
        } else if token_ids.contains(&(value_id as u32)) {
            // highlight whole text
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

    let token_around_snippets = opt.num_words_around_snippet * 2; // token seperator token seperator

    //group near tokens
    let grouped = group_hit_positions_for_snippet(&hit_pos_of_tokens_in_doc, opt);

    let get_document_windows = &(|vec: &Vec<i64>| grouped_to_positions_for_snippet(vec, documents_token_ids.len(), token_around_snippets));

    //get all required tokenids and their text
    // let mut all_tokens = grouped.iter().map(get_document_windows).flat_map(|el| (el.0..el.1).map(|pos|documents_token_ids[pos]).collect_vec()).cloned().collect_vec();
    let mut all_tokens = grouped.iter().map(get_document_windows).fold(Vec::with_capacity(10), |mut vecco, el| {
        vecco.extend((el.0..el.1).map(|pos| documents_token_ids[pos]));
        vecco
    });
    // let mut all_tokens = grouped.iter().map(get_document_windows).flat_map(|el| el.2).cloned().collect_vec();
    all_tokens.sort();
    all_tokens = all_tokens.into_iter().dedup().collect_vec();
    let id_to_text = get_id_text_map_for_ids(persistence, path, all_tokens.as_slice());

    // let estimated_snippet_size = std::cmp::min(u64::from(opt.max_snippets) * 100, documents_token_ids.len() as u64 * 10);

    trace_time!("create snippet string");

    let window_iter = grouped.iter().map(get_document_windows);
    let mut snippet = build_snippet(
        window_iter,
        &mut |pos: usize| token_ids.contains(&documents_token_ids[pos]),
        &mut |pos: usize| &id_to_text[&documents_token_ids[pos]],
        &opt,
    );

    ellipsis_snippet(&mut snippet, &hit_pos_of_tokens_in_doc, documents_token_ids.len(), &opt);

    Ok(Some(snippet))
}
