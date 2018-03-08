use str;
use std;
use persistence::Persistence;
use persistence::*;
#[allow(unused_imports)]
use persistence;
use search;
use search::*;
use util::concat;
use std::cmp;
// use hit_collector::HitCollector;

#[allow(unused_imports)]
use fst::{IntoStreamer, Map, MapBuilder, Set};
#[allow(unused_imports)]
use fst_levenshtein::Levenshtein;
use search_field::*;
// use search::Hit;

use heapsize::HeapSizeOf;
use itertools::Itertools;

use fnv::FnvHashSet;

#[cfg_attr(feature = "flame_it", flame)]
pub fn highlight_document(
    persistence: &Persistence,
    path: &str,
    value_id: u64,
    token_ids: &[u32],
    opt: &SnippetInfo,
) -> Result<Option<String>, search::SearchError> {
    let value_id_to_token_ids = persistence.get_valueid_to_parent(&concat(path, ".value_id_to_token_ids"))?;
    debug_time!(format!("highlight_document id {}", value_id));

    let documents_token_ids: Vec<u32> = {
        debug_time!("get documents_token_ids");
        persistence::trace_index_id_to_parent(value_id_to_token_ids);

        let vals = value_id_to_token_ids.get_values(value_id);
        if let Some(vals) = vals {
            vals
        } else {
            //got a text id, check if it was hit
            if token_ids.contains(&(value_id as u32)) {
                return Ok(Some(
                    opt.snippet_start_tag.to_string() + &get_text_for_id(persistence, path, value_id as u32) + &opt.snippet_end_tag,
                ));
            } else {
                return Ok(None); //No hits
            }
        }
    };
    trace!("documents_token_ids {}", get_readable_size(documents_token_ids.heap_size_of_children()));
    trace!("documents_token_ids {}", get_readable_size(documents_token_ids.len() * 4));

    let token_ids: FnvHashSet<u32> = token_ids.iter().cloned().collect(); // FIXME: Performance

    let to = std::cmp::min(documents_token_ids.len(), 100);
    trace!("documents_token_ids {:?}", &documents_token_ids[0..to]);

    let mut token_positions_in_document = vec![];
    {
        trace_time!("collect token_positions_in_document");
        //collect token_positions_in_document
        for token_id in &token_ids {
            let mut last_pos = 0;
            let mut iter = documents_token_ids.iter();
            while let Some(pos) = iter.position(|x| *x == *token_id) {
                // FIXME: Maybe Performance just walk once over data
                last_pos += pos;
                token_positions_in_document.push(last_pos);
                last_pos += 1;
            }
        }

        // for (i, id) in documents_token_ids.iter().enumerate() {
        //     if token_ids.contains(id){
        //         token_positions_in_document.push(i);
        //     }
        // }
    }
    if token_positions_in_document.is_empty() {
        return Ok(None); //No hits
    }
    token_positions_in_document.sort();

    println!("documents_token_ids {:?}", documents_token_ids.len());
    println!("token_positions_in_document {:?}", token_positions_in_document.len());

    let num_tokens = opt.num_words_around_snippet * 2; // token seperator token seperator

    //group near tokens
    let mut grouped: Vec<Vec<i64>> = vec![];
    {
        trace_time!("group near tokens");
        let mut previous_token_pos = -num_tokens;
        for token_pos in token_positions_in_document.iter() {
            if *token_pos as i64 - previous_token_pos >= num_tokens {
                grouped.push(vec![]);
            }
            previous_token_pos = *token_pos as i64;
            grouped.last_mut().unwrap().push(*token_pos as i64);
        }
    }

    let get_document_windows = &(|vec: &Vec<i64>| {
        let start_index = cmp::max(*vec.first().unwrap() as i64 - num_tokens, 0);
        let end_index = cmp::min(*vec.last().unwrap() as i64 + num_tokens + 1, documents_token_ids.len() as i64);
        (start_index, end_index, &documents_token_ids[start_index as usize..end_index as usize])
    });

    //get all required tokenids and their text
    let mut all_tokens = grouped.iter().map(get_document_windows).flat_map(|el| el.2).cloned().collect_vec();
    all_tokens.sort();
    all_tokens = all_tokens.into_iter().dedup().collect_vec();
    let id_to_text = get_id_text_map_for_ids(persistence, path, all_tokens.as_slice());

    let estimated_snippet_size = std::cmp::min(opt.max_snippets as u64 * 100, documents_token_ids.len() as u64 * 10);

    trace_time!("create snippet string");
    let mut snippet = grouped
        .iter()
        .map(get_document_windows)
        .map(|group| {
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
        .fold(String::with_capacity(estimated_snippet_size as usize), |snippet, snippet_part| {
            snippet + &snippet_part
        });

    if !token_positions_in_document.is_empty() {
        let first_index = *token_positions_in_document.first().unwrap() as i64;
        let last_index = *token_positions_in_document.last().unwrap() as i64;
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
