use crate::{
    error::VelociError,
    facet, highlight_field,
    persistence::Persistence,
    search::{request::snippet_info::DEFAULT_SNIPPETINFO, search_field::TermId},
    util::{self, extract_field_name},
};
use fnv::FnvHashMap;

/// This methods loads the tokens of a text from the fst and higlights them
pub fn get_why_found(
    persistence: &Persistence,
    anchor_ids: &[u32],
    term_id_hits_in_field: &FnvHashMap<String, FnvHashMap<String, Vec<TermId>>>,
) -> Result<FnvHashMap<u32, FnvHashMap<String, Vec<String>>>, VelociError> {
    debug!("why_found info {:?}", term_id_hits_in_field);
    info_time!("why_found");
    let mut anchor_highlights: FnvHashMap<_, FnvHashMap<_, Vec<_>>> = FnvHashMap::default();

    for (path, term_with_ids) in term_id_hits_in_field.iter() {
        let field_name = &extract_field_name(path); // extract_field_name removes .textindex
        let paths = util::get_steps_to_anchor(field_name);

        let all_term_ids_hits_in_path = term_with_ids.iter().fold(vec![], |mut acc, (_term, hits)| {
            acc.extend(hits.iter());
            acc
        });

        if all_term_ids_hits_in_path.is_empty() {
            continue;
        }

        for anchor_id in anchor_ids {
            let ids = facet::join_anchor_to_leaf(persistence, &[*anchor_id], &paths)?;

            for value_id in ids {
                let path = paths.last().unwrap().to_string();
                let highlighted_document = highlight_field::highlight_document(persistence, &path, u64::from(value_id), &all_term_ids_hits_in_path, &DEFAULT_SNIPPETINFO).unwrap();
                if let Some(highlighted_document) = highlighted_document {
                    let jepp = anchor_highlights.entry(*anchor_id).or_default();
                    let field_highlights = jepp.entry(field_name.clone()).or_default();
                    field_highlights.push(highlighted_document);
                }
            }
        }
    }

    Ok(anchor_highlights)
}
