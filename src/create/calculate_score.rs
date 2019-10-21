use crate::create::ValIdPairToken;
use buffered_index_writer::BufferedIndexWriter;
use itertools::Itertools;
use std::io::self;

pub(crate) fn calculate_and_add_token_score_in_doc(
    tokens_to_anchor_id: &mut Vec<ValIdPairToken>,
    anchor_id: u32,
    num_tokens_in_text: u32,
    index: &mut BufferedIndexWriter<u32, (u32, u32)>,
) -> Result<(), io::Error> {
    // Sort by tokenid, token_pos
    tokens_to_anchor_id.sort_unstable_by(|a, b| {
        let sort_valid = a.token_or_text_id.cmp(&b.token_or_text_id);
        if sort_valid == std::cmp::Ordering::Equal {
            a.token_pos.cmp(&b.token_pos)
        } else {
            sort_valid
        }
    });

    for (_, mut group) in &tokens_to_anchor_id.iter_mut().group_by(|el| el.token_or_text_id) {
        if let Some(first) = group.next() {
            let best_pos = first.token_pos;
            let num_occurences = first.num_occurences;
            let score = calculate_token_score_for_entry(best_pos, num_occurences, num_tokens_in_text, false);
            index.add(first.token_or_text_id, (anchor_id, score))?;
        }
    }
    Ok(())
}

#[inline]
pub(crate) fn calculate_token_score_for_entry(token_best_pos: u32, num_occurences: u32, num_tokens_in_text: u32, is_exact: bool) -> u32 {
    let mut score = if is_exact { 400. } else { 2000. / ((token_best_pos as f32 + 10.).log2() + 10.) };
    let mut num_occurence_modifier = (num_occurences as f32 + 1000.).log10() - 2.; // log 1000 is 3
    num_occurence_modifier -= (num_occurence_modifier - 1.) * 0.7; //reduce by 70%
    score /= num_occurence_modifier;
    let mut text_length_modifier = ((num_tokens_in_text + 10) as f32).log10();
    text_length_modifier -= (text_length_modifier - 1.) * 0.7; //reduce by 70%
    score /= text_length_modifier;
    let score = score as u32;
    debug_assert_ne!(
        score, 0,
        "token_best_pos:{:?} num_occurences:{:?} num_tokens_in_text:{:?} {:?}",
        token_best_pos, num_occurences, num_tokens_in_text, is_exact
    );
    score
}
