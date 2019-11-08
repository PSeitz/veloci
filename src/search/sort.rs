use crate::search::{sort_by_score_and_id, Hit};
use core::cmp::Ordering;

#[inline]
pub(crate) fn top_n_sort(data: Vec<Hit>, top_n: u32) -> Vec<Hit> {
    let mut worst_score = std::f32::MIN;

    let mut new_data: Vec<Hit> = Vec::with_capacity(top_n as usize * 5 + 1);
    for el in data {
        if el.score < worst_score {
            continue;
        }

        check_apply_top_n_sort(&mut new_data, top_n, &sort_by_score_and_id, &mut |the_worst: &Hit| worst_score = the_worst.score);

        new_data.push(el);
    }

    // Sort by score and anchor_id -- WITHOUT anchor_id SORTING SKIP MAY WORK NOT CORRECTLY FOR SAME SCORED ANCHOR_IDS
    new_data.sort_unstable_by(sort_by_score_and_id);
    new_data
}

#[inline]
pub(crate) fn check_apply_top_n_sort<T: std::fmt::Debug>(new_data: &mut Vec<T>, top_n: u32, sort_compare: &dyn Fn(&T, &T) -> Ordering, new_worst: &mut dyn FnMut(&T)) {
    if !new_data.is_empty() && new_data.len() as u32 == top_n + 200 {
        new_data.sort_unstable_by(sort_compare);
        new_data.truncate(top_n as usize);
        let new_worst_value = new_data.last().unwrap();
        trace!("new worst {:?}", new_worst_value);
        new_worst(new_worst_value);
        // worst_score = new_data.last().unwrap().score;
    }
}
