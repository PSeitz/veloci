use crate::search::search_field::TermId;
use fnv::FnvHashSet;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum FilterResult {
    Vec(Vec<TermId>),
    Set(FnvHashSet<TermId>),
}

impl FilterResult {
    pub fn from_result(res: &[TermId]) -> FilterResult {
        if res.len() > 100_000 {
            FilterResult::Vec(res.to_vec())
        } else {
            let mut filter = FnvHashSet::with_capacity_and_hasher(100_000, Default::default());
            for id in res {
                filter.insert(*id);
            }
            FilterResult::Set(filter)
        }
    }
}
