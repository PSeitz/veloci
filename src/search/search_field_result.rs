use crate::search;
use crate::search::*;
use crate::search_field::Explain;
use fnv::FnvHashMap;
// use half::f16;
use std::iter::FusedIterator;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SearchFieldResult {
    #[serde(skip_serializing_if = "FnvHashMap::is_empty")]
    pub explain: FnvHashMap<u32, Vec<Explain>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hits_scores: Vec<search::Hit>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hits_ids: Vec<TermId>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub boost_ids: Vec<(TermId, BoostVal)>,
    #[serde(skip_serializing_if = "FnvHashMap::is_empty")]
    pub terms: FnvHashMap<TermId, String>,
    #[serde(skip_serializing_if = "FnvHashMap::is_empty")]
    pub highlight: FnvHashMap<TermId, String>,
    pub request: RequestSearchPart,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phrase_boost: Option<RequestPhraseBoost>,
    /// store the term id hits field->Term->Hits, used for whyfound and term_locality_boost
    #[serde(skip_serializing_if = "FnvHashMap::is_empty")]
    pub term_id_hits_in_field: FnvHashMap<String, FnvHashMap<String, Vec<TermId>>>,
    /// store the text of the term hit field->Terms, used for whyfound
    #[serde(skip_serializing_if = "FnvHashMap::is_empty")]
    pub term_text_in_field: FnvHashMap<String, Vec<String>>,
}

impl SearchFieldResult {
    pub(crate) fn iter(&self, term_id: u8, _field_id: u8) -> SearchFieldResultIterator<'_> {
        SearchFieldResultIterator {
            data: &self.hits_scores,
            pos: 0,
            term_id,
        }
    }

    //Creates a new result, while keeping metadata for original hits
    pub(crate) fn new_from(other: &SearchFieldResult) -> Self {
        let mut res = SearchFieldResult::default();
        res.terms = other.terms.clone();
        res.highlight = other.highlight.clone();
        res.request = other.request.clone();
        res.phrase_boost = other.phrase_boost.clone();
        res.term_id_hits_in_field = other.term_id_hits_in_field.clone();
        res.term_text_in_field = other.term_text_in_field.clone();
        res
    }
}

impl std::fmt::Display for SearchFieldResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "path {}, hits_scores {}, terms {}, lev_distance {:?} ", self.request.path, self.hits_scores.len(), self.request.terms[0], self.request.levenshtein_distance)?;

        if !self.hits_scores.is_empty(){
            writeln!(f, "(hits_scores {})", self.hits_scores.len())?;
            for el in &self.hits_scores {
                writeln!(f, "({}, {})", el.id, el.score)?;
            }
        }
        if !self.hits_ids.is_empty(){
            writeln!(f, "(hits_ids {})", self.hits_ids.len())?;
            for el in &self.hits_ids {
                writeln!(f, "({})", el)?;
            }
        }

        //TODO explain, terms, highlight, term_id_hits_in_field, term_text_in_field
        Ok(())
    }
}

#[cfg(test)]
use crate::test;
#[bench]
fn bench_search_field_iterator(b: &mut test::Bencher) {
    let mut res = SearchFieldResult::default();
    res.hits_scores = (0..6_000_000).map(|el| search::Hit::new(el, 1.0)).collect();
    b.iter(|| {
        let iter = res.iter(0, 1);
        iter.last().unwrap()
    })
}

#[derive(Debug, Clone)]
pub struct SearchFieldResultIterator<'a> {
    data: &'a [search::Hit],
    pos: usize,
    term_id: u8,
    // field_id: u8,
}

impl<'a> Iterator for SearchFieldResultIterator<'a> {
    type Item = MiniHit;

    #[inline]
    fn count(self) -> usize {
        self.size_hint().0
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.data.len() - self.pos;
        (exact, Some(exact))
    }

    #[inline]
    fn next(&mut self) -> Option<MiniHit> {
        if self.data.len() == self.pos {
            None
        } else {
            let hit = &self.data[self.pos];
            self.pos += 1;
            Some(MiniHit {
                id: hit.id,
                term_id: self.term_id,
                // score: f16::from_f32(hit.score),
                score: hit.score,
                // field_id: self.field_id,
            })
        }
    }
}

impl<'a> ExactSizeIterator for SearchFieldResultIterator<'a> {
    #[inline]
    fn len(&self) -> usize {
        self.data.len() - self.pos
    }
}

impl<'a> FusedIterator for SearchFieldResultIterator<'a> {}

#[derive(Debug, Clone)]
pub struct MiniHit {
    pub id: u32,
    pub score: f32,
    pub term_id: u8,
    // pub field_id: u8,
}
