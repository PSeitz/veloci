use search;

#[test]
fn test_hit_coll() {
    let yo: Box<HitCollector> = Box::new(VecHitCollector { hits_vec: vec![] });

    for x in yo.iter() {
        info!("{:?}", x); // x: i32
    }

    for x in yo.iter() {
        info!("{:?}", x); // x: i32
    }

    // for x in yo.into_iter() {
    //     info!("{:?}", x); // x: i32
    // }
}

// trait Iter: Sync {
//     fn next(&self) -> Iterator<Item=(u32, f32)>;
// }

pub trait HitCollector: Sync + Send {
    // fn add(&mut self, hits: u32, score:f32);
    // fn union(&mut self, other:&Self);
    // fn intersect(&mut self, other:&Self);
    // fn iter<'a>(&'a self) -> VecHitCollectorIter<'a>;
    fn iter<'a>(&'a self) -> Box<Iterator<Item = search::Hit> + 'a>;
    // fn iter<'a>(&'a self) -> Box<'a,Iterator<Item=(u32, f32)>>;
    // fn iter<'b>(&'b self) -> Box<Iterator<Item=&'b (u32, f32)>>;
    fn into_iter(self) -> Box<Iterator<Item = search::Hit>>;
    fn get_score(&self, id: usize) -> Option<f32>;

    fn get_score_mut(&mut self, id: usize) -> Option<&mut f32>;
}

#[derive(Debug, Clone)]
pub struct VecHitCollector {
    pub hits_vec: Vec<search::Hit>,
}

#[derive(Debug, Clone)]
struct VecHitCollectorIter<'a> {
    hits_vec: &'a Vec<search::Hit>,
    pos: usize,
}
impl<'a> Iterator for VecHitCollectorIter<'a> {
    type Item = search::Hit;
    fn next(&mut self) -> Option<search::Hit> {//TODO RETURN BY REF
        if self.pos >= self.hits_vec.len() {
            None
        } else {
            self.pos += 1;
            self.hits_vec.get(self.pos - 1).map(|el| el.clone())
        }
        // Some(&(1, 1.0))
        // self.hits_vec.get(self.pos)
    }
}

#[derive(Debug, Clone)]
pub struct VecHitCollectorIntoIter {
    hits_vec: Vec<search::Hit>,
    pos: usize,
}
impl Iterator for VecHitCollectorIntoIter {
    type Item = search::Hit;
    fn next(&mut self) -> Option<search::Hit> { //TODO RETURN BY REF
        if self.pos >= self.hits_vec.len() {
            None
        } else {
            self.pos += 1;
            self.hits_vec.get(self.pos - 1).map(|el| el.clone())
        }
    }
}

impl HitCollector for VecHitCollector {
    // fn add(&mut self, hits: u32, score:f32) // { // }
    // fn union(&mut self, other:&Self) // { // }
    // fn intersect(&mut self, other:&Self) // { // }

    fn iter<'a>(&'a self) -> Box<Iterator<Item = search::Hit> + 'a> {
        Box::new(VecHitCollectorIter {
            hits_vec: &self.hits_vec,
            pos: 0,
        })
    }

    // fn iter<'b>(&'b self) -> Box<Iterator<Item=&'b search::Hit>>
    // {
    //     Box::new(VecHitCollectorIter{hits_vec: &self.hits_vec}) as Box<Iterator<Item=&search::Hit>>
    // }
    fn into_iter(self) -> Box<Iterator<Item = search::Hit>> {
        Box::new(VecHitCollectorIntoIter {
            hits_vec: self.hits_vec,
            pos: 0,
        })
    }

    fn get_score(&self, id: usize) -> Option<f32> {
        self.hits_vec.get(id).map(|el| el.score)
        // Some(1)
    }

    fn get_score_mut(&mut self, id: usize) -> Option<&mut f32> {
        self.hits_vec.get_mut(id).map(|el| &mut el.score)
        // Some(1)
    }
}
