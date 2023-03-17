use itertools::Itertools;

use crate::persistence::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

pub trait ToFieldPath {
    fn to_field_path(&self) -> FieldPath;
}

impl ToFieldPath for &str {
    fn to_field_path(&self) -> FieldPath {
        FieldPath::from_path(self)
    }
}

impl ToFieldPath for &String {
    fn to_field_path(&self) -> FieldPath {
        FieldPath::from_path(self)
    }
}

#[derive(Default, PartialEq, Eq, Clone)]
pub struct FieldPath {
    steps: Vec<FieldPathComponent>,
    pub suffix: Option<&'static str>,
}

impl FieldPath {
    pub fn from_path(path: &str) -> Self {
        let mut path = path.to_string();
        let mut suffix = None;
        for el in INDEX_FILE_ENDINGS {
            if path.ends_with(el) {
                suffix = Some(*el);
                path = path.trim_end_matches(el).to_string();
            }
        }

        let steps: Vec<_> = path
            .split('.')
            .map(|el| {
                if el.ends_with("[]") {
                    FieldPathComponent {
                        path: el[0..el.len() - 2].to_string(),
                        is_1_to_n: true,
                    }
                } else {
                    FieldPathComponent {
                        path: el.to_string(),
                        is_1_to_n: false,
                    }
                }
            })
            .collect();
        FieldPath { steps, suffix }
    }

    pub fn as_string(&self) -> String {
        let mut res = self.steps.iter().map(|sstep| sstep.as_string()).collect::<Vec<_>>().join(".");
        if let Some(suffix) = &self.suffix {
            res += suffix;
        }
        res
    }

    pub fn pop(&mut self) -> Option<FieldPathComponent> {
        self.steps.pop()
    }

    pub fn remove_stem(&mut self, other: &FieldPath) {
        for el in &other.steps {
            if let Some(pos) = self.steps.iter().position(|x| x == el) {
                self.steps.remove(pos);
            }
        }
    }

    pub fn contains(&self, other: &FieldPath) -> bool {
        for i in 0..std::cmp::min(self.steps.len(), other.steps.len()) {
            if self.steps[i] != other.steps[i] {
                return false;
            }
        }
        true
    }
}

impl std::fmt::Display for FieldPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}{:?}", self.steps.iter().map(|el| el.as_string()).join("."), self.suffix)?;
        Ok(())
    }
}

impl std::fmt::Debug for FieldPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self)?;
        Ok(())
    }
}

/// One component of a field path, e.g. fieldpath: "meanings.ger[]" has 2 fieldpath components: "meaning" and "ger[]"
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct FieldPathComponent {
    path: String,
    is_1_to_n: bool,
}

impl FieldPathComponent {
    pub fn as_string(&self) -> String {
        if self.is_1_to_n {
            format!("{}[]", self.path)
        } else {
            self.path.to_string()
        }
    }
}

pub fn steps_between_field_paths(start: &str, end: &str) -> Vec<FieldPath> {
    let mut start = start.to_field_path();
    let mut end_steps = end.to_field_path();
    end_steps.suffix = Some(VALUE_ID_TO_PARENT);

    let mut path_to_walk: Vec<FieldPath> = vec![];

    while !end_steps.contains(&start) {
        start.pop();
        start.suffix = Some(VALUE_ID_TO_PARENT);
        path_to_walk.push(start.clone());
    }

    start.suffix = Some(PARENT_TO_VALUE_ID);
    path_to_walk.push(start.clone());
    end_steps.remove_stem(&start);

    while let Some(step) = end_steps.pop() {
        start.steps.push(step);
        start.suffix = Some(PARENT_TO_VALUE_ID);
        path_to_walk.push(start.clone());
    }

    path_to_walk
}

#[test]
fn test_identity() {
    let path = "meanings.ger[].text";
    assert_eq!(path.to_field_path().as_string(), path);
}

#[test]
fn test_from_to_steps_2() {
    let start = "meanings.ger[].text";
    let end = "meanings.ger[].boost";
    let yops = steps_between_field_paths(start, end);

    assert_eq!(
        yops,
        (vec![
            "meanings.ger[].value_id_to_parent".to_field_path(),
            "meanings.ger[].parent_to_value_id".to_field_path(),
            "meanings.ger[].boost.parent_to_value_id".to_field_path(),
        ])
    );
}
