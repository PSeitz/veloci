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
}

impl FieldPath {
    pub fn from_path(path: &str) -> Self {
        let steps: Vec<_> = path
            .split(".")
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
        FieldPath { steps }
    }

    pub fn to_string(&self) -> String {
        self.steps.iter().map(|sstep| sstep.to_string()).collect::<Vec<_>>().join(".")
    }

    pub fn pop(&mut self) -> Option<FieldPathComponent> {
        self.steps.pop()
    }

    pub fn remove_stem(&mut self, other: &FieldPath) {
        for el in &other.steps {
            self.steps.remove_item(el);
        }
    }

    pub fn contains(&self, other: &FieldPath) -> bool {
        let self_str = self.to_string();
        self_str.contains(&other.to_string())
    }
}

impl std::fmt::Display for FieldPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.to_string())?;
        Ok(())
    }
}

impl std::fmt::Debug for FieldPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.to_string())?;
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
    pub fn to_string(&self) -> String {
        if self.is_1_to_n {
            format!("{}[]", self.path)
        } else {
            format!("{}", self.path)
        }
    }
}

pub fn steps_between_field_paths(start: &str, end: &str) -> (Vec<FieldPath>, Vec<FieldPath>) {
    let mut start = start.to_field_path();
    let mut end_steps = end.to_field_path();

    let mut path_to_walk_down: Vec<FieldPath> = vec![];

    while !end_steps.contains(&start) {
        start.pop();
        path_to_walk_down.push(start.clone());
    }

    end_steps.remove_stem(&start);

    let mut path_to_walk_up: Vec<FieldPath> = vec![];
    while let Some(sdtep) = end_steps.pop() {
        start.steps.push(sdtep);
        path_to_walk_up.push(start.clone());
    }

    // println!("hehe {:?}", end_steps);
    // println!("DOWN {:?}", path_to_walk_down);
    // println!("UP {:?}", path_to_walk_up);
    (path_to_walk_down, path_to_walk_up)
}

#[test]
fn test_identity() {
    let path = "meanings.ger[].text";
    assert_eq!(path.to_field_path().to_string(), path);
}

#[test]
fn test_from_to_steps() {
    let start = "meanings.ger[].text";
    let end = "meanings.ger[].boost";
    let yops = steps_between_field_paths(start, end);

    assert_eq!(yops, (vec!["meanings.ger[]".to_field_path(),], vec!["meanings.ger[].boost".to_field_path(),]));
}
