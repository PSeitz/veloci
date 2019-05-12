
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

trait ToFieldPath {
    fn to_field_path(&self) -> FieldPath;
}

impl ToFieldPath for &str {
    fn to_field_path(&self) -> FieldPath{
        FieldPath::from_path(self)
    }
}

#[derive(Default, PartialEq, Eq, Clone)]
pub struct FieldPath {
    steps: Vec<Step>,
}

impl FieldPath {
    pub fn from_path(path: &str) -> Self {
        let steps: Vec<_> = path.split(".").map(|el| if el.ends_with("[]") { 
            Step{path:el[0..el.len() - 2].to_string(), is_1_to_n:true}
        } else {
            Step{path:el.to_string(), is_1_to_n:false}
        }).collect();
        FieldPath{steps}
    }

    pub fn to_string(&self) -> String {
        self.steps.iter().map(|step|step.to_string()).collect::<Vec<_>>().join(".")
    }

    pub fn pop(&mut self) -> Option<Step> {
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


#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Step {
    path: String,
    is_1_to_n: bool
}

impl Step {
    pub fn to_string(&self) -> String {
        if self.is_1_to_n {
            format!("{}[]", self.path)
        }else{
            format!("{}", self.path)
        }
    }
}

pub fn steps_between_field_paths(start: &str, end:&str) -> (Vec<FieldPath>, Vec<FieldPath>) {
    let mut start = start.to_field_path();
    let mut end_steps = end.to_field_path();

    let mut path_to_walk_down: Vec<FieldPath> = vec![];

    while !end_steps.contains(&start) {
        start.pop();
        path_to_walk_down.push(start.clone());
    }

    end_steps.remove_stem(&start);

    let mut path_to_walk_up: Vec<FieldPath> = vec![];
    while let Some(step) = end_steps.pop() {
        start.steps.push(step);
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

    assert_eq!(yops, 
        (vec![
            "meanings.ger[]".to_field_path(),
        ],
        vec![
            "meanings.ger[].boost".to_field_path(),
        ])
    );

}