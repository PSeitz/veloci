#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Or,
    And,
}
impl std::fmt::Display for Operator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Operator::Or => write!(formatter, "OR"),
            Operator::And => write!(formatter, "AND"),
        }
    }
}

impl From<&str> for Operator {
    fn from(item: &str) -> Self {
        if item == "OR" {
            return Operator::Or;
        }
        if item == "AND" {
            return Operator::And;
        }
        panic!("could not convert {:?} to operator", item);
    }
}
