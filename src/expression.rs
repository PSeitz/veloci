#[derive(Debug)]
pub struct ScoreExpression {
    #[allow(dead_code)]
    expression: String,
    ops: Vec<OperatorType>,
}

#[derive(Debug)]
enum OperatorType {
    Division,
    Mul,
    Add,
    Sub,
    _Score_,
    Float(f32),
}

#[derive(Debug)]
#[allow(dead_code)]
enum OperationStep {
    OperatorType,
    Value,
}

impl ScoreExpression {
    pub fn get_score(&self, rank: f32) -> f32 {
        let left = match self.ops[0] {
            OperatorType::_Score_ => rank,
            OperatorType::Float(val) => val,
            _ => panic!("Need to start with float oder $SCORE"),
        };

        let right = match self.ops[2] {
            OperatorType::_Score_ => rank,
            OperatorType::Float(val) => val,
            _ => panic!("Need to end with float oder $SCORE"),
        };

        match self.ops[1] {
            OperatorType::Division => left / right,
            OperatorType::Mul => left * right,
            OperatorType::Add => left + right,
            OperatorType::Sub => left - right,
            _ => panic!("Need to be an operator [*, +, -, /]"),
        }
    }

    fn parse(expression: &str) -> Vec<OperatorType> {
        let mut operations: Vec<OperatorType> = vec![];
        let mut current = "".to_string();
        // let currVal = None;
        for next_char in expression.chars() {
            if let ' ' = next_char {
                let val = current.parse::<f32>();
                // trace!("{:?}", val);
                if let Ok(val) = val {
                    operations.push(OperatorType::Float(val));
                }
                current.clear();
            }

            if next_char != ' ' {
                current += &next_char.to_string();
            }
            match current.as_ref() {
                "+" => {
                    operations.push(OperatorType::Add);
                    current.clear();
                }
                "-" => {
                    operations.push(OperatorType::Sub);
                    current.clear();
                }
                "/" => {
                    operations.push(OperatorType::Division);
                    current.clear();
                }
                "*" => {
                    operations.push(OperatorType::Mul);
                    current.clear();
                }
                "$SCORE" => {
                    operations.push(OperatorType::_Score_);
                    current.clear();
                }
                _ => {}
            }
        }

        if let Ok(val) = current.parse::<f32>() {
            operations.push(OperatorType::Float(val));
        }
        // trace!("{:?}", operations);
        operations
    }

    pub fn new(expression: String) -> Self {
        let ops = ScoreExpression::parse(&expression);
        ScoreExpression { expression, ops }
    }
}

#[allow(dead_code)]
fn mult(val: f32) -> f32 {
    val * val
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser() {
        let expre = ScoreExpression::new("$SCORE + 2.0".to_string());
        assert_eq!(expre.get_score(10.0), 12.0);

        let expre = ScoreExpression::new("10.0 / $SCORE".to_string());
        assert_eq!(expre.get_score(10.0), 1.0);

        let expre = ScoreExpression::new("$SCORE * $SCORE".to_string());
        assert_eq!(expre.get_score(10.0), 100.0);
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use super::*;
    use crate::test::Bencher;

    #[bench]
    fn bench_expr_mult(b: &mut Bencher) {
        let expre = ScoreExpression::new("$SCORE * $SCORE".to_string());
        b.iter(|| expre.get_score(10.0));
    }

    #[bench]
    fn bench_mult(b: &mut Bencher) {
        b.iter(|| mult(10.0));
    }
}
