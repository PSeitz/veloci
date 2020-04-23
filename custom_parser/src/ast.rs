use std::{convert::From, fmt};

#[derive(Clone, PartialEq, Eq)]
pub struct UserFilter<'a> {
    /// the search term
    pub phrase: &'a str,
    /// levenshtein edit distance https://en.wikipedia.org/wiki/Levenshtein_distance
    pub levenshtein: Option<u8>,
}

impl<'a> fmt::Debug for UserFilter<'a> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if let Some(levenshtein) = self.levenshtein {
            write!(formatter, "\"{}\"~{:?}", self.phrase, levenshtein)
        } else {
            write!(formatter, "\"{}\"", self.phrase)
        }
    }
}

// conversion for tests
impl<'a> From<&'static str> for UserAST<'a> {
    fn from(item: &'a str) -> Self {
        let mut filter = UserFilter {
            phrase: item,
            levenshtein: None,
        };
        if item.chars().next().map(|c| c != '\"').unwrap_or(false) {
            let parts_field = item.splitn(2, ':').collect::<Vec<_>>();
            if parts_field.len() > 1 {
                filter.phrase = parts_field[1];
            }

            let parts_leven: Vec<_> = filter.phrase.splitn(2, '~').collect::<Vec<_>>();
            if parts_leven.len() > 1 {
                filter.phrase = parts_leven[0];
                filter.levenshtein = Some(parts_leven[1].parse().unwrap());
            }

            if parts_field.len() > 1 {
                return UserAST::Attributed(parts_field[0], Box::new(UserAST::Leaf(Box::new(filter))));
            }
        }
        UserAST::Leaf(Box::new(filter))
    }
}
impl From<&'static str> for Box<UserAST<'_>> {
    fn from(item: &'static str) -> Self {
        Box::new(item.into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Or,
    And,
}
impl fmt::Display for Operator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
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

#[derive(Clone, PartialEq, Eq)]
pub enum UserAST<'a> {
    Attributed(&'a str, Box<UserAST<'a>>),
    BinaryClause(Box<UserAST<'a>>, Operator, Box<UserAST<'a>>),
    Leaf(Box<UserFilter<'a>>),
}

impl<'a> fmt::Debug for UserAST<'a> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            UserAST::Attributed(attr, ast) => write!(formatter, "{}:{:?}", attr, ast),
            UserAST::BinaryClause(ast1, op, ast2) => write!(formatter, "({:?} {} {:?})", ast1, op, ast2),
            UserAST::Leaf(filter) => write!(formatter, "{:?}", filter),
        }
    }
}

impl<'a> From<(UserAST<'a>, Operator, UserAST<'a>)> for UserAST<'a> {
    fn from(item: (UserAST<'a>, Operator, UserAST<'a>)) -> Self {
        UserAST::BinaryClause(Box::new(item.0), item.1, Box::new(item.2))
    }
}

use std::collections::HashSet;


impl UserAST<'_> {
    /// walking the ast and grouping adjacent terms for phrase boosting
    pub fn get_phrase_pairs(&self) -> HashSet<[&str; 2]> {
        let mut collect = HashSet::new();
        self._get_phrase_pairs(&mut collect, &mut None, None);
        collect
    }

    fn _get_phrase_pairs<'a>(&'a self, collect: &mut HashSet<[&'a str; 2]>, last_term: &mut Option<&'a str>, curr_attr: Option<&'a str>) {
        match self {
            UserAST::Attributed(attr, ast) => {
                if curr_attr == Some(attr) || curr_attr.is_none(){
                    ast._get_phrase_pairs(collect, last_term, Some(attr));
                }else{
                    ast._get_phrase_pairs(collect, &mut None, Some(attr));
                }
            },
            UserAST::BinaryClause(ast1, _op, ast2) => {
                // let terms1 = ast1.get_terms_from_ast(op);
                // println!("terms1 {:?}", terms1);
                // // let mut terms2 = HashSet::new();
                // let terms2 = ast2.get_terms_from_ast(op);

                // for t1 in &terms1 {
                //     for t2 in &terms2 {
                //         collect.insert([t1, t2]);
                //         if let Some(last_term) = last_term {
                //             collect.insert([last_term, t2]);
                //         }

                //     }
                // }
                // if terms1.len() == 1 {
                //     ast1._get_phrase_pairs(collect, terms1.into_iter().next(), curr_attr);
                // }else{
                //     ast1._get_phrase_pairs(collect, last_term, curr_attr);
                // }
                
                // if terms2.len() == 1 {
                //     ast2._get_phrase_pairs(collect, terms2.into_iter().next(), curr_attr);
                // }else{
                //     ast2._get_phrase_pairs(collect, last_term, curr_attr);
                // }
                // ast2._get_phrase_pairs(collect, last_term, curr_attr);

                ast1._get_phrase_pairs(collect, last_term, curr_attr);
                ast2._get_phrase_pairs(collect, last_term, curr_attr);
            },
            UserAST::Leaf(filter) => {
                if let Some(last_term) = last_term {
                    collect.insert([last_term, &filter.phrase]);
                }
                *last_term = Some(&filter.phrase)
            },
        }
    }

    /// walking the ast in order, emitting all terms
    pub fn walk_terms<F>(&self, cb: &mut F)
    where
        F: FnMut(&str),
    {
            match self {
                UserAST::Attributed(_attr, ast) => {
                    ast.walk_terms(cb);
                },
                UserAST::BinaryClause(ast1, _op, ast2) => {
                    ast1.walk_terms(cb);
                    ast2.walk_terms(cb);
                },
                UserAST::Leaf(filter) => {
                    cb(&filter.phrase)
                },
            }
    }

}

#[cfg(test)]
mod test_ast {
    use crate::ast::UserAST;
    use crate::ast::Operator::*;
    use crate::parser::Parser;
    

    // #[test]
    // fn test_and_or() {
    //     let ast: UserAST = ("cool".into(), Or, "fancy".into()).into();
    //     assert_eq!(
    //         ast.get_phrase_pairs(),
    //         [["cool","fancy"]].iter().map(|el|*el).collect()
    //     );
    //     let ast: UserAST = ("super".into(), And, ("cool".into(), Or, "fancy".into()).into()).into();
    //     assert_eq!(
    //         ast.get_phrase_pairs(),
    //         [["cool","fancy"],["super","cool"],["super","fancy"]].iter().map(|el|*el).collect()
    //     );
    // }

    #[test]
    fn test_or() {
        // let ast: UserAST = Parser::parse("super cool fancy").unwrap();
        let ast: UserAST<'_> = ("super".into(), Or, ("cool".into(), Or, "fancy".into()).into()).into();
        assert_eq!(
            ast.get_phrase_pairs(),
            [["super","cool"],["cool","fancy"]].iter().map(|el|*el).collect()
        );
        let ast: UserAST<'_> = ("super".into(), Or, ("cool".into(), Or, ("fancy".into(), Or, "great".into()).into()).into()).into();
        assert_eq!(
            ast.get_phrase_pairs(),
            [["super","cool"],["cool","fancy"],["fancy","great"]].iter().map(|el|*el).collect()
        );

        let ast: UserAST<'_> = Parser::parse("super cool nice great").unwrap();
        assert_eq!(
            ast.get_phrase_pairs(),
            [["super","cool"],["cool","nice"],["nice","great"]].iter().map(|el|*el).collect()
        );

        // let ast: UserAST = ("super".into(), Or, ("cool".into(), Or, "fancy".into()).into()).into();
        // ast.walk_terms(&mut |term| println!("{:?}", term));

        let ast: UserAST<'_> = Parser::parse("myattr:(super cool)").unwrap();
        assert_eq!(
            ast.get_phrase_pairs(),
            [["super","cool"]].iter().map(|el|*el).collect()
        );

        let ast: UserAST<'_> = Parser::parse("myattr:(super cool) different scope").unwrap();
        assert_eq!(
            ast.get_phrase_pairs(),
            [["super","cool"],["cool","different"],["different","scope"]].iter().map(|el|*el).collect()
        );

        // let ast: UserAST = Parser::parse("different scope OR myattr:(super cool)").unwrap();
        // assert_eq!(
        //     ast.get_phrase_pairs(),
        //     [["super","cool"],["cool","different"],["different","scope"]].iter().map(|el|*el).collect()
        // );

    }
}
