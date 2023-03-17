mod leaf;
mod operator;

pub use leaf::UserFilter;
pub use operator::Operator;

use std::{collections::HashSet, convert::From, fmt};

#[derive(Clone, PartialEq, Eq)]
pub enum UserAST {
    Attributed(String, Box<UserAST>),
    BinaryClause(Box<UserAST>, Operator, Box<UserAST>),
    Leaf(Box<UserFilter>),
}

// conversion used in tests
impl From<&'static str> for UserAST {
    fn from(item: &str) -> Self {
        let mut filter = UserFilter {
            phrase: item.to_string(),
            levenshtein: None,
        };
        if item.chars().next().map(|c| c != '\"').unwrap_or(false) {
            let parts_field = item.splitn(2, ':').collect::<Vec<_>>();
            if parts_field.len() > 1 {
                filter.phrase = parts_field[1].to_string();
            }

            let yo = filter.phrase.to_string();
            let parts_leven: Vec<_> = yo.splitn(2, '~').collect::<Vec<_>>();
            if parts_leven.len() > 1 {
                filter.phrase = parts_leven[0].to_string();
                filter.levenshtein = Some(parts_leven[1].parse().unwrap());
            }

            if parts_field.len() > 1 {
                return UserAST::Attributed(parts_field[0].to_string(), Box::new(UserAST::Leaf(Box::new(filter))));
            }
        }
        UserAST::Leaf(Box::new(filter))
    }
}

impl From<&'static str> for Box<UserAST> {
    fn from(item: &'static str) -> Self {
        Box::new(item.into())
    }
}

impl fmt::Debug for UserAST {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            UserAST::Attributed(attr, ast) => write!(formatter, "{}:{:?}", attr, ast),
            UserAST::BinaryClause(ast1, op, ast2) => write!(formatter, "({:?} {} {:?})", ast1, op, ast2),
            UserAST::Leaf(filter) => write!(formatter, "{:?}", filter),
        }
    }
}

impl From<(UserAST, Operator, UserAST)> for UserAST {
    fn from(item: (UserAST, Operator, UserAST)) -> Self {
        UserAST::BinaryClause(Box::new(item.0), item.1, Box::new(item.2))
    }
}

impl UserAST {
    /// Filters the AST according to the bool returned in the should_filter callback.
    ///
    /// Can filter any parts of the AST, while keeping a valid ast.
    /// Filtering means a complete sub part of the AST will be removed.
    ///
    /// The should_filter callback provides two values:
    /// The current AST, and the current attribute filter `Option<&str>`, which is applied on the subtree
    pub fn filter_ast<F>(&self, should_filter: &mut F, current_attr: Option<&str>) -> Option<UserAST>
    where
        F: FnMut(&UserAST, Option<&str>) -> bool,
    {
        if should_filter(self, current_attr) {
            return None;
        }
        match self {
            UserAST::Attributed(attr, ast) => return UserAST::filter_ast(ast, should_filter, Some(attr)).map(|ast| UserAST::Attributed(attr.to_string(), ast.into())),
            UserAST::BinaryClause(ast1, op, ast2) => {
                let filtered_ast1 = UserAST::filter_ast(ast1, should_filter, current_attr);
                let filtered_ast2 = UserAST::filter_ast(ast2, should_filter, current_attr);
                return match (filtered_ast1, filtered_ast2) {
                    (Some(filtered_ast1), Some(filtered_ast2)) => return Some(UserAST::BinaryClause(filtered_ast1.into(), *op, filtered_ast2.into())),
                    (None, Some(filtered_ast2)) => Some(filtered_ast2),
                    (Some(filtered_ast1), None) => Some(filtered_ast1),
                    (None, None) => None,
                };
            }
            UserAST::Leaf(_filter) => {}
        }

        Some(self.clone())
    }

    /// Maps the AST according to the returned ast in the map_fn callback.
    ///
    /// Can be used to walk over the AST and replace parts of it.
    ///
    /// The map_fn callback provides two values:
    /// The current AST, and the current attribute filter `Option<&str>`, which is applied on the subtree
    pub fn map_ast<F>(mut self, map_fn: &mut F, current_attr: Option<&str>) -> UserAST
    where
        F: FnMut(UserAST, Option<&str>) -> UserAST,
    {
        match self {
            UserAST::Attributed(ref attr, ref mut ast) => *ast = Box::new(UserAST::map_ast(*ast.clone(), map_fn, Some(attr))),
            UserAST::BinaryClause(ref mut ast1, _op, ref mut ast2) => {
                *ast1 = Box::new(UserAST::map_ast(*ast1.clone(), map_fn, current_attr));
                *ast2 = Box::new(UserAST::map_ast(*ast2.clone(), map_fn, current_attr));
            }
            UserAST::Leaf(ref _filter) => {}
        }

        map_fn(self, current_attr)
    }

    /// walking the ast and grouping adjacent terms for phrase boosting
    pub fn get_phrase_pairs(&self) -> HashSet<[&str; 2]> {
        let mut collect = HashSet::new();
        self._get_phrase_pairs(&mut collect, &mut None, None);
        collect
    }

    fn _get_phrase_pairs<'a>(&'a self, collect: &mut HashSet<[&'a str; 2]>, last_term: &mut Option<&'a str>, curr_attr: Option<&'a str>) {
        match self {
            UserAST::Attributed(attr, ast) => {
                if curr_attr == Some(attr) || curr_attr.is_none() {
                    ast._get_phrase_pairs(collect, last_term, Some(attr));
                } else {
                    ast._get_phrase_pairs(collect, &mut None, Some(attr));
                }
            }
            UserAST::BinaryClause(ast1, _op, ast2) => {
                ast1._get_phrase_pairs(collect, last_term, curr_attr);
                ast2._get_phrase_pairs(collect, last_term, curr_attr);
            }
            UserAST::Leaf(filter) => {
                if let Some(last_term) = last_term {
                    collect.insert([last_term, &filter.phrase]);
                }
                *last_term = Some(&filter.phrase)
            }
        }
    }

    /// walking the ast in order, emitting all terms
    pub fn walk_terms<'a, F>(&'a self, cb: &mut F)
    where
        F: FnMut(&'a str),
    {
        match self {
            UserAST::Attributed(_attr, ast) => {
                ast.walk_terms(cb);
            }
            UserAST::BinaryClause(ast1, _op, ast2) => {
                ast1.walk_terms(cb);
                ast2.walk_terms(cb);
            }
            UserAST::Leaf(filter) => cb(&filter.phrase),
        }
    }
}

#[cfg(test)]
mod test_ast {
    use crate::{
        ast::{Operator::*, UserAST, UserFilter},
        parser::parse,
    };

    #[test]
    fn test_ast_external_lifetime() {
        let external_term_1 = "a".to_string();
        let filter_1 = UserFilter {
            phrase: external_term_1,
            levenshtein: None,
        };
        let left_ast: UserAST = UserAST::Leaf(Box::new(filter_1));
        let external_term_2 = "b".to_string();
        let filter_2 = UserFilter {
            phrase: external_term_2,
            levenshtein: None,
        };
        let right_ast: UserAST = UserAST::Leaf(Box::new(filter_2));

        UserAST::BinaryClause(Box::new(left_ast), Or, Box::new(right_ast));
    }

    #[test]
    fn test_filter_ast() {
        let ast: UserAST = ("super".into(), Or, ("cool".into(), Or, "fancy".into()).into()).into();
        let ast = ast.filter_ast(
            &mut |ast: &UserAST, _attr: Option<&str>| match ast {
                UserAST::Leaf(filter) => filter.phrase == "cool",
                _ => false,
            },
            None,
        );

        assert_eq!(ast, Some(("super".into(), Or, "fancy".into()).into()));

        let ast: UserAST = parse("myattr:(super cool)").unwrap();

        assert_eq!(ast.filter_ast(&mut |_ast, _attr| { true }, None), None);

        assert_eq!(
            ast.filter_ast(
                &mut |ast, _attr| {
                    match ast {
                        UserAST::Leaf(filter) => filter.phrase == "cool",
                        _ => false,
                    }
                },
                None
            ),
            Some(UserAST::Attributed("myattr".to_string(), "super".into()))
        );
    }
    #[test]
    fn test_map_ast() {
        let ast: UserAST = ("super".into(), Or, ("cool".into(), Or, "fancy".into()).into()).into();
        let ast = ast.map_ast(
            &mut |ast: UserAST, _attr: Option<&str>| match ast {
                UserAST::Leaf(ref map) if map.phrase == "cool" => UserAST::Leaf(Box::new(UserFilter {
                    phrase: "coolcool".to_string(),
                    levenshtein: None,
                })),
                _ => ast,
            },
            None,
        );

        let expected_mapped_ast: UserAST = ("super".into(), Or, ("coolcool".into(), Or, "fancy".into()).into()).into();
        assert_eq!(ast, expected_mapped_ast);

        let ast: UserAST = "kawaii".into();
        let ast = ast.map_ast(
            &mut |ast: UserAST, _attr: Option<&str>| match ast {
                UserAST::Leaf(ref map) if map.phrase == "kawaii" => {
                    let leftast = UserAST::Leaf(Box::new(UserFilter {
                        phrase: "kawaii".to_string(),
                        levenshtein: None,
                    }));
                    let rightast = UserAST::Leaf(Box::new(UserFilter {
                        phrase: "かわいい".to_string(),
                        levenshtein: None,
                    }));

                    UserAST::BinaryClause(Box::new(leftast), Or, Box::new(rightast))
                }
                _ => ast,
            },
            None,
        );

        let expected_mapped_ast: UserAST = ("kawaii".into(), Or, "かわいい".into()).into();
        assert_eq!(ast, expected_mapped_ast);
    }

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
    fn test_get_phrase_pairs_or() {
        // let ast: UserAST = parse("super cool fancy").unwrap();
        let ast: UserAST = ("super".into(), Or, ("cool".into(), Or, "fancy".into()).into()).into();
        assert_eq!(ast.get_phrase_pairs(), [["super", "cool"], ["cool", "fancy"]].iter().copied().collect());
        let ast: UserAST = ("super".into(), Or, ("cool".into(), Or, ("fancy".into(), Or, "great".into()).into()).into()).into();
        assert_eq!(ast.get_phrase_pairs(), [["super", "cool"], ["cool", "fancy"], ["fancy", "great"]].iter().copied().collect());

        let ast: UserAST = parse("super cool nice great").unwrap();
        assert_eq!(ast.get_phrase_pairs(), [["super", "cool"], ["cool", "nice"], ["nice", "great"]].iter().copied().collect());

        let ast: UserAST = parse("myattr:(super cool) AND fancy").unwrap();
        // let ast: UserAST = ("super".into(), Or, ("cool".into(), Or, "fancy".into()).into()).into();
        let mut terms = vec![];
        ast.walk_terms(&mut |term| terms.push(term));
        assert_eq!(terms, vec!["super", "cool", "fancy"]);

        let ast: UserAST = parse("myattr:(super cool)").unwrap();
        assert_eq!(ast.get_phrase_pairs(), [["super", "cool"]].iter().copied().collect());

        let ast: UserAST = parse("myattr:(super cool) different scope").unwrap();
        assert_eq!(
            ast.get_phrase_pairs(),
            [["super", "cool"], ["cool", "different"], ["different", "scope"]].iter().copied().collect()
        );

        // let ast: UserAST = parse("different scope OR myattr:(super cool)").unwrap();
        // assert_eq!(
        //     ast.get_phrase_pairs(),
        //     [["super","cool"],["cool","different"],["different","scope"]].iter().map(|el|*el).collect()
        // );
    }
}
