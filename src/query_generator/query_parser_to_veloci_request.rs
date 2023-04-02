use crate::{
    query_generator::*,
    search::request::search_request::{SearchRequest, SearchTree},
};

use crate::error::VelociError;
use query_parser::{
    self,
    ast::{Operator, UserAST},
};
pub(crate) fn ast_to_search_request(query_ast: &UserAST, all_fields: &[String], opt: &SearchQueryGeneratorParameters) -> Result<SearchRequest, VelociError> {
    filter_stopwords(query_ast, opt);
    let query_ast = expand_fields_in_query_ast(query_ast, all_fields)?;
    Ok(query_ast_to_request(&query_ast, opt, None))
}

/// Converts the SearchQueryGeneratorParameters into an SearchRequest ast
///
/// * has a special meaning as a searchterm, it counts as a wildcard, e.g.
/// foo* will match all tokens starting with foo
/// foo*bar will match all tokens starting with foo and ending with bar
/// *foo* will match all tokens containing foo
fn query_ast_to_request(ast: &UserAST, opt: &SearchQueryGeneratorParameters, field_name: Option<&str>) -> SearchRequest {
    match ast {
        UserAST::BinaryClause(ast1, op, ast2) => {
            let queries = [ast1, ast2].iter().map(|ast| query_ast_to_request(ast, opt, field_name)).collect();
            match op {
                Operator::And => SearchRequest::And(SearchTree {
                    queries,
                    options: Default::default(),
                }),
                Operator::Or => SearchRequest::Or(SearchTree {
                    queries,
                    options: Default::default(),
                }),
            }
        }
        UserAST::Attributed(attr, ast) => query_ast_to_request(ast, opt, Some(attr)),
        UserAST::Leaf(filter) => {
            let field_name: &str = field_name.as_ref().unwrap();
            let mut term = filter.phrase.to_string();

            let mut levenshtein_distance = None;
            // One star at the end means it's a starts_with query, which can be combined with
            // levensthein
            let starts_with = term.ends_with("*") && term.chars().filter(|&c| c == '*').count() == 1;
            if starts_with {
                term.pop();
            }

            // regex is currently enabled, when there is a star, except if there is only one star at the the end, e.g. fooba*
            // Then it uses a combination of fuzzy + starts_with
            // This enables fuzzy search with patterns, currently there is no fuzzy_search for regex
            let is_regex = term.contains('*');
            if is_regex {
                use itertools::Itertools;
                term = term.split('*').map(regex::escape).join(".*");
            } else {
                levenshtein_distance = if let Some(levenshtein) = filter.levenshtein {
                    Some(u32::from(levenshtein))
                } else {
                    Some(get_levenshteinn(&term, opt.levenshtein, opt.levenshtein_auto_limit, starts_with))
                };
            }

            let part = RequestSearchPart {
                boost: opt.boost_fields.as_ref().and_then(|boost| boost.get(field_name).map(|el| OrderedFloat(*el))),
                levenshtein_distance,
                path: field_name.to_string(),
                terms: vec![term],
                starts_with,
                is_regex,
                ignore_case: opt.ignore_case,
                ..Default::default()
            };
            SearchRequest::Search(part)
        }
    }
}

fn expand_fields_in_query_ast(ast: &UserAST, all_fields: &[String]) -> Result<UserAST, VelociError> {
    match ast {
        UserAST::BinaryClause(ast1, op, ast2) => Ok(UserAST::BinaryClause(
            expand_fields_in_query_ast(ast1, all_fields)?.into(),
            *op,
            expand_fields_in_query_ast(ast2, all_fields)?.into(),
        )),
        UserAST::Leaf(_) => {
            let mut field_iter = all_fields.iter();
            let mut curr_ast = field_iter
                .next()
                .map(|field_name| UserAST::Attributed(field_name.to_string(), Box::new(ast.clone())))
                .unwrap();

            for field_name in field_iter {
                let next_ast = UserAST::Attributed(field_name.to_string(), Box::new(ast.clone()));
                curr_ast = UserAST::BinaryClause(next_ast.into(), Operator::Or, curr_ast.into());
            }

            Ok(curr_ast)
        }
        UserAST::Attributed(field_name, _) => {
            // dont expand in UserAST::Attributed
            check_field(field_name, all_fields)?;
            Ok(ast.clone())
        }
    }
}

//TODO should be field specific
fn filter_stopwords(query_ast: &query_parser::ast::UserAST, opt: &SearchQueryGeneratorParameters) -> Option<UserAST> {
    query_ast.filter_ast(
        &mut |ast: &UserAST, _attr: Option<&str>| match ast {
            UserAST::Leaf(filter) => {
                if let Some(languages) = opt.stopword_lists.as_ref() {
                    languages.iter().any(|lang| stopwords::is_stopword(lang, &filter.phrase.to_lowercase()))
                } else if let Some(stopwords) = opt.stopwords.as_ref() {
                    stopwords.contains(&filter.phrase.to_lowercase())
                } else {
                    false
                }
            }
            _ => false,
        },
        None,
    )
}
#[cfg(all(test, feature = "unstable"))]
mod bench {
    use super::*;

    #[bench]
    fn bench_query_custom_parse_to_request(b: &mut test::Bencher) {
        let fields = vec![
            "Title".to_string(),
            "Author".to_string(),
            "Author1".to_string(),
            "Author2".to_string(),
            "Author3".to_string(),
            "Author4".to_string(),
            "Author5".to_string(),
            "Author6".to_string(),
            "Author7".to_string(),
            "Author8".to_string(),
            "Author9".to_string(),
            "Author10".to_string(),
            "Author11".to_string(),
            "Author12".to_string(),
            "Author13".to_string(),
        ];
        b.iter(|| {
            let query_ast = query_parser::parse("die drei fragezeigen und das unicorn").unwrap();
            ast_to_search_request(&query_ast, &fields, &SearchQueryGeneratorParameters::default()).unwrap()
        })
    }

    #[bench]
    fn bench_custom_parse_expand_fields_in_query_ast(b: &mut test::Bencher) {
        let fields = vec![
            "Title".to_string(),
            "Author".to_string(),
            "Author1".to_string(),
            "Author2".to_string(),
            "Author3".to_string(),
            "Author4".to_string(),
            "Author5".to_string(),
            "Author6".to_string(),
            "Author7".to_string(),
            "Author8".to_string(),
            "Author9".to_string(),
            "Author10".to_string(),
            "Author11".to_string(),
            "Author12".to_string(),
            "Author13".to_string(),
        ];
        b.iter(|| {
            let query_ast = query_parser::parse("die drei fragezeigen und das unicorn").unwrap();
            expand_fields_in_query_ast(&query_ast, &fields).unwrap()
        })
    }
}

#[test]
fn test_filter_stopwords() {
    let query_ast = query_parser::parse("die erbin").unwrap();
    // let mut query_ast = query_ast.simplify();
    let mut opt = SearchQueryGeneratorParameters::default();
    opt.stopword_lists = Some(vec!["de".to_string()]);
    let query_ast = filter_stopwords(&query_ast, &opt);
    assert_eq!(query_ast, Some("erbin".into()));
}

#[test]
fn test_filter_stopwords_by_userdefined_stopword_list() {
    let query_ast = query_parser::parse("die erbin").unwrap();
    let mut opt = SearchQueryGeneratorParameters::default();
    opt.stopwords = Some(["die".to_string()].iter().cloned().collect());
    let query_ast = filter_stopwords(&query_ast, &opt);
    assert_eq!(query_ast, Some("erbin".into()));
}

#[test]
fn test_field_expand() {
    use query_parser::ast::UserFilter;
    let fields = vec!["Title".to_string(), "Author[].name".to_string()];
    let ast = UserAST::Leaf(Box::new(UserFilter {
        phrase: "Fred".to_string(),
        levenshtein: None,
    }));
    let expanded_ast = expand_fields_in_query_ast(&ast, &fields).unwrap();
    assert_eq!(format!("{:?}", expanded_ast), "(Author[].name:\"Fred\" OR Title:\"Fred\")");

    let ast = UserAST::Attributed(
        "Title".to_string(),
        UserAST::Leaf(Box::new(UserFilter {
            phrase: "Fred".to_string(),
            levenshtein: None,
        }))
        .into(),
    );
    let expanded_ast = expand_fields_in_query_ast(&ast, &fields).unwrap();
    assert_eq!(format!("{:?}", expanded_ast), "Title:\"Fred\"");
}
