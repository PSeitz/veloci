
use ordered_float::OrderedFloat;
use crate::search::request::search_request::RequestSearchPart;
use crate::search::request::Request;
use crate::query_generator::{get_levenshteinn, check_field, SearchQueryGeneratorParameters};
use crate::error::VelociError;
use parser;
use parser::query_parser::{Operator, UserAST, UserFilter};
use crate::{
    search::{stopwords},
};

pub(crate) fn ast_to_request(query_ast: UserAST, all_fields: &[String], opt: &SearchQueryGeneratorParameters) -> Result<Request, VelociError> {
    let mut query_ast = query_ast.simplify();
    filter_stopwords(&mut query_ast, opt);
    query_ast = expand_fields_in_query_ast(query_ast, all_fields)?;
    let query_ast = query_ast.simplify();
    Ok(query_ast_to_request(query_ast, opt))
}

fn query_ast_to_request(ast: UserAST, opt: &SearchQueryGeneratorParameters) -> Request {
    match ast {
        UserAST::Clause(op, subqueries) => {
            let subqueries = subqueries.into_iter().map(|ast| query_ast_to_request(ast, opt)).collect();
            match op {
                Operator::And => Request {
                    and: Some(subqueries),
                    ..Default::default()
                },
                Operator::Or => Request {
                    or: Some(subqueries),
                    ..Default::default()
                },
            }
        }
        UserAST::Leaf(filter) => {
            let field_name = filter.field_name.as_ref().unwrap();
            let mut term = filter.phrase;

            let starts_with = term.ends_with("*");
            if term.ends_with("*") {
                term.pop();
            }
            let levenshtein_distance = if let Some(levenshtein) = filter.levenshtein {
                Some(u32::from(levenshtein))
            } else {
                Some(get_levenshteinn(&term, opt.levenshtein, opt.levenshtein_auto_limit, starts_with))
            };

            let part = RequestSearchPart {
                boost: opt.boost_fields.as_ref().and_then(|boost|boost.get(field_name).map(|el| OrderedFloat(*el))),
                levenshtein_distance,
                path: field_name.to_string(),
                terms: vec![term],
                starts_with: Some(starts_with),
                ..Default::default()
            };
            Request {
                search: Some(part),
                why_found: opt.why_found.unwrap_or(false),
                text_locality: opt.text_locality.unwrap_or(false),
                ..Default::default()
            }
        }
    }
}



fn expand_fields_in_query_ast(ast: UserAST, all_fields: &[String]) -> Result<UserAST, VelociError> {
    match ast {
        UserAST::Clause(op, subqueries) => {
            let subqueries: Result<_, _> = subqueries.into_iter().map(|ast| expand_fields_in_query_ast(ast, all_fields)).collect();
            Ok(UserAST::Clause(op, subqueries?))
        }
        UserAST::Leaf(filter) => {
            if let Some(field_name) = &filter.field_name {
                check_field(&field_name, &all_fields)?;
                Ok(UserAST::Leaf(filter))
            } else {
                let field_queries = all_fields
                    .iter()
                    .map(|field_name| {
                        let filter_with_field = UserFilter {
                            field_name: Some(field_name.to_string()),
                            phrase: filter.phrase.to_string(),
                            levenshtein: filter.levenshtein,
                        };
                        UserAST::Leaf(Box::new(filter_with_field))
                    })
                    .collect();
                Ok(UserAST::Clause(Operator::Or, field_queries))
            }
        }
    }
}


pub(crate) fn terms_for_phrase_from_ast(ast: &UserAST) -> Vec<&String> {
    match ast {
        UserAST::Clause(_, queries) => queries.iter().flat_map(|query| terms_for_phrase_from_ast(query)).collect(),
        UserAST::Leaf(filter) => vec![&filter.phrase],
    }
}


//TODO should be field specific
fn filter_stopwords(query_ast: &mut UserAST, opt: &SearchQueryGeneratorParameters) -> bool {
    match query_ast {
        UserAST::Clause(_, ref mut queries) => {
            queries.drain_filter(|mut query| filter_stopwords(&mut query, opt));
            false
        }
        UserAST::Leaf(ref filter) => {
            if let Some(languages) = opt.stopword_lists.as_ref() {
                languages.iter().any(|lang| stopwords::is_stopword(lang, &filter.phrase.to_lowercase()))
            } else {
                false
            }
        }
    }
}



#[bench]
fn bench_query_to_request(b: &mut test::Bencher) {
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
        let query_ast = parser::query_parser::parse("die drei fragezeigen und das unicorn").unwrap().0;
        ast_to_request(query_ast, &fields, &SearchQueryGeneratorParameters::default()).unwrap()
    })
}



#[test]
fn test_filter_stopwords() {
    let query_ast = parser::query_parser::parse("die erbin").unwrap().0;
    let mut query_ast = query_ast.simplify();
    let mut opt = SearchQueryGeneratorParameters::default();
    opt.stopword_lists = Some(vec!["de".to_string()]);
    filter_stopwords(&mut query_ast, &opt);
    assert_eq!(format!("{:?}", query_ast.simplify()), "\"erbin\"");
}



#[test]
fn test_field_expand() {
    let fields = vec!["Title".to_string(), "Author[].name".to_string()];
    let ast = UserAST::Leaf(Box::new(UserFilter {
        field_name: None,
        phrase: "Fred".to_string(),
        levenshtein: None,
    }));
    let expanded_ast = expand_fields_in_query_ast(ast, &fields).unwrap();
    assert_eq!(format!("{:?}", expanded_ast), "(Title:\"Fred\" OR Author[].name:\"Fred\")");

    let ast = UserAST::Leaf(Box::new(UserFilter {
        field_name: Some("Title".to_string()),
        phrase: "Fred".to_string(),
        levenshtein: None,
    }));
    let expanded_ast = expand_fields_in_query_ast(ast, &fields).unwrap();
    assert_eq!(format!("{:?}", expanded_ast), "Title:\"Fred\"");
}


