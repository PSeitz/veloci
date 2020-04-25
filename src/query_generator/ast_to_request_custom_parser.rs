
use crate::query_generator::*;
use crate::search::request::Request;
use crate::error::VelociError;
use custom_parser;
use custom_parser::ast::{Operator, UserAST};
#[allow(dead_code)]
pub(crate) fn ast_to_request(query_ast: UserAST<'_, '_>, all_fields: &[String], opt: &SearchQueryGeneratorParameters) -> Result<Request, VelociError> {
    // let mut query_ast = query_ast.simplify();
    filter_stopwords(&query_ast, opt);
    let query_ast = expand_fields_in_query_ast(query_ast, all_fields)?;
    // // let query_ast = query_ast.simplify();
    Ok(query_ast_to_request(&query_ast, opt, None))
    // unreachable!()
}
#[allow(dead_code)]
fn query_ast_to_request<'a>(ast: &UserAST<'_, '_>, opt: &SearchQueryGeneratorParameters, field_name: Option<&'a str>) -> Request {
    match ast {
        UserAST::BinaryClause(ast1, op, ast2) => {
            let subqueries = [ast1, ast2].iter().map(|ast| query_ast_to_request(ast, opt, field_name)).collect();
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
        UserAST::Attributed(attr, ast) => {
            query_ast_to_request(ast, opt, Some(attr))
        }
        UserAST::Leaf(filter) => {
            let field_name: &str = field_name.as_ref().unwrap();
            let mut term = filter.phrase;

            let starts_with = term.ends_with("*");
            if term.ends_with("*") {
                // term.pop();
                term = &term[..term.len()-1];
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
                terms: vec![term.to_string()],
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

#[allow(dead_code)]
fn expand_fields_in_query_ast<'a,'b>(ast: UserAST<'b, 'a>, all_fields: &'a [String]) -> Result<UserAST<'b, 'a>, VelociError> {
    match ast {
        UserAST::BinaryClause(ast1, op, ast2) => {
            Ok(UserAST::BinaryClause(expand_fields_in_query_ast(*ast1, all_fields)?.into(), op, expand_fields_in_query_ast(*ast2, all_fields)?.into()))
        }
        UserAST::Leaf(_) => {
            let mut field_iter = all_fields.iter();
            let mut curr_ast = field_iter.next().map(|field_name|UserAST::Attributed(
                field_name,
                Box::new(ast.clone())
            )).unwrap();

            for field_name in field_iter {
                let next_ast = UserAST::Attributed(
                    field_name,
                    Box::new(ast.clone())
                );
                curr_ast = UserAST::BinaryClause(next_ast.into(), Operator::Or, curr_ast.into());
            }

            Ok(curr_ast)
        }
        UserAST::Attributed(_, _) => { // dont expand in UserAST::Attributed
            Ok(ast)
        }
    }
}

// pub(crate) fn terms_for_phrase_from_ast<'a>(ast: &UserAST<'_>) -> Vec<&'a String> {
//     match ast {
//         UserAST::Clause(_, queries) => queries.iter().flat_map(|query| terms_for_phrase_from_ast(query)).collect(),
//         UserAST::Leaf(filter) => vec![&filter.phrase],
//     }
// }


//TODO should be field specific
// fn filter_stopwords(query_ast: &mut UserAST<'_>, opt: &SearchQueryGeneratorParameters) -> bool {
//     match query_ast {
//         UserAST::BinaryClause(ref ast1, op, ref ast2) => {
//             // queries.drain_filter(|mut query| filter_stopwords(&mut query, opt));
//             false
//         }
//         UserAST::Leaf(ref filter) => {
//             if let Some(languages) = opt.stopword_lists.as_ref() {
//                 languages.iter().any(|lang| stopwords::is_stopword(lang, &filter.phrase.to_lowercase()))
//             } else {
//                 false
//             }
//         }
//     }
// }
#[allow(dead_code)]
fn filter_stopwords<'a, 'b>(query_ast: &'a custom_parser::ast::UserAST<'a, 'a>, opt: &'b SearchQueryGeneratorParameters) -> Option<UserAST<'a, 'a>> {
    let ast = query_ast.filter_ast(&mut |ast: &UserAST<'_,'_>, _attr: Option<&str>|  {
            match ast {
                UserAST::Leaf(filter) => {
                    if let Some(languages) = opt.stopword_lists.as_ref() {
                        languages.iter().any(|lang| stopwords::is_stopword(lang, &filter.phrase.to_lowercase()))
                    } else {
                        false
                    }
                }
                _ => false,
            }
        }, None);
    ast
}



#[bench]
fn bench_custom_parse_to_request(b: &mut test::Bencher) {
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
        let query_ast = custom_parser::parse("die drei fragezeigen und das unicorn").unwrap();
        ast_to_request(query_ast, &fields, &SearchQueryGeneratorParameters::default()).unwrap()
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
        let query_ast = custom_parser::parse("die drei fragezeigen und das unicorn").unwrap();
        expand_fields_in_query_ast(query_ast, &fields).unwrap()
    })
}



#[test]
fn test_filter_stopwords() {
    let query_ast = custom_parser::parse("die erbin").unwrap();
    // let mut query_ast = query_ast.simplify();
    let mut opt = SearchQueryGeneratorParameters::default();
    opt.stopword_lists = Some(vec!["de".to_string()]);
    let query_ast = filter_stopwords(&query_ast, &opt);
    assert_eq!(query_ast, Some("erbin".into()));
}



#[test]
fn test_field_expand() {
    use custom_parser::ast::{UserFilter};
    let fields = vec!["Title".to_string(), "Author[].name".to_string()];
    let ast = UserAST::Leaf(Box::new(UserFilter {
        phrase: "Fred",
        levenshtein: None,
    }));
    let expanded_ast = expand_fields_in_query_ast(ast, &fields).unwrap();
    assert_eq!(format!("{:?}", expanded_ast), "(Author[].name:\"Fred\" OR Title:\"Fred\")");

    let ast =  UserAST::Attributed("Title", UserAST::Leaf(Box::new(UserFilter {
        phrase: "Fred",
        levenshtein: None,
    })).into());
    let expanded_ast = expand_fields_in_query_ast(ast, &fields).unwrap();
    assert_eq!(format!("{:?}", expanded_ast), "Title:\"Fred\"");
}

