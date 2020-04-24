
use crate::query_generator::*;
use crate::search::request::Request;
use crate::error::VelociError;
use custom_parser;
use custom_parser::ast::{Operator, UserAST, UserFilter};

pub(crate) fn ast_to_request(query_ast: UserAST<'_>, all_fields: &[String], opt: &SearchQueryGeneratorParameters) -> Result<Request, VelociError> {
    // let mut query_ast = query_ast.simplify();
    filter_stopwords(&query_ast, opt);
    let query_ast = expand_fields_in_query_ast(query_ast, all_fields)?;
    // // let query_ast = query_ast.simplify();
    // Ok(query_ast_to_request(query_ast, opt))
    unreachable!()
}

// fn query_ast_to_request<'a>(ast: UserAST<'a>, opt: &SearchQueryGeneratorParameters, field_name: Option<&'a str>) -> Request {
//     match ast {
//         UserAST::Clause(op, subqueries) => {
//             let subqueries = subqueries.into_iter().map(|ast| query_ast_to_request(ast, opt)).collect();
//             match op {
//                 Operator::And => Request {
//                     and: Some(subqueries),
//                     ..Default::default()
//                 },
//                 Operator::Or => Request {
//                     or: Some(subqueries),
//                     ..Default::default()
//                 },
//             }
//         }
//         UserAST::Leaf(filter) => {
//             let field_name = filter.field_name.as_ref().unwrap();
//             let mut term = filter.phrase;

//             let starts_with = term.ends_with("*");
//             if term.ends_with("*") {
//                 term.pop();
//             }
//             let levenshtein_distance = if let Some(levenshtein) = filter.levenshtein {
//                 Some(u32::from(levenshtein))
//             } else {
//                 Some(get_levenshteinn(&term, opt.levenshtein, opt.levenshtein_auto_limit, starts_with))
//             };

//             let part = RequestSearchPart {
//                 boost: opt.boost_fields.as_ref().and_then(|boost|boost.get(field_name).map(|el| OrderedFloat(*el))),
//                 levenshtein_distance,
//                 path: field_name.to_string(),
//                 terms: vec![term],
//                 starts_with: Some(starts_with),
//                 ..Default::default()
//             };
//             Request {
//                 search: Some(part),
//                 why_found: opt.why_found.unwrap_or(false),
//                 text_locality: opt.text_locality.unwrap_or(false),
//                 ..Default::default()
//             }
//         }
//     }
// }


fn expand_fields_in_query_ast<'a,'b>(ast: UserAST<'b>, all_fields: &'a [String]) -> Result<UserAST<'b>, VelociError> {
    match ast {
        UserAST::Leaf(filter) => {
            Ok(UserAST::Leaf(filter))
            // if let Some(field_name) = &filter.field_name {
            //     check_field(&field_name, &all_fields)?;
            //     Ok(UserAST::Leaf(filter))
            // } else {
            //     let field_queries = all_fields
            //         .iter()
            //         .map(|field_name| {
            //             let filter_with_field = UserFilter {
            //                 field_name: Some(field_name.to_string()),
            //                 phrase: filter.phrase.to_string(),
            //                 levenshtein: filter.levenshtein,
            //             };
            //             UserAST::Leaf(Box::new(filter_with_field))
            //         })
            //         .collect();
            //     Ok(UserAST::Clause(Operator::Or, field_queries))
            // }
        }
        _ => {
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

fn filter_stopwords<'a, 'b>(query_ast: &'a custom_parser::ast::UserAST<'a>, opt: &'b SearchQueryGeneratorParameters) -> Option<UserAST<'a>> {
    let ast = query_ast.filter_ast(&mut |ast: &UserAST<'_>, _attr: Option<&str>|  {
            match ast {
                UserAST::Leaf(filter) => {
                    if let Some(languages) = opt.stopword_lists.as_ref() {
                        languages.iter().any(|lang| stopwords::is_stopword(lang, &filter.phrase.to_lowercase()))
                    } else {
                        false
                    }
                    // filter.phrase == "cool"
                }
                _ => false,
            }
        }, None);
    ast
    // match query_ast {
    //     UserAST::BinaryClause(ref ast1, op, ref ast2) => {
    //         // queries.drain_filter(|mut query| filter_stopwords(&mut query, opt));
    //         false
    //     }
    //     UserAST::Leaf(ref filter) => {
    //         if let Some(languages) = opt.stopword_lists.as_ref() {
    //             languages.iter().any(|lang| stopwords::is_stopword(lang, &filter.phrase.to_lowercase()))
    //         } else {
    //             false
    //         }
    //     }
    // }
}



// #[bench]
// fn bench_query_to_request(b: &mut test::Bencher) {
//     let fields = vec![
//         "Title".to_string(),
//         "Author".to_string(),
//         "Author1".to_string(),
//         "Author2".to_string(),
//         "Author3".to_string(),
//         "Author4".to_string(),
//         "Author5".to_string(),
//         "Author6".to_string(),
//         "Author7".to_string(),
//         "Author8".to_string(),
//         "Author9".to_string(),
//         "Author10".to_string(),
//         "Author11".to_string(),
//         "Author12".to_string(),
//         "Author13".to_string(),
//     ];
//     b.iter(|| {
//         let query_ast = parser::query_parser::parse("die drei fragezeigen und das unicorn").unwrap().0;
//         ast_to_request(query_ast, &fields, &SearchQueryGeneratorParameters::default()).unwrap()
//     })
// }



#[test]
fn test_filter_stopwords() {
    let query_ast = custom_parser::parse("die erbin").unwrap();
    // let mut query_ast = query_ast.simplify();
    let mut opt = SearchQueryGeneratorParameters::default();
    opt.stopword_lists = Some(vec!["de".to_string()]);
    let query_ast = filter_stopwords(&query_ast, &opt);
    assert_eq!(query_ast, Some("erbin".into()));
}



// #[test]
// fn test_field_expand() {
//     let fields = vec!["Title".to_string(), "Author[].name".to_string()];
//     let ast = UserAST::Leaf(Box::new(UserFilter {
//         field_name: None,
//         phrase: "Fred".to_string(),
//         levenshtein: None,
//     }));
//     let expanded_ast = expand_fields_in_query_ast(ast, &fields).unwrap();
//     assert_eq!(format!("{:?}", expanded_ast), "(Title:\"Fred\" OR Author[].name:\"Fred\")");

//     let ast = UserAST::Leaf(Box::new(UserFilter {
//         field_name: Some("Title".to_string()),
//         phrase: "Fred".to_string(),
//         levenshtein: None,
//     }));
//     let expanded_ast = expand_fields_in_query_ast(ast, &fields).unwrap();
//     assert_eq!(format!("{:?}", expanded_ast), "Title:\"Fred\"");
// }


