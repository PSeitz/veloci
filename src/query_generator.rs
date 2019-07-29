use crate::persistence::TEXTINDEX;
use std::{collections::HashMap, f32, str};

use itertools::Itertools;
// use regex::Regex;

use crate::{
    error::VelociError,
    persistence::Persistence,
    search::{stopwords, *},
    util::*,
};
use ordered_float::OrderedFloat;
use std;

#[cfg(test)]
use crate::test;

// fn get_default_levenshtein(term: &str, levenshtein_auto_limit: usize) -> usize {
//     match term.chars().count() {
//         0..=3 => 0,
//         4..=6 => std::cmp::min(1, levenshtein_auto_limit),
//         _ => std::cmp::min(2, levenshtein_auto_limit),
//     }
// }

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchQueryGeneratorParameters {
    pub search_term: String,
    pub top: Option<usize>,
    pub skip: Option<usize>,
    pub operator: Option<String>,
    pub levenshtein: Option<usize>,
    pub levenshtein_auto_limit: Option<usize>,
    pub facetlimit: Option<usize>,
    pub why_found: Option<bool>,
    pub text_locality: Option<bool>,
    pub boost_queries: Option<Vec<RequestBoostPart>>,
    pub facets: Option<Vec<String>>,
    pub stopword_lists: Option<Vec<String>>,
    pub fields: Option<Vec<String>>,
    pub boost_fields: HashMap<String, f32>,
    pub boost_terms: HashMap<String, f32>,
    pub phrase_pairs: Option<bool>,
    pub explain: Option<bool>,
    pub filter: Option<String>,
    pub select: Option<String>,
    // pub filter: Option<Vec<RequestSearchPart>>,
}

fn get_default_levenshtein(term: &str, levenshtein_auto_limit: usize) -> usize {
    match term.chars().count() {
        0..=2 => 0,
        3..=5 => std::cmp::min(1, levenshtein_auto_limit),
        _ => std::cmp::min(2, levenshtein_auto_limit),
    }
}

fn get_all_search_field_names(persistence: &Persistence, fields: &Option<Vec<String>>) -> Result<Vec<String>, VelociError> {
    let res: Vec<_> = persistence
        .metadata
        .get_all_fields()
        .into_iter()
        .filter(|path| {
            if let Some(ref filter) = *fields {
                return filter.contains(path);
            }
            let path: String = path.add(TEXTINDEX);
            if !persistence.has_token_to_anchor(path) {
                // Index creation for fields may be disabled and therefore be unavailable
                return false;
            }
            true
        })
        .collect();
    if res.is_empty() {
        Err(VelociError::StringError(format!("Did not find any fields for {:?}", fields)))
    } else {
        Ok(res)
    }
}

fn get_levenshteinn(term: &str, levenshtein: Option<usize>, levenshtein_auto_limit: Option<usize>) -> u32 {
    let levenshtein_distance = levenshtein.unwrap_or_else(|| get_default_levenshtein(term, levenshtein_auto_limit.unwrap_or(1)));
    std::cmp::min(levenshtein_distance, term.chars().count() - 1) as u32
}

use parser::query_parser::{Operator, UserAST, UserFilter};
fn expand_fields_in_query_ast(ast: UserAST, all_fields: &[String]) -> UserAST {
    match ast {
        UserAST::Clause(op, subqueries) => {
            let subqueries = subqueries.into_iter().map(|ast| expand_fields_in_query_ast(ast, all_fields)).collect();
            UserAST::Clause(op, subqueries)
        }
        UserAST::Leaf(filter) => {
            if filter.field_name.is_some() {
                UserAST::Leaf(filter) // TODO check if field exists
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
                UserAST::Clause(Operator::Or, field_queries)
            }
        }
    }
}

#[test]
fn test_field_expand() {
    let fields = vec!["Title".to_string(), "Author[].name".to_string()];
    let ast = UserAST::Leaf(Box::new(UserFilter {
        field_name: None,
        phrase: "Fred".to_string(),
        levenshtein: None,
    }));
    let expanded_ast = expand_fields_in_query_ast(ast, &fields);
    assert_eq!(format!("{:?}", expanded_ast), "(Title:\"Fred\" OR Author[].name:\"Fred\")");

    let ast = UserAST::Leaf(Box::new(UserFilter {
        field_name: Some("Title".to_string()),
        phrase: "Fred".to_string(),
        levenshtein: None,
    }));
    let expanded_ast = expand_fields_in_query_ast(ast, &fields);
    assert_eq!(format!("{:?}", expanded_ast), "Title:\"Fred\"");
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

#[test]
fn test_filter_stopwords() {
    let query_ast = parser::query_parser::parse("die erbin").unwrap().0;
    let mut query_ast = query_ast.simplify();
    let mut opt = SearchQueryGeneratorParameters::default();
    opt.stopword_lists = Some(vec!["de".to_string()]);
    filter_stopwords(&mut query_ast, &opt);
    assert_eq!(format!("{:?}", query_ast.simplify()), "\"erbin\"");
}

fn ast_to_request(query_ast: UserAST, all_fields: &[String], opt: &SearchQueryGeneratorParameters) -> Request {
    let mut query_ast = query_ast.simplify();
    filter_stopwords(&mut query_ast, opt);
    query_ast = expand_fields_in_query_ast(query_ast, all_fields);
    let query_ast = query_ast.simplify();
    query_ast_to_request(query_ast, opt)
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
        ast_to_request(query_ast, &fields, &SearchQueryGeneratorParameters::default())
    })
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
            let term = &filter.phrase;

            let levenshtein_distance = if let Some(levenshtein) = filter.levenshtein {
                Some(u32::from(levenshtein))
            } else {
                Some(get_levenshteinn(term, opt.levenshtein, opt.levenshtein_auto_limit))
            };

            let part = RequestSearchPart {
                boost: opt.boost_fields.get(field_name).map(|el| OrderedFloat(*el)),
                levenshtein_distance,
                path: field_name.to_string(),
                terms: vec![term.to_string()],
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

fn terms_for_phrase_from_ast(ast: &UserAST) -> Vec<&String> {
    match ast {
        UserAST::Clause(_, queries) => queries.iter().flat_map(|query| terms_for_phrase_from_ast(query)).collect(),
        UserAST::Leaf(filter) => vec![&filter.phrase],
    }
}

use parser;

pub fn search_query(persistence: &Persistence, mut opt: SearchQueryGeneratorParameters) -> Result<Request, VelociError> {
    // let req = persistence.metadata.fulltext_indices.key
    opt.facetlimit = opt.facetlimit.or(Some(5));
    info_time!("generating search query");

    let all_fields = get_all_search_field_names(&persistence, &opt.fields)?; // all fields with applied field_filter
    let query_ast = parser::query_parser::parse(&opt.search_term).unwrap().0;
    let terms: Vec<String> = terms_for_phrase_from_ast(&query_ast).iter().map(|el| el.to_string()).collect();
    info!("Terms for Phrase{:?}", terms);
    let mut request = ast_to_request(query_ast, &all_fields, &opt);

    let facets_req: Option<Vec<FacetRequest>> = opt.facets.as_ref().map(|facets_fields| {
        facets_fields
            .iter()
            .map(|f| FacetRequest {
                field: f.to_string(),
                top: opt.facetlimit,
            })
            .collect()
    });

    let boost_terms_req: Vec<RequestSearchPart> = opt
        .boost_terms
        .iter()
        .flat_map(|(boost_term, boost_value): (&String, &f32)| {
            let mut boost_term = boost_term.to_string();
            let field_filter: Option<Vec<String>> = if boost_term.contains(':') {
                let mut parts: Vec<String> = boost_term.split(':').map(|el| el.to_string()).collect();
                boost_term = parts.remove(1);
                Some(parts)
            } else {
                None
            };

            get_all_search_field_names(&persistence, &field_filter)
                .unwrap()
                .iter()
                .map(|field_name| RequestSearchPart {
                    path: field_name.to_string(),
                    terms: vec![boost_term.to_string()],
                    boost: Some(OrderedFloat(*boost_value)),
                    ..Default::default()
                })
                .collect::<Vec<_>>()
        })
        .collect();

    let boost_term = if boost_terms_req.is_empty() { None } else { Some(boost_terms_req) };

    if opt.phrase_pairs.unwrap_or(false) && terms.len() >= 2 {
        request.phrase_boosts = Some(generate_phrase_queries_for_searchterm(
            persistence,
            &opt.fields,
            &terms,
            opt.levenshtein,
            opt.levenshtein_auto_limit,
            &opt.boost_fields,
        )?);
    }

    if let Some(filters) = opt.filter.as_ref() {
        let mut params = SearchQueryGeneratorParameters::default();
        params.levenshtein = Some(0);
        let query_ast = parser::query_parser::parse(filters).unwrap().0;
        let filter_request_ast = ast_to_request(query_ast, &all_fields, &params);
        request.filter = Some(Box::new(filter_request_ast));
    }

    request.top = opt.top;
    request.skip = opt.skip;
    request.facets = facets_req;
    request.why_found = opt.why_found.unwrap_or(false);
    request.text_locality = opt.text_locality.unwrap_or(false);
    request.boost_term = boost_term;
    request.boost = opt.boost_queries.clone();
    request.explain = opt.explain.unwrap_or(false);

    Ok(request)
}

pub fn generate_phrase_queries_for_searchterm(
    persistence: &Persistence,
    fields: &Option<Vec<String>>,
    terms: &[String],
    levenshtein: Option<usize>,
    levenshtein_auto_limit: Option<usize>,
    boost_fields: &HashMap<String, f32>,
) -> Result<Vec<RequestPhraseBoost>, VelociError> {
    let mut phase_boost_requests = vec![];
    for (term_a, term_b) in terms.iter().tuple_windows() {
        phase_boost_requests.extend(get_all_search_field_names(&persistence, &fields)?.iter().map(|field_name| RequestPhraseBoost {
            search1: RequestSearchPart {
                path: field_name.to_string(),
                terms: vec![term_a.to_string()],
                boost: boost_fields.get(field_name).map(|el| OrderedFloat(*el)),
                levenshtein_distance: Some(get_levenshteinn(term_a, levenshtein, levenshtein_auto_limit)),
                ..Default::default()
            },
            search2: RequestSearchPart {
                path: field_name.to_string(),
                terms: vec![term_b.to_string()],
                boost: boost_fields.get(field_name).map(|el| OrderedFloat(*el)),
                levenshtein_distance: Some(get_levenshteinn(term_b, levenshtein, levenshtein_auto_limit)),
                ..Default::default()
            },
        }));
    }

    Ok(phase_boost_requests)
}

pub fn suggest_query(
    request: &str,
    persistence: &Persistence,
    mut top: Option<usize>,
    skip: Option<usize>,
    levenshtein: Option<usize>,
    fields: &Option<Vec<String>>,
    levenshtein_auto_limit: Option<usize>,
) -> Result<Request, VelociError> {
    if top.is_none() {
        top = Some(10);
    }
    let requests = get_all_search_field_names(&persistence, &fields)?
        .iter()
        .map(|field_name| {
            let levenshtein_distance = levenshtein.unwrap_or_else(|| get_default_levenshtein(request, levenshtein_auto_limit.unwrap_or(1)));
            let starts_with = Some(true);
            RequestSearchPart {
                path: field_name.to_string(),
                terms: vec![request.to_string()],
                levenshtein_distance: Some(levenshtein_distance as u32),
                starts_with,
                top,
                skip,
                ..Default::default()
            }
        })
        .collect();

    Ok(Request {
        suggest: Some(requests),
        top,
        skip,
        ..Default::default()
    })
}
