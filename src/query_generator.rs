use std::{str, f32};
use std::collections::HashMap;

use regex::Regex;

use persistence::Persistence;
use util::*;
use search::*;
use std;

fn get_default_levenshtein(term: &str, levenshtein_auto_limit:usize) -> usize {
    match term.chars().count() {
        0..=3 => 0,
        4..=7 => std::cmp::min(1, levenshtein_auto_limit),
        _ => std::cmp::min(2, levenshtein_auto_limit), // levenshtein 2 very slow for IC and long texts
    }
}


fn get_all_field_names(persistence: &Persistence, fields: &Option<Vec<String>>) -> Vec<String> { // TODO ADD WARNING IF fields filter all
    persistence
        .meta_data
        .fulltext_indices
        .keys()
        .map(|field| extract_field_name(field))
        .filter(|el| {
            if let &Some(ref filter) = fields {
                return filter.contains(el);
            }
            true
        })
        .collect()
}

pub fn normalize_to_single_space(text: &str) -> String {
    lazy_static! {
        static ref REGEXES:Vec<Regex> = vec![
            Regex::new(r"\s\s+").unwrap() // replace tabs, newlines, double spaces with single spaces
        ];

    }
    let mut new_str = text.to_owned();
    for ref tupl in &*REGEXES {
        new_str = tupl.replace_all(&new_str, " ").into_owned();
    }

    new_str.trim().to_owned()
}



fn replace_all_with_space(s: &mut String, remove:&str){
    while let Some(pos) = s.find(remove) {
        s.splice(pos..=pos + remove.len() - 1, " ");
    }
}


#[cfg_attr(feature = "flame_it", flame)]
pub fn search_query(
    request: &str,
    persistence: &Persistence,
    top: Option<usize>,
    skip: Option<usize>,
    mut operator: Option<String>,
    levenshtein: Option<usize>,
    levenshtein_auto_limit: Option<usize>,
    mut facetlimit: Option<usize>,
    why_found: Option<bool>,
    facets: Option<Vec<String>>,
    fields: Option<Vec<String>>,
    boost_fields: HashMap<String, f32>,
    boost_terms: HashMap<String, f32>,
    // boost_fields_opt: Option<Vec<String>>,
) -> Request {
    // let req = persistence.meta_data.fulltext_indices.key
    facetlimit = facetlimit.or(Some(5));
    info_time!("generating search query");
    let terms: Vec<String> = if operator.is_none() && request.contains(" AND ") {
        operator = Some("and".to_string());

        let mut s = String::from(request);
        replace_all_with_space(&mut s, " AND ");
        s = normalize_to_single_space(&s);
        s.split(" ").map(|el| el.to_string()).collect()
    } else {
        let mut s = String::from(request);
        replace_all_with_space(&mut s, " OR ");
        s = normalize_to_single_space(&s);
        s.split(" ").map(|el| el.to_string()).collect()
    };

    // let terms = request.split(" ").map(|el|el.to_string()).collect::<Vec<&str>>();
    let op = operator.map(|op| op.to_lowercase()).unwrap_or("or".to_string());

    let facets_req: Option<Vec<FacetRequest>> = facets.map(|facets_fields| {
        facets_fields
            .iter()
            .map(|f| FacetRequest {
                field: f.to_string(),
                top: facetlimit,
            })
            .collect()
    });

    let boost_terms_req:Vec<RequestSearchPart> = boost_terms
        .iter()
        .flat_map(|(boost_term, boost_value):(&String, &f32)| {
            let mut boost_term = boost_term.to_string();
            let filter:Option<Vec<String>> = if boost_term.contains(":"){
                let mut parts:Vec<String> = boost_term.split(":").map(|el|el.to_string()).collect();
                boost_term = parts.remove(1);
                Some(parts)
            }else{
                None
            };

            get_all_field_names(&persistence, &filter)
            .iter()
            .map(|field_name| {
                RequestSearchPart {
                    path: field_name.to_string(),
                    terms: vec![boost_term.to_string()],
                    boost: Some(*boost_value),
                    ..Default::default()
                }
            }).collect::<Vec<_>>()

    }).collect();

    let boost_term = if boost_terms_req.is_empty() {None} else{Some(boost_terms_req)};

    if op == "and" {
        let requests: Vec<Request> = terms
            .iter()
            .map(|term| {
                let mut levenshtein_distance = levenshtein.unwrap_or_else(|| get_default_levenshtein(term, levenshtein_auto_limit.unwrap_or(1)));
                levenshtein_distance = std::cmp::min(levenshtein_distance, term.chars().count() - 1 );
                let parts = get_all_field_names(&persistence, &fields)
                    .iter()
                    .map(|field_name| {
                        let part = RequestSearchPart {
                            path: field_name.to_string(),
                            terms: vec![term.to_string()],
                            boost: boost_fields.get(field_name).map(|el| *el),
                            levenshtein_distance: Some(levenshtein_distance as u32),
                            resolve_token_to_parent_hits: Some(true),
                            ..Default::default()
                        };
                        Request {
                            search: Some(part),
                            why_found: why_found.unwrap_or(false),
                            ..Default::default()
                        }
                    })
                    .collect();

                Request {
                    or: Some(parts), // or over fields
                    why_found: why_found.unwrap_or(false),
                    ..Default::default()
                }
            })
            .collect();

        return Request {
            and: Some(requests), // and for terms
            top: top,
            skip: skip,
            boost_term: boost_term,
            facets: facets_req,
            why_found: why_found.unwrap_or(false),
            ..Default::default()
        };
    }

    let parts: Vec<Request> = get_all_field_names(&persistence, &fields)
        .iter()
        .flat_map(|field_name| {
            let requests: Vec<Request> = terms
                .iter()
                .map(|term| {
                    let levenshtein_distance = levenshtein.unwrap_or_else(|| get_default_levenshtein(term, levenshtein_auto_limit.unwrap_or(1)));
                    let part = RequestSearchPart {
                        path: field_name.to_string(),
                        terms: vec![term.to_string()],
                        boost: boost_fields.get(field_name).map(|el| *el),
                        levenshtein_distance: Some(levenshtein_distance as u32),
                        resolve_token_to_parent_hits: Some(true),
                        ..Default::default()
                    };
                    Request {
                        search: Some(part),
                        why_found: why_found.unwrap_or(false),
                        ..Default::default()
                    }
                })
                .collect();

            requests
        })
        .collect();


    Request {
        or: Some(parts),
        top: top,
        skip: skip,
        facets: facets_req,
        why_found: why_found.unwrap_or(false),
        boost_term: boost_term,
        ..Default::default()
    }
}



pub fn suggest_query(
    request: &str,
    persistence: &Persistence,
    mut top: Option<usize>,
    skip: Option<usize>,
    levenshtein: Option<usize>,
    fields: Option<Vec<String>>,
    levenshtein_auto_limit: Option<usize>,
) -> Request {
    // let req = persistence.meta_data.fulltext_indices.key

    if top.is_none() {
        top = Some(10);
    }
    // if skip.is_none() {top = Some(0); }

    let requests = get_all_field_names(&persistence, &fields)
        .iter()
        .map(|field_name| {
            let levenshtein_distance = levenshtein.unwrap_or_else(|| get_default_levenshtein(request, levenshtein_auto_limit.unwrap_or(1)));
            let starts_with = if request.chars().count() <= 3 { None } else { Some(true) };
            RequestSearchPart {
                path: field_name.to_string(),
                terms: vec![request.to_string()],
                levenshtein_distance: Some(levenshtein_distance as u32),
                starts_with: starts_with,
                top: top,
                skip: skip,
                ..Default::default()
            }
        })
        .collect();

    Request {
        suggest: Some(requests),
        top: top,
        skip: skip,
        ..Default::default()
    }
}