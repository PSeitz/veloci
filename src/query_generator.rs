use std::collections::HashMap;
use std::{f32, str};

use itertools::Itertools;
use regex::Regex;

use persistence::Persistence;
use search::*;
use std;
use util::*;
use ordered_float::OrderedFloat;

fn get_default_levenshtein(term: &str, levenshtein_auto_limit: usize) -> usize {
    match term.chars().count() {
        0..=3 => 0,
        4..=6 => std::cmp::min(1, levenshtein_auto_limit),
        _ => std::cmp::min(2, levenshtein_auto_limit),
    }
}

fn get_all_field_names(persistence: &Persistence, fields: &Option<Vec<String>>) -> Vec<String> {
    // TODO ADD WARNING IF fields filter all
    persistence
        .meta_data
        .fulltext_indices
        .keys()
        .map(|field| extract_field_name(field))
        .filter(|el| {
            if let Some(ref filter) = *fields {
                return filter.contains(el);
            }
            true
        })
        .collect()
}

fn normalize_to_single_space(text: &str) -> String {
    lazy_static! {
        static ref REGEXES:Vec<Regex> = vec![
            Regex::new(r"\s\s+").unwrap() // replace tabs, newlines, double spaces with single spaces
        ];

    }
    let mut new_str = text.to_owned();
    for tupl in REGEXES.iter() {
        new_str = tupl.replace_all(&new_str, " ").into_owned();
    }

    new_str.trim().to_owned()
}

fn replace_all_with_space(s: &mut String, remove: &str) {
    while let Some(pos) = s.find(remove) {
        s.replace_range(pos..pos + remove.len(), " ");
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
    pub fields: Option<Vec<String>>,
    pub boost_fields: HashMap<String, f32>,
    pub boost_terms: HashMap<String, f32>,
    pub phrase_pairs: Option<bool>,
}

fn get_levenshteinn(term:&str, levenshtein: Option<usize>, levenshtein_auto_limit: Option<usize>) -> u32{
    let levenshtein_distance = levenshtein.unwrap_or_else(|| get_default_levenshtein(term, levenshtein_auto_limit.unwrap_or(1)));
    std::cmp::min(levenshtein_distance, term.chars().count() - 1) as u32
}

fn tokenize_term(term: &str) -> Vec<String> {
    let s = normalize_to_single_space(term);
    s.split(' ').map(|el| el.to_string()).collect()
}

#[cfg_attr(feature = "flame_it", flame)]
pub fn search_query(persistence: &Persistence, mut opt: SearchQueryGeneratorParameters) -> Request {
    // let req = persistence.meta_data.fulltext_indices.key
    opt.facetlimit = opt.facetlimit.or(Some(5));
    info_time!("generating search query");
    let terms: Vec<String> = if opt.operator.is_none() && opt.search_term.contains(" AND ") {
        opt.operator = Some("and".to_string());
        let mut s = opt.search_term.to_string();
        replace_all_with_space(&mut s, " AND ");
        tokenize_term(&s)
    } else {
        let mut s = opt.search_term.to_string();
        replace_all_with_space(&mut s, " OR ");
        tokenize_term(&s)
    };

    // let terms = opt.search_term.split(" ").map(|el|el.to_string()).collect::<Vec<&str>>();
    let op = opt.operator.as_ref().map(|op| op.to_lowercase()).unwrap_or_else(|| "or".to_string());

    // Add phrase pairs
    // if opt.phrase_pairs.unwrap_or(false) && op == "or".to_string() && terms.len() >= 2 {
    //     for (term_a, term_b) in terms.clone().iter().tuple_windows() {
    //         terms.push(term_a.to_string() + term_b);
    //     }
    // }

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
            let filter: Option<Vec<String>> = if boost_term.contains(':') {
                let mut parts: Vec<String> = boost_term.split(':').map(|el| el.to_string()).collect();
                boost_term = parts.remove(1);
                Some(parts)
            } else {
                None
            };

            get_all_field_names(&persistence, &filter)
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

    let get_levenshtein = |term: &str| -> u32{
        get_levenshteinn(term, opt.levenshtein.clone(), opt.levenshtein_auto_limit.clone())
        // let levenshtein_distance = opt.levenshtein.unwrap_or_else(|| get_default_levenshtein(term, opt.levenshtein_auto_limit.unwrap_or(1)));
        // std::cmp::min(levenshtein_distance, term.chars().count() - 1) as u32
    };

    let mut request = if op == "and" {
        let requests: Vec<Request> = terms
            .iter()
            .map(|term| {
                let parts = get_all_field_names(&persistence, &opt.fields)
                    .iter()
                    .map(|field_name| {
                        let part = RequestSearchPart {
                            path: field_name.to_string(),
                            terms: vec![term.to_string()],
                            boost: opt.boost_fields.get(field_name).map(|el|OrderedFloat(*el)),
                            levenshtein_distance: Some(get_levenshtein(term)),
                            ..Default::default()
                        };
                        Request {
                            search: Some(part),
                            why_found: opt.why_found.unwrap_or(false),
                            text_locality: opt.text_locality.unwrap_or(false),
                            ..Default::default()
                        }
                    })
                    .collect();

                Request {
                    or: Some(parts), // or over fields
                    why_found: opt.why_found.unwrap_or(false),
                    text_locality: opt.text_locality.unwrap_or(false),
                    ..Default::default()
                }
            })
            .collect();

        Request {
            and: Some(requests), // and for terms
            ..Default::default()
        }
    } else {
        let parts: Vec<Request> = get_all_field_names(&persistence, &opt.fields)
            .iter()
            .flat_map(|field_name| {
                let requests: Vec<Request> = terms
                    .iter()
                    .map(|term| {
                        let part = RequestSearchPart {
                            path: field_name.to_string(),
                            terms: vec![term.to_string()],
                            boost: opt.boost_fields.get(field_name).map(|el|OrderedFloat(*el)),
                            levenshtein_distance: Some(get_levenshtein(term)),
                            ..Default::default()
                        };
                        Request {
                            search: Some(part),
                            why_found: opt.why_found.unwrap_or(false),
                            text_locality: opt.text_locality.unwrap_or(false),
                            ..Default::default()
                        }
                    })
                    .collect();

                requests
            })
            .collect();
        Request {
            or: Some(parts),
            ..Default::default()
        }
    };

    if opt.phrase_pairs.unwrap_or(false) && terms.len() >= 2 {
        request.phrase_boosts = Some(generate_phrase_queries_for_searchterm(persistence, &opt.fields, &terms, opt.levenshtein, opt.levenshtein_auto_limit, opt.boost_fields.clone()));
    }

    request.top = opt.top;
    request.skip = opt.skip;
    request.facets = facets_req;
    request.why_found = opt.why_found.unwrap_or(false);
    request.text_locality = opt.text_locality.unwrap_or(false);
    request.boost_term = boost_term;

    // Request {
    //     or: Some(parts),
    //     top: opt.top,
    //     skip: opt.skip,
    //     facets: facets_req,
    //     why_found: opt.why_found.unwrap_or(false),
    //     boost_term: boost_term,
    //     ..Default::default()
    // }

    request
}

pub fn generate_phrase_queries_for_searchterm(persistence: &Persistence, fields: &Option<Vec<String>>, terms: &Vec<String>, levenshtein: Option<usize>, levenshtein_auto_limit: Option<usize>, boost_fields: HashMap<String, f32>) -> Vec<RequestPhraseBoost> {
    let mut phase_boost_requests = vec![];
    for (term_a, term_b) in terms.clone().iter().tuple_windows() {

        phase_boost_requests.extend(get_all_field_names(&persistence, &fields).iter()
            .map(|field_name| {
                RequestPhraseBoost {
                    search1: RequestSearchPart {
                        path: field_name.to_string(),
                        terms: vec![term_a.to_string()],
                        boost: boost_fields.get(field_name).map(|el|OrderedFloat(*el)),
                        levenshtein_distance: Some(get_levenshteinn(term_a, levenshtein, levenshtein_auto_limit)),
                        ..Default::default()
                    },
                    search2: RequestSearchPart {
                        path: field_name.to_string(),
                        terms: vec![term_b.to_string()],
                        boost: boost_fields.get(field_name).map(|el|OrderedFloat(*el)),
                        levenshtein_distance: Some(get_levenshteinn(term_b, levenshtein, levenshtein_auto_limit)),
                        ..Default::default()
                    }
                }
            })
        );
    }

    phase_boost_requests
}

pub fn suggest_query(
    request: &str,
    persistence: &Persistence,
    mut top: Option<usize>,
    skip: Option<usize>,
    levenshtein: Option<usize>,
    fields: &Option<Vec<String>>,
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
                starts_with,
                top,
                skip,
                ..Default::default()
            }
        })
        .collect();

    Request {
        suggest: Some(requests),
        top,
        skip,
        ..Default::default()
    }
}
