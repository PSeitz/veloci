mod ast_to_request_custom_parser;
mod ast_to_request;
use ast_to_request::*;
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

// fn get_default_levenshtein(term: &str, levenshtein_auto_limit: usize) -> usize {
//     match term.chars().count() {
//         0..=3 => 0,
//         4..=6 => std::cmp::min(1, levenshtein_auto_limit),
//         _ => std::cmp::min(2, levenshtein_auto_limit),
//     }
// }

/// SearchQueryGeneratorParameters is convience layer to generatre requests.
///
/// `SearchQueryGeneratorParameters` provides defaults for a lot of search cases,
/// which can be hard to generate in a query. For example searching on all fields or generating phrase boosts.
///
/// The method `search_query` does the conversion from `SearchQueryGeneratorParameters` to Request.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchQueryGeneratorParameters {
    pub search_term: String,
    pub top: Option<usize>,
    pub skip: Option<usize>,
    pub operator: Option<String>,
    pub levenshtein: Option<usize>, // TODO, it's called levenshtein here, but levenshtein_distance in the request.

    /// Terms will have an automatic levenshtein assigned depending on their length, with levenshtein_auto_limit it's possible to limit the levenshtein distance
    /// This should be replaced by a mor powerful api
    /// 0..=2 => 0, 3..=5 => 1 or levenshtein_auto_limit, other = 2 or levenshtein_auto_limit
    pub levenshtein_auto_limit: Option<usize>,
    pub facetlimit: Option<usize>,
    pub why_found: Option<bool>,
    pub text_locality: Option<bool>,
    pub boost_queries: Option<Vec<RequestBoostPart>>,
    pub facets: Option<Vec<String>>,
    pub stopword_lists: Option<Vec<String>>,
    pub fields: Option<Vec<String>>,
    pub boost_fields: Option<HashMap<String, f32>>,

    /// format is term:field_name(optional)->boost_value
    /// city:berlin->2.0
    pub boost_terms: Option<HashMap<String, f32>>,
    pub phrase_pairs: Option<bool>,
    pub explain: Option<bool>,
    pub filter: Option<String>,
    pub select: Option<String>,
    // pub filter: Option<Vec<RequestSearchPart>>,
}

fn get_default_levenshtein(term: &str, levenshtein_auto_limit: usize, wildcard: bool) -> usize {
    if wildcard{
        match term.chars().count() {
            0..=3 => 0,
            4..=5 => std::cmp::min(1, levenshtein_auto_limit),
            _ => std::cmp::min(2, levenshtein_auto_limit),
        }
    }else{
        match term.chars().count() {
            0..=2 => 0,
            3..=5 => std::cmp::min(1, levenshtein_auto_limit),
            _ => std::cmp::min(2, levenshtein_auto_limit),
        }
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
        Err(VelociError::AllFieldsFiltered {
            all_fields: persistence.metadata.get_all_fields(),
            filter: fields.to_owned(),
        })
    } else {
        Ok(res)
    }
}

fn get_levenshteinn(term: &str, levenshtein: Option<usize>, levenshtein_auto_limit: Option<usize>, wildcard: bool) -> u32 {
    let levenshtein_distance = levenshtein.unwrap_or_else(|| get_default_levenshtein(term, levenshtein_auto_limit.unwrap_or(1), wildcard));
    std::cmp::min(levenshtein_distance, term.chars().count() - 1) as u32
}




fn check_field(field: &String, all_fields: &[String]) -> Result<(), VelociError> {
    if !all_fields.contains(field) {
        Err(VelociError::FieldNotFound {
            field: field.to_string(),
            all_fields: all_fields.to_vec(),
        })
    } else {
        Ok(())
    }
}


/// format is term:field_name(optional)->boost_value
/// city:berlin->2.0
pub fn handle_boost_term_query(persistence: &Persistence, boost_term: &str, boost_value: &f32) -> Vec<RequestSearchPart> {
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
}

pub fn search_query(persistence: &Persistence, mut opt: SearchQueryGeneratorParameters) -> Result<Request, VelociError> {
    // let req = persistence.metadata.fulltext_indices.key
    opt.facetlimit = opt.facetlimit.or(Some(5));
    info_time!("generating search query");

    let all_fields = persistence.metadata.get_all_fields();
    let all_search_fields = get_all_search_field_names(&persistence, &opt.fields)?; // all fields with applied field_filter
    let query_ast = parser::query_parser::parse(&opt.search_term).unwrap().0;
    let terms: Vec<String> = terms_for_phrase_from_ast(&query_ast).iter().map(|el| el.to_string()).collect();
    info!("Terms for Phrase{:?}", terms);
    let mut request = Request::default();
    request.search_req = Some(ast_to_request::ast_to_request(query_ast, &all_search_fields, &opt)?);

    let facetlimit = opt.facetlimit;

    let facets_req: Option<Result<Vec<FacetRequest>, _>> = opt.facets.map(|facets_fields| {
        facets_fields
            .into_iter()
            .map(|field| {
                check_field(&field, &all_fields)?;
                Ok(FacetRequest { field, top: facetlimit })
            })
            .collect::<Result<Vec<FacetRequest>, VelociError>>()
    });

    let facets_req = facets_req.map_or(Ok(None), |r| r.map(Some))?;

    let boost_term = opt
        .boost_terms
        .and_then(|boosts:HashMap<String, f32>|{
            let requests = boosts.iter()
            .flat_map(|(boost_term, boost_value): (&String, &f32)| {
                handle_boost_term_query(persistence, boost_term, boost_value)
            })
            .collect::<Vec<RequestSearchPart>>();
            Some(requests)
        });


    // let boost_term = if boost_terms_req.is_empty() { None } else { Some(boost_terms_req) };

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
        let filter_request_ast = ast_to_request::ast_to_request(query_ast, &all_fields, &params)?;
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
    boost_fields: &Option<HashMap<String, f32>>,
) -> Result<Vec<RequestPhraseBoost>, VelociError> {
    let mut phase_boost_requests = vec![];
    for (term_a, term_b) in terms.iter().tuple_windows() {
        phase_boost_requests.extend(get_all_search_field_names(&persistence, &fields)?.iter().map(|field_name| RequestPhraseBoost {
            search1: RequestSearchPart {
                path: field_name.to_string(),
                terms: vec![term_a.to_string()],
                boost: boost_fields.as_ref().and_then(|boost_fields|boost_fields.get(field_name).map(|el| OrderedFloat(*el))),
                levenshtein_distance: Some(get_levenshteinn(term_a, levenshtein, levenshtein_auto_limit, false)),
                ..Default::default()
            },
            search2: RequestSearchPart {
                path: field_name.to_string(),
                terms: vec![term_b.to_string()],
                boost: boost_fields.as_ref().and_then(|boost_fields|boost_fields.get(field_name).map(|el| OrderedFloat(*el))),
                levenshtein_distance: Some(get_levenshteinn(term_b, levenshtein, levenshtein_auto_limit, false)),
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
            let levenshtein_distance = levenshtein.unwrap_or_else(|| get_default_levenshtein(request, levenshtein_auto_limit.unwrap_or(1), true));
            RequestSearchPart {
                path: field_name.to_string(),
                terms: vec![request.to_string()],
                levenshtein_distance: Some(levenshtein_distance as u32),
                starts_with: true,
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
