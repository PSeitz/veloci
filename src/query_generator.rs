mod query_parser_to_veloci_request;
use crate::persistence::TEXTINDEX;
use query_parser_to_veloci_request::*;
use std::{
    collections::{HashMap, HashSet},
    f32, str,
};

use crate::{
    error::VelociError,
    persistence::Persistence,
    search::{stopwords, *},
    util::*,
};
use ordered_float::OrderedFloat;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct QueryParserOptions {
    /// This setting will disable parsing of the attribute specfier "attr:"
    /// e.g. "myfield:searchterm"
    pub no_attributes: bool,
    /// This setting will disable parsing of the parentheses
    /// e.g. "(nice)" - here the parentheses would be normally be part of the syntax and removed
    pub no_parentheses: bool,
    /// This setting will disable defining a levensthtein distance after a searchterm
    /// e.g. "searchterm~2"
    pub no_levensthein: bool,
    // pub no_quotes: bool
}

impl From<QueryParserOptions> for query_parser::Options {
    fn from(options: QueryParserOptions) -> Self {
        query_parser::Options {
            no_attributes: options.no_attributes,
            no_parentheses: options.no_parentheses,
            no_levensthein: options.no_levensthein,
        }
    }
}

/// SearchQueryGeneratorParameters is convience layer to generatre requests.
///
/// `SearchQueryGeneratorParameters` provides defaults for a lot of search cases,
/// which can be hard to generate in a query. For example searching on all fields or generating phrase boosts.
///
/// The method `search_query` does the conversion from `SearchQueryGeneratorParameters` to Request.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchQueryGeneratorParameters {
    /// The query language search searm will be parsed into an ast. Some settings can be controlled with parser_options
    pub search_term: String,
    pub parser_options: Option<QueryParserOptions>,

    pub top: Option<usize>,
    pub skip: Option<usize>,
    pub ignore_case: Option<bool>,
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
    pub stopwords: Option<HashSet<String>>,
    pub fields: Option<Vec<String>>,
    pub boost_fields: Option<HashMap<String, f32>>,

    /// format is term:field_name(optional)->boost_value
    /// city:berlin->2.0
    pub boost_terms: Option<HashMap<String, f32>>,
    pub phrase_pairs: Option<bool>,
    pub explain: Option<bool>,
    pub filter: Option<String>,
    pub filter_parser_options: Option<QueryParserOptions>,
    pub select: Option<String>,
    // pub filter: Option<Vec<RequestSearchPart>>,
}

fn get_default_levenshtein(term: &str, levenshtein_auto_limit: usize, wildcard: bool) -> usize {
    if wildcard {
        match term.chars().count() {
            0..=3 => 0,
            4..=5 => std::cmp::min(1, levenshtein_auto_limit),
            _ => std::cmp::min(2, levenshtein_auto_limit),
        }
    } else {
        match term.chars().count() {
            0..=2 => 0,
            3..=5 => std::cmp::min(1, levenshtein_auto_limit),
            _ => std::cmp::min(2, levenshtein_auto_limit),
        }
    }
}

/// get all fields, while applying the fields whitelist if applicable
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

fn check_field(field: &str, all_fields: &[String]) -> Result<(), VelociError> {
    // if !all_fields.contains(field) {   // https://github.com/rust-lang/rust/issues/42671  Vec::contains is too restrictive
    if !all_fields.iter().any(|x| x == field) {
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

    get_all_search_field_names(persistence, &field_filter)
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

/// Takes `SearchQueryGeneratorParameters` and generates a `Request` according to the settings
///
/// There is a lot of implicit logic in this method
/// For better configurability, it should be splitted in a multiple utility functions
///
pub fn search_query(persistence: &Persistence, mut opt: SearchQueryGeneratorParameters) -> Result<Request, VelociError> {
    opt.facetlimit = opt.facetlimit.or(Some(5));
    info_time!("generating search query");

    let all_fields = persistence.metadata.get_all_fields();
    let all_search_fields = get_all_search_field_names(persistence, &opt.fields)?; // all fields with applied field_filter

    let parser_options: QueryParserOptions = opt.parser_options.unwrap_or_default();
    let query_ast = query_parser::parse_with_opt(&opt.search_term, parser_options.into()).unwrap();

    let mut request = Request::default();

    request.search_req = Some(ast_to_search_request(&query_ast, &all_search_fields, &opt)?);
    if let Some(el) = request.search_req.as_mut() {
        el.simplify()
    }

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

    let boost_term = opt.boost_terms.map(|boosts: HashMap<String, f32>| {
        let requests = boosts
            .iter()
            .flat_map(|(boost_term, boost_value): (&String, &f32)| handle_boost_term_query(persistence, boost_term, boost_value))
            .collect::<Vec<RequestSearchPart>>();
        requests
    });

    let terms: HashSet<[&str; 2]> = query_ast.get_phrase_pairs();
    info!("Terms for Phrase{:?}", terms);
    if opt.phrase_pairs.unwrap_or(false) && !terms.is_empty() {
        request.phrase_boosts = Some(generate_phrase_queries_for_searchterm(
            persistence,
            &opt.fields,
            terms,
            opt.levenshtein,
            opt.levenshtein_auto_limit,
            &opt.boost_fields,
        )?);
    }

    if let Some(filters) = opt.filter.as_ref() {
        let mut params = SearchQueryGeneratorParameters::default();
        params.levenshtein = Some(0);
        let query_ast = query_parser::parse_with_opt(filters, opt.filter_parser_options.unwrap_or_default().into()).unwrap();
        let mut filter_request_ast = ast_to_search_request(&query_ast, &all_fields, &params)?;
        filter_request_ast.simplify();
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

/// Generates Phrase Boosts queries for adjoined terms on selected fields
///
pub fn generate_phrase_queries_simple(persistence: &Persistence, terms: &[&str], fields: Vec<String>) -> Result<Vec<RequestPhraseBoost>, VelociError> {
    let terms: HashSet<[&str; 2]> = terms.windows(2).map(|window| [window[0], window[1]]).collect();
    generate_phrase_queries_for_searchterm(persistence, &Some(fields), terms, Some(0), Some(0), &None)
}

/// Generates Phrase Boosts queries from provided terms.
///
pub fn generate_phrase_queries_for_searchterm(
    persistence: &Persistence,
    fields: &Option<Vec<String>>,
    terms: HashSet<[&str; 2]>,
    levenshtein: Option<usize>,
    levenshtein_auto_limit: Option<usize>,
    boost_fields: &Option<HashMap<String, f32>>,
) -> Result<Vec<RequestPhraseBoost>, VelociError> {
    let mut phase_boost_requests = vec![];
    for [term_a, term_b] in terms.iter() {
        phase_boost_requests.extend(get_all_search_field_names(persistence, fields)?.iter().map(|field_name| RequestPhraseBoost {
            search1: RequestSearchPart {
                path: field_name.to_string(),
                terms: vec![term_a.to_string()],
                boost: boost_fields.as_ref().and_then(|boost_fields| boost_fields.get(field_name).map(|el| OrderedFloat(*el))),
                levenshtein_distance: Some(get_levenshteinn(term_a, levenshtein, levenshtein_auto_limit, false)),
                ..Default::default()
            },
            search2: RequestSearchPart {
                path: field_name.to_string(),
                terms: vec![term_b.to_string()],
                boost: boost_fields.as_ref().and_then(|boost_fields| boost_fields.get(field_name).map(|el| OrderedFloat(*el))),
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
    let requests = get_all_search_field_names(persistence, fields)?
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
