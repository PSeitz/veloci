use crate::search::request::{boost_request::RequestBoostPart, snippet_info::SnippetInfo};
use core::cmp::Ordering;
use ordered_float::OrderedFloat;

/// Internal and External structure for defining the search requests tree.
#[serde(rename_all = "lowercase")]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum SearchRequest {
    Or(SearchTree),
    And(SearchTree),
    /// SearchRequest is a search on a field
    ///
    /// `RequestSearchPart` is boxed
    Search(RequestSearchPart),
}

// #[derive(Serialize, Deserialize, Default, Clone, Debug)]
// #[serde(default)]
#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct SearchTree {
    /// list of subqueries
    pub queries: Vec<SearchRequest>,
    #[serde(default)]
    /// Options which should be applied on the subqueries
    pub options: SearchRequestOptions,
}

// use std::fmt;
// use serde::de::{self, Deserialize, Deserializer, Visitor, SeqAccess, MapAccess};
// /// Custom deserializer supporting two formats
// ///
// /// 1. The default way: deserializing a map into the struct.
// /// 2. Omitting options: deserializing an array directly into the queries field, omitting the options.
// impl<'de> Deserialize<'de> for SearchTree {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {

//         struct SearchTreeVisitor;

//         impl<'de> Visitor<'de> for SearchTreeVisitor {
//             type Value = SearchTree;

//             fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
//                 formatter.write_str("struct SearchTree")
//             }

//             fn visit_seq<V>(self, seq: V) -> Result<SearchTree, V::Error>
//             where
//                 V: SeqAccess<'de>,
//             {
//                 Ok(SearchTree{
//                     queries: Deserialize::deserialize(de::value::SeqAccessDeserializer::new(seq))?,
//                     ..Default::default()
//                 })
//             }

//             fn visit_map<V>(self, map: V) -> Result<SearchTree, V::Error>
//             where
//                 V: MapAccess<'de>,
//             {
//                 Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
//             }
//         }

//         deserializer.deserialize_any(SearchTreeVisitor)
//     }
// }

// impl Default for SearchRequest {
//     fn default() -> SearchRequest {
//         SearchRequest::Noop
//     }
// }

impl SearchRequest {
    pub fn simplify(&mut self) {
        match self {
            // Pull up Or Conditions
            SearchRequest::Or(subtree) => {
                // move the tree down first, to do a complete simplify
                for sub_query in &mut subtree.queries {
                    sub_query.simplify();
                }
                let subitems = subtree
                    .queries
                    .drain_filter(|q| match q {
                        SearchRequest::Or(_) => true,
                        _ => false,
                    })
                    .flat_map(|q| match q {
                        SearchRequest::Or(search_tree) => search_tree.queries,
                        _ => unreachable!(),
                    })
                    .collect::<Vec<SearchRequest>>();

                subtree.queries.extend(subitems.into_iter());
            }

            // Pull up And Conditions
            SearchRequest::And(subtree) => {
                // move the tree down first, to do a complete simplify
                for sub_query in &mut subtree.queries {
                    sub_query.simplify();
                }
                let subitems = subtree
                    .queries
                    .drain_filter(|q| match q {
                        SearchRequest::And(_) => true,
                        _ => false,
                    })
                    .flat_map(|q| match q {
                        SearchRequest::And(search_tree) => search_tree.queries,
                        _ => unreachable!(),
                    })
                    .collect::<Vec<SearchRequest>>();

                subtree.queries.extend(subitems.into_iter());
            }
            SearchRequest::Search(_req) => {}
        }
    }

    pub fn get_options(&self) -> &SearchRequestOptions {
        match self {
            SearchRequest::Or(SearchTree { options, .. }) => options,
            SearchRequest::And(SearchTree { options, .. }) => options,
            SearchRequest::Search(el) => &el.options,
        }
    }

    pub fn get_options_mut(&mut self) -> &mut SearchRequestOptions {
        match self {
            SearchRequest::Or(SearchTree { options, .. }) => options,
            SearchRequest::And(SearchTree { options, .. }) => options,
            SearchRequest::Search(el) => &mut el.options,
        }
    }

    pub fn as_request_search_part(&self) -> &RequestSearchPart {
        match self {
            SearchRequest::Search(el) => el,
            _ => panic!("as_request_search_part"),
        }
    }

    pub fn get_boost(&self) -> Option<&[RequestBoostPart]> {
        self.get_options().boost.as_deref()
    }
}

#[derive(Serialize, Deserialize, Default, Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct SearchRequestOptions {
    #[serde(skip_serializing)]
    #[serde(default)]
    //TODO explain on part of query tree not yet supported, fix and enable on
    pub(crate) explain: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub top: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip: Option<usize>,

    // TODO check conceptual location of RequestBoostPart, how is it used
    /// Not working currently when used in RequestSearchPart, use Toplevel request.boost
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<Vec<RequestBoostPart>>,
}

fn is_false(val: &bool) -> bool {
    *val == false
}

/// Searching on a field, TODO rename
#[derive(Serialize, Deserialize, Default, Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct RequestSearchPart {
    pub path: String,
    pub terms: Vec<String>, //TODO only first term used currently

    #[serde(skip_serializing_if = "Option::is_none")]
    pub levenshtein_distance: Option<u32>,

    #[serde(skip_serializing_if = "is_false")]
    #[serde(default)]
    pub starts_with: bool,

    /// TODO document
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_value: Option<RequestBoostPart>,

    /// boosts the search part with this value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boost: Option<OrderedFloat<f32>>, // TODO Move to SearchRequestOptions, to boost whole subtrees

    /// Matches terms cases insensitive
    ///
    /// default is to ignore case
    ///
    /// e.g. "Term" would match "terM" with `ignore_case` true
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ignore_case: Option<bool>,

    /// return the snippet hit
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet: Option<bool>,

    /// Override default SnippetInfo
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet_info: Option<SnippetInfo>,

    //TODO REMOVE, AND MOVE TO SearchRequestOptions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top: Option<usize>,

    //TODO REMOVE, AND MOVE TO SearchRequestOptions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip: Option<usize>,

    #[serde(default)]
    pub options: SearchRequestOptions,
}

impl RequestSearchPart {
    pub fn is_explain(&self) -> bool {
        self.options.explain
    }
}

impl Ord for RequestSearchPart {
    fn cmp(&self, other: &RequestSearchPart) -> Ordering {
        format!("{:?}", self).cmp(&format!("{:?}", other))
    }
}
