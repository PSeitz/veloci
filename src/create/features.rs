use fnv::FnvHashSet;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Features {
    BoostTextLocality,
    BoostingFieldData,
    Search,
    Filters,
    Facets,
    Select,
    WhyFound,
    Highlight,
    PhraseBoost,
}

impl Features {
    pub fn invert(features: &FnvHashSet<Features>) -> FnvHashSet<Features> {
        let all_features: &[Features] = &[
            Features::BoostTextLocality,
            Features::BoostingFieldData,
            Features::Search,
            Features::Filters,
            Features::Facets,
            Features::Select,
            Features::WhyFound,
            Features::Highlight,
            Features::PhraseBoost,
        ];

        all_features.iter().filter(|feature| features.contains(feature)).cloned().collect()
    }

    pub fn features_to_disabled_indices(features: &FnvHashSet<Features>) -> FnvHashSet<IndexCreationType> {
        let mut hashset = FnvHashSet::default();

        let add_if_features_not_used = |f: &[Features], index_cardinality: IndexCreationType, hashset: &mut FnvHashSet<IndexCreationType>| {
            for feature in f {
                if features.contains(feature) {
                    return;
                }
            }
            hashset.insert(index_cardinality);
        };

        add_if_features_not_used(
            &[Features::BoostTextLocality, Features::Highlight, Features::BoostingFieldData],
            IndexCreationType::TokensToTextID,
            &mut hashset,
        );
        add_if_features_not_used(&[Features::Search], IndexCreationType::TokenToAnchorIDScore, &mut hashset);

        add_if_features_not_used(&[Features::Select, Features::Facets], IndexCreationType::ParentToValueID, &mut hashset);
        add_if_features_not_used(&[Features::BoostingFieldData], IndexCreationType::ValueIDToParent, &mut hashset);

        add_if_features_not_used(&[Features::PhraseBoost], IndexCreationType::PhrasePairToAnchor, &mut hashset);
        add_if_features_not_used(&[Features::Select, Features::WhyFound], IndexCreationType::TextIDToTokenIds, &mut hashset);
        add_if_features_not_used(&[Features::BoostingFieldData], IndexCreationType::TextIDToParent, &mut hashset);
        add_if_features_not_used(&[Features::Facets, Features::Select], IndexCreationType::ParentToTextID, &mut hashset); //TODO can be diabled if facets is on non root element
        add_if_features_not_used(&[Features::BoostTextLocality, Features::Select, Features::Filters], IndexCreationType::TextIDToAnchor, &mut hashset);

        hashset
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Hash)]
pub enum IndexCreationType {
    TokensToTextID,       // Used by boost_text_locality, highlighting(why?), queries with boost indices on fields (slow search path)
    TokenToAnchorIDScore, //normal search
    PhrasePairToAnchor,   //phrase boost
    TextIDToTokenIds,     // highlight document(why_found, when select), select
    TextIDToParent,       // queries with boost indices on fields (slow search path)
    ParentToTextID,       // facets on root, facets on sublevel with no special direct index, select
    ParentToValueID,      // select
    ValueIDToParent,      // select
    TextIDToAnchor,       // Boost text locality, exact filters like facets
}
