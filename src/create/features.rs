use fnv::FnvHashSet;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Features {
    All,
    TokensToTextID,
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
    pub fn get_default_features() -> FnvHashSet<Features> {
        [Features::Search, Features::TokensToTextID].iter().cloned().collect()
    }

    pub fn invert(features: &FnvHashSet<Features>) -> FnvHashSet<Features> {
        let all_features: &[Features] = &[
            Features::TokensToTextID,
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

    /// detects the needed index types from features
    pub fn features_to_disabled_indices(features: &FnvHashSet<Features>) -> FnvHashSet<IndexCreationType> {
        let mut hashset = FnvHashSet::default();

        let add_if_features_not_used = |f: &[Features], index_type: IndexCreationType, hashset: &mut FnvHashSet<IndexCreationType>| {
            for feature in f {
                if features.contains(feature) {
                    return;
                }
            }
            hashset.insert(index_type);
        };

        add_if_features_not_used(
            &[
                Features::All,
                Features::TokensToTextID,
                Features::BoostTextLocality,
                Features::Highlight,
                Features::BoostingFieldData,
            ],
            IndexCreationType::TokensToTextID,
            &mut hashset,
        );
        add_if_features_not_used(&[Features::All, Features::Search], IndexCreationType::TokenToAnchorIDScore, &mut hashset);

        add_if_features_not_used(&[Features::All, Features::Select, Features::Facets], IndexCreationType::ParentToValueID, &mut hashset);
        add_if_features_not_used(&[Features::All, Features::BoostingFieldData], IndexCreationType::ValueIDToParent, &mut hashset);

        add_if_features_not_used(&[Features::All, Features::PhraseBoost], IndexCreationType::PhrasePairToAnchor, &mut hashset);
        add_if_features_not_used(&[Features::All, Features::Select, Features::WhyFound], IndexCreationType::TextIDToTokenIds, &mut hashset);
        add_if_features_not_used(&[Features::All, Features::BoostingFieldData], IndexCreationType::TextIDToParent, &mut hashset);
        add_if_features_not_used(&[Features::All, Features::Facets, Features::Select], IndexCreationType::ParentToTextID, &mut hashset); //TODO can be diabled if facets is on non root element
        add_if_features_not_used(
            &[Features::All, Features::BoostTextLocality, Features::Select, Features::Filters],
            IndexCreationType::TextIDToAnchor,
            &mut hashset,
        );

        hashset
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Hash)]
pub enum IndexCreationType {
    TokensToTextID, // Used by boost_text_locality, highlighting(why?), when resolving from a field to boost data (boost but indirectly) TODO: detect when boost is on same level and activate
    TokenToAnchorIDScore, //normal search
    PhrasePairToAnchor, //phrase boost
    TextIDToTokenIds, // highlight document(why_found, when select), select
    TextIDToParent, // queries with boost indices on fields (slow search path)
    ParentToTextID, // facets on root, facets on sublevel with no special direct index, select
    ParentToValueID, // select
    ValueIDToParent, // select
    TextIDToAnchor, // Boost text locality, exact filters like facets
}
