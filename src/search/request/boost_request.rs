use ordered_float::OrderedFloat;

#[derive(Serialize, Deserialize, Default, Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct RequestBoostPart {
    pub path: String,
    pub boost_fun: Option<BoostFunction>,
    pub param: Option<OrderedFloat<f32>>,
    pub skip_when_score: Option<Vec<OrderedFloat<f32>>>,
    pub expression: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
#[derive(Default)]
pub enum BoostFunction {
    Log2,
    #[default]
    Log10,
    Multiply,
    Add,
}


