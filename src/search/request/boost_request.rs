use ordered_float::OrderedFloat;

#[derive(Serialize, Deserialize, Default, Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct RequestBoostPart {
    pub path: String,
    /// The boost function that is applied on the existing score
    pub boost_fun: Option<BoostFunction>,
    /// a fixed number that is added to BoostFunction
    pub param: Option<OrderedFloat<f32>>,
    pub skip_when_score: Option<Vec<OrderedFloat<f32>>>,
    /// A formula to boost the value. Can be really powerful, but is very limited in its syntax
    /// currently.
    /// Format is: "x op y"
    ///
    /// x, y can be $SCORE or a number
    /// op needs to be one of [*, +, -, /]
    /// Examples:
    /// "$SCORE + 2.0"
    /// "10.0 / $SCORE"
    /// "$SCORE * $SCORE"
    pub expression: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Default)]
pub enum BoostFunction {
    Log2,
    #[default]
    Log10,
    Multiply,
    Add,
    /// Replaces the score with the value. Can be used to order by a field.
    Replace,
}
